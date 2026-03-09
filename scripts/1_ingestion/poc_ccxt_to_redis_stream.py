#!/usr/bin/env python
"""
Minimal PoC publisher:
- reads ticker updates via ccxt.pro
- emits tick and connection events into a Redis stream
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import socket
import sys
import time
import uuid
from datetime import datetime, timezone
from typing import Any

REDIS_IMPORT_ERROR: Exception | None = None
try:
    import redis
except Exception as exc:  # pragma: no cover - import environment dependent
    redis = None
    REDIS_IMPORT_ERROR = exc

CCXTPRO_IMPORT_ERROR: Exception | None = None
try:
    import ccxt.pro as ccxtpro
except Exception as exc:  # pragma: no cover - import environment dependent
    ccxtpro = None
    CCXTPRO_IMPORT_ERROR = exc

from ingestion_common import iter_tickers, parse_symbol_filters, select_symbols, supports_ws_flag


LOGGER = logging.getLogger("poc_ccxt_to_redis_stream")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish ccxt.pro ticker events into Redis stream.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis connection URL.")
    parser.add_argument("--stream", default="ingest:events:v1", help="Redis stream key.")
    parser.add_argument("--stream-maxlen", type=int, default=5_000_000, help="Approximate max stream length.")
    parser.add_argument("--exchange", default="binance", help="ccxt exchange id.")
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default="BTC/USDT",
        help=(
            "Optional comma-separated symbol filter. Supports exact symbols and SQL LIKE patterns "
            "(%% and _), case-insensitive (for example BTC/USDT,ETH/%%,%%sol/%%)."
        ),
    )
    parser.add_argument("--only-spot", action="store_true", help="Use spot markets only.")
    parser.add_argument(
        "--max-symbols",
        type=int,
        default=25,
        help="Maximum symbols selected from exchange markets. <=0 means all selected symbols.",
    )
    parser.add_argument("--schema-version", default="1.0", help="Schema version written to events.")
    parser.add_argument(
        "--producer-id",
        default=None,
        help="Optional producer id. Default: host-pid.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="Run duration in seconds. 0 means infinite.",
    )
    parser.add_argument(
        "--reconnect-sleep-s",
        type=float,
        default=2.0,
        help="Sleep before reconnect after watcher errors.",
    )
    parser.add_argument("--print-every", type=int, default=25, help="Log every N tick events.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level.",
    )
    return parser.parse_args()


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def ensure_ccxtpro_available() -> None:
    if ccxtpro is not None:
        return
    cause = f"{type(CCXTPRO_IMPORT_ERROR).__name__}: {CCXTPRO_IMPORT_ERROR}"
    raise SystemExit(
        "Failed to import 'ccxt.pro'. "
        f"interpreter={os.path.realpath(sys.executable)} | cause={cause}. "
        "Install with: python -m pip install -U ccxt"
    ) from CCXTPRO_IMPORT_ERROR


def ensure_redis_available() -> None:
    if redis is not None:
        return
    cause = f"{type(REDIS_IMPORT_ERROR).__name__}: {REDIS_IMPORT_ERROR}"
    raise SystemExit(
        "Failed to import 'redis'. "
        f"interpreter={os.path.realpath(sys.executable)} | cause={cause}. "
        "Install with: python -m pip install -U redis"
    ) from REDIS_IMPORT_ERROR


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def normalize_text_lower(value: Any) -> str:
    return normalize_text(value).lower()


def symbol_asset(symbol: str) -> str:
    if not symbol:
        return ""
    base = symbol.split("/", 1)[0]
    return normalize_text_lower(base)


def to_int_ms(value: Any, default_value: int) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    return int(default_value)


def make_dedup_key(parts: list[str]) -> str:
    raw = "|".join(parts).encode("utf-8", errors="ignore")
    return hashlib.sha256(raw).hexdigest()


def build_tick_event(
    *,
    exchange_id: str,
    symbol: str,
    ticker: dict[str, Any],
    schema_version: str,
    producer_id: str,
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    event_ts_ms = to_int_ms(ticker.get("timestamp"), now_ms)
    symbol_norm = normalize_text_lower(symbol)
    exchange_norm = normalize_text_lower(exchange_id)
    price = ticker.get("close")
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    last = ticker.get("last")
    dedup_key = make_dedup_key(
        [
            exchange_norm,
            symbol_norm,
            str(event_ts_ms),
            str(price),
            str(bid),
            str(ask),
            str(last),
        ]
    )
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "tick",
        "schema_version": schema_version,
        "source_system": "ccxt_ws",
        "producer_id": producer_id,
        "produced_at_ms": now_ms,
        "exchange_id": exchange_norm,
        "symbol": symbol,
        "symbol_norm": symbol_norm,
        "asset_norm": symbol_asset(symbol),
        "event_ts_ms": event_ts_ms,
        "ingestion_ts_ms": now_ms,
        "dedup_key": dedup_key,
        "payload": {
            "price": price,
            "bid": bid,
            "ask": ask,
            "last": last,
            "open": ticker.get("open"),
            "high": ticker.get("high"),
            "low": ticker.get("low"),
            "base_volume": ticker.get("baseVolume"),
            "quote_volume": ticker.get("quoteVolume"),
            "exchange_ts_ms": ticker.get("timestamp"),
            "raw_json": json.dumps(ticker, ensure_ascii=True, default=str, separators=(",", ":")),
        },
    }


def build_connection_event(
    *,
    exchange_id: str,
    symbol: str,
    schema_version: str,
    producer_id: str,
    event_name: str,
    severity: str,
    details: dict[str, Any],
) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    symbol_norm = normalize_text_lower(symbol)
    exchange_norm = normalize_text_lower(exchange_id)
    dedup_key = make_dedup_key(
        [
            exchange_norm,
            symbol_norm,
            event_name,
            severity,
            str(now_ms // 1000),
            str(details.get("message", "")),
        ]
    )
    return {
        "event_id": str(uuid.uuid4()),
        "event_type": "connection_event",
        "schema_version": schema_version,
        "source_system": "ccxt_ws",
        "producer_id": producer_id,
        "produced_at_ms": now_ms,
        "exchange_id": exchange_norm,
        "symbol": symbol,
        "symbol_norm": symbol_norm,
        "asset_norm": symbol_asset(symbol),
        "event_ts_ms": now_ms,
        "ingestion_ts_ms": now_ms,
        "dedup_key": dedup_key,
        "payload": {
            "event_name": event_name,
            "severity": severity,
            "details_json": json.dumps(details, ensure_ascii=True, default=str, separators=(",", ":")),
        },
    }


def publish_event(
    redis_client: redis.Redis,
    *,
    stream: str,
    stream_maxlen: int,
    event: dict[str, Any],
) -> str:
    fields = {
        "event_json": json.dumps(event, ensure_ascii=True, default=str, separators=(",", ":")),
        "event_type": str(event.get("event_type", "")),
        "exchange_id": str(event.get("exchange_id", "")),
        "symbol_norm": str(event.get("symbol_norm", "")),
        "schema_version": str(event.get("schema_version", "")),
    }
    return str(redis_client.xadd(stream, fields, maxlen=stream_maxlen, approximate=True))


def create_exchange(exchange_id: str) -> Any:
    exchange_class = getattr(ccxtpro, exchange_id, None)
    if exchange_class is None:
        raise SystemExit(f"Exchange class not found in ccxt.pro: {exchange_id}")
    return exchange_class(
        {
            "enableRateLimit": True,
            "newUpdates": True,
        }
    )


def summarize_symbols(symbols: list[str], limit: int = 8) -> str:
    if not symbols:
        return "-"
    if len(symbols) <= limit:
        return ", ".join(symbols)
    return f"{', '.join(symbols[:limit])}, ... (+{len(symbols) - limit})"


def publish_connection_event(
    redis_client: redis.Redis,
    *,
    stream: str,
    stream_maxlen: int,
    exchange_id: str,
    symbol: str,
    schema_version: str,
    producer_id: str,
    event_name: str,
    severity: str,
    details: dict[str, Any],
) -> None:
    event = build_connection_event(
        exchange_id=exchange_id,
        symbol=symbol,
        schema_version=schema_version,
        producer_id=producer_id,
        event_name=event_name,
        severity=severity,
        details=details,
    )
    publish_event(redis_client, stream=stream, stream_maxlen=stream_maxlen, event=event)


async def run(args: argparse.Namespace) -> None:
    ensure_ccxtpro_available()
    ensure_redis_available()

    redis_client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    redis_client.ping()

    producer_id = args.producer_id or f"{socket.gethostname()}-{os.getpid()}"
    exchange_id = normalize_text_lower(args.exchange)
    symbol_filters = parse_symbol_filters(args.symbols)
    if args.max_symbols == 0:
        raise SystemExit("--max-symbols must be > 0 or < 0 (0 is not allowed)")
    max_symbols = None if args.max_symbols < 0 else args.max_symbols

    exchange = create_exchange(exchange_id)
    await exchange.load_markets()
    selected_symbols = select_symbols(
        exchange.markets,
        only_spot=bool(args.only_spot),
        max_symbols=max_symbols,
        symbol_filters=symbol_filters,
    )
    if not selected_symbols:
        raise SystemExit(
            "No symbols selected after filters. "
            f"exchange={exchange_id} filters={symbol_filters or '-'} only_spot={args.only_spot}"
        )

    supports_watch_tickers = supports_ws_flag(getattr(exchange, "has", {}).get("watchTickers"))
    supports_watch_ticker = supports_ws_flag(getattr(exchange, "has", {}).get("watchTicker"))
    if not supports_watch_tickers and not supports_watch_ticker:
        raise SystemExit(f"Exchange does not support watchTickers/watchTicker: {exchange_id}")

    mode = "watch_tickers" if supports_watch_tickers else "watch_ticker"
    LOGGER.info(
        "Starting publisher | exchange=%s mode=%s symbols_selected=%s filters=%s sample=%s stream=%s producer_id=%s",
        exchange_id,
        mode,
        len(selected_symbols),
        symbol_filters or "-",
        summarize_symbols(selected_symbols),
        args.stream,
        producer_id,
    )

    started = time.monotonic()
    published_ticks = 0
    published_conn = 0
    symbol_set = set(selected_symbols)
    last_symbol = selected_symbols[0]

    try:
        watch_tickers_with_symbols = True
        next_symbol_index = 0
        while True:
            if args.duration_seconds > 0 and (time.monotonic() - started) >= args.duration_seconds:
                LOGGER.info("Duration reached. Stopping publisher.")
                break
            try:
                if mode == "watch_tickers":
                    if watch_tickers_with_symbols:
                        payload = await exchange.watch_tickers(selected_symbols)
                    else:
                        payload = await exchange.watch_tickers()

                    for ticker in iter_tickers(payload):
                        if not isinstance(ticker, dict):
                            continue
                        ticker_symbol = normalize_text(ticker.get("symbol"))
                        if not ticker_symbol or ticker_symbol not in symbol_set:
                            continue
                        last_symbol = ticker_symbol
                        event = build_tick_event(
                            exchange_id=exchange_id,
                            symbol=ticker_symbol,
                            ticker=ticker,
                            schema_version=args.schema_version,
                            producer_id=producer_id,
                        )
                        stream_id = publish_event(
                            redis_client,
                            stream=args.stream,
                            stream_maxlen=args.stream_maxlen,
                            event=event,
                        )
                        published_ticks += 1
                        if args.print_every > 0 and published_ticks % args.print_every == 0:
                            LOGGER.info(
                                "Published ticks=%s connection_events=%s last_stream_id=%s",
                                published_ticks,
                                published_conn,
                                stream_id,
                            )
                else:
                    current_symbol = selected_symbols[next_symbol_index % len(selected_symbols)]
                    next_symbol_index += 1
                    last_symbol = current_symbol
                    ticker = await exchange.watch_ticker(current_symbol)
                    if not isinstance(ticker, dict):
                        continue
                    event = build_tick_event(
                        exchange_id=exchange_id,
                        symbol=current_symbol,
                        ticker=ticker,
                        schema_version=args.schema_version,
                        producer_id=producer_id,
                    )
                    stream_id = publish_event(
                        redis_client,
                        stream=args.stream,
                        stream_maxlen=args.stream_maxlen,
                        event=event,
                    )
                    published_ticks += 1
                    if args.print_every > 0 and published_ticks % args.print_every == 0:
                        LOGGER.info(
                            "Published ticks=%s connection_events=%s last_stream_id=%s",
                            published_ticks,
                            published_conn,
                            stream_id,
                        )
            except TypeError as exc:
                # Some exchanges reject symbol lists for watch_tickers.
                if mode == "watch_tickers" and watch_tickers_with_symbols:
                    watch_tickers_with_symbols = False
                    publish_connection_event(
                        redis_client,
                        stream=args.stream,
                        stream_maxlen=args.stream_maxlen,
                        exchange_id=exchange_id,
                        symbol=last_symbol,
                        schema_version=args.schema_version,
                        producer_id=producer_id,
                        event_name="watch_tickers_symbol_list_unsupported",
                        severity="warning",
                        details={"error_type": type(exc).__name__, "message": str(exc)},
                    )
                    published_conn += 1
                    LOGGER.warning("watch_tickers(list) unsupported. Falling back to watch_tickers() without symbol list.")
                    continue
                raise
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                details = {"error_type": type(exc).__name__, "message": str(exc)}
                publish_connection_event(
                    redis_client,
                    stream=args.stream,
                    stream_maxlen=args.stream_maxlen,
                    exchange_id=exchange_id,
                    symbol=last_symbol,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="watch_error",
                    severity="error",
                    details=details,
                )
                published_conn += 1
                LOGGER.warning("%s failed. Published connection_event | error=%s", mode, details["message"])
                await asyncio.sleep(max(0.0, float(args.reconnect_sleep_s)))
                try:
                    await exchange.close()
                except Exception:  # noqa: BLE001
                    pass
                exchange = create_exchange(exchange_id)
                await exchange.load_markets()
                publish_connection_event(
                    redis_client,
                    stream=args.stream,
                    stream_maxlen=args.stream_maxlen,
                    exchange_id=exchange_id,
                    symbol=last_symbol,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="reconnect",
                    severity="info",
                    details={"message": "exchange session recreated"},
                )
                published_conn += 1
    finally:
        await exchange.close()
        LOGGER.info("Publisher stopped | ticks=%s connection_events=%s", published_ticks, published_conn)


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    asyncio.run(run(args))


if __name__ == "__main__":
    main()
