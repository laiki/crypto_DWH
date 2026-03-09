#!/usr/bin/env python
"""
Multi-exchange ccxt.pro publisher for Redis Streams.

Features:
- reads ticker updates from multiple exchanges in one worker process
- emits tick and connection events into a Redis stream
- supports short-lived buffering via Redis Streams MAXLEN
- supports orchestrator-based exchange sharding across worker processes
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import logging
import os
import signal
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

from ingestion_common import (
    DEFAULT_EXCLUDED_EXCHANGES,
    is_terminal_exclusion_error,
    iter_tickers,
    parse_exchange_list,
    parse_symbol_filters,
    resolve_excluded_exchanges,
    select_symbols,
    supports_ws_flag,
    terminal_error_reason,
)


LOGGER = logging.getLogger("ccxt_to_redis_stream")


def parse_args() -> argparse.Namespace:
    default_excluded_csv = ", ".join(sorted(DEFAULT_EXCLUDED_EXCHANGES))
    parser = argparse.ArgumentParser(
        description="Publish ccxt.pro ticker events from multiple exchanges into Redis Streams.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis connection URL.")
    parser.add_argument("--stream", default="ingest:events:v1", help="Redis stream key.")
    parser.add_argument(
        "--stream-maxlen",
        type=int,
        default=50_000,
        help="Approximate max stream length for the short-lived operational buffer.",
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated exchange IDs handled by this worker process. Default: all ccxt.pro exchanges after default exclusions.",
    )
    parser.add_argument(
        "--exchanges-exclude",
        default=None,
        help=(
            "Comma-separated exchange IDs additionally excluded from the requested exchange set. "
            f"Default excluded: {default_excluded_csv}."
        ),
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help=(
            "Optional comma-separated symbol filter. Supports exact symbols and SQL LIKE patterns "
            "(%% and _), case-insensitive (for example BTC/USDT,ETH/%%,%%sol/%%)."
        ),
    )
    parser.add_argument("--only-spot", action="store_true", help="Use spot markets only.")
    parser.add_argument(
        "--only-watch-tickers",
        action="store_true",
        help="Use only exchanges with watchTickers support.",
    )
    parser.add_argument(
        "--max-symbols-per-exchange",
        type=int,
        default=100,
        help="Maximum symbols selected from exchange markets. <=0 means all selected symbols.",
    )
    parser.add_argument(
        "--max-symbol-streams-per-exchange",
        type=int,
        default=25,
        help="Only for watch_ticker fallback: cap concurrent symbol streams per exchange. <=0 means all.",
    )
    parser.add_argument("--schema-version", default="1.0", help="Schema version written to events.")
    parser.add_argument(
        "--producer-id-prefix",
        default=None,
        help="Optional producer id prefix. Default: host-pid.",
    )
    parser.add_argument(
        "--queue-size",
        type=int,
        default=200_000,
        help="Maximum buffered events between exchange tasks and the Redis writer.",
    )
    parser.add_argument(
        "--exchange-start-delay-s",
        type=float,
        default=0.2,
        help="Delay between exchange worker starts inside this process.",
    )
    parser.add_argument(
        "--reconnect-base-s",
        type=float,
        default=1.0,
        help="Reconnect backoff base for exchange websocket loops.",
    )
    parser.add_argument(
        "--reconnect-max-s",
        type=float,
        default=30.0,
        help="Reconnect backoff max for exchange websocket loops.",
    )
    parser.add_argument(
        "--redis-retry-s",
        type=float,
        default=1.0,
        help="Sleep before retry after Redis publish failures.",
    )
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="Run duration in seconds. 0 means infinite.",
    )
    parser.add_argument("--print-every", type=int, default=100, help="Log every N published tick events.")
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


def make_producer_id(prefix: str, exchange_id: str) -> str:
    return f"{prefix}-{exchange_id}"


def summarize_symbols(symbols: list[str], limit: int = 8) -> str:
    if not symbols:
        return "-"
    if len(symbols) <= limit:
        return ", ".join(symbols)
    return f"{', '.join(symbols[:limit])}, ... (+{len(symbols) - limit})"


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


def connect_redis(redis_url: str) -> redis.Redis:
    client = redis.Redis.from_url(redis_url, decode_responses=True)
    client.ping()
    return client


def create_exchange(exchange_id: str) -> Any:
    exchange_class = getattr(ccxtpro, exchange_id, None)
    if exchange_class is None:
        raise SystemExit(f"Exchange class not found in ccxt.pro: {exchange_id}")
    return exchange_class({"enableRateLimit": True, "newUpdates": True})


def limit_symbol_streams(symbols: list[str], max_symbol_streams_per_exchange: int) -> list[str]:
    if max_symbol_streams_per_exchange <= 0:
        return symbols
    return symbols[:max_symbol_streams_per_exchange]


async def enqueue_connection_event(
    queue: asyncio.Queue[dict[str, Any]],
    *,
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
    await queue.put(event)


async def enqueue_tick_event(
    queue: asyncio.Queue[dict[str, Any]],
    *,
    exchange_id: str,
    ticker: dict[str, Any],
    schema_version: str,
    producer_id: str,
) -> None:
    symbol = normalize_text(ticker.get("symbol"))
    if not symbol:
        return
    await queue.put(
        build_tick_event(
            exchange_id=exchange_id,
            symbol=symbol,
            ticker=ticker,
            schema_version=schema_version,
            producer_id=producer_id,
        )
    )


async def redis_publish_loop(
    *,
    redis_url: str,
    stream: str,
    stream_maxlen: int,
    queue: asyncio.Queue[dict[str, Any]],
    stop_event: asyncio.Event,
    redis_retry_s: float,
    print_every: int,
) -> None:
    redis_client = await asyncio.to_thread(connect_redis, redis_url)
    published_total = 0
    published_ticks = 0
    published_connection_events = 0
    try:
        while not stop_event.is_set() or not queue.empty():
            try:
                event = await asyncio.wait_for(queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            try:
                while True:
                    try:
                        stream_id = await asyncio.to_thread(
                            publish_event,
                            redis_client,
                            stream=stream,
                            stream_maxlen=stream_maxlen,
                            event=event,
                        )
                        published_total += 1
                        if event.get("event_type") == "tick":
                            published_ticks += 1
                        else:
                            published_connection_events += 1
                        if print_every > 0 and published_ticks > 0 and published_ticks % print_every == 0:
                            LOGGER.info(
                                "Published ticks=%s connection_events=%s total=%s last_stream_id=%s",
                                published_ticks,
                                published_connection_events,
                                published_total,
                                stream_id,
                            )
                        break
                    except Exception as exc:  # noqa: BLE001
                        LOGGER.warning("Redis publish failed. Retrying in %.2fs | error=%r", redis_retry_s, exc)
                        await asyncio.sleep(max(0.0, redis_retry_s))
                        redis_client = await asyncio.to_thread(connect_redis, redis_url)
            finally:
                queue.task_done()
    finally:
        LOGGER.info(
            "Redis publisher stopped | ticks=%s connection_events=%s total=%s",
            published_ticks,
            published_connection_events,
            published_total,
        )


async def watch_tickers_loop(
    *,
    exchange: Any,
    exchange_id: str,
    producer_id: str,
    symbols: list[str],
    queue: asyncio.Queue[dict[str, Any]],
    stop_event: asyncio.Event,
    args: argparse.Namespace,
) -> None:
    backoff = args.reconnect_base_s
    use_symbol_list = True
    last_symbol = symbols[0] if symbols else ""
    symbol_set = set(symbols)
    while not stop_event.is_set():
        try:
            if use_symbol_list:
                tickers_payload = await exchange.watch_tickers(symbols)
            else:
                tickers_payload = await exchange.watch_tickers()
            for ticker in iter_tickers(tickers_payload):
                if not isinstance(ticker, dict):
                    continue
                ticker_symbol = normalize_text(ticker.get("symbol"))
                if not ticker_symbol or ticker_symbol not in symbol_set:
                    continue
                last_symbol = ticker_symbol
                await enqueue_tick_event(
                    queue,
                    exchange_id=exchange_id,
                    ticker=ticker,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                )
            backoff = args.reconnect_base_s
        except TypeError as exc:
            if use_symbol_list:
                use_symbol_list = False
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol=last_symbol,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="watch_tickers_signature_fallback",
                    severity="warning",
                    details={"error_type": type(exc).__name__, "message": str(exc)},
                )
                continue
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol=last_symbol,
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="disconnect",
                severity="warning",
                details={"mode": "watch_tickers", "error": repr(exc)},
            )
            sleep_s = min(backoff, args.reconnect_max_s)
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2.0, args.reconnect_max_s)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol=last_symbol,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="terminal_error_excluded",
                    severity="error",
                    details={"error_class": terminal_error_reason(exc), "error": repr(exc)},
                )
                return
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol=last_symbol,
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="disconnect",
                severity="error",
                details={"mode": "watch_tickers", "error": repr(exc)},
            )
            sleep_s = min(backoff, args.reconnect_max_s)
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol=last_symbol,
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="reconnect_wait",
                severity="info",
                details={"mode": "watch_tickers", "sleep_seconds": sleep_s},
            )
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2.0, args.reconnect_max_s)


async def watch_ticker_symbol_loop(
    *,
    exchange: Any,
    exchange_id: str,
    symbol: str,
    producer_id: str,
    queue: asyncio.Queue[dict[str, Any]],
    stop_event: asyncio.Event,
    terminal_error_event: asyncio.Event,
    terminal_error_state: dict[str, str],
    args: argparse.Namespace,
) -> None:
    backoff = args.reconnect_base_s
    while not stop_event.is_set():
        try:
            ticker = await exchange.watch_ticker(symbol)
            if isinstance(ticker, dict):
                await enqueue_tick_event(
                    queue,
                    exchange_id=exchange_id,
                    ticker=ticker,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                )
            backoff = args.reconnect_base_s
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                terminal_error_state["reason"] = terminal_error_reason(exc)
                terminal_error_state["error"] = repr(exc)
                terminal_error_event.set()
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol=symbol,
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="terminal_error_excluded",
                    severity="error",
                    details={"error_class": terminal_error_state["reason"], "error": terminal_error_state["error"]},
                )
                return
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol=symbol,
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="disconnect",
                severity="error",
                details={"mode": "watch_ticker", "error": repr(exc)},
            )
            sleep_s = min(backoff, args.reconnect_max_s)
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol=symbol,
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="reconnect_wait",
                severity="info",
                details={"mode": "watch_ticker", "sleep_seconds": sleep_s},
            )
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2.0, args.reconnect_max_s)


async def run_exchange(
    *,
    exchange_id: str,
    args: argparse.Namespace,
    symbol_filters: list[str],
    queue: asyncio.Queue[dict[str, Any]],
    stop_event: asyncio.Event,
    producer_id_prefix: str,
) -> None:
    exchange_cls = getattr(ccxtpro, exchange_id, None)
    if exchange_cls is None:
        LOGGER.warning("Exchange '%s' is not available in ccxt.pro.", exchange_id)
        return

    producer_id = make_producer_id(producer_id_prefix, exchange_id)
    reconnect_backoff = args.reconnect_base_s
    while not stop_event.is_set():
        exchange = exchange_cls({"enableRateLimit": True, "newUpdates": True})
        try:
            await exchange.load_markets()
            max_symbols = None if args.max_symbols_per_exchange <= 0 else args.max_symbols_per_exchange
            selected_symbols = select_symbols(
                exchange.markets,
                only_spot=bool(args.only_spot),
                max_symbols=max_symbols,
                symbol_filters=symbol_filters,
            )
            if not selected_symbols:
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol="",
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="exchange_skipped_no_symbols",
                    severity="warning",
                    details={"filters": symbol_filters, "only_spot": bool(args.only_spot)},
                )
                return

            watch_tickers_supported = supports_ws_flag(getattr(exchange, "has", {}).get("watchTickers"))
            watch_ticker_supported = supports_ws_flag(getattr(exchange, "has", {}).get("watchTicker"))
            if args.only_watch_tickers and not watch_tickers_supported:
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol=selected_symbols[0],
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="exchange_skipped_only_watch_tickers",
                    severity="warning",
                    details={
                        "watch_tickers": watch_tickers_supported,
                        "watch_ticker": watch_ticker_supported,
                    },
                )
                return

            if not watch_tickers_supported and not watch_ticker_supported:
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol=selected_symbols[0],
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="exchange_skipped_unsupported",
                    severity="warning",
                    details={"message": "watchTickers and watchTicker are not supported"},
                )
                return

            LOGGER.info(
                "Starting exchange publisher | exchange=%s mode=%s symbols_selected=%s filters=%s sample=%s",
                exchange_id,
                "watch_tickers" if watch_tickers_supported else "watch_ticker",
                len(selected_symbols),
                symbol_filters or "-",
                summarize_symbols(selected_symbols),
            )
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol=selected_symbols[0],
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="worker_start",
                severity="info",
                details={
                    "symbols_selected": len(selected_symbols),
                    "mode": "watch_tickers" if watch_tickers_supported else "watch_ticker",
                },
            )

            if watch_tickers_supported:
                await watch_tickers_loop(
                    exchange=exchange,
                    exchange_id=exchange_id,
                    producer_id=producer_id,
                    symbols=selected_symbols,
                    queue=queue,
                    stop_event=stop_event,
                    args=args,
                )
                return

            symbol_streams = limit_symbol_streams(selected_symbols, int(args.max_symbol_streams_per_exchange))
            terminal_error_event = asyncio.Event()
            terminal_error_state: dict[str, str] = {}
            tasks = [
                asyncio.create_task(
                    watch_ticker_symbol_loop(
                        exchange=exchange,
                        exchange_id=exchange_id,
                        symbol=symbol,
                        producer_id=producer_id,
                        queue=queue,
                        stop_event=stop_event,
                        terminal_error_event=terminal_error_event,
                        terminal_error_state=terminal_error_state,
                        args=args,
                    ),
                    name=f"watch_ticker:{exchange_id}:{symbol}",
                )
                for symbol in symbol_streams
            ]
            try:
                while not stop_event.is_set():
                    if terminal_error_event.is_set():
                        return
                    if all(task.done() for task in tasks):
                        break
                    await asyncio.sleep(0.5)
            finally:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                await enqueue_connection_event(
                    queue,
                    exchange_id=exchange_id,
                    symbol="",
                    schema_version=args.schema_version,
                    producer_id=producer_id,
                    event_name="terminal_error_excluded",
                    severity="error",
                    details={"error_class": terminal_error_reason(exc), "error": repr(exc)},
                )
                return
            sleep_s = min(reconnect_backoff, args.reconnect_max_s)
            LOGGER.warning("Exchange runtime failed | exchange=%s sleep=%.2fs error=%r", exchange_id, sleep_s, exc)
            await enqueue_connection_event(
                queue,
                exchange_id=exchange_id,
                symbol="",
                schema_version=args.schema_version,
                producer_id=producer_id,
                event_name="exchange_runtime_error",
                severity="error",
                details={"error": repr(exc), "sleep_seconds": sleep_s},
            )
            await asyncio.sleep(sleep_s)
            reconnect_backoff = min(reconnect_backoff * 2.0, args.reconnect_max_s)
        finally:
            with contextlib.suppress(Exception):
                await exchange.close()


async def async_main(args: argparse.Namespace) -> None:
    ensure_ccxtpro_available()
    ensure_redis_available()

    available_exchanges = list(getattr(ccxtpro, "exchanges", []))
    requested_exchanges = parse_exchange_list(args.exchanges, available_exchanges)
    excluded_exchanges = resolve_excluded_exchanges(
        args.exchanges_exclude,
        available_exchanges,
        DEFAULT_EXCLUDED_EXCHANGES,
    )
    selected_exchanges = [exchange_id for exchange_id in requested_exchanges if exchange_id not in excluded_exchanges]
    excluded_from_selection = sorted(set(requested_exchanges) - set(selected_exchanges))
    if not selected_exchanges:
        raise SystemExit("No valid exchanges available after exclusions.")
    if excluded_from_selection:
        LOGGER.info(
            "Exchanges excluded from selection (%s): %s",
            len(excluded_from_selection),
            ", ".join(excluded_from_selection),
        )

    symbol_filters = parse_symbol_filters(args.symbols)
    producer_id_prefix = args.producer_id_prefix or f"{socket.gethostname()}-{os.getpid()}"
    queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=int(args.queue_size))
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        if not stop_event.is_set():
            LOGGER.info("Stop signal received. Starting graceful shutdown.")
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):
            loop.add_signal_handler(sig, _request_stop)

    await asyncio.to_thread(connect_redis, args.redis_url)
    LOGGER.info(
        "Starting Redis stream publisher | exchanges=%s queue_size=%s stream=%s redis_url=%s",
        len(selected_exchanges),
        args.queue_size,
        args.stream,
        args.redis_url,
    )

    publisher_task = asyncio.create_task(
        redis_publish_loop(
            redis_url=args.redis_url,
            stream=args.stream,
            stream_maxlen=int(args.stream_maxlen),
            queue=queue,
            stop_event=stop_event,
            redis_retry_s=float(args.redis_retry_s),
            print_every=int(args.print_every),
        ),
        name="redis_publisher",
    )

    exchange_tasks: list[asyncio.Task[Any]] = []
    try:
        started_monotonic = time.monotonic()
        for idx, exchange_id in enumerate(selected_exchanges):
            task = asyncio.create_task(
                run_exchange(
                    exchange_id=exchange_id,
                    args=args,
                    symbol_filters=symbol_filters,
                    queue=queue,
                    stop_event=stop_event,
                    producer_id_prefix=producer_id_prefix,
                ),
                name=f"exchange_worker:{exchange_id}",
            )
            exchange_tasks.append(task)
            if idx < len(selected_exchanges) - 1 and args.exchange_start_delay_s > 0:
                await asyncio.sleep(float(args.exchange_start_delay_s))

        while not stop_event.is_set():
            if args.duration_seconds > 0 and (time.monotonic() - started_monotonic) >= float(args.duration_seconds):
                LOGGER.info("Duration reached. Starting graceful shutdown.")
                stop_event.set()
                break
            if exchange_tasks and all(task.done() for task in exchange_tasks):
                LOGGER.warning("All exchange workers have exited. Starting shutdown.")
                stop_event.set()
                break
            await asyncio.sleep(1.0)
    finally:
        stop_event.set()
        await asyncio.gather(*exchange_tasks, return_exceptions=True)
        await queue.join()
        await publisher_task


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        LOGGER.info("Interrupted by user.")


if __name__ == "__main__":
    main()
