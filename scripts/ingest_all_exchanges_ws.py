#!/usr/bin/env python3
"""
WebSocket ingestion for ccxt.pro exchanges into SQLite.

Features:
- Streams ticker data for all available symbols per exchange.
- Writes every received record with local UTC ingestion timestamp.
- Logs disconnect/reconnect events into SQLite.
- Reconnects automatically with exponential backoff.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import json
import logging
import signal
import sqlite3
import time
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any, Iterable

try:
    import ccxt.pro as ccxtpro
except ImportError as exc:
    raise SystemExit(
        "ccxt.pro is not installed. Please install it (e.g. pip install ccxtpro)."
    ) from exc

try:
    from ccxt.base.errors import AuthenticationError as CcxtAuthenticationError
except Exception:  # noqa: BLE001
    CcxtAuthenticationError = None


LOGGER = logging.getLogger("crypto_ws_ingestion")

DEFAULT_EXCLUDED_EXCHANGES = {
    "alpaca",
    "arkham",
    "bequant",
    "bitfinex",
    "bitmex",
    "bitopro",
    "blockchaincom",
    "oxfun",
    "probit",
}

TERMINAL_EXCLUSION_ERROR_NAMES = {
    "authenticationerror",
    "exchangenotavailable",
    "requesttimeout",
    "exchangeerror",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def ms_to_utc_iso(ms: Any) -> str | None:
    if not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def json_dumps_safe(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, default=str, separators=(",", ":"))


def iter_tickers(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        if "symbol" in payload:
            yield payload
            return
        for value in payload.values():
            if isinstance(value, dict) and value.get("symbol"):
                yield value
        return
    if isinstance(payload, list):
        for value in payload:
            if isinstance(value, dict) and value.get("symbol"):
                yield value


class SQLiteWriter:
    def __init__(self, db_path: Path, batch_size: int, flush_interval_s: float) -> None:
        self.db_path = db_path
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self._conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._conn.execute("PRAGMA temp_store=MEMORY;")
        self._create_schema()

    def _create_schema(self) -> None:
        self._conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS market_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ingestion_ts_utc TEXT NOT NULL,
                exchange_id TEXT NOT NULL,
                symbol TEXT NOT NULL,
                market_type TEXT,
                price REAL,
                bid REAL,
                ask REAL,
                last REAL,
                open REAL,
                high REAL,
                low REAL,
                base_volume REAL,
                quote_volume REAL,
                exchange_ts_ms INTEGER,
                exchange_ts_utc TEXT,
                raw_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_market_ticks_ex_sym_ts
                ON market_ticks(exchange_id, symbol, ingestion_ts_utc);

            CREATE TABLE IF NOT EXISTS connection_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_ts_utc TEXT NOT NULL,
                exchange_id TEXT NOT NULL,
                symbol TEXT,
                event_type TEXT NOT NULL,
                details_json TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_connection_events_ex_ts
                ON connection_events(exchange_id, event_ts_utc);
            """
        )
        self._conn.commit()

    async def run(self, queue: asyncio.Queue[tuple[str, dict[str, Any]]], stop_event: asyncio.Event) -> None:
        pending: list[tuple[str, dict[str, Any]]] = []
        last_flush = time.monotonic()
        while not stop_event.is_set() or not queue.empty() or pending:
            try:
                item = await asyncio.wait_for(queue.get(), timeout=self.flush_interval_s)
                pending.append(item)
            except asyncio.TimeoutError:
                pass

            should_flush = (
                len(pending) >= self.batch_size
                or (pending and (time.monotonic() - last_flush) >= self.flush_interval_s)
                or (pending and stop_event.is_set() and queue.empty())
            )
            if not should_flush:
                continue

            self._flush(pending)
            for _ in pending:
                queue.task_done()
            pending.clear()
            last_flush = time.monotonic()

    def _flush(self, items: list[tuple[str, dict[str, Any]]]) -> None:
        tick_rows: list[tuple[Any, ...]] = []
        event_rows: list[tuple[Any, ...]] = []

        for kind, payload in items:
            if kind == "tick":
                tick_rows.append(
                    (
                        payload["ingestion_ts_utc"],
                        payload["exchange_id"],
                        payload["symbol"],
                        payload.get("market_type"),
                        payload.get("price"),
                        payload.get("bid"),
                        payload.get("ask"),
                        payload.get("last"),
                        payload.get("open"),
                        payload.get("high"),
                        payload.get("low"),
                        payload.get("base_volume"),
                        payload.get("quote_volume"),
                        payload.get("exchange_ts_ms"),
                        payload.get("exchange_ts_utc"),
                        payload["raw_json"],
                    )
                )
            elif kind == "event":
                event_rows.append(
                    (
                        payload["event_ts_utc"],
                        payload["exchange_id"],
                        payload.get("symbol"),
                        payload["event_type"],
                        payload.get("details_json"),
                    )
                )

        if tick_rows:
            self._conn.executemany(
                """
                INSERT INTO market_ticks (
                    ingestion_ts_utc, exchange_id, symbol, market_type, price, bid, ask, last,
                    open, high, low, base_volume, quote_volume, exchange_ts_ms, exchange_ts_utc, raw_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                tick_rows,
            )
        if event_rows:
            self._conn.executemany(
                """
                INSERT INTO connection_events (
                    event_ts_utc, exchange_id, symbol, event_type, details_json
                ) VALUES (?, ?, ?, ?, ?)
                """,
                event_rows,
            )
        self._conn.commit()

    async def close(self) -> None:
        self._conn.close()


async def put_event(
    queue: asyncio.Queue[tuple[str, dict[str, Any]]],
    exchange_id: str,
    event_type: str,
    details: dict[str, Any] | None = None,
    symbol: str | None = None,
) -> None:
    payload = {
        "event_ts_utc": utc_now_iso(),
        "exchange_id": exchange_id,
        "symbol": symbol,
        "event_type": event_type,
        "details_json": json_dumps_safe(details or {}),
    }
    await queue.put(("event", payload))


async def put_tick(
    queue: asyncio.Queue[tuple[str, dict[str, Any]]],
    exchange_id: str,
    ticker: dict[str, Any],
) -> None:
    exchange_ts_ms = ticker.get("timestamp")
    payload = {
        "ingestion_ts_utc": utc_now_iso(),
        "exchange_id": exchange_id,
        "symbol": ticker.get("symbol", "UNKNOWN"),
        "market_type": ticker.get("type"),
        "price": ticker.get("last") if ticker.get("last") is not None else ticker.get("close"),
        "bid": ticker.get("bid"),
        "ask": ticker.get("ask"),
        "last": ticker.get("last"),
        "open": ticker.get("open"),
        "high": ticker.get("high"),
        "low": ticker.get("low"),
        "base_volume": ticker.get("baseVolume"),
        "quote_volume": ticker.get("quoteVolume"),
        "exchange_ts_ms": exchange_ts_ms,
        "exchange_ts_utc": ms_to_utc_iso(exchange_ts_ms),
        "raw_json": json_dumps_safe(ticker),
    }
    await queue.put(("tick", payload))


def select_symbols(
    markets: dict[str, dict[str, Any]],
    only_spot: bool,
    max_symbols: int | None,
) -> list[str]:
    symbols: list[str] = []
    for symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        if market.get("active") is False:
            continue
        if only_spot and not market.get("spot", False):
            continue
        symbols.append(symbol)
    symbols = sorted(set(symbols))
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[:max_symbols]
    return symbols


def supports_ws_flag(flag_value: Any) -> bool:
    return flag_value is True


def error_class_name(exc: Exception) -> str:
    return exc.__class__.__name__


def is_auth_error(exc: Exception) -> bool:
    if CcxtAuthenticationError is not None and isinstance(exc, CcxtAuthenticationError):
        return True
    error_name = error_class_name(exc).lower()
    if "authenticationerror" in error_name:
        return True
    message = str(exc).lower()
    auth_markers = (
        'requires "apikey" credential',
        "api key",
        "not authenticated",
        "authentication failed",
        "invalid api",
    )
    return any(marker in message for marker in auth_markers)


def is_terminal_exclusion_error(exc: Exception) -> bool:
    if is_auth_error(exc):
        return True
    return error_class_name(exc).lower() in TERMINAL_EXCLUSION_ERROR_NAMES


def terminal_error_reason(exc: Exception) -> str:
    return error_class_name(exc)


async def watch_tickers_loop(
    exchange: Any,
    exchange_id: str,
    symbols: list[str],
    queue: asyncio.Queue[tuple[str, dict[str, Any]]],
    stop_event: asyncio.Event,
    reconnect_base_s: float,
    reconnect_max_s: float,
) -> None:
    backoff = reconnect_base_s
    use_symbol_list = True
    had_disconnect = False
    while not stop_event.is_set():
        try:
            if use_symbol_list:
                tickers_payload = await exchange.watch_tickers(symbols)
            else:
                tickers_payload = await exchange.watch_tickers()
            if had_disconnect:
                await put_event(
                    queue,
                    exchange_id,
                    "reconnected",
                    details={"mode": "watch_tickers"},
                )
                had_disconnect = False
            for ticker in iter_tickers(tickers_payload):
                await put_tick(queue, exchange_id, ticker)
            backoff = reconnect_base_s
        except TypeError as exc:
            if use_symbol_list:
                use_symbol_list = False
                await put_event(
                    queue,
                    exchange_id,
                    "watch_tickers_signature_fallback",
                    details={"error": repr(exc)},
                )
                continue
            await put_event(
                queue,
                exchange_id,
                "disconnect",
                details={"mode": "watch_tickers", "error": repr(exc)},
            )
            sleep_s = min(backoff, reconnect_max_s)
            await put_event(
                queue,
                exchange_id,
                "reconnect_wait",
                details={"mode": "watch_tickers", "sleep_seconds": sleep_s},
            )
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2.0, reconnect_max_s)
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                reason = terminal_error_reason(exc)
                await put_event(
                    queue,
                    exchange_id,
                    "terminal_error_excluded",
                    details={"mode": "watch_tickers", "error_class": reason, "error": repr(exc)},
                )
                await put_event(
                    queue,
                    exchange_id,
                    "exchange_removed_from_polling",
                    details={"reason": reason},
                )
                return
            had_disconnect = True
            await put_event(
                queue,
                exchange_id,
                "disconnect",
                details={"mode": "watch_tickers", "error": repr(exc)},
            )
            sleep_s = min(backoff, reconnect_max_s)
            await put_event(
                queue,
                exchange_id,
                "reconnect_wait",
                details={"mode": "watch_tickers", "sleep_seconds": sleep_s},
            )
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2.0, reconnect_max_s)


async def watch_ticker_symbol_loop(
    exchange: Any,
    exchange_id: str,
    symbol: str,
    queue: asyncio.Queue[tuple[str, dict[str, Any]]],
    stop_event: asyncio.Event,
    terminal_error_event: asyncio.Event,
    terminal_error_state: dict[str, str],
    reconnect_base_s: float,
    reconnect_max_s: float,
) -> None:
    backoff = reconnect_base_s
    had_disconnect = False
    while not stop_event.is_set():
        try:
            ticker = await exchange.watch_ticker(symbol)
            if had_disconnect:
                await put_event(
                    queue,
                    exchange_id,
                    "reconnected",
                    details={"mode": "watch_ticker"},
                    symbol=symbol,
                )
                had_disconnect = False
            if isinstance(ticker, dict):
                await put_tick(queue, exchange_id, ticker)
            backoff = reconnect_base_s
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                reason = terminal_error_reason(exc)
                await put_event(
                    queue,
                    exchange_id,
                    "terminal_error_excluded",
                    details={"mode": "watch_ticker", "error_class": reason, "error": repr(exc)},
                    symbol=symbol,
                )
                terminal_error_state["reason"] = reason
                terminal_error_state["error"] = repr(exc)
                terminal_error_event.set()
                return
            had_disconnect = True
            await put_event(
                queue,
                exchange_id,
                "disconnect",
                details={"mode": "watch_ticker", "error": repr(exc)},
                symbol=symbol,
            )
            sleep_s = min(backoff, reconnect_max_s)
            await put_event(
                queue,
                exchange_id,
                "reconnect_wait",
                details={"mode": "watch_ticker", "sleep_seconds": sleep_s},
                symbol=symbol,
            )
            await asyncio.sleep(sleep_s)
            backoff = min(backoff * 2.0, reconnect_max_s)


async def run_exchange(
    exchange_id: str,
    args: argparse.Namespace,
    queue: asyncio.Queue[tuple[str, dict[str, Any]]],
    stop_event: asyncio.Event,
) -> None:
    exchange_cls = getattr(ccxtpro, exchange_id, None)
    if exchange_cls is None:
        LOGGER.warning("Exchange '%s' is not available in ccxt.pro.", exchange_id)
        return

    await put_event(queue, exchange_id, "worker_start", {"exchange_id": exchange_id})

    reconnect_backoff = args.reconnect_base_s
    while not stop_event.is_set():
        exchange = exchange_cls(
            {
                "enableRateLimit": True,
                "newUpdates": True,
            }
        )
        try:
            await put_event(queue, exchange_id, "connecting")
            markets = await exchange.load_markets()
            symbols = select_symbols(markets, args.only_spot, args.max_symbols_per_exchange)
            if not symbols:
                await put_event(queue, exchange_id, "no_symbols")
                return

            watch_tickers_supported = supports_ws_flag(exchange.has.get("watchTickers"))
            watch_ticker_supported = supports_ws_flag(exchange.has.get("watchTicker"))

            await put_event(
                queue,
                exchange_id,
                "connected",
                details={
                    "symbols_total": len(symbols),
                    "watch_tickers": watch_tickers_supported,
                    "watch_ticker": watch_ticker_supported,
                },
            )

            if args.only_watch_tickers and not watch_tickers_supported:
                await put_event(
                    queue,
                    exchange_id,
                    "exchange_skipped_only_watch_tickers",
                    details={
                        "reason": "watch_tickers_not_supported",
                        "watch_tickers": watch_tickers_supported,
                        "watch_ticker": watch_ticker_supported,
                    },
                )
                return

            reconnect_backoff = args.reconnect_base_s

            if watch_tickers_supported:
                await put_event(queue, exchange_id, "stream_mode", {"mode": "watch_tickers"})
                await watch_tickers_loop(
                    exchange=exchange,
                    exchange_id=exchange_id,
                    symbols=symbols,
                    queue=queue,
                    stop_event=stop_event,
                    reconnect_base_s=args.reconnect_base_s,
                    reconnect_max_s=args.reconnect_max_s,
                )
                return

            if watch_ticker_supported:
                await put_event(queue, exchange_id, "stream_mode", {"mode": "watch_ticker"})
                symbol_list = symbols
                if args.max_symbol_streams_per_exchange is not None and args.max_symbol_streams_per_exchange > 0:
                    symbol_list = symbols[: args.max_symbol_streams_per_exchange]
                    await put_event(
                        queue,
                        exchange_id,
                        "stream_limit_applied",
                        details={
                            "requested_symbols": len(symbols),
                            "streamed_symbols": len(symbol_list),
                            "max_symbol_streams_per_exchange": args.max_symbol_streams_per_exchange,
                        },
                    )

                terminal_error_event = asyncio.Event()
                terminal_error_state: dict[str, str] = {}
                symbol_tasks = [
                    asyncio.create_task(
                        watch_ticker_symbol_loop(
                            exchange=exchange,
                            exchange_id=exchange_id,
                            symbol=symbol,
                            queue=queue,
                            stop_event=stop_event,
                            terminal_error_event=terminal_error_event,
                            terminal_error_state=terminal_error_state,
                            reconnect_base_s=args.reconnect_base_s,
                            reconnect_max_s=args.reconnect_max_s,
                        ),
                        name=f"{exchange_id}:{symbol}",
                    )
                    for symbol in symbol_list
                ]
                while not stop_event.is_set() and not terminal_error_event.is_set():
                    await asyncio.sleep(0.2)
                if terminal_error_event.is_set():
                    await put_event(
                        queue,
                        exchange_id,
                        "exchange_removed_from_polling",
                        details={
                            "reason": terminal_error_state.get("reason", "terminal_error"),
                            "error": terminal_error_state.get("error", ""),
                        },
                    )
                for task in symbol_tasks:
                    task.cancel()
                await asyncio.gather(*symbol_tasks, return_exceptions=True)
                return

            await put_event(
                queue,
                exchange_id,
                "unsupported",
                details={"watchTickers": exchange.has.get("watchTickers"), "watchTicker": exchange.has.get("watchTicker")},
            )
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                reason = terminal_error_reason(exc)
                await put_event(
                    queue,
                    exchange_id,
                    "terminal_error_excluded",
                    details={"phase": "setup", "error_class": reason, "error": repr(exc)},
                )
                await put_event(
                    queue,
                    exchange_id,
                    "exchange_removed_from_polling",
                    details={"reason": reason},
                )
                LOGGER.warning(
                    "Exchange %s removed from polling due to terminal error (%s): %s",
                    exchange_id,
                    reason,
                    repr(exc),
                )
                return
            await put_event(
                queue,
                exchange_id,
                "setup_error",
                details={"error": repr(exc), "retry_in_seconds": reconnect_backoff},
            )
            await asyncio.sleep(reconnect_backoff)
            reconnect_backoff = min(reconnect_backoff * 2.0, args.reconnect_max_s)
        finally:
            with contextlib.suppress(Exception):
                await exchange.close()
            await put_event(queue, exchange_id, "connection_closed")


def parse_exchange_list(exchange_arg: str | None) -> list[str]:
    all_exchanges = list(getattr(ccxtpro, "exchanges", []))
    if not exchange_arg:
        return all_exchanges
    selected = [item.strip().lower() for item in exchange_arg.split(",") if item.strip()]
    filtered = [ex for ex in selected if ex in all_exchanges]
    return list(dict.fromkeys(filtered))


def parse_explicit_exchange_list(exchange_arg: str | None) -> list[str]:
    all_exchanges = list(getattr(ccxtpro, "exchanges", []))
    if not exchange_arg:
        return []
    selected = [item.strip().lower() for item in exchange_arg.split(",") if item.strip()]
    filtered = [ex for ex in selected if ex in all_exchanges]
    return list(dict.fromkeys(filtered))


def resolve_excluded_exchanges(explicit_exclude_arg: str | None) -> set[str]:
    explicit_excluded = set(parse_explicit_exchange_list(explicit_exclude_arg))
    return set(DEFAULT_EXCLUDED_EXCHANGES) | explicit_excluded


def choose_stream_mode(watch_tickers_supported: bool, watch_ticker_supported: bool) -> str:
    if watch_tickers_supported:
        return "watch_tickers"
    if watch_ticker_supported:
        return "watch_ticker"
    return "unsupported"


async def probe_exchange_capability(exchange_id: str, args: argparse.Namespace) -> dict[str, Any]:
    exchange_cls = getattr(ccxtpro, exchange_id, None)
    if exchange_cls is None:
        return {
            "exchange_id": exchange_id,
            "stream_mode": "unavailable",
            "watch_tickers": False,
            "watch_ticker": False,
            "symbol_count": 0,
            "symbols": [],
            "error": "exchange class not found in ccxt.pro",
        }

    exchange = exchange_cls({"enableRateLimit": True, "newUpdates": True})
    try:
        markets = await exchange.load_markets()
        symbols = select_symbols(markets, args.only_spot, args.max_symbols_per_exchange)

        watch_tickers_supported = supports_ws_flag(exchange.has.get("watchTickers"))
        watch_ticker_supported = supports_ws_flag(exchange.has.get("watchTicker"))
        stream_mode = choose_stream_mode(watch_tickers_supported, watch_ticker_supported)

        listed_symbols = symbols
        if args.list_symbol_limit is not None and args.list_symbol_limit >= 0:
            listed_symbols = symbols[: args.list_symbol_limit]

        return {
            "exchange_id": exchange_id,
            "stream_mode": stream_mode,
            "watch_tickers": watch_tickers_supported,
            "watch_ticker": watch_ticker_supported,
            "symbol_count": len(symbols),
            "symbols": listed_symbols,
            "symbols_truncated": len(listed_symbols) < len(symbols),
            "error": None,
        }
    except Exception as exc:  # noqa: BLE001
        if is_terminal_exclusion_error(exc):
            reason = terminal_error_reason(exc)
            return {
                "exchange_id": exchange_id,
                "stream_mode": "excluded_terminal",
                "watch_tickers": False,
                "watch_ticker": False,
                "symbol_count": 0,
                "symbols": [],
                "error": f"{reason}: {repr(exc)}",
            }
        return {
            "exchange_id": exchange_id,
            "stream_mode": "error",
            "watch_tickers": False,
            "watch_ticker": False,
            "symbol_count": 0,
            "symbols": [],
            "error": repr(exc),
        }
    finally:
        with contextlib.suppress(Exception):
            await exchange.close()


def flatten_capability_rows(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for item in results:
        symbols = item.get("symbols") or []
        status = "ok" if item.get("error") is None else "error"
        base_row = {
            "exchange_id": item.get("exchange_id"),
            "stream_mode": item.get("stream_mode"),
            "watch_tickers": item.get("watch_tickers"),
            "watch_ticker": item.get("watch_ticker"),
            "symbol_count": item.get("symbol_count"),
            "status": status,
            "error": item.get("error") or "",
        }
        if symbols:
            for symbol in symbols:
                row = dict(base_row)
                row["symbol"] = symbol
                rows.append(row)
        else:
            row = dict(base_row)
            row["symbol"] = ""
            rows.append(row)
    return rows


def write_capabilities_csv(rows: list[dict[str, Any]], output_path: Path) -> None:
    fieldnames = [
        "exchange_id",
        "symbol",
        "stream_mode",
        "watch_tickers",
        "watch_ticker",
        "symbol_count",
        "status",
        "error",
    ]
    with output_path.open("w", encoding="utf-8", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def write_capabilities_html(rows: list[dict[str, Any]], output_path: Path) -> None:
    headers = [
        "exchange_id",
        "symbol",
        "stream_mode",
        "watch_tickers",
        "watch_ticker",
        "symbol_count",
        "status",
        "error",
    ]
    th_html = "".join(f"<th>{escape(header)}</th>" for header in headers)
    tr_html = []
    for row in rows:
        tds = "".join(f"<td>{escape(str(row.get(header, '')))}</td>" for header in headers)
        tr_html.append(f"<tr>{tds}</tr>")
    generated_utc = utc_now_iso()
    html_content = (
        "<!doctype html>\n"
        "<html lang=\"de\">\n"
        "<head>\n"
        "  <meta charset=\"utf-8\">\n"
        "  <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n"
        "  <title>CCXT Exchange Symbol Capabilities</title>\n"
        "  <style>\n"
        "    body { font-family: Segoe UI, sans-serif; margin: 16px; }\n"
        "    table { border-collapse: collapse; width: 100%; }\n"
        "    th, td { border: 1px solid #ccc; padding: 6px 8px; text-align: left; font-size: 13px; }\n"
        "    th { background: #f2f2f2; position: sticky; top: 0; }\n"
        "    tr:nth-child(even) { background: #fafafa; }\n"
        "    .meta { margin-bottom: 10px; color: #444; font-size: 12px; }\n"
        "    .wrap { overflow-x: auto; }\n"
        "  </style>\n"
        "</head>\n"
        "<body>\n"
        "  <h1>CCXT Exchange Symbol Capabilities</h1>\n"
        f"  <div class=\"meta\">Generated UTC: {escape(generated_utc)} | Rows: {len(rows)}</div>\n"
        "  <div class=\"wrap\">\n"
        "    <table>\n"
        f"      <thead><tr>{th_html}</tr></thead>\n"
        f"      <tbody>{''.join(tr_html)}</tbody>\n"
        "    </table>\n"
        "  </div>\n"
        "</body>\n"
        "</html>\n"
    )
    output_path.write_text(html_content, encoding="utf-8")


def resolve_list_output_path(args: argparse.Namespace) -> Path:
    if args.list_output:
        return Path(args.list_output)
    return Path("data") / f"exchange_symbol_capabilities.{args.list_format}"


async def list_capabilities(args: argparse.Namespace, selected_exchanges: list[str]) -> None:
    semaphore = asyncio.Semaphore(max(1, args.list_concurrency))

    async def _probe(exchange_id: str) -> dict[str, Any]:
        async with semaphore:
            return await probe_exchange_capability(exchange_id, args)

    results = await asyncio.gather(*(_probe(exchange_id) for exchange_id in selected_exchanges))
    results_sorted = sorted(results, key=lambda item: item["exchange_id"])
    if args.only_watch_tickers:
        results_sorted = [item for item in results_sorted if item.get("watch_tickers") is True]
    rows = flatten_capability_rows(results_sorted)

    output_path = resolve_list_output_path(args)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if args.list_format == "csv":
        write_capabilities_csv(rows, output_path)
    else:
        write_capabilities_html(rows, output_path)

    truncated_exchanges = sum(1 for item in results_sorted if item.get("symbols_truncated"))
    print(
        f"List output written: {output_path} | rows={len(rows)} | exchanges={len(results_sorted)} | "
        f"truncated_exchanges={truncated_exchanges}"
    )


def install_signal_handlers(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _set_stop() -> None:
        if not stop_event.is_set():
            stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _set_stop)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _set_stop())


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Ingest ticker data from all ccxt.pro exchanges via WebSocket into SQLite."
    )
    parser.add_argument("--db-path", default="data/crypto_ws_ticks.db", help="SQLite DB file.")
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated list of exchange IDs. Default: all ccxt.pro exchanges.",
    )
    parser.add_argument(
        "--exchanges-exclude",
        default=None,
        help=(
            "Comma-separated list of exchange IDs to additionally exclude. "
            "Default excluded: alpaca, arkham, bequant, bitfinex, bitmex, bitopro, blockchaincom, oxfun, probit."
        ),
    )
    parser.add_argument(
        "--max-exchanges",
        type=int,
        default=None,
        help="Optional limit for number of exchanges (test mode).",
    )
    parser.add_argument(
        "--max-symbols-per-exchange",
        type=int,
        default=None,
        help="Optional limit for symbols per exchange (test mode).",
    )
    parser.add_argument(
        "--max-symbol-streams-per-exchange",
        type=int,
        default=None,
        help="Only for watch_ticker fallback: limit parallel symbol streams.",
    )
    parser.add_argument(
        "--only-spot",
        action="store_true",
        help="Ingest spot markets only.",
    )
    parser.add_argument(
        "--only-watch-tickers",
        action="store_true",
        help=(
            "Use only exchanges with watchTickers support. "
            "In list mode, only symbol rows from those exchanges are written."
        ),
    )
    parser.add_argument("--queue-size", type=int, default=200000, help="Maximum queue size.")
    parser.add_argument("--batch-size", type=int, default=500, help="SQLite insert batch size.")
    parser.add_argument("--flush-interval-s", type=float, default=1.0, help="Flush interval for SQLite writer.")
    parser.add_argument("--exchange-start-delay-s", type=float, default=0.2, help="Delay between exchange worker starts.")
    parser.add_argument("--reconnect-base-s", type=float, default=1.0, help="Initial reconnect backoff.")
    parser.add_argument("--reconnect-max-s", type=float, default=30.0, help="Maximum reconnect backoff.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level.",
    )
    parser.add_argument(
        "--list-capabilities",
        action="store_true",
        help="List exchanges and symbols this script can query, then exit.",
    )
    parser.add_argument(
        "--list-symbol-limit",
        type=int,
        default=30,
        help="Maximum number of output symbols per exchange in list mode (-1 for all).",
    )
    parser.add_argument(
        "--list-format",
        choices=["html", "csv"],
        default="html",
        help="Output format for list mode (default: html).",
    )
    parser.add_argument(
        "--list-output",
        default=None,
        help="Optional output path for list mode. Default: data/exchange_symbol_capabilities.<format>.",
    )
    parser.add_argument(
        "--list-concurrency",
        type=int,
        default=5,
        help="Concurrency while probing in list mode.",
    )
    return parser.parse_args()


async def async_main(args: argparse.Namespace) -> None:
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    requested_exchanges = parse_exchange_list(args.exchanges)
    selected_exchanges = list(requested_exchanges)
    excluded_exchanges = resolve_excluded_exchanges(args.exchanges_exclude)
    if excluded_exchanges:
        selected_exchanges = [exchange_id for exchange_id in selected_exchanges if exchange_id not in excluded_exchanges]
    excluded_from_selection = sorted(set(requested_exchanges) - set(selected_exchanges))
    if args.max_exchanges is not None and args.max_exchanges > 0:
        selected_exchanges = selected_exchanges[: args.max_exchanges]
    if not selected_exchanges:
        raise SystemExit("No valid exchanges found for ingestion.")
    if excluded_from_selection:
        LOGGER.info(
            "Exchanges excluded from selection (%s): %s",
            len(excluded_from_selection),
            ", ".join(excluded_from_selection),
        )

    if args.list_capabilities:
        await list_capabilities(args=args, selected_exchanges=selected_exchanges)
        return

    db_path = Path(args.db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    LOGGER.info("Starting ingestion for %s exchanges.", len(selected_exchanges))
    LOGGER.debug("Exchanges: %s", selected_exchanges)

    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=args.queue_size)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    writer = SQLiteWriter(db_path=db_path, batch_size=args.batch_size, flush_interval_s=args.flush_interval_s)
    writer_task = asyncio.create_task(writer.run(queue=queue, stop_event=stop_event), name="sqlite_writer")

    exchange_tasks: list[asyncio.Task[Any]] = []
    for exchange_id in selected_exchanges:
        task = asyncio.create_task(
            run_exchange(exchange_id=exchange_id, args=args, queue=queue, stop_event=stop_event),
            name=f"exchange_worker:{exchange_id}",
        )
        exchange_tasks.append(task)
        await asyncio.sleep(args.exchange_start_delay_s)

    try:
        while not stop_event.is_set():
            if exchange_tasks and all(task.done() for task in exchange_tasks):
                LOGGER.warning("All exchange workers have exited. Starting shutdown.")
                stop_event.set()
                break
            await asyncio.sleep(1.0)
    finally:
        LOGGER.info("Shutdown started, stopping exchange tasks...")
        for task in exchange_tasks:
            task.cancel()
        await asyncio.gather(*exchange_tasks, return_exceptions=True)

        LOGGER.info("Waiting for SQLite queue drain...")
        await queue.join()
        stop_event.set()
        await writer_task
        await writer.close()
        LOGGER.info("Shutdown complete.")


def main() -> None:
    args = parse_args()
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
