#!/usr/bin/env python
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
import os
import signal
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from html import escape
from pathlib import Path
from typing import Any

CCXTPRO_IMPORT_ERROR: ImportError | None = None
try:
    import ccxt.pro as ccxtpro
except ImportError as exc:
    ccxtpro = None
    CCXTPRO_IMPORT_ERROR = exc

from ingestion_common import (
    DEFAULT_EXCLUDED_EXCHANGES,
    configure_logging_with_file,
    is_terminal_exclusion_error,
    iter_tickers,
    parse_exchange_list,
    parse_symbol_filters,
    resolve_excluded_exchanges,
    select_symbols,
    supports_ws_flag,
    terminal_error_reason,
)


LOGGER = logging.getLogger("crypto_ws_ingestion")


def ensure_ccxtpro_available() -> None:
    if ccxtpro is not None:
        return
    cause = (
        f"{type(CCXTPRO_IMPORT_ERROR).__name__}: {CCXTPRO_IMPORT_ERROR}"
        if CCXTPRO_IMPORT_ERROR is not None
        else "unknown ImportError"
    )
    raise SystemExit(
        "Failed to import 'ccxt.pro'. "
        "The active interpreter may not match your conda environment or ccxt installation. "
        f"interpreter={sys.executable} | cause={cause}. "
        "Verify with: "
        f"\"{sys.executable}\" -c \"import ccxt, ccxt.pro; print(ccxt.__version__)\" "
        "and reinstall if needed: "
        f"\"{sys.executable}\" -m pip install -U --force-reinstall ccxt"
    ) from CCXTPRO_IMPORT_ERROR


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def ms_to_utc_iso(ms: Any) -> str | None:
    if not isinstance(ms, (int, float)):
        return None
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def json_dumps_safe(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, default=str, separators=(",", ":"))


def parse_iso_to_epoch_s(value: str) -> int:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    dt = datetime.fromisoformat(text)
    return int(dt.timestamp())


def normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def normalize_text_lower(value: Any) -> str:
    return normalize_text(value).lower()


def symbol_asset(symbol: str) -> str:
    if "/" in symbol:
        return symbol.split("/", 1)[0]
    return symbol


def partition_window_from_epoch_s(epoch_s: int) -> tuple[str, str, str]:
    dt = datetime.fromtimestamp(epoch_s, tz=timezone.utc)
    date_part = dt.strftime("%Y-%m-%d")
    hour_part = dt.strftime("%H")
    partition_key = dt.strftime("%Y%m%d_%H")
    return date_part, hour_part, partition_key


def ensure_manifest_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS partition_registry (
            partition_id TEXT PRIMARY KEY,
            layer TEXT NOT NULL,
            db_path TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT NOT NULL,
            ts_min_epoch_s INTEGER NOT NULL,
            ts_max_epoch_s INTEGER NOT NULL,
            row_count_market INTEGER NOT NULL,
            row_count_events INTEGER NOT NULL,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_partition_registry_layer_exchange_time
            ON partition_registry(layer, exchange_id_norm, ts_min_epoch_s, ts_max_epoch_s);

        CREATE TABLE IF NOT EXISTS partition_pairs (
            partition_id TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            symbol_norm TEXT NOT NULL,
            asset_norm TEXT NOT NULL,
            ts_min_epoch_s INTEGER NOT NULL,
            ts_max_epoch_s INTEGER NOT NULL,
            row_count_market INTEGER NOT NULL,
            created_utc TEXT NOT NULL,
            updated_utc TEXT NOT NULL,
            PRIMARY KEY (partition_id, exchange_id_norm, symbol_norm)
        );

        CREATE INDEX IF NOT EXISTS idx_partition_pairs_lookup
            ON partition_pairs(exchange_id_norm, symbol_norm, ts_min_epoch_s, ts_max_epoch_s);
        """
    )
    connection.commit()


def ensure_partition_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS market_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingestion_ts_utc TEXT NOT NULL,
            ingestion_ts_epoch_s INTEGER NOT NULL,
            exchange_id TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            symbol TEXT NOT NULL,
            symbol_norm TEXT NOT NULL,
            asset_norm TEXT NOT NULL,
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

        CREATE INDEX IF NOT EXISTS idx_market_ticks_pair_ts
            ON market_ticks(exchange_id_norm, symbol_norm, ingestion_ts_epoch_s);

        CREATE INDEX IF NOT EXISTS idx_market_ticks_ts
            ON market_ticks(ingestion_ts_epoch_s);

        CREATE TABLE IF NOT EXISTS connection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts_utc TEXT NOT NULL,
            event_ts_epoch_s INTEGER NOT NULL,
            exchange_id TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            symbol TEXT,
            symbol_norm TEXT,
            asset_norm TEXT,
            event_type TEXT NOT NULL,
            details_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_connection_events_ex_ts
            ON connection_events(exchange_id_norm, event_ts_epoch_s);
        """
    )
    connection.commit()


class VaultPartitionWriter:
    def __init__(self, vault_root: Path, layer: str, batch_size: int, flush_interval_s: float) -> None:
        self.vault_root = vault_root
        self.layer = layer
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s
        self._partition_conns: dict[str, sqlite3.Connection] = {}
        self._partition_paths: dict[str, Path] = {}
        self._manifest_db_path = self.vault_root / "meta" / "vault_manifest.db"
        self._manifest_db_path.parent.mkdir(parents=True, exist_ok=True)
        self._manifest_conn = sqlite3.connect(str(self._manifest_db_path), check_same_thread=False)
        self._manifest_conn.execute("PRAGMA journal_mode=WAL;")
        self._manifest_conn.execute("PRAGMA synchronous=NORMAL;")
        ensure_manifest_schema(self._manifest_conn)

    def _resolve_partition(self, exchange_id_norm: str, epoch_s: int) -> tuple[str, Path, str, str]:
        date_part, hour_part, partition_key = partition_window_from_epoch_s(epoch_s)
        partition_id = f"{self.layer}:{exchange_id_norm}:{partition_key}"
        db_path = (
            self.vault_root
            / self.layer
            / f"exchange={exchange_id_norm}"
            / f"date={date_part}"
            / f"hour={hour_part}"
            / f"part_{exchange_id_norm}_{partition_key}.db"
        )
        return partition_id, db_path, date_part, hour_part

    def _connection_for_partition(self, partition_id: str, db_path: Path) -> sqlite3.Connection:
        conn = self._partition_conns.get(partition_id)
        if conn is not None:
            return conn
        db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(db_path), check_same_thread=False)
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA temp_store=MEMORY;")
        ensure_partition_schema(conn)
        self._partition_conns[partition_id] = conn
        self._partition_paths[partition_id] = db_path
        return conn

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
        partition_market_rows: dict[str, list[tuple[Any, ...]]] = {}
        partition_event_rows: dict[str, list[tuple[Any, ...]]] = {}
        partition_stats: dict[str, dict[str, Any]] = {}
        pair_stats: dict[tuple[str, str, str], dict[str, Any]] = {}

        for kind, payload in items:
            exchange_id = normalize_text(payload.get("exchange_id"))
            exchange_id_norm = normalize_text_lower(exchange_id)
            if not exchange_id_norm:
                continue

            if kind == "tick":
                ingestion_ts_utc = normalize_text(payload.get("ingestion_ts_utc"))
                if not ingestion_ts_utc:
                    continue
                epoch_s = parse_iso_to_epoch_s(ingestion_ts_utc)
                symbol = normalize_text(payload.get("symbol"))
                symbol_norm = normalize_text_lower(symbol)
                if not symbol_norm:
                    continue
                asset_norm = normalize_text_lower(symbol_asset(symbol))
                partition_id, partition_path, date_part, hour_part = self._resolve_partition(exchange_id_norm, epoch_s)
                rows = partition_market_rows.setdefault(partition_id, [])
                rows.append(
                    (
                        ingestion_ts_utc,
                        int(epoch_s),
                        exchange_id,
                        exchange_id_norm,
                        symbol,
                        symbol_norm,
                        asset_norm,
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
                        payload.get("raw_json"),
                    )
                )
                stats = partition_stats.setdefault(
                    partition_id,
                    {
                        "partition_path": partition_path,
                        "exchange_id_norm": exchange_id_norm,
                        "date_part": date_part,
                        "hour_part": hour_part,
                        "min_epoch": epoch_s,
                        "max_epoch": epoch_s,
                        "market_count": 0,
                        "event_count": 0,
                    },
                )
                stats["min_epoch"] = min(int(stats["min_epoch"]), epoch_s)
                stats["max_epoch"] = max(int(stats["max_epoch"]), epoch_s)
                stats["market_count"] = int(stats["market_count"]) + 1

                pair_key = (partition_id, exchange_id_norm, symbol_norm)
                pair = pair_stats.setdefault(
                    pair_key,
                    {
                        "asset_norm": asset_norm,
                        "min_epoch": epoch_s,
                        "max_epoch": epoch_s,
                        "count": 0,
                    },
                )
                pair["min_epoch"] = min(int(pair["min_epoch"]), epoch_s)
                pair["max_epoch"] = max(int(pair["max_epoch"]), epoch_s)
                pair["count"] = int(pair["count"]) + 1
            elif kind == "event":
                event_ts_utc = normalize_text(payload.get("event_ts_utc"))
                if not event_ts_utc:
                    continue
                epoch_s = parse_iso_to_epoch_s(event_ts_utc)
                symbol = normalize_text(payload.get("symbol"))
                symbol_norm = normalize_text_lower(symbol) if symbol else None
                asset_norm = normalize_text_lower(symbol_asset(symbol)) if symbol else None
                partition_id, partition_path, date_part, hour_part = self._resolve_partition(exchange_id_norm, epoch_s)
                rows = partition_event_rows.setdefault(partition_id, [])
                rows.append(
                    (
                        event_ts_utc,
                        int(epoch_s),
                        exchange_id,
                        exchange_id_norm,
                        symbol if symbol else None,
                        symbol_norm,
                        asset_norm,
                        normalize_text(payload.get("event_type")),
                        payload.get("details_json"),
                    )
                )
                stats = partition_stats.setdefault(
                    partition_id,
                    {
                        "partition_path": partition_path,
                        "exchange_id_norm": exchange_id_norm,
                        "date_part": date_part,
                        "hour_part": hour_part,
                        "min_epoch": epoch_s,
                        "max_epoch": epoch_s,
                        "market_count": 0,
                        "event_count": 0,
                    },
                )
                stats["min_epoch"] = min(int(stats["min_epoch"]), epoch_s)
                stats["max_epoch"] = max(int(stats["max_epoch"]), epoch_s)
                stats["event_count"] = int(stats["event_count"]) + 1

        for partition_id in sorted(set(partition_market_rows) | set(partition_event_rows)):
            stats = partition_stats.get(partition_id)
            if stats is None:
                continue
            db_path = Path(stats["partition_path"])
            connection = self._connection_for_partition(partition_id, db_path)
            if partition_id in partition_market_rows:
                connection.executemany(
                    """
                    INSERT INTO market_ticks (
                        ingestion_ts_utc,
                        ingestion_ts_epoch_s,
                        exchange_id,
                        exchange_id_norm,
                        symbol,
                        symbol_norm,
                        asset_norm,
                        market_type,
                        price,
                        bid,
                        ask,
                        last,
                        open,
                        high,
                        low,
                        base_volume,
                        quote_volume,
                        exchange_ts_ms,
                        exchange_ts_utc,
                        raw_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    partition_market_rows[partition_id],
                )
            if partition_id in partition_event_rows:
                connection.executemany(
                    """
                    INSERT INTO connection_events (
                        event_ts_utc,
                        event_ts_epoch_s,
                        exchange_id,
                        exchange_id_norm,
                        symbol,
                        symbol_norm,
                        asset_norm,
                        event_type,
                        details_json
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    partition_event_rows[partition_id],
                )
            connection.commit()

        now_utc = utc_now_iso()
        for partition_id, stats in partition_stats.items():
            partition_epoch = int(stats["min_epoch"])
            date_part, hour_part, _ = partition_window_from_epoch_s(partition_epoch)
            window_start_utc = f"{date_part}T{hour_part}:00:00+00:00"
            window_end_dt = datetime.fromisoformat(window_start_utc) + timedelta(hours=1)
            window_end_utc = window_end_dt.isoformat(timespec="seconds")
            self._manifest_conn.execute(
                """
                INSERT INTO partition_registry (
                    partition_id,
                    layer,
                    db_path,
                    exchange_id_norm,
                    window_start_utc,
                    window_end_utc,
                    ts_min_epoch_s,
                    ts_max_epoch_s,
                    row_count_market,
                    row_count_events,
                    created_utc,
                    updated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(partition_id) DO UPDATE SET
                    db_path=excluded.db_path,
                    ts_min_epoch_s=MIN(partition_registry.ts_min_epoch_s, excluded.ts_min_epoch_s),
                    ts_max_epoch_s=MAX(partition_registry.ts_max_epoch_s, excluded.ts_max_epoch_s),
                    row_count_market=partition_registry.row_count_market + excluded.row_count_market,
                    row_count_events=partition_registry.row_count_events + excluded.row_count_events,
                    updated_utc=excluded.updated_utc
                """,
                (
                    partition_id,
                    self.layer,
                    str(Path(stats["partition_path"]).resolve()),
                    str(stats["exchange_id_norm"]),
                    window_start_utc,
                    window_end_utc,
                    int(stats["min_epoch"]),
                    int(stats["max_epoch"]),
                    int(stats["market_count"]),
                    int(stats["event_count"]),
                    now_utc,
                    now_utc,
                ),
            )

        for (partition_id, exchange_id_norm, symbol_norm), pair in pair_stats.items():
            self._manifest_conn.execute(
                """
                INSERT INTO partition_pairs (
                    partition_id,
                    exchange_id_norm,
                    symbol_norm,
                    asset_norm,
                    ts_min_epoch_s,
                    ts_max_epoch_s,
                    row_count_market,
                    created_utc,
                    updated_utc
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(partition_id, exchange_id_norm, symbol_norm) DO UPDATE SET
                    ts_min_epoch_s=MIN(partition_pairs.ts_min_epoch_s, excluded.ts_min_epoch_s),
                    ts_max_epoch_s=MAX(partition_pairs.ts_max_epoch_s, excluded.ts_max_epoch_s),
                    row_count_market=partition_pairs.row_count_market + excluded.row_count_market,
                    updated_utc=excluded.updated_utc
                """,
                (
                    partition_id,
                    exchange_id_norm,
                    symbol_norm,
                    str(pair["asset_norm"]),
                    int(pair["min_epoch"]),
                    int(pair["max_epoch"]),
                    int(pair["count"]),
                    now_utc,
                    now_utc,
                ),
            )
        self._manifest_conn.commit()

    async def close(self) -> None:
        for connection in self._partition_conns.values():
            connection.close()
        self._partition_conns.clear()
        self._manifest_conn.close()


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
    selected_price = ticker.get("last") if ticker.get("last") is not None else ticker.get("close")
    payload = {
        "ingestion_ts_utc": utc_now_iso(),
        "exchange_id": exchange_id,
        "symbol": ticker.get("symbol", "UNKNOWN"),
        "market_type": ticker.get("type"),
        "price": selected_price,
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
    if LOGGER.isEnabledFor(logging.DEBUG):
        LOGGER.debug(
            "Tick received | ingestion_ts_utc=%s | exchange_id=%s | symbol=%s | price=%s",
            payload["ingestion_ts_utc"],
            payload["exchange_id"],
            payload["symbol"],
            payload["price"],
        )
    await queue.put(("tick", payload))


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
    symbol_filters: list[str],
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
            symbols = select_symbols(
                markets,
                args.only_spot,
                args.max_symbols_per_exchange,
                symbol_filters,
            )
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


def choose_stream_mode(watch_tickers_supported: bool, watch_ticker_supported: bool) -> str:
    if watch_tickers_supported:
        return "watch_tickers"
    if watch_ticker_supported:
        return "watch_ticker"
    return "unsupported"


async def probe_exchange_capability(
    exchange_id: str,
    args: argparse.Namespace,
    symbol_filters: list[str],
) -> dict[str, Any]:
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
        symbols = select_symbols(
            markets,
            args.only_spot,
            args.max_symbols_per_exchange,
            symbol_filters,
        )

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


async def list_capabilities(
    args: argparse.Namespace,
    selected_exchanges: list[str],
    symbol_filters: list[str],
) -> None:
    semaphore = asyncio.Semaphore(max(1, args.list_concurrency))

    async def _probe(exchange_id: str) -> dict[str, Any]:
        async with semaphore:
            return await probe_exchange_capability(exchange_id, args, symbol_filters)

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
    default_excluded_csv = ", ".join(sorted(DEFAULT_EXCLUDED_EXCHANGES))
    parser = argparse.ArgumentParser(
        description="Ingest ticker data from all ccxt.pro exchanges into VAULT 2.0 partitioned SQLite storage.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--vault-root",
        default="data/vault2",
        help="VAULT 2.0 root directory. Partition files and manifest DB are created below this path.",
    )
    parser.add_argument(
        "--vault-layer",
        default="ingestion",
        help="VAULT layer name used for partition registry entries.",
    )
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
            f"Default excluded: {default_excluded_csv}."
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
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help=(
            "Optional comma-separated symbol filter. "
            "Supports exact symbols and SQL LIKE patterns (%% and _), case-insensitive "
            "(for example BTC/USDT,eth/usdt,%%btc/%%)."
        ),
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
        "--log-file",
        default=None,
        help="Optional log file path. Default: <vault-root>/logs/ingest_all_exchanges_ws_<pid>.log.",
    )
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
    ensure_ccxtpro_available()
    vault_root = Path(args.vault_root)
    default_log_path = vault_root / "logs" / f"ingest_all_exchanges_ws_{os.getpid()}.log"
    log_file_path = Path(args.log_file) if args.log_file else default_log_path
    configure_logging_with_file(args.log_level, log_file_path)
    LOGGER.info("File logging enabled: %s", log_file_path)

    available_exchanges = list(getattr(ccxtpro, "exchanges", []))
    requested_exchanges = parse_exchange_list(args.exchanges, available_exchanges)
    selected_exchanges = list(requested_exchanges)
    excluded_exchanges = resolve_excluded_exchanges(
        args.exchanges_exclude,
        available_exchanges,
        DEFAULT_EXCLUDED_EXCHANGES,
    )
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
    symbol_filters = parse_symbol_filters(args.symbols)
    if symbol_filters:
        LOGGER.info("Symbol filters enabled (%s): %s", len(symbol_filters), ", ".join(symbol_filters))

    if args.list_capabilities:
        await list_capabilities(
            args=args,
            selected_exchanges=selected_exchanges,
            symbol_filters=symbol_filters,
        )
        return

    vault_root.mkdir(parents=True, exist_ok=True)
    LOGGER.info("VAULT root: %s", vault_root)
    LOGGER.info("VAULT layer: %s", args.vault_layer)

    LOGGER.info("Starting ingestion for %s exchanges.", len(selected_exchanges))
    LOGGER.debug("Exchanges: %s", selected_exchanges)

    queue: asyncio.Queue[tuple[str, dict[str, Any]]] = asyncio.Queue(maxsize=args.queue_size)
    stop_event = asyncio.Event()
    install_signal_handlers(stop_event)

    writer = VaultPartitionWriter(
        vault_root=vault_root,
        layer=args.vault_layer,
        batch_size=args.batch_size,
        flush_interval_s=args.flush_interval_s,
    )
    writer_task = asyncio.create_task(writer.run(queue=queue, stop_event=stop_event), name="sqlite_writer")

    exchange_tasks: list[asyncio.Task[Any]] = []
    for exchange_id in selected_exchanges:
        task = asyncio.create_task(
            run_exchange(
                exchange_id=exchange_id,
                args=args,
                symbol_filters=symbol_filters,
                queue=queue,
                stop_event=stop_event,
            ),
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
