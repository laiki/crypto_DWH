#!/usr/bin/env python3
"""
Export a configurable time window from worker SQLite databases into a staging export.

Supported output formats:
- sqlite (default): one SQLite file with both tables.
- csv: one CSV per table.
- json: one JSON array file per table.
"""

from __future__ import annotations

import argparse
import csv
import glob
import hashlib
import json
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any


MARKET_TICK_COLUMNS = [
    "ingestion_ts_utc",
    "exchange_id",
    "symbol",
    "market_type",
    "price",
    "bid",
    "ask",
    "last",
    "open",
    "high",
    "low",
    "base_volume",
    "quote_volume",
    "exchange_ts_ms",
    "exchange_ts_utc",
    "raw_json",
]

CONNECTION_EVENT_COLUMNS = [
    "event_ts_utc",
    "exchange_id",
    "symbol",
    "event_type",
    "details_json",
]

STAGING_METADATA_CONTRACT_NAME = "staging_export_run_metadata"
STAGING_METADATA_CONTRACT_VERSION = "1.0.0"
STAGING_STATE_CONTRACT_NAME = "staging_export_state"
STAGING_STATE_CONTRACT_VERSION = "1.0.0"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Export the last N hours from worker SQLite databases into staging output "
            "(sqlite/csv/json), with optional exchange and asset filters."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--hours",
        type=float,
        required=True,
        help="Export window in hours (e.g. 24, 12, 1.5).",
    )
    parser.add_argument(
        "--input-glob",
        default="data/worker_*_crypto_ws_ticks.db",
        help="Glob pattern for worker DB files.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/staging",
        help="Output directory for staging exports.",
    )
    parser.add_argument(
        "--output-prefix",
        default="staging_export",
        help="Output filename prefix.",
    )
    parser.add_argument(
        "--output-format",
        choices=["sqlite", "csv", "json"],
        default="sqlite",
        help="Export format.",
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Optional comma-separated exchange IDs filter (e.g. binance,kraken).",
    )
    parser.add_argument(
        "--assets",
        default=None,
        help="Optional comma-separated base assets filter (e.g. BTC,ETH,SOL).",
    )
    parser.add_argument(
        "--skip-connection-events",
        action="store_true",
        help="Skip exporting connection_events.",
    )
    parser.add_argument(
        "--incremental",
        action="store_true",
        help="Enable incremental export mode based on persisted per-table watermark state.",
    )
    parser.add_argument(
        "--state-path",
        default="data/staging/staging_export_state.json",
        help="Path to incremental watermark state JSON file.",
    )
    parser.add_argument(
        "--state-key",
        default=None,
        help="Optional custom key for state profile. Default: hash derived from source glob and filters.",
    )
    parser.add_argument(
        "--update-state",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Persist updated watermark state after successful incremental export.",
    )
    parser.add_argument(
        "--list-only",
        action="store_true",
        help="Only print unique exchange/asset lists and exit.",
    )
    parser.add_argument(
        "--print-unique-exchanges",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print unique exchange list from selected worker DBs and time window.",
    )
    parser.add_argument(
        "--print-unique-assets",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print unique asset list from selected worker DBs and time window.",
    )
    parser.add_argument(
        "--chunk-size",
        type=int,
        default=5000,
        help="Fetch chunk size for CSV/JSON streaming export.",
    )
    return parser.parse_args()


def normalize_csv_values(raw: str | None, *, to_lower: bool = False, to_upper: bool = False) -> list[str]:
    if raw is None:
        return []
    values = [item.strip() for item in raw.split(",")]
    values = [item for item in values if item]
    if to_lower:
        values = [item.lower() for item in values]
    if to_upper:
        values = [item.upper() for item in values]
    # Keep deterministic order while preserving first occurrence.
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def worker_db_paths(input_glob: str) -> list[Path]:
    matches = sorted(Path(path) for path in glob.glob(input_glob))
    return [path for path in matches if path.is_file() and path.suffix.lower() == ".db"]


def hours_label(hours: float) -> str:
    if float(hours).is_integer():
        return str(int(hours))
    text = f"{hours:.3f}".rstrip("0").rstrip(".")
    return text.replace(".", "p")


def utc_iso(timestamp: datetime) -> str:
    return timestamp.astimezone(timezone.utc).isoformat(timespec="milliseconds")

def output_base_name(output_prefix: str, hours: float, run_started_utc: datetime) -> str:
    now_utc = run_started_utc.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return f"{output_prefix}_{now_utc}_last_{hours_label(hours)}h"


def window_start_iso(window_end_utc: datetime, hours: float) -> str:
    timestamp = window_end_utc - timedelta(hours=hours)
    return utc_iso(timestamp)


def sqlite_asset_expr(column_name: str) -> str:
    return (
        "upper("
        f"CASE WHEN instr({column_name}, '/') > 0 "
        f"THEN substr({column_name}, 1, instr({column_name}, '/') - 1) "
        f"ELSE {column_name} END"
        ")"
    )


def max_iso_value(left: str | None, right: str | None) -> str | None:
    if left is None:
        return right
    if right is None:
        return left
    return max(left, right)


def build_market_where_clause(
    lower_bound_utc: str | None,
    upper_bound_utc: str | None,
    lower_inclusive: bool,
    exchanges: list[str],
    assets: list[str],
) -> tuple[str, list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    if lower_bound_utc is not None:
        lower_op = ">=" if lower_inclusive else ">"
        conditions.append(f"ingestion_ts_utc {lower_op} ?")
        params.append(lower_bound_utc)
    if upper_bound_utc is not None:
        conditions.append("ingestion_ts_utc <= ?")
        params.append(upper_bound_utc)
    if exchanges:
        placeholders = ",".join("?" for _ in exchanges)
        conditions.append(f"lower(exchange_id) IN ({placeholders})")
        params.extend(exchanges)
    if assets:
        placeholders = ",".join("?" for _ in assets)
        conditions.append(f"{sqlite_asset_expr('symbol')} IN ({placeholders})")
        params.extend(assets)
    if not conditions:
        conditions.append("1=1")
    return " AND ".join(conditions), params


def build_event_where_clause(
    lower_bound_utc: str | None,
    upper_bound_utc: str | None,
    lower_inclusive: bool,
    exchanges: list[str],
    assets: list[str],
) -> tuple[str, list[Any]]:
    conditions: list[str] = []
    params: list[Any] = []
    if lower_bound_utc is not None:
        lower_op = ">=" if lower_inclusive else ">"
        conditions.append(f"event_ts_utc {lower_op} ?")
        params.append(lower_bound_utc)
    if upper_bound_utc is not None:
        conditions.append("event_ts_utc <= ?")
        params.append(upper_bound_utc)
    if exchanges:
        placeholders = ",".join("?" for _ in exchanges)
        conditions.append(f"lower(exchange_id) IN ({placeholders})")
        params.extend(exchanges)
    if assets:
        placeholders = ",".join("?" for _ in assets)
        conditions.append(f"{sqlite_asset_expr('symbol')} IN ({placeholders})")
        params.extend(assets)
    if not conditions:
        conditions.append("1=1")
    return " AND ".join(conditions), params


def state_profile_key(
    custom_key: str | None,
    input_glob: str,
    exchanges_filter: list[str],
    assets_filter: list[str],
) -> str:
    if custom_key is not None and custom_key.strip():
        return custom_key.strip()

    descriptor = {
        "input_glob": input_glob,
        "exchanges": exchanges_filter,
        "assets": assets_filter,
    }
    digest = hashlib.sha256(
        json.dumps(descriptor, ensure_ascii=True, separators=(",", ":"), sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return f"default_{digest}"


def load_state_document(state_path: Path) -> dict[str, Any]:
    if not state_path.exists():
        return {
            "contract_name": STAGING_STATE_CONTRACT_NAME,
            "contract_version": STAGING_STATE_CONTRACT_VERSION,
            "profiles": {},
        }
    try:
        raw = json.loads(state_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        raise SystemExit(f"Failed to read state file: {state_path} | error={exc!r}") from exc

    if not isinstance(raw, dict):
        raise SystemExit(f"Invalid state file format: {state_path} (expected JSON object).")

    profiles = raw.get("profiles")
    if not isinstance(profiles, dict):
        raw["profiles"] = {}

    raw["contract_name"] = STAGING_STATE_CONTRACT_NAME
    raw["contract_version"] = STAGING_STATE_CONTRACT_VERSION
    return raw


def write_state_document(state_path: Path, payload: dict[str, Any]) -> None:
    state_path.parent.mkdir(parents=True, exist_ok=True)
    state_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def table_exists(connection: sqlite3.Connection, table_name: str) -> bool:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1",
        (table_name,),
    ).fetchone()
    return row is not None


def source_asset(symbol: Any) -> str | None:
    if not isinstance(symbol, str):
        return None
    value = symbol.strip()
    if not value:
        return None
    if "/" in value:
        return value.split("/", 1)[0].upper()
    return value.upper()


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)


def collect_source_uniques(db_paths: list[Path], market_where: str, market_params: list[Any]) -> tuple[list[str], list[str]]:
    unique_exchanges: set[str] = set()
    unique_assets: set[str] = set()

    for db_path in db_paths:
        connection = connect_readonly(db_path)
        try:
            if not table_exists(connection, "market_ticks"):
                continue

            exchange_query = f"SELECT DISTINCT exchange_id FROM market_ticks WHERE {market_where}"
            for (exchange_id,) in connection.execute(exchange_query, market_params):
                if isinstance(exchange_id, str) and exchange_id.strip():
                    unique_exchanges.add(exchange_id.strip())

            symbol_query = f"SELECT DISTINCT symbol FROM market_ticks WHERE {market_where}"
            for (symbol,) in connection.execute(symbol_query, market_params):
                asset = source_asset(symbol)
                if asset:
                    unique_assets.add(asset)
        finally:
            connection.close()

    return sorted(unique_exchanges), sorted(unique_assets)


def ensure_sqlite_output_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS market_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db TEXT NOT NULL,
            source_row_id INTEGER NOT NULL,
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

        CREATE INDEX IF NOT EXISTS idx_market_ticks_time_ex_symbol
            ON market_ticks(ingestion_ts_utc, exchange_id, symbol);

        CREATE INDEX IF NOT EXISTS idx_market_ticks_source
            ON market_ticks(source_db, source_row_id);

        CREATE TABLE IF NOT EXISTS connection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_db TEXT NOT NULL,
            source_row_id INTEGER NOT NULL,
            event_ts_utc TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT,
            event_type TEXT NOT NULL,
            details_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_connection_events_time_ex
            ON connection_events(event_ts_utc, exchange_id);

        CREATE INDEX IF NOT EXISTS idx_connection_events_source
            ON connection_events(source_db, source_row_id);
        """
    )
    connection.commit()


def insert_sqlite_from_sources(
    db_paths: list[Path],
    output_db_path: Path,
    market_where: str,
    market_params: list[Any],
    event_where: str,
    event_params: list[Any],
    include_connection_events: bool,
) -> tuple[int, int]:
    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(str(output_db_path))
    total_market_rows = 0
    total_event_rows = 0
    try:
        ensure_sqlite_output_schema(connection)

        for index, db_path in enumerate(db_paths):
            alias = f"src{index}"
            connection.execute(f"ATTACH DATABASE ? AS {alias}", (str(db_path),))
            try:
                has_market_ticks = connection.execute(
                    f"SELECT 1 FROM {alias}.sqlite_master WHERE type='table' AND name='market_ticks' LIMIT 1"
                ).fetchone() is not None
                has_connection_events = connection.execute(
                    f"SELECT 1 FROM {alias}.sqlite_master WHERE type='table' AND name='connection_events' LIMIT 1"
                ).fetchone() is not None

                if has_market_ticks:
                    market_insert_sql = (
                        "INSERT INTO market_ticks ("
                        "source_db, source_row_id, ingestion_ts_utc, exchange_id, symbol, market_type, "
                        "price, bid, ask, last, open, high, low, base_volume, quote_volume, "
                        "exchange_ts_ms, exchange_ts_utc, raw_json"
                        ") "
                        f"SELECT ?, id, {', '.join(MARKET_TICK_COLUMNS)} "
                        f"FROM {alias}.market_ticks "
                        f"WHERE {market_where}"
                    )
                    connection.execute(market_insert_sql, [str(db_path)] + market_params)
                    inserted_market = connection.execute("SELECT changes()").fetchone()[0]
                    total_market_rows += int(inserted_market)

                if include_connection_events and has_connection_events:
                    event_insert_sql = (
                        "INSERT INTO connection_events ("
                        "source_db, source_row_id, event_ts_utc, exchange_id, symbol, event_type, details_json"
                        ") "
                        f"SELECT ?, id, {', '.join(CONNECTION_EVENT_COLUMNS)} "
                        f"FROM {alias}.connection_events "
                        f"WHERE {event_where}"
                    )
                    connection.execute(event_insert_sql, [str(db_path)] + event_params)
                    inserted_events = connection.execute("SELECT changes()").fetchone()[0]
                    total_event_rows += int(inserted_events)

                connection.commit()
            finally:
                connection.execute(f"DETACH DATABASE {alias}")
    finally:
        connection.close()

    return total_market_rows, total_event_rows


def stream_market_rows(
    db_path: Path,
    market_where: str,
    market_params: list[Any],
    chunk_size: int,
) -> Any:
    connection = connect_readonly(db_path)
    try:
        if not table_exists(connection, "market_ticks"):
            return
        sql = (
            "SELECT id, "
            + ", ".join(MARKET_TICK_COLUMNS)
            + f" FROM market_ticks WHERE {market_where}"
        )
        cursor = connection.execute(sql, market_params)
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                source_row_id = row[0]
                payload = dict(zip(MARKET_TICK_COLUMNS, row[1:]))
                payload["source_db"] = str(db_path)
                payload["source_row_id"] = source_row_id
                yield payload
    finally:
        connection.close()


def stream_event_rows(
    db_path: Path,
    event_where: str,
    event_params: list[Any],
    chunk_size: int,
) -> Any:
    connection = connect_readonly(db_path)
    try:
        if not table_exists(connection, "connection_events"):
            return
        sql = (
            "SELECT id, "
            + ", ".join(CONNECTION_EVENT_COLUMNS)
            + f" FROM connection_events WHERE {event_where}"
        )
        cursor = connection.execute(sql, event_params)
        while True:
            rows = cursor.fetchmany(chunk_size)
            if not rows:
                break
            for row in rows:
                source_row_id = row[0]
                payload = dict(zip(CONNECTION_EVENT_COLUMNS, row[1:]))
                payload["source_db"] = str(db_path)
                payload["source_row_id"] = source_row_id
                yield payload
    finally:
        connection.close()


class JsonArrayWriter:
    def __init__(self, output_path: Path) -> None:
        self.output_path = output_path
        self._file = output_path.open("w", encoding="utf-8")
        self._first = True
        self._file.write("[\n")

    def write(self, payload: dict[str, Any]) -> None:
        if not self._first:
            self._file.write(",\n")
        self._file.write(json.dumps(payload, ensure_ascii=True, separators=(",", ":")))
        self._first = False

    def close(self) -> None:
        self._file.write("\n]\n")
        self._file.close()


def write_csv_or_json_exports(
    db_paths: list[Path],
    output_base: Path,
    output_format: str,
    market_where: str,
    market_params: list[Any],
    event_where: str,
    event_params: list[Any],
    include_connection_events: bool,
    chunk_size: int,
) -> tuple[int, int, list[Path], list[str], list[str], str | None, str | None]:
    output_base.parent.mkdir(parents=True, exist_ok=True)

    output_files: list[Path] = []
    exported_exchanges: set[str] = set()
    exported_assets: set[str] = set()
    market_count = 0
    event_count = 0
    max_market_ts_utc: str | None = None
    max_event_ts_utc: str | None = None

    market_fields = ["source_db", "source_row_id"] + MARKET_TICK_COLUMNS
    event_fields = ["source_db", "source_row_id"] + CONNECTION_EVENT_COLUMNS

    market_path = output_base.with_name(f"{output_base.name}_market_ticks.{output_format}")
    output_files.append(market_path)

    if output_format == "csv":
        market_writer_file = market_path.open("w", encoding="utf-8", newline="")
        market_writer = csv.DictWriter(market_writer_file, fieldnames=market_fields)
        market_writer.writeheader()
        market_writer_obj: Any = market_writer
    else:
        market_writer_file = JsonArrayWriter(market_path)
        market_writer_obj = market_writer_file

    event_writer_file: Any = None
    event_writer_obj: Any = None
    if include_connection_events:
        event_path = output_base.with_name(f"{output_base.name}_connection_events.{output_format}")
        output_files.append(event_path)
        if output_format == "csv":
            event_writer_file = event_path.open("w", encoding="utf-8", newline="")
            event_writer = csv.DictWriter(event_writer_file, fieldnames=event_fields)
            event_writer.writeheader()
            event_writer_obj = event_writer
        else:
            event_writer_file = JsonArrayWriter(event_path)
            event_writer_obj = event_writer_file

    try:
        for db_path in db_paths:
            for payload in stream_market_rows(
                db_path=db_path,
                market_where=market_where,
                market_params=market_params,
                chunk_size=chunk_size,
            ):
                if output_format == "csv":
                    market_writer_obj.writerow(payload)
                else:
                    market_writer_obj.write(payload)
                market_count += 1
                tick_ts = payload.get("ingestion_ts_utc")
                if isinstance(tick_ts, str) and tick_ts:
                    max_market_ts_utc = max_iso_value(max_market_ts_utc, tick_ts)
                exchange_id = payload.get("exchange_id")
                if isinstance(exchange_id, str) and exchange_id.strip():
                    exported_exchanges.add(exchange_id.strip())
                asset = source_asset(payload.get("symbol"))
                if asset:
                    exported_assets.add(asset)

            if not include_connection_events:
                continue

            for payload in stream_event_rows(
                db_path=db_path,
                event_where=event_where,
                event_params=event_params,
                chunk_size=chunk_size,
            ):
                if output_format == "csv":
                    event_writer_obj.writerow(payload)
                else:
                    event_writer_obj.write(payload)
                event_count += 1
                event_ts = payload.get("event_ts_utc")
                if isinstance(event_ts, str) and event_ts:
                    max_event_ts_utc = max_iso_value(max_event_ts_utc, event_ts)
                exchange_id = payload.get("exchange_id")
                if isinstance(exchange_id, str) and exchange_id.strip():
                    exported_exchanges.add(exchange_id.strip())

    finally:
        if output_format == "csv":
            market_writer_file.close()
            if event_writer_file is not None:
                event_writer_file.close()
        else:
            market_writer_file.close()
            if event_writer_file is not None:
                event_writer_file.close()

    return (
        market_count,
        event_count,
        output_files,
        sorted(exported_exchanges),
        sorted(exported_assets),
        max_market_ts_utc,
        max_event_ts_utc,
    )


def collect_exported_summary_from_sqlite(
    output_db_path: Path,
    include_connection_events: bool,
) -> tuple[list[str], list[str], str | None, str | None]:
    connection = sqlite3.connect(str(output_db_path))
    try:
        exchanges = [
            exchange_id
            for (exchange_id,) in connection.execute("SELECT DISTINCT exchange_id FROM market_ticks ORDER BY exchange_id")
        ]
        assets_set: set[str] = set()
        for (symbol,) in connection.execute("SELECT DISTINCT symbol FROM market_ticks ORDER BY symbol"):
            asset = source_asset(symbol)
            if asset:
                assets_set.add(asset)
        market_max_row = connection.execute("SELECT MAX(ingestion_ts_utc) FROM market_ticks").fetchone()
        max_market_ts_utc = market_max_row[0] if market_max_row is not None else None

        max_event_ts_utc: str | None = None
        if include_connection_events:
            event_max_row = connection.execute("SELECT MAX(event_ts_utc) FROM connection_events").fetchone()
            max_event_ts_utc = event_max_row[0] if event_max_row is not None else None

        return exchanges, sorted(assets_set), max_market_ts_utc, max_event_ts_utc
    finally:
        connection.close()


def write_metadata(metadata_path: Path, payload: dict[str, Any]) -> None:
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def print_list(title: str, values: list[str]) -> None:
    print(f"{title} ({len(values)}):")
    if not values:
        print("  -")
        return
    print("  " + ", ".join(values))


def main() -> None:
    args = parse_args()

    if args.hours <= 0:
        raise SystemExit("--hours must be > 0.")
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be > 0.")

    exchanges_filter = normalize_csv_values(args.exchanges, to_lower=True)
    assets_filter = normalize_csv_values(args.assets, to_upper=True)

    db_paths = worker_db_paths(args.input_glob)
    if not db_paths:
        raise SystemExit(f"No worker DB files found for input glob: {args.input_glob}")

    run_started_utc = datetime.now(timezone.utc)
    window_end_utc = run_started_utc
    window_end_utc_iso = utc_iso(window_end_utc)
    window_start_default_utc = window_start_iso(window_end_utc, args.hours)
    incremental_enabled = bool(args.incremental)
    include_events = not args.skip_connection_events
    state_path = Path(args.state_path)
    state_key = state_profile_key(
        custom_key=args.state_key,
        input_glob=args.input_glob,
        exchanges_filter=exchanges_filter,
        assets_filter=assets_filter,
    )

    state_document: dict[str, Any] | None = None
    previous_market_watermark_utc: str | None = None
    previous_event_watermark_utc: str | None = None
    if incremental_enabled:
        state_document = load_state_document(state_path)
        profiles = state_document.get("profiles", {})
        profile_raw = profiles.get(state_key, {})
        if isinstance(profile_raw, dict):
            watermark_raw = profile_raw.get("watermark", {})
            if isinstance(watermark_raw, dict):
                market_wm_raw = watermark_raw.get("market_ticks_ingestion_ts_utc")
                event_wm_raw = watermark_raw.get("connection_events_event_ts_utc")
                if isinstance(market_wm_raw, str) and market_wm_raw:
                    previous_market_watermark_utc = market_wm_raw
                if isinstance(event_wm_raw, str) and event_wm_raw:
                    previous_event_watermark_utc = event_wm_raw

    market_lower_bound_utc = previous_market_watermark_utc or window_start_default_utc
    event_lower_bound_utc = previous_event_watermark_utc or window_start_default_utc
    market_lower_inclusive = not (incremental_enabled and previous_market_watermark_utc is not None)
    event_lower_inclusive = not (incremental_enabled and previous_event_watermark_utc is not None)

    market_where, market_params = build_market_where_clause(
        lower_bound_utc=market_lower_bound_utc,
        upper_bound_utc=window_end_utc_iso,
        lower_inclusive=market_lower_inclusive,
        exchanges=exchanges_filter,
        assets=assets_filter,
    )
    event_where, event_params = build_event_where_clause(
        lower_bound_utc=event_lower_bound_utc,
        upper_bound_utc=window_end_utc_iso,
        lower_inclusive=event_lower_inclusive,
        exchanges=exchanges_filter,
        assets=assets_filter,
    )

    source_exchanges, source_assets = collect_source_uniques(db_paths, market_where, market_params)

    print(f"Worker DB files used: {len(db_paths)}")
    print(f"Export mode: {'incremental' if incremental_enabled else 'window'}")
    print(f"Window default start UTC: {window_start_default_utc}")
    print(f"Window end UTC: {window_end_utc_iso}")
    print(
        "Market ticks query bounds: "
        f"{'>=' if market_lower_inclusive else '>'} {market_lower_bound_utc} and <= {window_end_utc_iso}"
    )
    print(
        "Connection events query bounds: "
        f"{'>=' if event_lower_inclusive else '>'} {event_lower_bound_utc} and <= {window_end_utc_iso}"
    )
    if incremental_enabled:
        print(f"State file: {state_path}")
        print(f"State key: {state_key}")
        print(f"Previous market watermark: {previous_market_watermark_utc or '-'}")
        print(f"Previous event watermark: {previous_event_watermark_utc or '-'}")
    if exchanges_filter:
        print(f"Exchange filter: {', '.join(exchanges_filter)}")
    if assets_filter:
        print(f"Asset filter: {', '.join(assets_filter)}")
    if args.print_unique_exchanges:
        print_list("Unique exchanges in source window", source_exchanges)
    if args.print_unique_assets:
        print_list("Unique assets in source window", source_assets)

    if args.list_only:
        print("List-only mode enabled. No export files were created.")
        return

    output_dir = Path(args.output_dir)
    base_name = output_base_name(args.output_prefix, args.hours, run_started_utc)
    output_base = output_dir / base_name
    run_id = base_name

    output_files: list[Path]
    exported_exchanges: list[str]
    exported_assets: list[str]
    exported_market_max_ts_utc: str | None = None
    exported_event_max_ts_utc: str | None = None

    if args.output_format == "sqlite":
        output_db_path = output_base.with_suffix(".db")
        market_count, event_count = insert_sqlite_from_sources(
            db_paths=db_paths,
            output_db_path=output_db_path,
            market_where=market_where,
            market_params=market_params,
            event_where=event_where,
            event_params=event_params,
            include_connection_events=include_events,
        )
        (
            exported_exchanges,
            exported_assets,
            exported_market_max_ts_utc,
            exported_event_max_ts_utc,
        ) = collect_exported_summary_from_sqlite(
            output_db_path=output_db_path,
            include_connection_events=include_events,
        )
        output_files = [output_db_path]
    else:
        (
            market_count,
            event_count,
            output_files,
            exported_exchanges,
            exported_assets,
            exported_market_max_ts_utc,
            exported_event_max_ts_utc,
        ) = write_csv_or_json_exports(
            db_paths=db_paths,
            output_base=output_base,
            output_format=args.output_format,
            market_where=market_where,
            market_params=market_params,
            event_where=event_where,
            event_params=event_params,
            include_connection_events=include_events,
            chunk_size=args.chunk_size,
        )

    new_market_watermark_utc: str | None = None
    new_event_watermark_utc: str | None = None
    state_updated = False
    if incremental_enabled:
        new_market_watermark_utc = max_iso_value(previous_market_watermark_utc, exported_market_max_ts_utc)
        new_market_watermark_utc = max_iso_value(new_market_watermark_utc, window_end_utc_iso)
        if include_events:
            new_event_watermark_utc = max_iso_value(previous_event_watermark_utc, exported_event_max_ts_utc)
            new_event_watermark_utc = max_iso_value(new_event_watermark_utc, window_end_utc_iso)
        else:
            new_event_watermark_utc = previous_event_watermark_utc

        if args.update_state:
            if state_document is None:
                raise SystemExit("Internal error: incremental mode expected loaded state document.")
            profiles = state_document.setdefault("profiles", {})
            profiles[state_key] = {
                "updated_utc": utc_iso(datetime.now(timezone.utc)),
                "source": {
                    "input_glob": args.input_glob,
                },
                "filters": {
                    "exchanges": exchanges_filter,
                    "assets": assets_filter,
                },
                "watermark": {
                    "market_ticks_ingestion_ts_utc": new_market_watermark_utc,
                    "connection_events_event_ts_utc": new_event_watermark_utc,
                },
            }
            write_state_document(state_path, state_document)
            state_updated = True

    run_finished_utc = datetime.now(timezone.utc)
    metadata = {
        "contract_name": STAGING_METADATA_CONTRACT_NAME,
        "contract_version": STAGING_METADATA_CONTRACT_VERSION,
        "run_id": run_id,
        "run_started_utc": utc_iso(run_started_utc),
        "run_finished_utc": utc_iso(run_finished_utc),
        "mode": {
            "incremental": incremental_enabled,
        },
        "window": {
            "hours": args.hours,
            "default_start_utc": window_start_default_utc,
            "end_utc": window_end_utc_iso,
        },
        "query_bounds": {
            "market_ticks": {
                "lower_utc": market_lower_bound_utc,
                "lower_inclusive": market_lower_inclusive,
                "upper_utc": window_end_utc_iso,
            },
            "connection_events": {
                "lower_utc": event_lower_bound_utc,
                "lower_inclusive": event_lower_inclusive,
                "upper_utc": window_end_utc_iso,
                "included": include_events,
            },
        },
        "source": {
            "input_glob": args.input_glob,
            "worker_db_count": len(db_paths),
            "worker_db_files": [str(path) for path in db_paths],
        },
        "options": {
            "output_format": args.output_format,
            "include_connection_events": include_events,
            "chunk_size": args.chunk_size,
        },
        "filters": {
            "exchanges": exchanges_filter,
            "assets": assets_filter,
        },
        "uniques": {
            "source": {
                "exchanges": source_exchanges,
                "assets": source_assets,
            },
            "exported": {
                "exchanges": exported_exchanges,
                "assets": exported_assets,
            },
        },
        "watermarks": {
            "previous": {
                "market_ticks_ingestion_ts_utc": previous_market_watermark_utc,
                "connection_events_event_ts_utc": previous_event_watermark_utc,
            },
            "exported_max": {
                "market_ticks_ingestion_ts_utc": exported_market_max_ts_utc,
                "connection_events_event_ts_utc": exported_event_max_ts_utc,
            },
            "new": {
                "market_ticks_ingestion_ts_utc": new_market_watermark_utc,
                "connection_events_event_ts_utc": new_event_watermark_utc,
            },
        },
        "state": {
            "path": str(state_path),
            "key": state_key,
            "update_requested": args.update_state,
            "updated": state_updated,
        },
        "row_counts": {
            "market_ticks": market_count,
            "connection_events": event_count,
        },
        "output_files": [str(path) for path in output_files],
    }
    metadata_path = output_base.with_name(f"{output_base.name}_metadata.json")
    write_metadata(metadata_path, metadata)

    print(f"Export format: {args.output_format}")
    print(f"Exported market_ticks rows: {market_count}")
    print(f"Exported connection_events rows: {event_count}")
    print_list("Unique exchanges exported", exported_exchanges)
    print_list("Unique assets exported", exported_assets)
    if incremental_enabled:
        print(f"New market watermark: {new_market_watermark_utc or '-'}")
        print(f"New event watermark: {new_event_watermark_utc or '-'}")
        print(f"State updated: {'yes' if state_updated else 'no'}")
    print("Created files:")
    for output_path in output_files + [metadata_path]:
        print(f"  - {output_path}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
