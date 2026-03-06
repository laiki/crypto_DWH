#!/usr/bin/env python3
"""
Export a configurable time window from VAULT 2.0 partitioned ingestion storage into a staging SQLite DB.

This exporter is intentionally VAULT-2.0-only (no legacy worker DB compatibility).
"""

from __future__ import annotations

import argparse
import logging
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

LOGGER = logging.getLogger("staging_exporter")


@dataclass(frozen=True)
class PartitionItem:
    partition_id: str
    db_path: str
    exchange_id_norm: str
    ts_min_epoch_s: int
    ts_max_epoch_s: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export a VAULT 2.0 time window into staging SQLite output.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--vault-root", default="data/vault2", help="VAULT 2.0 root directory.")
    parser.add_argument(
        "--manifest-db",
        default=None,
        help="Optional manifest DB path. Default: <vault-root>/meta/vault_manifest.db",
    )
    parser.add_argument("--layer", default="ingestion", help="Manifest layer to export from.")
    parser.add_argument("--hours", type=float, required=True, help="Export window in hours.")
    parser.add_argument(
        "--start-relative-from-hour",
        type=float,
        default=0.0,
        help=(
            "Shift export window end backwards by N hours from source max timestamp. "
            "Example: --hours 1 --start-relative-from-hour 2 exports [t-3h, t-2h]."
        ),
    )
    parser.add_argument("--output-dir", default="data/staging", help="Output directory for staging DB.")
    parser.add_argument("--output-prefix", default="staging_export", help="Output file prefix.")
    parser.add_argument("--output-db", default=None, help="Optional explicit output DB path.")
    parser.add_argument("--exchanges", default=None, help="Optional comma-separated exchange filter.")
    parser.add_argument("--assets", default=None, help="Optional comma-separated base-asset filter.")
    parser.add_argument("--symbols", default=None, help="Optional comma-separated exact symbol filter.")
    parser.add_argument("--skip-connection-events", action="store_true", help="Skip connection_events export.")
    parser.add_argument("--list-only", action="store_true", help="Print selected scope/window and exit.")
    parser.add_argument("--chunk-size", type=int, default=50000, help="Chunk size for inserts.")
    parser.add_argument("--progress", action=argparse.BooleanOptionalAction, default=True, help="Enable progress logs.")
    parser.add_argument(
        "--progress-interval-seconds",
        type=float,
        default=15.0,
        help="Seconds between progress logs.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level.",
    )
    return parser.parse_args()


def normalize_csv(raw: str | None, *, lower: bool = False, upper: bool = False) -> list[str]:
    if raw is None:
        return []
    values = [item.strip() for item in raw.split(",") if item.strip()]
    if lower:
        values = [item.lower() for item in values]
    if upper:
        values = [item.upper() for item in values]
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def hours_label(hours: float) -> str:
    if float(hours).is_integer():
        return str(int(hours))
    return f"{hours:.3f}".rstrip("0").rstrip(".").replace(".", "p")


def output_base_name(prefix: str, hours: float, start_relative_from_hour: float) -> str:
    now_utc = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    name = f"{prefix}_{now_utc}_last_{hours_label(hours)}h"
    if start_relative_from_hour > 0:
        name = f"{name}_from_{hours_label(start_relative_from_hour)}h"
    return name


def output_db_path(args: argparse.Namespace) -> Path:
    if args.output_db:
        return Path(args.output_db)
    out_dir = Path(args.output_dir)
    return out_dir / f"{output_base_name(args.output_prefix, args.hours, args.start_relative_from_hour)}.db"


def connect_readonly(db_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{db_path.resolve()}?mode=ro", uri=True)


def placeholders(count: int) -> str:
    return ",".join("?" for _ in range(count))


def resolve_source_max_epoch(
    manifest_connection: sqlite3.Connection,
    *,
    layer: str,
    exchanges_filter: list[str],
    assets_filter: list[str],
    symbols_filter: list[str],
) -> int | None:
    conditions = ["pr.layer = ?"]
    params: list[Any] = [layer]

    if exchanges_filter:
        conditions.append(f"pp.exchange_id_norm IN ({placeholders(len(exchanges_filter))})")
        params.extend(exchanges_filter)
    if assets_filter:
        conditions.append(f"pp.asset_norm IN ({placeholders(len(assets_filter))})")
        params.extend(assets_filter)
    if symbols_filter:
        conditions.append(f"pp.symbol_norm IN ({placeholders(len(symbols_filter))})")
        params.extend(symbols_filter)

    sql = (
        "SELECT MAX(pp.ts_max_epoch_s) "
        "FROM partition_pairs pp "
        "JOIN partition_registry pr ON pr.partition_id = pp.partition_id "
        f"WHERE {' AND '.join(conditions)}"
    )
    row = manifest_connection.execute(sql, params).fetchone()
    if row is None or row[0] is None:
        return None
    return int(row[0])


def select_partitions(
    manifest_connection: sqlite3.Connection,
    *,
    layer: str,
    window_start_epoch_s: int,
    window_end_epoch_s: int,
    exchanges_filter: list[str],
    assets_filter: list[str],
    symbols_filter: list[str],
) -> list[PartitionItem]:
    conditions = [
        "pr.layer = ?",
        "pr.ts_max_epoch_s >= ?",
        "pr.ts_min_epoch_s <= ?",
    ]
    params: list[Any] = [layer, window_start_epoch_s, window_end_epoch_s]

    if exchanges_filter:
        conditions.append(f"pp.exchange_id_norm IN ({placeholders(len(exchanges_filter))})")
        params.extend(exchanges_filter)
    if assets_filter:
        conditions.append(f"pp.asset_norm IN ({placeholders(len(assets_filter))})")
        params.extend(assets_filter)
    if symbols_filter:
        conditions.append(f"pp.symbol_norm IN ({placeholders(len(symbols_filter))})")
        params.extend(symbols_filter)

    sql = (
        "SELECT DISTINCT "
        "pr.partition_id, pr.db_path, pr.exchange_id_norm, pr.ts_min_epoch_s, pr.ts_max_epoch_s "
        "FROM partition_registry pr "
        "JOIN partition_pairs pp ON pp.partition_id = pr.partition_id "
        f"WHERE {' AND '.join(conditions)} "
        "ORDER BY pr.exchange_id_norm ASC, pr.ts_min_epoch_s ASC"
    )
    rows = manifest_connection.execute(sql, params).fetchall()
    return [
        PartitionItem(
            partition_id=str(row[0]),
            db_path=str(row[1]),
            exchange_id_norm=str(row[2]),
            ts_min_epoch_s=int(row[3]),
            ts_max_epoch_s=int(row[4]),
        )
        for row in rows
    ]


def ensure_output_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS market_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_partition_id TEXT NOT NULL,
            source_db_path TEXT NOT NULL,
            source_row_id INTEGER NOT NULL,
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

        CREATE INDEX IF NOT EXISTS idx_market_ticks_time
            ON market_ticks(ingestion_ts_epoch_s);

        CREATE TABLE IF NOT EXISTS connection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source_partition_id TEXT NOT NULL,
            source_db_path TEXT NOT NULL,
            source_row_id INTEGER NOT NULL,
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

        CREATE TABLE IF NOT EXISTS staging_export_run_metadata (
            run_id TEXT PRIMARY KEY,
            created_utc TEXT NOT NULL,
            vault_root TEXT NOT NULL,
            manifest_db TEXT NOT NULL,
            layer TEXT NOT NULL,
            hours REAL NOT NULL,
            start_relative_from_hour REAL NOT NULL,
            window_start_epoch_s INTEGER NOT NULL,
            window_end_epoch_s INTEGER NOT NULL,
            window_start_utc TEXT NOT NULL,
            window_end_utc TEXT NOT NULL,
            exchange_filter_csv TEXT NOT NULL,
            asset_filter_csv TEXT NOT NULL,
            symbol_filter_csv TEXT NOT NULL,
            partitions_selected INTEGER NOT NULL,
            market_rows INTEGER NOT NULL,
            connection_event_rows INTEGER NOT NULL,
            runtime_seconds REAL NOT NULL
        );
        """
    )
    connection.commit()


def format_hhmmss(total_seconds: float) -> str:
    value = max(0, int(total_seconds))
    hours = value // 3600
    minutes = (value % 3600) // 60
    seconds = value % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def export_to_sqlite(
    *,
    partitions: list[PartitionItem],
    out_db_path: Path,
    window_start_epoch_s: int,
    window_end_epoch_s: int,
    exchanges_filter: list[str],
    assets_filter: list[str],
    symbols_filter: list[str],
    include_connection_events: bool,
    progress_enabled: bool,
    progress_interval_seconds: float,
) -> tuple[int, int]:
    out_db_path.parent.mkdir(parents=True, exist_ok=True)
    output_connection = sqlite3.connect(str(out_db_path))
    try:
        ensure_output_schema(output_connection)

        market_rows_total = 0
        event_rows_total = 0

        started = time.monotonic()
        last_log = started - progress_interval_seconds

        for idx, partition in enumerate(partitions, start=1):
            alias = "src"
            output_connection.execute(f"ATTACH DATABASE ? AS {alias}", (partition.db_path,))
            try:
                market_conditions = [
                    "ingestion_ts_epoch_s >= ?",
                    "ingestion_ts_epoch_s <= ?",
                ]
                market_params: list[Any] = [window_start_epoch_s, window_end_epoch_s]
                if exchanges_filter:
                    market_conditions.append(f"exchange_id_norm IN ({placeholders(len(exchanges_filter))})")
                    market_params.extend(exchanges_filter)
                if assets_filter:
                    market_conditions.append(f"asset_norm IN ({placeholders(len(assets_filter))})")
                    market_params.extend(assets_filter)
                if symbols_filter:
                    market_conditions.append(f"symbol_norm IN ({placeholders(len(symbols_filter))})")
                    market_params.extend(symbols_filter)

                market_sql = (
                    "INSERT INTO market_ticks ("
                    "source_partition_id, source_db_path, source_row_id, ingestion_ts_utc, ingestion_ts_epoch_s, "
                    "exchange_id, exchange_id_norm, symbol, symbol_norm, asset_norm, market_type, price, bid, ask, "
                    "last, open, high, low, base_volume, quote_volume, exchange_ts_ms, exchange_ts_utc, raw_json"
                    ") "
                    "SELECT ?, ?, id, ingestion_ts_utc, ingestion_ts_epoch_s, "
                    "exchange_id, exchange_id_norm, symbol, symbol_norm, asset_norm, market_type, price, bid, ask, "
                    "last, open, high, low, base_volume, quote_volume, exchange_ts_ms, exchange_ts_utc, raw_json "
                    f"FROM {alias}.market_ticks WHERE {' AND '.join(market_conditions)}"
                )
                output_connection.execute(market_sql, [partition.partition_id, partition.db_path] + market_params)
                inserted_market = int(output_connection.execute("SELECT changes()").fetchone()[0])
                market_rows_total += inserted_market

                inserted_events = 0
                if include_connection_events:
                    event_conditions = [
                        "event_ts_epoch_s >= ?",
                        "event_ts_epoch_s <= ?",
                    ]
                    event_params: list[Any] = [window_start_epoch_s, window_end_epoch_s]
                    if exchanges_filter:
                        event_conditions.append(f"exchange_id_norm IN ({placeholders(len(exchanges_filter))})")
                        event_params.extend(exchanges_filter)
                    if assets_filter:
                        event_conditions.append(f"asset_norm IN ({placeholders(len(assets_filter))})")
                        event_params.extend(assets_filter)
                    if symbols_filter:
                        event_conditions.append(f"symbol_norm IN ({placeholders(len(symbols_filter))})")
                        event_params.extend(symbols_filter)

                    event_sql = (
                        "INSERT INTO connection_events ("
                        "source_partition_id, source_db_path, source_row_id, event_ts_utc, event_ts_epoch_s, "
                        "exchange_id, exchange_id_norm, symbol, symbol_norm, asset_norm, event_type, details_json"
                        ") "
                        "SELECT ?, ?, id, event_ts_utc, event_ts_epoch_s, exchange_id, exchange_id_norm, "
                        "symbol, symbol_norm, asset_norm, event_type, details_json "
                        f"FROM {alias}.connection_events WHERE {' AND '.join(event_conditions)}"
                    )
                    output_connection.execute(event_sql, [partition.partition_id, partition.db_path] + event_params)
                    inserted_events = int(output_connection.execute("SELECT changes()").fetchone()[0])
                    event_rows_total += inserted_events

                output_connection.commit()

                if progress_enabled:
                    now = time.monotonic()
                    if (now - last_log >= progress_interval_seconds) or idx == len(partitions):
                        elapsed = now - started
                        pct = idx / max(1, len(partitions)) * 100.0
                        rate = idx / max(elapsed, 1e-9)
                        remaining = max(0, len(partitions) - idx)
                        eta = remaining / max(rate, 1e-9)
                        LOGGER.info(
                            "Progress partitions %s/%s (%.1f%%) | elapsed=%s eta=%s | "
                            "market_rows=%s event_rows=%s | last=%s rows(m/e)=%s/%s",
                            idx,
                            len(partitions),
                            pct,
                            format_hhmmss(elapsed),
                            format_hhmmss(eta),
                            market_rows_total,
                            event_rows_total,
                            partition.partition_id,
                            inserted_market,
                            inserted_events,
                        )
                        last_log = now
            finally:
                output_connection.execute(f"DETACH DATABASE {alias}")

        return market_rows_total, event_rows_total
    finally:
        output_connection.close()


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)

    if args.hours <= 0:
        raise SystemExit("--hours must be > 0.")
    if args.start_relative_from_hour < 0:
        raise SystemExit("--start-relative-from-hour must be >= 0.")
    if args.chunk_size <= 0:
        raise SystemExit("--chunk-size must be > 0.")
    if args.progress_interval_seconds <= 0:
        raise SystemExit("--progress-interval-seconds must be > 0.")

    vault_root = Path(args.vault_root)
    manifest_db_path = Path(args.manifest_db) if args.manifest_db else vault_root / "meta" / "vault_manifest.db"
    if not manifest_db_path.exists():
        raise SystemExit(f"Manifest DB does not exist: {manifest_db_path}")

    exchanges_filter = normalize_csv(args.exchanges, lower=True)
    assets_filter = normalize_csv(args.assets, upper=True)
    assets_filter = [item.lower() for item in assets_filter]
    symbols_filter = normalize_csv(args.symbols, lower=True)

    with connect_readonly(manifest_db_path) as manifest_connection:
        source_max_epoch_s = resolve_source_max_epoch(
            manifest_connection,
            layer=args.layer,
            exchanges_filter=exchanges_filter,
            assets_filter=assets_filter,
            symbols_filter=symbols_filter,
        )
        if source_max_epoch_s is None:
            raise SystemExit("No source rows found in manifest for selected filters.")

        end_epoch = int(source_max_epoch_s - args.start_relative_from_hour * 3600)
        start_epoch = int(end_epoch - args.hours * 3600)
        if end_epoch <= start_epoch:
            raise SystemExit("Computed window is empty. Check --hours and --start-relative-from-hour.")

        partitions = select_partitions(
            manifest_connection,
            layer=args.layer,
            window_start_epoch_s=start_epoch,
            window_end_epoch_s=end_epoch,
            exchanges_filter=exchanges_filter,
            assets_filter=assets_filter,
            symbols_filter=symbols_filter,
        )

    start_utc = datetime.fromtimestamp(start_epoch, tz=timezone.utc).isoformat(timespec="seconds")
    end_utc = datetime.fromtimestamp(end_epoch, tz=timezone.utc).isoformat(timespec="seconds")

    LOGGER.info("Manifest: %s", manifest_db_path)
    LOGGER.info("Layer: %s", args.layer)
    LOGGER.info("Window UTC: %s -> %s", start_utc, end_utc)
    LOGGER.info("Filters | exchanges=%s assets=%s symbols=%s", exchanges_filter or "-", assets_filter or "-", symbols_filter or "-")
    LOGGER.info("Selected partitions: %s", len(partitions))

    if args.list_only:
        return

    if not partitions:
        raise SystemExit("No partitions overlap the selected window and filters.")

    out_db = output_db_path(args)
    started = time.monotonic()
    market_rows, event_rows = export_to_sqlite(
        partitions=partitions,
        out_db_path=out_db,
        window_start_epoch_s=start_epoch,
        window_end_epoch_s=end_epoch,
        exchanges_filter=exchanges_filter,
        assets_filter=assets_filter,
        symbols_filter=symbols_filter,
        include_connection_events=not args.skip_connection_events,
        progress_enabled=bool(args.progress),
        progress_interval_seconds=float(args.progress_interval_seconds),
    )

    runtime = time.monotonic() - started
    with sqlite3.connect(str(out_db)) as output_connection:
        run_id = f"staging_export_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
        output_connection.execute(
            """
            INSERT OR REPLACE INTO staging_export_run_metadata (
                run_id,
                created_utc,
                vault_root,
                manifest_db,
                layer,
                hours,
                start_relative_from_hour,
                window_start_epoch_s,
                window_end_epoch_s,
                window_start_utc,
                window_end_utc,
                exchange_filter_csv,
                asset_filter_csv,
                symbol_filter_csv,
                partitions_selected,
                market_rows,
                connection_event_rows,
                runtime_seconds
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                run_id,
                datetime.now(timezone.utc).isoformat(timespec="seconds"),
                str(vault_root.resolve()),
                str(manifest_db_path.resolve()),
                args.layer,
                float(args.hours),
                float(args.start_relative_from_hour),
                int(start_epoch),
                int(end_epoch),
                start_utc,
                end_utc,
                ",".join(exchanges_filter),
                ",".join(assets_filter),
                ",".join(symbols_filter),
                len(partitions),
                int(market_rows),
                int(event_rows),
                float(runtime),
            ),
        )
        output_connection.commit()

    LOGGER.info("Output DB: %s", out_db)
    LOGGER.info("Rows copied | market_ticks=%s | connection_events=%s", market_rows, event_rows)
    LOGGER.info("Runtime seconds: %.3f", runtime)


if __name__ == "__main__":
    main()
