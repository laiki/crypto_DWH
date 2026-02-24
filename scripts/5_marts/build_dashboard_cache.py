#!/usr/bin/env python3
"""
Build precomputed dashboard cache tables for fast UI reads.

This script materializes the latest dashboard snapshots into physical SQLite
tables. The dashboard app can read these tables directly to avoid repeatedly
executing expensive mart view logic on every symbol change.
"""

from __future__ import annotations

import argparse
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


REQUIRED_VIEWS = (
    "vw_mart_dashboard_platform_quality_daily",
    "vw_mart_dashboard_price_deviation_daily",
    "vw_mart_dashboard_price_curve_24h_binance",
)

CACHE_TABLES = (
    "dash_cache_platform_quality_daily_latest",
    "dash_cache_price_deviation_daily_latest",
    "dash_cache_price_curve_24h_binance_latest",
    "dash_cache_symbols",
    "dash_cache_refresh_metadata",
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build precomputed dashboard cache tables.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        default="data/core/core_kpi.db",
        help="Path to SQLite Core DB that contains mart views.",
    )
    parser.add_argument(
        "--vacuum",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run VACUUM after cache build to compact DB pages.",
    )
    return parser.parse_args()


def ensure_required_views(connection: sqlite3.Connection) -> None:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type = 'view';"
    ).fetchall()
    existing = {row[0] for row in rows}
    missing = [view_name for view_name in REQUIRED_VIEWS if view_name not in existing]
    if missing:
        raise RuntimeError(
            "Missing required mart views for cache build: " + ", ".join(missing)
        )


def drop_cache_tables(connection: sqlite3.Connection) -> None:
    for table_name in CACHE_TABLES:
        connection.execute(f"DROP TABLE IF EXISTS {table_name};")


def create_cache_tables(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE dash_cache_platform_quality_daily_latest AS
        WITH latest_day AS (
            SELECT MAX(kpi_date_utc) AS kpi_date_utc
            FROM vw_mart_dashboard_platform_quality_daily
        )
        SELECT
            kpi_date_utc,
            exchange_id,
            symbols_covered,
            avg_latency_ms,
            min_latency_ms,
            max_latency_ms,
            update_frequency_hz,
            disconnect_count,
            default_quality_score,
            default_quality_rank
        FROM vw_mart_dashboard_platform_quality_daily
        WHERE kpi_date_utc = (SELECT kpi_date_utc FROM latest_day);
        """
    )

    connection.execute(
        """
        CREATE TABLE dash_cache_price_deviation_daily_latest AS
        WITH latest_day AS (
            SELECT MAX(kpi_date_utc) AS kpi_date_utc
            FROM vw_mart_dashboard_price_deviation_daily
        )
        SELECT
            kpi_date_utc,
            symbol,
            aligned_points_compared,
            max_price_diff_abs,
            max_price_diff_pct,
            avg_price_diff_abs,
            avg_price_diff_pct,
            max_diff_bucket_start_utc,
            max_diff_exchange_pair,
            default_deviation_rank
        FROM vw_mart_dashboard_price_deviation_daily
        WHERE kpi_date_utc = (SELECT kpi_date_utc FROM latest_day);
        """
    )

    connection.execute(
        """
        CREATE TABLE dash_cache_price_curve_24h_binance_latest AS
        SELECT
            run_id,
            exchange_id,
            symbol,
            bucket_start_utc,
            price_close,
            point_index_asc
        FROM vw_mart_dashboard_price_curve_24h_binance;
        """
    )

    connection.execute(
        """
        CREATE TABLE dash_cache_symbols AS
        SELECT symbol
        FROM (
            SELECT symbol FROM dash_cache_price_curve_24h_binance_latest
            UNION
            SELECT symbol FROM dash_cache_price_deviation_daily_latest
        )
        WHERE symbol IS NOT NULL
        ORDER BY symbol ASC;
        """
    )

    connection.execute(
        """
        CREATE TABLE dash_cache_refresh_metadata (
            refresh_ts_utc TEXT NOT NULL,
            platform_latest_kpi_date_utc TEXT,
            deviation_latest_kpi_date_utc TEXT,
            platform_rows INTEGER NOT NULL,
            deviation_rows INTEGER NOT NULL,
            curve_rows INTEGER NOT NULL,
            symbol_rows INTEGER NOT NULL
        );
        """
    )


def create_indexes(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_cache_platform_exchange
        ON dash_cache_platform_quality_daily_latest(exchange_id);
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_cache_platform_disconnect
        ON dash_cache_platform_quality_daily_latest(disconnect_count, exchange_id);
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_cache_deviation_symbol
        ON dash_cache_price_deviation_daily_latest(symbol);
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_cache_curve_symbol_point
        ON dash_cache_price_curve_24h_binance_latest(symbol, point_index_asc);
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_cache_symbols_symbol
        ON dash_cache_symbols(symbol);
        """
    )


def write_metadata(connection: sqlite3.Connection) -> dict[str, int | str | None]:
    platform_latest_day = connection.execute(
        "SELECT MAX(kpi_date_utc) FROM dash_cache_platform_quality_daily_latest;"
    ).fetchone()[0]
    deviation_latest_day = connection.execute(
        "SELECT MAX(kpi_date_utc) FROM dash_cache_price_deviation_daily_latest;"
    ).fetchone()[0]

    platform_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM dash_cache_platform_quality_daily_latest;"
        ).fetchone()[0]
    )
    deviation_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM dash_cache_price_deviation_daily_latest;"
        ).fetchone()[0]
    )
    curve_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM dash_cache_price_curve_24h_binance_latest;"
        ).fetchone()[0]
    )
    symbol_rows = int(
        connection.execute("SELECT COUNT(*) FROM dash_cache_symbols;").fetchone()[0]
    )

    refresh_ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    connection.execute(
        """
        INSERT INTO dash_cache_refresh_metadata (
            refresh_ts_utc,
            platform_latest_kpi_date_utc,
            deviation_latest_kpi_date_utc,
            platform_rows,
            deviation_rows,
            curve_rows,
            symbol_rows
        ) VALUES (?, ?, ?, ?, ?, ?, ?);
        """,
        (
            refresh_ts_utc,
            platform_latest_day,
            deviation_latest_day,
            platform_rows,
            deviation_rows,
            curve_rows,
            symbol_rows,
        ),
    )

    return {
        "refresh_ts_utc": refresh_ts_utc,
        "platform_latest_kpi_date_utc": platform_latest_day,
        "deviation_latest_kpi_date_utc": deviation_latest_day,
        "platform_rows": platform_rows,
        "deviation_rows": deviation_rows,
        "curve_rows": curve_rows,
        "symbol_rows": symbol_rows,
    }


def build_cache(db_path: Path, vacuum: bool) -> dict[str, int | str | None]:
    if not db_path.exists():
        raise FileNotFoundError(f"DB path does not exist: {db_path}")

    with sqlite3.connect(str(db_path)) as connection:
        connection.execute("PRAGMA foreign_keys = OFF;")
        ensure_required_views(connection)

        connection.execute("BEGIN IMMEDIATE;")
        try:
            drop_cache_tables(connection)
            create_cache_tables(connection)
            create_indexes(connection)
            summary = write_metadata(connection)
            connection.commit()
        except Exception:
            connection.rollback()
            raise

        if vacuum:
            connection.execute("VACUUM;")

    return summary


def main() -> None:
    args = parse_args()
    db_path = Path(args.db_path).expanduser().resolve()
    summary = build_cache(db_path=db_path, vacuum=bool(args.vacuum))

    print("Dashboard cache build completed.")
    print(f"DB path: {db_path}")
    print(f"Refresh timestamp (UTC): {summary['refresh_ts_utc']}")
    print(f"Platform latest day: {summary['platform_latest_kpi_date_utc']}")
    print(f"Deviation latest day: {summary['deviation_latest_kpi_date_utc']}")
    print(f"Platform rows: {summary['platform_rows']}")
    print(f"Deviation rows: {summary['deviation_rows']}")
    print(f"Curve rows: {summary['curve_rows']}")
    print(f"Symbol rows: {summary['symbol_rows']}")


if __name__ == "__main__":
    main()
