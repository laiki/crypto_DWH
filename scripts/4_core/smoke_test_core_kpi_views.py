#!/usr/bin/env python3
"""
Run a deterministic smoke test for Core KPI SQL views.

This script:
1) creates in-memory source tables with minimal fixture data,
2) applies `core_kpi_views.sql`,
3) validates view contracts (names and columns),
4) validates core row semantics for daily and hourly KPI outputs.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
from pathlib import Path


EXPECTED_VIEW_NAMES = {
    "vw_core_latency_samples",
    "vw_core_kpi_latency_daily",
    "vw_core_kpi_latency_hourly",
    "vw_core_update_intervals",
    "vw_core_kpi_update_frequency_daily",
    "vw_core_kpi_update_frequency_hourly",
    "vw_core_kpi_connection_drops_daily",
    "vw_core_kpi_connection_drops_hourly",
    "vw_core_price_deviation_aligned",
    "vw_core_kpi_price_deviation_daily",
    "vw_core_kpi_price_deviation_hourly",
    "vw_core_kpi_daily_exchange_symbol",
    "vw_core_kpi_hourly_exchange_symbol",
    "vw_core_kpi_daily_exchange",
    "vw_core_kpi_hourly_exchange",
}


EXPECTED_VIEW_COLUMNS: dict[str, list[str]] = {
    "vw_core_latency_samples": [
        "kpi_date_utc",
        "exchange_id",
        "symbol",
        "ingestion_ts_utc",
        "exchange_ts_utc",
        "latency_ms",
    ],
    "vw_core_kpi_latency_daily": [
        "kpi_date_utc",
        "exchange_id",
        "symbol",
        "latency_sample_count",
        "avg_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
    ],
    "vw_core_kpi_latency_hourly": [
        "kpi_hour_utc",
        "exchange_id",
        "symbol",
        "latency_sample_count",
        "avg_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
    ],
    "vw_core_update_intervals": [
        "kpi_date_utc",
        "exchange_id",
        "symbol",
        "ingestion_ts_utc",
        "prev_ingestion_ts_utc",
        "update_interval_s",
    ],
    "vw_core_kpi_update_frequency_daily": [
        "kpi_date_utc",
        "exchange_id",
        "symbol",
        "interval_count",
        "avg_update_interval_s",
        "min_update_interval_s",
        "max_update_interval_s",
        "update_frequency_hz",
    ],
    "vw_core_kpi_update_frequency_hourly": [
        "kpi_hour_utc",
        "exchange_id",
        "symbol",
        "interval_count",
        "avg_update_interval_s",
        "min_update_interval_s",
        "max_update_interval_s",
        "update_frequency_hz",
    ],
    "vw_core_kpi_connection_drops_daily": [
        "kpi_date_utc",
        "exchange_id",
        "disconnect_count",
    ],
    "vw_core_kpi_connection_drops_hourly": [
        "kpi_hour_utc",
        "exchange_id",
        "disconnect_count",
    ],
    "vw_core_price_deviation_aligned": [
        "kpi_date_utc",
        "bucket_start_utc",
        "bucket_epoch_s",
        "symbol",
        "exchange_count",
        "max_price",
        "min_price",
        "price_diff_abs",
        "price_diff_pct",
        "max_price_exchange_id",
        "min_price_exchange_id",
        "max_diff_exchange_pair",
    ],
    "vw_core_kpi_price_deviation_daily": [
        "kpi_date_utc",
        "symbol",
        "aligned_points_compared",
        "max_price_diff_abs",
        "max_price_diff_pct",
        "avg_price_diff_abs",
        "avg_price_diff_pct",
        "max_diff_bucket_start_utc",
        "max_diff_exchange_pair",
    ],
    "vw_core_kpi_price_deviation_hourly": [
        "kpi_hour_utc",
        "symbol",
        "aligned_points_compared",
        "max_price_diff_abs",
        "max_price_diff_pct",
        "avg_price_diff_abs",
        "avg_price_diff_pct",
        "max_diff_bucket_start_utc",
        "max_diff_exchange_pair",
    ],
    "vw_core_kpi_daily_exchange_symbol": [
        "kpi_date_utc",
        "exchange_id",
        "symbol",
        "latency_sample_count",
        "avg_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
        "interval_count",
        "avg_update_interval_s",
        "min_update_interval_s",
        "max_update_interval_s",
        "update_frequency_hz",
        "disconnect_count",
    ],
    "vw_core_kpi_hourly_exchange_symbol": [
        "kpi_hour_utc",
        "exchange_id",
        "symbol",
        "latency_sample_count",
        "avg_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
        "interval_count",
        "avg_update_interval_s",
        "min_update_interval_s",
        "max_update_interval_s",
        "update_frequency_hz",
        "disconnect_count",
    ],
    "vw_core_kpi_daily_exchange": [
        "kpi_date_utc",
        "exchange_id",
        "latency_sample_count",
        "avg_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
        "interval_count",
        "avg_update_interval_s",
        "min_update_interval_s",
        "max_update_interval_s",
        "update_frequency_hz",
        "disconnect_count",
    ],
    "vw_core_kpi_hourly_exchange": [
        "kpi_hour_utc",
        "exchange_id",
        "latency_sample_count",
        "avg_latency_ms",
        "min_latency_ms",
        "max_latency_ms",
        "interval_count",
        "avg_update_interval_s",
        "min_update_interval_s",
        "max_update_interval_s",
        "update_frequency_hz",
        "disconnect_count",
    ],
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Smoke test for scripts/4_core/core_kpi_views.sql using in-memory fixtures.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--views-sql",
        default=str(Path(__file__).resolve().parent / "core_kpi_views.sql"),
        help="Path to Core KPI views SQL file.",
    )
    return parser.parse_args()


def create_source_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE market_ticks (
            ingestion_ts_utc TEXT,
            exchange_ts_utc TEXT,
            exchange_id TEXT,
            symbol TEXT
        );

        CREATE TABLE connection_events (
            event_ts_utc TEXT,
            exchange_id TEXT,
            event_type TEXT
        );

        CREATE TABLE cleansed_market (
            bucket_start_utc TEXT,
            bucket_epoch_s INTEGER,
            exchange_id TEXT,
            symbol TEXT,
            price REAL,
            is_missing INTEGER,
            is_stale INTEGER
        );
        """
    )


def insert_fixtures(connection: sqlite3.Connection) -> None:
    connection.executemany(
        """
        INSERT INTO market_ticks (
            ingestion_ts_utc, exchange_ts_utc, exchange_id, symbol
        ) VALUES (?, ?, ?, ?)
        """,
        [
            ("2026-02-21T10:00:01Z", "2026-02-21T10:00:00Z", "binance", "BTC/USDT"),
            ("2026-02-21T10:00:03Z", "2026-02-21T10:00:01Z", "binance", "BTC/USDT"),
            ("2026-02-21T11:00:03Z", "2026-02-21T11:00:02Z", "binance", "BTC/USDT"),
            ("2026-02-21T10:00:05Z", "2026-02-21T10:00:04Z", "binance", "ETH/USDT"),
            ("2026-02-21T10:00:04Z", "2026-02-21T10:00:03Z", "kraken", "BTC/USDT"),
        ],
    )

    connection.executemany(
        """
        INSERT INTO connection_events (
            event_ts_utc, exchange_id, event_type
        ) VALUES (?, ?, ?)
        """,
        [
            ("2026-02-21T10:05:00Z", "binance", "disconnect"),
            ("2026-02-21T10:20:00Z", "binance", "connect"),
            ("2026-02-21T11:05:00Z", "binance", "disconnect"),
        ],
    )

    connection.executemany(
        """
        INSERT INTO cleansed_market (
            bucket_start_utc, bucket_epoch_s, exchange_id, symbol, price, is_missing, is_stale
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            ("2026-02-21T10:00:00Z", 1761300000, "binance", "BTC/USDT", 100.0, 0, 0),
            ("2026-02-21T10:00:00Z", 1761300000, "kraken", "BTC/USDT", 101.0, 0, 0),
            ("2026-02-21T11:00:00Z", 1761303600, "binance", "BTC/USDT", 102.0, 0, 0),
            ("2026-02-21T11:00:00Z", 1761303600, "kraken", "BTC/USDT", 104.0, 0, 0),
            ("2026-02-21T11:00:00Z", 1761303600, "coinbase", "BTC/USDT", 150.0, 0, 1),
            ("2026-02-21T10:00:00Z", 1761300000, "binance", "ETH/USDT", 2500.0, 0, 0),
        ],
    )


def read_view_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='view' AND name LIKE 'vw_core_%'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def read_view_columns(connection: sqlite3.Connection, view_name: str) -> list[str]:
    rows = connection.execute(f'PRAGMA table_info("{view_name}")').fetchall()
    return [str(row[1]) for row in rows]


def approx_equal(actual: float, expected: float, tol: float) -> bool:
    return abs(actual - expected) <= tol


def expect_row(
    connection: sqlite3.Connection,
    failures: list[str],
    label: str,
    sql: str,
    params: tuple[str, ...] = (),
) -> sqlite3.Row | None:
    row = connection.execute(sql, params).fetchone()
    if row is None:
        failures.append(f"{label}: expected one row, got none.")
    return row


def validate_contracts(connection: sqlite3.Connection, failures: list[str]) -> None:
    actual_view_names = read_view_names(connection)
    if actual_view_names != EXPECTED_VIEW_NAMES:
        missing = sorted(EXPECTED_VIEW_NAMES - actual_view_names)
        unexpected = sorted(actual_view_names - EXPECTED_VIEW_NAMES)
        failures.append(
            "View name contract mismatch: "
            f"missing={missing if missing else '[]'}, unexpected={unexpected if unexpected else '[]'}."
        )

    for view_name, expected_columns in EXPECTED_VIEW_COLUMNS.items():
        actual_columns = read_view_columns(connection, view_name)
        if actual_columns != expected_columns:
            failures.append(
                f"{view_name}: column contract mismatch. "
                f"expected={expected_columns}, actual={actual_columns}."
            )


def validate_semantics(connection: sqlite3.Connection, failures: list[str]) -> None:
    day = "2026-02-21"
    hour_10 = "2026-02-21T10:00:00Z"
    hour_11 = "2026-02-21T11:00:00Z"

    row = expect_row(
        connection,
        failures,
        "latency_daily_binance_btc",
        """
        SELECT latency_sample_count, avg_latency_ms, min_latency_ms, max_latency_ms
        FROM vw_core_kpi_latency_daily
        WHERE kpi_date_utc = ? AND exchange_id = 'binance' AND symbol = 'BTC/USDT'
        """,
        (day,),
    )
    if row is not None:
        if int(row["latency_sample_count"]) != 3:
            failures.append("latency_daily_binance_btc: latency_sample_count must be 3.")
        if not approx_equal(float(row["avg_latency_ms"]), 1333.333, 2.0):
            failures.append("latency_daily_binance_btc: avg_latency_ms must be near 1333.333.")
        if not approx_equal(float(row["min_latency_ms"]), 1000.0, 2.0):
            failures.append("latency_daily_binance_btc: min_latency_ms must be near 1000.")
        if not approx_equal(float(row["max_latency_ms"]), 2000.0, 2.0):
            failures.append("latency_daily_binance_btc: max_latency_ms must be near 2000.")

    row = expect_row(
        connection,
        failures,
        "latency_hourly_binance_btc_hour10",
        """
        SELECT latency_sample_count
        FROM vw_core_kpi_latency_hourly
        WHERE kpi_hour_utc = ? AND exchange_id = 'binance' AND symbol = 'BTC/USDT'
        """,
        (hour_10,),
    )
    if row is not None and int(row["latency_sample_count"]) != 2:
        failures.append("latency_hourly_binance_btc_hour10: latency_sample_count must be 2.")

    row = expect_row(
        connection,
        failures,
        "update_daily_binance_btc",
        """
        SELECT interval_count, avg_update_interval_s, update_frequency_hz
        FROM vw_core_kpi_update_frequency_daily
        WHERE kpi_date_utc = ? AND exchange_id = 'binance' AND symbol = 'BTC/USDT'
        """,
        (day,),
    )
    if row is not None:
        if int(row["interval_count"]) != 2:
            failures.append("update_daily_binance_btc: interval_count must be 2.")
        if not approx_equal(float(row["avg_update_interval_s"]), 1801.0, 0.05):
            failures.append("update_daily_binance_btc: avg_update_interval_s must be near 1801.")
        if not approx_equal(float(row["update_frequency_hz"]), 0.000555, 0.000001):
            failures.append("update_daily_binance_btc: update_frequency_hz must be near 0.000555.")

    row = expect_row(
        connection,
        failures,
        "update_hourly_binance_btc_hour10",
        """
        SELECT interval_count, avg_update_interval_s, update_frequency_hz
        FROM vw_core_kpi_update_frequency_hourly
        WHERE kpi_hour_utc = ? AND exchange_id = 'binance' AND symbol = 'BTC/USDT'
        """,
        (hour_10,),
    )
    if row is not None:
        if int(row["interval_count"]) != 1:
            failures.append("update_hourly_binance_btc_hour10: interval_count must be 1.")
        if not approx_equal(float(row["avg_update_interval_s"]), 2.0, 0.05):
            failures.append("update_hourly_binance_btc_hour10: avg_update_interval_s must be near 2.")
        if not approx_equal(float(row["update_frequency_hz"]), 0.5, 0.00002):
            failures.append("update_hourly_binance_btc_hour10: update_frequency_hz must be 0.5.")

    row = expect_row(
        connection,
        failures,
        "update_hourly_binance_btc_hour11",
        """
        SELECT interval_count, avg_update_interval_s, update_frequency_hz
        FROM vw_core_kpi_update_frequency_hourly
        WHERE kpi_hour_utc = ? AND exchange_id = 'binance' AND symbol = 'BTC/USDT'
        """,
        (hour_11,),
    )
    if row is not None:
        if int(row["interval_count"]) != 1:
            failures.append("update_hourly_binance_btc_hour11: interval_count must be 1.")
        if not approx_equal(float(row["avg_update_interval_s"]), 3600.0, 0.05):
            failures.append("update_hourly_binance_btc_hour11: avg_update_interval_s must be near 3600.")
        if not approx_equal(float(row["update_frequency_hz"]), 0.000278, 0.000001):
            failures.append("update_hourly_binance_btc_hour11: update_frequency_hz must be near 0.000278.")

    row = expect_row(
        connection,
        failures,
        "disconnect_daily_binance",
        """
        SELECT disconnect_count
        FROM vw_core_kpi_connection_drops_daily
        WHERE kpi_date_utc = ? AND exchange_id = 'binance'
        """,
        (day,),
    )
    if row is not None and int(row["disconnect_count"]) != 2:
        failures.append("disconnect_daily_binance: disconnect_count must be 2.")

    row = expect_row(
        connection,
        failures,
        "price_deviation_daily_btc",
        """
        SELECT
            aligned_points_compared,
            max_price_diff_abs,
            avg_price_diff_abs,
            max_diff_bucket_start_utc,
            max_diff_exchange_pair
        FROM vw_core_kpi_price_deviation_daily
        WHERE kpi_date_utc = ? AND symbol = 'BTC/USDT'
        """,
        (day,),
    )
    if row is not None:
        if int(row["aligned_points_compared"]) != 2:
            failures.append("price_deviation_daily_btc: aligned_points_compared must be 2.")
        if not approx_equal(float(row["max_price_diff_abs"]), 2.0, 0.000001):
            failures.append("price_deviation_daily_btc: max_price_diff_abs must be 2.")
        if not approx_equal(float(row["avg_price_diff_abs"]), 1.5, 0.000001):
            failures.append("price_deviation_daily_btc: avg_price_diff_abs must be 1.5.")
        if str(row["max_diff_bucket_start_utc"]) != hour_11:
            failures.append("price_deviation_daily_btc: max_diff_bucket_start_utc must be 2026-02-21T11:00:00Z.")
        if str(row["max_diff_exchange_pair"]) != "kraken|binance":
            failures.append("price_deviation_daily_btc: max_diff_exchange_pair must be kraken|binance.")

    row = expect_row(
        connection,
        failures,
        "daily_exchange_symbol_binance_eth",
        """
        SELECT latency_sample_count, interval_count
        FROM vw_core_kpi_daily_exchange_symbol
        WHERE kpi_date_utc = ? AND exchange_id = 'binance' AND symbol = 'ETH/USDT'
        """,
        (day,),
    )
    if row is not None:
        if int(row["latency_sample_count"]) != 1:
            failures.append("daily_exchange_symbol_binance_eth: latency_sample_count must be 1.")
        if row["interval_count"] is not None:
            failures.append("daily_exchange_symbol_binance_eth: interval_count must be NULL.")

    row = expect_row(
        connection,
        failures,
        "daily_exchange_binance",
        """
        SELECT latency_sample_count, interval_count, disconnect_count
        FROM vw_core_kpi_daily_exchange
        WHERE kpi_date_utc = ? AND exchange_id = 'binance'
        """,
        (day,),
    )
    if row is not None:
        if int(row["latency_sample_count"]) != 4:
            failures.append("daily_exchange_binance: latency_sample_count must be 4.")
        if int(row["interval_count"]) != 2:
            failures.append("daily_exchange_binance: interval_count must be 2.")
        if int(row["disconnect_count"]) != 2:
            failures.append("daily_exchange_binance: disconnect_count must be 2.")

    row = expect_row(
        connection,
        failures,
        "hourly_exchange_binance_hour10",
        """
        SELECT latency_sample_count, interval_count, disconnect_count
        FROM vw_core_kpi_hourly_exchange
        WHERE kpi_hour_utc = ? AND exchange_id = 'binance'
        """,
        (hour_10,),
    )
    if row is not None:
        if int(row["latency_sample_count"]) != 3:
            failures.append("hourly_exchange_binance_hour10: latency_sample_count must be 3.")
        if int(row["interval_count"]) != 1:
            failures.append("hourly_exchange_binance_hour10: interval_count must be 1.")
        if int(row["disconnect_count"]) != 1:
            failures.append("hourly_exchange_binance_hour10: disconnect_count must be 1.")


def run_smoke_test(views_sql_path: Path) -> int:
    if not views_sql_path.exists():
        print(f"Views SQL file not found: {views_sql_path}", file=sys.stderr)
        return 1

    connection = sqlite3.connect(":memory:")
    connection.row_factory = sqlite3.Row

    create_source_tables(connection)
    insert_fixtures(connection)

    views_sql = views_sql_path.read_text(encoding="utf-8")
    connection.executescript(views_sql)

    failures: list[str] = []
    validate_contracts(connection, failures)
    validate_semantics(connection, failures)

    if failures:
        print(f"Smoke test FAILED with {len(failures)} issue(s):")
        for failure in failures:
            print(f"- {failure}")
        return 1

    print("Smoke test PASSED.")
    print(f"- views_sql: {views_sql_path}")
    print(f"- validated_views: {len(EXPECTED_VIEW_NAMES)}")
    print("- contracts: names + columns")
    print("- semantics: daily + hourly KPIs")
    return 0


def main() -> int:
    args = parse_args()
    return run_smoke_test(Path(args.views_sql))


if __name__ == "__main__":
    raise SystemExit(main())
