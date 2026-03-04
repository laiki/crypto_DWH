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
import threading
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


REQUIRED_VIEWS = (
    "vw_mart_dashboard_platform_quality_daily",
    "vw_mart_dashboard_price_deviation_daily",
    "vw_mart_dashboard_price_curve_24h_binance",
    "vw_mart_dashboard_symbol_deviation_bucket",
    "vw_mart_dashboard_symbol_observed_quality_base",
)

CACHE_TABLES = (
    "dash_cache_platform_quality_daily_latest",
    "dash_cache_price_deviation_daily_latest",
    "dash_cache_price_curve_24h_binance_latest",
    "dash_cache_symbol_deviation_bucket",
    "dash_cache_symbols",
    "dash_cache_refresh_metadata",
)


def format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


class CacheBuildProgress:
    def __init__(self, *, total_steps: int, enabled: bool, interval_seconds: float) -> None:
        self.total_steps = max(1, total_steps)
        self.enabled = enabled
        self.interval_seconds = max(0.1, interval_seconds)
        self.started_monotonic = time.monotonic()
        self.last_update_monotonic = 0.0
        self.completed_steps = 0
        self.current_step_label: str | None = None
        self.current_step_started_monotonic = 0.0
        self._lock = threading.Lock()

    def _eta_seconds(self, now_monotonic: float) -> float | None:
        if self.completed_steps <= 0:
            return None
        elapsed = now_monotonic - self.started_monotonic
        avg_step_seconds = elapsed / float(self.completed_steps)
        remaining_steps = max(0, self.total_steps - self.completed_steps)
        return avg_step_seconds * float(remaining_steps)

    def _emit(self, message: str, *, force: bool = False) -> None:
        if not self.enabled:
            return
        now_monotonic = time.monotonic()
        with self._lock:
            if not force and (now_monotonic - self.last_update_monotonic) < self.interval_seconds:
                return
            progress_pct = min(100.0, (self.completed_steps / float(self.total_steps)) * 100.0)
            elapsed_txt = format_duration(now_monotonic - self.started_monotonic)
            eta_seconds = self._eta_seconds(now_monotonic)
            eta_txt = format_duration(eta_seconds) if eta_seconds is not None else "n/a"
            print(
                f"[progress] steps {self.completed_steps}/{self.total_steps} ({progress_pct:.1f}%) "
                f"| elapsed={elapsed_txt} | eta={eta_txt} | {message}",
                flush=True,
            )
            self.last_update_monotonic = now_monotonic

    def start_step(self, label: str) -> None:
        self.current_step_label = label
        self.current_step_started_monotonic = time.monotonic()
        self._emit(f"started: {label}", force=True)

    def heartbeat(self, note: str) -> None:
        label = self.current_step_label or "-"
        self._emit(f"working: {label} | {note}", force=False)

    def finish_step(self, detail: str | None = None) -> None:
        step_label = self.current_step_label or "-"
        step_elapsed_seconds = 0.0
        if self.current_step_started_monotonic > 0:
            step_elapsed_seconds = time.monotonic() - self.current_step_started_monotonic
        self.completed_steps = min(self.total_steps, self.completed_steps + 1)
        suffix = f" | {detail}" if detail else ""
        self._emit(
            f"finished: {step_label} | step_elapsed={format_duration(step_elapsed_seconds)}{suffix}",
            force=True,
        )
        self.current_step_label = None
        self.current_step_started_monotonic = 0.0


def run_with_heartbeat(
    callback: Callable[[], Any],
    *,
    progress: CacheBuildProgress | None,
    heartbeat_note: str,
) -> Any:
    if progress is None or not progress.enabled:
        return callback()

    stop_event = threading.Event()

    def _heartbeat_loop() -> None:
        while not stop_event.wait(progress.interval_seconds):
            progress.heartbeat(heartbeat_note)

    heartbeat_thread = threading.Thread(target=_heartbeat_loop, daemon=True)
    heartbeat_thread.start()
    try:
        return callback()
    finally:
        stop_event.set()
        heartbeat_thread.join(timeout=0.5)


def parse_args() -> argparse.Namespace:
    default_mart_views_sql = Path(__file__).resolve().parent / "mart_dashboard_views.sql"
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
    parser.add_argument(
        "--apply-mart-views",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Apply mart views SQL before cache build.",
    )
    parser.add_argument(
        "--mart-views-sql",
        default=str(default_mart_views_sql),
        help="Path to mart dashboard views SQL file.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Print periodic progress updates with elapsed time and ETA.",
    )
    parser.add_argument(
        "--progress-interval-seconds",
        type=float,
        default=15.0,
        help="Seconds between periodic progress updates.",
    )
    return parser.parse_args()


def apply_mart_views_sql(
    connection: sqlite3.Connection,
    mart_views_sql_path: Path,
) -> None:
    if not mart_views_sql_path.exists():
        raise FileNotFoundError(f"Mart views SQL path does not exist: {mart_views_sql_path}")

    sql_text = mart_views_sql_path.read_text(encoding="utf-8")
    try:
        connection.executescript(sql_text)
    except sqlite3.Error as exc:
        raise RuntimeError(
            "Failed to apply mart views SQL. Ensure upstream Core views/tables exist "
            "in the same database before building dashboard cache."
        ) from exc


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
        CREATE TABLE dash_cache_symbol_deviation_bucket AS
        SELECT
            run_id,
            symbol,
            bucket_start_utc,
            bucket_epoch_s,
            exchange_count,
            max_price_close,
            min_price_close,
            price_diff_abs,
            price_diff_pct,
            max_price_exchange_id,
            min_price_exchange_id,
            max_diff_exchange_pair
        FROM vw_mart_dashboard_symbol_deviation_bucket;
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
            UNION
            SELECT symbol FROM dash_cache_symbol_deviation_bucket
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
            symbol_deviation_bucket_rows INTEGER NOT NULL,
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
        CREATE INDEX IF NOT EXISTS idx_dash_cache_symbol_deviation_run_symbol_bucket
        ON dash_cache_symbol_deviation_bucket(run_id, symbol, bucket_start_utc);
        """
    )
    connection.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_cache_symbol_deviation_symbol_bucket
        ON dash_cache_symbol_deviation_bucket(symbol, bucket_start_utc);
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
    symbol_deviation_bucket_rows = int(
        connection.execute(
            "SELECT COUNT(*) FROM dash_cache_symbol_deviation_bucket;"
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
            symbol_deviation_bucket_rows,
            symbol_rows
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
        """,
        (
            refresh_ts_utc,
            platform_latest_day,
            deviation_latest_day,
            platform_rows,
            deviation_rows,
            curve_rows,
            symbol_deviation_bucket_rows,
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
        "symbol_deviation_bucket_rows": symbol_deviation_bucket_rows,
        "symbol_rows": symbol_rows,
    }


def build_cache(
    db_path: Path,
    vacuum: bool,
    apply_mart_views: bool,
    mart_views_sql_path: Path,
    progress: CacheBuildProgress | None = None,
) -> dict[str, int | str | None]:
    if not db_path.exists():
        raise FileNotFoundError(f"DB path does not exist: {db_path}")

    with sqlite3.connect(str(db_path)) as connection:
        if progress is not None:
            progress.start_step("prepare connection")
        connection.execute("PRAGMA foreign_keys = OFF;")
        if progress is not None:
            progress.finish_step()
        if apply_mart_views:
            if progress is not None:
                progress.start_step("apply mart views sql")
            run_with_heartbeat(
                callback=lambda: apply_mart_views_sql(connection, mart_views_sql_path),
                progress=progress,
                heartbeat_note="applying mart views SQL",
            )
            if progress is not None:
                progress.finish_step()

        if progress is not None:
            progress.start_step("validate required views")
        ensure_required_views(connection)
        if progress is not None:
            progress.finish_step()

        if progress is not None:
            progress.start_step("begin transaction")
        connection.execute("BEGIN IMMEDIATE;")
        if progress is not None:
            progress.finish_step()
        try:
            if progress is not None:
                progress.start_step("drop old cache tables")
            drop_cache_tables(connection)
            if progress is not None:
                progress.finish_step()

            if progress is not None:
                progress.start_step("create cache tables")
            run_with_heartbeat(
                callback=lambda: create_cache_tables(connection),
                progress=progress,
                heartbeat_note="materializing cache tables",
            )
            if progress is not None:
                progress.finish_step()

            if progress is not None:
                progress.start_step("create cache indexes")
            run_with_heartbeat(
                callback=lambda: create_indexes(connection),
                progress=progress,
                heartbeat_note="creating cache indexes",
            )
            if progress is not None:
                progress.finish_step()

            if progress is not None:
                progress.start_step("write refresh metadata")
            summary = run_with_heartbeat(
                callback=lambda: write_metadata(connection),
                progress=progress,
                heartbeat_note="counting cache rows and writing metadata",
            )
            if progress is not None:
                progress.finish_step()

            if progress is not None:
                progress.start_step("commit transaction")
            connection.commit()
            if progress is not None:
                progress.finish_step()
        except Exception:
            connection.rollback()
            raise

        if vacuum:
            if progress is not None:
                progress.start_step("vacuum db")
            run_with_heartbeat(
                callback=lambda: connection.execute("VACUUM;"),
                progress=progress,
                heartbeat_note="vacuum in progress",
            )
            if progress is not None:
                progress.finish_step()

    return summary


def main() -> None:
    args = parse_args()
    if args.progress_interval_seconds <= 0:
        raise SystemExit("--progress-interval-seconds must be > 0.")
    db_path = Path(args.db_path).expanduser().resolve()
    mart_views_sql_path = Path(args.mart_views_sql).expanduser().resolve()
    planned_steps = [
        "prepare connection",
        "validate required views",
        "begin transaction",
        "drop old cache tables",
        "create cache tables",
        "create cache indexes",
        "write refresh metadata",
        "commit transaction",
    ]
    if args.apply_mart_views:
        planned_steps.insert(1, "apply mart views sql")
    if args.vacuum:
        planned_steps.append("vacuum db")

    progress = CacheBuildProgress(
        total_steps=len(planned_steps),
        enabled=bool(args.progress),
        interval_seconds=args.progress_interval_seconds,
    )
    if progress.enabled:
        print(
            "Progress logging: enabled "
            f"| interval_seconds={args.progress_interval_seconds:.1f} "
            f"| planned_steps={len(planned_steps)}",
            flush=True,
        )
    summary = build_cache(
        db_path=db_path,
        vacuum=bool(args.vacuum),
        apply_mart_views=bool(args.apply_mart_views),
        mart_views_sql_path=mart_views_sql_path,
        progress=progress,
    )

    print("Dashboard cache build completed.")
    print(f"DB path: {db_path}")
    print(f"Applied mart views: {bool(args.apply_mart_views)}")
    print(f"Mart views SQL path: {mart_views_sql_path}")
    print(f"Refresh timestamp (UTC): {summary['refresh_ts_utc']}")
    print(f"Platform latest day: {summary['platform_latest_kpi_date_utc']}")
    print(f"Deviation latest day: {summary['deviation_latest_kpi_date_utc']}")
    print(f"Platform rows: {summary['platform_rows']}")
    print(f"Deviation rows: {summary['deviation_rows']}")
    print(f"Curve rows: {summary['curve_rows']}")
    print(f"Symbol deviation bucket rows: {summary['symbol_deviation_bucket_rows']}")
    print(f"Symbol rows: {summary['symbol_rows']}")


if __name__ == "__main__":
    main()
