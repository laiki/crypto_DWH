#!/usr/bin/env python3
"""
Build a query-ready Core SQLite database from staging and cleansing sources.

This script:
1) validates input DB paths and required tables,
2) creates/replaces output Core DB,
3) copies market_ticks, connection_events, and cleansed_market,
4) reapplies source table indexes,
5) applies Core KPI views,
6) writes core build metadata.
"""

from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from datetime import date
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


STAGING_SCHEMA = "staging_src"
CLEANSING_SCHEMA = "cleansing_src"

REQUIRED_TABLES_BY_SCHEMA = {
    STAGING_SCHEMA: ("market_ticks", "connection_events"),
    CLEANSING_SCHEMA: ("cleansed_market",),
}

CORE_VIEW_PREFIX = "vw_core_"


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="seconds")


def normalize_optional_text(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped if stripped else None


def validate_kpi_date(raw_value: str | None) -> str | None:
    if raw_value is None:
        return None
    normalized = raw_value.strip()
    try:
        date.fromisoformat(normalized)
    except ValueError as exc:
        raise SystemExit(f"Invalid --kpi-date value: {raw_value!r}. Expected YYYY-MM-DD.") from exc
    return normalized


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build Core SQLite database from staging and cleansing DBs and apply KPI views.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        default=None,
        help=(
            "Legacy single-DB mode. DB must contain market_ticks, connection_events, and cleansed_market. "
            "Cannot be combined with --staging-db/--cleansing-db."
        ),
    )
    parser.add_argument(
        "--staging-db",
        default=None,
        help="Path to staging SQLite DB (must contain market_ticks and connection_events).",
    )
    parser.add_argument(
        "--cleansing-db",
        default=None,
        help="Path to cleansing SQLite DB (must contain cleansed_market).",
    )
    parser.add_argument(
        "--output-db",
        default="data/core/core_kpi.db",
        help="Output Core SQLite DB path.",
    )
    parser.add_argument(
        "--views-sql",
        default="scripts/4_core/core_kpi_views.sql",
        help="Path to Core KPI views SQL file.",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Replace output DB if it already exists.",
    )
    parser.add_argument(
        "--kpi-date",
        default=None,
        help=(
            "Optional UTC date filter in YYYY-MM-DD. Applies to market_ticks/connection_events "
            "by date and cleansed_market by bucket_start_utc date."
        ),
    )
    parser.add_argument(
        "--cleansing-run-id",
        default=None,
        help="Optional run_id filter for cleansed_market.",
    )
    parser.add_argument(
        "--vacuum",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Run VACUUM on output DB after build.",
    )
    return parser.parse_args()


def resolve_input_paths(args: argparse.Namespace) -> tuple[Path, Path, str]:
    if args.db_path:
        if args.staging_db or args.cleansing_db:
            raise SystemExit("Use either --db-path OR (--staging-db and --cleansing-db), not both.")
        db_path = Path(args.db_path)
        return db_path, db_path, "single_db_legacy"

    if not args.staging_db or not args.cleansing_db:
        raise SystemExit("Provide both --staging-db and --cleansing-db (or use legacy --db-path).")

    return Path(args.staging_db), Path(args.cleansing_db), "split_db"


def remove_sqlite_artifacts(db_path: Path) -> None:
    for suffix in ("", "-wal", "-shm"):
        candidate = Path(str(db_path) + suffix)
        if candidate.exists():
            candidate.unlink()


def quote_ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def existing_table_names(connection: sqlite3.Connection, schema_name: str) -> set[str]:
    rows = connection.execute(
        f"SELECT name FROM {schema_name}.sqlite_master WHERE type='table'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def missing_required_tables(connection: sqlite3.Connection) -> list[str]:
    missing: list[str] = []
    for schema_name, required_tables in REQUIRED_TABLES_BY_SCHEMA.items():
        existing = existing_table_names(connection, schema_name)
        for table_name in required_tables:
            if table_name not in existing:
                missing.append(f"{schema_name}.{table_name}")
    return missing


def attach_sources(connection: sqlite3.Connection, staging_db_path: Path, cleansing_db_path: Path) -> None:
    connection.execute(f"ATTACH DATABASE ? AS {STAGING_SCHEMA}", (str(staging_db_path),))
    connection.execute(f"ATTACH DATABASE ? AS {CLEANSING_SCHEMA}", (str(cleansing_db_path),))


def fetch_table_ddl(connection: sqlite3.Connection, schema_name: str, table_name: str) -> str:
    row = connection.execute(
        f"SELECT sql FROM {schema_name}.sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    if row is None or row[0] is None:
        raise RuntimeError(f"Could not load DDL for {schema_name}.{table_name}")
    return str(row[0]).strip()


def fetch_index_ddls(connection: sqlite3.Connection, schema_name: str, table_name: str) -> list[str]:
    rows = connection.execute(
        f"""
        SELECT sql
        FROM {schema_name}.sqlite_master
        WHERE type='index'
          AND tbl_name=?
          AND sql IS NOT NULL
        ORDER BY name
        """,
        (table_name,),
    ).fetchall()
    return [str(row[0]).strip() for row in rows]


def build_where_clause(conditions: list[str]) -> str:
    if not conditions:
        return ""
    return " WHERE " + " AND ".join(conditions)


def copy_table_from_source(
    connection: sqlite3.Connection,
    *,
    source_schema: str,
    table_name: str,
    where_clause: str,
    where_params: list[Any],
) -> int:
    table_ident = quote_ident(table_name)
    source_table = f"{source_schema}.{table_ident}"

    source_table_ddl = fetch_table_ddl(connection, source_schema, table_name)
    source_index_ddls = fetch_index_ddls(connection, source_schema, table_name)

    connection.execute(f"DROP TABLE IF EXISTS main.{table_ident}")
    connection.execute(source_table_ddl)

    count_sql = f"SELECT COUNT(*) FROM {source_table}{where_clause}"
    source_count_row = connection.execute(count_sql, where_params).fetchone()
    source_count = int(source_count_row[0]) if source_count_row is not None else 0

    insert_sql = f"INSERT INTO {table_ident} SELECT * FROM {source_table}{where_clause}"
    connection.execute(insert_sql, where_params)

    for index_ddl in source_index_ddls:
        connection.execute(index_ddl)

    return source_count


def apply_views_sql(connection: sqlite3.Connection, views_sql_path: Path) -> int:
    script = views_sql_path.read_text(encoding="utf-8")
    connection.executescript(script)
    row = connection.execute(
        "SELECT COUNT(*) FROM sqlite_master WHERE type='view' AND name LIKE ?",
        (f"{CORE_VIEW_PREFIX}%",),
    ).fetchone()
    return int(row[0]) if row is not None else 0


def ensure_build_metadata_table(connection: sqlite3.Connection) -> None:
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS core_build_metadata (
            build_id TEXT PRIMARY KEY,
            created_utc TEXT NOT NULL,
            input_mode TEXT NOT NULL,
            staging_db_path TEXT NOT NULL,
            cleansing_db_path TEXT NOT NULL,
            output_db_path TEXT NOT NULL,
            views_sql_path TEXT NOT NULL,
            kpi_date_filter TEXT,
            cleansing_run_id_filter TEXT,
            row_count_market_ticks INTEGER NOT NULL,
            row_count_connection_events INTEGER NOT NULL,
            row_count_cleansed_market INTEGER NOT NULL,
            created_core_views_count INTEGER NOT NULL,
            runtime_seconds REAL NOT NULL
        )
        """
    )


def write_build_metadata(
    connection: sqlite3.Connection,
    *,
    build_id: str,
    created_utc: str,
    input_mode: str,
    staging_db_path: Path,
    cleansing_db_path: Path,
    output_db_path: Path,
    views_sql_path: Path,
    kpi_date_filter: str | None,
    cleansing_run_id_filter: str | None,
    market_rows: int,
    event_rows: int,
    cleansing_rows: int,
    created_core_views_count: int,
    runtime_seconds: float,
) -> None:
    ensure_build_metadata_table(connection)
    connection.execute(
        """
        INSERT OR REPLACE INTO core_build_metadata (
            build_id,
            created_utc,
            input_mode,
            staging_db_path,
            cleansing_db_path,
            output_db_path,
            views_sql_path,
            kpi_date_filter,
            cleansing_run_id_filter,
            row_count_market_ticks,
            row_count_connection_events,
            row_count_cleansed_market,
            created_core_views_count,
            runtime_seconds
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            build_id,
            created_utc,
            input_mode,
            str(staging_db_path),
            str(cleansing_db_path),
            str(output_db_path),
            str(views_sql_path),
            kpi_date_filter,
            cleansing_run_id_filter,
            market_rows,
            event_rows,
            cleansing_rows,
            created_core_views_count,
            runtime_seconds,
        ),
    )


def main() -> int:
    args = parse_args()
    started = time.monotonic()
    started_utc = utc_now()

    try:
        staging_db_path, cleansing_db_path, input_mode = resolve_input_paths(args)
    except SystemExit as exc:
        print(str(exc), file=sys.stderr)
        return 1

    output_db_path = Path(args.output_db)
    views_sql_path = Path(args.views_sql)
    kpi_date_filter = validate_kpi_date(args.kpi_date)
    cleansing_run_id_filter = normalize_optional_text(args.cleansing_run_id)

    if not staging_db_path.exists():
        print(f"Staging database file not found: {staging_db_path}", file=sys.stderr)
        return 1
    if not cleansing_db_path.exists():
        print(f"Cleansing database file not found: {cleansing_db_path}", file=sys.stderr)
        return 1
    if not views_sql_path.exists():
        print(f"Views SQL file not found: {views_sql_path}", file=sys.stderr)
        return 1

    staging_abs = staging_db_path.resolve()
    cleansing_abs = cleansing_db_path.resolve()
    output_abs = output_db_path.resolve()
    if output_abs in {staging_abs, cleansing_abs}:
        print("Output DB path must be different from all input DB paths.", file=sys.stderr)
        return 1

    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    if output_db_path.exists():
        if not args.overwrite:
            print(
                f"Output DB already exists and --no-overwrite was requested: {output_db_path}",
                file=sys.stderr,
            )
            return 1
        remove_sqlite_artifacts(output_db_path)

    connection = sqlite3.connect(str(output_db_path))
    market_rows = 0
    event_rows = 0
    cleansing_rows = 0
    created_core_views_count = 0
    error_message: str | None = None
    exit_code = 0

    try:
        connection.execute("PRAGMA journal_mode=WAL;")
        connection.execute("PRAGMA synchronous=NORMAL;")

        attach_sources(connection, staging_db_path=staging_db_path, cleansing_db_path=cleansing_db_path)
        missing_tables = missing_required_tables(connection)
        if missing_tables:
            raise RuntimeError("Missing required source tables: " + ", ".join(missing_tables))

        market_conditions: list[str] = []
        event_conditions: list[str] = []
        cleansing_conditions: list[str] = []
        params_market: list[Any] = []
        params_event: list[Any] = []
        params_cleansing: list[Any] = []

        if kpi_date_filter is not None:
            market_conditions.append("date(ingestion_ts_utc) = ?")
            event_conditions.append("date(event_ts_utc) = ?")
            cleansing_conditions.append("date(bucket_start_utc) = ?")
            params_market.append(kpi_date_filter)
            params_event.append(kpi_date_filter)
            params_cleansing.append(kpi_date_filter)

        if cleansing_run_id_filter is not None:
            cleansing_conditions.append("run_id = ?")
            params_cleansing.append(cleansing_run_id_filter)

        market_where = build_where_clause(market_conditions)
        event_where = build_where_clause(event_conditions)
        cleansing_where = build_where_clause(cleansing_conditions)

        market_rows = copy_table_from_source(
            connection,
            source_schema=STAGING_SCHEMA,
            table_name="market_ticks",
            where_clause=market_where,
            where_params=params_market,
        )
        event_rows = copy_table_from_source(
            connection,
            source_schema=STAGING_SCHEMA,
            table_name="connection_events",
            where_clause=event_where,
            where_params=params_event,
        )
        cleansing_rows = copy_table_from_source(
            connection,
            source_schema=CLEANSING_SCHEMA,
            table_name="cleansed_market",
            where_clause=cleansing_where,
            where_params=params_cleansing,
        )

        created_core_views_count = apply_views_sql(connection, views_sql_path)

        runtime_seconds = round(time.monotonic() - started, 3)
        build_id = f"core_build_{started_utc.strftime('%Y%m%d_%H%M%S')}"
        write_build_metadata(
            connection,
            build_id=build_id,
            created_utc=utc_iso(started_utc),
            input_mode=input_mode,
            staging_db_path=staging_db_path,
            cleansing_db_path=cleansing_db_path,
            output_db_path=output_db_path,
            views_sql_path=views_sql_path,
            kpi_date_filter=kpi_date_filter,
            cleansing_run_id_filter=cleansing_run_id_filter,
            market_rows=market_rows,
            event_rows=event_rows,
            cleansing_rows=cleansing_rows,
            created_core_views_count=created_core_views_count,
            runtime_seconds=runtime_seconds,
        )

        if args.vacuum:
            connection.execute("VACUUM")

        connection.commit()
    except Exception as exc:  # noqa: BLE001
        connection.rollback()
        error_message = repr(exc)
        exit_code = 1
    finally:
        connection.close()

    if exit_code != 0:
        print(f"Core build failed: {error_message}", file=sys.stderr)
        return exit_code

    runtime_seconds = round(time.monotonic() - started, 3)
    print(f"Output DB: {output_db_path}")
    print(f"Input mode: {input_mode}")
    print(f"Rows copied | market_ticks={market_rows} | connection_events={event_rows} | cleansed_market={cleansing_rows}")
    print(f"Core views created: {created_core_views_count}")
    print(f"Filters | kpi_date={kpi_date_filter or '-'} | cleansing_run_id={cleansing_run_id_filter or '-'}")
    print(f"Runtime seconds: {runtime_seconds}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
