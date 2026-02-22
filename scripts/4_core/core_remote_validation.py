#!/usr/bin/env python3
"""
Run Core KPI SQL validation against a SQLite database and write run reports.

This script:
1) checks required source tables,
2) applies Core KPI views,
3) executes Core KPI assertions,
4) writes JSON and Markdown reports.
"""

from __future__ import annotations

import argparse
import json
import platform
import sqlite3
import sys
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


REQUIRED_SOURCE_TABLES = (
    "market_ticks",
    "connection_events",
    "cleansed_market",
)

CORE_VIEW_NAMES = (
    "vw_core_latency_samples",
    "vw_core_kpi_latency_daily",
    "vw_core_update_intervals",
    "vw_core_kpi_update_frequency_daily",
    "vw_core_kpi_connection_drops_daily",
    "vw_core_price_deviation_aligned",
    "vw_core_kpi_price_deviation_daily",
    "vw_core_kpi_daily_exchange_symbol",
    "vw_core_kpi_daily_exchange",
)


@dataclass
class AssertionResult:
    assertion_name: str
    severity: str
    is_failed: int
    failed_rows: int
    details: str


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(ts: datetime) -> str:
    return ts.astimezone(timezone.utc).isoformat(timespec="seconds")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate Core KPI SQL views and assertions on a SQLite database.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        required=True,
        help="Path to SQLite database that contains market_ticks, connection_events, and cleansed_market.",
    )
    parser.add_argument(
        "--views-sql",
        default="scripts/4_core/core_kpi_views.sql",
        help="Path to Core KPI views SQL file.",
    )
    parser.add_argument(
        "--assertions-sql",
        default="scripts/4_core/core_kpi_assertions.sql",
        help="Path to Core KPI assertions SQL file.",
    )
    parser.add_argument(
        "--output-dir",
        default="logs/core_validation",
        help="Directory for JSON and Markdown reports.",
    )
    parser.add_argument(
        "--report-prefix",
        default="core_validation",
        help="Prefix used for report file names.",
    )
    parser.add_argument(
        "--fail-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Return exit code 2 when at least one error-level assertion fails.",
    )
    return parser.parse_args()


def existing_table_names(connection: sqlite3.Connection) -> set[str]:
    rows = connection.execute(
        "SELECT name FROM sqlite_master WHERE type='table'"
    ).fetchall()
    return {str(row[0]) for row in rows}


def missing_required_tables(connection: sqlite3.Connection) -> list[str]:
    existing = existing_table_names(connection)
    missing = [name for name in REQUIRED_SOURCE_TABLES if name not in existing]
    return missing


def apply_view_sql(connection: sqlite3.Connection, views_sql: str) -> None:
    connection.executescript(views_sql)


def load_assertions(connection: sqlite3.Connection, assertions_sql: str) -> list[AssertionResult]:
    cursor = connection.execute(assertions_sql)
    rows = cursor.fetchall()
    results: list[AssertionResult] = []
    for row in rows:
        results.append(
            AssertionResult(
                assertion_name=str(row[0]),
                severity=str(row[1]),
                is_failed=int(row[2]),
                failed_rows=int(row[3]),
                details=str(row[4]),
            )
        )
    return results


def view_row_counts(connection: sqlite3.Connection) -> dict[str, int]:
    counts: dict[str, int] = {}
    for view_name in CORE_VIEW_NAMES:
        row = connection.execute(f"SELECT COUNT(*) FROM {view_name}").fetchone()
        counts[view_name] = int(row[0]) if row is not None else 0
    return counts


def summarize_assertions(results: list[AssertionResult]) -> dict[str, int]:
    error_failed = sum(1 for item in results if item.severity == "error" and item.is_failed == 1)
    warn_failed = sum(1 for item in results if item.severity == "warn" and item.is_failed == 1)
    total_error = sum(1 for item in results if item.severity == "error")
    total_warn = sum(1 for item in results if item.severity == "warn")
    return {
        "assertion_total": len(results),
        "error_total": total_error,
        "warn_total": total_warn,
        "error_failed": error_failed,
        "warn_failed": warn_failed,
    }


def format_markdown_report(payload: dict[str, Any]) -> str:
    lines: list[str] = []
    lines.append("# Core Validation Report")
    lines.append("")
    lines.append(f"- Run started UTC: `{payload['run_started_utc']}`")
    lines.append(f"- Run finished UTC: `{payload['run_finished_utc']}`")
    lines.append(f"- Duration seconds: `{payload['runtime_seconds']}`")
    lines.append(f"- Host: `{payload['host']}`")
    lines.append(f"- Python: `{payload['python_version']}`")
    lines.append(f"- DB path: `{payload['db_path']}`")
    lines.append("")
    lines.append("## Table Check")
    lines.append("")
    if payload["missing_required_tables"]:
        lines.append("Missing required tables:")
        for table_name in payload["missing_required_tables"]:
            lines.append(f"- `{table_name}`")
    else:
        lines.append("All required source tables are present.")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    summary = payload["summary"]
    lines.append(f"- Assertions total: `{summary['assertion_total']}`")
    lines.append(f"- Error failed: `{summary['error_failed']}`")
    lines.append(f"- Warn failed: `{summary['warn_failed']}`")
    lines.append("")
    lines.append("## Assertion Results")
    lines.append("")
    lines.append("| assertion_name | severity | is_failed | failed_rows | details |")
    lines.append("|---|---|---:|---:|---|")
    for item in payload["assertions"]:
        lines.append(
            "| {assertion_name} | {severity} | {is_failed} | {failed_rows} | {details} |".format(
                assertion_name=item["assertion_name"],
                severity=item["severity"],
                is_failed=item["is_failed"],
                failed_rows=item["failed_rows"],
                details=item["details"].replace("|", "\\|"),
            )
        )
    lines.append("")
    lines.append("## KPI View Row Counts")
    lines.append("")
    lines.append("| view_name | row_count |")
    lines.append("|---|---:|")
    for view_name, count in payload["view_row_counts"].items():
        lines.append(f"| {view_name} | {count} |")
    lines.append("")
    return "\n".join(lines) + "\n"


def write_reports(
    output_dir: Path,
    report_prefix: str,
    report_payload: dict[str, Any],
) -> tuple[Path, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    timestamp = utc_now().strftime("%Y%m%d_%H%M%SZ")
    base_name = f"{report_prefix}_{timestamp}"
    json_path = output_dir / f"{base_name}.json"
    md_path = output_dir / f"{base_name}.md"

    json_path.write_text(
        json.dumps(report_payload, ensure_ascii=True, indent=2),
        encoding="utf-8",
    )
    md_path.write_text(format_markdown_report(report_payload), encoding="utf-8")
    return json_path, md_path


def main() -> int:
    args = parse_args()
    run_started = utc_now()

    db_path = Path(args.db_path)
    views_sql_path = Path(args.views_sql)
    assertions_sql_path = Path(args.assertions_sql)
    output_dir = Path(args.output_dir)

    if not db_path.exists():
        print(f"Database file not found: {db_path}", file=sys.stderr)
        return 1
    if not views_sql_path.exists():
        print(f"Views SQL file not found: {views_sql_path}", file=sys.stderr)
        return 1
    if not assertions_sql_path.exists():
        print(f"Assertions SQL file not found: {assertions_sql_path}", file=sys.stderr)
        return 1

    views_sql = read_text(views_sql_path)
    assertions_sql = read_text(assertions_sql_path)

    try:
        connection = sqlite3.connect(str(db_path))
    except sqlite3.Error as exc:
        print(f"Failed to connect to database: {db_path} | error={exc!r}", file=sys.stderr)
        return 1

    assertions: list[AssertionResult] = []
    counts: dict[str, int] = {}
    missing_tables: list[str] = []
    error_message: str | None = None
    exit_code = 0

    try:
        missing_tables = missing_required_tables(connection)
        if missing_tables:
            raise RuntimeError(
                "Missing required source tables: " + ", ".join(missing_tables)
            )

        apply_view_sql(connection, views_sql)
        assertions = load_assertions(connection, assertions_sql)
        counts = view_row_counts(connection)
    except Exception as exc:  # noqa: BLE001
        error_message = repr(exc)
        exit_code = 1
    finally:
        connection.close()

    run_finished = utc_now()
    runtime_seconds = round((run_finished - run_started).total_seconds(), 3)
    summary = summarize_assertions(assertions) if assertions else {
        "assertion_total": 0,
        "error_total": 0,
        "warn_total": 0,
        "error_failed": 0,
        "warn_failed": 0,
    }

    payload = {
        "run_started_utc": utc_iso(run_started),
        "run_finished_utc": utc_iso(run_finished),
        "runtime_seconds": runtime_seconds,
        "host": platform.node(),
        "python_version": platform.python_version(),
        "db_path": str(db_path),
        "views_sql_path": str(views_sql_path),
        "assertions_sql_path": str(assertions_sql_path),
        "missing_required_tables": missing_tables,
        "summary": summary,
        "assertions": [asdict(item) for item in assertions],
        "view_row_counts": counts,
        "error_message": error_message,
    }

    json_report_path, md_report_path = write_reports(
        output_dir=output_dir,
        report_prefix=args.report_prefix,
        report_payload=payload,
    )

    print(f"JSON report: {json_report_path}")
    print(f"Markdown report: {md_report_path}")
    print(
        "Summary: "
        f"errors_failed={summary['error_failed']} | "
        f"warn_failed={summary['warn_failed']} | "
        f"runtime_s={runtime_seconds}"
    )

    if exit_code != 0:
        print(f"Validation execution failed: {error_message}", file=sys.stderr)
        return exit_code

    if args.fail_on_error and summary["error_failed"] > 0:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
