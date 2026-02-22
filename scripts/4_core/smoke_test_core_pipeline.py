#!/usr/bin/env python3
"""
Run an integration smoke test for the Core pipeline wrapper.

This script:
1) creates temporary staging/cleansing SQLite fixture databases,
2) runs `core_pipeline.py` in `fast` phase,
3) runs `core_pipeline.py` in `full` phase,
4) validates expected exit codes, report payloads, and build metadata.
"""

from __future__ import annotations

import argparse
import json
import shutil
import sqlite3
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Any


FIXTURE_KPI_DATE = "2026-02-21"
FIXTURE_CLEANSING_RUN_ID = "cleansing_fast"
REPORT_PREFIX = "pipeline_smoke"


def parse_args(project_root: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Integration smoke test for scripts/4_core/core_pipeline.py.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--pipeline-script",
        default=str(project_root / "scripts" / "4_core" / "core_pipeline.py"),
        help="Path to core_pipeline.py.",
    )
    parser.add_argument(
        "--views-sql",
        default=str(project_root / "scripts" / "4_core" / "core_kpi_views.sql"),
        help="Path to Core KPI views SQL file.",
    )
    parser.add_argument(
        "--assertions-sql",
        default=str(project_root / "scripts" / "4_core" / "core_kpi_assertions.sql"),
        help="Path to Core KPI assertions SQL file.",
    )
    parser.add_argument(
        "--work-dir",
        default=None,
        help="Optional existing work directory. If omitted, a temporary directory is used.",
    )
    parser.add_argument(
        "--keep-work-dir",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Keep generated work directory after test execution.",
    )
    return parser.parse_args()


def ensure_paths_exist(paths: list[tuple[Path, str]]) -> None:
    missing = [f"{label}: {path}" for path, label in paths if not path.exists()]
    if missing:
        raise RuntimeError("Required files missing:\n- " + "\n- ".join(missing))


def create_staging_db(db_path: Path) -> None:
    connection = sqlite3.connect(str(db_path))
    try:
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
            """
        )

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
                ("2026-02-20T09:00:02Z", "2026-02-20T09:00:01Z", "binance", "BTC/USDT"),
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
                ("2026-02-20T09:05:00Z", "binance", "disconnect"),
            ],
        )

        connection.commit()
    finally:
        connection.close()


def create_cleansing_db(db_path: Path) -> None:
    connection = sqlite3.connect(str(db_path))
    try:
        connection.executescript(
            """
            CREATE TABLE cleansed_market (
                run_id TEXT,
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

        connection.executemany(
            """
            INSERT INTO cleansed_market (
                run_id,
                bucket_start_utc,
                bucket_epoch_s,
                exchange_id,
                symbol,
                price,
                is_missing,
                is_stale
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                (FIXTURE_CLEANSING_RUN_ID, "2026-02-21T10:00:00Z", 1761300000, "binance", "BTC/USDT", 100.0, 0, 0),
                (FIXTURE_CLEANSING_RUN_ID, "2026-02-21T10:00:00Z", 1761300000, "kraken", "BTC/USDT", 101.0, 0, 0),
                (FIXTURE_CLEANSING_RUN_ID, "2026-02-21T11:00:00Z", 1761303600, "binance", "BTC/USDT", 102.0, 0, 0),
                (FIXTURE_CLEANSING_RUN_ID, "2026-02-21T11:00:00Z", 1761303600, "kraken", "BTC/USDT", 104.0, 0, 0),
                ("cleansing_other", "2026-02-21T10:00:00Z", 1761300000, "coinbase", "BTC/USDT", 99.0, 0, 0),
                ("cleansing_old", "2026-02-20T09:00:00Z", 1761210000, "binance", "BTC/USDT", 98.0, 0, 0),
            ],
        )

        connection.commit()
    finally:
        connection.close()


def run_command(command: list[str], cwd: Path) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )


def find_single_report(report_dir: Path, pattern: str) -> Path:
    matches = sorted(report_dir.glob(pattern))
    if len(matches) != 1:
        raise RuntimeError(
            f"Expected exactly one report for pattern '{pattern}' in {report_dir}, got {len(matches)}."
        )
    return matches[0]


def load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def load_build_metadata(db_path: Path) -> dict[str, Any]:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT
                row_count_market_ticks,
                row_count_connection_events,
                row_count_cleansed_market,
                kpi_date_filter,
                cleansing_run_id_filter,
                created_core_views_count
            FROM core_build_metadata
            ORDER BY created_utc DESC
            LIMIT 1
            """
        ).fetchone()
    finally:
        connection.close()

    if row is None:
        raise RuntimeError(f"No core_build_metadata row found in {db_path}.")
    return dict(row)


def expect(condition: bool, message: str) -> None:
    if not condition:
        raise RuntimeError(message)


def run_pipeline_phase(
    *,
    phase: str,
    project_root: Path,
    pipeline_script: Path,
    staging_db: Path,
    cleansing_db: Path,
    output_db: Path,
    report_dir: Path,
    views_sql: Path,
    assertions_sql: Path,
    with_fast_filters: bool,
) -> subprocess.CompletedProcess[str]:
    command = [
        sys.executable,
        str(pipeline_script),
        "--phase",
        phase,
        "--staging-db",
        str(staging_db),
        "--cleansing-db",
        str(cleansing_db),
        "--output-db",
        str(output_db),
        "--views-sql",
        str(views_sql),
        "--assertions-sql",
        str(assertions_sql),
        "--validation-output-dir",
        str(report_dir),
        "--report-prefix",
        REPORT_PREFIX,
        "--overwrite",
        "--no-vacuum",
        "--fail-on-error",
    ]
    if with_fast_filters:
        command.extend(["--kpi-date", FIXTURE_KPI_DATE, "--cleansing-run-id", FIXTURE_CLEANSING_RUN_ID])

    return run_command(command, cwd=project_root)


def validate_fast_phase(
    *,
    process: subprocess.CompletedProcess[str],
    report_dir: Path,
    output_db: Path,
) -> dict[str, Any]:
    expect(process.returncode == 0, f"Fast phase expected exit code 0, got {process.returncode}.")
    expect("Pipeline completed successfully." in process.stdout, "Fast phase missing success marker in stdout.")
    expect(output_db.exists(), f"Fast phase output DB not found: {output_db}")

    report_json = find_single_report(report_dir, f"{REPORT_PREFIX}_fast_*.json")
    report_payload = load_json(report_json)
    expect(report_payload["view_row_counts_skipped"] is True, "Fast report must skip view row counts.")
    expect(report_payload["kpi_date_filter"] == FIXTURE_KPI_DATE, "Fast report kpi_date_filter mismatch.")
    expect(
        report_payload["cleansing_run_id_filter"] == FIXTURE_CLEANSING_RUN_ID,
        "Fast report cleansing_run_id_filter mismatch.",
    )
    expect(report_payload["summary"]["error_failed"] == 0, "Fast report contains failed error assertions.")

    metadata = load_build_metadata(output_db)
    expect(metadata["created_core_views_count"] >= 15, "Fast build created_core_views_count must be >= 15.")
    expect(metadata["kpi_date_filter"] == FIXTURE_KPI_DATE, "Fast build metadata kpi_date_filter mismatch.")
    expect(
        metadata["cleansing_run_id_filter"] == FIXTURE_CLEANSING_RUN_ID,
        "Fast build metadata cleansing_run_id_filter mismatch.",
    )
    expect(metadata["row_count_market_ticks"] == 5, "Fast build market_ticks row count must be 5.")
    expect(metadata["row_count_connection_events"] == 3, "Fast build connection_events row count must be 3.")
    expect(metadata["row_count_cleansed_market"] == 4, "Fast build cleansed_market row count must be 4.")
    return metadata


def validate_full_phase(
    *,
    process: subprocess.CompletedProcess[str],
    report_dir: Path,
    output_db: Path,
) -> dict[str, Any]:
    expect(process.returncode == 0, f"Full phase expected exit code 0, got {process.returncode}.")
    expect("Pipeline completed successfully." in process.stdout, "Full phase missing success marker in stdout.")
    expect(output_db.exists(), f"Full phase output DB not found: {output_db}")

    report_json = find_single_report(report_dir, f"{REPORT_PREFIX}_full_*.json")
    report_payload = load_json(report_json)
    expect(report_payload["view_row_counts_skipped"] is False, "Full report must include view row counts.")
    expect(report_payload["kpi_date_filter"] is None, "Full report kpi_date_filter must be null.")
    expect(report_payload["cleansing_run_id_filter"] is None, "Full report cleansing_run_id_filter must be null.")
    expect(report_payload["summary"]["error_failed"] == 0, "Full report contains failed error assertions.")
    expect(
        "vw_core_kpi_hourly_exchange" in report_payload["view_row_counts"],
        "Full report missing expected view row count key.",
    )

    metadata = load_build_metadata(output_db)
    expect(metadata["created_core_views_count"] >= 15, "Full build created_core_views_count must be >= 15.")
    expect(metadata["kpi_date_filter"] is None, "Full build metadata kpi_date_filter must be null.")
    expect(metadata["cleansing_run_id_filter"] is None, "Full build metadata cleansing_run_id_filter must be null.")
    expect(metadata["row_count_market_ticks"] == 6, "Full build market_ticks row count must be 6.")
    expect(metadata["row_count_connection_events"] == 4, "Full build connection_events row count must be 4.")
    expect(metadata["row_count_cleansed_market"] == 6, "Full build cleansed_market row count must be 6.")
    return metadata


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    args = parse_args(project_root)

    pipeline_script = Path(args.pipeline_script).expanduser().resolve()
    views_sql = Path(args.views_sql).expanduser().resolve()
    assertions_sql = Path(args.assertions_sql).expanduser().resolve()

    try:
        ensure_paths_exist(
            [
                (pipeline_script, "pipeline script"),
                (views_sql, "views SQL"),
                (assertions_sql, "assertions SQL"),
            ]
        )
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return 1

    created_temp_dir = False
    if args.work_dir:
        work_dir = Path(args.work_dir).expanduser().resolve()
        work_dir.mkdir(parents=True, exist_ok=True)
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="core_pipeline_smoke_")).resolve()
        created_temp_dir = True

    inputs_dir = work_dir / "inputs"
    outputs_dir = work_dir / "outputs"
    reports_fast_dir = work_dir / "reports_fast"
    reports_full_dir = work_dir / "reports_full"
    for directory in (inputs_dir, outputs_dir, reports_fast_dir, reports_full_dir):
        directory.mkdir(parents=True, exist_ok=True)

    staging_db = inputs_dir / "staging_fixture.db"
    cleansing_db = inputs_dir / "cleansing_fixture.db"
    output_fast_db = outputs_dir / "core_fast.db"
    output_full_db = outputs_dir / "core_full.db"

    try:
        create_staging_db(staging_db)
        create_cleansing_db(cleansing_db)

        fast_process = run_pipeline_phase(
            phase="fast",
            project_root=project_root,
            pipeline_script=pipeline_script,
            staging_db=staging_db,
            cleansing_db=cleansing_db,
            output_db=output_fast_db,
            report_dir=reports_fast_dir,
            views_sql=views_sql,
            assertions_sql=assertions_sql,
            with_fast_filters=True,
        )
        if fast_process.returncode != 0:
            print("Fast phase stdout:", file=sys.stderr)
            print(fast_process.stdout, file=sys.stderr)
            print("Fast phase stderr:", file=sys.stderr)
            print(fast_process.stderr, file=sys.stderr)
        fast_metadata = validate_fast_phase(
            process=fast_process,
            report_dir=reports_fast_dir,
            output_db=output_fast_db,
        )

        full_process = run_pipeline_phase(
            phase="full",
            project_root=project_root,
            pipeline_script=pipeline_script,
            staging_db=staging_db,
            cleansing_db=cleansing_db,
            output_db=output_full_db,
            report_dir=reports_full_dir,
            views_sql=views_sql,
            assertions_sql=assertions_sql,
            with_fast_filters=False,
        )
        if full_process.returncode != 0:
            print("Full phase stdout:", file=sys.stderr)
            print(full_process.stdout, file=sys.stderr)
            print("Full phase stderr:", file=sys.stderr)
            print(full_process.stderr, file=sys.stderr)
        full_metadata = validate_full_phase(
            process=full_process,
            report_dir=reports_full_dir,
            output_db=output_full_db,
        )

        expect(
            int(fast_metadata["row_count_market_ticks"]) < int(full_metadata["row_count_market_ticks"]),
            "Expected fast market row count to be less than full row count.",
        )
        expect(
            int(fast_metadata["row_count_cleansed_market"]) < int(full_metadata["row_count_cleansed_market"]),
            "Expected fast cleansing row count to be less than full row count.",
        )

        print("Pipeline integration smoke test PASSED.")
        print(f"- work_dir: {work_dir}")
        print("- phases: fast=0, full=0")
        print("- reports: validated fast/full JSON payload contracts")
        print("- build metadata: validated scoped vs unscoped row counts")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"Pipeline integration smoke test FAILED: {exc}", file=sys.stderr)
        print(f"- work_dir: {work_dir}", file=sys.stderr)
        return 1
    finally:
        if created_temp_dir and not args.keep_work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
