#!/usr/bin/env python3
"""
Run operational Core pipeline phases (fast/full) as one command.

Phase behavior:
- fast:
  - runs build_core_db.py with optional scope filters
  - runs core_validation_runner.py with same filters and --skip-view-row-counts
- full:
  - runs build_core_db.py without scope filters
  - runs core_validation_runner.py without scope filters and with view row counts
- both:
  - runs fast first, then full
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence


@dataclass
class StepResult:
    phase: str
    step_name: str
    exit_code: int
    json_report_path: str | None = None
    markdown_report_path: str | None = None


def parse_args(project_root: Path) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Core build+validate pipeline phases (fast/full/both).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--phase",
        choices=["fast", "full", "both"],
        default="both",
        help="Pipeline phase to execute.",
    )
    parser.add_argument(
        "--staging-db",
        required=True,
        help="Path to staging SQLite DB.",
    )
    parser.add_argument(
        "--cleansing-db",
        required=True,
        help="Path to cleansing SQLite DB.",
    )
    parser.add_argument(
        "--output-db",
        default=str(project_root / "data" / "core" / "core_kpi.db"),
        help="Output Core DB path used by build phase(s).",
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
        "--validation-output-dir",
        default=str(project_root / "logs" / "core_validation"),
        help="Directory for validation reports.",
    )
    parser.add_argument(
        "--report-prefix",
        default="core_validation",
        help="Base report prefix. Suffix _fast/_full is added per phase.",
    )
    parser.add_argument(
        "--kpi-date",
        default=None,
        help="Optional scope date (YYYY-MM-DD) used in fast phase.",
    )
    parser.add_argument(
        "--cleansing-run-id",
        default=None,
        help="Optional run_id scope used in fast phase.",
    )
    parser.add_argument(
        "--overwrite",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Pass overwrite behavior to build phase.",
    )
    parser.add_argument(
        "--vacuum",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Pass vacuum behavior to build phase.",
    )
    parser.add_argument(
        "--fail-on-error",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Pass fail-on-error behavior to validation phase.",
    )
    return parser.parse_args()


def resolve_path(raw_path: str) -> Path:
    return Path(raw_path).expanduser().resolve()


def parse_report_paths(stdout_text: str) -> tuple[str | None, str | None]:
    json_match = re.search(r"^JSON report:\s+(.+)$", stdout_text, re.MULTILINE)
    md_match = re.search(r"^Markdown report:\s+(.+)$", stdout_text, re.MULTILINE)
    return (
        json_match.group(1).strip() if json_match else None,
        md_match.group(1).strip() if md_match else None,
    )


def run_step(*, phase: str, step_name: str, command: Sequence[str], cwd: Path) -> StepResult:
    print(f"[{phase}] {step_name} command:")
    print("  " + " ".join(command))
    result = subprocess.run(
        command,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout:
        print(result.stdout.rstrip())
    if result.stderr:
        print(result.stderr.rstrip(), file=sys.stderr)

    json_report, md_report = parse_report_paths(result.stdout)
    return StepResult(
        phase=phase,
        step_name=step_name,
        exit_code=int(result.returncode),
        json_report_path=json_report,
        markdown_report_path=md_report,
    )


def build_phase_commands(
    *,
    phase: str,
    python_exe: str,
    build_script: Path,
    validation_script: Path,
    staging_db: Path,
    cleansing_db: Path,
    output_db: Path,
    views_sql: Path,
    assertions_sql: Path,
    validation_output_dir: Path,
    report_prefix: str,
    overwrite: bool,
    vacuum: bool,
    fail_on_error: bool,
    kpi_date: str | None,
    cleansing_run_id: str | None,
) -> tuple[list[str], list[str]]:
    build_cmd = [
        python_exe,
        str(build_script),
        "--staging-db",
        str(staging_db),
        "--cleansing-db",
        str(cleansing_db),
        "--output-db",
        str(output_db),
        "--views-sql",
        str(views_sql),
        "--overwrite" if overwrite else "--no-overwrite",
        "--vacuum" if vacuum else "--no-vacuum",
    ]

    validation_cmd = [
        python_exe,
        str(validation_script),
        "--staging-db",
        str(staging_db),
        "--cleansing-db",
        str(cleansing_db),
        "--views-sql",
        str(views_sql),
        "--assertions-sql",
        str(assertions_sql),
        "--output-dir",
        str(validation_output_dir),
        "--report-prefix",
        f"{report_prefix}_{phase}",
        "--fail-on-error" if fail_on_error else "--no-fail-on-error",
    ]

    if phase == "fast":
        if kpi_date:
            build_cmd.extend(["--kpi-date", kpi_date])
            validation_cmd.extend(["--kpi-date", kpi_date])
        if cleansing_run_id:
            build_cmd.extend(["--cleansing-run-id", cleansing_run_id])
            validation_cmd.extend(["--cleansing-run-id", cleansing_run_id])
        validation_cmd.append("--skip-view-row-counts")

    return build_cmd, validation_cmd


def run_phase(
    *,
    phase: str,
    project_root: Path,
    python_exe: str,
    build_script: Path,
    validation_script: Path,
    staging_db: Path,
    cleansing_db: Path,
    output_db: Path,
    views_sql: Path,
    assertions_sql: Path,
    validation_output_dir: Path,
    report_prefix: str,
    overwrite: bool,
    vacuum: bool,
    fail_on_error: bool,
    kpi_date: str | None,
    cleansing_run_id: str | None,
) -> tuple[int, list[StepResult]]:
    print(f"=== Phase: {phase.upper()} ===")

    build_cmd, validation_cmd = build_phase_commands(
        phase=phase,
        python_exe=python_exe,
        build_script=build_script,
        validation_script=validation_script,
        staging_db=staging_db,
        cleansing_db=cleansing_db,
        output_db=output_db,
        views_sql=views_sql,
        assertions_sql=assertions_sql,
        validation_output_dir=validation_output_dir,
        report_prefix=report_prefix,
        overwrite=overwrite,
        vacuum=vacuum,
        fail_on_error=fail_on_error,
        kpi_date=kpi_date,
        cleansing_run_id=cleansing_run_id,
    )

    results: list[StepResult] = []
    build_result = run_step(phase=phase, step_name="build", command=build_cmd, cwd=project_root)
    results.append(build_result)
    if build_result.exit_code != 0:
        return build_result.exit_code, results

    validation_result = run_step(phase=phase, step_name="validate", command=validation_cmd, cwd=project_root)
    results.append(validation_result)
    if validation_result.exit_code != 0:
        return validation_result.exit_code, results

    return 0, results


def main() -> int:
    project_root = Path(__file__).resolve().parents[2]
    args = parse_args(project_root)

    staging_db = resolve_path(args.staging_db)
    cleansing_db = resolve_path(args.cleansing_db)
    output_db = resolve_path(args.output_db)
    views_sql = resolve_path(args.views_sql)
    assertions_sql = resolve_path(args.assertions_sql)
    validation_output_dir = resolve_path(args.validation_output_dir)

    build_script = (project_root / "scripts" / "4_core" / "build_core_db.py").resolve()
    validation_script = (project_root / "scripts" / "4_core" / "core_validation_runner.py").resolve()

    for required_path, label in (
        (build_script, "build script"),
        (validation_script, "validation script"),
        (staging_db, "staging DB"),
        (cleansing_db, "cleansing DB"),
        (views_sql, "views SQL"),
        (assertions_sql, "assertions SQL"),
    ):
        if not required_path.exists():
            print(f"Required {label} not found: {required_path}", file=sys.stderr)
            return 1

    phases = ["fast", "full"] if args.phase == "both" else [args.phase]
    all_results: list[StepResult] = []

    for phase in phases:
        code, phase_results = run_phase(
            phase=phase,
            project_root=project_root,
            python_exe=sys.executable,
            build_script=build_script,
            validation_script=validation_script,
            staging_db=staging_db,
            cleansing_db=cleansing_db,
            output_db=output_db,
            views_sql=views_sql,
            assertions_sql=assertions_sql,
            validation_output_dir=validation_output_dir,
            report_prefix=args.report_prefix,
            overwrite=bool(args.overwrite),
            vacuum=bool(args.vacuum),
            fail_on_error=bool(args.fail_on_error),
            kpi_date=args.kpi_date,
            cleansing_run_id=args.cleansing_run_id,
        )
        all_results.extend(phase_results)
        if code != 0:
            print(f"Pipeline aborted in phase '{phase}' with exit code {code}.", file=sys.stderr)
            for item in all_results:
                if item.json_report_path or item.markdown_report_path:
                    print(
                        f"- {item.phase}:{item.step_name} reports | "
                        f"json={item.json_report_path or '-'} | md={item.markdown_report_path or '-'}"
                    )
            return code

    print("Pipeline completed successfully.")
    for item in all_results:
        if item.json_report_path or item.markdown_report_path:
            print(
                f"- {item.phase}:{item.step_name} reports | "
                f"json={item.json_report_path or '-'} | md={item.markdown_report_path or '-'}"
            )
    return 0


if __name__ == "__main__":
    sys.exit(main())
