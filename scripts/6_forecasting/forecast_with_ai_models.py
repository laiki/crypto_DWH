#!/usr/bin/env python
"""
Load trained AI forecasting artifacts and write predictions for a selected cleansing run.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import multiprocessing
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ai_model_backends as ai
import train_staging_models as common

LOGGER_NAME = "forecast_with_ai_models"


@dataclass(frozen=True)
class AIInferenceTask:
    model_id: int
    training_run_id: str
    exchange_id: str
    symbol: str
    model_name: str
    horizon_seconds: int
    artifact_path: str
    feature_config_json: str
    cleansing_db_path: str
    cleansing_run_id: str
    batch_size: int


def log(level: str, message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {level} | {LOGGER_NAME} | {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply trained AI forecasting artifacts to a cleansing run and persist forecast rows.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--cleansing-db",
        default="data/cleansing/latest_cleansing.db",
        help="Cleansing DB path used for AI forecast inference.",
    )
    parser.add_argument(
        "--cleansing-run-id",
        default=None,
        help="Optional cleansing run_id. If omitted, latest run_id is used.",
    )
    parser.add_argument(
        "--forecast-db",
        default="data/core/core_kpi.db",
        help="SQLite DB path where trained AI model registry rows and predictions are stored.",
    )
    parser.add_argument(
        "--training-run-id",
        default=None,
        help="Optional training_run_id to use. If omitted, latest completed AI training run is used.",
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help="Optional comma-separated symbol filter for AI forecast scope.",
    )
    parser.add_argument(
        "--exchange-id",
        default=None,
        help="Optional exchange_id filter for AI forecast scope.",
    )
    parser.add_argument(
        "--limit-series",
        type=int,
        default=None,
        help="Optional maximum number of exchange/symbol series to forecast.",
    )
    parser.add_argument(
        "--model-backends",
        default="chronos2",
        help="Comma-separated AI backend names to load from the forecast registry.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=32,
        help="Maximum number of per-series forecast windows passed to the backend in one batch.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for per-model AI inference tasks.",
    )
    parser.add_argument(
        "--replace-existing",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Delete existing forecast rows for the selected training run and cleansing run before inserting new ones.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable periodic progress and ETA logging.",
    )
    parser.add_argument(
        "--progress-interval-seconds",
        type=int,
        default=20,
        help="Minimum interval between periodic progress logs.",
    )
    parser.add_argument(
        "--log-model-details",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log per-model completion lines.",
    )
    parser.add_argument(
        "--log-skip-details",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log skipped or failed inference details.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.limit_series is not None and args.limit_series <= 0:
        raise SystemExit("--limit-series must be > 0 when set.")
    if args.batch_size <= 0:
        raise SystemExit("--batch-size must be > 0.")
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0.")
    if args.progress_interval_seconds <= 0:
        raise SystemExit("--progress-interval-seconds must be > 0.")


def resolve_training_run_id(connection: sqlite3.Connection, requested: str | None, backend_names: list[str]) -> str:
    if requested:
        row = connection.execute(
            "SELECT training_run_id FROM forecast_training_runs WHERE training_run_id = ? LIMIT 1",
            (requested,),
        ).fetchone()
        if row is None:
            raise SystemExit(f"Training run_id not found: {requested}")
        return str(row[0])

    backend_placeholders = ",".join("?" for _ in backend_names)
    row = connection.execute(
        f"""
        SELECT fmr.training_run_id
        FROM forecast_model_registry AS fmr
        JOIN forecast_training_runs AS ftr
          ON ftr.training_run_id = fmr.training_run_id
        WHERE ftr.status = 'completed'
          AND fmr.status = 'trained'
          AND lower(fmr.model_name) IN ({backend_placeholders})
        GROUP BY fmr.training_run_id
        ORDER BY MAX(ftr.started_utc) DESC, fmr.training_run_id DESC
        LIMIT 1
        """,
        [name.lower() for name in backend_names],
    ).fetchone()
    if row is None:
        raise SystemExit("No completed AI training_run_id found in forecast_training_runs.")
    return str(row[0])


def load_inference_tasks(
    connection: sqlite3.Connection,
    *,
    training_run_id: str,
    backend_names: list[str],
    cleansing_db_path: Path,
    cleansing_run_id: str,
    exchange_id_filter: str | None,
    symbol_filters: list[str],
    limit_series: int | None,
    batch_size: int,
) -> list[AIInferenceTask]:
    conditions = [
        "training_run_id = ?",
        "status = 'trained'",
    ]
    params: list[Any] = [training_run_id]
    backend_placeholders = ",".join("?" for _ in backend_names)
    conditions.append(f"lower(model_name) IN ({backend_placeholders})")
    params.extend([name.lower() for name in backend_names])
    if exchange_id_filter:
        conditions.append("lower(exchange_id) = ?")
        params.append(exchange_id_filter.lower())
    symbol_clause, symbol_params = common.build_case_insensitive_symbol_clause(
        column_name="symbol",
        symbols=symbol_filters,
    )
    if symbol_clause:
        conditions.append(symbol_clause)
        params.extend(symbol_params)

    rows = connection.execute(
        f"""
        SELECT
            model_id,
            training_run_id,
            exchange_id,
            symbol,
            model_name,
            horizon_seconds,
            artifact_path,
            feature_config_json
        FROM forecast_model_registry
        WHERE {' AND '.join(conditions)}
        ORDER BY exchange_id ASC, symbol ASC, model_name ASC, horizon_seconds ASC, model_id ASC
        """,
        params,
    ).fetchall()

    tasks: list[AIInferenceTask] = []
    allowed_pairs: set[tuple[str, str]] | None = None
    if limit_series is not None:
        allowed_pairs = set()
        for row in rows:
            key = (str(row[2]), str(row[3]))
            if key not in allowed_pairs and len(allowed_pairs) >= limit_series:
                continue
            allowed_pairs.add(key)

    for row in rows:
        exchange_id = str(row[2])
        symbol = str(row[3])
        if allowed_pairs is not None and (exchange_id, symbol) not in allowed_pairs:
            continue
        tasks.append(
            AIInferenceTask(
                model_id=int(row[0]),
                training_run_id=str(row[1]),
                exchange_id=exchange_id,
                symbol=symbol,
                model_name=str(row[4]),
                horizon_seconds=int(row[5]),
                artifact_path=str(row[6]),
                feature_config_json=str(row[7]),
                cleansing_db_path=str(cleansing_db_path),
                cleansing_run_id=cleansing_run_id,
                batch_size=int(batch_size),
            )
        )
    return tasks


def delete_existing_predictions(
    connection: sqlite3.Connection,
    *,
    training_run_id: str,
    cleansing_run_id: str,
    exchange_id_filter: str | None,
    symbol_filters: list[str],
    backend_names: list[str],
) -> None:
    conditions = [
        "training_run_id = ?",
        "cleansing_run_id = ?",
    ]
    params: list[Any] = [training_run_id, cleansing_run_id]
    backend_placeholders = ",".join("?" for _ in backend_names)
    conditions.append(f"lower(model_name) IN ({backend_placeholders})")
    params.extend([name.lower() for name in backend_names])
    if exchange_id_filter:
        conditions.append("lower(exchange_id) = ?")
        params.append(exchange_id_filter.lower())
    symbol_clause, symbol_params = common.build_case_insensitive_symbol_clause(
        column_name="symbol",
        symbols=symbol_filters,
    )
    if symbol_clause:
        conditions.append(symbol_clause)
        params.extend(symbol_params)
    connection.execute(f"DELETE FROM forecast_predictions WHERE {' AND '.join(conditions)}", params)
    connection.commit()


def process_inference_task(task: AIInferenceTask) -> dict[str, Any]:
    result: dict[str, Any] = {
        "model_id": task.model_id,
        "training_run_id": task.training_run_id,
        "exchange_id": task.exchange_id,
        "symbol": task.symbol,
        "model_name": task.model_name,
        "horizon_seconds": task.horizon_seconds,
        "status": "failed",
        "prediction_rows": [],
        "error_message": None,
        "row_count": 0,
    }

    artifact_metadata = json.loads(task.feature_config_json)
    artifact_path = Path(task.artifact_path)
    if artifact_path.is_file():
        loaded_artifact = ai.load_artifact(artifact_path)
        artifact_metadata = dict(loaded_artifact.get("metadata", artifact_metadata))

    backend_name = str(artifact_metadata.get("backend_name", task.model_name)).lower()
    backend = ai.get_backend(backend_name)

    with sqlite3.connect(f"file:{task.cleansing_db_path}?mode=ro", uri=True) as cleansing_connection:
        cleansing_series = common.load_cleansing_series(
            cleansing_connection,
            cleansing_run_id=task.cleansing_run_id,
            exchange_id=task.exchange_id,
            symbol=task.symbol,
        )
    if cleansing_series.empty:
        result["status"] = "skipped"
        result["error_message"] = "no_cleansing_rows"
        return result

    try:
        prediction_points = backend.predict_points(
            cleansing_series,
            artifact_payload=artifact_metadata,
            batch_size=int(task.batch_size),
        )
    except Exception as exc:  # noqa: BLE001
        result["status"] = "failed"
        result["error_message"] = f"backend_inference_failed:{exc!r}"
        return result

    if not prediction_points:
        result["status"] = "skipped"
        result["error_message"] = "no_inference_rows"
        return result

    generated_at_utc = common.utc_now_iso()
    prediction_rows: list[tuple[Any, ...]] = []
    for point in prediction_points:
        prediction_rows.append(
            (
                task.model_id,
                task.training_run_id,
                task.cleansing_run_id,
                task.exchange_id,
                task.symbol,
                task.model_name,
                int(task.horizon_seconds),
                point.generated_bucket_start_utc,
                int(point.generated_bucket_epoch_s),
                point.target_bucket_start_utc,
                int(point.target_bucket_epoch_s),
                float(point.predicted_price),
                float(point.actual_price) if point.actual_price is not None else None,
                float(point.abs_error) if point.abs_error is not None else None,
                generated_at_utc,
            )
        )

    result["status"] = "completed"
    result["prediction_rows"] = prediction_rows
    result["row_count"] = len(prediction_rows)
    return result


def log_progress(
    *,
    total_tasks: int,
    processed_tasks: int,
    completed_tasks: int,
    failed_tasks: int,
    skipped_tasks: int,
    predictions_written: int,
    started_monotonic: float,
    last_item: str,
) -> None:
    elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
    task_pct = (processed_tasks / total_tasks * 100.0) if total_tasks > 0 else 0.0
    eta_seconds = common.estimate_eta_seconds(
        elapsed_seconds=elapsed_seconds,
        completed_items=processed_tasks,
        total_items=total_tasks,
    )
    eta_txt = common.format_hhmmss(eta_seconds) if eta_seconds is not None else "n/a"
    log(
        "INFO",
        f"Progress models {processed_tasks}/{total_tasks} ({task_pct:.1f}%) | "
        f"completed={completed_tasks} failed={failed_tasks} skipped={skipped_tasks} "
        f"predictions={predictions_written} | "
        f"elapsed={common.format_hhmmss(elapsed_seconds)} eta={eta_txt} | "
        f"last_item={last_item}",
    )


def main() -> None:
    args = parse_args()
    validate_args(args)
    backend_names = ai.parse_backend_names(args.model_backends)
    if ai.MISSING_BACKEND_DEPENDENCY_MESSAGE is not None:
        raise SystemExit(ai.MISSING_BACKEND_DEPENDENCY_MESSAGE)

    symbol_filters = common.parse_csv_values(args.symbols)
    cleansing_db_path = common.resolve_file(args.cleansing_db)
    if not cleansing_db_path.is_file():
        raise SystemExit(f"Cleansing DB does not exist: {cleansing_db_path}")
    forecast_db_path = common.resolve_file(args.forecast_db)
    if not forecast_db_path.is_file():
        raise SystemExit(f"Forecast DB does not exist: {forecast_db_path}")

    with sqlite3.connect(str(cleansing_db_path)) as cleansing_conn:
        cleansing_run_id = common.resolve_cleansing_run_id(cleansing_conn, args.cleansing_run_id)
        row = cleansing_conn.execute(
            "SELECT COUNT(*) FROM cleansed_market WHERE run_id = ?",
            (cleansing_run_id,),
        ).fetchone()
        cleansing_row_count = int(row[0]) if row is not None and row[0] is not None else 0

    forecast_conn = sqlite3.connect(str(forecast_db_path))
    try:
        common.ensure_forecast_tables(forecast_conn)
        training_run_id = resolve_training_run_id(forecast_conn, args.training_run_id, backend_names)
        tasks = load_inference_tasks(
            forecast_conn,
            training_run_id=training_run_id,
            backend_names=backend_names,
            cleansing_db_path=cleansing_db_path,
            cleansing_run_id=cleansing_run_id,
            exchange_id_filter=args.exchange_id,
            symbol_filters=symbol_filters,
            limit_series=args.limit_series,
            batch_size=int(args.batch_size),
        )
        if not tasks:
            raise SystemExit("No trained AI models found for selected filters.")

        if args.replace_existing:
            delete_existing_predictions(
                forecast_conn,
                training_run_id=training_run_id,
                cleansing_run_id=cleansing_run_id,
                exchange_id_filter=args.exchange_id,
                symbol_filters=symbol_filters,
                backend_names=backend_names,
            )

        unique_pairs = sorted({(task.exchange_id, task.symbol) for task in tasks})
        log("INFO", f"Training run: {training_run_id}")
        log("INFO", f"Cleansing run: {cleansing_run_id}")
        log("INFO", f"AI backends: {backend_names}")
        log("INFO", f"Cleansing rows: {cleansing_row_count}")
        log("INFO", f"Forecast tasks: {len(tasks)}")
        log("INFO", f"Forecast pairs: {len(unique_pairs)}")
        log("INFO", f"Replace existing predictions: {args.replace_existing}")
        log("INFO", f"Worker processes: {args.workers}")

        processed_tasks = 0
        completed_tasks = 0
        failed_tasks = 0
        skipped_tasks = 0
        predictions_written = 0
        progress_started_monotonic = time.monotonic()
        last_progress_logged_monotonic = progress_started_monotonic - float(args.progress_interval_seconds)
        last_completed_item = "-"

        def maybe_log_progress(*, force: bool = False) -> None:
            nonlocal last_progress_logged_monotonic
            if not args.progress:
                return
            now = time.monotonic()
            if not force and (now - last_progress_logged_monotonic) < float(args.progress_interval_seconds):
                return
            last_progress_logged_monotonic = now
            log_progress(
                total_tasks=len(tasks),
                processed_tasks=processed_tasks,
                completed_tasks=completed_tasks,
                failed_tasks=failed_tasks,
                skipped_tasks=skipped_tasks,
                predictions_written=predictions_written,
                started_monotonic=progress_started_monotonic,
                last_item=last_completed_item,
            )

        maybe_log_progress(force=True)

        def handle_result(result: dict[str, Any]) -> None:
            nonlocal processed_tasks, completed_tasks, failed_tasks, skipped_tasks
            nonlocal predictions_written, last_completed_item
            processed_tasks += 1
            last_completed_item = (
                f"{result.get('exchange_id', '-')}:"
                f"{result.get('symbol', '-')}:"
                f"{result.get('model_name', '-')}:"
                f"h{result.get('horizon_seconds', '-')}"
            )
            status = str(result.get("status", "failed"))
            if status == "completed":
                completed_tasks += 1
            elif status == "skipped":
                skipped_tasks += 1
            else:
                failed_tasks += 1

            prediction_rows = result.get("prediction_rows") or []
            if prediction_rows:
                forecast_conn.executemany(
                    """
                    INSERT INTO forecast_predictions (
                        model_id,
                        training_run_id,
                        cleansing_run_id,
                        exchange_id,
                        symbol,
                        model_name,
                        horizon_seconds,
                        forecast_generated_bucket_start_utc,
                        forecast_generated_bucket_epoch_s,
                        target_bucket_start_utc,
                        target_bucket_epoch_s,
                        predicted_price,
                        actual_price,
                        abs_error,
                        generated_at_utc
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    prediction_rows,
                )
                forecast_conn.commit()
                predictions_written += len(prediction_rows)

            if args.log_model_details or (args.log_skip_details and status != "completed"):
                log(
                    "INFO" if status == "completed" else "WARNING",
                    f"AI inference {status} exchange={result.get('exchange_id')} symbol={result.get('symbol')} "
                    f"model={result.get('model_name')} horizon_s={result.get('horizon_seconds')} "
                    f"rows={len(prediction_rows)} error={result.get('error_message')}",
                )
            maybe_log_progress()

        if args.workers == 1:
            for task in tasks:
                handle_result(process_inference_task(task))
        else:
            ctx = multiprocessing.get_context("spawn")
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=int(args.workers),
                mp_context=ctx,
            ) as executor:
                for result in executor.map(process_inference_task, tasks):
                    handle_result(result)

        maybe_log_progress(force=True)
    finally:
        forecast_conn.close()

    log(
        "INFO",
        f"AI forecast run finished training_run_id={training_run_id} cleansing_run_id={cleansing_run_id} "
        f"forecast_db={forecast_db_path} predictions_written={predictions_written}",
    )


if __name__ == "__main__":
    main()
