#!/usr/bin/env python
"""
Load trained forecasting models and write predictions for a selected cleansing run.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import multiprocessing
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import train_staging_models as common

LOGGER_NAME = "forecast_with_trained_models"


@dataclass(frozen=True)
class ModelInferenceTask:
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


def log(level: str, message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {level} | {LOGGER_NAME} | {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Apply trained forecasting models to a cleansing run and persist forecast rows.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--cleansing-db",
        default="data/cleansing/latest_cleansing.db",
        help="Cleansing DB path used for forecast inference.",
    )
    parser.add_argument(
        "--cleansing-run-id",
        default=None,
        help="Optional cleansing run_id. If omitted, latest run_id is used.",
    )
    parser.add_argument(
        "--forecast-db",
        default="data/core/core_kpi.db",
        help="SQLite DB path where trained model registry and forecast rows are stored.",
    )
    parser.add_argument(
        "--training-run-id",
        default=None,
        help="Optional training_run_id to use. If omitted, latest completed training run is used.",
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help=(
            "Optional comma-separated symbol filter for forecast scope. "
            "Supports exact symbols and SQL LIKE patterns (%% and _), case-insensitive."
        ),
    )
    parser.add_argument(
        "--exchange-id",
        default=None,
        help="Optional exchange_id filter for forecast scope.",
    )
    parser.add_argument(
        "--limit-series",
        type=int,
        default=None,
        help="Optional maximum number of exchange/symbol series to forecast.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for per-model inference tasks.",
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
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0.")
    if args.progress_interval_seconds <= 0:
        raise SystemExit("--progress-interval-seconds must be > 0.")


def resolve_training_run_id(connection: sqlite3.Connection, requested: str | None) -> str:
    if requested:
        row = connection.execute(
            "SELECT training_run_id FROM forecast_training_runs WHERE training_run_id = ? LIMIT 1",
            (requested,),
        ).fetchone()
        if row is None:
            raise SystemExit(f"Training run_id not found: {requested}")
        return str(row[0])

    row = connection.execute(
        """
        SELECT training_run_id
        FROM forecast_training_runs
        WHERE status = 'completed'
        ORDER BY started_utc DESC, training_run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise SystemExit("No completed training_run_id found in forecast_training_runs.")
    return str(row[0])


def load_inference_tasks(
    connection: sqlite3.Connection,
    *,
    training_run_id: str,
    cleansing_db_path: Path,
    cleansing_run_id: str,
    exchange_id_filter: str | None,
    symbol_filters: list[str],
    limit_series: int | None,
) -> list[ModelInferenceTask]:
    conditions = [
        "training_run_id = ?",
        "status = 'trained'",
    ]
    params: list[Any] = [training_run_id]
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

    tasks: list[ModelInferenceTask] = []
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
            ModelInferenceTask(
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
) -> None:
    conditions = [
        "training_run_id = ?",
        "cleansing_run_id = ?",
    ]
    params: list[Any] = [training_run_id, cleansing_run_id]
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


def process_inference_task(task: ModelInferenceTask) -> dict[str, Any]:
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

    feature_config = json.loads(task.feature_config_json)
    feature_columns = [str(item) for item in feature_config.get("feature_columns", [])]
    lag_steps = [int(item) for item in feature_config.get("lag_steps", [])]
    rolling_windows = [int(item) for item in feature_config.get("rolling_windows", [])]

    if not feature_columns:
        result["status"] = "skipped"
        result["error_message"] = "no_feature_columns_in_registry"
        return result

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

    feature_table = common.build_feature_table(
        cleansing_series,
        lag_steps=lag_steps,
        rolling_windows=rolling_windows,
    )
    missing_columns = [column for column in feature_columns if column not in feature_table.columns]
    if missing_columns:
        result["status"] = "failed"
        result["error_message"] = f"missing_feature_columns:{','.join(missing_columns[:10])}"
        return result

    inference_frame = common.build_cleansing_inference_frame(
        feature_table,
        feature_columns,
        horizon_seconds=int(task.horizon_seconds),
    )
    if inference_frame.empty:
        result["status"] = "skipped"
        result["error_message"] = "no_inference_rows_after_feature_engineering"
        return result

    artifact = common.joblib.load(task.artifact_path)
    if isinstance(artifact, dict) and "model" in artifact:
        model = artifact["model"]
    else:
        model = artifact

    X_infer = inference_frame[feature_columns].astype(float)
    y_pred = model.predict(X_infer).astype(float)
    actual_price_by_epoch = dict(
        zip(
            cleansing_series["bucket_epoch_s"].astype(int).tolist(),
            cleansing_series["price"].astype(float).tolist(),
        )
    )
    generated_at_utc = common.utc_now_iso()
    prediction_rows: list[tuple[Any, ...]] = []
    for row_idx, predicted_price in enumerate(y_pred):
        generated_epoch = int(inference_frame.iloc[row_idx]["bucket_epoch_s"])
        target_epoch = int(inference_frame.iloc[row_idx]["target_bucket_epoch_s"])
        generated_utc = inference_frame.iloc[row_idx]["bucket_start_utc"].strftime("%Y-%m-%dT%H:%M:%S%z")
        target_utc = str(inference_frame.iloc[row_idx]["target_bucket_start_utc"])
        actual_price = actual_price_by_epoch.get(target_epoch)
        abs_error = abs(float(actual_price) - float(predicted_price)) if actual_price is not None else None
        prediction_rows.append(
            (
                task.model_id,
                task.training_run_id,
                task.cleansing_run_id,
                task.exchange_id,
                task.symbol,
                task.model_name,
                int(task.horizon_seconds),
                str(generated_utc),
                int(generated_epoch),
                str(target_utc),
                int(target_epoch),
                float(predicted_price),
                float(actual_price) if actual_price is not None else None,
                float(abs_error) if abs_error is not None else None,
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
    if common.MISSING_DEPENDENCY_MESSAGE is not None:
        raise SystemExit(common.MISSING_DEPENDENCY_MESSAGE)

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
        training_run_id = resolve_training_run_id(forecast_conn, args.training_run_id)
        tasks = load_inference_tasks(
            forecast_conn,
            training_run_id=training_run_id,
            cleansing_db_path=cleansing_db_path,
            cleansing_run_id=cleansing_run_id,
            exchange_id_filter=args.exchange_id,
            symbol_filters=symbol_filters,
            limit_series=args.limit_series,
        )
        if not tasks:
            raise SystemExit("No trained models found for selected filters.")

        if args.replace_existing:
            delete_existing_predictions(
                forecast_conn,
                training_run_id=training_run_id,
                cleansing_run_id=cleansing_run_id,
                exchange_id_filter=args.exchange_id,
                symbol_filters=symbol_filters,
            )

        unique_pairs = sorted({(task.exchange_id, task.symbol) for task in tasks})
        log("INFO", f"Training run: {training_run_id}")
        log("INFO", f"Cleansing run: {cleansing_run_id}")
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
        last_item = "-"
        started_monotonic = time.monotonic()
        last_progress_logged_monotonic = started_monotonic - float(args.progress_interval_seconds)

        def maybe_log_progress(*, force: bool = False) -> None:
            nonlocal last_progress_logged_monotonic
            if not args.progress:
                return
            now_monotonic = time.monotonic()
            if (not force) and (now_monotonic - last_progress_logged_monotonic < float(args.progress_interval_seconds)):
                return
            log_progress(
                total_tasks=len(tasks),
                processed_tasks=processed_tasks,
                completed_tasks=completed_tasks,
                failed_tasks=failed_tasks,
                skipped_tasks=skipped_tasks,
                predictions_written=predictions_written,
                started_monotonic=started_monotonic,
                last_item=last_item,
            )
            last_progress_logged_monotonic = now_monotonic

        def persist_result(result: dict[str, Any]) -> None:
            nonlocal processed_tasks
            nonlocal completed_tasks
            nonlocal failed_tasks
            nonlocal skipped_tasks
            nonlocal predictions_written
            nonlocal last_item

            processed_tasks += 1
            last_item = (
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
                    f"Inference {status} exchange={result.get('exchange_id')} symbol={result.get('symbol')} "
                    f"model={result.get('model_name')} horizon_s={result.get('horizon_seconds')} "
                    f"rows={len(prediction_rows)} error={result.get('error_message')}",
                )
            maybe_log_progress()

        maybe_log_progress(force=True)

        if args.workers == 1:
            for task in tasks:
                try:
                    result = process_inference_task(task)
                except Exception as exc:  # noqa: BLE001
                    result = {
                        "model_id": task.model_id,
                        "training_run_id": task.training_run_id,
                        "exchange_id": task.exchange_id,
                        "symbol": task.symbol,
                        "model_name": task.model_name,
                        "horizon_seconds": task.horizon_seconds,
                        "status": "failed",
                        "prediction_rows": [],
                        "error_message": f"worker_exception:{exc!r}",
                    }
                persist_result(result)
        else:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=int(args.workers),
                mp_context=multiprocessing.get_context("spawn"),
            ) as executor:
                future_to_task: dict[concurrent.futures.Future[dict[str, Any]], ModelInferenceTask] = {}
                for task in tasks:
                    future = executor.submit(process_inference_task, task)
                    future_to_task[future] = task
                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        result = {
                            "model_id": task.model_id,
                            "training_run_id": task.training_run_id,
                            "exchange_id": task.exchange_id,
                            "symbol": task.symbol,
                            "model_name": task.model_name,
                            "horizon_seconds": task.horizon_seconds,
                            "status": "failed",
                            "prediction_rows": [],
                            "error_message": f"worker_exception:{exc!r}",
                        }
                    persist_result(result)

        maybe_log_progress(force=True)
        log(
            "INFO",
            f"Forecast run finished training_run_id={training_run_id} cleansing_run_id={cleansing_run_id} "
            f"predictions_written={predictions_written}",
        )
    finally:
        forecast_conn.close()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
