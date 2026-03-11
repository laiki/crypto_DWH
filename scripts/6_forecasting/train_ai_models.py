#!/usr/bin/env python
"""
Train AI forecasting backends from staging history and register model artifacts.

The script is backend-extensible. Chronos2 is the first implemented backend.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import multiprocessing
import sqlite3
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import ai_model_backends as ai
import train_staging_models as common

LOGGER_NAME = "train_ai_models"


@dataclass(frozen=True)
class AIPairWorkerTask:
    exchange_id: str
    symbol: str
    staging_db_paths: tuple[str, ...]
    training_cutoff_utc: str
    bin_seconds: int
    horizons_steps: tuple[int, ...]
    horizons_seconds: tuple[int, ...]
    backend_names: tuple[str, ...]
    pretrained_model_id: str
    device: str
    min_context_points: int
    max_context_points: int
    cv_windows: int
    test_size: float
    training_run_id: str
    model_dir: str


def log(level: str, message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {level} | {LOGGER_NAME} | {message}", flush=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Train AI forecasting backends from staging history and register "
            "artifact metadata in the forecast DB."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--staging-db",
        nargs="+",
        default=["data/staging/latest_staging.db"],
        help="Staging DB inputs used for AI training. Accepts one or more .db files, directories, or glob patterns.",
    )
    parser.add_argument(
        "--staging-db-exclude",
        nargs="+",
        default=None,
        help="Optional staging DB inputs to exclude after resolution.",
    )
    parser.add_argument(
        "--training-cutoff-utc",
        default=None,
        help="Optional UTC cutoff for staging history. Only rows before this timestamp are used.",
    )
    parser.add_argument(
        "--forecast-db",
        default="data/core/core_kpi.db",
        help="SQLite DB path where AI training registry rows are written.",
    )
    parser.add_argument(
        "--model-dir",
        default="data/forecasting/models",
        help="Directory for persisted AI model artifacts.",
    )
    parser.add_argument(
        "--bin-seconds",
        type=int,
        default=60,
        help="Training cadence in seconds used for staging resampling.",
    )
    parser.add_argument(
        "--secondary-horizon-multiple",
        type=int,
        default=30,
        help="Secondary forecast horizon as integer multiple of bin-seconds.",
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help="Optional comma-separated symbol filter for AI training scope.",
    )
    parser.add_argument(
        "--exchange-id",
        default=None,
        help="Optional exchange_id filter for AI training scope.",
    )
    parser.add_argument(
        "--limit-series",
        type=int,
        default=None,
        help="Optional maximum number of exchange/symbol series to process.",
    )
    parser.add_argument(
        "--model-backends",
        default="chronos2",
        help="Comma-separated AI backend names to evaluate and register.",
    )
    parser.add_argument(
        "--chronos-model-id",
        default="amazon/chronos-2",
        help="Pretrained Chronos2 model identifier.",
    )
    parser.add_argument(
        "--device",
        default="auto",
        help="AI runtime device. Use auto, cpu, cuda, or mps.",
    )
    parser.add_argument(
        "--min-context-points",
        type=int,
        default=60,
        help="Minimum number of resampled history points required before AI inference/training starts.",
    )
    parser.add_argument(
        "--max-context-points",
        type=int,
        default=512,
        help="Maximum number of context points passed to the AI backend per forecast window.",
    )
    parser.add_argument(
        "--cv-windows",
        type=int,
        default=3,
        help="Maximum number of rolling backtest windows used for CV-style evaluation.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Tail fraction reserved for holdout evaluation.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for per-pair AI training tasks.",
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
        "--log-pair-details",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log per-pair start lines.",
    )
    parser.add_argument(
        "--log-skip-details",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log skipped or failed AI model lines.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.secondary_horizon_multiple <= 1:
        raise SystemExit("--secondary-horizon-multiple must be > 1.")
    if args.bin_seconds <= 0:
        raise SystemExit("--bin-seconds must be > 0.")
    if args.limit_series is not None and args.limit_series <= 0:
        raise SystemExit("--limit-series must be > 0 when set.")
    if not 0.01 <= args.test_size <= 0.5:
        raise SystemExit("--test-size must be within [0.01, 0.5].")
    if args.min_context_points < 30:
        raise SystemExit("--min-context-points must be >= 30.")
    if args.max_context_points < args.min_context_points:
        raise SystemExit("--max-context-points must be >= --min-context-points.")
    if args.cv_windows <= 0:
        raise SystemExit("--cv-windows must be > 0.")
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0.")
    if args.progress_interval_seconds <= 0:
        raise SystemExit("--progress-interval-seconds must be > 0.")


def process_pair_task(task: AIPairWorkerTask) -> dict[str, Any]:
    models_per_pair = len(task.horizons_steps) * len(task.backend_names)
    result: dict[str, Any] = {
        "exchange_id": task.exchange_id,
        "symbol": task.symbol,
        "pair_key": f"{task.exchange_id}:{task.symbol}",
        "pair_trained_models": 0,
        "model_slots_processed": 0,
        "model_slots_failed": 0,
        "model_slots_skipped": 0,
        "skipped_series_item": None,
        "model_results": [],
    }

    staging_raw = common.load_staging_series_before_cutoff(
        [Path(path_raw) for path_raw in task.staging_db_paths],
        exchange_id=task.exchange_id,
        symbol=task.symbol,
        cutoff_utc=task.training_cutoff_utc,
    )
    staging_resampled = common.resample_staging_series(staging_raw, bin_seconds=int(task.bin_seconds))
    if len(staging_resampled) < int(task.min_context_points) + max(task.horizons_steps):
        result["skipped_series_item"] = {
            "exchange_id": task.exchange_id,
            "symbol": task.symbol,
            "reason": f"insufficient_staging_rows:{len(staging_resampled)}",
        }
        result["model_slots_processed"] = models_per_pair
        result["model_slots_skipped"] = models_per_pair
        return result

    for horizon_step, horizon_seconds in zip(task.horizons_steps, task.horizons_seconds):
        for backend_name in task.backend_names:
            backend = ai.get_backend(backend_name)
            config = ai.AITrainingConfig(
                model_name=backend_name,
                pretrained_model_id=task.pretrained_model_id,
                device=task.device,
                min_context_points=int(task.min_context_points),
                max_context_points=int(task.max_context_points),
                cv_windows=int(task.cv_windows),
                test_size=float(task.test_size),
                bin_seconds=int(task.bin_seconds),
                horizon_steps=int(horizon_step),
                horizon_seconds=int(horizon_seconds),
            )
            backend_result = backend.train_and_evaluate(staging_resampled, config=config)
            artifact_path = common.make_model_artifact_path(
                model_dir=Path(task.model_dir),
                training_run_id=task.training_run_id,
                exchange_id=task.exchange_id,
                symbol=task.symbol,
                model_name=backend_name,
                horizon_seconds=int(horizon_seconds),
            )
            if backend_result.status == "trained":
                ai.save_artifact(
                    artifact_path,
                    payload={
                        "backend_name": backend_name,
                        "metadata": backend_result.artifact_payload,
                    },
                )
                result["pair_trained_models"] += 1
            elif backend_result.status == "failed":
                result["model_slots_failed"] += 1
            else:
                result["model_slots_skipped"] += 1

            result["model_slots_processed"] += 1
            result["model_results"].append(
                {
                    "status": backend_result.status,
                    "exchange_id": task.exchange_id,
                    "symbol": task.symbol,
                    "model_name": backend_name,
                    "horizon_steps": int(horizon_step),
                    "horizon_seconds": int(horizon_seconds),
                    "artifact_path": str(artifact_path),
                    "feature_config": backend_result.artifact_payload,
                    "train_rows": int(backend_result.train_rows),
                    "test_rows": int(backend_result.test_rows),
                    "cv_fold_count": int(backend_result.cv_fold_count),
                    "holdout_metrics": backend_result.holdout_metrics,
                    "cv_mean_metrics": backend_result.cv_mean_metrics,
                    "prediction_rows": [],
                    "error_message": backend_result.error_message,
                }
            )

    return result


def log_progress(
    *,
    total_pairs: int,
    processed_pairs: int,
    total_model_slots: int,
    processed_model_slots: int,
    trained_models: int,
    failed_models: int,
    skipped_model_slots: int,
    started_monotonic: float,
    last_pair: str,
) -> None:
    elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
    pair_pct = (processed_pairs / total_pairs * 100.0) if total_pairs > 0 else 0.0
    model_pct = (processed_model_slots / total_model_slots * 100.0) if total_model_slots > 0 else 0.0
    eta_seconds = common.estimate_eta_seconds(
        elapsed_seconds=elapsed_seconds,
        completed_items=processed_pairs,
        total_items=total_pairs,
    )
    eta_txt = common.format_hhmmss(eta_seconds) if eta_seconds is not None else "n/a"
    log(
        "INFO",
        "Progress "
        f"pairs {processed_pairs}/{total_pairs} ({pair_pct:.1f}%) | "
        f"model_slots {processed_model_slots}/{total_model_slots} ({model_pct:.1f}%) | "
        f"trained={trained_models} failed={failed_models} skipped={skipped_model_slots} | "
        f"elapsed={common.format_hhmmss(elapsed_seconds)} eta={eta_txt} | "
        f"last_pair={last_pair}",
    )


def main() -> None:
    args = parse_args()
    validate_args(args)
    backend_names = ai.parse_backend_names(args.model_backends)
    if ai.MISSING_BACKEND_DEPENDENCY_MESSAGE is not None:
        raise SystemExit(ai.MISSING_BACKEND_DEPENDENCY_MESSAGE)

    symbol_filters = common.parse_csv_values(args.symbols)
    staging_db_paths = common.resolve_staging_db_paths(args.staging_db)
    excluded_staging_db_paths = (
        common.resolve_staging_db_paths(args.staging_db_exclude, allow_empty=True)
        if args.staging_db_exclude
        else []
    )
    excluded_staging_db_set = set(excluded_staging_db_paths)
    staging_db_paths = [path for path in staging_db_paths if path not in excluded_staging_db_set]
    if not staging_db_paths:
        raise SystemExit(
            "No staging DB files remain after applying --staging-db-exclude. "
            "Adjust include or exclude inputs."
        )

    forecast_db_path = common.resolve_file(args.forecast_db)
    forecast_db_path.parent.mkdir(parents=True, exist_ok=True)
    model_dir = common.resolve_file(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    staging_min_utc, staging_max_utc = common.summarize_staging_time_window(staging_db_paths)
    if staging_min_utc is None:
        raise SystemExit("No staging ingestion timestamps found in market_ticks. Cannot train AI forecasting models.")

    training_cutoff_utc = common.resolve_training_cutoff_utc(
        requested_cutoff_utc=args.training_cutoff_utc,
        staging_max_utc=staging_max_utc,
    )
    bin_seconds = int(args.bin_seconds)
    horizons_steps = [1, int(args.secondary_horizon_multiple)]
    horizons_seconds = [int(step) * bin_seconds for step in horizons_steps]
    preflight = common.estimate_max_training_rows(
        staging_min_utc=staging_min_utc,
        training_cutoff_utc=training_cutoff_utc,
        bin_seconds=bin_seconds,
        lag_steps=[int(args.min_context_points)],
        rolling_windows=[],
        horizons_steps=horizons_steps,
    )
    if preflight["max_trainable_rows"] < int(args.min_context_points):
        raise SystemExit(
            "AI training preflight failed: available staging history cannot satisfy minimum context requirements. "
            f"history_hours={preflight['available_seconds'] / 3600.0:.2f}, "
            f"max_resampled_rows={preflight['max_resampled_rows']}, "
            f"max_trainable_rows={preflight['max_trainable_rows']}, "
            f"required_min_context_points={args.min_context_points}."
        )

    scope_pairs = common.load_scope_pairs_from_staging(
        staging_db_paths,
        exchange_id_filter=args.exchange_id,
        symbol_filters=symbol_filters,
        training_cutoff_utc=training_cutoff_utc,
    )
    if args.limit_series is not None:
        scope_pairs = scope_pairs[: int(args.limit_series)]
    if not scope_pairs:
        raise SystemExit("No exchange/symbol scope pairs found for selected AI training filters.")

    log("INFO", f"Training cutoff UTC: {training_cutoff_utc}")
    log("INFO", f"Bin seconds: {bin_seconds}")
    log("INFO", f"AI backends: {backend_names}")
    log("INFO", f"Scope pairs: {len(scope_pairs)}")
    log("INFO", f"Staging inputs: {', '.join(args.staging_db)}")
    log("INFO", f"Resolved staging DB count: {len(staging_db_paths)}")
    if excluded_staging_db_paths:
        log("INFO", f"Resolved excluded staging DB count: {len(excluded_staging_db_paths)}")
    log("INFO", f"Staging time window UTC: min={staging_min_utc or '-'} max={staging_max_utc or '-'}")
    log("INFO", f"Training horizons seconds: {horizons_seconds}")

    training_run_id = "forecast_ai_train_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")

    forecast_conn = sqlite3.connect(str(forecast_db_path))
    try:
        common.ensure_forecast_tables(forecast_conn)
        common.insert_training_run_start(
            forecast_conn,
            training_run_id=training_run_id,
            args=args,
            staging_db_count=len(staging_db_paths),
            staging_input=" ".join(args.staging_db),
            cleansing_db_path=Path("-"),
            cleansing_run_id="-",
            forecast_db_path=forecast_db_path,
            training_cutoff_utc=training_cutoff_utc,
            bin_seconds=bin_seconds,
            lag_steps=[int(args.min_context_points)],
            rolling_windows=[],
            series_total=len(scope_pairs),
        )

        series_trained = 0
        models_trained = 0
        skipped_series: list[dict[str, Any]] = []
        failed_models: list[dict[str, Any]] = []
        failed_model_slots = 0
        skipped_model_slots = 0
        processed_pairs = 0
        processed_model_slots = 0
        total_pairs = len(scope_pairs)
        total_model_slots = total_pairs * len(backend_names) * len(horizons_steps)
        progress_started_monotonic = time.monotonic()
        last_progress_logged_monotonic = progress_started_monotonic - float(args.progress_interval_seconds)
        last_completed_pair = "-"

        def maybe_log_progress(*, force: bool = False) -> None:
            nonlocal last_progress_logged_monotonic
            if not args.progress:
                return
            now = time.monotonic()
            if not force and (now - last_progress_logged_monotonic) < float(args.progress_interval_seconds):
                return
            last_progress_logged_monotonic = now
            log_progress(
                total_pairs=total_pairs,
                processed_pairs=processed_pairs,
                total_model_slots=total_model_slots,
                processed_model_slots=processed_model_slots,
                trained_models=models_trained,
                failed_models=failed_model_slots,
                skipped_model_slots=skipped_model_slots,
                started_monotonic=progress_started_monotonic,
                last_pair=last_completed_pair,
            )

        maybe_log_progress(force=True)
        tasks = [
            AIPairWorkerTask(
                exchange_id=scope.exchange_id,
                symbol=scope.symbol,
                staging_db_paths=tuple(str(path) for path in staging_db_paths),
                training_cutoff_utc=training_cutoff_utc,
                bin_seconds=bin_seconds,
                horizons_steps=tuple(horizons_steps),
                horizons_seconds=tuple(horizons_seconds),
                backend_names=tuple(backend_names),
                pretrained_model_id=args.chronos_model_id,
                device=args.device,
                min_context_points=int(args.min_context_points),
                max_context_points=int(args.max_context_points),
                cv_windows=int(args.cv_windows),
                test_size=float(args.test_size),
                training_run_id=training_run_id,
                model_dir=str(model_dir),
            )
            for scope in scope_pairs
        ]

        if args.workers == 1:
            task_results = [process_pair_task(task) for task in tasks]
        else:
            ctx = multiprocessing.get_context("spawn")
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=int(args.workers),
                mp_context=ctx,
            ) as executor:
                task_results = list(executor.map(process_pair_task, tasks))

        for task_result in task_results:
            processed_pairs += 1
            last_completed_pair = str(task_result.get("pair_key", "-"))
            model_results = task_result.get("model_results", [])
            processed_model_slots += int(task_result.get("model_slots_processed", 0))
            failed_model_slots += int(task_result.get("model_slots_failed", 0))
            skipped_model_slots += int(task_result.get("model_slots_skipped", 0))
            if task_result.get("pair_trained_models", 0) > 0:
                series_trained += 1
            if task_result.get("skipped_series_item") is not None:
                skipped_series.append(task_result["skipped_series_item"])

            for model_result in model_results:
                common.insert_model_registry_row(
                    forecast_conn,
                    training_run_id=training_run_id,
                    exchange_id=str(model_result["exchange_id"]),
                    symbol=str(model_result["symbol"]),
                    model_name=str(model_result["model_name"]),
                    horizon_steps=int(model_result["horizon_steps"]),
                    horizon_seconds=int(model_result["horizon_seconds"]),
                    artifact_path=Path(str(model_result["artifact_path"])),
                    feature_config=dict(model_result["feature_config"]),
                    train_rows=int(model_result["train_rows"]),
                    test_rows=int(model_result["test_rows"]),
                    cv_fold_count=int(model_result["cv_fold_count"]),
                    holdout_metrics=dict(model_result["holdout_metrics"]),
                    cv_mean_metrics=dict(model_result["cv_mean_metrics"]),
                    status=str(model_result["status"]),
                    error_message=model_result.get("error_message"),
                )
                if str(model_result["status"]) == "trained":
                    models_trained += 1
                elif str(model_result["status"]) == "failed":
                    failed_models.append(
                        {
                            "exchange_id": model_result["exchange_id"],
                            "symbol": model_result["symbol"],
                            "model_name": model_result["model_name"],
                            "horizon_seconds": model_result["horizon_seconds"],
                            "error_message": model_result.get("error_message"),
                        }
                    )
                    if args.log_skip_details:
                        log(
                            "WARNING",
                            f"AI training failed exchange={model_result['exchange_id']} "
                            f"symbol={model_result['symbol']} model={model_result['model_name']} "
                            f"horizon_s={model_result['horizon_seconds']} error={model_result.get('error_message')}",
                        )
            maybe_log_progress()

        maybe_log_progress(force=True)
        common.update_training_run_finish(
            forecast_conn,
            training_run_id=training_run_id,
            status="completed",
            series_trained=series_trained,
            models_trained=models_trained,
            predictions_written=0,
            notes={
                "backend_names": backend_names,
                "chronos_model_id": args.chronos_model_id,
                "device": ai.resolve_device(args.device),
                "min_context_points": int(args.min_context_points),
                "max_context_points": int(args.max_context_points),
                "cv_windows": int(args.cv_windows),
                "skipped_series": skipped_series,
                "failed_models": failed_models,
                "failed_model_slots": int(failed_model_slots),
                "skipped_model_slots": int(skipped_model_slots),
            },
        )
    except Exception:
        common.update_training_run_finish(
            forecast_conn,
            training_run_id=training_run_id,
            status="failed",
            series_trained=0,
            models_trained=0,
            predictions_written=0,
            notes={"error": "training_failed"},
        )
        raise
    finally:
        forecast_conn.close()

    log(
        "INFO",
        f"AI training run finished id={training_run_id} forecast_db={forecast_db_path} model_dir={model_dir}",
    )


if __name__ == "__main__":
    main()
