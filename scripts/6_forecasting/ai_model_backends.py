#!/usr/bin/env python
"""
Backend registry for AI forecasting models.

Chronos2 and MOIRAI2 are implemented as pretrained zero-shot /
inference-time evaluation backends, not weight fine-tuning paths.
Additional AI backends can be added by extending BACKEND_FACTORIES
with another AIModelBackend instance.
"""

from __future__ import annotations

import math
import sys
from abc import ABC
from abc import abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import joblib
import numpy as np
import pandas as pd

import train_staging_models as common

TORCH_IMPORT_ERROR: str | None = None
try:
    import torch
except Exception as exc:  # noqa: BLE001
    missing_name = getattr(exc, "name", None)
    if missing_name:
        TORCH_IMPORT_ERROR = (
            f"Missing dependency '{missing_name}' for AI forecasting backends. "
            f"Install dependencies with '{sys.executable} -m pip install -r requirements.txt'"
        )
    else:
        TORCH_IMPORT_ERROR = f"Failed to import torch for AI forecasting backends: {exc!r}"

CHRONOS_IMPORT_ERROR: str | None = None
try:
    from chronos import BaseChronosPipeline
except Exception as exc:  # noqa: BLE001
    missing_name = getattr(exc, "name", None)
    if missing_name:
        CHRONOS_IMPORT_ERROR = (
            f"Missing dependency '{missing_name}' for Chronos2 forecasting backend. "
            f"Install dependencies with '{sys.executable} -m pip install -r requirements.txt'"
        )
    else:
        CHRONOS_IMPORT_ERROR = f"Failed to import Chronos2 backend dependencies: {exc!r}"

MOIRAI2_IMPORT_ERROR: str | None = None
try:
    from gluonts.dataset.pandas import PandasDataset
    from uni2ts.model.moirai2 import Moirai2Forecast
    from uni2ts.model.moirai2 import Moirai2Module
except Exception as exc:  # noqa: BLE001
    missing_name = getattr(exc, "name", None)
    if missing_name:
        MOIRAI2_IMPORT_ERROR = (
            f"Missing dependency '{missing_name}' for MOIRAI2 forecasting backend. "
            f"Install dependencies with '{sys.executable} -m pip install -r requirements.txt'"
        )
    else:
        MOIRAI2_IMPORT_ERROR = f"Failed to import MOIRAI2 backend dependencies: {exc!r}"

BACKEND_DEPENDENCY_ERRORS: dict[str, str | None] = {
    "chronos2": CHRONOS_IMPORT_ERROR or TORCH_IMPORT_ERROR,
    "moirai2": MOIRAI2_IMPORT_ERROR or TORCH_IMPORT_ERROR,
}


@dataclass(frozen=True)
class AITrainingConfig:
    model_name: str
    pretrained_model_id: str
    device: str
    min_context_points: int
    max_context_points: int
    cv_windows: int
    test_size: float
    bin_seconds: int
    horizon_steps: int
    horizon_seconds: int


@dataclass(frozen=True)
class AITrainingResult:
    status: str
    artifact_payload: dict[str, Any]
    train_rows: int
    test_rows: int
    cv_fold_count: int
    holdout_metrics: dict[str, float | None]
    cv_mean_metrics: dict[str, float | None]
    error_message: str | None = None


@dataclass(frozen=True)
class AIPredictionPoint:
    generated_bucket_start_utc: str
    generated_bucket_epoch_s: int
    target_bucket_start_utc: str
    target_bucket_epoch_s: int
    predicted_price: float
    actual_price: float | None
    abs_error: float | None


class AIModelBackend(ABC):
    name: str

    @abstractmethod
    def train_and_evaluate(
        self,
        series: pd.DataFrame,
        *,
        config: AITrainingConfig,
    ) -> AITrainingResult:
        raise NotImplementedError

    @abstractmethod
    def predict_points(
        self,
        series: pd.DataFrame,
        *,
        artifact_payload: dict[str, Any],
        batch_size: int,
    ) -> list[AIPredictionPoint]:
        raise NotImplementedError


_PIPELINE_CACHE: dict[tuple[str, str], Any] = {}


def resolve_device(requested_device: str) -> str:
    if requested_device != "auto":
        return requested_device
    if TORCH_IMPORT_ERROR is not None:
        return "cpu"
    if torch.cuda.is_available():
        return "cuda"
    if hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return "mps"
    return "cpu"


def _get_chronos_pipeline(*, pretrained_model_id: str, device: str) -> Any:
    if BACKEND_DEPENDENCY_ERRORS["chronos2"] is not None:
        raise RuntimeError(str(BACKEND_DEPENDENCY_ERRORS["chronos2"]))
    resolved_device = resolve_device(device)
    cache_key = (pretrained_model_id, resolved_device)
    if cache_key not in _PIPELINE_CACHE:
        _PIPELINE_CACHE[cache_key] = BaseChronosPipeline.from_pretrained(
            pretrained_model_id,
            device_map=resolved_device,
        )
    return _PIPELINE_CACHE[cache_key]


def _forecast_sequences_with_chronos(
    *,
    contexts: list[np.ndarray],
    pretrained_model_id: str,
    device: str,
    prediction_length: int,
) -> list[np.ndarray]:
    if not contexts:
        return []
    pipeline = _get_chronos_pipeline(pretrained_model_id=pretrained_model_id, device=device)
    inputs = [{"target": np.asarray(context, dtype=np.float32)} for context in contexts]
    _, mean = pipeline.predict_quantiles(
        inputs,
        prediction_length=int(prediction_length),
        quantile_levels=[0.1, 0.5, 0.9],
    )

    if isinstance(mean, list):
        return [np.asarray(item, dtype=float).reshape(-1) for item in mean]

    mean_array = np.asarray(mean, dtype=float)
    if mean_array.ndim == 1:
        return [mean_array.reshape(-1)]
    return [mean_array[row_idx].reshape(-1) for row_idx in range(mean_array.shape[0])]


def _make_batch_time_index(*, context_length: int, bin_seconds: int) -> pd.DatetimeIndex:
    return pd.date_range(
        start="2000-01-01T00:00:00Z",
        periods=context_length,
        freq=pd.to_timedelta(int(bin_seconds), unit="s"),
    )


def _forecast_sequences_with_moirai2(
    *,
    contexts: list[np.ndarray],
    pretrained_model_id: str,
    device: str,
    prediction_length: int,
    context_length: int,
    bin_seconds: int,
    batch_size: int,
) -> list[np.ndarray]:
    if not contexts:
        return []
    if BACKEND_DEPENDENCY_ERRORS["moirai2"] is not None:
        raise RuntimeError(str(BACKEND_DEPENDENCY_ERRORS["moirai2"]))

    rows: list[pd.DataFrame] = []
    for idx, context in enumerate(contexts):
        context_array = np.asarray(context, dtype=float).reshape(-1)
        time_index = _make_batch_time_index(
            context_length=len(context_array),
            bin_seconds=int(bin_seconds),
        )
        rows.append(
            pd.DataFrame(
                {
                    "unique_id": f"series_{idx:05d}",
                    "ds": time_index,
                    "y": context_array,
                }
            )
        )
    dataset_df = pd.concat(rows, ignore_index=True)
    dataset = PandasDataset.from_long_dataframe(
        dataset_df,
        item_id="unique_id",
        target="y",
        timestamp="ds",
        freq=f"{int(bin_seconds)}s",
    )

    module = Moirai2Module.from_pretrained(pretrained_model_id)
    resolved_device = resolve_device(device)
    if resolved_device != "cpu" and hasattr(module, "to"):
        module = module.to(resolved_device)
    model = Moirai2Forecast(
        module=module,
        prediction_length=int(prediction_length),
        context_length=int(context_length),
        target_dim=1,
        feat_dynamic_real_dim=0,
        past_feat_dynamic_real_dim=0,
    )
    predictor = model.create_predictor(batch_size=int(batch_size))
    forecasts = list(predictor.predict(dataset))

    output: list[np.ndarray] = []
    for forecast in forecasts:
        forecast_array = np.asarray(forecast.forecast_array, dtype=float)
        if forecast_array.ndim == 1:
            output.append(forecast_array.reshape(-1))
            continue
        median_idx = forecast_array.shape[0] // 2
        output.append(forecast_array[median_idx].reshape(-1))
    return output


def _truncate_context(values: np.ndarray, max_context_points: int) -> np.ndarray:
    if len(values) <= max_context_points:
        return values
    return values[-int(max_context_points) :]


def _build_cv_origins(
    *,
    series_length: int,
    holdout_start_idx: int,
    horizon_steps: int,
    min_context_points: int,
    cv_windows: int,
) -> list[int]:
    origins: list[int] = []
    candidate = int(holdout_start_idx) - int(horizon_steps)
    while candidate >= int(min_context_points) and len(origins) < int(cv_windows):
        origins.append(candidate)
        candidate -= int(horizon_steps)
    origins.reverse()
    return origins


class Chronos2Backend(AIModelBackend):
    name = "chronos2"

    def train_and_evaluate(
        self,
        series: pd.DataFrame,
        *,
        config: AITrainingConfig,
    ) -> AITrainingResult:
        if BACKEND_DEPENDENCY_ERRORS["chronos2"] is not None:
            return AITrainingResult(
                status="failed",
                artifact_payload={},
                train_rows=0,
                test_rows=0,
                cv_fold_count=0,
                holdout_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                cv_mean_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                error_message=str(BACKEND_DEPENDENCY_ERRORS["chronos2"]),
            )

        working = series.sort_values(by=["bucket_epoch_s"], ascending=[True]).reset_index(drop=True)
        prices = working["price"].astype(float).to_numpy()
        if len(prices) < int(config.min_context_points) + int(config.horizon_steps):
            return AITrainingResult(
                status="skipped",
                artifact_payload={},
                train_rows=0,
                test_rows=0,
                cv_fold_count=0,
                holdout_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                cv_mean_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                error_message=f"insufficient_context_rows:{len(prices)}",
            )

        holdout_steps = max(int(config.horizon_steps), int(math.floor(len(prices) * float(config.test_size))))
        holdout_steps = min(holdout_steps, len(prices) - int(config.min_context_points))
        if holdout_steps < int(config.horizon_steps):
            return AITrainingResult(
                status="skipped",
                artifact_payload={},
                train_rows=0,
                test_rows=0,
                cv_fold_count=0,
                holdout_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                cv_mean_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                error_message=f"insufficient_holdout_rows:{holdout_steps}",
            )

        holdout_start_idx = len(prices) - holdout_steps
        holdout_context = _truncate_context(prices[:holdout_start_idx], int(config.max_context_points))
        holdout_actual = prices[holdout_start_idx : holdout_start_idx + int(config.horizon_steps)]
        holdout_pred = _forecast_sequences_with_chronos(
            contexts=[holdout_context],
            pretrained_model_id=config.pretrained_model_id,
            device=config.device,
            prediction_length=int(config.horizon_steps),
        )[0][: len(holdout_actual)]
        holdout_metrics = common.compute_metrics(holdout_actual.astype(float), holdout_pred.astype(float))

        cv_metrics: list[dict[str, float | None]] = []
        for origin_idx in _build_cv_origins(
            series_length=len(prices),
            holdout_start_idx=holdout_start_idx,
            horizon_steps=int(config.horizon_steps),
            min_context_points=int(config.min_context_points),
            cv_windows=int(config.cv_windows),
        ):
            target_slice = prices[origin_idx : origin_idx + int(config.horizon_steps)]
            if len(target_slice) < int(config.horizon_steps):
                continue
            context_slice = _truncate_context(prices[:origin_idx], int(config.max_context_points))
            pred_slice = _forecast_sequences_with_chronos(
                contexts=[context_slice],
                pretrained_model_id=config.pretrained_model_id,
                device=config.device,
                prediction_length=int(config.horizon_steps),
            )[0][: len(target_slice)]
            cv_metrics.append(common.compute_metrics(target_slice.astype(float), pred_slice.astype(float)))

        cv_mean_metrics = {
            metric_name: common.metric_mean(cv_metrics, metric_name)
            for metric_name in ["mae", "rmse", "mape", "r2"]
        }
        artifact_payload = {
            "backend_name": self.name,
            "backend_family": "foundation_model",
            "execution_mode": "pretrained_zero_shot_evaluation",
            "pretrained_model_id": config.pretrained_model_id,
            "device": resolve_device(config.device),
            "min_context_points": int(config.min_context_points),
            "max_context_points": int(config.max_context_points),
            "cv_windows": int(config.cv_windows),
            "bin_seconds": int(config.bin_seconds),
            "horizon_steps": int(config.horizon_steps),
            "horizon_seconds": int(config.horizon_seconds),
            "prediction_length": int(config.horizon_steps),
        }
        return AITrainingResult(
            status="trained",
            artifact_payload=artifact_payload,
            train_rows=int(holdout_start_idx),
            test_rows=int(len(holdout_actual)),
            cv_fold_count=int(len(cv_metrics)),
            holdout_metrics=holdout_metrics,
            cv_mean_metrics=cv_mean_metrics,
            error_message=None,
        )

    def predict_points(
        self,
        series: pd.DataFrame,
        *,
        artifact_payload: dict[str, Any],
        batch_size: int,
    ) -> list[AIPredictionPoint]:
        if BACKEND_DEPENDENCY_ERRORS["chronos2"] is not None:
            raise RuntimeError(str(BACKEND_DEPENDENCY_ERRORS["chronos2"]))

        working = series.sort_values(by=["bucket_epoch_s"], ascending=[True]).reset_index(drop=True)
        if working.empty:
            return []
        prices = working["price"].astype(float).to_numpy()
        epochs = working["bucket_epoch_s"].astype(int).to_numpy()
        starts = pd.to_datetime(working["bucket_start_utc"], utc=True, errors="coerce")
        if starts.isna().any():
            working = working.dropna(subset=["bucket_start_utc"]).copy()
            prices = working["price"].astype(float).to_numpy()
            epochs = working["bucket_epoch_s"].astype(int).to_numpy()
            starts = pd.to_datetime(working["bucket_start_utc"], utc=True, errors="coerce")

        min_context_points = int(artifact_payload["min_context_points"])
        max_context_points = int(artifact_payload["max_context_points"])
        horizon_steps = int(artifact_payload["horizon_steps"])
        horizon_seconds = int(artifact_payload["horizon_seconds"])
        pretrained_model_id = str(artifact_payload["pretrained_model_id"])
        device = str(artifact_payload.get("device", "auto"))

        actual_price_by_epoch = {int(epoch): float(price) for epoch, price in zip(epochs.tolist(), prices.tolist())}
        eligible_indices = list(range(min_context_points - 1, len(prices)))
        if not eligible_indices:
            return []

        prediction_points: list[AIPredictionPoint] = []
        for batch_start in range(0, len(eligible_indices), max(1, int(batch_size))):
            batch_indices = eligible_indices[batch_start : batch_start + max(1, int(batch_size))]
            contexts = [
                _truncate_context(prices[: idx + 1], max_context_points)
                for idx in batch_indices
            ]
            batch_forecasts = _forecast_sequences_with_chronos(
                contexts=contexts,
                pretrained_model_id=pretrained_model_id,
                device=device,
                prediction_length=horizon_steps,
            )
            for idx, forecast_values in zip(batch_indices, batch_forecasts):
                if len(forecast_values) < horizon_steps:
                    continue
                generated_epoch = int(epochs[idx])
                generated_utc = pd.Timestamp(starts.iloc[idx]).strftime("%Y-%m-%dT%H:%M:%S%z")
                target_epoch = generated_epoch + horizon_seconds
                target_utc = pd.to_datetime(target_epoch, unit="s", utc=True).strftime("%Y-%m-%dT%H:%M:%S%z")
                predicted_price = float(forecast_values[horizon_steps - 1])
                actual_price = actual_price_by_epoch.get(target_epoch)
                abs_error = abs(actual_price - predicted_price) if actual_price is not None else None
                prediction_points.append(
                    AIPredictionPoint(
                        generated_bucket_start_utc=str(generated_utc),
                        generated_bucket_epoch_s=generated_epoch,
                        target_bucket_start_utc=str(target_utc),
                        target_bucket_epoch_s=target_epoch,
                        predicted_price=predicted_price,
                        actual_price=float(actual_price) if actual_price is not None else None,
                        abs_error=float(abs_error) if abs_error is not None else None,
                    )
                )
        return prediction_points


class Moirai2Backend(AIModelBackend):
    name = "moirai2"

    def train_and_evaluate(
        self,
        series: pd.DataFrame,
        *,
        config: AITrainingConfig,
    ) -> AITrainingResult:
        if BACKEND_DEPENDENCY_ERRORS["moirai2"] is not None:
            return AITrainingResult(
                status="failed",
                artifact_payload={},
                train_rows=0,
                test_rows=0,
                cv_fold_count=0,
                holdout_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                cv_mean_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                error_message=str(BACKEND_DEPENDENCY_ERRORS["moirai2"]),
            )

        working = series.sort_values(by=["bucket_epoch_s"], ascending=[True]).reset_index(drop=True)
        prices = working["price"].astype(float).to_numpy()
        if len(prices) < int(config.min_context_points) + int(config.horizon_steps):
            return AITrainingResult(
                status="skipped",
                artifact_payload={},
                train_rows=0,
                test_rows=0,
                cv_fold_count=0,
                holdout_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                cv_mean_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                error_message=f"insufficient_context_rows:{len(prices)}",
            )

        holdout_steps = max(int(config.horizon_steps), int(math.floor(len(prices) * float(config.test_size))))
        holdout_steps = min(holdout_steps, len(prices) - int(config.min_context_points))
        if holdout_steps < int(config.horizon_steps):
            return AITrainingResult(
                status="skipped",
                artifact_payload={},
                train_rows=0,
                test_rows=0,
                cv_fold_count=0,
                holdout_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                cv_mean_metrics={"mae": None, "rmse": None, "mape": None, "r2": None},
                error_message=f"insufficient_holdout_rows:{holdout_steps}",
            )

        holdout_start_idx = len(prices) - holdout_steps
        holdout_context = _truncate_context(prices[:holdout_start_idx], int(config.max_context_points))
        holdout_actual = prices[holdout_start_idx : holdout_start_idx + int(config.horizon_steps)]
        holdout_pred = _forecast_sequences_with_moirai2(
            contexts=[holdout_context],
            pretrained_model_id=config.pretrained_model_id,
            device=config.device,
            prediction_length=int(config.horizon_steps),
            context_length=int(config.max_context_points),
            bin_seconds=int(config.bin_seconds),
            batch_size=1,
        )[0][: len(holdout_actual)]
        holdout_metrics = common.compute_metrics(holdout_actual.astype(float), holdout_pred.astype(float))

        cv_metrics: list[dict[str, float | None]] = []
        for origin_idx in _build_cv_origins(
            series_length=len(prices),
            holdout_start_idx=holdout_start_idx,
            horizon_steps=int(config.horizon_steps),
            min_context_points=int(config.min_context_points),
            cv_windows=int(config.cv_windows),
        ):
            target_slice = prices[origin_idx : origin_idx + int(config.horizon_steps)]
            if len(target_slice) < int(config.horizon_steps):
                continue
            context_slice = _truncate_context(prices[:origin_idx], int(config.max_context_points))
            pred_slice = _forecast_sequences_with_moirai2(
                contexts=[context_slice],
                pretrained_model_id=config.pretrained_model_id,
                device=config.device,
                prediction_length=int(config.horizon_steps),
                context_length=int(config.max_context_points),
                bin_seconds=int(config.bin_seconds),
                batch_size=1,
            )[0][: len(target_slice)]
            cv_metrics.append(common.compute_metrics(target_slice.astype(float), pred_slice.astype(float)))

        cv_mean_metrics = {
            metric_name: common.metric_mean(cv_metrics, metric_name)
            for metric_name in ["mae", "rmse", "mape", "r2"]
        }
        artifact_payload = {
            "backend_name": self.name,
            "backend_family": "foundation_model",
            "execution_mode": "pretrained_zero_shot_evaluation",
            "pretrained_model_id": config.pretrained_model_id,
            "device": resolve_device(config.device),
            "min_context_points": int(config.min_context_points),
            "max_context_points": int(config.max_context_points),
            "cv_windows": int(config.cv_windows),
            "bin_seconds": int(config.bin_seconds),
            "horizon_steps": int(config.horizon_steps),
            "horizon_seconds": int(config.horizon_seconds),
            "prediction_length": int(config.horizon_steps),
        }
        return AITrainingResult(
            status="trained",
            artifact_payload=artifact_payload,
            train_rows=int(holdout_start_idx),
            test_rows=int(len(holdout_actual)),
            cv_fold_count=int(len(cv_metrics)),
            holdout_metrics=holdout_metrics,
            cv_mean_metrics=cv_mean_metrics,
            error_message=None,
        )

    def predict_points(
        self,
        series: pd.DataFrame,
        *,
        artifact_payload: dict[str, Any],
        batch_size: int,
    ) -> list[AIPredictionPoint]:
        if BACKEND_DEPENDENCY_ERRORS["moirai2"] is not None:
            raise RuntimeError(str(BACKEND_DEPENDENCY_ERRORS["moirai2"]))

        working = series.sort_values(by=["bucket_epoch_s"], ascending=[True]).reset_index(drop=True)
        if working.empty:
            return []
        prices = working["price"].astype(float).to_numpy()
        epochs = working["bucket_epoch_s"].astype(int).to_numpy()
        starts = pd.to_datetime(working["bucket_start_utc"], utc=True, errors="coerce")
        if starts.isna().any():
            working = working.dropna(subset=["bucket_start_utc"]).copy()
            prices = working["price"].astype(float).to_numpy()
            epochs = working["bucket_epoch_s"].astype(int).to_numpy()
            starts = pd.to_datetime(working["bucket_start_utc"], utc=True, errors="coerce")

        min_context_points = int(artifact_payload["min_context_points"])
        max_context_points = int(artifact_payload["max_context_points"])
        horizon_steps = int(artifact_payload["horizon_steps"])
        horizon_seconds = int(artifact_payload["horizon_seconds"])
        pretrained_model_id = str(artifact_payload["pretrained_model_id"])
        device = str(artifact_payload.get("device", "auto"))
        bin_seconds = int(artifact_payload["bin_seconds"])

        actual_price_by_epoch = {int(epoch): float(price) for epoch, price in zip(epochs.tolist(), prices.tolist())}
        eligible_indices = list(range(min_context_points - 1, len(prices)))
        if not eligible_indices:
            return []

        prediction_points: list[AIPredictionPoint] = []
        for batch_start in range(0, len(eligible_indices), max(1, int(batch_size))):
            batch_indices = eligible_indices[batch_start : batch_start + max(1, int(batch_size))]
            contexts = [
                _truncate_context(prices[: idx + 1], max_context_points)
                for idx in batch_indices
            ]
            batch_forecasts = _forecast_sequences_with_moirai2(
                contexts=contexts,
                pretrained_model_id=pretrained_model_id,
                device=device,
                prediction_length=horizon_steps,
                context_length=max_context_points,
                bin_seconds=bin_seconds,
                batch_size=max(1, int(batch_size)),
            )
            for idx, forecast_values in zip(batch_indices, batch_forecasts):
                if len(forecast_values) < horizon_steps:
                    continue
                generated_epoch = int(epochs[idx])
                generated_utc = pd.Timestamp(starts.iloc[idx]).strftime("%Y-%m-%dT%H:%M:%S%z")
                target_epoch = generated_epoch + horizon_seconds
                target_utc = pd.to_datetime(target_epoch, unit="s", utc=True).strftime("%Y-%m-%dT%H:%M:%S%z")
                predicted_price = float(forecast_values[horizon_steps - 1])
                actual_price = actual_price_by_epoch.get(target_epoch)
                abs_error = abs(actual_price - predicted_price) if actual_price is not None else None
                prediction_points.append(
                    AIPredictionPoint(
                        generated_bucket_start_utc=str(generated_utc),
                        generated_bucket_epoch_s=generated_epoch,
                        target_bucket_start_utc=str(target_utc),
                        target_bucket_epoch_s=target_epoch,
                        predicted_price=predicted_price,
                        actual_price=float(actual_price) if actual_price is not None else None,
                        abs_error=float(abs_error) if abs_error is not None else None,
                    )
                )
        return prediction_points


BACKEND_FACTORIES: dict[str, AIModelBackend] = {
    "chronos2": Chronos2Backend(),
    "moirai2": Moirai2Backend(),
}


def parse_backend_names(raw_value: str | None) -> list[str]:
    names = common.parse_csv_values(raw_value)
    if not names:
        return ["chronos2"]
    resolved: list[str] = []
    seen: set[str] = set()
    for name in names:
        key = name.lower()
        if key not in BACKEND_FACTORIES:
            raise SystemExit(
                f"Unsupported AI backend: {name}. Supported values: {', '.join(sorted(BACKEND_FACTORIES))}"
            )
        if key in seen:
            continue
        seen.add(key)
        resolved.append(key)
    return resolved


def get_backend(name: str) -> AIModelBackend:
    key = str(name).lower()
    if key not in BACKEND_FACTORIES:
        raise KeyError(key)
    return BACKEND_FACTORIES[key]


def ensure_backends_available(names: list[str]) -> None:
    errors = [BACKEND_DEPENDENCY_ERRORS[name] for name in names if BACKEND_DEPENDENCY_ERRORS.get(name)]
    if errors:
        unique_errors = list(dict.fromkeys(str(item) for item in errors))
        raise SystemExit(" | ".join(unique_errors))


def save_artifact(path: Path, *, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    joblib.dump(payload, path)


def load_artifact(path: Path) -> dict[str, Any]:
    loaded = joblib.load(path)
    if not isinstance(loaded, dict):
        raise RuntimeError(f"AI model artifact is not a dict payload: {path}")
    return loaded
