#!/usr/bin/env python
"""
Train baseline regression forecasts on cleansed crypto time series.

Models:
- Ridge (linear baseline)
- HistGradientBoostingRegressor (non-linear baseline)

The script trains both models on a single exchange/symbol series, evaluates
with time-aware cross-validation and holdout testing, and writes metrics plus
predictions to disk.
"""

from __future__ import annotations

import argparse
import json
import math
import re
import sqlite3
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import numpy as np
    import pandas as pd
    from sklearn.base import clone
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.metrics import mean_absolute_error
    from sklearn.metrics import mean_squared_error
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except ModuleNotFoundError as exc:
    missing = exc.name or "unknown"
    raise SystemExit(
        "Missing dependency "
        f"'{missing}' for interpreter '{sys.executable}'. "
        "Install dependencies with: "
        f"'{sys.executable} -m pip install -r requirements.txt'"
    ) from exc


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]
DEFAULT_DB_PATH_RAW = "data/core/core_kpi.db"
DEFAULT_DB_FALLBACK_RELATIVE_PATHS = (
    Path("data/core/core_kpi.db"),
    Path("scripts/data/core/core_kpi.db"),
)
LOGGER_NAME = "train_regression_forecast"


@dataclass(frozen=True)
class SeriesSelection:
    exchange_id: str
    symbol: str
    row_count: int
    min_bucket_start_utc: str
    max_bucket_start_utc: str


def log(level: str, message: str) -> None:
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} | {level} | {LOGGER_NAME} | {message}", flush=True)


def parse_int_csv(raw_value: str, *, arg_name: str) -> list[int]:
    parts = [part.strip() for part in raw_value.split(",")]
    values: list[int] = []
    for part in parts:
        if not part:
            continue
        try:
            value = int(part)
        except ValueError as exc:
            raise SystemExit(f"Invalid integer in {arg_name}: {part!r}") from exc
        if value <= 0:
            raise SystemExit(f"Values in {arg_name} must be > 0, got {value}.")
        values.append(value)
    if not values:
        raise SystemExit(f"{arg_name} must contain at least one integer value.")
    return sorted(set(values))


def resolve_db_path(raw_path: str) -> Path:
    requested = Path(raw_path).expanduser()
    candidates: list[Path] = []

    if requested.is_absolute():
        candidates.append(requested)
    else:
        candidates.append((Path.cwd() / requested).resolve())
        candidates.append((REPO_ROOT / requested).resolve())

    if raw_path == DEFAULT_DB_PATH_RAW:
        for rel in DEFAULT_DB_FALLBACK_RELATIVE_PATHS:
            candidates.append((Path.cwd() / rel).resolve())
            candidates.append((REPO_ROOT / rel).resolve())

    unique_candidates: list[Path] = []
    seen: set[str] = set()
    for candidate in candidates:
        normalized = str(candidate)
        if normalized in seen:
            continue
        seen.add(normalized)
        unique_candidates.append(candidate)

    for candidate in unique_candidates:
        if candidate.is_file():
            return candidate

    searched = "\n".join(f"- {candidate}" for candidate in unique_candidates)
    raise SystemExit(f"Database path does not exist. Checked:\n{searched}")


def safe_symbol_for_filename(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return normalized.strip("_") or "symbol"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train Ridge and HistGradientBoosting forecasts from cleansed_market.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--db-path",
        default=DEFAULT_DB_PATH_RAW,
        help="Path to Core SQLite DB that contains cleansed_market.",
    )
    parser.add_argument(
        "--run-id",
        default=None,
        help="Optional run_id. If omitted, latest run_id is used.",
    )
    parser.add_argument(
        "--symbol",
        default=None,
        help="Optional symbol filter. If omitted, the series with most rows is selected.",
    )
    parser.add_argument(
        "--exchange-id",
        default=None,
        help="Optional exchange filter. If omitted, the series with most rows is selected.",
    )
    parser.add_argument(
        "--horizon-steps",
        type=int,
        default=1,
        help="Forecast horizon in bucket steps (predict t+h from features at t).",
    )
    parser.add_argument(
        "--lag-steps",
        default="1,2,3,5,10,15",
        help="Comma-separated lag steps used as features.",
    )
    parser.add_argument(
        "--rolling-windows",
        default="3,5,10",
        help="Comma-separated rolling windows for lagged mean/std features.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Holdout ratio from tail of series.",
    )
    parser.add_argument(
        "--cv-splits",
        type=int,
        default=5,
        help="Target number of TimeSeriesSplit folds on training data.",
    )
    parser.add_argument(
        "--min-model-rows",
        type=int,
        default=120,
        help="Minimum rows after feature engineering to proceed.",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=1.0,
        help="Regularization strength for Ridge.",
    )
    parser.add_argument(
        "--hgb-max-iter",
        type=int,
        default=300,
        help="Maximum iterations for HistGradientBoostingRegressor.",
    )
    parser.add_argument(
        "--hgb-learning-rate",
        type=float,
        default=0.05,
        help="Learning rate for HistGradientBoostingRegressor.",
    )
    parser.add_argument(
        "--output-dir",
        default="data/forecasting",
        help="Directory for JSON metrics and CSV predictions.",
    )
    parser.add_argument(
        "--output-prefix",
        default=None,
        help="Optional output file prefix.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.horizon_steps <= 0:
        raise SystemExit("--horizon-steps must be > 0.")
    if not 0.01 <= args.test_size <= 0.5:
        raise SystemExit("--test-size must be within [0.01, 0.5].")
    if args.cv_splits <= 0:
        raise SystemExit("--cv-splits must be > 0.")
    if args.min_model_rows < 30:
        raise SystemExit("--min-model-rows must be >= 30.")


def resolve_run_id(connection: sqlite3.Connection, requested_run_id: str | None) -> str:
    if requested_run_id:
        row = connection.execute(
            """
            SELECT run_id
            FROM cleansed_market
            WHERE run_id = ?
            LIMIT 1;
            """,
            (requested_run_id,),
        ).fetchone()
        if row is None:
            raise SystemExit(f"run_id not found in cleansed_market: {requested_run_id}")
        return str(row[0])

    row = connection.execute(
        """
        SELECT run_id
        FROM cleansed_market
        GROUP BY run_id
        ORDER BY run_id DESC
        LIMIT 1;
        """
    ).fetchone()
    if row is None:
        raise SystemExit("No run_id found in cleansed_market.")
    return str(row[0])


def choose_series(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    symbol: str | None,
    exchange_id: str | None,
) -> SeriesSelection:
    rows = connection.execute(
        """
        SELECT
            exchange_id,
            symbol,
            COUNT(*) AS row_count,
            MIN(bucket_start_utc) AS min_bucket_start_utc,
            MAX(bucket_start_utc) AS max_bucket_start_utc
        FROM cleansed_market
        WHERE run_id = :run_id
          AND price IS NOT NULL
          AND is_missing = 0
          AND is_stale = 0
          AND (:symbol IS NULL OR symbol = :symbol)
          AND (:exchange_id IS NULL OR exchange_id = :exchange_id)
        GROUP BY exchange_id, symbol
        ORDER BY row_count DESC, exchange_id ASC, symbol ASC;
        """,
        {
            "run_id": run_id,
            "symbol": symbol,
            "exchange_id": exchange_id,
        },
    ).fetchall()

    if not rows:
        raise SystemExit(
            "No matching cleaned series found "
            f"for run_id={run_id}, exchange_id={exchange_id!r}, symbol={symbol!r}."
        )

    best = rows[0]
    return SeriesSelection(
        exchange_id=str(best[0]),
        symbol=str(best[1]),
        row_count=int(best[2]),
        min_bucket_start_utc=str(best[3]),
        max_bucket_start_utc=str(best[4]),
    )


def load_series_dataframe(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    exchange_id: str,
    symbol: str,
) -> pd.DataFrame:
    return pd.read_sql_query(
        """
        SELECT
            bucket_start_utc,
            bucket_epoch_s,
            exchange_id,
            symbol,
            price
        FROM cleansed_market
        WHERE run_id = :run_id
          AND exchange_id = :exchange_id
          AND symbol = :symbol
          AND price IS NOT NULL
          AND is_missing = 0
          AND is_stale = 0
        ORDER BY bucket_epoch_s ASC;
        """,
        connection,
        params={
            "run_id": run_id,
            "exchange_id": exchange_id,
            "symbol": symbol,
        },
    )


def build_feature_frame(
    series_df: pd.DataFrame,
    *,
    lag_steps: list[int],
    rolling_windows: list[int],
    horizon_steps: int,
) -> tuple[pd.DataFrame, pd.Series, pd.DataFrame]:
    working = series_df.copy()
    working["bucket_start_utc"] = pd.to_datetime(working["bucket_start_utc"], utc=True, errors="coerce")
    working["bucket_epoch_s"] = pd.to_numeric(working["bucket_epoch_s"], errors="coerce")
    working["price"] = pd.to_numeric(working["price"], errors="coerce")
    working = working.dropna(subset=["bucket_start_utc", "bucket_epoch_s", "price"]).copy()
    working["bucket_epoch_s"] = working["bucket_epoch_s"].astype(int)
    working = working.sort_values(by=["bucket_epoch_s"], ascending=[True]).drop_duplicates(
        subset=["bucket_epoch_s"], keep="first"
    )
    working = working.reset_index(drop=True)

    minute_of_day = (working["bucket_start_utc"].dt.hour * 60 + working["bucket_start_utc"].dt.minute).astype(float)
    working["tod_sin"] = np.sin(2.0 * np.pi * minute_of_day / 1440.0)
    working["tod_cos"] = np.cos(2.0 * np.pi * minute_of_day / 1440.0)
    working["dow_sin"] = np.sin(2.0 * np.pi * working["bucket_start_utc"].dt.dayofweek.astype(float) / 7.0)
    working["dow_cos"] = np.cos(2.0 * np.pi * working["bucket_start_utc"].dt.dayofweek.astype(float) / 7.0)

    for lag in lag_steps:
        working[f"lag_{lag}"] = working["price"].shift(lag)

    lagged_price = working["price"].shift(1)
    for window in rolling_windows:
        working[f"roll_mean_{window}"] = lagged_price.rolling(window=window).mean()
        working[f"roll_std_{window}"] = lagged_price.rolling(window=window).std()

    if {1, 5}.issubset(set(lag_steps)):
        working["momentum_lag1_lag5"] = working["lag_1"] - working["lag_5"]
    if {1, 10}.issubset(set(lag_steps)):
        working["momentum_lag1_lag10"] = working["lag_1"] - working["lag_10"]

    working["target_price"] = working["price"].shift(-horizon_steps)
    working["target_bucket_start_utc"] = working["bucket_start_utc"].shift(-horizon_steps)
    working["target_bucket_epoch_s"] = working["bucket_epoch_s"].shift(-horizon_steps)

    feature_columns = [
        column
        for column in working.columns
        if column
        not in {
            "bucket_start_utc",
            "bucket_epoch_s",
            "exchange_id",
            "symbol",
            "price",
            "target_price",
            "target_bucket_start_utc",
            "target_bucket_epoch_s",
        }
    ]
    cleaned = working.dropna(
        subset=feature_columns + ["target_price", "target_bucket_start_utc", "target_bucket_epoch_s"]
    ).copy()

    feature_frame = cleaned[feature_columns].astype(float)
    target_series = cleaned["target_price"].astype(float)
    meta_frame = cleaned[["target_bucket_start_utc", "target_bucket_epoch_s"]].copy()
    meta_frame["target_bucket_start_utc"] = meta_frame["target_bucket_start_utc"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S%z"
    )
    return feature_frame, target_series, meta_frame


def safe_mape(y_true: np.ndarray, y_pred: np.ndarray, *, epsilon: float = 1e-9) -> float | None:
    mask = np.abs(y_true) > epsilon
    if not np.any(mask):
        return None
    ratio = np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])
    return float(np.mean(ratio) * 100.0)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(math.sqrt(mean_squared_error(y_true, y_pred)))
    mape = safe_mape(y_true, y_pred)
    return {
        "mae": mae,
        "rmse": rmse,
        "mape": mape,
    }


def mean_metric_dict(values: list[dict[str, float | None]]) -> dict[str, float | None]:
    result: dict[str, float | None] = {}
    for metric_name in ("mae", "rmse", "mape"):
        metric_values = [value[metric_name] for value in values if value[metric_name] is not None]
        result[metric_name] = float(np.mean(metric_values)) if metric_values else None
    return result


def evaluate_cv(
    model: Any,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    *,
    requested_splits: int,
) -> dict[str, Any]:
    max_possible_splits = len(X_train) - 1
    effective_splits = min(requested_splits, max_possible_splits)
    if effective_splits < 2:
        return {
            "fold_count": 0,
            "fold_metrics": [],
            "mean_metrics": {"mae": None, "rmse": None, "mape": None},
        }

    splitter = TimeSeriesSplit(n_splits=effective_splits)
    fold_metrics: list[dict[str, float | None]] = []
    for fold_index, (fit_idx, valid_idx) in enumerate(splitter.split(X_train), start=1):
        fold_model = clone(model)
        fold_model.fit(X_train.iloc[fit_idx], y_train.iloc[fit_idx])
        pred = fold_model.predict(X_train.iloc[valid_idx])
        metrics = compute_metrics(y_train.iloc[valid_idx].to_numpy(dtype=float), pred.astype(float))
        metrics["fold_index"] = float(fold_index)
        fold_metrics.append(metrics)

    return {
        "fold_count": effective_splits,
        "fold_metrics": fold_metrics,
        "mean_metrics": mean_metric_dict(fold_metrics),
    }


def create_models(args: argparse.Namespace) -> dict[str, Any]:
    ridge_model = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=args.ridge_alpha)),
        ]
    )
    hgb_model = HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=args.hgb_learning_rate,
        max_iter=args.hgb_max_iter,
        max_depth=6,
        min_samples_leaf=20,
        l2_regularization=0.0,
        random_state=42,
    )
    return {
        "ridge": ridge_model,
        "hist_gradient_boosting": hgb_model,
    }


def build_output_prefix(
    args: argparse.Namespace,
    *,
    run_id: str,
    exchange_id: str,
    symbol: str,
) -> str:
    if args.output_prefix:
        return args.output_prefix
    timestamp_tag = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")
    safe_symbol = safe_symbol_for_filename(symbol)
    safe_exchange = safe_symbol_for_filename(exchange_id)
    safe_run_id = safe_symbol_for_filename(run_id)
    return f"regression_forecast_{timestamp_tag}_{safe_run_id}_{safe_exchange}_{safe_symbol}"


def main() -> None:
    args = parse_args()
    validate_args(args)

    lag_steps = parse_int_csv(args.lag_steps, arg_name="--lag-steps")
    rolling_windows = parse_int_csv(args.rolling_windows, arg_name="--rolling-windows")
    db_path = resolve_db_path(args.db_path)

    output_dir = Path(args.output_dir).expanduser()
    if not output_dir.is_absolute():
        output_dir = (Path.cwd() / output_dir).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    log("INFO", f"Using DB: {db_path}")
    with sqlite3.connect(str(db_path)) as connection:
        run_id = resolve_run_id(connection, args.run_id)
        selected_series = choose_series(
            connection,
            run_id=run_id,
            symbol=args.symbol,
            exchange_id=args.exchange_id,
        )
        log(
            "INFO",
            "Selected series "
            f"exchange_id={selected_series.exchange_id} "
            f"symbol={selected_series.symbol} "
            f"rows={selected_series.row_count} "
            f"window={selected_series.min_bucket_start_utc} -> {selected_series.max_bucket_start_utc}",
        )

        series_df = load_series_dataframe(
            connection,
            run_id=run_id,
            exchange_id=selected_series.exchange_id,
            symbol=selected_series.symbol,
        )

    X, y, meta = build_feature_frame(
        series_df,
        lag_steps=lag_steps,
        rolling_windows=rolling_windows,
        horizon_steps=args.horizon_steps,
    )
    if len(X) < args.min_model_rows:
        raise SystemExit(
            f"Not enough rows after feature engineering: {len(X)} < {args.min_model_rows}. "
            "Choose a denser series or reduce feature settings."
        )

    split_index = int(math.floor(len(X) * (1.0 - args.test_size)))
    split_index = max(1, min(len(X) - 1, split_index))
    X_train, X_test = X.iloc[:split_index], X.iloc[split_index:]
    y_train, y_test = y.iloc[:split_index], y.iloc[split_index:]
    meta_test = meta.iloc[split_index:].reset_index(drop=True)

    log(
        "INFO",
        f"Prepared modeling frame rows={len(X)} features={X.shape[1]} "
        f"train={len(X_train)} test={len(X_test)} horizon_steps={args.horizon_steps}",
    )

    models = create_models(args)
    results: dict[str, Any] = {}
    prediction_frames: list[pd.DataFrame] = []

    for model_name, model in models.items():
        log("INFO", f"Training model: {model_name}")
        cv_summary = evaluate_cv(model, X_train, y_train, requested_splits=args.cv_splits)

        fitted_model = clone(model)
        fitted_model.fit(X_train, y_train)
        y_pred = fitted_model.predict(X_test).astype(float)
        holdout_metrics = compute_metrics(y_test.to_numpy(dtype=float), y_pred)

        results[model_name] = {
            "cv": cv_summary,
            "holdout": holdout_metrics,
        }
        log(
            "INFO",
            "Holdout metrics "
            f"model={model_name} "
            f"MAE={holdout_metrics['mae']:.8f} "
            f"RMSE={holdout_metrics['rmse']:.8f} "
            f"MAPE={holdout_metrics['mape'] if holdout_metrics['mape'] is not None else 'n/a'}",
        )

        prediction_frames.append(
            pd.DataFrame(
                {
                    "model_name": model_name,
                    "target_bucket_start_utc": meta_test["target_bucket_start_utc"].astype(str),
                    "target_bucket_epoch_s": pd.to_numeric(
                        meta_test["target_bucket_epoch_s"], errors="coerce"
                    ).astype("Int64"),
                    "y_true": y_test.to_numpy(dtype=float),
                    "y_pred": y_pred,
                    "abs_error": np.abs(y_test.to_numpy(dtype=float) - y_pred),
                }
            )
        )

    best_model_name = min(
        results.keys(),
        key=lambda name: float(results[name]["holdout"]["rmse"]),
    )
    log("INFO", f"Best holdout RMSE model: {best_model_name}")

    output_prefix = build_output_prefix(
        args,
        run_id=run_id,
        exchange_id=selected_series.exchange_id,
        symbol=selected_series.symbol,
    )
    json_path = output_dir / f"{output_prefix}.json"
    predictions_path = output_dir / f"{output_prefix}_predictions.csv"

    summary_payload = {
        "run_context": {
            "db_path": str(db_path),
            "run_id": run_id,
            "exchange_id": selected_series.exchange_id,
            "symbol": selected_series.symbol,
            "series_rows_raw": int(len(series_df)),
            "series_rows_modeled": int(len(X)),
            "series_window_start_utc": selected_series.min_bucket_start_utc,
            "series_window_end_utc": selected_series.max_bucket_start_utc,
        },
        "feature_config": {
            "horizon_steps": args.horizon_steps,
            "lag_steps": lag_steps,
            "rolling_windows": rolling_windows,
            "feature_count": int(X.shape[1]),
            "feature_columns": list(X.columns),
        },
        "split_config": {
            "test_size": args.test_size,
            "cv_splits_requested": args.cv_splits,
            "split_index": split_index,
            "train_rows": int(len(X_train)),
            "test_rows": int(len(X_test)),
        },
        "model_results": results,
        "best_model_by_holdout_rmse": best_model_name,
        "artifacts": {
            "predictions_csv": str(predictions_path),
        },
    }

    json_path.write_text(json.dumps(summary_payload, indent=2), encoding="utf-8")
    pd.concat(prediction_frames, ignore_index=True).to_csv(predictions_path, index=False)

    log("INFO", f"Result JSON: {json_path}")
    log("INFO", f"Predictions CSV: {predictions_path}")


if __name__ == "__main__":
    main()
