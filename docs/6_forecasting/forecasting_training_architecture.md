# Forecasting Architecture (Split Training and Inference)

## Decision Summary

- Forecasting is executed in two separate batch phases.
- `train_staging_models.py` trains models from staging history only.
- `forecast_with_trained_models.py` applies already trained models to one selected cleansing run.
- Model artifacts are stored on filesystem (`joblib`), while metadata, registry rows, and forecast rows are stored in the forecast DB (typically the Core/KPI DB).

## Why This Design

- Training history and forecast target windows have different time semantics and should not be forced into one batch.
- A single staging export is often too small for robust model training, while multiple staging exports can be combined naturally.
- Forecast generation should be repeatable for any cleansing run without re-training models.
- Dashboard-ready predictions remain centralized in one DB table (`forecast_predictions`).

## Phase 1: Training

Input:
- one or more staging exports (`market_ticks`)
- optional training cutoff

Processing:
1. Resolve all staging DB inputs (file, directory, or glob).
2. Merge eligible staging history across all selected staging DBs.
3. Resample per `(exchange_id, symbol)` to the configured cadence.
4. Build features and train all configured regressors.
5. Evaluate with holdout and `TimeSeriesSplit`.
6. Persist model artifacts and registry metadata.

Output:
- `forecast_training_runs`
- `forecast_model_registry`
- `model_artifacts`
- model files under `data/forecasting/models/<training_run_id>/...`

## Phase 2: Forecast Inference

Input:
- one cleansing DB / run (`cleansed_market`)
- one selected `training_run_id` from the forecast DB

Processing:
1. Load trained model registry rows for the selected training run.
2. Load the corresponding model artifacts.
3. Rebuild the required feature set on cleansing rows.
4. Generate predictions for each model / horizon.
5. Persist forecast rows for dashboard consumption.

Output:
- `forecast_predictions`

## Storage Pattern

- Model files:
  - `data/forecasting/models/<training_run_id>/<exchange_id>/<symbol>/<model_name>.joblib`
- Forecast DB:
  - typically `data/core/core_kpi.db`
  - stores `forecast_training_runs`, `forecast_model_registry`, `forecast_predictions`, and `model_artifacts`

## Operational Implication

- Training can use much larger history than any single downstream cleansing export.
- Forecast generation can be rerun repeatedly for different cleansing runs without touching model training.
- This supports a clean lifecycle:
  - retrain models on demand or on schedule
  - forecast each new cleansing export as it becomes available
