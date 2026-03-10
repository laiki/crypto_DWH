# Forecasting Scripts

## Recommended workflow

Forecasting is split into two phases:

1. `train_staging_models.py`
   - trains models from one or more staging exports
   - writes model artifacts to `data/forecasting/models/...`
   - registers metadata and evaluation metrics in the forecast DB
2. `forecast_with_trained_models.py`
   - loads trained models from the forecast DB / model directory
   - applies them to one selected cleansing run
   - writes forecast rows to `forecast_predictions`

This decouples long training history from run-specific forecast generation.

## Files

- `train_staging_models.py`: training-only batch for staging history
- `forecast_with_trained_models.py`: inference-only batch for cleansing runs
- `train_staging_models_and_forecasts.py`: legacy combined entrypoint; kept for compatibility, but the split workflow above is preferred

## Input requirements

### Training

- Staging SQLite input (`market_ticks`), provided as one or more `.db` files, directories, or glob patterns spanning multiple staging slices
- Optional exclude input to remove selected staging exports after include resolution
- Writable forecast DB (typically Core DB)
- Writable model artifact directory

### Forecast inference

- Cleansing SQLite DB (`cleansed_market`)
- Forecast DB containing trained model registry rows
- Model artifact directory referenced by the registry

## Example runs

### 1. Train models from staging history

```bash
python scripts/6_forecasting/train_staging_models.py \
  --staging-db "data/staging/*.db" \
  --staging-db-exclude "data/staging/*_last_1h.db" \
  --forecast-db data/core/core_kpi.db \
  --model-dir data/forecasting/models \
  --bin-seconds 60 \
  --workers 4 \
  --progress \
  --progress-interval-seconds 30 \
  --secondary-horizon-multiple 30
```

Optional:
- `--training-cutoff-utc 2026-03-10T00:14:00+00:00`
- `--symbols "BTC/USDT,%eth/%"`
- `--exchange-id binance`

Example with an explicit list of staging DB files:

```bash
python scripts/6_forecasting/train_staging_models.py \
  --staging-db \
    data/staging/staging_export_20260310_083654_last_1h_from_2h.db \
    data/staging/staging_export_20260310_083651_last_1h_from_1h.db \
    data/staging/staging_export_20260310_083648_last_1h.db \
  --forecast-db data/core/core_kpi.db \
  --model-dir data/forecasting/models
```

Example with include-directory plus explicit excludes:

```bash
python scripts/6_forecasting/train_staging_models.py \
  --staging-db data/staging \
  --staging-db-exclude \
    data/staging/staging_export_20260310_083648_last_1h.db \
    "data/staging/*_from_2h.db" \
  --forecast-db data/core/core_kpi.db
```

### 2. Forecast one cleansing run with trained models

```bash
python scripts/6_forecasting/forecast_with_trained_models.py \
  --cleansing-db data/cleansing/latest_cleansing.db \
  --forecast-db data/core/core_kpi.db \
  --training-run-id forecast_train_20260310_123924Z \
  --workers 4 \
  --replace-existing \
  --progress \
  --progress-interval-seconds 30
```

If `--training-run-id` is omitted, the latest completed training run is used.

## Staging history selection

- `--staging-db` accepts:
  - one file, for example `data/staging/latest_staging.db`
  - multiple files, for example `--staging-db a.db b.db c.db`
  - one directory, for example `data/staging`
  - one glob, for example `"data/staging/staging_export_20260310_*.db"`
- `--staging-db-exclude` accepts the same input forms and is applied after include resolution.
- Multiple staging DBs are merged for training-history lookup.
- This allows model training over larger historical windows than a single staging export can provide.

## Output artifacts

- Model artifacts (`joblib`) under `data/forecasting/models/<training_run_id>/...`
- Training metadata and evaluation metrics in:
  - `forecast_training_runs`
  - `forecast_model_registry`
  - `model_artifacts`
- Forecast rows for the dashboard in:
  - `forecast_predictions`

## Evaluation logic

### Training

- Time-aware CV: `TimeSeriesSplit`
- Holdout split: tail-based
- Metrics: MAE, RMSE, MAPE, R²

### Forecast inference

- Reuses stored model feature configuration from the registry
- Writes `actual_price` and `abs_error` when the forecast horizon overlaps existing cleansing rows

## Feature logic

- Leakage-safe lag features
- Leakage-safe rolling mean/std features
- `pandas-ta` indicators over lagged price
- Cyclical time features (`tod_sin`, `tod_cos`, `dow_sin`, `dow_cos`)
- Optional momentum features
- Two horizons by default: `1 * bin_seconds` and `N * bin_seconds`
