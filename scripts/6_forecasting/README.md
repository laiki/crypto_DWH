# Forecasting Scripts (Staging and Cleansing)

## Files

- `train_staging_models_and_forecasts.py`: trains models from staging history, runs inference on cleansing series, and writes forecasting outputs to the forecast DB.

## Models

- `Ridge`
- `HistGradientBoostingRegressor`

## Input requirements

- One staging SQLite DB (`market_ticks`)
- One cleansing SQLite DB (`cleansed_market`)
- Forecast output DB (typically Core DB), writable

## Example run

```bash
python scripts/6_forecasting/train_staging_models_and_forecasts.py \
  --staging-db data/staging/latest_staging.db \
  --cleansing-db data/cleansing/latest_cleansing.db \
  --forecast-db data/core/core_kpi.db \
  --model-dir data/forecasting/models \
  --workers 4 \
  --progress \
  --progress-interval-seconds 30 \
  --secondary-horizon-multiple 30
```

## Symbol scope filter

- `--symbols` (alias: `--symbol`) accepts comma-separated exact values and SQL-like patterns (`%`, `_`), case-insensitive.
- Example: `--symbols "BTC/USDT,%btc/%"`.

## Parallel execution model

- `--workers` controls per-pair process parallelism for training/inference.
- SQLite writes for model registry and forecast rows are single-writer in the main process.

## Output artifacts

- Model artifacts (`joblib`) under `data/forecasting/models/<run_id>/...`
- Registry metadata and metrics in forecast tables / view (`model_artifacts`) inside the forecast DB
- Forecast rows (`forecast_predictions`) for dashboard overlays

## Evaluation logic

- Time-aware CV: `TimeSeriesSplit`
- Holdout split: tail-based
- Metrics: MAE, RMSE, MAPE, R²

## Feature logic

- Leakage-safe lag features
- Leakage-safe rolling mean/std features
- `pandas-ta` indicators over lagged price
- Cyclical time features (`tod_sin`, `tod_cos`, `dow_sin`, `dow_cos`)
- Optional momentum features
- Two horizons: `1 * bin_seconds` and `N * bin_seconds`
