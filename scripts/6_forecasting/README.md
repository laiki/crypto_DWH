# Forecasting Scripts

## Files
- `train_staging_models_and_forecasts.py`: staging-history training runner with cleansing-based forecast inference and DB persistence.

## Models
- `Ridge`: linear baseline with regularization.
- `HistGradientBoostingRegressor`: non-linear tree boosting baseline.

## Input Requirements
- One or more staging SQLite DBs matching the staging glob.
- One cleansing SQLite DB with table `cleansed_market`.
- Forecast output DB (typically Core DB) writable for forecast tables.

## Example Run
```bash
python scripts/6_forecasting/train_staging_models_and_forecasts.py \
  --staging-glob "data/staging/staging_export_*_last_*.db" \
  --cleansing-db data/cleansing/latest_cleansing.db \
  --forecast-db data/core/core_kpi.db \
  --workers 4 \
  --progress \
  --progress-interval-seconds 30 \
  --secondary-horizon-multiple 30
```

Symbol scope filter:
- Use `--symbols` (alias: `--symbol`) with comma-separated exact values and/or SQL-like patterns (`%`, `_`), case-insensitive.
- Example: `--symbols "BTC/USDT,%btc/%"`.

Parallel execution model:
- `--workers` controls per-pair process parallelism for training/inference compute.
- SQLite writes for model registry and forecast rows remain single-writer in the main process.

## Output Artifacts
- Model artifacts (`joblib`) under `data/forecasting/models/<run_id>/...`
- Registry metadata and metrics in forecast tables / registry view (`model_artifacts`)
- Forecast rows (`forecast_predictions`) for dashboard overlay use

## Evaluation Logic
- Time-aware CV: `TimeSeriesSplit` on training portion.
- Holdout split: tail-based split.
- Metrics:
  - `MAE`
  - `RMSE`
  - `MAPE` (safe handling for near-zero true values)
  - `R²` (stored as test score in `model_artifacts.test_score_r2`)

## Feature Logic
- Leakage-safe lag features (`lag_n`)
- Leakage-safe rolling stats over lagged price (`roll_mean_n`, `roll_std_n`)
- Technical-indicator features over lagged price via `pandas-ta` (`SMA`, `EMA`, `RSI`, `ROC`, Bollinger Bands)
- Cyclical time features (`tod_sin`, `tod_cos`, `dow_sin`, `dow_cos`)
- Optional momentum features based on configured lags
- Target uses configured forecast horizons (`1 * bin_seconds` and `N * bin_seconds`)
