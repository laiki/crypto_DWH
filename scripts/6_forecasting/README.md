# Forecasting Scripts

## Files
- `train_regression_forecast.py`: baseline forecasting runner with two scikit-learn regressors.

## First Baseline Models
- `Ridge`: linear baseline with regularization.
- `HistGradientBoostingRegressor`: non-linear tree boosting baseline.

## Input Requirements
- SQLite DB with table `cleansed_market` (for example `data/core/core_kpi.db` or `scripts/data/core/core_kpi.db`).
- Required columns in `cleansed_market`: `run_id`, `exchange_id`, `symbol`, `bucket_start_utc`, `bucket_epoch_s`, `price`, `is_missing`, `is_stale`.

## Example Run
```bash
python scripts/6_forecasting/train_regression_forecast.py \
  --db-path data/core/core_kpi.db \
  --symbol BTC/USDT \
  --exchange-id binance \
  --horizon-steps 1
```

If `--symbol` and `--exchange-id` are omitted, the script automatically selects the densest available series in the latest run.

## Output Artifacts
- JSON summary with run context, feature config, split config, model metrics, and best model by holdout RMSE.
- CSV with holdout predictions and absolute errors for both models.

Default output directory:
- `data/forecasting/`

## Evaluation Logic
- Time-aware CV: `TimeSeriesSplit` on training portion only.
- Holdout split: tail-based split using `--test-size`.
- Metrics:
  - `MAE`
  - `RMSE`
  - `MAPE` (safe handling for near-zero true values)

## Feature Logic
- Leakage-safe lag features (`lag_n`)
- Leakage-safe rolling stats over lagged price (`roll_mean_n`, `roll_std_n`)
- Cyclical time features (`tod_sin`, `tod_cos`, `dow_sin`, `dow_cos`)
- Optional momentum features based on configured lags
- Target is shifted by `--horizon-steps`
