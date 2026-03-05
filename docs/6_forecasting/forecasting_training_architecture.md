# Forecasting Training Architecture (Staging-Based, Leakage-Safe)

## Decision Summary
- Forecast model training is executed as a dedicated batch phase, not inside `staging_exporter.py`.
- Training data is sourced from staging exports (for larger history), not from Core KPI DB.
- Resampling to target cadence (for example `60s`) is done per `(exchange_id, symbol)` in memory and discarded immediately after model training.
- Model artifacts are stored on filesystem (`joblib`), while metadata/metrics are stored in a dedicated SQLite registry DB.

## Why This Design
- Keeps staging export deterministic and operationally stable.
- Avoids persistent storage of additional resampled training tables (disk-capacity constraint).
- Enables large training history while preserving the cleansing cadence semantics.
- Enforces strict leakage guardrail against latest cleansing period.

## Leakage Guardrail
For each training run:
1. Read selected cleansing run metadata.
2. Compute `training_cutoff_utc = MIN(cleansed_market.bucket_start_utc)` of that run.
3. Only staging rows with timestamp `< training_cutoff_utc` are eligible for training.

This ensures no training sample overlaps with the period used by current cleansing outputs.

## Storage Pattern
- Model files:
  - `data/forecasting/models/<run_id>/<exchange_id>/<symbol>/<model_name>.joblib`
- Registry DB:
  - `data/forecasting/forecasting_registry.db`
  - stores model path, config, metrics, status, hashes, and run context
  - `model_artifacts` view exposes per-model test score (`test_score_r2`) for dashboard/model QA

## Processing Pattern (per exchange/symbol)
1. Read raw staging rows before cutoff.
2. Resample to configured cadence (for example `60s`) in memory.
3. Build features and train all configured regressors from the same resampled frame.
4. Evaluate (`TimeSeriesSplit` + holdout).
5. Persist artifacts and registry rows.
6. Drop in-memory frame and continue with next series.

No persistent intermediate resampled DB is created.

## Diagrams
- Architecture: `diagrams/6_forecasting/uml_architecture_forecasting_training_staging_cutoff.mmd`
- Deployment: `diagrams/6_forecasting/uml_deployment_forecasting_training_runtime.mmd`
- ER: `diagrams/6_forecasting/uml_er_forecasting_model_registry.mmd`
- Sequence: `diagrams/6_forecasting/uml_sequence_forecasting_staging_cutoff_training.mmd`
