# End-to-End Runbook (VAULT 2.0)

This document is the central execution guide for the full pipeline with the new VAULT 2.0 architecture.

Pipeline order:
1. Ingestion (partitioned VAULT storage)
2. Staging export (manifest-pruned window)
3. Cleansing
4. Core build
5. Marts / dashboard cache
6. Forecasting (train from staging history, infer on cleansing)
7. Dashboard

All commands below assume execution from repo root (`/home/wgo/dev/crypto_DWH`).

Runtime artifact note:
- Paths under `data/` and `scripts/data/` in this runbook are execution-time output locations.
- They are local generated artifacts and should not be treated as versioned repository content.

## Prerequisites

```bash
python -m pip install -r requirements.txt
python -m pip install --upgrade ccxt
python -c "import ccxt, ccxt.pro as p; print(ccxt.__version__, len(p.exchanges))"
```

## 1) Start Ingestion (recommended: auto-sharded workers)

```bash
python scripts/1_ingestion/orchestrator_auto_shard.py \
  --vault-root "scripts/data/vault2" \
  --logs-dir "logs" \
  --plan-output "scripts/data/orchestrator_plan.json" \
  --worker-log-level INFO \
  --log-level INFO
```

Runtime output structure:
- Partitions: `scripts/data/vault2/ingestion/exchange=<id>/date=<YYYY-MM-DD>/hour=<HH>/part_*.db`
- Manifest: `scripts/data/vault2/meta/vault_manifest.db`

## 2) Build staging slices from VAULT manifest

Example: generate three separate 1h windows.

```bash
for offset in 0 1 2; do
  python scripts/2_staging/staging_exporter.py \
    --vault-root "scripts/data/vault2" \
    --layer ingestion \
    --hours 1 \
    --start-relative-from-hour "$offset" \
    --output-dir "scripts/data/staging" \
    --progress \
    --progress-interval-seconds 20
done
```

## 3) Cleansing + Core + Marts per staging slice

```bash
for sdb in scripts/data/staging/staging_export_*_last_1h*.db; do
  base="$(basename "$sdb" .db)"
  cdb="scripts/data/cleansing/cleaned_${base}_60s.db"
  core="scripts/data/core/core_kpi_${base}.db"

  python scripts/3_cleansing/cleansing_resample.py \
    --input-db "$sdb" \
    --output-db "$cdb" \
    --workers 4 \
    --progress \
    --progress-interval-seconds 30

  python scripts/4_core/build_core_db.py \
    --staging-db "$sdb" \
    --cleansing-db "$cdb" \
    --output-db "$core" \
    --views-sql scripts/4_core/core_kpi_views.sql \
    --progress \
    --progress-interval-seconds 30

  python scripts/5_marts/build_dashboard_cache.py \
    --db-path "$core" \
    --apply-mart-views \
    --progress \
    --progress-interval-seconds 30
done
```

## 4) Forecasting (training from staging, inference on cleansing)

Pick latest cleansing/core DB first:

```bash
LATEST_CDB="$(ls -1t data/cleansing/cleaned_staging_export_*_60s.db | head -n1)"
LATEST_BASE="$(basename "$LATEST_CDB" .db)"
LATEST_STAGING_BASE="${LATEST_BASE#cleaned_}"
LATEST_STAGING_BASE="${LATEST_STAGING_BASE%_60s}"
LATEST_CORE="data/core/core_kpi_${LATEST_STAGING_BASE}.db"
TRAINING_STAGING_DIR="data/staging"
MODEL_DIR="data/forecasting/models"
```

Train models from staging history:

```bash
python scripts/6_forecasting/train_staging_models.py \
  --staging-db "$TRAINING_STAGING_DIR" \
  --forecast-db "$LATEST_CORE" \
  --model-dir "$MODEL_DIR" \
  --workers 4 \
  --progress \
  --progress-interval-seconds 30
```

Create forecasts for one cleansing run with trained models:

```bash
python scripts/6_forecasting/forecast_with_trained_models.py \
  --cleansing-db "$LATEST_CDB" \
  --forecast-db "$LATEST_CORE" \
  --workers 4 \
  --replace-existing \
  --progress \
  --progress-interval-seconds 30
```

Optional:
- add `--staging-db-exclude ...` to remove selected staging exports from the training history after include resolution
- add `--training-run-id ...` during forecasting to select a specific trained model set instead of the latest completed run

## 5) Start Dashboard

```bash
CORE_DB_PATH="$LATEST_CORE" streamlit run scripts/5_marts/dashboard_mvp_app.py
```

## 6) Optional diagram/doc rendering

```bash
scripts/render_mermaid_all.sh svg diagrams
scripts/render_markdown_all.sh docs
```

## Main artifact locations

- VAULT partitions: `scripts/data/vault2/ingestion/...`
- VAULT manifest: `scripts/data/vault2/meta/vault_manifest.db`
- Staging DBs: `scripts/data/staging/staging_export_*.db`
- Cleansing DBs: `scripts/data/cleansing/cleaned_*.db`
- Core DBs: `scripts/data/core/core_kpi_*.db`
- Forecast model files: `scripts/data/forecasting/models/`
