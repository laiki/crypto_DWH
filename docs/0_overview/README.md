# End-to-End Runbook (Ingestion -> Dashboard)

Dieses Dokument ist die zentrale Ablaufbeschreibung für die komplette Pipeline:
1. Ingestion starten
2. Staging-Exporte in Stundenkontingenten erzeugen
3. Cleansing
4. Core Build
5. Mart Cache
6. Forecasting
7. Dashboard/Presentation

Alle Kommandos unten sind für den Start aus dem Repo-Root (`/home/wgo/dev/crypto_DWH`) geschrieben.

## Voraussetzungen

```bash
python -m pip install -r requirements.txt
python -m pip install ccxt
```

Hinweise:
- Für `pandas-ta` wird Python 3.12 empfohlen.
- Forecasting speichert Modellartefakte unter `scripts/data/forecasting/models`.

## 1) Ingestion starten (empfohlen: auto-sharded Worker)

```bash
python scripts/1_ingestion/orchestrator_auto_shard.py \
  --worker-db-template "scripts/data/worker_{worker_id}_crypto_ws_ticks.db" \
  --logs-dir "logs" \
  --plan-output "scripts/data/orchestrator_plan.json" \
  --worker-log-level INFO \
  --log-level INFO
```

Ingestion einige Stunden laufen lassen, damit ausreichend Historie entsteht.

## 2) Staging in Stundenkontingenten erzeugen (Beispiel: letzte 3 Einzelstunden)

Beispiel erzeugt drei 1h-Slices relativ zum Ende der Quelldaten:
- `--start-relative-from-hour 0` => `[t-1h, t]`
- `--start-relative-from-hour 1` => `[t-2h, t-1h]`
- `--start-relative-from-hour 2` => `[t-3h, t-2h]`

```bash
for offset in 0 1 2; do
  python scripts/2_staging/staging_exporter.py \
    --hours 1 \
    --start-relative-from-hour "$offset" \
    --input-glob "scripts/data/worker_*_crypto_ws_ticks.db" \
    --output-dir "scripts/data/staging" \
    --progress \
    --progress-interval-seconds 20
done
```

## 3) Cleansing + Core + Marts für alle erzeugten Staging-Slices

```bash
for sdb in scripts/data/staging/staging_export_*_last_1h_from_*h.db; do
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

## 4) Forecasting (auf neuestem Cleansing/Core-Slice)

Neueste Cleansing/Core-DB bestimmen:

```bash
LATEST_CDB="$(ls -1t scripts/data/cleansing/cleaned_staging_export_*_last_1h_from_*h_60s.db | head -n1)"
LATEST_BASE="$(basename "$LATEST_CDB" .db)"                 # cleaned_<staging_base>_60s
LATEST_STAGING_BASE="${LATEST_BASE#cleaned_}"               # <staging_base>_60s
LATEST_STAGING_BASE="${LATEST_STAGING_BASE%_60s}"           # <staging_base>
LATEST_CORE="scripts/data/core/core_kpi_${LATEST_STAGING_BASE}.db"
echo "$LATEST_CDB"
echo "$LATEST_CORE"
```

Forecasting-Run:

```bash
python scripts/6_forecasting/train_staging_models_and_forecasts.py \
  --staging-glob "scripts/data/worker_*_crypto_ws_ticks.db" \
  --cleansing-db "$LATEST_CDB" \
  --forecast-db "$LATEST_CORE" \
  --model-dir "scripts/data/forecasting/models" \
  --progress \
  --progress-interval-seconds 30
```

Wichtiger Hinweis zum Datenquell-Glob:
- Das Forecasting-Skript trainiert leakage-sicher nur auf Daten `< training_cutoff_utc`.
- Wenn das gewählte `--staging-glob` nur Daten **ab** diesem Cutoff enthält, werden alle Serien geskippt und kein Modell geschrieben.
- Deshalb ist für Forecasting meist sinnvoll:
  - entweder `scripts/data/worker_*_crypto_ws_ticks.db` als Trainingsquelle,
  - oder ein Staging-Glob, das explizit ältere Slices vor dem Cutoff enthält.

## 5) Dashboard starten (Presentation)

Dashboard nutzt `CORE_DB_PATH` aus der Umgebung:

```bash
CORE_DB_PATH="$LATEST_CORE" streamlit run scripts/5_marts/dashboard_mvp_app.py
```

## 6) Optionale Doku-/Diagramm-Renderings für Präsentation

```bash
scripts/render_mermaid_all.sh svg diagrams
scripts/render_markdown_all.sh docs
```

## 7) Ergebnis-Orte (wichtigste Artefakte)

- Worker Raw DBs: `scripts/data/worker_*_crypto_ws_ticks.db`
- Staging DBs: `scripts/data/staging/staging_export_*.db`
- Cleansing DBs: `scripts/data/cleansing/cleaned_*.db`
- Core DBs: `scripts/data/core/core_kpi_*.db`
- Forecast Modelle: `scripts/data/forecasting/models/`
- Dashboard: `streamlit` auf Basis von `CORE_DB_PATH`

## 8) Alternative: komplette Pipeline aus `scripts/` starten

```bash
cd /home/wgo/dev/crypto_DWH/scripts
```

### 8.1 Ingestion

```bash
python 1_ingestion/orchestrator_auto_shard.py \
  --worker-db-template "data/worker_{worker_id}_crypto_ws_ticks.db" \
  --logs-dir "../logs" \
  --plan-output "data/orchestrator_plan.json" \
  --worker-log-level INFO \
  --log-level INFO
```

### 8.2 Staging (3x 1h-Kontingent)

```bash
for offset in 0 1 2; do
  python 2_staging/staging_exporter.py \
    --hours 1 \
    --start-relative-from-hour "$offset" \
    --input-glob "data/worker_*_crypto_ws_ticks.db" \
    --output-dir "data/staging" \
    --progress \
    --progress-interval-seconds 20
done
```

### 8.3 Cleansing + Core + Marts

```bash
for sdb in data/staging/staging_export_*_last_1h_from_*h.db; do
  base="$(basename "$sdb" .db)"
  cdb="data/cleansing/cleaned_${base}_60s.db"
  core="data/core/core_kpi_${base}.db"

  python 3_cleansing/cleansing_resample.py \
    --input-db "$sdb" \
    --output-db "$cdb" \
    --workers 4 \
    --progress \
    --progress-interval-seconds 30

  python 4_core/build_core_db.py \
    --staging-db "$sdb" \
    --cleansing-db "$cdb" \
    --output-db "$core" \
    --views-sql 4_core/core_kpi_views.sql \
    --progress \
    --progress-interval-seconds 30

  python 5_marts/build_dashboard_cache.py \
    --db-path "$core" \
    --apply-mart-views \
    --progress \
    --progress-interval-seconds 30
done
```

### 8.4 Forecasting + Dashboard

```bash
LATEST_CDB="$(ls -1t data/cleansing/cleaned_staging_export_*_last_1h_from_*h_60s.db | head -n1)"
LATEST_BASE="$(basename "$LATEST_CDB" .db)"
LATEST_STAGING_BASE="${LATEST_BASE#cleaned_}"
LATEST_STAGING_BASE="${LATEST_STAGING_BASE%_60s}"
LATEST_CORE="data/core/core_kpi_${LATEST_STAGING_BASE}.db"

python 6_forecasting/train_staging_models_and_forecasts.py \
  --staging-glob "data/worker_*_crypto_ws_ticks.db" \
  --cleansing-db "$LATEST_CDB" \
  --forecast-db "$LATEST_CORE" \
  --model-dir "data/forecasting/models" \
  --progress \
  --progress-interval-seconds 30

CORE_DB_PATH="$LATEST_CORE" streamlit run 5_marts/dashboard_mvp_app.py
```

### 8.5 Optionale Renderings

```bash
./render_mermaid_all.sh svg ../diagrams
./render_markdown_all.sh ../docs
```

## 9) Pipeline-Diagramm

- `diagrams/0_overview/uml_flow_end_to_end_pipeline_subgraphs.mmd`
