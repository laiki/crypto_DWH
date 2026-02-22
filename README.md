# Crypto Data Engineering Project

## Goal
This project builds a data warehouse for cryptocurrencies and a dashboard to visualize key KPIs.

Focus:
- Price comparison of the same coins across multiple exchanges
- Latency and update frequency of real-time data
- Connection interruptions per platform
- Forecasts per currency and exchange

## Tech Stack (planned)
- Python
- CCXT (exchange data ingestion)
- SQLite (raw data / intermediate stages)
- Scikit-learn + additional time-series models (forecasting)
- Mermaid for architecture diagrams

## Target Architecture
1. ETL/Ingestion:
- Service process collects market data via CCXT
- Reception timestamp is stored per record

2. Staging:
- Daily export of the last 24h per exchange into a separate staging database

3. Cleansing:
- Handle data gaps
- Identify/flag outliers

4. Core:
- Compute KPIs (update frequency, latency, disconnects/day)

5. Marts:
- Compare the same currency across exchanges at aligned timestamps
- KPI analysis for dashboard consumption

## Dashboard (MVP)
- User selects a cryptocurrency
- Price chart for the last 24h (starting with Binance)
- Maximum price differences between exchanges
- Min/Max latency per platform

## Mermaid Diagrams
Architecture diagrams are maintained as `.mmd` files.

Stage structure:
- `diagrams/0_overview/`
- `diagrams/1_ingestion/`
- `diagrams/2_staging/`
- `diagrams/3_cleansing/`
- `diagrams/4_core/`
- `diagrams/5_marts/`
- `diagrams/6_forecasting/`

Selected ETL/Staging use-case diagrams:
- `diagrams/1_ingestion/uml_sequence_ingestion_failure_recovery.mmd`
- `diagrams/1_ingestion/uml_sequence_orchestrator_worker_sharding.mmd`
- `diagrams/1_ingestion/uml_activity_symbol_normalization.mmd`
- `diagrams/1_ingestion/uml_activity_compute_exchange_score.mmd`
- `diagrams/1_ingestion/uml_activity_compute_exchange_score_swimlanes.mmd`
- `diagrams/1_ingestion/uml_sequence_runtime_supervision_restart_policy.mmd`
- `diagrams/1_ingestion/uml_activity_runtime_supervision_loop.mmd`
- `diagrams/1_ingestion/uml_state_worker_runtime_supervision.mmd`
- `diagrams/2_staging/uml_sequence_staging_incremental_export.mmd`
- `diagrams/2_staging/uml_usecase_ad_hoc_analysis.mmd`

If `mermaid-cli` is installed:
```powershell
mmdc -i diagrams/0_overview/uml_architecture.mmd -o diagrams/0_overview/uml_architecture.svg
mmdc -i diagrams/1_ingestion/uml_sequence_ingestion.mmd -o diagrams/1_ingestion/uml_sequence_ingestion.svg
mmdc -i diagrams/1_ingestion/uml_sequence_ingestion_failure_recovery.mmd -o diagrams/1_ingestion/uml_sequence_ingestion_failure_recovery.svg
mmdc -i diagrams/1_ingestion/uml_sequence_orchestrator_worker_sharding.mmd -o diagrams/1_ingestion/uml_sequence_orchestrator_worker_sharding.svg
mmdc -i diagrams/1_ingestion/uml_activity_symbol_normalization.mmd -o diagrams/1_ingestion/uml_activity_symbol_normalization.svg
mmdc -i diagrams/1_ingestion/uml_activity_compute_exchange_score.mmd -o diagrams/1_ingestion/uml_activity_compute_exchange_score.svg
mmdc -i diagrams/1_ingestion/uml_activity_compute_exchange_score_swimlanes.mmd -o diagrams/1_ingestion/uml_activity_compute_exchange_score_swimlanes.svg
mmdc -i diagrams/1_ingestion/uml_sequence_runtime_supervision_restart_policy.mmd -o diagrams/1_ingestion/uml_sequence_runtime_supervision_restart_policy.svg
mmdc -i diagrams/1_ingestion/uml_activity_runtime_supervision_loop.mmd -o diagrams/1_ingestion/uml_activity_runtime_supervision_loop.svg
mmdc -i diagrams/1_ingestion/uml_state_worker_runtime_supervision.mmd -o diagrams/1_ingestion/uml_state_worker_runtime_supervision.svg
mmdc -i diagrams/2_staging/uml_sequence_staging_incremental_export.mmd -o diagrams/2_staging/uml_sequence_staging_incremental_export.svg
mmdc -i diagrams/2_staging/uml_usecase_ad_hoc_analysis.mmd -o diagrams/2_staging/uml_usecase_ad_hoc_analysis.svg
mmdc -i diagrams/4_core/uml_er_core.mmd -o diagrams/4_core/uml_er_core.svg
mmdc -i diagrams/6_forecasting/uml_forecasting_pipeline.mmd -o diagrams/6_forecasting/uml_forecasting_pipeline.svg
```

## Project Status
Current: ingestion/staging/cleansing implemented; Core KPI catalog and SQL views defined.

## Script Documentation
Detailed English documentation for all ingestion/orchestration scripts is available at:
- `scripts/1_ingestion/README.md`

## Staging Exporter
The staging exporter creates a bounded staging snapshot from worker ingestion databases.

Script:
- `scripts/2_staging/staging_exporter.py`

Key behavior:
- Exports the last `N` hours from `worker_*.db` files.
- Supports output formats: `sqlite` (default), `csv`, `json`.
- Supports filters for exchanges and assets.
- Prints unique exchange and asset lists from all selected source DBs.
- Writes run metadata as JSON for traceability.
- Supports incremental export mode with persisted watermark state.

Examples:
```bash
python scripts/2_staging/staging_exporter.py --hours 24
python scripts/2_staging/staging_exporter.py --hours 24 --output-format csv
python scripts/2_staging/staging_exporter.py --hours 12 --exchanges binance,kraken --assets BTC,ETH
python scripts/2_staging/staging_exporter.py --hours 6 --list-only
python scripts/2_staging/staging_exporter.py --hours 1 --incremental
python scripts/2_staging/staging_exporter.py --hours 1 --incremental --state-path data/staging/custom_state.json
```

Default input pattern:
- `data/worker_*_crypto_ws_ticks.db`

Default output location:
- `data/staging/`

Output naming:
- `<prefix>_<YYYYMMDD_HHMMSS>_last_<hours>h...`
- Includes UTC timestamp and export window hours.

Run metadata contract:
- `docs/staging_run_contract.md`

Incremental state file:
- Default: `data/staging/staging_export_state.json`
- The state is keyed by source scope (input glob + filters) unless `--state-key` is set.
- Contract: `docs/staging_state_contract.md`

## Cleansing Resampling
The cleansing step creates a comparable 1-minute time grid per `(exchange_id, symbol)`.

Script:
- `scripts/3_cleansing/cleansing_resample.py`

Cleansing rules:
- `observed`: bucket contains one or more ticks, use last tick in bucket.
- `forward_fill`: bucket has no tick, reuse last known value within `max_forward_fill_s`.
- `interpolation` (optional): bucket has no tick, use linear interpolation between previous and next value when both sides are within `max_forward_fill_s`; otherwise fallback to `forward_fill` behavior.
- `missing`: bucket has no eligible value for fill.

Execution model:
- Supports parallel pair processing with `--workers`.
- Uses a single-writer output model (worker queue -> one writer) to avoid SQLite write contention.

Data quality checks (`--enable-dq`, default enabled):
- `NULL` ingestion timestamp and `NULL` effective price checks.
- Invalid timestamp parse checks.
- Non-positive price checks (`<= 0`).
- Negative numeric value checks (`price`, `bid`, `ask`, `last`, `base_volume`, `quote_volume`).
- `bid > ask` consistency checks.
- Duplicate tick checks (same pair + timestamp + effective price).
- Outlier return checks on observed bins (`--dq-outlier-threshold-pct`).

Output tables:
- `cleansed_market`
- `cleansing_pair_summary`
- `cleansing_run_metadata`
- `cleansing_dq_summary`
- `cleansing_dq_run_summary`

`cleansed_market` also stores run parameters per row:
- `bin_seconds`
- `max_forward_fill_s`
- `fill_method_mode`

Process JSON export:
- For each run, a JSON file is created next to the output DB with name `<output_db_name>.json` (for example `cleaned_xxx.db.json`).
- It contains all run data from:
  - `cleansing_run_metadata`
  - `cleansing_pair_summary`
  - `cleansed_market`
  - `cleansing_dq_summary`
  - `cleansing_dq_run_summary`

Default output path:
- If input DB is under `.../staging/...`: `<staging_parent>/cleansing/cleaned_<input_name>_<bin_seconds>s.db`
- Otherwise: `<input_db_dir>/cleansing/cleaned_<input_name>_<bin_seconds>s.db`

Default log path:
- `<output_db_name>.log` (derived from `<output_db_name>.json`)

Examples:
```bash
python scripts/3_cleansing/cleansing_resample.py --input-db data/staging/staging_export_20260221_120000_last_24h.db
python scripts/3_cleansing/cleansing_resample.py --input-db data/staging/staging_export_20260221_120000_last_24h.db --exchanges binance,kraken --symbols BTC/USDT,ETH/USDT
python scripts/3_cleansing/cleansing_resample.py --input-db data/staging/staging_export_20260221_120000_last_24h.db --fill-method interpolation
python scripts/3_cleansing/cleansing_resample.py --input-db data/staging/staging_export_20260221_120000_last_24h.db --workers 4 --enable-dq --dq-outlier-threshold-pct 0.15
```

Approach documentation:
- `docs/cleansing_area_approach.md`

Operational insights are tracked at:
- `docs/insights.md`

## Core KPI Layer
Core KPI definitions:
- `docs/core_kpi_catalog.md`

Core SQL views and assertions:
- `scripts/4_core/core_kpi_views.sql`
- `scripts/4_core/core_kpi_assertions.sql`
- `scripts/4_core/README.md`

Cleansing to Core interface contract:
- `docs/cleansing_core_handoff_contract.md`

Validation checklist:
- `docs/core_kpi_validation_checks.md`

Remote validation runbook:
- `docs/core_remote_validation_runbook.md`

Remote acceptance checklist:
- `docs/core_remote_acceptance_checklist.md`

Validation runner:
- `scripts/4_core/core_remote_validation.py`

## SQL Snippets
Connection drop statistics grouped by exchange and sorted by frequency:

```sql
SELECT
  exchange_id,
  COUNT(*) AS disconnect_count
FROM connection_events
WHERE event_type = 'disconnect'
GROUP BY exchange_id
ORDER BY disconnect_count DESC, exchange_id ASC;
```

Update interval statistics per symbol and exchange (min/avg/max in seconds):

```sql
WITH ordered_ticks AS (
  SELECT
    exchange_id,
    symbol,
    ingestion_ts_utc,
    LAG(ingestion_ts_utc) OVER (
      PARTITION BY exchange_id, symbol
      ORDER BY ingestion_ts_utc
    ) AS prev_ts
  FROM market_ticks
),
intervals AS (
  SELECT
    exchange_id,
    symbol,
    (julianday(ingestion_ts_utc) - julianday(prev_ts)) * 86400.0 AS interval_s
  FROM ordered_ticks
  WHERE prev_ts IS NOT NULL
)
SELECT
  exchange_id,
  symbol,
  COUNT(*) AS interval_count,
  ROUND(MIN(interval_s), 3) AS min_interval_s,
  ROUND(AVG(interval_s), 3) AS avg_interval_s,
  ROUND(MAX(interval_s), 3) AS max_interval_s
FROM intervals
GROUP BY exchange_id, symbol
ORDER BY exchange_id, symbol;
```

Update interval statistics per symbol for a specific exchange (`:exchange_id`):

```sql
WITH ordered_ticks AS (
  SELECT
    exchange_id,
    symbol,
    ingestion_ts_utc,
    LAG(ingestion_ts_utc) OVER (
      PARTITION BY exchange_id, symbol
      ORDER BY ingestion_ts_utc
    ) AS prev_ts
  FROM market_ticks
  WHERE exchange_id = :exchange_id
),
intervals AS (
  SELECT
    exchange_id,
    symbol,
    (julianday(ingestion_ts_utc) - julianday(prev_ts)) * 86400.0 AS interval_s
  FROM ordered_ticks
  WHERE prev_ts IS NOT NULL
)
SELECT
  exchange_id,
  symbol,
  COUNT(*) AS interval_count,
  ROUND(MIN(interval_s), 3) AS min_interval_s,
  ROUND(AVG(interval_s), 3) AS avg_interval_s,
  ROUND(MAX(interval_s), 3) AS max_interval_s
FROM intervals
GROUP BY exchange_id, symbol
ORDER BY symbol;
```

Using `:exchange_id` in the `sqlite3` shell:

```sql
.parameter init
.parameter set :exchange_id 'binance'
.parameter list
```

Note:
- If your local `sqlite3` version does not support `.parameter`, use a literal value directly:
  - `WHERE exchange_id = 'binance'`

Next steps:
1. Build reproducible Core DB runner for staging + cleansing outputs
2. Add automated smoke tests for Core SQL views
3. Define mart views for dashboard panels
4. Implement first dashboard layout

Session task backlog:
- `docs/next_session_tasks.md`
