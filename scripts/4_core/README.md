# Core Layer SQL

## Files
- `build_core_db.py`: builds a query-ready Core DB from staging + cleansing sources and applies KPI views.
- `core_kpi_views.sql`: creates Core KPI views.
- `core_kpi_assertions.sql`: runs validation checks against the KPI views.
- `core_validation_runner.py`: applies views, executes assertions, and writes JSON/Markdown reports.
- `core_pipeline.py`: orchestrates build + validate phases (`fast`, `full`, `both`).

## Related Diagrams
- `diagrams/4_core/uml_sequence_core_validation_runner.mmd`
- `diagrams/4_core/uml_activity_core_validation_runner.mmd`
- `diagrams/4_core/uml_er_core_validation_runner.mmd`
- `diagrams/4_core/uml_sequence_core_build_db.mmd`
- `diagrams/4_core/uml_activity_core_build_db.mmd`
- `diagrams/4_core/uml_er_core_build_db.mmd`
- `diagrams/4_core/uml_state_core_build_db.mmd`
- `diagrams/4_core/dfd_core_build_db.mmd`
- `diagrams/4_core/uml_deployment_core_build_db.mmd`
- `diagrams/4_core/uml_sequence_core_pipeline_fast_full.mmd`
- `diagrams/4_core/uml_activity_core_pipeline_fast_phase.mmd`
- `diagrams/4_core/uml_activity_core_pipeline_full_phase.mmd`
- `diagrams/4_core/uml_state_core_pipeline_modes.mmd`
- `diagrams/4_core/uml_sequence_core_kpi_views_smoke_test.mmd`
- `diagrams/4_core/uml_activity_core_kpi_views_smoke_test.mmd`
- `diagrams/4_core/uml_er_core_kpi_views_smoke_test.mmd`
- `diagrams/4_core/uml_deployment_core_kpi_views_smoke_test.mmd`

## Script Boundaries
- `docs/4_core/core_scripts_responsibilities.md`

## Required Inputs
Validation script expects:
- staging DB with:
  - `market_ticks`
  - `connection_events`
- cleansing DB with:
  - `cleansed_market`

Legacy single-DB mode is still available via `--db-path`.

## View Set
`core_kpi_views.sql` creates these stable view names:
- `vw_core_latency_samples`
- `vw_core_kpi_latency_daily`
- `vw_core_kpi_latency_hourly`
- `vw_core_update_intervals`
- `vw_core_kpi_update_frequency_daily`
- `vw_core_kpi_update_frequency_hourly`
- `vw_core_kpi_connection_drops_daily`
- `vw_core_kpi_connection_drops_hourly`
- `vw_core_price_deviation_aligned`
- `vw_core_kpi_price_deviation_daily`
- `vw_core_kpi_price_deviation_hourly`
- `vw_core_kpi_daily_exchange_symbol`
- `vw_core_kpi_daily_exchange`
- `vw_core_kpi_hourly_exchange_symbol`
- `vw_core_kpi_hourly_exchange`

## Example Execution
Build Core DB artifact:

```bash
python scripts/4_core/build_core_db.py \
  --staging-db data/staging/staging_export_YYYYMMDD_HHMMSS_last_24h.db \
  --cleansing-db data/cleansing/cleaned_staging_export_YYYYMMDD_HHMMSS_last_24h_60s.db \
  --output-db data/core/core_kpi.db \
  --views-sql scripts/4_core/core_kpi_views.sql
```

Run combined pipeline (fast + full):

```bash
python scripts/4_core/core_pipeline.py \
  --phase both \
  --staging-db data/staging/staging_export_YYYYMMDD_HHMMSS_last_24h.db \
  --cleansing-db data/cleansing/cleaned_staging_export_YYYYMMDD_HHMMSS_last_24h_60s.db \
  --output-db data/core/core_kpi.db \
  --kpi-date 2026-02-21 \
  --cleansing-run-id cleansing_20260221_164315_60s
```

Create views:

```bash
sqlite3 data/core/core_kpi.db ".read scripts/4_core/core_kpi_views.sql"
```

Run assertions:

```bash
sqlite3 -header -column data/core/core_kpi.db ".read scripts/4_core/core_kpi_assertions.sql"
```

Run full validation with report output:

```bash
python scripts/4_core/core_validation_runner.py \
  --staging-db data/staging/staging_export_YYYYMMDD_HHMMSS_last_24h.db \
  --cleansing-db data/cleansing/cleaned_staging_export_YYYYMMDD_HHMMSS_last_24h_60s.db
```

Performance-oriented execution example:

```bash
python scripts/4_core/core_validation_runner.py \
  --staging-db data/staging/staging_export_YYYYMMDD_HHMMSS_last_24h.db \
  --cleansing-db data/cleansing/cleaned_staging_export_YYYYMMDD_HHMMSS_last_24h_60s.db \
  --kpi-date 2026-02-21 \
  --cleansing-run-id cleansing_20260221_164315_60s \
  --skip-view-row-counts
```

Runtime options:
- `--skip-view-row-counts`: skips `COUNT(*)` over all KPI views.
- `--kpi-date YYYY-MM-DD`: filters source aliases to one UTC date.
- `--cleansing-run-id <run_id>`: filters cleansing source alias to one run.

Smoke test for Core KPI views:

```bash
python scripts/4_core/smoke_test_core_kpi_views.py
```

Optional SQL path override:

```bash
python scripts/4_core/smoke_test_core_kpi_views.py \
  --views-sql scripts/4_core/core_kpi_views.sql
```

Build options:
- `--kpi-date YYYY-MM-DD`: builds scoped Core artifact for one UTC date.
- `--cleansing-run-id <run_id>`: limits copied `cleansed_market` rows to one run.
- `--overwrite/--no-overwrite`: controls output DB replacement.
- `--vacuum`: runs `VACUUM` on output DB after build.

Pipeline phase behavior:
- `fast`:
  - build with optional scope filters (`--kpi-date`, `--cleansing-run-id`)
  - validate with same scope and `--skip-view-row-counts`
- `full`:
  - build without scope filters
  - validate without scope filters and with view row counts
- `both`:
  - executes `fast` first, then `full`

Default report output directory:
- `logs/core_validation/`
