# Core Layer SQL

## Files
- `build_core_db.py`: builds a query-ready Core DB from staging + cleansing sources and applies KPI views.
- `core_kpi_views.sql`: creates Core KPI views.
- `core_kpi_assertions.sql`: runs validation checks against the KPI views.
- `core_validation_runner.py`: applies views, executes assertions, and writes JSON/Markdown reports.

## Related Diagrams
- `diagrams/4_core/uml_sequence_core_validation_runner.mmd`
- `diagrams/4_core/uml_activity_core_validation_runner.mmd`
- `diagrams/4_core/uml_er_core_validation_runner.mmd`

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
- `vw_core_update_intervals`
- `vw_core_kpi_update_frequency_daily`
- `vw_core_kpi_connection_drops_daily`
- `vw_core_price_deviation_aligned`
- `vw_core_kpi_price_deviation_daily`
- `vw_core_kpi_daily_exchange_symbol`
- `vw_core_kpi_daily_exchange`

## Example Execution
Build Core DB artifact:

```bash
python scripts/4_core/build_core_db.py \
  --staging-db data/staging/staging_export_YYYYMMDD_HHMMSS_last_24h.db \
  --cleansing-db data/cleansing/cleaned_staging_export_YYYYMMDD_HHMMSS_last_24h_60s.db \
  --output-db data/core/core_kpi.db \
  --views-sql scripts/4_core/core_kpi_views.sql
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

Build options:
- `--kpi-date YYYY-MM-DD`: builds scoped Core artifact for one UTC date.
- `--cleansing-run-id <run_id>`: limits copied `cleansed_market` rows to one run.
- `--overwrite/--no-overwrite`: controls output DB replacement.
- `--vacuum`: runs `VACUUM` on output DB after build.

Default report output directory:
- `logs/core_validation/`
