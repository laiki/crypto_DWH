# Core Layer SQL

## Files
- `core_kpi_views.sql`: creates Core KPI views.
- `core_kpi_assertions.sql`: runs validation checks against the KPI views.
- `core_validation_runner.py`: applies views, executes assertions, and writes JSON/Markdown reports.

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

Default report output directory:
- `logs/core_validation/`
