# Core Layer SQL

## Files
- `core_kpi_views.sql`: creates Core KPI views.
- `core_kpi_assertions.sql`: runs validation checks against the KPI views.
- `core_remote_validation.py`: applies views, executes assertions, and writes JSON/Markdown reports.

## Required Input Tables
Both scripts expect the following tables in the same SQLite database:
- `market_ticks`
- `connection_events`
- `cleansed_market`

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
python scripts/4_core/core_remote_validation.py --db-path data/core/core_kpi.db
```

Default report output directory:
- `logs/core_validation/`
