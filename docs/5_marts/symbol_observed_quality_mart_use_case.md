# Symbol Observed Coverage Mart Use Case

Last updated: 2026-02-27

## Purpose
This mart use case adds an explicit data-quality KPI for cross-exchange symbol comparison.

Problem:
- Large spread outliers are often driven by sparse/non-continuous quote publication on some exchanges.
- A symbol comparison is less trustworthy if an exchange contributes only a small fraction of truly observed points.

Goal:
- quantify observed quote coverage per `run_id + symbol + exchange_id`
- compare each exchange against the symbol-specific maximum observed count
- provide dashboard-ready quality bands for filtering and drill-down

## Mart View
- View name: `vw_mart_dashboard_symbol_observed_quality_base`
- Source: `cleansed_market`
- Created in: `scripts/5_marts/mart_dashboard_views.sql`

### Row Granularity
- one row per valid cleansed market point:
  - `run_id`
  - `symbol`
  - `exchange_id`
  - `bucket_start_utc`
  - `bucket_epoch_s`

### Exported Columns
- `run_id`
- `symbol`
- `exchange_id`
- `bucket_start_utc`
- `bucket_epoch_s`
- `observed_flag` (`1` if `fill_method = 'observed'`, else `0`)

### Validity Filter in the View
Rows are restricted to valid cleansed points:
- `run_id IS NOT NULL`
- `symbol IS NOT NULL`
- `exchange_id IS NOT NULL`
- `bucket_start_utc IS NOT NULL`
- `bucket_epoch_s IS NOT NULL`
- `price IS NOT NULL`
- `is_missing = 0`
- `is_stale = 0`

## KPI Definition (Dashboard Query Level)
For selected `run_id` and window (`window_start_utc`, `window_end_utc`):

1. Aggregate per `symbol + exchange_id`:
- `total_points = COUNT(*)`
- `observed_points = SUM(observed_flag)`

2. Compute symbol reference:
- `max_observed_points_for_symbol = MAX(observed_points) OVER (PARTITION BY symbol)`

3. Compute coverage ratio:
- `observed_vs_max_pct = 100 * observed_points / max_observed_points_for_symbol`

4. Assign quality bands:
- `0-50%`
- `50-75%`
- `75-90%`
- `90-100%`

## Dashboard Integration
- Sidebar config provides a multi-select filter for quality bands.
- Symbol availability, exchange inclusion, and downstream spread visualizations are filtered by selected bands.
- Platform quality tab includes a dedicated observed-coverage section (metrics + bar chart + table).

## Operational Flow
1. Apply mart views:
   - `sqlite3 data/core/core_kpi.db ".read scripts/5_marts/mart_dashboard_views.sql"`
2. Build cache (optional but recommended for other panels):
   - `python scripts/5_marts/build_dashboard_cache.py --db-path data/core/core_kpi.db`
3. Start dashboard:
   - `streamlit run scripts/5_marts/dashboard_mvp_app.py`

## Design Notes
- The KPI is intentionally window-aware and run-aware.
- The mart view acts as a stable semantic base, while window-specific aggregation remains dynamic in dashboard SQL.
- This avoids hardcoding fixed windows and keeps filters consistent with other dynamic run/window panels.
