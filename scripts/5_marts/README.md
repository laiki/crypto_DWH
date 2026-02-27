# Mart Layer SQL

## Files
- `mart_dashboard_views.sql`: creates dashboard-ready mart views on top of Core KPIs.
- `dashboard_query_templates.sql`: query templates with default filter/sort behavior for dashboard panels.
- `dashboard_mvp_app.py`: Streamlit dashboard MVP with dynamic run/window analysis on `cleansed_market`.
- `build_dashboard_cache.py`: materializes fast dashboard cache tables from mart views.

## Related Diagram Files
- `diagrams/5_marts/uml_sequence_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_activity_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_er_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_deployment_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_sequence_dashboard_mvp_runtime.mmd`
- `diagrams/5_marts/uml_activity_dashboard_cache_build.mmd`
- `diagrams/5_marts/uml_sequence_dashboard_cache_refresh_runtime.mmd`
- `diagrams/5_marts/uml_er_dashboard_cache_tables.mmd`
- `diagrams/5_marts/uml_deployment_dashboard_cache_runtime.mmd`

## View Set
`mart_dashboard_views.sql` creates these mart view names:
- `vw_mart_dashboard_platform_quality_daily`
- `vw_mart_dashboard_platform_quality_hourly`
- `vw_mart_dashboard_price_deviation_daily`
- `vw_mart_dashboard_price_deviation_hourly`
- `vw_mart_dashboard_symbol_deviation_bucket`
- `vw_mart_latest_cleansing_run`
- `vw_mart_dashboard_price_curve_24h_binance`

Platform quality ranking:
- `default_quality_score`: weighted score over min/avg/max latency, update frequency, disconnect count, and symbol coverage.
  - latency caps: min `1000 ms`, avg `10000 ms`, max `600000 ms`
  - weights: avg latency `35%`, max latency `30%`, min latency `15%`, update frequency `10%`, disconnects `8%`, symbols `2%`
- `default_quality_rank`: dense rank by `default_quality_score DESC` (rank `1` is best quality).

## Required Upstream Objects
- Core KPI views from `scripts/4_core/core_kpi_views.sql`
- `cleansed_market` table in the same SQLite database (with `run_id` column)

## Apply Mart Views
```bash
sqlite3 data/core/core_kpi.db ".read scripts/5_marts/mart_dashboard_views.sql"
```

## Use Dashboard Query Templates
```bash
sqlite3 -header -column data/core/core_kpi.db ".read scripts/5_marts/dashboard_query_templates.sql"
```

If your SQL client does not support `:symbol` parameters, replace them directly.

## Run Dashboard MVP App
Install dependencies from root `requirements.txt`, then start:

```bash
streamlit run scripts/5_marts/dashboard_mvp_app.py
```

In the sidebar, provide the Core SQLite DB path (default: `data/core/core_kpi.db`).

Runtime behavior:
- symbol start page can render violin distributions of `price_diff_pct` per symbol (selected run/window)
- price curve and price deviation are computed for a selected `run_id` and selectable UTC window
- no fixed 24h or daily-only restriction for these two panels
- platform quality uses cache table when available, otherwise falls back to mart view
- symbol deviation panels use cache table when available, otherwise fall back to mart view or raw query

## Build Precomputed Dashboard Cache (Recommended)
Cache is recommended for fast platform quality panel loading:

```bash
python scripts/5_marts/build_dashboard_cache.py --db-path data/core/core_kpi.db
```

By default, the cache builder applies `mart_dashboard_views.sql` automatically
before materializing cache tables.

If mart views are already applied and you want to skip that step:

```bash
python scripts/5_marts/build_dashboard_cache.py --db-path data/core/core_kpi.db --no-apply-mart-views
```

Optional compaction:

```bash
python scripts/5_marts/build_dashboard_cache.py --db-path data/core/core_kpi.db --vacuum
```

Created cache tables:
- `dash_cache_platform_quality_daily_latest`
- `dash_cache_price_deviation_daily_latest`
- `dash_cache_price_curve_24h_binance_latest`
- `dash_cache_symbol_deviation_bucket`
- `dash_cache_symbols`
- `dash_cache_refresh_metadata`

The Streamlit app can run without cache tables, but cache improves quality panel performance.
