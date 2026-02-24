# Mart Layer SQL

## Files
- `mart_dashboard_views.sql`: creates dashboard-ready mart views on top of Core KPIs.
- `dashboard_query_templates.sql`: query templates with default filter/sort behavior for dashboard panels.
- `dashboard_mvp_app.py`: Streamlit dashboard MVP consuming mart views directly.
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
- `vw_mart_latest_cleansing_run`
- `vw_mart_dashboard_price_curve_24h_binance`

Platform quality ranking:
- `default_quality_score`: combined score over min latency, update frequency, disconnect count, and symbol coverage.
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

## Build Precomputed Dashboard Cache (Required)
The dashboard is operated in cache-only mode for usable response times.
Build cache tables before starting the app:

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
- `dash_cache_symbols`
- `dash_cache_refresh_metadata`

The Streamlit app requires these cache tables and fails fast when they are missing.
