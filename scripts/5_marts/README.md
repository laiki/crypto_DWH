# Mart Layer SQL

## Files
- `mart_dashboard_views.sql`: creates dashboard-ready mart views on top of Core KPIs.
- `dashboard_query_templates.sql`: query templates with default filter/sort behavior for dashboard panels.

## Related Diagram Files
- `diagrams/5_marts/uml_sequence_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_activity_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_er_mart_dashboard_extracts.mmd`
- `diagrams/5_marts/uml_deployment_mart_dashboard_extracts.mmd`

## View Set
`mart_dashboard_views.sql` creates these mart view names:
- `vw_mart_dashboard_platform_quality_daily`
- `vw_mart_dashboard_platform_quality_hourly`
- `vw_mart_dashboard_price_deviation_daily`
- `vw_mart_dashboard_price_deviation_hourly`
- `vw_mart_latest_cleansing_run`
- `vw_mart_dashboard_price_curve_24h_binance`

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
