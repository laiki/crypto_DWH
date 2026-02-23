# Mart KPI Extracts for Dashboard

Last updated: 2026-02-22

## Purpose
This document defines dashboard-ready extracts on top of the Core layer.

Goal:
- dashboard reads ready-to-visualize datasets
- no additional KPI transformations in UI/backend
- stable default filter and sort behavior

## Source and Artifacts
- source SQL:
  - `scripts/5_marts/mart_dashboard_views.sql`
  - `scripts/5_marts/dashboard_query_templates.sql`
- required upstream:
  - Core KPI views from `scripts/4_core/core_kpi_views.sql`
  - `cleansed_market` table in same Core DB

## Dashboard Panels and Extracts

### Panel 1: Platform Quality (Daily)
- mart view: `vw_mart_dashboard_platform_quality_daily`
- default filter:
  - latest `kpi_date_utc`
- default order:
  - `default_quality_rank ASC`, `exchange_id ASC`
- key columns:
  - `kpi_date_utc`
  - `exchange_id`
  - `symbols_covered`
  - `avg_latency_ms`
  - `min_latency_ms`
  - `max_latency_ms`
  - `update_frequency_hz`
  - `disconnect_count`
  - `default_quality_rank`

### Panel 2: Platform Quality (Hourly)
- mart view: `vw_mart_dashboard_platform_quality_hourly`
- default filter:
  - latest `kpi_hour_utc`
- default order:
  - `default_quality_rank ASC`, `exchange_id ASC`

### Panel 3: Price Deviation (Daily)
- mart view: `vw_mart_dashboard_price_deviation_daily`
- default filter:
  - latest `kpi_date_utc`
  - optional `symbol`
- default order:
  - `default_deviation_rank ASC`, `symbol ASC`
- key columns:
  - `kpi_date_utc`
  - `symbol`
  - `max_price_diff_abs`
  - `max_price_diff_pct`
  - `avg_price_diff_abs`
  - `avg_price_diff_pct`
  - `max_diff_bucket_start_utc`
  - `max_diff_exchange_pair`

### Panel 4: Price Deviation (Hourly)
- mart view: `vw_mart_dashboard_price_deviation_hourly`
- default filter:
  - latest `kpi_hour_utc`
  - optional `symbol`
- default order:
  - `default_deviation_rank ASC`, `symbol ASC`

### Panel 5: Price Curve Last 24h (Binance)
- mart view: `vw_mart_dashboard_price_curve_24h_binance`
- default filter:
  - required `symbol`
  - latest cleansing `run_id` (resolved by `vw_mart_latest_cleansing_run`)
- default order:
  - `point_index_asc ASC`
- key columns:
  - `run_id`
  - `exchange_id`
  - `symbol`
  - `bucket_start_utc`
  - `price_close`
  - `point_index_asc`

## Operational Usage
1. Build Core DB and apply Core views.
2. Apply mart views:
   - `.read scripts/5_marts/mart_dashboard_views.sql`
3. Use templates in:
   - `scripts/5_marts/dashboard_query_templates.sql`

## Done-Criteria Mapping
- exact extract columns are defined in mart views
- default filter/sort behavior is encoded in rank columns and templates
- dashboard can query mart views directly without extra KPI transformations
