# Mart KPI Extracts for Dashboard

Last updated: 2026-02-25

## Purpose
This document defines dashboard-ready extracts on top of the Core layer.

Goal:
- dashboard reads ready-to-visualize datasets
- no hard dependency on fixed 24h/day windows
- stable default filter and sort behavior

## Source and Artifacts
- source SQL:
  - `scripts/5_marts/mart_dashboard_views.sql`
  - `scripts/5_marts/dashboard_query_templates.sql`
- required upstream:
  - Core KPI views from `scripts/4_core/core_kpi_views.sql`
  - `cleansed_market` table in same Core DB
- design note:
  - `docs/5_marts/symbol_deviation_mart_use_case.md`

## Dashboard Panels and Extracts

### Panel 1: Run + Window Selection
- source table: `cleansed_market`
- key fields:
  - `run_id`
  - run coverage (`MIN(bucket_start_utc)`, `MAX(bucket_start_utc)`)

### Panel 2: Price Curve (Window, Multi-Exchange)
- source table: `cleansed_market`
- required filters:
  - `run_id = :run_id`
  - `symbol = :symbol`
  - `bucket_start_utc BETWEEN :window_start_utc AND :window_end_utc`
  - valid cleansed points only (`price IS NOT NULL`, `is_missing = 0`, `is_stale = 0`)
- default order:
  - `bucket_epoch_s ASC`, `exchange_id ASC`
- key columns:
  - `run_id`
  - `exchange_id`
  - `symbol`
  - `bucket_start_utc`
  - `bucket_epoch_s`
  - `price_close`
  - `fill_method`

### Panel 3: Price Deviation (Window)
- source table: `cleansed_market` (bucket-level cross-exchange aggregation)
- required filters:
  - `run_id = :run_id`
  - `symbol = :symbol`
  - `bucket_start_utc BETWEEN :window_start_utc AND :window_end_utc`
  - valid cleansed points only (`price IS NOT NULL`, `is_missing = 0`, `is_stale = 0`)
- key columns:
  - `bucket_start_utc`
  - `bucket_epoch_s`
  - `exchange_count`
  - `max_price`
  - `min_price`
  - `price_diff_abs`
  - `price_diff_pct`
  - `max_diff_exchange_pair`

### Panel 3b: Symbol Start Page (Violin Grid)
- goal:
  - show one violin distribution per symbol for bucket-level `%` close-price deviation
  - allow quick jump into symbol detail page
- source priority:
  - cache table: `dash_cache_symbol_deviation_bucket`
  - fallback view: `vw_mart_dashboard_symbol_deviation_bucket`
  - fallback query: raw aggregation on `cleansed_market`
- required filters:
  - `run_id = :run_id`
  - `bucket_start_utc BETWEEN :window_start_utc AND :window_end_utc`
- key columns:
  - `run_id`
  - `symbol`
  - `bucket_start_utc`
  - `bucket_epoch_s`
  - `exchange_count`
  - `price_diff_pct`
  - `max_diff_exchange_pair`

### Panel 4: Platform Quality (Daily)
- source priority:
  - cache table: `dash_cache_platform_quality_daily_latest`
  - fallback view: `vw_mart_dashboard_platform_quality_daily` (latest day)
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
  - `default_quality_score`
  - `default_quality_rank`

## Default Quality Ranking Formula
`default_quality_rank` is calculated from `default_quality_score` (rank 1 = best quality).

`default_quality_score` is a weighted score from six components:
1. min latency component (lower `min_latency_ms` is better, capped at 1000 ms)
2. avg latency component (lower `avg_latency_ms` is better, capped at 10000 ms)
3. max latency component (lower `max_latency_ms` is better, capped at 600000 ms)
4. update frequency component (higher `update_frequency_hz` is better)
5. disconnect component (lower `disconnect_count` is better, capped at 10 disconnects)
6. symbol coverage component (higher `symbols_covered` is better)

Weights:
- `min_latency_ms`: `0.15`
- `avg_latency_ms`: `0.35`
- `max_latency_ms`: `0.30`
- `update_frequency_hz`: `0.10`
- `disconnect_count`: `0.08`
- `symbols_covered`: `0.02`

Normalization strategy:
- Latency and disconnect components use absolute cap-based scaling to `[0, 1]` (cap breach => `0.0`).
- Update frequency and symbol coverage use per-partition (`kpi_date_utc` or `kpi_hour_utc`) min-max scaling.
- If all exchanges share the same value for update frequency or symbol coverage, that component is set to `1.0` for all rows.

## Operational Usage
1. Build Core DB and apply Core views.
2. Apply mart views:
   - `.read scripts/5_marts/mart_dashboard_views.sql`
3. Use templates in:
  - `scripts/5_marts/dashboard_query_templates.sql`
4. Start dashboard:
   - `streamlit run scripts/5_marts/dashboard_mvp_app.py`

## Done-Criteria Mapping
- exact extract columns are defined in SQL templates and mart views
- run/window filters remove fixed 24h/day constraints for price curve and spread analysis
- dashboard can visualize 1h staging exports directly when loaded into `cleansed_market`
- symbol start page supports distribution-based symbol triage via violin plots
