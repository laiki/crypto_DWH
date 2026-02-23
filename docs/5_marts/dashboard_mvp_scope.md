# Dashboard MVP Scope (Phase 1)

Last updated: 2026-02-23

## Purpose
Define a fixed MVP dashboard scope that visualizes already collected and computed KPI metrics from the mart layer.

Primary objective:
- deliver a runnable dashboard with stable SQL inputs and no ad-hoc KPI transformations in UI code.

## Scope Decision
Dashboard implementation is prioritized before forecasting feature engineering.

Reason:
- validate data quality and KPI usefulness earlier through direct visual inspection
- reduce feedback loop time for ingestion/cleansing/core tuning

## MVP Panels and Data Mapping

1. Symbol Selector
- user selects one cryptocurrency symbol (for example `BTC/USDT`)
- source query:
  - distinct symbols from
    - `vw_mart_dashboard_price_curve_24h_binance`
    - `vw_mart_dashboard_price_deviation_daily`

2. Price Curve Last 24h (Binance)
- goal: visualize the selected symbol time series
- source view:
  - `vw_mart_dashboard_price_curve_24h_binance`
- default filter:
  - `symbol = :symbol`
- default order:
  - `point_index_asc ASC`

3. Maximum Price Deviation Snapshot (Daily)
- goal: show maximum cross-exchange spread metrics for selected symbol
- source view:
  - `vw_mart_dashboard_price_deviation_daily`
- default filter:
  - latest `kpi_date_utc`
  - `symbol = :symbol`
- key fields:
  - `max_price_diff_abs`
  - `max_price_diff_pct`
  - `avg_price_diff_abs`
  - `avg_price_diff_pct`
  - `max_diff_exchange_pair`
  - `max_diff_bucket_start_utc`

4. Platform Quality Snapshot (Daily)
- goal: compare exchange quality for latency/disconnect behavior
- source view:
  - `vw_mart_dashboard_platform_quality_daily`
- default filter:
  - latest `kpi_date_utc`
- default order:
  - `default_quality_rank ASC`, `exchange_id ASC`
- key fields:
  - `min_latency_ms`
  - `max_latency_ms`
  - `avg_latency_ms`
  - `disconnect_count`
  - `update_frequency_hz`
  - `symbols_covered`

## Out of Scope (MVP)
- forecasting model outputs
- multi-exchange price curve panel (beyond Binance baseline)
- user authentication/authorization
- write-back workflows
- advanced alerting

## Technical Contract
- UI framework: `streamlit`
- data access: read-only SQLite queries against Core DB with mart views applied
- required DB objects:
  - `vw_mart_dashboard_platform_quality_daily`
  - `vw_mart_dashboard_price_deviation_daily`
  - `vw_mart_dashboard_price_curve_24h_binance`

## Done Criteria
1. One command starts dashboard locally.
2. Symbol selector is populated from mart views.
3. All three MVP panels render from real SQLite data.
4. Missing-view errors are shown with actionable setup hints.
5. No panel computes KPI logic outside SQL mart definitions.

## Immediate Next Steps After MVP
1. Add SQL access smoke/integration tests for dashboard data access.
2. Add operational refresh runbook:
   - `core_pipeline` build/validate
   - apply `mart_dashboard_views.sql`
   - restart/refresh dashboard process
