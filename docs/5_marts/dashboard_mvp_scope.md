# Dashboard MVP Scope (Phase 1)

Last updated: 2026-02-25

## Purpose
Define a fixed MVP dashboard scope that visualizes already collected and computed KPI metrics from the mart/core layer.

Primary objective:
- deliver a runnable dashboard with stable SQL inputs and no hard-coded 24h/day window assumptions.

## Scope Decision
Dashboard implementation is prioritized before forecasting feature engineering.

Reason:
- validate data quality and KPI usefulness earlier through direct visual inspection
- reduce feedback loop time for ingestion/cleansing/core tuning

## MVP Panels and Data Mapping

1. Run + Time Window Selector
- user selects one cleansing `run_id`
- user selects UTC start/end window inside selected run coverage
- source query:
  - `cleansed_market` grouped by `run_id`

2. Symbol Selector
- user selects one cryptocurrency symbol (for example `BTC/USDT`) for selected run/window
- source query:
  - distinct symbols from `cleansed_market` filtered by `run_id` and window

3. Symbol Start Page (Violin Grid)
- goal: distribution view per symbol before opening detail analysis
- visualization:
  - one violin plot per symbol
  - y-axis: `price_diff_pct` (bucket-level percentage close spread between exchanges)
- source priority:
  - `dash_cache_symbol_deviation_bucket`
  - fallback: `vw_mart_dashboard_symbol_deviation_bucket`
  - fallback: raw query over `cleansed_market`
- default filters:
  - `run_id = :run_id`
  - `bucket_start_utc BETWEEN :window_start_utc AND :window_end_utc`

4. Price Curve (Window, Multi-Exchange)
- goal: visualize the selected symbol time series in selected window
- source table:
  - `cleansed_market`
- default filters:
  - `run_id = :run_id`
  - `bucket_start_utc BETWEEN :window_start_utc AND :window_end_utc`
  - `symbol = :symbol`
  - valid cleansed points only (`price IS NOT NULL`, `is_missing = 0`, `is_stale = 0`)
- default order:
  - `bucket_epoch_s ASC`, `exchange_id ASC`

5. Price Deviation (Window)
- goal: show cross-exchange spread metrics for selected symbol in selected run/window
- source table:
  - `cleansed_market` (bucket-aligned cross-exchange aggregation)
- default filters:
  - `run_id = :run_id`
  - `bucket_start_utc BETWEEN :window_start_utc AND :window_end_utc`
  - `symbol = :symbol`
- key fields:
  - `exchange_count`
  - `max_price_diff_abs`
  - `max_price_diff_pct`
  - `max_diff_exchange_pair`
  - `bucket_start_utc` (series and max timestamp)

6. Platform Quality Snapshot (Daily)
- goal: compare exchange quality for latency/disconnect behavior
- source priority:
  - `dash_cache_platform_quality_daily_latest` (preferred)
  - fallback: `vw_mart_dashboard_platform_quality_daily` (latest day)
- default filter:
  - latest `kpi_date_utc`
- default order:
  - `default_quality_rank ASC`, `exchange_id ASC`
- key fields:
  - `default_quality_score`
  - `default_quality_rank`
  - `min_latency_ms`
  - `max_latency_ms`
  - `avg_latency_ms`
  - `disconnect_count`
  - `update_frequency_hz`
  - `symbols_covered`

Ranking definition:
- `default_quality_rank = 1` means best quality.
- Score dimensions:
  - lower `min_latency_ms`
  - lower `avg_latency_ms`
  - lower `max_latency_ms`
  - higher `update_frequency_hz`
  - lower `disconnect_count`
  - higher `symbols_covered`
- Score strategy:
  - weighted model with higher latency weight (`avg`: 35%, `max`: 30%, `min`: 15%)
  - cap-based penalties for high latency (`min`: 1000 ms, `avg`: 10000 ms, `max`: 600000 ms)
  - disconnect cap at `10` for linear penalty to zero

## Out of Scope (MVP)
- forecasting model outputs
- user authentication/authorization
- write-back workflows
- advanced alerting

## Technical Contract
- UI framework: `streamlit`
- data access:
  - dynamic window panels read directly from `cleansed_market`
  - symbol deviation start page uses mart/cache spread fact dataset
  - platform quality reads cache table when available, else mart view fallback
- required DB objects:
  - required:
    - `cleansed_market`
  - optional but recommended for quality panel performance:
    - `dash_cache_platform_quality_daily_latest`
    - `dash_cache_symbol_deviation_bucket`
    - `dash_cache_refresh_metadata`
  - fallback:
    - `vw_mart_dashboard_platform_quality_daily`
    - `vw_mart_dashboard_symbol_deviation_bucket`

## Done Criteria
1. One command starts dashboard locally.
2. Run selector is populated from `cleansed_market`.
3. Symbol selector is populated for selected run/window.
4. Price curve and price deviation render for selected window without fixed 24h/day assumptions.
5. Platform quality panel renders from cache (preferred) or view fallback.
6. Symbol start page renders violin distributions and supports symbol jump action.

## Immediate Next Steps After MVP
1. Add SQL access smoke/integration tests for dynamic run/window queries.
2. Add optional fast-path cache for selected rolling windows if runtime grows.
