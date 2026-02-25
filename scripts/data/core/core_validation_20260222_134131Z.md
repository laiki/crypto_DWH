# Core Validation Report

- Run started UTC: `2026-02-22T13:32:07+00:00`
- Run finished UTC: `2026-02-22T13:41:31+00:00`
- Duration seconds: `564.656`
- Host: `vmd191183`
- Python: `3.11.14`
- Input mode: `split_db`
- Staging DB path: `data/staging/staging_export_20260221_164315_last_1h.db`
- Cleansing DB path: `data/cleansing/cleaned_staging_export_20260221_164315_last_1h_60s.db`

## Table Check

All required source tables are present.

## Summary

- Assertions total: `10`
- Error failed: `2`
- Warn failed: `0`

## Assertion Results

| assertion_name | severity | is_failed | failed_rows | details |
|---|---|---:|---:|---|
| disconnect_count_non_negative | error | 0 | 0 | disconnect_count must be >= 0 |
| latency_non_negative | error | 0 | 0 | min_latency_ms must be >= 0 |
| latency_upper_bound | error | 1 | 196 | max_latency_ms exceeds configured bound (600000 ms) |
| price_diff_abs_non_negative | error | 0 | 0 | max_price_diff_abs must be >= 0 |
| price_diff_pct_bounds | error | 1 | 62 | max_price_diff_pct must be between 0 and 200 |
| update_frequency_bounds | error | 0 | 0 | update_frequency_hz must be between 0 and 100 |
| update_interval_positive | error | 0 | 0 | min_update_interval_s must be > 0 |
| coverage_latency_daily | warn | 0 | 0 | No rows found in vw_core_kpi_latency_daily |
| coverage_price_deviation_daily | warn | 0 | 0 | No rows found in vw_core_kpi_price_deviation_daily |
| coverage_update_frequency_daily | warn | 0 | 0 | No rows found in vw_core_kpi_update_frequency_daily |

## KPI View Row Counts

| view_name | row_count |
|---|---:|
| vw_core_latency_samples | 2543541 |
| vw_core_kpi_latency_daily | 3430 |
| vw_core_update_intervals | 3157721 |
| vw_core_kpi_update_frequency_daily | 6153 |
| vw_core_kpi_connection_drops_daily | 7 |
| vw_core_price_deviation_aligned | 47563 |
| vw_core_kpi_price_deviation_daily | 1456 |
| vw_core_kpi_daily_exchange_symbol | 6228 |
| vw_core_kpi_daily_exchange | 21 |

