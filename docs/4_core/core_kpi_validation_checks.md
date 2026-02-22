# Core KPI Validation Checks

Last updated: 2026-02-22

## Purpose
This document defines post-refresh checks for Core KPI views.

Execution target:
- after each Core refresh
- against the same DB where `scripts/4_core/core_kpi_views.sql` was applied

## Validation Checklist

1. Latency values are non-negative (daily + hourly).
- expectation: `min_latency_ms >= 0`
- failure impact: clock parsing/order issue or wrong timestamp mapping

2. Latency values are within technical bounds (daily + hourly).
- expectation: `max_latency_ms <= 600000` (10 minutes)
- failure impact: severe clock skew, stale exchange timestamps, or wrong timezone handling

3. Update intervals are strictly positive (daily + hourly).
- expectation: `min_update_interval_s > 0`
- failure impact: duplicate ordering or timestamp parse issue

4. Update frequency is in a realistic range (daily + hourly).
- expectation: `0 <= update_frequency_hz <= 100`
- failure impact: interval calculation error or duplicate timestamp concentration

5. Disconnect counts are non-negative (daily + hourly).
- expectation: `disconnect_count >= 0`
- failure impact: aggregation error

6. Price deviation absolute values are non-negative (daily + hourly).
- expectation: `max_price_diff_abs >= 0`
- failure impact: incorrect min/max calculation

7. Price deviation percentage is non-negative and bounded (daily + hourly).
- expectation: `0 <= max_price_diff_pct <= 200`
- failure impact: wrong denominator handling or bad price quality

8. Coverage exists for key KPI areas.
- expectation:
  - at least one row in latency daily KPI
  - at least one row in latency hourly KPI
  - at least one row in update frequency daily KPI
  - at least one row in update frequency hourly KPI
  - at least one row in price deviation daily KPI
  - at least one row in price deviation hourly KPI
- failure impact: upstream data missing or wrong source-to-core contract

## SQL Assertions
The SQL checks are implemented in:
- `scripts/4_core/core_kpi_assertions.sql`

Behavior:
- returns one row per assertion
- includes `severity`, `is_failed`, `failed_rows`, and short `details`
- designed for direct CLI execution with `sqlite3`

## Operational Rule
- hard fail: any `severity = 'error'` assertion with `is_failed = 1`
- warning only: `severity = 'warn'` assertions may be reviewed but do not block refresh publication
