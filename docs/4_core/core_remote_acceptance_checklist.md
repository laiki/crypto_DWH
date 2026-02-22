# Core Remote Acceptance Checklist

Last updated: 2026-02-22

Use this checklist after running:
- `scripts/4_core/core_validation_runner.py`

## A) Input Availability
- [ ] Staging DB file exists and is readable.
- [ ] Cleansing DB file exists and is readable.
- [ ] Required source table `market_ticks` exists in staging DB.
- [ ] Required source table `connection_events` exists in staging DB.
- [ ] Required source table `cleansed_market` exists in cleansing DB.

## B) Core View Build
- [ ] `core_kpi_views.sql` applied successfully.
- [ ] Expected Core view count is 9.
- [ ] No SQL runtime error in runner output.

## C) Assertion Outcome
- [ ] `error_failed = 0` in report summary.
- [ ] If `warn_failed > 0`, warnings are reviewed and accepted.
- [ ] Assertion report contains all expected checks (latency, update frequency, disconnects, price deviation, coverage).

## D) KPI Coverage Sanity
- [ ] `vw_core_kpi_latency_daily` has rows for the expected date range.
- [ ] `vw_core_kpi_update_frequency_daily` has rows for the expected date range.
- [ ] `vw_core_kpi_connection_drops_daily` has rows for active exchanges.
- [ ] `vw_core_kpi_price_deviation_daily` has rows for symbols with at least two exchanges.

## E) Report and Traceability
- [ ] JSON report file is created and archived.
- [ ] Markdown report file is created and archived.
- [ ] Report timestamp is UTC and matches the refresh window.
- [ ] Refresh ticket/log references the report file names.

## F) Release Decision
- [ ] Dashboard/mart refresh approved (all error assertions passed).
- [ ] Or refresh blocked with documented reason and remediation actions.
