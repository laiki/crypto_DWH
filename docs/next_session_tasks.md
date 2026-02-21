# Next Session Tasks

Last updated: 2026-02-21

## Priority 1
1. Define Core Layer KPI catalog.
   - Scope: latency, update frequency, connection drops, cross-exchange price deviation.
   - Output: clear KPI definitions (formula, unit, aggregation window, required source fields).
   - Done when: KPI table is complete and unambiguous for implementation.

2. Implement SQL views for Core KPIs.
   - Scope: create reproducible views on top of staging/cleansed outputs.
   - Output: SQL view scripts with stable names and documented dependencies.
   - Done when: views run without manual edits and return expected columns.

## Priority 2
1. Define validation checks for Core KPI results.
   - Scope: sanity checks for impossible values and missing source coverage.
   - Output: checklist + optional SQL assertions.
   - Done when: validation can be executed after each Core refresh.

2. Prepare handoff contract from Cleansing to Core.
   - Scope: required columns, timestamp conventions (UTC), symbol/exchange keys.
   - Output: short interface contract document.
   - Done when: Core can consume Cleansing outputs without ad hoc mapping.
