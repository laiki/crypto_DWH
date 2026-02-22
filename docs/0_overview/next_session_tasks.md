# Next Session Tasks

Last updated: 2026-02-22

## Completed in This Session (2026-02-22)
1. Defined Core Layer KPI catalog.
   - Artifact: `docs/4_core/core_kpi_catalog.md`

2. Implemented SQL views for Core KPIs with stable names and dependencies.
   - Artifacts:
     - `scripts/4_core/core_kpi_views.sql`
     - `scripts/4_core/README.md`

3. Defined validation checks and provided executable SQL assertions.
   - Artifacts:
     - `docs/4_core/core_kpi_validation_checks.md`
     - `scripts/4_core/core_kpi_assertions.sql`

4. Prepared Cleansing to Core handoff contract.
   - Artifact: `docs/4_core/cleansing_core_handoff_contract.md`

5. Added remote validation runbook and acceptance checklist.
   - Artifacts:
     - `docs/4_core/core_remote_validation_runbook.md`
     - `docs/4_core/core_remote_acceptance_checklist.md`

6. Added executable Core validation runner with report output.
   - Artifact: `scripts/4_core/core_validation_runner.py`
   - Output format: JSON and Markdown reports under `logs/core_validation/`

## Next Priority 1
1. Implement a small Core build runner.
   - Scope: merge `market_ticks`, `connection_events`, and `cleansed_market` into one Core DB and apply view script automatically.
   - Output: script (for example `scripts/4_core/build_core_db.py`) with reproducible CLI arguments.
   - Done when: one command produces a query-ready Core KPI database.

2. Add automated smoke test for Core SQL views.
   - Scope: create minimal fixture data and assert expected view columns/row semantics.
   - Output: repeatable test command/script.
   - Done when: CI/local execution detects breaking changes in KPI SQL.

## Next Priority 2
1. Define mart-ready KPI extracts for dashboard.
   - Scope: select exact columns and sort/filter defaults for platform quality and price deviation panels.
   - Output: SQL select templates or views for dashboard consumption.
   - Done when: dashboard can query Core KPIs without additional transformations.

2. Align forecasting feature contract with Core/Cleansing outputs.
   - Scope: define feature tables, label windows, and train/validation split timestamps per exchange/symbol.
   - Output: short feature contract document for model training.
   - Done when: forecasting scripts can consume stable feature inputs.
