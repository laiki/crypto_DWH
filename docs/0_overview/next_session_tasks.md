# Next Session Tasks

Last updated: 2026-02-23

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
     - `docs/4_core/core_validation_runbook.md`
     - `docs/4_core/core_remote_acceptance_checklist.md`

6. Added executable Core validation runner with report output.
   - Artifact: `scripts/4_core/core_validation_runner.py`
   - Output format: JSON and Markdown reports under `logs/core_validation/`

7. Implemented Core build runner for reproducible artifact creation.
   - Artifact: `scripts/4_core/build_core_db.py`
   - Scope: copy `market_ticks`, `connection_events`, `cleansed_market` into one Core DB and apply Core KPI views.
   - Output: query-ready Core DB + `core_build_metadata` table.

8. Implemented build+validate pipeline wrapper for operations.
   - Artifact: `scripts/4_core/core_pipeline.py`
   - Scope: run `fast`, `full`, or `both` phases with clear exit-code behavior.
   - Output: one-command orchestration including validation report links.

9. Added automated smoke test for Core KPI SQL views.
   - Artifact: `scripts/4_core/smoke_test_core_kpi_views.py`
   - Scope: in-memory fixture data, contract checks (view names/columns), and daily/hourly semantic checks.
   - Output: repeatable command with clear pass/fail exit code.

10. Added automated integration smoke test for `core_pipeline.py`.
    - Artifact: `scripts/4_core/smoke_test_core_pipeline.py`
    - Scope: file-based fixture DBs, `fast`/`full` phase runs, expected exit codes, and report/metadata checks.
    - Output: repeatable command with clear pass/fail exit code.

11. Defined mart-ready KPI extracts for dashboard.
    - Artifacts:
      - `scripts/5_marts/mart_dashboard_views.sql`
      - `scripts/5_marts/dashboard_query_templates.sql`
      - `scripts/5_marts/README.md`
      - `docs/5_marts/mart_kpi_extracts.md`
    - Scope: exact columns and default filter/sort behavior for platform quality, price deviation, and 24h price curve.
    - Output: SQL views and templates consumable directly by dashboard.

## Priority Shift (2026-02-23)
Dashboard delivery is now prioritized before forecasting so that already collected and generated KPIs are visible and reviewable end-to-end.

## Completed in This Session (2026-02-23)
1. Defined dashboard MVP scope and fixed panel-to-mart mapping.
   - Artifact:
     - `docs/5_marts/dashboard_mvp_scope.md`

2. Implemented first runnable dashboard MVP scaffold over mart extracts.
   - Artifacts:
     - `scripts/5_marts/dashboard_mvp_app.py`
     - `diagrams/5_marts/uml_sequence_dashboard_mvp_runtime.mmd`
   - Integration updates:
     - `scripts/5_marts/README.md`
     - `requirements.txt`

## Next Priority 1 (Dashboard Hardening)
1. Add dashboard data-access and query integration tests.
   - Scope: verify expected columns, null-handling, and default filter/sort behavior from mart queries.
   - Output: repeatable smoke/integration tests for dashboard SQL access layer.
   - Done when: broken mart contract causes deterministic test failure.

2. Define dashboard refresh and release workflow.
   - Scope: sequence `core_pipeline` + mart SQL apply + dashboard cache refresh for daily operation.
   - Output: short runbook with command sequence and failure handling.
   - Done when: dashboard refresh is reproducible and operationally documented.

3. Add dashboard UX/data fallback handling for empty or partially missing KPI snapshots.
   - Scope: graceful panel behavior for missing symbol data and absent latest-day KPI rows.
   - Output: explicit UI states and operator hints for recovery.
   - Done when: dashboard remains usable even with incomplete daily ingestion coverage.

## Next Priority 2
1. Align forecasting feature contract with Core/Cleansing outputs.
   - Scope: define feature tables, label windows, and train/validation split timestamps per exchange/symbol.
   - Output: short feature contract document for model training.
   - Done when: forecasting scripts can consume stable feature inputs.
