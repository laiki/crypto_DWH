# Next Session Tasks

Last updated: 2026-03-04

## Completed in This Session (2026-03-04)
1. Made staging window anchoring robust against ingestion gaps.
   - Artifacts:
     - `scripts/2_staging/staging_exporter.py`
   - Scope:
     - default export window end now anchors to latest available source timestamp
       (instead of command runtime timestamp)
     - metadata now includes anchor strategy and detected source max timestamps

2. Added faster source-uniques handling for staging exports.
   - Artifacts:
     - `scripts/2_staging/staging_exporter.py`
   - Scope:
     - added `--source-uniques-mode` (`off`, `fast`, `full`)
     - `--list-only` no longer runs expensive unique scans by default

3. Added progress + ETA logging for staging exports.
   - Artifacts:
     - `scripts/2_staging/staging_exporter.py`
   - Scope:
     - added `--progress/--no-progress`
     - added `--progress-interval-seconds`
     - periodic heartbeats during long-running export operations

4. Added progress + ETA logging for cleansing resampling.
   - Artifacts:
     - `scripts/3_cleansing/cleansing_resample.py`
   - Scope:
     - added `--progress/--no-progress`
     - added `--progress-interval-seconds`
     - progress reporting for single-worker and multi-worker processing
     - normalized elapsed/ETA display to `HH:MM:SS` and added processing rate output

5. Removed Legacy single-DB Core compatibility path.
   - Artifacts:
     - `scripts/4_core/build_core_db.py`
     - `scripts/4_core/core_validation_runner.py`
     - `scripts/4_core/README.md`
   - Scope:
     - removed `--db-path` legacy mode from both scripts
     - input resolution now requires `--staging-db` and `--cleansing-db`
     - removed legacy mention from Core README

6. Fixed accidental dependency folder tracking in Git.
   - Artifacts:
     - `.gitignore`
   - Scope:
     - added `node_modules/` to ignore list
     - removed accidentally staged `node_modules` entries from index

7. Added progress + ETA logging for Core build execution.
   - Artifacts:
     - `scripts/4_core/build_core_db.py`
   - Scope:
     - added `--progress/--no-progress`
     - added `--progress-interval-seconds`
     - step-based progress output with elapsed time and ETA
     - heartbeat logging for long-running SQL phases (`COUNT`, `INSERT ... SELECT`, optional `VACUUM`)

8. Evaluated parallel queue-worker copy pattern for Core build and deferred implementation.
   - Reason:
     - SQLite output target is single-writer constrained
     - existing table copy path uses efficient in-engine `INSERT ... SELECT`
   - Decision:
     - keep current single-writer Core build path
     - prioritize observability and targeted SQL/runtime optimizations over parallel worker complexity

9. Added progress + ETA logging for dashboard cache materialization.
   - Artifacts:
     - `scripts/5_marts/build_dashboard_cache.py`
   - Scope:
     - added `--progress/--no-progress`
     - added `--progress-interval-seconds`
     - step-based progress output with elapsed time and ETA
     - heartbeat logging for long-running phases (mart view apply, cache table materialization, index creation, metadata counts, optional `VACUUM`)

10. Fixed dashboard symbol-ranking inconsistency between start page and detail chart.
   - Artifacts:
     - `scripts/5_marts/dashboard_mvp_app.py`
   - Scope:
     - root cause: start-page max deviation ranking used a different data/filter path than the selected-symbol detail chart
     - start-page violin/ranking now derives from the same `cleansed_market` price-curve base and applies the same observed-quality band exchange filter logic
     - added explicit UI hint that observed-quality bands are applied consistently across ranking and detail views

11. Added first forecasting baseline with two scikit-learn regressors.
   - Artifacts:
     - `scripts/6_forecasting/train_regression_forecast.py`
   - Scope:
     - implemented first forecasting CLI for `Ridge` and `HistGradientBoostingRegressor`
     - added lag and rolling feature engineering with leakage-safe target shift (`horizon_steps`)
     - added time-aware evaluation (`TimeSeriesSplit`) and holdout metrics (`MAE`, `RMSE`, `MAPE`)
     - writes reproducible artifacts (JSON summary + predictions CSV)

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

## Completed in This Session (2026-02-27)
1. Extended dashboard filtering across all major tabs.
   - Artifacts:
     - `scripts/5_marts/dashboard_mvp_app.py`
   - Scope:
     - column-based filters for Price Curve, Price Deviation, and Platform Quality
     - dual-axis Price Deviation chart (absolute + percentage in one chart)
     - dark-mode refinements and pagination behavior fixes

2. Added observed coverage quality KPI for symbol comparison robustness.
   - Artifacts:
     - `scripts/5_marts/dashboard_mvp_app.py`
   - Scope:
     - observed-vs-max ratio per symbol/exchange grouped into bands
       (`0-50%`, `50-75%`, `75-90%`, `90-100%`)
     - sidebar band filter (default now `90-100%`)
     - symbols with `<= 1` remaining exchange after band filter are excluded
     - sidebar shows filtered symbol count (`symbols=<n>`)

3. Added dedicated mart base view for observed quality inputs.
   - Artifacts:
     - `scripts/5_marts/mart_dashboard_views.sql`
     - `scripts/5_marts/dashboard_query_templates.sql`
     - `scripts/5_marts/build_dashboard_cache.py`
     - `scripts/5_marts/README.md`
   - View:
     - `vw_mart_dashboard_symbol_observed_quality_base`
   - Scope:
     - stable mart-level base for observed flag extraction
     - dashboard uses view with fallback to raw query for backward compatibility

4. Added dedicated documentation for the new mart use case.
   - Artifacts:
     - `docs/5_marts/symbol_observed_quality_mart_use_case.md`
     - `docs/5_marts/mart_kpi_extracts.md`
     - `docs/5_marts/dashboard_mvp_scope.md`

5. Added DEBUG tick receive logging in ingestion runtime.
   - Artifact:
     - `scripts/1_ingestion/ingest_all_exchanges_ws.py`
   - Scope:
     - for every received tick (when log level is DEBUG), log:
       - `ingestion_ts_utc`
       - `exchange_id`
       - `symbol`
       - selected `price`
   - Goal:
     - support trace-level investigation of source-vs-manual price discrepancies.

## Open Operational Topic
1. Cache rebuild can fail on dropping `dash_cache_price_deviation_daily_latest`
   with `sqlite3.OperationalError: unable to open database file`.
   - Scope: isolate SQLite/environment cause and make cache refresh robust.
   - Done when: cache build runs reproducibly without manual intervention.

## Next Priority 1 (Dashboard Data Drill-Down)
1. Add raw ingestion data drill-down in Price Curve tab (on demand).
   - Scope:
     - configurable ingestion DB path in dashboard config
     - load only when user explicitly requests it (button)
     - plot raw series for selected symbol/exchanges/time range as `exchange_raw`
     - overlay in existing chart when possible, fallback chart below if required
   - Done when: user can inspect cleaned vs raw series directly per symbol/exchange.

2. Investigate ingestion/source discrepancy for extreme spreads via debug traces.
   - Scope:
     - collect DEBUG logs for affected windows across exchanges
     - correlate logged tick timestamps/prices with stored raw rows and dashboard views
     - verify symbol mapping and selected source price field semantics (`last` vs `close`)
   - Done when:
     - root cause location is identified (source payload, mapping, runtime, or storage).

3. Add fast sanity checks for observed-quality mart contract.
   - Scope:
     - verify required columns and non-null assumptions
     - verify band allocation rules for boundary values
   - Done when: mart contract breaks are caught by deterministic tests.

4. Define dashboard refresh and release workflow.
   - Scope: sequence `core_pipeline` + mart SQL apply + dashboard cache refresh for daily operation.
   - Output: short runbook with command sequence and failure handling.
   - Done when: dashboard refresh is reproducible and operationally documented.

## Next Priority 2
1. Align forecasting feature contract with Core/Cleansing outputs.
   - Scope: define feature tables, label windows, and train/validation split timestamps per exchange/symbol.
   - Output: short feature contract document for model training.
   - Done when: forecasting scripts can consume stable feature inputs.
