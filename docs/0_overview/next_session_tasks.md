# Next Session Tasks

Last updated: 2026-03-09

## Completed in This Session (2026-03-09)
1. Productized the Redis-decoupled ingestion path.
   - Artifacts:
     - `scripts/1_ingestion/ccxt_to_redis_stream.py`
     - `scripts/1_ingestion/orchestrator_redis_auto_shard.py`
     - `scripts/1_ingestion/redis_stream_to_vault_writer.py`
     - `scripts/1_ingestion/poc_ccxt_to_redis_stream.py`
     - `scripts/1_ingestion/poc_redis_stream_to_vault_writer.py`
   - Scope:
     - introduced product script names for Redis publisher, auto-sharded orchestrator, and VAULT writer
     - kept old `poc_*` entrypoints as backward-compatible wrappers
     - switched Redis writer default target to `data/vault2_redis`

2. Implemented Redis publisher auto-sharding for multi-worker collection.
   - Artifacts:
     - `scripts/1_ingestion/orchestrator_redis_auto_shard.py`
     - `scripts/1_ingestion/ccxt_to_redis_stream.py`
   - Scope:
     - profiles exchange traffic before runtime
     - computes shard plans and distributes exchanges across worker processes
     - supervises worker processes with restart behavior analogous to the direct VAULT orchestrator

3. Aligned Redis product defaults with the original direct-ingestion exchange policy.
   - Artifacts:
     - `scripts/1_ingestion/ccxt_to_redis_stream.py`
     - `scripts/1_ingestion/orchestrator_redis_auto_shard.py`
     - `scripts/1_ingestion/ingestion_common.py`
   - Scope:
     - default selection starts from all supported `ccxt.pro` exchanges
     - the shared default exclusion list remains active unless explicitly narrowed:
       `alpaca`, `arkham`, `bequant`, `bitfinex`, `bitmex`, `bitopro`, `blockchaincom`, `oxfun`, `probit`
     - CLI help text now documents these defaults explicitly

4. Finalized Redis retention stance for operations.
   - Artifacts:
     - `scripts/1_ingestion/README.md`
     - `docs/0_overview/redis_event_contract.md`
     - `README.md`
   - Scope:
     - Redis is documented as a short-lived operational buffer only
     - VAULT remains the only system of record
     - sizing rule documented as
       `stream_maxlen = peak_events_per_second * desired_buffer_seconds`
     - current product baseline keeps about one minute of backlog as a starting target

5. Documented Podman-first startup and Redis runtime order.
   - Artifacts:
     - `scripts/1_ingestion/start_redis_podman.sh`
     - `scripts/1_ingestion/README.md`
     - `docs/0_overview/redis_event_contract.md`
     - `README.md`
   - Scope:
     - documents `scripts/1_ingestion/start_redis_podman.sh` as recommended local Redis start path
     - clarifies that `podman compose` requires an installed compose provider
     - fixes required runtime order to:
       1. start Redis
       2. start `redis_stream_to_vault_writer.py`
       3. start `orchestrator_redis_auto_shard.py`

## Immediate Operator Checklist
1. Start Redis:
   - `scripts/1_ingestion/start_redis_podman.sh`

2. Start the VAULT writer:
   - `python scripts/1_ingestion/redis_stream_to_vault_writer.py --redis-url redis://localhost:6379/0 --stream ingest:events:v1 --group cg.vault_writer --consumer writer-1 --dlq-stream ingest:events:dlq:v1 --stream-maxlen 50000 --dlq-maxlen 10000 --vault-root data/vault2_redis --vault-layer ingestion --log-level INFO`

3. Start the publisher orchestrator:
   - `python scripts/1_ingestion/orchestrator_redis_auto_shard.py --redis-url redis://localhost:6379/0 --stream ingest:events:v1 --stream-maxlen 50000 --workers 4 --only-spot --max-symbols-per-exchange 100 --log-level INFO --worker-log-level INFO`

4. Runtime expectation:
   - if `--exchanges` is omitted, runtime starts from all supported `ccxt.pro` exchanges and then applies the shared default exclusion list
   - use `--symbols` when startup scope should be bounded for the first live run
   - start the writer before publishers so Redis stays only a short operational buffer and not an accumulating backlog store

## Current Forecasting Workflow
1. Train models from staging history:
   - `python scripts/6_forecasting/train_staging_models.py --staging-db data/staging --forecast-db data/core/core_kpi.db --model-dir data/forecasting/models --workers 4 --progress --progress-interval-seconds 30`

2. Optionally exclude selected staging exports from the training history:
   - `python scripts/6_forecasting/train_staging_models.py --staging-db data/staging --staging-db-exclude "data/staging/*_last_1h.db" --forecast-db data/core/core_kpi.db --model-dir data/forecasting/models --workers 4`

3. Create forecasts for one cleansing run:
   - `python scripts/6_forecasting/forecast_with_trained_models.py --cleansing-db data/cleansing/latest_cleansing.db --forecast-db data/core/core_kpi.db --workers 4 --replace-existing --progress --progress-interval-seconds 30`

4. Runtime note:
   - `train_staging_models_and_forecasts.py` remains in the repository as a legacy compatibility entrypoint
   - the current recommended production path is the split workflow above

## Completed in This Session (2026-03-05)
1. Extended forecasting model registry with explicit test-score persistence for model artifacts.
   - Artifacts:
     - `scripts/6_forecasting/train_staging_models_and_forecasts.py`
     - `docs/6_forecasting/forecasting_training_architecture.md`
     - `diagrams/6_forecasting/uml_er_forecasting_model_registry.mmd`
   - Scope:
     - added `holdout_r2` and `cv_mean_r2` columns in `forecast_model_registry` with migration for existing DBs
     - `model_artifacts` view now exposes `test_score_r2` (mapped from holdout `R²`) for dashboard/model QA
     - metric computation and fallback structures now include `r2`

2. Added `pandas-ta` based feature engineering on top of price-only time series.
   - Artifacts:
     - `scripts/6_forecasting/train_staging_models_and_forecasts.py`
     - `requirements.txt`
   - Scope:
     - added dependency import `pandas_ta as pta`
     - extended `build_feature_table(...)` with leakage-safe TA features derived from `price.shift(1)`:
       - SMA / EMA per configured rolling windows
       - RSI(14), ROC(10), Bollinger Bands (lower/middle/upper/bandwidth)
     - recorded package dependency centrally via `requirements.txt`

3. Added central end-to-end operations runbook for full pipeline execution.
   - Artifacts:
     - `docs/0_overview/README.md`
     - `README.md`
     - `diagrams/0_overview/uml_flow_end_to_end_pipeline_subgraphs.mmd`
   - Scope:
     - documents complete execution chain from ingestion to dashboard/presentation
     - includes staged hourly-slice workflow (`--hours 1` with `--start-relative-from-hour`)
     - documents forecasting cutoff behavior and training-source implications (staging slices vs worker raw DBs)
     - includes full command variant for execution from `scripts/` directory
     - adds LR directed end-to-end Mermaid flow with per-phase subgraphs and script-level nodes

4. Parallelized forecasting training/inference execution with configurable worker count.
   - Artifacts:
     - `scripts/6_forecasting/train_staging_models_and_forecasts.py`
     - `scripts/6_forecasting/README.md`
     - `docs/0_overview/README.md`
   - Scope:
     - added `--workers` argument for configurable process parallelism
     - implemented process-pool fanout per `(exchange_id, symbol)` with robust `spawn` context
     - kept SQLite writes in single-writer main process to avoid write contention
     - updated runbook and forecasting examples to include parallel execution parameters

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
     - `scripts/6_forecasting/train_staging_models_and_forecasts.py`
   - Scope:
     - implemented first forecasting CLI for `Ridge` and `HistGradientBoostingRegressor`
     - added lag and rolling feature engineering with leakage-safe target shift (`horizon_steps`)
     - added time-aware evaluation (`TimeSeriesSplit`) and holdout metrics (`MAE`, `RMSE`, `MAPE`)
     - writes reproducible artifacts (JSON summary + predictions CSV)

12. Documented staging-based forecasting architecture with strict cleansing cutoff.
   - Artifacts:
     - `docs/6_forecasting/forecasting_training_architecture.md`
     - `diagrams/6_forecasting/uml_architecture_forecasting_training_staging_cutoff.mmd`
     - `diagrams/6_forecasting/uml_deployment_forecasting_training_runtime.mmd`
     - `diagrams/6_forecasting/uml_er_forecasting_model_registry.mmd`
     - `diagrams/6_forecasting/uml_sequence_forecasting_staging_cutoff_training.mmd`
   - Scope:
     - defined model-training separation from staging export operations
     - defined in-memory per-series resampling without persistent training-resample DB
     - defined leakage guardrail: training data strictly before first timestamp of selected cleansing run
     - defined artifact strategy: model files on filesystem + metadata/metrics in dedicated registry DB

13. Extended staging exporter with relative window offset for deterministic backfill slices.
   - Artifacts:
     - `scripts/2_staging/staging_exporter.py`
     - `docs/2_staging/staging_run_contract.md`
   - Scope:
     - added `--start-relative-from-hour` to shift window end from source max timestamp
     - supports patterns like `--hours 1 --start-relative-from-hour 2` => window `[t-3h, t-2h]`
     - output base name now includes offset marker (`..._last_<hours>h_from_<offset>h`) when offset is used
     - documented new metadata fields (`window.start_relative_from_hour`, `window.anchor.source_max_utc`)
     - disallowed combination of relative offset with incremental mode to avoid ambiguous watermark semantics

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

## Update 2026-03-06 (VAULT 2.0 Hard Cut)
1. Implemented VAULT 2.0 ingestion storage and removed monolithic worker DB write path.
   - Artifacts:
     - `scripts/1_ingestion/ingest_all_exchanges_ws.py`
     - `scripts/1_ingestion/orchestrator_auto_shard.py`
   - Scope:
     - partitioned SQLite writes by `exchange/date/hour`
     - manifest maintenance in `<vault-root>/meta/vault_manifest.db`
     - worker command forwarding switched to `--vault-root` and `--vault-layer`

2. Replaced staging exporter with manifest-pruned VAULT 2.0 exporter.
   - Artifact:
     - `scripts/2_staging/staging_exporter.py`
   - Scope:
     - no legacy worker DB input mode
     - source window resolved from manifest and filter scope
     - output remains staging SQLite DB for downstream cleansing/core

3. Switched forecasting training history source to VAULT 2.0 manifest and partitions.
   - Artifact:
     - `scripts/6_forecasting/train_staging_models_and_forecasts.py`
   - Scope:
     - removed `--staging-glob`
     - added `--vault-root`, `--manifest-db`, `--vault-layer`
     - per-pair training data loading now prunes partitions via manifest before cutoff

4. Updated runbook and script documentation to VAULT 2.0 commands and paths.
   - Artifacts:
     - `docs/0_overview/README.md`
     - `scripts/1_ingestion/README.md`
     - `scripts/6_forecasting/README.md`

## Update 2026-03-06 (VAULT 2.0 Docs and Diagram Alignment)
1. Removed remaining legacy architecture guidance from operational docs.
   - Artifacts:
     - `docs/1_ingestion/multiprocess_ingestion_layout.md`
     - `docs/2_staging/staging_run_contract.md`
     - `docs/4_core/core_validation_runbook.md`
   - Scope:
     - ingestion layout now documents direct VAULT partition and manifest writes
     - staging contract now documents `staging_export_run_metadata` table in output DB
     - removed obsolete Core validation legacy invocation snippet

2. Added explicit deprecation marker for staging state-file contract.
   - Artifact:
     - `docs/2_staging/staging_state_contract.md`
   - Scope:
     - marks `staging_export_state.json` as removed in VAULT 2.0
     - redirects to active staging run contract

3. Refreshed end-to-end and phase diagrams to VAULT 2.0 data flow.
   - Artifacts:
     - `diagrams/0_overview/uml_flow_end_to_end_pipeline_subgraphs.mmd`
     - `diagrams/0_overview/uml_flow_end_to_end_pipeline_simple_subgraphs.mmd`
     - `diagrams/1_ingestion/uml_architecture_ingestion_multiprocess.mmd`
     - `diagrams/1_ingestion/uml_deployment_ingestion_multiprocess.mmd`
     - `diagrams/2_staging/uml_sequence_staging_incremental_export.mmd`
     - `diagrams/2_staging/uml_deployment_staging_export.mmd`
     - `diagrams/2_staging/uml_er_staging_export.mmd`
     - `diagrams/2_staging/uml_usecase_ad_hoc_analysis.mmd`
     - `diagrams/6_forecasting/uml_architecture_forecasting_training_staging_cutoff.mmd`
     - `diagrams/6_forecasting/uml_sequence_forecasting_staging_cutoff_training.mmd`
     - `diagrams/6_forecasting/uml_er_forecasting_model_registry.mmd`
     - `diagrams/6_forecasting/uml_deployment_forecasting_training_runtime.mmd`

4. Removed remaining compatibility wording in mart docs where no longer required.
   - Artifacts:
     - `docs/5_marts/mart_kpi_extracts.md`
     - `docs/5_marts/symbol_deviation_mart_use_case.md`
     - `docs/7_presentation/crypto_dwh_data_engineering_leitfaden_slides.md`

## Update 2026-03-09 (Redis PoC Ingestion Decoupling)
1. Implemented minimal ccxt-to-Redis publisher PoC.
   - Artifact:
     - `scripts/1_ingestion/poc_ccxt_to_redis_stream.py`
   - Scope:
     - watches one exchange with symbol selection via SQL-like patterns
       (`--symbols` with `%` and `_`)
     - publishes `tick` and `connection_event` envelopes to `ingest:events:v1`
     - includes schema fields and deterministic `dedup_key`

2. Implemented Redis consumer group writer PoC for VAULT.
   - Artifact:
     - `scripts/1_ingestion/poc_redis_stream_to_vault_writer.py`
   - Scope:
     - reads via `XREADGROUP`
     - writes into VAULT-style partition DBs and manifest
     - idempotency via `processed_event_keys`
     - DLQ handoff to `ingest:events:dlq:v1` after retry exhaustion

3. Added Redis local runtime scaffold and docs.
   - Artifacts:
     - `scripts/1_ingestion/docker-compose.redis.yml`
     - `scripts/1_ingestion/README.md`
     - `docs/0_overview/redis_event_contract.md`
     - `diagrams/0_overview/uml_sequence_redis_stream_ingestion_contract.mmd`
   - Scope:
     - minimal operational commands for Redis, publisher, and writer
     - explicit startup order: Redis -> writer consumer -> publisher
     - contract and sequence sketch for rollout alignment

4. Extended dependencies for PoC runtime.
   - Artifact:
     - `requirements.txt`
   - Scope:
     - added `redis`
