# Core Validation Runbook

Last updated: 2026-02-22

## Purpose
This runbook describes how to validate Core KPI SQL views and assertions on the remote server where the real SQLite data is stored.

Role separation reference:
- `docs/4_core/core_scripts_responsibilities.md`

Recommended execution order:
1. Build/update Core artifact with `scripts/4_core/build_core_db.py`.
2. Validate quality gate with `scripts/4_core/core_validation_runner.py`.

Operational shortcut:
- `scripts/4_core/core_pipeline.py` can run both steps in one command.

Fast/full phase wrapper examples:

```bash
python scripts/4_core/core_pipeline.py \
  --phase fast \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db \
  --kpi-date 2026-02-21 \
  --cleansing-run-id cleansing_20260221_164315_60s
```

```bash
python scripts/4_core/core_pipeline.py \
  --phase full \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db
```

## Inputs
- staging SQLite DB that contains:
  - `market_ticks`
  - `connection_events`
- cleansing SQLite DB that contains:
  - `cleansed_market`
- SQL artifacts:
  - `scripts/4_core/core_kpi_views.sql`
  - `scripts/4_core/core_kpi_assertions.sql`
- validation runner:
  - `scripts/4_core/core_validation_runner.py`

## One-Time Preparation
1. Ensure Python 3.10+ is available on the remote host.
2. Ensure project files are present on the remote host.
3. Ensure write permissions for report directory (default: `logs/core_validation`).

## Execution (Recommended)
Run from project root on remote host:

```bash
python scripts/4_core/core_validation_runner.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db
```

Optional custom paths:

```bash
python scripts/4_core/core_validation_runner.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db \
  --views-sql scripts/4_core/core_kpi_views.sql \
  --assertions-sql scripts/4_core/core_kpi_assertions.sql \
  --output-dir logs/core_validation
```

Performance-oriented scoped execution:

```bash
python scripts/4_core/core_validation_runner.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db \
  --kpi-date 2026-02-21 \
  --cleansing-run-id cleansing_20260221_164315_60s \
  --skip-view-row-counts
```

## Outputs
For each run, two files are written:
- JSON report:
  - `logs/core_validation/core_validation_<UTC_TIMESTAMP>.json`
- Markdown report:
  - `logs/core_validation/core_validation_<UTC_TIMESTAMP>.md`

Report content includes:
- run metadata (UTC timestamps, host, python version, input paths, scope options)
- table presence check
- assertion results (`error`/`warn`, failed rows)
- KPI view row counts
- summary (`error_failed`, `warn_failed`)

## Assertion Catalog (Current)
Source of truth:
- `scripts/4_core/core_kpi_assertions.sql`

Current assertion count:
- total: `20`
- `error`: `14`
- `warn`: `6`

`error` assertions:
- `latency_non_negative`
- `latency_upper_bound`
- `latency_non_negative_hourly`
- `latency_upper_bound_hourly`
- `update_interval_positive`
- `update_frequency_bounds`
- `update_interval_positive_hourly`
- `update_frequency_bounds_hourly`
- `disconnect_count_non_negative`
- `disconnect_count_non_negative_hourly`
- `price_diff_abs_non_negative`
- `price_diff_pct_bounds`
- `price_diff_abs_non_negative_hourly`
- `price_diff_pct_bounds_hourly`

`warn` assertions:
- `coverage_latency_daily`
- `coverage_latency_hourly`
- `coverage_update_frequency_daily`
- `coverage_update_frequency_hourly`
- `coverage_price_deviation_daily`
- `coverage_price_deviation_hourly`

Scope options:
- `--kpi-date YYYY-MM-DD`:
  - filters `market_ticks` by `date(ingestion_ts_utc)`
  - filters `connection_events` by `date(event_ts_utc)`
  - filters `cleansed_market` by `date(bucket_start_utc)`
- `--cleansing-run-id`:
  - filters `cleansed_market` by exact `run_id`
- `--skip-view-row-counts`:
  - skips expensive `COUNT(*)` queries over all KPI views

## Exit Codes
- `0`: validation completed and no failed `error` assertions
- `2`: validation completed but at least one `error` assertion failed (default blocking behavior)
- `1`: execution/setup error (missing files, missing tables, SQL runtime error)

If you want non-blocking behavior for failed `error` assertions:

```bash
python scripts/4_core/core_validation_runner.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db \
  --no-fail-on-error
```

Legacy mode (single combined DB) is still supported:

```bash
python scripts/4_core/core_validation_runner.py \
  --db-path /path/to/combined_core.db
```

## Manual SQL Fallback
If Python execution is not available:

```bash
sqlite3 :memory: <<'SQL'
ATTACH DATABASE '/path/to/staging_export.db' AS staging_src;
ATTACH DATABASE '/path/to/cleaned_staging_export_60s.db' AS cleansing_src;
CREATE TEMP VIEW market_ticks AS SELECT * FROM staging_src.market_ticks;
CREATE TEMP VIEW connection_events AS SELECT * FROM staging_src.connection_events;
CREATE TEMP VIEW cleansed_market AS SELECT * FROM cleansing_src.cleansed_market;
.read scripts/4_core/core_kpi_views.sql
.read scripts/4_core/core_kpi_assertions.sql
SQL
```

## Operational Recommendation
- Execute after every Core refresh.
- Archive generated validation reports together with refresh metadata.
- Treat any failed `error` assertion as release blocker for dashboard/mart refresh.

## Runtime Benchmark (Observed)
Observed on 2026-02-22 for a source scope of approximately 1 hour:
- `core_pipeline.py --phase fast`: about `3 minutes`
- `core_pipeline.py --phase full`: about `6 minutes`

Interpretation:
- `fast` is about `50%` of `full` runtime in the observed setup.
- This aligns with fast-mode optimizations (scoping and skipped view row counts).

Recommended operating mode:
1. Use `--phase fast` for iterative monitoring and quick feedback loops.
2. Use `--phase full` before release/publication decisions.
3. Use `--phase both` when one command should perform fast check plus full gate sequentially.
