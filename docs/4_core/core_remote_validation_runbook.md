# Core Remote Validation Runbook

Last updated: 2026-02-22

## Purpose
This runbook describes how to validate Core KPI SQL views and assertions on the remote server where the real SQLite data is stored.

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
  - `scripts/4_core/core_remote_validation.py`

## One-Time Preparation
1. Ensure Python 3.10+ is available on the remote host.
2. Ensure project files are present on the remote host.
3. Ensure write permissions for report directory (default: `logs/core_validation`).

## Execution (Recommended)
Run from project root on remote host:

```bash
python scripts/4_core/core_remote_validation.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db
```

Optional custom paths:

```bash
python scripts/4_core/core_remote_validation.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db \
  --views-sql scripts/4_core/core_kpi_views.sql \
  --assertions-sql scripts/4_core/core_kpi_assertions.sql \
  --output-dir logs/core_validation
```

## Outputs
For each run, two files are written:
- JSON report:
  - `logs/core_validation/core_validation_<UTC_TIMESTAMP>.json`
- Markdown report:
  - `logs/core_validation/core_validation_<UTC_TIMESTAMP>.md`

Report content includes:
- run metadata (UTC timestamps, host, python version, DB path)
- table presence check
- assertion results (`error`/`warn`, failed rows)
- KPI view row counts
- summary (`error_failed`, `warn_failed`)

## Exit Codes
- `0`: validation completed and no failed `error` assertions
- `2`: validation completed but at least one `error` assertion failed (default blocking behavior)
- `1`: execution/setup error (missing files, missing tables, SQL runtime error)

If you want non-blocking behavior for failed `error` assertions:

```bash
python scripts/4_core/core_remote_validation.py \
  --staging-db /path/to/staging_export.db \
  --cleansing-db /path/to/cleaned_staging_export_60s.db \
  --no-fail-on-error
```

Legacy mode (single combined DB) is still supported:

```bash
python scripts/4_core/core_remote_validation.py \
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
