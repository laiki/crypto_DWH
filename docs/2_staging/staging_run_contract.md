# Staging Run Contract (VAULT 2.0)

## Purpose
This contract defines the audit metadata persisted by:
- `scripts/2_staging/staging_exporter.py`

The contract is stored inside the staging SQLite output itself.

## Storage Location
Per export DB:
- table: `staging_export_run_metadata`

Example output DB name:
- `data/staging/staging_export_20260306_120000_last_1h.db`
- with relative offset:
  - `data/staging/staging_export_20260306_120000_last_1h_from_2h.db`

## Required Columns (`staging_export_run_metadata`)
- `run_id`: unique run identifier.
- `created_utc`: UTC timestamp when metadata row was written.
- `vault_root`: resolved VAULT root path.
- `manifest_db`: resolved manifest DB path.
- `layer`: exported VAULT layer (for example `ingestion`).
- `hours`: requested export window in hours.
- `start_relative_from_hour`: relative offset from source max timestamp.
- `window_start_epoch_s`: effective lower bound (inclusive).
- `window_end_epoch_s`: effective upper bound (inclusive).
- `window_start_utc`: ISO-8601 UTC string for lower bound.
- `window_end_utc`: ISO-8601 UTC string for upper bound.
- `exchange_filter_csv`: normalized exchange filter CSV (lowercase, possibly empty).
- `asset_filter_csv`: normalized asset filter CSV (lowercase, possibly empty).
- `symbol_filter_csv`: normalized symbol filter CSV (lowercase, possibly empty).
- `partitions_selected`: number of selected source partitions.
- `market_rows`: copied row count for `market_ticks`.
- `connection_event_rows`: copied row count for `connection_events`.
- `runtime_seconds`: measured export runtime.

## Validation Rules
- `hours > 0`
- `start_relative_from_hour >= 0`
- `window_start_epoch_s <= window_end_epoch_s`
- `partitions_selected >= 0`
- `market_rows >= 0`
- `connection_event_rows >= 0`
- `runtime_seconds >= 0`

## Behavioral Contract
1. Source max timestamp is resolved from manifest scope after applying exchange/asset/symbol filters.
2. Export window is derived from source max timestamp, `hours`, and `start_relative_from_hour`.
3. Only manifest-pruned partitions that overlap the window are read.
4. Output includes source provenance (`source_partition_id`, `source_db_path`, `source_row_id`) per exported row.
5. No state file and no incremental watermark profile is used in VAULT 2.0 staging exporter.

## Versioning Policy
- Additive metadata fields are allowed without breaking downstream consumers.
- Breaking semantic changes require contract document update and downstream review.
