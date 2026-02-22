# Staging Run Contract

## Purpose
This document defines the metadata contract written by:
- `scripts/2_staging/staging_exporter.py`

The metadata file is used for auditability, reproducibility, and downstream validation.

## File
For each export run, the exporter writes:
- `<export_base>_metadata.json`

Example:
- `data/staging/staging_export_20260221_103000_last_24h_metadata.json`

## Contract Identity
- `contract_name`: fixed value `staging_export_run_metadata`
- `contract_version`: current value `1.0.0`

## Required Fields
- `run_id`:
  - Unique run identifier derived from export base name.
- `run_started_utc`:
  - UTC timestamp when the run started.
- `run_finished_utc`:
  - UTC timestamp when the run finished.
- `mode`:
  - `incremental`: boolean flag whether incremental mode was enabled.
- `window`:
  - `hours`: configured export window in hours.
  - `default_start_utc`: default lower bound from the configured hour window.
  - `end_utc`: upper bound of extracted data window.
- `query_bounds`:
  - `market_ticks.lower_utc`: effective lower bound for market ticks query.
  - `market_ticks.lower_inclusive`: lower bound inclusiveness for market ticks.
  - `market_ticks.upper_utc`: effective upper bound for market ticks query.
  - `connection_events.lower_utc`: effective lower bound for event query.
  - `connection_events.lower_inclusive`: lower bound inclusiveness for events.
  - `connection_events.upper_utc`: effective upper bound for event query.
  - `connection_events.included`: whether events were included in export.
- `source`:
  - `input_glob`: source DB glob pattern.
  - `worker_db_count`: number of source DB files used.
  - `worker_db_files`: explicit source DB file list.
- `options`:
  - `output_format`: `sqlite`, `csv`, or `json`.
  - `include_connection_events`: boolean.
  - `chunk_size`: chunk size for streaming export formats.
- `filters`:
  - `exchanges`: normalized exchange filters (lowercase).
  - `assets`: normalized asset filters (uppercase base assets).
- `uniques`:
  - `source.exchanges`: unique exchanges found in source window.
  - `source.assets`: unique assets found in source window.
  - `exported.exchanges`: unique exchanges in exported result.
  - `exported.assets`: unique assets in exported result.
- `watermarks`:
  - `previous.market_ticks_ingestion_ts_utc`: market watermark loaded from state before run.
  - `previous.connection_events_event_ts_utc`: event watermark loaded from state before run.
  - `exported_max.market_ticks_ingestion_ts_utc`: max market timestamp in exported result.
  - `exported_max.connection_events_event_ts_utc`: max event timestamp in exported result.
  - `new.market_ticks_ingestion_ts_utc`: resulting market watermark after run.
  - `new.connection_events_event_ts_utc`: resulting event watermark after run.
- `state`:
  - `path`: state file path used by the exporter.
  - `key`: state profile key used for this run.
  - `update_requested`: whether state update was requested.
  - `updated`: whether the state file was updated.
- `row_counts`:
  - `market_ticks`: number of exported market rows.
  - `connection_events`: number of exported event rows.
- `output_files`:
  - List of created data output files for this run.

## Validation Rules
- `run_started_utc <= run_finished_utc`
- `window.default_start_utc <= window.end_utc`
- `query_bounds.market_ticks.lower_utc <= query_bounds.market_ticks.upper_utc`
- `query_bounds.connection_events.lower_utc <= query_bounds.connection_events.upper_utc`
- `source.worker_db_count == len(source.worker_db_files)`
- `row_counts.market_ticks >= 0`
- `row_counts.connection_events >= 0`

## Compatibility Policy
- Minor contract changes add fields but keep existing fields stable.
- Breaking changes must increment major version and be documented here.
