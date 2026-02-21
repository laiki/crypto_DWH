# Staging State Contract

## Purpose
This document defines the persistent watermark state used by:
- `scripts/2_staging/staging_exporter.py`

The state enables incremental extraction across runs.

## File
Default path:
- `data/staging/staging_export_state.json`

## Contract Identity
- `contract_name`: fixed value `staging_export_state`
- `contract_version`: current value `1.0.0`

## Top-Level Structure
- `profiles`: object keyed by `state_key`

Each profile contains:
- `updated_utc`: last update timestamp
- `source.input_glob`: input source glob used for this profile
- `filters.exchanges`: normalized exchange filter list
- `filters.assets`: normalized asset filter list
- `watermark.market_ticks_ingestion_ts_utc`: last processed market timestamp
- `watermark.connection_events_event_ts_utc`: last processed event timestamp

## State Key
- If `--state-key` is set, that value is used directly.
- Otherwise, a deterministic default key is derived from:
  - `input_glob`
  - exchange filters
  - asset filters

## Update Behavior
- In incremental mode, the exporter calculates new watermarks per table.
- If `--update-state` is enabled, the corresponding profile is written back.
- If `--no-update-state` is set, state is read but not modified.
