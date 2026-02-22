# Cleansing to Core Handoff Contract

Last updated: 2026-02-22

## Purpose
This contract defines the required interface from Cleansing output to Core KPI computation.

It ensures Core SQL views can run without ad hoc mapping.

## Source Object
- required table: `cleansed_market`
- producer: `scripts/3_cleansing/cleansing_resample.py`
- consumer: `scripts/4_core/core_kpi_views.sql`

## Required Columns
- `bucket_start_utc` (TEXT, not null)
- `bucket_epoch_s` (INTEGER, not null)
- `exchange_id` (TEXT, not null)
- `symbol` (TEXT, not null)
- `price` (REAL, nullable for missing bins)
- `is_missing` (INTEGER, not null; expected values `0` or `1`)
- `is_stale` (INTEGER, not null; expected values `0` or `1`)
- `fill_method` (TEXT, not null; expected values: `observed`, `forward_fill`, `interpolation`, `missing`)
- `source_ingestion_ts_utc` (TEXT, nullable)
- `bin_seconds` (INTEGER, not null)
- `max_forward_fill_s` (INTEGER, not null)
- `fill_method_mode` (TEXT, not null)

## Key and Uniqueness
- expected business key:
  - `(exchange_id, symbol, bucket_epoch_s, run_id)`
- for a single run:
  - one row per `(exchange_id, symbol, bucket_epoch_s)`

## Timestamp Conventions
- all timestamps must be UTC.
- `bucket_start_utc`:
  - ISO-8601 UTC string
  - represents start of aligned bin
- `bucket_epoch_s`:
  - Unix epoch seconds for the same bin start
- `source_ingestion_ts_utc`:
  - UTC timestamp of source tick used for the bin (nullable for missing bins)

## Exchange and Symbol Keys
- `exchange_id`:
  - CCXT exchange code in lowercase (for example `binance`, `kraken`)
- `symbol`:
  - canonical CCXT symbol format `<BASE>/<QUOTE>` (for example `BTC/USDT`)
- no downstream remapping is performed in Core views.

## Data Quality Contract for Core Consumption
Rows are considered comparable for cross-exchange KPI only if:
- `price IS NOT NULL`
- `is_missing = 0`
- `is_stale = 0`

Rows that violate these conditions remain stored but are excluded from cross-exchange price deviation KPI.

## Dependency on Staging Objects
Core KPI script also requires:
- `market_ticks` table
- `connection_events` table

These objects are expected from staging export and must be available in the same Core database context as `cleansed_market`.

## Compatibility Policy
- additive columns are allowed.
- breaking changes to required columns, value domains, or key semantics require contract version update and Core SQL review.
