# Core KPI Catalog

Last updated: 2026-02-22

## Purpose
This document defines the Core KPI metrics used for exchange quality and cross-exchange comparison.

The KPI definitions are implementation-ready:
- fixed formulas
- fixed units
- fixed grain and aggregation window
- explicit source fields

## Data Sources
- `market_ticks` (staging export)
- `connection_events` (staging export)
- `cleansed_market` (cleansing output)

All timestamps are UTC and must be stored as ISO-8601 text.

## KPI Definitions

### KPI 1: Latency
- `kpi_name`: `latency_ms`
- `business meaning`: delay between exchange timestamp and local ingestion timestamp.
- `formula`:
  - `latency_ms = (ingestion_ts_utc - exchange_ts_utc) * 1000`
  - SQLite implementation: `(julianday(ingestion_ts_utc) - julianday(exchange_ts_utc)) * 86400000`
- `unit`: milliseconds
- `source fields`:
  - `market_ticks.ingestion_ts_utc`
  - `market_ticks.exchange_ts_utc`
  - `market_ticks.exchange_id`
  - `market_ticks.symbol`
- `validity filter`:
  - both timestamps are not null and parseable
  - latency is `>= 0`
- `grain`:
  - sample: per tick
  - aggregate: per `(kpi_date_utc, exchange_id, symbol)`
- `daily aggregates`:
  - `latency_sample_count`
  - `avg_latency_ms`
  - `min_latency_ms`
  - `max_latency_ms`

### KPI 2: Update Frequency
- `kpi_name`: `update_frequency_hz`
- `business meaning`: effective update rate of incoming market ticks.
- `formula`:
  - `update_interval_s = ingestion_ts_utc - lag(ingestion_ts_utc)` per `(exchange_id, symbol)`
  - `update_frequency_hz = 1 / avg(update_interval_s)`
- `unit`: hertz (`1/second`)
- `source fields`:
  - `market_ticks.ingestion_ts_utc`
  - `market_ticks.exchange_id`
  - `market_ticks.symbol`
- `validity filter`:
  - current and previous timestamp parseable
  - `update_interval_s > 0`
- `grain`:
  - interval: per consecutive tick pair
  - aggregate: per `(kpi_date_utc, exchange_id, symbol)`
- `daily aggregates`:
  - `interval_count`
  - `avg_update_interval_s`
  - `min_update_interval_s`
  - `max_update_interval_s`
  - `update_frequency_hz`

### KPI 3: Connection Drops
- `kpi_name`: `disconnect_count`
- `business meaning`: number of websocket disconnect events by platform.
- `formula`:
  - count rows in `connection_events` where `event_type = 'disconnect'`
- `unit`: count
- `source fields`:
  - `connection_events.event_ts_utc`
  - `connection_events.exchange_id`
  - `connection_events.event_type`
- `validity filter`:
  - `event_ts_utc` parseable
  - `event_type = 'disconnect'`
- `grain`:
  - aggregate: per `(kpi_date_utc, exchange_id)`
- `daily aggregates`:
  - `disconnect_count`

### KPI 4: Cross-Exchange Price Deviation
- `kpi_name`: `price_diff_abs` and `price_diff_pct`
- `business meaning`: spread between exchanges for the same symbol at the same aligned timestamp.
- `formula at aligned timestamp (bucket)`:
  - `price_diff_abs = max(price) - min(price)`
  - `price_diff_pct = (price_diff_abs / min(price)) * 100` (if `min(price) > 0`)
- `unit`:
  - `price_diff_abs`: quote currency units (for example USDT)
  - `price_diff_pct`: percent
- `source fields`:
  - `cleansed_market.bucket_start_utc`
  - `cleansed_market.bucket_epoch_s`
  - `cleansed_market.symbol`
  - `cleansed_market.exchange_id`
  - `cleansed_market.price`
  - `cleansed_market.is_missing`
  - `cleansed_market.is_stale`
- `validity filter`:
  - `price is not null`
  - `is_missing = 0`
  - `is_stale = 0`
  - at least two exchanges available in bucket
- `grain`:
  - aligned point: per `(bucket_start_utc, symbol)`
  - aggregate: per `(kpi_date_utc, symbol)`
- `daily aggregates`:
  - `aligned_points_compared`
  - `max_price_diff_abs`
  - `max_price_diff_pct`
  - `avg_price_diff_abs`
  - `avg_price_diff_pct`
  - `max_diff_bucket_start_utc`
  - `max_diff_exchange_pair`

## Aggregation Window
- default window: one UTC day (`kpi_date_utc = date(<timestamp_utc>)`)
- all KPI daily views use UTC day boundaries

## Implementation Mapping
Core KPI views are implemented in:
- `scripts/4_core/core_kpi_views.sql`

Validation checks are defined in:
- `docs/4_core/core_kpi_validation_checks.md`
- `scripts/4_core/core_kpi_assertions.sql`
