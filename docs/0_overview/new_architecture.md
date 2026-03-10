# New Architecture

## Goal
VAULT 2.0 maximizes access speed through file-based partitioning, a clear indexing strategy, and a central manifest catalog for query pruning.

## Core Principles
1. No per-symbol tables.
2. File partitioning by `exchange` and time window (for example hourly).
3. Normalized filter columns (`*_norm`) for index-friendly WHERE predicates.
4. Manifest DB to preselect relevant partitions before each query.

## File Layout
```text
data/vault2/
ingestion/exchange=binance/date=2026-03-06/hour=09/part_binance_20260306_09.db
ingestion/exchange=kraken/date=2026-03-06/hour=09/part_kraken_20260306_09.db
staging/exchange=binance/window=20260306_0800_0900/staging_binance_20260306_0800_0900.db
meta/vault_manifest.db
```

## Table Model per Partition DB
```sql
CREATE TABLE market_ticks (
  ingestion_ts_utc TEXT NOT NULL,
  ingestion_ts_epoch_s INTEGER NOT NULL,
  exchange_id TEXT NOT NULL,
  exchange_id_norm TEXT NOT NULL,
  symbol TEXT NOT NULL,
  symbol_norm TEXT NOT NULL,
  price REAL,
  bid REAL,
  ask REAL,
  last REAL
);

CREATE INDEX idx_ticks_pair_ts
  ON market_ticks(exchange_id_norm, symbol_norm, ingestion_ts_epoch_s);

CREATE INDEX idx_ticks_ts
  ON market_ticks(ingestion_ts_epoch_s);
```

## Manifest DB for Partition Pruning
```sql
CREATE TABLE partition_registry (
  partition_id TEXT PRIMARY KEY,
  layer TEXT NOT NULL,
  db_path TEXT NOT NULL,
  exchange_id_norm TEXT NOT NULL,
  ts_min_epoch_s INTEGER NOT NULL,
  ts_max_epoch_s INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  created_utc TEXT NOT NULL
);

CREATE TABLE partition_pairs (
  partition_id TEXT NOT NULL,
  exchange_id_norm TEXT NOT NULL,
  symbol_norm TEXT NOT NULL,
  ts_min_epoch_s INTEGER NOT NULL,
  ts_max_epoch_s INTEGER NOT NULL,
  row_count INTEGER NOT NULL,
  PRIMARY KEY (partition_id, exchange_id_norm, symbol_norm)
);

CREATE INDEX idx_registry_exchange_time
  ON partition_registry(exchange_id_norm, ts_min_epoch_s, ts_max_epoch_s);

CREATE INDEX idx_pairs_lookup
  ON partition_pairs(exchange_id_norm, symbol_norm, ts_min_epoch_s, ts_max_epoch_s);
```

## Forecast Access Path
1. Cleansing provides relevant `(exchange, symbol)` pairs.
2. Manifest returns matching partitions before the `cutoff`.
3. Only those files are read (`ATTACH` or sequential access).
4. Queries use only `*_norm` columns without `lower(column)`.
5. Processing can run in chunks for stable RAM usage.

## Expected Performance Effects
1. Significantly fewer full scans.
2. Full index usage through sargable predicates.
3. Lower I/O via partition pruning.
4. Better parallelization (workers per exchange/partition).

## Migration Plan
1. Add `*_norm` columns and indexes to the current structure.
2. Switch ingestion to exchange+hour partition files.
3. Maintain manifest entries when writing partitions.
4. Move staging, forecasting, and core readers to manifest pruning.
5. Remove legacy monolithic access paths.

## Event-Backbone Option (Redis Decoupling)
The proposed Redis-based ingestion split is a strong architectural option:

1. Producers:
   - ccxt websocket collectors publish market ticks and connection events.
   - additional external adapters can publish into the same event contracts.
2. Backbone:
   - Redis PubSub for low-latency fan-out.
   - Redis Streams for short-lived buffering, consumer decoupling, and dead-letter handling.
3. Processing:
   - schema validation, dedup/idempotency, and normalization happen before sink writes.
4. Sinks:
   - dedicated writer services persist into SQLite VAULT (current), Postgres, or data lake.
   - VAULT is the system of record; Redis is not a historical store.

### Why this is useful
1. Source and storage are decoupled.
2. Adding a new source does not require DB-writer changes.
3. Switching DB technology does not require source-collector changes.
4. Short replay and backlog handling become operationally manageable (with Streams).

### Design cautions
1. Keep a strict event schema version (`event_type`, `schema_version`, `exchange`, `symbol`, `ts`, payload).
2. Enforce idempotent writes in sink services (dedup key per event).
3. Prefer Redis Streams over PubSub for the operational buffer path that feeds persistent writers.
4. Add consumer lag and DLQ monitoring from day one.

### Diagrams
- `diagrams/0_overview/uml_architecture_ingestion_redis_decoupling_architecture_beta.mmd`
- `diagrams/0_overview/uml_architecture_ingestion_redis_decoupling_flowchart.mmd`
- `diagrams/0_overview/uml_sequence_redis_stream_ingestion_contract.mmd`

### Contract Sketch
- `docs/0_overview/redis_event_contract.md`
