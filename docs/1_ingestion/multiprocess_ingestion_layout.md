# Multiprocess Ingestion Layout (VAULT 2.0)

## Goal
Scale websocket ingestion horizontally while keeping SQLite writes contention-safe and query-friendly for downstream phases.

## Core Idea
1. One orchestrator shards exchanges across worker processes.
2. Each worker runs async websocket loops for its assigned exchanges.
3. Workers write directly into VAULT partition files, grouped by `exchange + UTC hour`.
4. A shared manifest DB tracks partition metadata and pair-level min/max ranges.

There is no merge job and no monolithic raw DB in VAULT 2.0.

## Process Roles
- Orchestrator
  - evaluates exchange capabilities and throughput profile
  - computes shard plan (greedy bin packing)
  - starts and supervises worker processes
  - restarts failed workers with backoff
- Worker
  - subscribes to `watch_tickers` / `watch_ticker`
  - normalizes keys (`exchange_id_norm`, `symbol_norm`, `asset_norm`)
  - writes tick/event rows to partition DBs
  - updates manifest entries for partition and pair ranges

## Storage Layout
- Partition files:
  - `data/vault2/ingestion/exchange=<id>/date=<YYYY-MM-DD>/hour=<HH>/part_<id>_<YYYYMMDD_HH>.db`
- Manifest DB:
  - `data/vault2/meta/vault_manifest.db`
  - tables: `partition_registry`, `partition_pairs`

## Why This Layout
- Avoids multi-process write contention on a single SQLite file.
- Enables pruning by exchange/time before opening partition files.
- Preserves replayability because raw ticks/events stay immutable in partition scope.
- Supports independent retention per layer.

## Recommended Runtime Defaults
- Workers:
  - start with `min(8, max(2, cpu_count // 2))`
  - tune based on network and exchange limits
- Queue + flush:
  - queue size `50_000 .. 200_000`
  - batch size `500 .. 2_000`
  - flush interval `0.5 .. 1.0s`
- Supervision:
  - heartbeat events every ~30s
  - exponential restart backoff up to 60s

## Operational Notes
- One exchange should belong to exactly one worker to avoid partition ownership conflicts.
- All timestamps must be UTC.
- Downstream filters must use normalized columns to stay index-friendly.

## Rollout State
VAULT 2.0 is the only active ingestion architecture in this repository.
