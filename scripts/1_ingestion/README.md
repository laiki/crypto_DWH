# Ingestion Scripts (VAULT 2.0)

This folder contains the websocket ingestion runtime and auto-sharding orchestrator for VAULT 2.0.

## Scripts

- `ingest_all_exchanges_ws.py`
- `orchestrator_auto_shard.py`
- `ingestion_common.py`
- `poc_ccxt_to_redis_stream.py`
- `poc_redis_stream_to_vault_writer.py`
- `docker-compose.redis.yml`

## What changed in VAULT 2.0

- Ingestion no longer writes to monolithic `worker_*.db` files.
- Ingestion writes partitioned SQLite files under `--vault-root`:
  - `ingestion/exchange=<id>/date=<YYYY-MM-DD>/hour=<HH>/part_*.db`
- A central manifest is maintained at:
  - `<vault-root>/meta/vault_manifest.db`

## Prerequisites

```bash
python -m pip install --upgrade ccxt
python -c "import ccxt, ccxt.pro as p; print(ccxt.__version__, len(p.exchanges))"
```

## Runtime Modes

Two ingestion modes exist:

- Direct VAULT path:
  - `ingest_all_exchanges_ws.py` writes directly into VAULT partitions.
  - `orchestrator_auto_shard.py` is the recommended entrypoint for direct mode because it spawns and supervises worker processes automatically.
- Redis-decoupled path:
  - `poc_ccxt_to_redis_stream.py` publishes events into Redis Streams.
  - `poc_redis_stream_to_vault_writer.py` consumes Redis events and writes them into VAULT.

All commands below assume execution from the repository root.

## Direct VAULT Path

### Recommended startup order

Use exactly one direct startup variant for a given run:

1. Start `orchestrator_auto_shard.py` for the normal multi-worker runtime.
2. Use `ingest_all_exchanges_ws.py` only when you intentionally want a single-process/manual run instead of the orchestrator.

Do not start `orchestrator_auto_shard.py` and a manual `ingest_all_exchanges_ws.py` process for the same exchange scope at the same time unless duplicate ingestion is explicitly intended.

### Direct path: orchestrated runtime (recommended)

This is the preferred direct path. The orchestrator calculates the shard plan and starts worker processes internally, so no second ingestion command needs to be started manually.

```bash
python scripts/1_ingestion/orchestrator_auto_shard.py \
  --vault-root data/vault2 \
  --workers 4 \
  --log-level INFO
```

### Direct path: manual single-process runtime

```bash
python scripts/1_ingestion/ingest_all_exchanges_ws.py \
  --vault-root data/vault2 \
  --vault-layer ingestion \
  --exchanges binance,kraken \
  --symbols "BTC/USDT,%btc/%" \
  --log-level INFO
```

## Capability listing mode

```bash
python scripts/1_ingestion/ingest_all_exchanges_ws.py \
  --list-capabilities \
  --list-format html \
  --list-output data/exchange_symbol_capabilities.html
```

## Key runtime options

- `--vault-root`: shared VAULT root used by all workers.
- `--vault-layer`: manifest layer label (default `ingestion`).
- `--symbols` / `--symbol`: case-insensitive exact values or SQL-like patterns (`%`, `_`).
- `--only-watch-tickers`: restrict exchanges to `watchTickers` support.
- `--max-symbols-per-exchange`: cap symbol scope per exchange.

## Operational note

For best throughput and deterministic ownership of partition files, each exchange should be assigned to exactly one worker (handled by the orchestrator shard plan).

## Redis Stream PoC (Decoupled Ingestion)

### Required startup order

1. Start Redis.
2. Start `poc_redis_stream_to_vault_writer.py`.
3. Start `poc_ccxt_to_redis_stream.py`.

Reason:
- Starting the writer first ensures events are consumed immediately and persisted to VAULT without backlog buildup.
- It also ensures DLQ and retry handling are active from the first published event.

### Start Redis locally

Using Podman:

```bash
podman compose -f scripts/1_ingestion/docker-compose.redis.yml up -d
```

Using Docker:

```bash
docker compose -f scripts/1_ingestion/docker-compose.redis.yml up -d
```

### Start Redis consumer (stream to VAULT writer)

```bash
python scripts/1_ingestion/poc_redis_stream_to_vault_writer.py \
  --redis-url redis://localhost:6379/0 \
  --stream ingest:events:v1 \
  --group cg.vault_writer \
  --consumer writer-1 \
  --dlq-stream ingest:events:dlq:v1 \
  --vault-root data/vault2_redis_poc \
  --vault-layer ingestion \
  --log-level INFO
```

### Start ccxt publisher (ccxt to Redis stream)

```bash
python scripts/1_ingestion/poc_ccxt_to_redis_stream.py \
  --redis-url redis://localhost:6379/0 \
  --stream ingest:events:v1 \
  --exchange binance \
  --symbols "BTC/%,ETH/%,SOL/%,ADA/%" \
  --only-spot \
  --max-symbols 100 \
  --log-level INFO
```

Result paths:
- stream input: `ingest:events:v1`
- DLQ stream: `ingest:events:dlq:v1`
- VAULT output root: `data/vault2_redis_poc/`
