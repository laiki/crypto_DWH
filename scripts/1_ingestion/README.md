# Ingestion Scripts (VAULT 2.0)

This folder contains the websocket ingestion runtime and auto-sharding orchestrator for VAULT 2.0.

## Scripts

- `ingest_all_exchanges_ws.py`
- `orchestrator_auto_shard.py`
- `ingestion_common.py`
- `ccxt_to_redis_stream.py`
- `orchestrator_redis_auto_shard.py`
- `redis_stream_to_vault_writer.py`
- `poc_ccxt_to_redis_stream.py` (compatibility wrapper)
- `poc_redis_stream_to_vault_writer.py` (compatibility wrapper)
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
  - `ccxt_to_redis_stream.py` publishes events into Redis Streams.
  - `orchestrator_redis_auto_shard.py` is the recommended entrypoint for production publisher sharding.
  - `redis_stream_to_vault_writer.py` consumes Redis events and writes them into VAULT.

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

## Redis Stream Product Path (Decoupled Ingestion)

Redis in this mode is an operational buffer only.

- Redis is not the historical store.
- VAULT is the system of record for downstream analytics, replay, and auditability.
- Stream retention should stay short and only absorb brief outages, restarts, and bursts.
- The current product baseline starts with about one minute of backlog capacity as an initial sizing target.
- Because trimming is entry-count-based (`MAXLEN ~`) and not time-based, effective wall-clock retention depends on the real event rate.
- By default, publisher selection starts from all `ccxt.pro` exchanges and then applies the shared exclusion list from the direct ingestion runtime.
- Current default exclusions: `alpaca`, `arkham`, `bequant`, `bitfinex`, `bitmex`, `bitopro`, `blockchaincom`, `oxfun`, `probit`.

### Required startup order

1. Start Redis.
2. Start `redis_stream_to_vault_writer.py`.
3. Start `orchestrator_redis_auto_shard.py` for the normal multi-worker publisher runtime.
4. Use `ccxt_to_redis_stream.py` only when you intentionally want a single-process/manual publisher run instead of the orchestrator.

Reason:
- Starting the writer first ensures events are consumed immediately and persisted to VAULT without backlog buildup.
- It also ensures DLQ and retry handling are active from the first published event.
- The orchestrator distributes exchange load across multiple publisher workers, similar to the direct VAULT path.

### Retention policy

The Redis stream is intentionally configured as a short-lived buffer, not as archival storage.

- Default main stream size: about `50,000` entries.
- Default DLQ size: about `10,000` entries.
- These defaults are meant for roughly one minute of operational buffering in the current product baseline, not for historical replay.
- If the observed publish rate is materially higher or lower, adjust `--stream-maxlen` and `--dlq-maxlen` explicitly for the environment.

Sizing rule:

```text
stream_maxlen = peak_events_per_second * desired_buffer_seconds
dlq_maxlen = expected_dlq_events_per_second * desired_dlq_buffer_seconds
```

Practical guidance:

- Start with a short `desired_buffer_seconds`, for example `60`.
- Estimate `peak_events_per_second` from the real `(exchange count, symbol count, update rate)` combination.
- Add safety headroom for short bursts before setting the final `--stream-maxlen`.
- Keep DLQ separate from the main stream and size it from expected failure bursts, not from normal publish rate.

### Start Redis locally

Using Podman helper script (recommended when `podman compose` provider is not installed):

```bash
scripts/1_ingestion/start_redis_podman.sh
```

Using Podman Compose:

```bash
podman compose -f scripts/1_ingestion/docker-compose.redis.yml up -d
```

Note:
- `podman compose` requires a compose provider (`podman-compose` or `docker-compose`) on the host.
- If `podman compose` fails with `looking up compose provider failed`, use `scripts/1_ingestion/start_redis_podman.sh` instead.

Using Docker:

```bash
docker compose -f scripts/1_ingestion/docker-compose.redis.yml up -d
```

### Start Redis consumer (stream to VAULT writer)

```bash
python scripts/1_ingestion/redis_stream_to_vault_writer.py \
  --redis-url redis://localhost:6379/0 \
  --stream ingest:events:v1 \
  --group cg.vault_writer \
  --consumer writer-1 \
  --dlq-stream ingest:events:dlq:v1 \
  --stream-maxlen 50000 \
  --dlq-maxlen 10000 \
  --vault-root data/vault2_redis \
  --vault-layer ingestion \
  --log-level INFO
```

### Start Redis publisher runtime (recommended auto-sharded mode)

```bash
python scripts/1_ingestion/orchestrator_redis_auto_shard.py \
  --redis-url redis://localhost:6379/0 \
  --stream ingest:events:v1 \
  --stream-maxlen 50000 \
  --workers 4 \
  --symbols "BTC/%,ETH/%,SOL/%,ADA/%" \
  --only-spot \
  --max-symbols-per-exchange 100 \
  --log-level INFO \
  --worker-log-level INFO
```

Default exchange selection:
- If `--exchanges` is omitted, the orchestrator considers all supported `ccxt.pro` exchanges.
- The shared default exclusion list is still applied unless you override it with an explicit `--exchanges` subset and additional `--exchanges-exclude` values.

### Start Redis publisher runtime (manual single-process mode)

```bash
python scripts/1_ingestion/ccxt_to_redis_stream.py \
  --redis-url redis://localhost:6379/0 \
  --stream ingest:events:v1 \
  --stream-maxlen 50000 \
  --exchanges binance,kraken \
  --symbols "BTC/%,ETH/%,SOL/%,ADA/%" \
  --only-spot \
  --max-symbols-per-exchange 100 \
  --log-level INFO
```

Result paths:
- stream input: `ingest:events:v1`
- DLQ stream: `ingest:events:dlq:v1`
- VAULT output root: `data/vault2_redis/`
- long-term persistence target: `data/vault2_redis/`
