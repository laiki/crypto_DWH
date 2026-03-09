# Redis Event Contract (Ingestion Decoupling)

## Goal
Define a minimal, stable event contract so source collectors and DB writers can evolve independently.

## Delivery Model
Use **Redis Streams** as the source of truth for persistence pipelines.

- Semantics: at-least-once delivery.
- Ordering: guaranteed per stream key, not globally across all producers.
- Reliability: replay via stream IDs and consumer groups.

PubSub can still be used for live monitoring, but not as the only persistence path.

## Stream Topology (Minimal)
- Main stream:
  - `ingest:events:v1`
- Dead-letter stream:
  - `ingest:events:dlq:v1`
- Optional monitoring stream:
  - `ingest:metrics:v1`

## Consumer Groups (Minimal)
- `cg.vault_writer`
- `cg.pg_writer` (optional)
- `cg.lake_writer` (optional)
- `cg.monitor` (optional)

Each writer group can have multiple consumers for horizontal scale.

## Operational Start Order
Recommended startup sequence for the PoC and initial production rollout:

1. Start Redis.
2. Start writer consumers (at least `cg.vault_writer`).
3. Start publishers (ccxt collectors and other adapters).

This order guarantees that persistence, retries, and DLQ handoff are active before the first source events arrive.

## Envelope Schema (All Event Types)
Required top-level fields:

- `event_id`:
  - UUIDv7 string; unique per emitted event.
- `event_type`:
  - one of: `tick`, `connection_event`, `heartbeat`.
- `schema_version`:
  - for example `1.0`.
- `source_system`:
  - for example `ccxt_ws`.
- `producer_id`:
  - stable producer instance ID.
- `produced_at_ms`:
  - producer timestamp (epoch ms).
- `exchange_id`:
  - lowercase normalized exchange ID.
- `symbol`:
  - canonical symbol like `BTC/USDT`.
- `symbol_norm`:
  - lowercase symbol.
- `asset_norm`:
  - lowercase base asset.
- `event_ts_ms`:
  - event timestamp (exchange timestamp if available, otherwise ingestion timestamp).
- `ingestion_ts_ms`:
  - local collector receive timestamp.
- `dedup_key`:
  - deterministic idempotency key (see below).
- `payload`:
  - type-specific object.

Optional:
- `trace_id`
- `run_id`
- `market_type`
- `tags` (object)

## Event Payloads

### 1) `tick`
Required payload fields:
- `price`
- `bid`
- `ask`
- `last`
- `open`
- `high`
- `low`
- `base_volume`
- `quote_volume`
- `exchange_ts_ms`
- `raw_json`

### 2) `connection_event`
Required payload fields:
- `event_name`:
  - for example `disconnect`, `reconnect`, `auth_error`, `subscribe_error`.
- `severity`:
  - `info`, `warning`, `error`.
- `details_json`:
  - structured details.

## Dedup and Idempotency
Recommended dedup key derivation:

1. Tick:
   - hash of `(exchange_id, symbol_norm, event_ts_ms, price, bid, ask, last)`
2. Connection event:
   - hash of `(exchange_id, symbol_norm, event_name, event_ts_ms, severity)`

Writer services must be idempotent:

- keep `processed_event_keys` table with unique key on `dedup_key`, or
- add unique constraint directly in target tables if feasible.

If a write fails repeatedly:
- publish original event + error metadata into `ingest:events:dlq:v1`.

## Redis Stream Operations (Reference)
- Create group:
  - `XGROUP CREATE ingest:events:v1 cg.vault_writer $ MKSTREAM`
- Read:
  - `XREADGROUP GROUP cg.vault_writer writer-1 COUNT 500 BLOCK 2000 STREAMS ingest:events:v1 >`
- Ack:
  - `XACK ingest:events:v1 cg.vault_writer <id>`
- Pending recovery:
  - `XAUTOCLAIM ingest:events:v1 cg.vault_writer writer-2 60000 0-0 COUNT 100`

## Retention (Initial)
- Keep main stream by approximate max length:
  - `XADD ingest:events:v1 MAXLEN ~ 5000000 ...`
- Keep DLQ longer than main stream.

## Example Events

```json
{
  "event_id": "0195f9fb-5f8f-7c1f-9f0d-cf4e5ef4c001",
  "event_type": "tick",
  "schema_version": "1.0",
  "source_system": "ccxt_ws",
  "producer_id": "collector-worker-2",
  "produced_at_ms": 1772811000123,
  "exchange_id": "binance",
  "symbol": "BTC/USDT",
  "symbol_norm": "btc/usdt",
  "asset_norm": "btc",
  "event_ts_ms": 1772811000000,
  "ingestion_ts_ms": 1772811000120,
  "dedup_key": "b9fb1f9f8f0b1f0f5b7a2a5a...",
  "payload": {
    "price": 91234.12,
    "bid": 91234.10,
    "ask": 91234.14,
    "last": 91234.12,
    "open": 90500.00,
    "high": 91500.00,
    "low": 90100.00,
    "base_volume": 1234.56,
    "quote_volume": 112345678.90,
    "exchange_ts_ms": 1772811000000,
    "raw_json": "{\"symbol\":\"BTC/USDT\"}"
  }
}
```

```json
{
  "event_id": "0195f9fb-5f8f-7c1f-9f0d-cf4e5ef4c002",
  "event_type": "connection_event",
  "schema_version": "1.0",
  "source_system": "ccxt_ws",
  "producer_id": "collector-worker-2",
  "produced_at_ms": 1772811010456,
  "exchange_id": "binance",
  "symbol": "BTC/USDT",
  "symbol_norm": "btc/usdt",
  "asset_norm": "btc",
  "event_ts_ms": 1772811010456,
  "ingestion_ts_ms": 1772811010456,
  "dedup_key": "4ea95a1d6d7caa55f6a2e31f...",
  "payload": {
    "event_name": "disconnect",
    "severity": "warning",
    "details_json": "{\"reason\":\"websocket reset\"}"
  }
}
```

## Recommended Rollout
1. Keep current VAULT writer and introduce one Redis publisher path in parallel.
2. Add one `vault_writer` consumer group writing into the same VAULT schema.
3. Verify parity (row counts, hashes, latency).
4. Switch collectors to Redis-first mode after parity is stable.
