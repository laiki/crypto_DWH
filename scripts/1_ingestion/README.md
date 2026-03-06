# Ingestion Scripts (VAULT 2.0)

This folder contains the websocket ingestion runtime and auto-sharding orchestrator for VAULT 2.0.

## Scripts

- `ingest_all_exchanges_ws.py`
- `orchestrator_auto_shard.py`
- `ingestion_common.py`

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

## Direct ingestion example

```bash
python scripts/1_ingestion/ingest_all_exchanges_ws.py \
  --vault-root data/vault2 \
  --vault-layer ingestion \
  --exchanges binance,kraken \
  --symbols "BTC/USDT,%btc/%" \
  --log-level INFO
```

## Orchestrated ingestion example

```bash
python scripts/1_ingestion/orchestrator_auto_shard.py \
  --vault-root data/vault2 \
  --workers 4 \
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
