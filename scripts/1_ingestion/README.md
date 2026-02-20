# Scripts README

This document describes all Python scripts in `scripts/1_ingestion/` and explains the profiling and auto-sharding logic in detail.

## Script Overview

- `ingest_all_exchanges_ws.py`
  - Runs websocket ingestion with `ccxt.pro`.
  - Writes market ticks and connection events to SQLite.
  - Supports reconnect handling, exclusion rules, and capability listing output (`html`/`csv`).

- `orchestrator_auto_shard.py`
  - Profiles exchange load automatically.
  - Builds a shard plan across multiple worker processes.
  - Spawns, supervises, and restarts ingestion workers.

- `ingestion_common.py`
  - Shared constants and helpers used by both scripts.
  - Central place for default exchange exclusions and terminal error classification.

## Prerequisites

- Python 3.10+ (recommended).
- `ccxt` installed in the same environment used to run the scripts.
- Write access to the project directories (`data/`, `logs/`).

Install example:

```powershell
python -m pip install ccxt
```

## `ingest_all_exchanges_ws.py`

### Purpose

Continuously ingests ticker data via websocket from selected exchanges and stores:

- `market_ticks` table:
  - one row per received ticker update
  - includes local ingestion timestamp (`ingestion_ts_utc`)
- `connection_events` table:
  - lifecycle and error events (connect, disconnect, reconnect, excluded, etc.)

### Main behavior

- Uses `watchTickers` when supported.
- Falls back to `watchTicker` streams when needed.
- Handles reconnects with exponential backoff.
- Excludes exchanges on terminal errors.
- Supports `--list-capabilities` mode to export exchange/symbol capability rows.
- Writes logs to console and file:
  - console level follows `--log-level`
  - file level is at least `INFO`
  - if `--log-level DEBUG`, file level is also `DEBUG`

### Typical usage

Run ingestion:

```powershell
python scripts/1_ingestion/ingest_all_exchanges_ws.py --db-path data/crypto_ws_ticks.db
```

List capabilities to HTML:

```powershell
python scripts/1_ingestion/ingest_all_exchanges_ws.py --list-capabilities --list-format html
```

Only exchanges with `watchTickers`:

```powershell
python scripts/1_ingestion/ingest_all_exchanges_ws.py --only-watch-tickers
```

Custom log file:

```powershell
python scripts/1_ingestion/ingest_all_exchanges_ws.py --log-file logs/ingestion.log
```

## `orchestrator_auto_shard.py`

### Purpose

Automatically balances exchange ingestion load across multiple worker processes.

Logging behavior:

- Orchestrator logs are written to console and file.
- Console level follows `--log-level`.
- File level is at least `INFO`, and becomes `DEBUG` when `--log-level DEBUG`.
- Default orchestrator file path: `logs/orchestrator_auto_shard.log` (override with `--log-file`).

### End-to-end flow

1. Build initial exchange set:
   - start from all `ccxt.pro` exchanges or `--exchanges`
   - apply default exclusions and `--exchanges-exclude`
2. Profile each exchange for a short time window.
3. Compute a numeric load score for each exchange.
4. Distribute exchanges to workers with greedy bin packing.
5. Spawn one ingestion process per worker plan.
6. Monitor processes and restart failed workers with backoff.

### Profiling Explained

Profiling is implemented to estimate real traffic load before assigning exchanges to worker processes.

#### 1) Capability and symbol discovery

For each exchange:

- `load_markets()` is called.
- Symbols are filtered using:
  - active market only
  - `--only-spot` if enabled
  - `--max-symbols-per-exchange` (if provided)
- Profiling symbol subset is controlled by `--profile-symbol-limit`.

#### 2) Profiling mode selection

- If `watchTickers` is available:
  - mode = `watch_tickers`
  - profile updates from `watch_tickers(symbols)` (or signature fallback)
- Else if `watchTicker` is available:
  - mode = `watch_ticker`
  - profile multiple symbol streams concurrently
- Else:
  - mode = `unsupported` (excluded)

#### 3) Sampling window

- Duration per exchange: `--profile-seconds` (default `30.0`).
- Receive timeout per websocket call: `--profile-request-timeout-s` (default `8.0`).
- Parallel profiling width: `--profile-concurrency` (default `4`).

#### 4) Metrics collected

During profiling, the accumulator tracks:

- `ticks`: number of ticker payloads received
- `bytes_total`: approximate transferred bytes using serialized ticker payload size
- `reconnects`: reconnect/error events seen during profiling

Derived metrics:

- `ticks_per_sec = ticks / duration_s`
- `bytes_per_sec = bytes_total / duration_s`
- `reconnects_per_min = reconnects / (duration_s / 60)`

#### 5) Score formula used for sharding

```text
score = bytes_per_sec + 2000 * reconnects_per_min
```

Rules:

- if profiling succeeds, minimum score is `1.0`
- if profiling fails (`error != None`), score becomes `0.0` and exchange is excluded from runnable set

This heavily penalizes unstable exchanges by weighting reconnect frequency.

#### 6) Terminal exclusions during profiling

If a terminal error occurs (from shared config), the exchange is excluded from execution:

- `AuthenticationError`
- `ExchangeNotAvailable`
- `RequestTimeout`
- `ExchangeError`

#### 7) Worker assignment (auto-sharding)

The orchestrator uses a greedy bin-packing strategy:

1. Sort exchanges by score descending.
2. Repeatedly assign the next exchange to the worker with the smallest current total score.

This is a practical approximation of load balancing (Longest Processing Time style).

#### 8) Process supervision

- Each worker runs `ingest_all_exchanges_ws.py` with a worker-specific exchange list.
- Worker output is written to `logs/worker_<id>.log`.
- On crash/exit, worker is restarted with exponential backoff:
  - base: `--restart-base-s`
  - max: `--restart-max-s`

### Typical usage

Plan only (no worker processes):

```powershell
python scripts/1_ingestion/orchestrator_auto_shard.py --dry-run
```

Run with auto worker count:

```powershell
python scripts/1_ingestion/orchestrator_auto_shard.py
```

Run with explicit worker count and faster profiling:

```powershell
python scripts/1_ingestion/orchestrator_auto_shard.py --workers 4 --profile-seconds 20
```

## `ingestion_common.py`

Shared helpers and constants:

- `DEFAULT_EXCLUDED_EXCHANGES`
- `TERMINAL_EXCLUSION_ERROR_NAMES`
- `is_terminal_exclusion_error(...)`
- `resolve_excluded_exchanges(...)`
- `parse_exchange_list(...)`
- `select_symbols(...)`
- `iter_tickers(...)`

This module is the single source of truth for exclusion and terminal error rules across ingestion and orchestration.

## Output Artifacts

- Ingestion SQLite DB:
  - default: `data/crypto_ws_ticks.db`
  - orchestrated workers: `data/worker_<id>_crypto_ws_ticks.db` (template configurable)
- Capability exports:
  - default: `data/exchange_symbol_capabilities.html` (or `.csv`)
- Orchestrator plan:
  - default: `data/orchestrator_plan.json`
- Worker logs:
  - default directory: `logs/`

## Operational Notes

- Always run scripts via `python ...` to ensure the correct interpreter/environment is used.
- For large-scale runs, start with:
  - `--only-watch-tickers`
  - `--only-spot`
  - moderate `--profile-concurrency`
- Validate shard quality via:
  - plan scores per worker in `orchestrator_plan.json`
  - runtime queue pressure and reconnect behavior in worker logs.
