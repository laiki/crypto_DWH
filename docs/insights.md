# Insights

## Ingestion Volume

### 2026-02-21 - First Measurement (Preliminary)
- Context:
  - Ingestion with parameters `--only-spot` and `--only-watch-tickers`
- Observation:
  - After the first 12 hours: around `45 GB` of data
- Linear estimate (rough):
  - Per hour: `45 GB / 12 h = 3.75 GB/h`
  - Per day: `3.75 GB/h * 24 h = 90 GB/day`
- Result:
  - Expected data volume: around `90 GB` per day
- Note:
  - This is an initial estimate and should be validated with a longer measurement period.

## Exchange Coverage

### 2026-02-21 - Active Exchange Yield
- Context:
  - Ingestion run across exchanges supported by CCXT.
- Observation:
  - Out of around `70` supported exchanges, only `17` delivered data.
- Impact:
  - Effective exchange coverage is significantly lower than nominal CCXT support and should be reflected in KPI interpretation.

## Operations and Infrastructure

### 2026-02-21 - Hosting and Network Throughput Considerations
- Context:
  - Ingestion run with parameters `--only-spot` and `--only-watch-tickers`.
- Observation:
  - Running ingestion from a residential network can lead to provider-level IP connection drops.
  - Reported network usage (from `nload`) was around `17 Mbit/s` download during the run.
- Impact:
  - It is operationally preferable to run ingestion on a hosted server to reduce household ISP-related disruptions.
  - The observed bandwidth demand may be problematic in private home environments.

## Staging Exporter Performance

### 2026-02-21 - Unique List Runtime
- Context:
  - Staging exporter in list mode (`--list-only`) to generate unique exchanges and assets.
- Observation:
  - Listing unique exchanges and assets currently takes around `20 minutes`.
- Impact:
  - Discovery/list-only runs are currently too slow for quick operational checks.

### 2026-02-21 - One-Hour Export Runtime
- Context:
  - Staging exporter run for a one-hour window.
- Observation:
  - Exporting one hour of data currently takes around `15-20 minutes`.
  - Runtime is similar to the unique-list-only mode (`--list-only`).
- Impact:
  - Current staging export throughput is low and can become a bottleneck for iterative analysis runs.
