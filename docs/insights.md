# Findings

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
