# Crypto Data Engineering Project

## Goal
This project builds a data warehouse for cryptocurrencies and a dashboard to visualize key KPIs.

Focus:
- Price comparison of the same coins across multiple exchanges
- Latency and update frequency of real-time data
- Connection interruptions per platform
- Forecasts per currency and exchange

## Tech Stack (planned)
- Python
- CCXT (exchange data ingestion)
- SQLite (raw data / intermediate stages)
- Scikit-learn + additional time-series models (forecasting)
- Mermaid for architecture diagrams

## Target Architecture
1. ETL/Ingestion:
- Service process collects market data via CCXT
- Reception timestamp is stored per record

2. Staging:
- Daily export of the last 24h per exchange into a separate staging database

3. Cleansing:
- Handle data gaps
- Identify/flag outliers

4. Core:
- Compute KPIs (update frequency, latency, disconnects/day)

5. Marts:
- Compare the same currency across exchanges at aligned timestamps
- KPI analysis for dashboard consumption

## Dashboard (MVP)
- User selects a cryptocurrency
- Price chart for the last 24h (starting with Binance)
- Maximum price differences between exchanges
- Min/Max latency per platform

## Mermaid Diagrams
Architecture diagrams are maintained as `.mmd` files.

Stage structure:
- `diagrams/0_overview/`
- `diagrams/1_ingestion/`
- `diagrams/2_staging/`
- `diagrams/3_cleansing/`
- `diagrams/4_core/`
- `diagrams/5_marts/`
- `diagrams/6_forecasting/`

If `mermaid-cli` is installed:
```powershell
mmdc -i diagrams/0_overview/uml_architecture.mmd -o diagrams/0_overview/uml_architecture.svg
mmdc -i diagrams/1_ingestion/uml_sequence_ingestion.mmd -o diagrams/1_ingestion/uml_sequence_ingestion.svg
mmdc -i diagrams/4_core/uml_er_core.mmd -o diagrams/4_core/uml_er_core.svg
mmdc -i diagrams/6_forecasting/uml_forecasting_pipeline.mmd -o diagrams/6_forecasting/uml_forecasting_pipeline.svg
```

## Project Status
Current: concept and architecture definition.

## Script Documentation
Detailed English documentation for all ingestion/orchestration scripts is available at:
- `scripts/1_ingestion/README.md`

## Staging Exporter
The staging exporter creates a bounded staging snapshot from worker ingestion databases.

Script:
- `scripts/2_staging/staging_exporter.py`

Key behavior:
- Exports the last `N` hours from `worker_*.db` files.
- Supports output formats: `sqlite` (default), `csv`, `json`.
- Supports filters for exchanges and assets.
- Prints unique exchange and asset lists from all selected source DBs.
- Writes run metadata as JSON for traceability.

Examples:
```bash
python scripts/2_staging/staging_exporter.py --hours 24
python scripts/2_staging/staging_exporter.py --hours 24 --output-format csv
python scripts/2_staging/staging_exporter.py --hours 12 --exchanges binance,kraken --assets BTC,ETH
python scripts/2_staging/staging_exporter.py --hours 6 --list-only
```

Default input pattern:
- `data/worker_*_crypto_ws_ticks.db`

Default output location:
- `data/staging/`

Output naming:
- `<prefix>_<YYYYMMDD_HHMMSS>_last_<hours>h...`
- Includes UTC timestamp and export window hours.

Run metadata contract:
- `docs/staging_run_contract.md`

Operational insights are tracked at:
- `docs/insights.md`

Next steps:
1. Detailed data model (staging/core/marts)
2. Definition of KPI formulas
3. Ingestion prototype with CCXT
4. First dashboard layout
