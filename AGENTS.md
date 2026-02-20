# AGENTS.md

## Project Context
- Project type: Data Engineering project thesis.
- Domain: Analysis of cryptocurrencies across multiple exchanges.
- Programming language: Python.
- Focus: Build a data warehouse and a dashboard to visualize results.

## Collaboration Rules
- Default response language: German.
- Unless explicitly requested otherwise, start with conceptual work first (architecture, data model, KPIs, approach), not direct implementation.
- Explain changes and decisions clearly, in small and traceable steps.
- Create architecture diagrams as Mermaid files (`.mmd`) by default.
- If `mermaid-cli` is available, also render diagrams as SVG/PNG/PDF.
- In scripts, write all comments, help texts, parameter descriptions, log/debug/info output, and other documentation text in English.
- Maintain Python dependencies centrally in `requirements.txt` at the project root and automatically extend it whenever a script needs additional external packages.

## Domain Goals
- Compare the same cryptocurrencies across different exchanges.
- Analyze:
  - Price differences at the same timestamp.
  - Real-time data latency.
  - Real-time update frequency/rate.
  - Connection drops per platform.
- Build forecasts per currency and exchange using classical ML and modern AI time-series models.

## Target Architecture (DWH Pipeline)

### 1) ETL / Ingestion
- Data source: `ccxt` (CryptoCurrency eXchange Trading Library), real-time/market data from exchanges.
- Process: A server process continuously writes into SQLite.
- Required field: Ingestion timestamp per record.
- To verify:
  - Cross-exchange normalization of coin/symbol names via CCXT.
  - Whether and how CCXT exposes per-exchange update frequency.

### 2) Staging Area
- Daily export job (cron): Last 24h of data per platform into a separate database/zone.
- Goal: Decouple raw ingestion from downstream transformations.

### 3) Cleansing Area
- Identify and handle data gaps.
- Detect and mark outliers (or clean them, depending on defined rules).

### 4) Core Layer
- Compute central meta information/KPIs:
  - Update frequency.
  - Connection drops per day.
  - Latency.

### 5) Marts / Analytics
- Compare the same cryptocurrency across different exchanges.
- Compare prices at aligned timestamps (time alignment required).
- KPI focus:
  - Maximum price differences per cryptocurrency (as graph).
  - Latency per exchange.
  - Number of connection drops per exchange.

## Forecasting Framework
- Per currency and exchange.
- Model families:
  - Scikit-learn linear models (e.g. `Ridge`; evaluate additional suitable regressors).
  - AI models from external reference list:
    - Chronos-2
    - TimesFM-2.5
    - MOIRAI-2
- Important note: Define model choice and metrics (e.g. MAE/RMSE/MAPE) up front per use case.

## Dashboard Requirements
- User can select a cryptocurrency.
- Visualizations:
  - Price curve of the last 24h (start with Binance first).
  - Maximum price differences between exchanges.
  - Min/Max latency per platform.
- Goal: Fast comparison of platform quality and price deviations.

## Open Validation and Design Questions
- Canonical symbol mapping for coins/trading pairs across exchanges.
- Consistent time synchronization (UTC) and resampling strategy for "same timestamp".
- Definition of "connection drop" and measurable latency formula.
- Handling of missing ticks and extreme outliers.
- Separation of online ingestion and batch transformation (operational stability).

## Expected Project Outcomes
- Traceable DWH architecture with documented data flows.
- KPI-based analysis of exchange quality and price deviations.
- Comparison of classical and modern forecasting approaches.
- A dashboard that clearly visualizes key insights.
