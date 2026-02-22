# Core Scripts Responsibilities

Last updated: 2026-02-22

## Purpose
This document clarifies the responsibilities and boundaries of Core layer scripts.

## Script Roles

### 1) `scripts/4_core/build_core_db.py` (implemented)
- **Primary role**: Build product-ready Core database artifacts.
- **Input**:
  - staging DB (`market_ticks`, `connection_events`)
  - cleansing DB (`cleansed_market`)
- **Output**:
  - unified Core DB for downstream analytics/dashboard
  - Core KPI views applied (`core_kpi_views.sql`)
- **Ownership**:
  - data assembly/merge
  - reproducible Core artifact creation
- **Non-goal**:
  - does not replace quality gate reporting by itself

### 2) `scripts/4_core/core_validation_runner.py` (implemented)
- **Primary role**: Validate KPI quality and consistency.
- **Input**:
  - staging DB (`market_ticks`, `connection_events`)
  - cleansing DB (`cleansed_market`)
  - SQL view and assertion scripts
- **Output**:
  - JSON + Markdown validation report
  - process exit code (`0`, `1`, `2`) for automation control
- **Ownership**:
  - assertions execution
  - quality gate decision support
  - runtime scoping options (`--kpi-date`, `--cleansing-run-id`, `--skip-view-row-counts`)
- **Non-goal**:
  - does not publish a query-ready Core artifact DB

## Why Both Scripts Exist
- Build and validation are intentionally separated:
  - **build** creates the artifact to be consumed.
  - **validation** decides whether that artifact/input scope is trustworthy.
- This separation supports:
  - reusable validation in ad hoc runs
  - CI/CD-style quality gates
  - clearer failure diagnostics without mutating product artifacts

## Recommended Pipeline Order
1. Run `build_core_db.py` to produce/update Core artifact.
2. Run `core_validation_runner.py` against staging/cleansing scope or built artifact scope.
3. Publish dashboard/mart refresh only if validation passes (`error_failed = 0`).
