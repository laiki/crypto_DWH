# Staging State Contract (Deprecated in VAULT 2.0)

## Status
This document is intentionally kept as a deprecation marker.

`staging_export_state.json` is not used anymore in VAULT 2.0.

## What Changed
- Old architecture used an incremental state file with watermarks.
- VAULT 2.0 staging exporter resolves windows from manifest metadata and explicit CLI parameters.
- Export reproducibility is tracked in table `staging_export_run_metadata` inside each staging output DB.

## Current Source of Truth
Use:
- `docs/2_staging/staging_run_contract.md`
- `scripts/2_staging/staging_exporter.py --help`

## Removal Policy
This deprecated contract can be removed once all historical references are cleaned from archived presentation material.
