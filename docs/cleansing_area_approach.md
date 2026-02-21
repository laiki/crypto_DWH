# Cleansing Area Approach

## Scope
The cleansing step transforms staging tick data into a comparable time-aligned series per:
- `exchange_id`
- `symbol`

Current project decision:
- Time bin size is configurable via `--bin-seconds`.
- Current default is `1 minute` (`60 seconds`).

## Why 1-Minute Bins
- Provides a stable baseline for cross-exchange comparison.
- Reduces noise and volume versus sub-second or 5-second bins.
- Fits current objective of one-day analysis in constrained storage conditions.

Tradeoff:
- Microstructure effects (very short spikes and micro-latency differences) are smoothed out.

## Cleansing Rules
For each `(exchange_id, symbol)` pair, ticks are sorted by ingestion time and mapped into minute buckets.

Per bucket, the output uses one of the following fill methods:
- `observed`:
  - At least one tick exists in the bucket.
  - The last tick in the bucket is used.
- `forward_fill`:
  - No tick in the bucket.
  - Last known value from an earlier bucket is reused if age is within `max_forward_fill_s`.
- `interpolation` (optional, enabled via `--fill-method interpolation`):
  - No tick in the bucket.
  - A linear interpolation is computed between previous and next known value.
  - Both distances (previous-to-bucket and bucket-to-next) must be within `max_forward_fill_s`.
  - If the next value is farther away than `max_forward_fill_s`, interpolation is not applied and fallback handling is the same as `forward_fill`.
- `missing`:
  - No tick in the bucket and no eligible value for forward fill.

## Data Quality Fields
Each cleansed row carries quality indicators:
- `bin_seconds`
- `max_forward_fill_s`
- `fill_method_mode`
- `tick_count_in_bin`
- `fill_method` (`observed`, `forward_fill`, `interpolation`, `missing`)
- `age_seconds`
- `is_stale`
- `is_missing`

Definitions:
- `age_seconds`: difference between bucket start and source tick timestamp.
- `is_stale`: `1` when `age_seconds > stale_after_s`, otherwise `0`.
- `is_missing`: `1` only for `missing` rows, otherwise `0`.

## Output Objects
The cleansing output database contains:
- `cleansed_market`
  - Time-aligned price series with quality flags.
- `cleansing_pair_summary`
  - Aggregated counts per `(exchange_id, symbol)`.
- `cleansing_run_metadata`
  - Run-level parameters and totals for traceability.

Default output DB location:
- If input DB is under `.../staging/...`: `<staging_parent>/cleansing/cleaned_<input_name>_<bin_seconds>s.db`
- Otherwise: `<input_db_dir>/cleansing/cleaned_<input_name>_<bin_seconds>s.db`

In addition, a process JSON file is written next to the output DB:
- `<output_db_name>.json` (for example `cleaned_xxx.db.json`)
- Includes all rows for this run from:
  - `cleansing_run_metadata`
  - `cleansing_pair_summary`
  - `cleansed_market`

## Operational Note
- No scheduler is required right now.
- Cleansing runs are expected to be triggered manually on bounded staging exports.
