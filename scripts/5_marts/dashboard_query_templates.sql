-- Dashboard Query Templates
-- These templates define queries used by the dashboard panels.
-- Replace :param placeholders with your query tool parameter syntax.

-- Panel A: Platform quality (daily), default latest day and best quality first.
WITH latest_day AS (
    SELECT MAX(kpi_date_utc) AS kpi_date_utc
    FROM vw_mart_dashboard_platform_quality_daily
)
SELECT
    kpi_date_utc,
    exchange_id,
    symbols_covered,
    latency_sample_count,
    avg_latency_ms,
    min_latency_ms,
    max_latency_ms,
    interval_count,
    avg_update_interval_s,
    update_frequency_hz,
    disconnect_count,
    default_quality_score,
    default_quality_rank
FROM vw_mart_dashboard_platform_quality_daily
WHERE kpi_date_utc = (SELECT kpi_date_utc FROM latest_day)
ORDER BY default_quality_rank ASC, exchange_id ASC;

-- Panel B: Platform quality (hourly), default latest hour and best quality first.
WITH latest_hour AS (
    SELECT MAX(kpi_hour_utc) AS kpi_hour_utc
    FROM vw_mart_dashboard_platform_quality_hourly
)
SELECT
    kpi_hour_utc,
    exchange_id,
    symbols_covered,
    avg_latency_ms,
    max_latency_ms,
    update_frequency_hz,
    disconnect_count,
    default_quality_score,
    default_quality_rank
FROM vw_mart_dashboard_platform_quality_hourly
WHERE kpi_hour_utc = (SELECT kpi_hour_utc FROM latest_hour)
ORDER BY default_quality_rank ASC, exchange_id ASC;

-- Panel C: Available cleansing runs (for dynamic window analysis).
SELECT
    run_id,
    MIN(bucket_start_utc) AS min_bucket_start_utc,
    MAX(bucket_start_utc) AS max_bucket_start_utc,
    COUNT(*) AS row_count
FROM cleansed_market
GROUP BY run_id
ORDER BY run_id DESC;

-- Panel D: Symbols available for selected run and window.
SELECT DISTINCT symbol
FROM cleansed_market
WHERE run_id = :run_id
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
  AND price IS NOT NULL
  AND is_missing = 0
  AND is_stale = 0
ORDER BY symbol ASC;

-- Panel E: Price curve for selected run/symbol/window (multi-exchange).
SELECT
    run_id,
    exchange_id,
    symbol,
    bucket_start_utc,
    bucket_epoch_s,
    price_close,
    fill_method
FROM (
    SELECT
        run_id,
        exchange_id,
        symbol,
        bucket_start_utc,
        bucket_epoch_s,
        ROUND(price, 12) AS price_close,
        fill_method
    FROM cleansed_market
    WHERE run_id = :run_id
      AND symbol = :symbol
      AND bucket_start_utc >= :window_start_utc
      AND bucket_start_utc <= :window_end_utc
      AND price IS NOT NULL
      AND is_missing = 0
      AND is_stale = 0
)
ORDER BY bucket_epoch_s ASC, exchange_id ASC;

-- Panel F: Price deviation series for selected run/symbol/window.
WITH filtered AS (
    SELECT
        cm.bucket_start_utc,
        cm.bucket_epoch_s,
        cm.exchange_id,
        cm.symbol,
        cm.price
    FROM cleansed_market AS cm
    WHERE cm.run_id = :run_id
      AND cm.symbol = :symbol
      AND cm.bucket_start_utc >= :window_start_utc
      AND cm.bucket_start_utc <= :window_end_utc
      AND cm.price IS NOT NULL
      AND cm.is_missing = 0
      AND cm.is_stale = 0
),
ranked AS (
    SELECT
        f.symbol,
        f.bucket_epoch_s,
        f.exchange_id,
        f.price,
        ROW_NUMBER() OVER (
            PARTITION BY f.symbol, f.bucket_epoch_s
            ORDER BY f.price DESC, f.exchange_id ASC
        ) AS rn_max,
        ROW_NUMBER() OVER (
            PARTITION BY f.symbol, f.bucket_epoch_s
            ORDER BY f.price ASC, f.exchange_id ASC
        ) AS rn_min
    FROM filtered AS f
),
bucket_agg AS (
    SELECT
        f.bucket_start_utc,
        f.bucket_epoch_s,
        f.symbol,
        COUNT(*) AS exchange_count,
        MAX(f.price) AS max_price,
        MIN(f.price) AS min_price,
        MAX(f.price) - MIN(f.price) AS price_diff_abs,
        CASE
            WHEN MIN(f.price) > 0.0 THEN ((MAX(f.price) - MIN(f.price)) / MIN(f.price)) * 100.0
            ELSE NULL
        END AS price_diff_pct
    FROM filtered AS f
    GROUP BY f.bucket_start_utc, f.bucket_epoch_s, f.symbol
    HAVING COUNT(*) >= 2
),
max_exchange AS (
    SELECT symbol, bucket_epoch_s, exchange_id AS max_price_exchange_id
    FROM ranked
    WHERE rn_max = 1
),
min_exchange AS (
    SELECT symbol, bucket_epoch_s, exchange_id AS min_price_exchange_id
    FROM ranked
    WHERE rn_min = 1
)
SELECT
    b.bucket_start_utc,
    b.bucket_epoch_s,
    b.exchange_count,
    ROUND(b.max_price, 12) AS max_price,
    ROUND(b.min_price, 12) AS min_price,
    ROUND(b.price_diff_abs, 12) AS price_diff_abs,
    ROUND(b.price_diff_pct, 9) AS price_diff_pct,
    mx.max_price_exchange_id || '|' || mn.min_price_exchange_id AS max_diff_exchange_pair
FROM bucket_agg AS b
INNER JOIN max_exchange AS mx
    ON mx.symbol = b.symbol
   AND mx.bucket_epoch_s = b.bucket_epoch_s
INNER JOIN min_exchange AS mn
    ON mn.symbol = b.symbol
   AND mn.bucket_epoch_s = b.bucket_epoch_s
ORDER BY b.bucket_epoch_s ASC;

-- Panel G: Symbol start page violin input (run/window, all symbols).
-- Preferred source (cache materialization):
SELECT
    run_id,
    symbol,
    bucket_start_utc,
    bucket_epoch_s,
    exchange_count,
    price_diff_pct
FROM dash_cache_symbol_deviation_bucket
WHERE run_id = :run_id
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
ORDER BY symbol ASC, bucket_epoch_s ASC;

-- Panel H: Symbol detail spread series from mart view (fallback if no cache table).
SELECT
    run_id,
    symbol,
    bucket_start_utc,
    bucket_epoch_s,
    exchange_count,
    max_price_close,
    min_price_close,
    price_diff_abs,
    price_diff_pct,
    max_diff_exchange_pair
FROM vw_mart_dashboard_symbol_deviation_bucket
WHERE run_id = :run_id
  AND symbol = :symbol
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
ORDER BY bucket_epoch_s ASC;

-- Panel I: Symbol observed coverage quality bands (run/window).
WITH base AS (
    SELECT
        oq.symbol,
        oq.exchange_id,
        COUNT(*) AS total_points,
        SUM(oq.observed_flag) AS observed_points
    FROM vw_mart_dashboard_symbol_observed_quality_base AS oq
    WHERE oq.run_id = :run_id
      AND oq.bucket_start_utc >= :window_start_utc
      AND oq.bucket_start_utc <= :window_end_utc
    GROUP BY oq.symbol, oq.exchange_id
),
with_max AS (
    SELECT
        b.*,
        MAX(b.observed_points) OVER (PARTITION BY b.symbol) AS max_observed_points_for_symbol
    FROM base AS b
)
SELECT
    symbol,
    exchange_id,
    total_points,
    observed_points,
    max_observed_points_for_symbol,
    CASE
        WHEN max_observed_points_for_symbol > 0
            THEN ROUND((100.0 * observed_points) / max_observed_points_for_symbol, 4)
        ELSE 0.0
    END AS observed_vs_max_pct,
    CASE
        WHEN max_observed_points_for_symbol <= 0 THEN '0-50%'
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 50.0 THEN '0-50%'
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 75.0 THEN '50-75%'
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 90.0 THEN '75-90%'
        ELSE '90-100%'
    END AS observed_quality_band
FROM with_max
ORDER BY symbol ASC, observed_vs_max_pct DESC, exchange_id ASC;
