-- Dashboard Query Templates (Mart Layer)
-- These templates define default filters and sort orders for dashboard panels.
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

-- Panel C: Price deviation (daily), optional symbol filter, default highest spread first.
WITH latest_day AS (
    SELECT MAX(kpi_date_utc) AS kpi_date_utc
    FROM vw_mart_dashboard_price_deviation_daily
)
SELECT
    kpi_date_utc,
    symbol,
    aligned_points_compared,
    max_price_diff_abs,
    max_price_diff_pct,
    avg_price_diff_abs,
    avg_price_diff_pct,
    max_diff_bucket_start_utc,
    max_diff_exchange_pair,
    default_deviation_rank
FROM vw_mart_dashboard_price_deviation_daily
WHERE kpi_date_utc = (SELECT kpi_date_utc FROM latest_day)
  AND (:symbol IS NULL OR symbol = :symbol)
ORDER BY default_deviation_rank ASC, symbol ASC;

-- Panel D: Price deviation (hourly), optional symbol filter, default highest spread first.
WITH latest_hour AS (
    SELECT MAX(kpi_hour_utc) AS kpi_hour_utc
    FROM vw_mart_dashboard_price_deviation_hourly
)
SELECT
    kpi_hour_utc,
    symbol,
    aligned_points_compared,
    max_price_diff_abs,
    max_price_diff_pct,
    avg_price_diff_abs,
    avg_price_diff_pct,
    max_diff_bucket_start_utc,
    max_diff_exchange_pair,
    default_deviation_rank
FROM vw_mart_dashboard_price_deviation_hourly
WHERE kpi_hour_utc = (SELECT kpi_hour_utc FROM latest_hour)
  AND (:symbol IS NULL OR symbol = :symbol)
ORDER BY default_deviation_rank ASC, symbol ASC;

-- Panel E: Price chart (24h), default Binance and selected symbol.
SELECT
    run_id,
    exchange_id,
    symbol,
    bucket_start_utc,
    price_close,
    point_index_asc
FROM vw_mart_dashboard_price_curve_24h_binance
WHERE symbol = :symbol
ORDER BY point_index_asc ASC;
