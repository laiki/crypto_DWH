-- Mart Dashboard Views
-- Last updated: 2026-02-22
--
-- Required upstream artifacts (already present in Core DB):
--   - vw_core_kpi_daily_exchange
--   - vw_core_kpi_hourly_exchange
--   - vw_core_kpi_daily_exchange_symbol
--   - vw_core_kpi_hourly_exchange_symbol
--   - vw_core_kpi_price_deviation_daily
--   - vw_core_kpi_price_deviation_hourly
--   - cleansed_market
--
-- This script is idempotent. Existing mart views are dropped and recreated.

DROP VIEW IF EXISTS vw_mart_dashboard_platform_quality_daily;
DROP VIEW IF EXISTS vw_mart_dashboard_platform_quality_hourly;
DROP VIEW IF EXISTS vw_mart_dashboard_price_deviation_daily;
DROP VIEW IF EXISTS vw_mart_dashboard_price_deviation_hourly;
DROP VIEW IF EXISTS vw_mart_latest_cleansing_run;
DROP VIEW IF EXISTS vw_mart_dashboard_price_curve_24h_binance;

CREATE VIEW vw_mart_dashboard_platform_quality_daily AS
WITH symbol_coverage AS (
    SELECT
        kpi_date_utc,
        exchange_id,
        COUNT(DISTINCT symbol) AS symbols_covered
    FROM vw_core_kpi_daily_exchange_symbol
    GROUP BY kpi_date_utc, exchange_id
)
SELECT
    q.kpi_date_utc,
    q.exchange_id,
    COALESCE(sc.symbols_covered, 0) AS symbols_covered,
    q.latency_sample_count,
    q.avg_latency_ms,
    q.min_latency_ms,
    q.max_latency_ms,
    q.interval_count,
    q.avg_update_interval_s,
    q.min_update_interval_s,
    q.max_update_interval_s,
    q.update_frequency_hz,
    COALESCE(q.disconnect_count, 0) AS disconnect_count,
    DENSE_RANK() OVER (
        PARTITION BY q.kpi_date_utc
        ORDER BY
            COALESCE(q.disconnect_count, 0) DESC,
            COALESCE(q.max_latency_ms, 0.0) DESC,
            q.exchange_id ASC
    ) AS default_quality_rank
FROM vw_core_kpi_daily_exchange AS q
LEFT JOIN symbol_coverage AS sc
    ON sc.kpi_date_utc = q.kpi_date_utc
   AND sc.exchange_id = q.exchange_id;

CREATE VIEW vw_mart_dashboard_platform_quality_hourly AS
WITH symbol_coverage AS (
    SELECT
        kpi_hour_utc,
        exchange_id,
        COUNT(DISTINCT symbol) AS symbols_covered
    FROM vw_core_kpi_hourly_exchange_symbol
    GROUP BY kpi_hour_utc, exchange_id
)
SELECT
    q.kpi_hour_utc,
    q.exchange_id,
    COALESCE(sc.symbols_covered, 0) AS symbols_covered,
    q.latency_sample_count,
    q.avg_latency_ms,
    q.min_latency_ms,
    q.max_latency_ms,
    q.interval_count,
    q.avg_update_interval_s,
    q.min_update_interval_s,
    q.max_update_interval_s,
    q.update_frequency_hz,
    COALESCE(q.disconnect_count, 0) AS disconnect_count,
    DENSE_RANK() OVER (
        PARTITION BY q.kpi_hour_utc
        ORDER BY
            COALESCE(q.disconnect_count, 0) DESC,
            COALESCE(q.max_latency_ms, 0.0) DESC,
            q.exchange_id ASC
    ) AS default_quality_rank
FROM vw_core_kpi_hourly_exchange AS q
LEFT JOIN symbol_coverage AS sc
    ON sc.kpi_hour_utc = q.kpi_hour_utc
   AND sc.exchange_id = q.exchange_id;

CREATE VIEW vw_mart_dashboard_price_deviation_daily AS
SELECT
    d.kpi_date_utc,
    d.symbol,
    d.aligned_points_compared,
    d.max_price_diff_abs,
    d.max_price_diff_pct,
    d.avg_price_diff_abs,
    d.avg_price_diff_pct,
    d.max_diff_bucket_start_utc,
    d.max_diff_exchange_pair,
    DENSE_RANK() OVER (
        PARTITION BY d.kpi_date_utc
        ORDER BY
            COALESCE(d.max_price_diff_pct, 0.0) DESC,
            COALESCE(d.max_price_diff_abs, 0.0) DESC,
            d.symbol ASC
    ) AS default_deviation_rank
FROM vw_core_kpi_price_deviation_daily AS d;

CREATE VIEW vw_mart_dashboard_price_deviation_hourly AS
SELECT
    d.kpi_hour_utc,
    d.symbol,
    d.aligned_points_compared,
    d.max_price_diff_abs,
    d.max_price_diff_pct,
    d.avg_price_diff_abs,
    d.avg_price_diff_pct,
    d.max_diff_bucket_start_utc,
    d.max_diff_exchange_pair,
    DENSE_RANK() OVER (
        PARTITION BY d.kpi_hour_utc
        ORDER BY
            COALESCE(d.max_price_diff_pct, 0.0) DESC,
            COALESCE(d.max_price_diff_abs, 0.0) DESC,
            d.symbol ASC
    ) AS default_deviation_rank
FROM vw_core_kpi_price_deviation_hourly AS d;

CREATE VIEW vw_mart_latest_cleansing_run AS
SELECT
    run_id AS latest_run_id
FROM cleansed_market
WHERE run_id IS NOT NULL
GROUP BY run_id
ORDER BY run_id DESC
LIMIT 1;

CREATE VIEW vw_mart_dashboard_price_curve_24h_binance AS
WITH latest_run AS (
    SELECT latest_run_id FROM vw_mart_latest_cleansing_run
),
filtered AS (
    SELECT
        cm.run_id,
        cm.exchange_id,
        cm.symbol,
        cm.bucket_start_utc,
        cm.bucket_epoch_s,
        cm.price
    FROM cleansed_market AS cm
    INNER JOIN latest_run AS lr
        ON lr.latest_run_id = cm.run_id
    WHERE cm.exchange_id = 'binance'
      AND cm.price IS NOT NULL
      AND cm.is_missing = 0
      AND cm.is_stale = 0
),
latest_bucket AS (
    SELECT MAX(bucket_start_utc) AS max_bucket_start_utc FROM filtered
)
SELECT
    f.run_id,
    f.exchange_id,
    f.symbol,
    f.bucket_start_utc,
    f.bucket_epoch_s,
    ROUND(f.price, 12) AS price_close,
    ROW_NUMBER() OVER (
        PARTITION BY f.symbol
        ORDER BY f.bucket_start_utc ASC
    ) AS point_index_asc
FROM filtered AS f
CROSS JOIN latest_bucket AS lb
WHERE lb.max_bucket_start_utc IS NOT NULL
  AND (julianday(lb.max_bucket_start_utc) - julianday(f.bucket_start_utc)) BETWEEN 0.0 AND 1.0;
