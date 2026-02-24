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
),
base AS (
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
        COALESCE(q.disconnect_count, 0) AS disconnect_count
    FROM vw_core_kpi_daily_exchange AS q
    LEFT JOIN symbol_coverage AS sc
        ON sc.kpi_date_utc = q.kpi_date_utc
       AND sc.exchange_id = q.exchange_id
),
stats AS (
    SELECT
        b.*,
        MIN(b.min_latency_ms) OVER (PARTITION BY b.kpi_date_utc) AS min_latency_partition_min,
        MAX(b.min_latency_ms) OVER (PARTITION BY b.kpi_date_utc) AS min_latency_partition_max,
        MIN(b.update_frequency_hz) OVER (PARTITION BY b.kpi_date_utc) AS update_frequency_partition_min,
        MAX(b.update_frequency_hz) OVER (PARTITION BY b.kpi_date_utc) AS update_frequency_partition_max,
        MIN(b.disconnect_count) OVER (PARTITION BY b.kpi_date_utc) AS disconnect_partition_min,
        MAX(b.disconnect_count) OVER (PARTITION BY b.kpi_date_utc) AS disconnect_partition_max,
        MIN(b.symbols_covered) OVER (PARTITION BY b.kpi_date_utc) AS symbols_partition_min,
        MAX(b.symbols_covered) OVER (PARTITION BY b.kpi_date_utc) AS symbols_partition_max
    FROM base AS b
),
scored AS (
    SELECT
        s.*,
        CASE
            WHEN s.min_latency_ms IS NULL
              OR s.min_latency_partition_min IS NULL
              OR s.min_latency_partition_max IS NULL THEN 0.0
            WHEN s.min_latency_partition_max = s.min_latency_partition_min THEN 1.0
            ELSE (s.min_latency_partition_max - s.min_latency_ms)
                 / (s.min_latency_partition_max - s.min_latency_partition_min)
        END AS score_min_latency,
        CASE
            WHEN s.update_frequency_hz IS NULL
              OR s.update_frequency_partition_min IS NULL
              OR s.update_frequency_partition_max IS NULL THEN 0.0
            WHEN s.update_frequency_partition_max = s.update_frequency_partition_min THEN 1.0
            ELSE (s.update_frequency_hz - s.update_frequency_partition_min)
                 / (s.update_frequency_partition_max - s.update_frequency_partition_min)
        END AS score_update_frequency,
        CASE
            WHEN s.disconnect_count IS NULL
              OR s.disconnect_partition_min IS NULL
              OR s.disconnect_partition_max IS NULL THEN 0.0
            WHEN s.disconnect_partition_max = s.disconnect_partition_min THEN 1.0
            ELSE (s.disconnect_partition_max - s.disconnect_count)
                 / (s.disconnect_partition_max - s.disconnect_partition_min)
        END AS score_disconnect_count,
        CASE
            WHEN s.symbols_covered IS NULL
              OR s.symbols_partition_min IS NULL
              OR s.symbols_partition_max IS NULL THEN 0.0
            WHEN s.symbols_partition_max = s.symbols_partition_min THEN 1.0
            ELSE (s.symbols_covered - s.symbols_partition_min)
                 / (s.symbols_partition_max - s.symbols_partition_min)
        END AS score_symbols_covered
    FROM stats AS s
),
final_scored AS (
    SELECT
        s.*,
        ROUND(
            (s.score_min_latency
             + s.score_update_frequency
             + s.score_disconnect_count
             + s.score_symbols_covered) / 4.0,
            6
        ) AS default_quality_score
    FROM scored AS s
)
SELECT
    f.kpi_date_utc,
    f.exchange_id,
    f.symbols_covered,
    f.latency_sample_count,
    f.avg_latency_ms,
    f.min_latency_ms,
    f.max_latency_ms,
    f.interval_count,
    f.avg_update_interval_s,
    f.min_update_interval_s,
    f.max_update_interval_s,
    f.update_frequency_hz,
    f.disconnect_count,
    f.default_quality_score,
    DENSE_RANK() OVER (
        PARTITION BY f.kpi_date_utc
        ORDER BY
            f.default_quality_score DESC,
            f.disconnect_count ASC,
            COALESCE(f.min_latency_ms, 1e12) ASC,
            f.exchange_id ASC
    ) AS default_quality_rank
FROM final_scored AS f;

CREATE VIEW vw_mart_dashboard_platform_quality_hourly AS
WITH symbol_coverage AS (
    SELECT
        kpi_hour_utc,
        exchange_id,
        COUNT(DISTINCT symbol) AS symbols_covered
    FROM vw_core_kpi_hourly_exchange_symbol
    GROUP BY kpi_hour_utc, exchange_id
),
base AS (
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
        COALESCE(q.disconnect_count, 0) AS disconnect_count
    FROM vw_core_kpi_hourly_exchange AS q
    LEFT JOIN symbol_coverage AS sc
        ON sc.kpi_hour_utc = q.kpi_hour_utc
       AND sc.exchange_id = q.exchange_id
),
stats AS (
    SELECT
        b.*,
        MIN(b.min_latency_ms) OVER (PARTITION BY b.kpi_hour_utc) AS min_latency_partition_min,
        MAX(b.min_latency_ms) OVER (PARTITION BY b.kpi_hour_utc) AS min_latency_partition_max,
        MIN(b.update_frequency_hz) OVER (PARTITION BY b.kpi_hour_utc) AS update_frequency_partition_min,
        MAX(b.update_frequency_hz) OVER (PARTITION BY b.kpi_hour_utc) AS update_frequency_partition_max,
        MIN(b.disconnect_count) OVER (PARTITION BY b.kpi_hour_utc) AS disconnect_partition_min,
        MAX(b.disconnect_count) OVER (PARTITION BY b.kpi_hour_utc) AS disconnect_partition_max,
        MIN(b.symbols_covered) OVER (PARTITION BY b.kpi_hour_utc) AS symbols_partition_min,
        MAX(b.symbols_covered) OVER (PARTITION BY b.kpi_hour_utc) AS symbols_partition_max
    FROM base AS b
),
scored AS (
    SELECT
        s.*,
        CASE
            WHEN s.min_latency_ms IS NULL
              OR s.min_latency_partition_min IS NULL
              OR s.min_latency_partition_max IS NULL THEN 0.0
            WHEN s.min_latency_partition_max = s.min_latency_partition_min THEN 1.0
            ELSE (s.min_latency_partition_max - s.min_latency_ms)
                 / (s.min_latency_partition_max - s.min_latency_partition_min)
        END AS score_min_latency,
        CASE
            WHEN s.update_frequency_hz IS NULL
              OR s.update_frequency_partition_min IS NULL
              OR s.update_frequency_partition_max IS NULL THEN 0.0
            WHEN s.update_frequency_partition_max = s.update_frequency_partition_min THEN 1.0
            ELSE (s.update_frequency_hz - s.update_frequency_partition_min)
                 / (s.update_frequency_partition_max - s.update_frequency_partition_min)
        END AS score_update_frequency,
        CASE
            WHEN s.disconnect_count IS NULL
              OR s.disconnect_partition_min IS NULL
              OR s.disconnect_partition_max IS NULL THEN 0.0
            WHEN s.disconnect_partition_max = s.disconnect_partition_min THEN 1.0
            ELSE (s.disconnect_partition_max - s.disconnect_count)
                 / (s.disconnect_partition_max - s.disconnect_partition_min)
        END AS score_disconnect_count,
        CASE
            WHEN s.symbols_covered IS NULL
              OR s.symbols_partition_min IS NULL
              OR s.symbols_partition_max IS NULL THEN 0.0
            WHEN s.symbols_partition_max = s.symbols_partition_min THEN 1.0
            ELSE (s.symbols_covered - s.symbols_partition_min)
                 / (s.symbols_partition_max - s.symbols_partition_min)
        END AS score_symbols_covered
    FROM stats AS s
),
final_scored AS (
    SELECT
        s.*,
        ROUND(
            (s.score_min_latency
             + s.score_update_frequency
             + s.score_disconnect_count
             + s.score_symbols_covered) / 4.0,
            6
        ) AS default_quality_score
    FROM scored AS s
)
SELECT
    f.kpi_hour_utc,
    f.exchange_id,
    f.symbols_covered,
    f.latency_sample_count,
    f.avg_latency_ms,
    f.min_latency_ms,
    f.max_latency_ms,
    f.interval_count,
    f.avg_update_interval_s,
    f.min_update_interval_s,
    f.max_update_interval_s,
    f.update_frequency_hz,
    f.disconnect_count,
    f.default_quality_score,
    DENSE_RANK() OVER (
        PARTITION BY f.kpi_hour_utc
        ORDER BY
            f.default_quality_score DESC,
            f.disconnect_count ASC,
            COALESCE(f.min_latency_ms, 1e12) ASC,
            f.exchange_id ASC
    ) AS default_quality_rank
FROM final_scored AS f;

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
WITH base_filtered AS (
    SELECT
        cm.run_id,
        cm.exchange_id,
        cm.symbol,
        cm.bucket_start_utc,
        cm.bucket_epoch_s,
        cm.price
    FROM cleansed_market AS cm
    WHERE cm.exchange_id = 'binance'
      AND cm.run_id IS NOT NULL
      AND cm.price IS NOT NULL
      AND cm.is_missing = 0
      AND cm.is_stale = 0
),
latest_run_per_symbol AS (
    SELECT
        ranked.symbol,
        ranked.run_id AS latest_run_id
    FROM (
        SELECT
            bf.symbol,
            bf.run_id,
            ROW_NUMBER() OVER (
                PARTITION BY bf.symbol
                ORDER BY bf.run_id DESC
            ) AS row_num_desc
        FROM base_filtered AS bf
    ) AS ranked
    WHERE ranked.row_num_desc = 1
),
filtered AS (
    SELECT
        bf.run_id,
        bf.exchange_id,
        bf.symbol,
        bf.bucket_start_utc,
        bf.bucket_epoch_s,
        bf.price
    FROM base_filtered AS bf
    INNER JOIN latest_run_per_symbol AS lrs
        ON lrs.symbol = bf.symbol
       AND lrs.latest_run_id = bf.run_id
),
latest_bucket_per_symbol AS (
    SELECT
        symbol,
        MAX(bucket_start_utc) AS max_bucket_start_utc
    FROM filtered
    GROUP BY symbol
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
INNER JOIN latest_bucket_per_symbol AS lb
    ON lb.symbol = f.symbol
WHERE lb.max_bucket_start_utc IS NOT NULL
  AND (julianday(lb.max_bucket_start_utc) - julianday(f.bucket_start_utc)) BETWEEN 0.0 AND 1.0;
