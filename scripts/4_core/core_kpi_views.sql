-- Core KPI Views
-- Last updated: 2026-02-22
--
-- Required input tables in the same SQLite database:
--   - market_ticks
--   - connection_events
--   - cleansed_market
--
-- This script is idempotent. Existing views are dropped and recreated.

DROP VIEW IF EXISTS vw_core_kpi_daily_exchange;
DROP VIEW IF EXISTS vw_core_kpi_daily_exchange_symbol;
DROP VIEW IF EXISTS vw_core_kpi_hourly_exchange;
DROP VIEW IF EXISTS vw_core_kpi_hourly_exchange_symbol;
DROP VIEW IF EXISTS vw_core_kpi_price_deviation_daily;
DROP VIEW IF EXISTS vw_core_kpi_price_deviation_hourly;
DROP VIEW IF EXISTS vw_core_price_deviation_aligned;
DROP VIEW IF EXISTS vw_core_kpi_connection_drops_daily;
DROP VIEW IF EXISTS vw_core_kpi_connection_drops_hourly;
DROP VIEW IF EXISTS vw_core_kpi_update_frequency_daily;
DROP VIEW IF EXISTS vw_core_kpi_update_frequency_hourly;
DROP VIEW IF EXISTS vw_core_update_intervals;
DROP VIEW IF EXISTS vw_core_kpi_latency_daily;
DROP VIEW IF EXISTS vw_core_kpi_latency_hourly;
DROP VIEW IF EXISTS vw_core_latency_samples;

CREATE VIEW vw_core_latency_samples AS
SELECT
    date(mt.ingestion_ts_utc) AS kpi_date_utc,
    mt.exchange_id,
    mt.symbol,
    mt.ingestion_ts_utc,
    mt.exchange_ts_utc,
    (julianday(mt.ingestion_ts_utc) - julianday(mt.exchange_ts_utc)) * 86400000.0 AS latency_ms
FROM market_ticks AS mt
WHERE mt.ingestion_ts_utc IS NOT NULL
  AND mt.exchange_ts_utc IS NOT NULL
  AND strftime('%s', mt.ingestion_ts_utc) IS NOT NULL
  AND strftime('%s', mt.exchange_ts_utc) IS NOT NULL
  AND (julianday(mt.ingestion_ts_utc) - julianday(mt.exchange_ts_utc)) * 86400000.0 >= 0.0;

CREATE VIEW vw_core_kpi_latency_daily AS
SELECT
    kpi_date_utc,
    exchange_id,
    symbol,
    COUNT(*) AS latency_sample_count,
    ROUND(AVG(latency_ms), 3) AS avg_latency_ms,
    ROUND(MIN(latency_ms), 3) AS min_latency_ms,
    ROUND(MAX(latency_ms), 3) AS max_latency_ms
FROM vw_core_latency_samples
GROUP BY kpi_date_utc, exchange_id, symbol;

CREATE VIEW vw_core_kpi_latency_hourly AS
SELECT
    strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc) AS kpi_hour_utc,
    exchange_id,
    symbol,
    COUNT(*) AS latency_sample_count,
    ROUND(AVG(latency_ms), 3) AS avg_latency_ms,
    ROUND(MIN(latency_ms), 3) AS min_latency_ms,
    ROUND(MAX(latency_ms), 3) AS max_latency_ms
FROM vw_core_latency_samples
GROUP BY strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc), exchange_id, symbol;

CREATE VIEW vw_core_update_intervals AS
WITH ordered_ticks AS (
    SELECT
        mt.exchange_id,
        mt.symbol,
        mt.ingestion_ts_utc,
        LAG(mt.ingestion_ts_utc) OVER (
            PARTITION BY mt.exchange_id, mt.symbol
            ORDER BY mt.ingestion_ts_utc
        ) AS prev_ingestion_ts_utc
    FROM market_ticks AS mt
    WHERE mt.ingestion_ts_utc IS NOT NULL
)
SELECT
    date(ingestion_ts_utc) AS kpi_date_utc,
    exchange_id,
    symbol,
    ingestion_ts_utc,
    prev_ingestion_ts_utc,
    (julianday(ingestion_ts_utc) - julianday(prev_ingestion_ts_utc)) * 86400.0 AS update_interval_s
FROM ordered_ticks
WHERE prev_ingestion_ts_utc IS NOT NULL
  AND strftime('%s', ingestion_ts_utc) IS NOT NULL
  AND strftime('%s', prev_ingestion_ts_utc) IS NOT NULL
  AND (julianday(ingestion_ts_utc) - julianday(prev_ingestion_ts_utc)) * 86400.0 > 0.0;

CREATE VIEW vw_core_kpi_update_frequency_daily AS
SELECT
    kpi_date_utc,
    exchange_id,
    symbol,
    COUNT(*) AS interval_count,
    ROUND(AVG(update_interval_s), 6) AS avg_update_interval_s,
    ROUND(MIN(update_interval_s), 6) AS min_update_interval_s,
    ROUND(MAX(update_interval_s), 6) AS max_update_interval_s,
    ROUND(CASE WHEN AVG(update_interval_s) > 0.0 THEN 1.0 / AVG(update_interval_s) END, 6) AS update_frequency_hz
FROM vw_core_update_intervals
GROUP BY kpi_date_utc, exchange_id, symbol;

CREATE VIEW vw_core_kpi_update_frequency_hourly AS
SELECT
    strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc) AS kpi_hour_utc,
    exchange_id,
    symbol,
    COUNT(*) AS interval_count,
    ROUND(AVG(update_interval_s), 6) AS avg_update_interval_s,
    ROUND(MIN(update_interval_s), 6) AS min_update_interval_s,
    ROUND(MAX(update_interval_s), 6) AS max_update_interval_s,
    ROUND(CASE WHEN AVG(update_interval_s) > 0.0 THEN 1.0 / AVG(update_interval_s) END, 6) AS update_frequency_hz
FROM vw_core_update_intervals
GROUP BY strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc), exchange_id, symbol;

CREATE VIEW vw_core_kpi_connection_drops_daily AS
SELECT
    date(ce.event_ts_utc) AS kpi_date_utc,
    ce.exchange_id,
    COUNT(*) AS disconnect_count
FROM connection_events AS ce
WHERE ce.event_type = 'disconnect'
  AND ce.event_ts_utc IS NOT NULL
  AND strftime('%s', ce.event_ts_utc) IS NOT NULL
GROUP BY date(ce.event_ts_utc), ce.exchange_id;

CREATE VIEW vw_core_kpi_connection_drops_hourly AS
SELECT
    strftime('%Y-%m-%dT%H:00:00Z', ce.event_ts_utc) AS kpi_hour_utc,
    ce.exchange_id,
    COUNT(*) AS disconnect_count
FROM connection_events AS ce
WHERE ce.event_type = 'disconnect'
  AND ce.event_ts_utc IS NOT NULL
  AND strftime('%s', ce.event_ts_utc) IS NOT NULL
GROUP BY strftime('%Y-%m-%dT%H:00:00Z', ce.event_ts_utc), ce.exchange_id;

CREATE VIEW vw_core_price_deviation_aligned AS
WITH filtered AS (
    SELECT
        date(cm.bucket_start_utc) AS kpi_date_utc,
        cm.bucket_start_utc,
        cm.bucket_epoch_s,
        cm.exchange_id,
        cm.symbol,
        cm.price
    FROM cleansed_market AS cm
    WHERE cm.price IS NOT NULL
      AND cm.is_missing = 0
      AND cm.is_stale = 0
      AND cm.bucket_start_utc IS NOT NULL
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
        f.kpi_date_utc,
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
    GROUP BY f.kpi_date_utc, f.bucket_start_utc, f.bucket_epoch_s, f.symbol
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
    b.kpi_date_utc,
    b.bucket_start_utc,
    b.bucket_epoch_s,
    b.symbol,
    b.exchange_count,
    ROUND(b.max_price, 12) AS max_price,
    ROUND(b.min_price, 12) AS min_price,
    ROUND(b.price_diff_abs, 12) AS price_diff_abs,
    ROUND(b.price_diff_pct, 9) AS price_diff_pct,
    mx.max_price_exchange_id,
    mn.min_price_exchange_id,
    mx.max_price_exchange_id || '|' || mn.min_price_exchange_id AS max_diff_exchange_pair
FROM bucket_agg AS b
INNER JOIN max_exchange AS mx
    ON mx.symbol = b.symbol
   AND mx.bucket_epoch_s = b.bucket_epoch_s
INNER JOIN min_exchange AS mn
    ON mn.symbol = b.symbol
   AND mn.bucket_epoch_s = b.bucket_epoch_s;

CREATE VIEW vw_core_kpi_price_deviation_daily AS
WITH daily_agg AS (
    SELECT
        kpi_date_utc,
        symbol,
        COUNT(*) AS aligned_points_compared,
        MAX(price_diff_abs) AS max_price_diff_abs,
        MAX(price_diff_pct) AS max_price_diff_pct,
        AVG(price_diff_abs) AS avg_price_diff_abs,
        AVG(price_diff_pct) AS avg_price_diff_pct
    FROM vw_core_price_deviation_aligned
    GROUP BY kpi_date_utc, symbol
),
daily_peak AS (
    SELECT
        kpi_date_utc,
        symbol,
        bucket_start_utc,
        max_diff_exchange_pair,
        price_diff_abs,
        ROW_NUMBER() OVER (
            PARTITION BY kpi_date_utc, symbol
            ORDER BY price_diff_abs DESC, bucket_epoch_s ASC
        ) AS rn
    FROM vw_core_price_deviation_aligned
)
SELECT
    a.kpi_date_utc,
    a.symbol,
    a.aligned_points_compared,
    ROUND(a.max_price_diff_abs, 12) AS max_price_diff_abs,
    ROUND(a.max_price_diff_pct, 9) AS max_price_diff_pct,
    ROUND(a.avg_price_diff_abs, 12) AS avg_price_diff_abs,
    ROUND(a.avg_price_diff_pct, 9) AS avg_price_diff_pct,
    p.bucket_start_utc AS max_diff_bucket_start_utc,
    p.max_diff_exchange_pair
FROM daily_agg AS a
LEFT JOIN daily_peak AS p
    ON p.kpi_date_utc = a.kpi_date_utc
   AND p.symbol = a.symbol
   AND p.rn = 1;

CREATE VIEW vw_core_kpi_price_deviation_hourly AS
WITH hourly_agg AS (
    SELECT
        strftime('%Y-%m-%dT%H:00:00Z', bucket_start_utc) AS kpi_hour_utc,
        symbol,
        COUNT(*) AS aligned_points_compared,
        MAX(price_diff_abs) AS max_price_diff_abs,
        MAX(price_diff_pct) AS max_price_diff_pct,
        AVG(price_diff_abs) AS avg_price_diff_abs,
        AVG(price_diff_pct) AS avg_price_diff_pct
    FROM vw_core_price_deviation_aligned
    GROUP BY strftime('%Y-%m-%dT%H:00:00Z', bucket_start_utc), symbol
),
hourly_peak AS (
    SELECT
        strftime('%Y-%m-%dT%H:00:00Z', bucket_start_utc) AS kpi_hour_utc,
        symbol,
        bucket_start_utc,
        max_diff_exchange_pair,
        price_diff_abs,
        bucket_epoch_s,
        ROW_NUMBER() OVER (
            PARTITION BY strftime('%Y-%m-%dT%H:00:00Z', bucket_start_utc), symbol
            ORDER BY price_diff_abs DESC, bucket_epoch_s ASC
        ) AS rn
    FROM vw_core_price_deviation_aligned
)
SELECT
    a.kpi_hour_utc,
    a.symbol,
    a.aligned_points_compared,
    ROUND(a.max_price_diff_abs, 12) AS max_price_diff_abs,
    ROUND(a.max_price_diff_pct, 9) AS max_price_diff_pct,
    ROUND(a.avg_price_diff_abs, 12) AS avg_price_diff_abs,
    ROUND(a.avg_price_diff_pct, 9) AS avg_price_diff_pct,
    p.bucket_start_utc AS max_diff_bucket_start_utc,
    p.max_diff_exchange_pair
FROM hourly_agg AS a
LEFT JOIN hourly_peak AS p
    ON p.kpi_hour_utc = a.kpi_hour_utc
   AND p.symbol = a.symbol
   AND p.rn = 1;

CREATE VIEW vw_core_kpi_daily_exchange_symbol AS
WITH keys AS (
    SELECT kpi_date_utc, exchange_id, symbol FROM vw_core_kpi_latency_daily
    UNION
    SELECT kpi_date_utc, exchange_id, symbol FROM vw_core_kpi_update_frequency_daily
)
SELECT
    k.kpi_date_utc,
    k.exchange_id,
    k.symbol,
    l.latency_sample_count,
    l.avg_latency_ms,
    l.min_latency_ms,
    l.max_latency_ms,
    u.interval_count,
    u.avg_update_interval_s,
    u.min_update_interval_s,
    u.max_update_interval_s,
    u.update_frequency_hz,
    d.disconnect_count
FROM keys AS k
LEFT JOIN vw_core_kpi_latency_daily AS l
    ON l.kpi_date_utc = k.kpi_date_utc
   AND l.exchange_id = k.exchange_id
   AND l.symbol = k.symbol
LEFT JOIN vw_core_kpi_update_frequency_daily AS u
    ON u.kpi_date_utc = k.kpi_date_utc
   AND u.exchange_id = k.exchange_id
   AND u.symbol = k.symbol
LEFT JOIN vw_core_kpi_connection_drops_daily AS d
    ON d.kpi_date_utc = k.kpi_date_utc
   AND d.exchange_id = k.exchange_id;

CREATE VIEW vw_core_kpi_hourly_exchange_symbol AS
WITH keys AS (
    SELECT kpi_hour_utc, exchange_id, symbol FROM vw_core_kpi_latency_hourly
    UNION
    SELECT kpi_hour_utc, exchange_id, symbol FROM vw_core_kpi_update_frequency_hourly
)
SELECT
    k.kpi_hour_utc,
    k.exchange_id,
    k.symbol,
    l.latency_sample_count,
    l.avg_latency_ms,
    l.min_latency_ms,
    l.max_latency_ms,
    u.interval_count,
    u.avg_update_interval_s,
    u.min_update_interval_s,
    u.max_update_interval_s,
    u.update_frequency_hz,
    d.disconnect_count
FROM keys AS k
LEFT JOIN vw_core_kpi_latency_hourly AS l
    ON l.kpi_hour_utc = k.kpi_hour_utc
   AND l.exchange_id = k.exchange_id
   AND l.symbol = k.symbol
LEFT JOIN vw_core_kpi_update_frequency_hourly AS u
    ON u.kpi_hour_utc = k.kpi_hour_utc
   AND u.exchange_id = k.exchange_id
   AND u.symbol = k.symbol
LEFT JOIN vw_core_kpi_connection_drops_hourly AS d
    ON d.kpi_hour_utc = k.kpi_hour_utc
   AND d.exchange_id = k.exchange_id;

CREATE VIEW vw_core_kpi_daily_exchange AS
WITH latency_exchange AS (
    SELECT
        kpi_date_utc,
        exchange_id,
        COUNT(*) AS latency_sample_count,
        ROUND(AVG(latency_ms), 3) AS avg_latency_ms,
        ROUND(MIN(latency_ms), 3) AS min_latency_ms,
        ROUND(MAX(latency_ms), 3) AS max_latency_ms
    FROM vw_core_latency_samples
    GROUP BY kpi_date_utc, exchange_id
),
update_exchange AS (
    SELECT
        kpi_date_utc,
        exchange_id,
        COUNT(*) AS interval_count,
        ROUND(AVG(update_interval_s), 6) AS avg_update_interval_s,
        ROUND(MIN(update_interval_s), 6) AS min_update_interval_s,
        ROUND(MAX(update_interval_s), 6) AS max_update_interval_s,
        ROUND(CASE WHEN AVG(update_interval_s) > 0.0 THEN 1.0 / AVG(update_interval_s) END, 6) AS update_frequency_hz
    FROM vw_core_update_intervals
    GROUP BY kpi_date_utc, exchange_id
),
keys AS (
    SELECT kpi_date_utc, exchange_id FROM latency_exchange
    UNION
    SELECT kpi_date_utc, exchange_id FROM update_exchange
    UNION
    SELECT kpi_date_utc, exchange_id FROM vw_core_kpi_connection_drops_daily
)
SELECT
    k.kpi_date_utc,
    k.exchange_id,
    l.latency_sample_count,
    l.avg_latency_ms,
    l.min_latency_ms,
    l.max_latency_ms,
    u.interval_count,
    u.avg_update_interval_s,
    u.min_update_interval_s,
    u.max_update_interval_s,
    u.update_frequency_hz,
    d.disconnect_count
FROM keys AS k
LEFT JOIN latency_exchange AS l
    ON l.kpi_date_utc = k.kpi_date_utc
   AND l.exchange_id = k.exchange_id
LEFT JOIN update_exchange AS u
    ON u.kpi_date_utc = k.kpi_date_utc
   AND u.exchange_id = k.exchange_id
LEFT JOIN vw_core_kpi_connection_drops_daily AS d
    ON d.kpi_date_utc = k.kpi_date_utc
   AND d.exchange_id = k.exchange_id;

CREATE VIEW vw_core_kpi_hourly_exchange AS
WITH latency_exchange AS (
    SELECT
        strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc) AS kpi_hour_utc,
        exchange_id,
        COUNT(*) AS latency_sample_count,
        ROUND(AVG(latency_ms), 3) AS avg_latency_ms,
        ROUND(MIN(latency_ms), 3) AS min_latency_ms,
        ROUND(MAX(latency_ms), 3) AS max_latency_ms
    FROM vw_core_latency_samples
    GROUP BY strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc), exchange_id
),
update_exchange AS (
    SELECT
        strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc) AS kpi_hour_utc,
        exchange_id,
        COUNT(*) AS interval_count,
        ROUND(AVG(update_interval_s), 6) AS avg_update_interval_s,
        ROUND(MIN(update_interval_s), 6) AS min_update_interval_s,
        ROUND(MAX(update_interval_s), 6) AS max_update_interval_s,
        ROUND(CASE WHEN AVG(update_interval_s) > 0.0 THEN 1.0 / AVG(update_interval_s) END, 6) AS update_frequency_hz
    FROM vw_core_update_intervals
    GROUP BY strftime('%Y-%m-%dT%H:00:00Z', ingestion_ts_utc), exchange_id
),
keys AS (
    SELECT kpi_hour_utc, exchange_id FROM latency_exchange
    UNION
    SELECT kpi_hour_utc, exchange_id FROM update_exchange
    UNION
    SELECT kpi_hour_utc, exchange_id FROM vw_core_kpi_connection_drops_hourly
)
SELECT
    k.kpi_hour_utc,
    k.exchange_id,
    l.latency_sample_count,
    l.avg_latency_ms,
    l.min_latency_ms,
    l.max_latency_ms,
    u.interval_count,
    u.avg_update_interval_s,
    u.min_update_interval_s,
    u.max_update_interval_s,
    u.update_frequency_hz,
    d.disconnect_count
FROM keys AS k
LEFT JOIN latency_exchange AS l
    ON l.kpi_hour_utc = k.kpi_hour_utc
   AND l.exchange_id = k.exchange_id
LEFT JOIN update_exchange AS u
    ON u.kpi_hour_utc = k.kpi_hour_utc
   AND u.exchange_id = k.exchange_id
LEFT JOIN vw_core_kpi_connection_drops_hourly AS d
    ON d.kpi_hour_utc = k.kpi_hour_utc
   AND d.exchange_id = k.exchange_id;
