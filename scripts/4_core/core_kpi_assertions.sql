-- Core KPI Assertion Checks
-- Requires:
--   - scripts/4_core/core_kpi_views.sql already applied on the same database
--
-- Output columns:
--   assertion_name, severity, is_failed, failed_rows, details

WITH thresholds AS (
    SELECT
        600000.0 AS max_latency_ms_limit,
        100.0 AS max_update_frequency_hz_limit,
        200.0 AS max_price_diff_pct_limit
),
assertions AS (
    SELECT
        'latency_non_negative' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_latency_daily
            WHERE min_latency_ms < 0.0
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_latency_daily
            WHERE min_latency_ms < 0.0
        ) AS failed_rows,
        'min_latency_ms must be >= 0' AS details

    UNION ALL

    SELECT
        'latency_upper_bound' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_latency_daily AS l, thresholds
            WHERE l.max_latency_ms > thresholds.max_latency_ms_limit
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_latency_daily AS l, thresholds
            WHERE l.max_latency_ms > thresholds.max_latency_ms_limit
        ) AS failed_rows,
        'max_latency_ms exceeds configured bound (600000 ms)' AS details

    UNION ALL

    SELECT
        'update_interval_positive' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_update_frequency_daily
            WHERE min_update_interval_s <= 0.0
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_update_frequency_daily
            WHERE min_update_interval_s <= 0.0
        ) AS failed_rows,
        'min_update_interval_s must be > 0' AS details

    UNION ALL

    SELECT
        'update_frequency_bounds' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_update_frequency_daily AS u, thresholds
            WHERE u.update_frequency_hz < 0.0
               OR u.update_frequency_hz > thresholds.max_update_frequency_hz_limit
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_update_frequency_daily AS u, thresholds
            WHERE u.update_frequency_hz < 0.0
               OR u.update_frequency_hz > thresholds.max_update_frequency_hz_limit
        ) AS failed_rows,
        'update_frequency_hz must be between 0 and 100' AS details

    UNION ALL

    SELECT
        'disconnect_count_non_negative' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_connection_drops_daily
            WHERE disconnect_count < 0
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_connection_drops_daily
            WHERE disconnect_count < 0
        ) AS failed_rows,
        'disconnect_count must be >= 0' AS details

    UNION ALL

    SELECT
        'price_diff_abs_non_negative' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_price_deviation_daily
            WHERE max_price_diff_abs < 0.0
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_price_deviation_daily
            WHERE max_price_diff_abs < 0.0
        ) AS failed_rows,
        'max_price_diff_abs must be >= 0' AS details

    UNION ALL

    SELECT
        'price_diff_pct_bounds' AS assertion_name,
        'error' AS severity,
        CASE WHEN EXISTS (
            SELECT 1
            FROM vw_core_kpi_price_deviation_daily AS p, thresholds
            WHERE p.max_price_diff_pct < 0.0
               OR p.max_price_diff_pct > thresholds.max_price_diff_pct_limit
        ) THEN 1 ELSE 0 END AS is_failed,
        (
            SELECT COUNT(*)
            FROM vw_core_kpi_price_deviation_daily AS p, thresholds
            WHERE p.max_price_diff_pct < 0.0
               OR p.max_price_diff_pct > thresholds.max_price_diff_pct_limit
        ) AS failed_rows,
        'max_price_diff_pct must be between 0 and 200' AS details

    UNION ALL

    SELECT
        'coverage_latency_daily' AS assertion_name,
        'warn' AS severity,
        CASE WHEN (SELECT COUNT(*) FROM vw_core_kpi_latency_daily) = 0 THEN 1 ELSE 0 END AS is_failed,
        CASE WHEN (SELECT COUNT(*) FROM vw_core_kpi_latency_daily) = 0 THEN 1 ELSE 0 END AS failed_rows,
        'No rows found in vw_core_kpi_latency_daily' AS details

    UNION ALL

    SELECT
        'coverage_update_frequency_daily' AS assertion_name,
        'warn' AS severity,
        CASE WHEN (SELECT COUNT(*) FROM vw_core_kpi_update_frequency_daily) = 0 THEN 1 ELSE 0 END AS is_failed,
        CASE WHEN (SELECT COUNT(*) FROM vw_core_kpi_update_frequency_daily) = 0 THEN 1 ELSE 0 END AS failed_rows,
        'No rows found in vw_core_kpi_update_frequency_daily' AS details

    UNION ALL

    SELECT
        'coverage_price_deviation_daily' AS assertion_name,
        'warn' AS severity,
        CASE WHEN (SELECT COUNT(*) FROM vw_core_kpi_price_deviation_daily) = 0 THEN 1 ELSE 0 END AS is_failed,
        CASE WHEN (SELECT COUNT(*) FROM vw_core_kpi_price_deviation_daily) = 0 THEN 1 ELSE 0 END AS failed_rows,
        'No rows found in vw_core_kpi_price_deviation_daily' AS details
)
SELECT
    assertion_name,
    severity,
    is_failed,
    failed_rows,
    details
FROM assertions
ORDER BY
    CASE severity WHEN 'error' THEN 0 ELSE 1 END,
    assertion_name;
