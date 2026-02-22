-- Core KPI Assertion Checks
-- Requires:
--   - scripts/4_core/core_kpi_views.sql already applied on the same database
--
-- Output columns:
--   assertion_name, severity, is_failed, failed_rows, details
--
-- Performance note:
--   This version aggregates each KPI view once and reuses precomputed counters.

WITH thresholds AS (
    SELECT
        600000.0 AS max_latency_ms_limit,
        100.0 AS max_update_frequency_hz_limit,
        200.0 AS max_price_diff_pct_limit
),
latency_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN min_latency_ms < 0.0 THEN 1 ELSE 0 END) AS fail_non_negative,
        SUM(CASE WHEN max_latency_ms > (SELECT max_latency_ms_limit FROM thresholds) THEN 1 ELSE 0 END) AS fail_upper_bound
    FROM vw_core_kpi_latency_daily
),
update_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN min_update_interval_s <= 0.0 THEN 1 ELSE 0 END) AS fail_interval_positive,
        SUM(
            CASE
                WHEN update_frequency_hz < 0.0
                  OR update_frequency_hz > (SELECT max_update_frequency_hz_limit FROM thresholds)
                THEN 1 ELSE 0
            END
        ) AS fail_frequency_bounds
    FROM vw_core_kpi_update_frequency_daily
),
disconnect_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN disconnect_count < 0 THEN 1 ELSE 0 END) AS fail_non_negative
    FROM vw_core_kpi_connection_drops_daily
),
price_stats AS (
    SELECT
        COUNT(*) AS total_rows,
        SUM(CASE WHEN max_price_diff_abs < 0.0 THEN 1 ELSE 0 END) AS fail_abs_non_negative,
        SUM(
            CASE
                WHEN max_price_diff_pct < 0.0
                  OR max_price_diff_pct > (SELECT max_price_diff_pct_limit FROM thresholds)
                THEN 1 ELSE 0
            END
        ) AS fail_pct_bounds
    FROM vw_core_kpi_price_deviation_daily
),
assertions AS (
    SELECT
        'latency_non_negative' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(ls.fail_non_negative, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(ls.fail_non_negative, 0) AS failed_rows,
        'min_latency_ms must be >= 0' AS details
    FROM latency_stats AS ls

    UNION ALL

    SELECT
        'latency_upper_bound' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(ls.fail_upper_bound, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(ls.fail_upper_bound, 0) AS failed_rows,
        'max_latency_ms exceeds configured bound (600000 ms)' AS details
    FROM latency_stats AS ls

    UNION ALL

    SELECT
        'update_interval_positive' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(us.fail_interval_positive, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(us.fail_interval_positive, 0) AS failed_rows,
        'min_update_interval_s must be > 0' AS details
    FROM update_stats AS us

    UNION ALL

    SELECT
        'update_frequency_bounds' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(us.fail_frequency_bounds, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(us.fail_frequency_bounds, 0) AS failed_rows,
        'update_frequency_hz must be between 0 and 100' AS details
    FROM update_stats AS us

    UNION ALL

    SELECT
        'disconnect_count_non_negative' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(ds.fail_non_negative, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(ds.fail_non_negative, 0) AS failed_rows,
        'disconnect_count must be >= 0' AS details
    FROM disconnect_stats AS ds

    UNION ALL

    SELECT
        'price_diff_abs_non_negative' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(ps.fail_abs_non_negative, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(ps.fail_abs_non_negative, 0) AS failed_rows,
        'max_price_diff_abs must be >= 0' AS details
    FROM price_stats AS ps

    UNION ALL

    SELECT
        'price_diff_pct_bounds' AS assertion_name,
        'error' AS severity,
        CASE WHEN COALESCE(ps.fail_pct_bounds, 0) > 0 THEN 1 ELSE 0 END AS is_failed,
        COALESCE(ps.fail_pct_bounds, 0) AS failed_rows,
        'max_price_diff_pct must be between 0 and 200' AS details
    FROM price_stats AS ps

    UNION ALL

    SELECT
        'coverage_latency_daily' AS assertion_name,
        'warn' AS severity,
        CASE WHEN COALESCE(ls.total_rows, 0) = 0 THEN 1 ELSE 0 END AS is_failed,
        CASE WHEN COALESCE(ls.total_rows, 0) = 0 THEN 1 ELSE 0 END AS failed_rows,
        'No rows found in vw_core_kpi_latency_daily' AS details
    FROM latency_stats AS ls

    UNION ALL

    SELECT
        'coverage_update_frequency_daily' AS assertion_name,
        'warn' AS severity,
        CASE WHEN COALESCE(us.total_rows, 0) = 0 THEN 1 ELSE 0 END AS is_failed,
        CASE WHEN COALESCE(us.total_rows, 0) = 0 THEN 1 ELSE 0 END AS failed_rows,
        'No rows found in vw_core_kpi_update_frequency_daily' AS details
    FROM update_stats AS us

    UNION ALL

    SELECT
        'coverage_price_deviation_daily' AS assertion_name,
        'warn' AS severity,
        CASE WHEN COALESCE(ps.total_rows, 0) = 0 THEN 1 ELSE 0 END AS is_failed,
        CASE WHEN COALESCE(ps.total_rows, 0) = 0 THEN 1 ELSE 0 END AS failed_rows,
        'No rows found in vw_core_kpi_price_deviation_daily' AS details
    FROM price_stats AS ps
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
