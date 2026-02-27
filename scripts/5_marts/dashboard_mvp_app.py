#!/usr/bin/env python3
"""
Dashboard MVP for mart-based crypto KPI visualization.

Panels:
- Price curve for a selectable UTC time window from cleansed data.
- Price deviation snapshot/series for the same selectable UTC time window.
- Platform quality snapshot (daily, all exchanges).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import json
import sqlite3
from pathlib import Path

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st


DEFAULT_DB_PATH = Path("data/core/core_kpi.db")
THEME_OPTIONS = ("Dark", "Light")
PAGINATION_SIGNATURE_STATE_KEY = "violin_page_signature_v1"
OBSERVED_QUALITY_BANDS = ("0-50%", "50-75%", "75-90%", "90-100%")

DARK_THEME_CSS = """
<style>
[data-testid="stAppViewContainer"] {
  background: #0e1117;
  color: #e6edf3;
}
[data-testid="stSidebar"] {
  background: #161b22;
}
[data-testid="stSidebar"] * {
  color: #aeb9c5;
}
[data-testid="stMetricValue"] {
  color: #f0f6fc;
}
[data-testid="stMetricLabel"] {
  color: #9aa6b2;
}
[data-testid="stDataFrame"] {
  background: #0e1117;
}
h1, h2, h3, h4, h5, h6, p, span, label, div {
  color: #bcc7d3;
}
[data-testid="stTextInput"] input,
[data-testid="stTextArea"] textarea,
[data-testid="stNumberInput"] input {
  background: #1a2330 !important;
  border: 1px solid #2c3746 !important;
  color: #9da9b6 !important;
}
[data-testid="stSelectbox"] [data-baseweb="select"] *,
[data-testid="stMultiSelect"] [data-baseweb="select"] * {
  color: #9da9b6 !important;
}
[data-testid="stSelectbox"] [data-baseweb="select"] > div,
[data-testid="stMultiSelect"] [data-baseweb="select"] > div {
  background: #1a2330 !important;
  border-color: #2c3746 !important;
}
[data-baseweb="popover"] [role="listbox"] {
  background: #1a2330 !important;
  border: 1px solid #2c3746 !important;
}
[data-baseweb="popover"] [role="option"] {
  background: #1a2330 !important;
  color: #9da9b6 !important;
}
[data-baseweb="popover"] [role="option"][aria-selected="true"],
[data-baseweb="popover"] [role="option"]:hover {
  background: #243246 !important;
}
[data-testid="stCheckbox"] label,
[data-testid="stCheckbox"] span {
  color: #9da9b6 !important;
}
[data-testid="stButton"] button,
[data-testid="stDownloadButton"] button {
  background: #1a2330 !important;
  border: 1px solid #2c3746 !important;
  color: #9da9b6 !important;
}
</style>
"""

LIGHT_THEME_CSS = """
<style>
[data-testid="stAppViewContainer"] {
  background: #ffffff;
  color: #111827;
}
[data-testid="stSidebar"] {
  background: #f8fafc;
}
[data-testid="stSidebar"] * {
  color: #111827;
}
[data-testid="stMetricValue"] {
  color: #111827;
}
[data-testid="stMetricLabel"] {
  color: #4b5563;
}
[data-testid="stDataFrame"] {
  background: #ffffff;
}
h1, h2, h3, h4, h5, h6, p, span, label, div {
  color: #111827;
}
</style>
"""

PLOTLY_CHART_CONFIG = {
    "displayModeBar": True,
    "displaylogo": False,
    "modeBarButtonsToAdd": ["resetScale2d", "autoScale2d"],
}

SQL_PLATFORM_QUALITY_DAILY_CACHE = """
SELECT
    kpi_date_utc,
    exchange_id,
    symbols_covered,
    avg_latency_ms,
    min_latency_ms,
    max_latency_ms,
    update_frequency_hz,
    disconnect_count,
    default_quality_score,
    default_quality_rank
FROM dash_cache_platform_quality_daily_latest
ORDER BY default_quality_rank ASC, exchange_id ASC;
"""

SQL_PLATFORM_QUALITY_DAILY_VIEW = """
WITH latest_day AS (
    SELECT MAX(kpi_date_utc) AS kpi_date_utc
    FROM vw_mart_dashboard_platform_quality_daily
)
SELECT
    kpi_date_utc,
    exchange_id,
    symbols_covered,
    avg_latency_ms,
    min_latency_ms,
    max_latency_ms,
    update_frequency_hz,
    disconnect_count,
    default_quality_score,
    default_quality_rank
FROM vw_mart_dashboard_platform_quality_daily
WHERE kpi_date_utc = (SELECT kpi_date_utc FROM latest_day)
ORDER BY default_quality_rank ASC, exchange_id ASC;
"""

SQL_CLEANSED_RUNS = """
SELECT
    run_id,
    MIN(bucket_start_utc) AS min_bucket_start_utc,
    MAX(bucket_start_utc) AS max_bucket_start_utc,
    COUNT(*) AS row_count
FROM cleansed_market
GROUP BY run_id
ORDER BY run_id DESC;
"""

SQL_RUN_RANGE = """
SELECT
    MIN(bucket_start_utc) AS min_bucket_start_utc,
    MAX(bucket_start_utc) AS max_bucket_start_utc,
    COUNT(*) AS row_count,
    SUM(CASE WHEN price IS NOT NULL AND is_missing = 0 AND is_stale = 0 THEN 1 ELSE 0 END) AS valid_row_count
FROM cleansed_market
WHERE run_id = :run_id;
"""

SQL_SYMBOLS_BY_RUN_WINDOW = """
SELECT DISTINCT symbol
FROM cleansed_market
WHERE run_id = :run_id
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
  AND price IS NOT NULL
  AND is_missing = 0
  AND is_stale = 0
  AND symbol IS NOT NULL
ORDER BY symbol ASC;
"""

SQL_SYMBOLS_BY_RUN = """
SELECT DISTINCT symbol
FROM cleansed_market
WHERE run_id = :run_id
  AND price IS NOT NULL
  AND is_missing = 0
  AND is_stale = 0
  AND symbol IS NOT NULL
ORDER BY symbol ASC;
"""

SQL_SYMBOL_OBSERVED_QUALITY_WINDOW_VIEW = """
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
    END AS observed_quality_band,
    CASE
        WHEN max_observed_points_for_symbol <= 0 THEN 1
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 50.0 THEN 1
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 75.0 THEN 2
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 90.0 THEN 3
        ELSE 4
    END AS observed_quality_band_order
FROM with_max
ORDER BY symbol ASC, observed_vs_max_pct DESC, exchange_id ASC;
"""

SQL_SYMBOL_OBSERVED_QUALITY_WINDOW_RAW = """
WITH base AS (
    SELECT
        cm.symbol,
        cm.exchange_id,
        COUNT(*) AS total_points,
        SUM(CASE WHEN cm.fill_method = 'observed' THEN 1 ELSE 0 END) AS observed_points
    FROM cleansed_market AS cm
    WHERE cm.run_id = :run_id
      AND cm.bucket_start_utc >= :window_start_utc
      AND cm.bucket_start_utc <= :window_end_utc
      AND cm.symbol IS NOT NULL
      AND cm.exchange_id IS NOT NULL
      AND cm.price IS NOT NULL
      AND cm.is_missing = 0
      AND cm.is_stale = 0
    GROUP BY cm.symbol, cm.exchange_id
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
    END AS observed_quality_band,
    CASE
        WHEN max_observed_points_for_symbol <= 0 THEN 1
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 50.0 THEN 1
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 75.0 THEN 2
        WHEN (100.0 * observed_points) / max_observed_points_for_symbol < 90.0 THEN 3
        ELSE 4
    END AS observed_quality_band_order
FROM with_max
ORDER BY symbol ASC, observed_vs_max_pct DESC, exchange_id ASC;
"""

SQL_PRICE_CURVE_WINDOW = """
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
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
  AND symbol = :symbol
  AND price IS NOT NULL
  AND is_missing = 0
  AND is_stale = 0
ORDER BY bucket_epoch_s ASC, exchange_id ASC;
"""

SQL_PRICE_DEVIATION_WINDOW_RAW = """
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
"""

SQL_PRICE_DEVIATION_WINDOW_CACHE = """
SELECT
    bucket_start_utc,
    bucket_epoch_s,
    exchange_count,
    max_price_close AS max_price,
    min_price_close AS min_price,
    price_diff_abs,
    price_diff_pct,
    max_diff_exchange_pair
FROM dash_cache_symbol_deviation_bucket
WHERE run_id = :run_id
  AND symbol = :symbol
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
ORDER BY bucket_epoch_s ASC;
"""

SQL_PRICE_DEVIATION_WINDOW_VIEW = """
SELECT
    bucket_start_utc,
    bucket_epoch_s,
    exchange_count,
    max_price_close AS max_price,
    min_price_close AS min_price,
    price_diff_abs,
    price_diff_pct,
    max_diff_exchange_pair
FROM vw_mart_dashboard_symbol_deviation_bucket
WHERE run_id = :run_id
  AND symbol = :symbol
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
ORDER BY bucket_epoch_s ASC;
"""

SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW_RAW = """
WITH filtered AS (
    SELECT
        cm.bucket_start_utc,
        cm.bucket_epoch_s,
        cm.symbol,
        cm.price
    FROM cleansed_market AS cm
    WHERE cm.run_id = :run_id
      AND cm.bucket_start_utc >= :window_start_utc
      AND cm.bucket_start_utc <= :window_end_utc
      AND cm.price IS NOT NULL
      AND cm.is_missing = 0
      AND cm.is_stale = 0
      AND cm.symbol IS NOT NULL
),
bucket_agg AS (
    SELECT
        f.bucket_start_utc,
        f.bucket_epoch_s,
        f.symbol,
        COUNT(*) AS exchange_count,
        CASE
            WHEN MIN(f.price) > 0.0 THEN ((MAX(f.price) - MIN(f.price)) / MIN(f.price)) * 100.0
            ELSE NULL
        END AS price_diff_pct
    FROM filtered AS f
    GROUP BY f.bucket_start_utc, f.bucket_epoch_s, f.symbol
    HAVING COUNT(*) >= 2
)
SELECT
    symbol,
    bucket_start_utc,
    bucket_epoch_s,
    exchange_count,
    ROUND(price_diff_pct, 9) AS price_diff_pct
FROM bucket_agg
WHERE price_diff_pct IS NOT NULL
ORDER BY symbol ASC, bucket_epoch_s ASC;
"""

SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW_CACHE = """
SELECT
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
"""

SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW_VIEW = """
SELECT
    symbol,
    bucket_start_utc,
    bucket_epoch_s,
    exchange_count,
    price_diff_pct
FROM vw_mart_dashboard_symbol_deviation_bucket
WHERE run_id = :run_id
  AND bucket_start_utc >= :window_start_utc
  AND bucket_start_utc <= :window_end_utc
ORDER BY symbol ASC, bucket_epoch_s ASC;
"""

SQL_EXISTING_OBJECTS = """
SELECT type, name
FROM sqlite_master
WHERE type IN ('table', 'view');
"""

SQL_CACHE_METADATA = """
SELECT
    refresh_ts_utc,
    platform_latest_kpi_date_utc,
    deviation_latest_kpi_date_utc,
    platform_rows,
    deviation_rows,
    curve_rows,
    symbol_rows
FROM dash_cache_refresh_metadata
ORDER BY refresh_ts_utc DESC
LIMIT 1;
"""


@st.cache_data(show_spinner=False, ttl=180)
def _query_dataframe_cached(
    db_path_str: str,
    query: str,
    params_json: str,
) -> pd.DataFrame:
    params = json.loads(params_json) if params_json else None
    with sqlite3.connect(db_path_str) as connection:
        connection.execute("PRAGMA query_only = ON;")
        return pd.read_sql_query(query, connection, params=params)


def _query_dataframe(db_path: Path, query: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    params_json = json.dumps(params or {}, sort_keys=True)
    return _query_dataframe_cached(str(db_path), query, params_json)


def _get_existing_objects(db_path: Path) -> set[str]:
    objects_df = _query_dataframe(db_path, SQL_EXISTING_OBJECTS)
    if objects_df.empty:
        return set()
    return set(objects_df["name"].astype(str).tolist())


def _format_float(value: object, decimals: int = 3) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):.{decimals}f}"


def _to_utc_timestamp(value: object) -> pd.Timestamp | None:
    parsed = pd.to_datetime(value, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed


def _to_naive_utc_datetime(ts: pd.Timestamp) -> datetime:
    return ts.tz_convert("UTC").tz_localize(None).to_pydatetime()


def _naive_utc_datetime_to_iso(ts: datetime) -> str:
    return ts.replace(tzinfo=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _get_symbol_query_param() -> str | None:
    raw_value = st.query_params.get("symbol")
    if raw_value is None:
        return None
    if isinstance(raw_value, list):
        if not raw_value:
            return None
        value = raw_value[0]
    else:
        value = raw_value
    value_str = str(value).strip()
    return value_str if value_str else None


def _set_symbol_query_param(symbol: str) -> None:
    st.query_params["symbol"] = symbol


def _parse_contains_terms(raw_value: str) -> list[str]:
    return [term.strip().lower() for term in raw_value.split(",") if term.strip()]


def _parse_numeric_criteria(raw_value: str) -> tuple[list[tuple[float, float]], list[str]]:
    criteria: list[tuple[float, float]] = []
    invalid_tokens: list[str] = []
    for token in [part.strip() for part in raw_value.split(",") if part.strip()]:
        try:
            if token.startswith(">="):
                value = float(token[2:].strip())
                criteria.append((value, float("inf")))
            elif token.startswith(">"):
                value = float(token[1:].strip())
                criteria.append((value + 1e-12, float("inf")))
            elif token.startswith("<="):
                value = float(token[2:].strip())
                criteria.append((float("-inf"), value))
            elif token.startswith("<"):
                value = float(token[1:].strip())
                criteria.append((float("-inf"), value - 1e-12))
            elif "-" in token:
                low_raw, high_raw = token.split("-", 1)
                low = float(low_raw.strip())
                high = float(high_raw.strip())
                criteria.append((min(low, high), max(low, high)))
            else:
                value = float(token)
                criteria.append((value, value))
        except ValueError:
            invalid_tokens.append(token)
    return criteria, invalid_tokens


def _apply_contains_filter(
    df: pd.DataFrame,
    column_name: str,
    terms: list[str],
) -> pd.DataFrame:
    if not terms:
        return df
    mask = pd.Series(False, index=df.index)
    lower_series = df[column_name].astype(str).str.lower()
    for term in terms:
        mask = mask | lower_series.str.contains(term, na=False)
    return df[mask]


def _apply_numeric_criteria_filter(
    df: pd.DataFrame,
    column_name: str,
    criteria: list[tuple[float, float]],
) -> pd.DataFrame:
    if not criteria:
        return df
    mask = pd.Series(False, index=df.index)
    for low, high in criteria:
        mask = mask | df[column_name].between(low, high, inclusive="both")
    return df[mask]


def _build_deviation_from_curve(price_curve_df: pd.DataFrame) -> pd.DataFrame:
    output_columns = [
        "bucket_start_utc",
        "bucket_epoch_s",
        "exchange_count",
        "max_price",
        "min_price",
        "price_diff_abs",
        "price_diff_pct",
        "max_diff_exchange_pair",
    ]
    if price_curve_df.empty:
        return pd.DataFrame(columns=output_columns)

    working = price_curve_df.copy()
    for column_name in ["bucket_epoch_s", "price_close"]:
        working[column_name] = pd.to_numeric(working[column_name], errors="coerce")
    working["exchange_id"] = working["exchange_id"].astype(str)
    working = working.dropna(subset=["bucket_start_utc", "bucket_epoch_s", "price_close", "exchange_id"])
    if working.empty:
        return pd.DataFrame(columns=output_columns)

    working["bucket_epoch_s"] = working["bucket_epoch_s"].astype(int)
    group_columns = ["bucket_start_utc", "bucket_epoch_s"]
    agg_df = (
        working.groupby(group_columns, as_index=False)
        .agg(
            exchange_count=("exchange_id", "nunique"),
            max_price=("price_close", "max"),
            min_price=("price_close", "min"),
        )
    )
    agg_df = agg_df[agg_df["exchange_count"] >= 2].copy()
    if agg_df.empty:
        return pd.DataFrame(columns=output_columns)

    agg_df["price_diff_abs"] = agg_df["max_price"] - agg_df["min_price"]
    agg_df["price_diff_pct"] = (agg_df["price_diff_abs"] / agg_df["min_price"]) * 100.0
    agg_df.loc[agg_df["min_price"] <= 0.0, "price_diff_pct"] = pd.NA

    max_exchange_df = (
        working.sort_values(
            by=["bucket_start_utc", "bucket_epoch_s", "price_close", "exchange_id"],
            ascending=[True, True, False, True],
        )
        .drop_duplicates(subset=group_columns, keep="first")
        .loc[:, group_columns + ["exchange_id"]]
        .rename(columns={"exchange_id": "max_price_exchange_id"})
    )
    min_exchange_df = (
        working.sort_values(
            by=["bucket_start_utc", "bucket_epoch_s", "price_close", "exchange_id"],
            ascending=[True, True, True, True],
        )
        .drop_duplicates(subset=group_columns, keep="first")
        .loc[:, group_columns + ["exchange_id"]]
        .rename(columns={"exchange_id": "min_price_exchange_id"})
    )
    merged_df = agg_df.merge(max_exchange_df, on=group_columns, how="left").merge(
        min_exchange_df,
        on=group_columns,
        how="left",
    )
    merged_df["max_diff_exchange_pair"] = (
        merged_df["max_price_exchange_id"].astype(str)
        + "|"
        + merged_df["min_price_exchange_id"].astype(str)
    )
    merged_df["max_price"] = merged_df["max_price"].round(12)
    merged_df["min_price"] = merged_df["min_price"].round(12)
    merged_df["price_diff_abs"] = merged_df["price_diff_abs"].round(12)
    merged_df["price_diff_pct"] = merged_df["price_diff_pct"].round(9)
    merged_df = merged_df[merged_df["price_diff_pct"].notna()].copy()
    return merged_df[output_columns].sort_values(by=["bucket_epoch_s"], ascending=[True]).reset_index(
        drop=True
    )


def _apply_theme(theme_mode: str) -> str:
    if theme_mode == "Dark":
        st.markdown(DARK_THEME_CSS, unsafe_allow_html=True)
        return "plotly_dark"
    st.markdown(LIGHT_THEME_CSS, unsafe_allow_html=True)
    return "plotly_white"


def main() -> None:
    st.set_page_config(page_title="Crypto KPI Dashboard MVP", layout="wide")
    st.title("Crypto KPI Dashboard MVP")
    st.caption("Source: SQLite Core DB (dynamic window analysis on cleansed data)")

    with st.sidebar:
        st.header("Configuration")
        theme_mode = st.radio(
            "Theme",
            options=THEME_OPTIONS,
            index=0,
            horizontal=True,
        )
        db_path_raw = st.text_input("Core SQLite DB path", str(DEFAULT_DB_PATH))
        if st.button("Refresh Query Cache"):
            st.cache_data.clear()
            st.success("Query cache cleared.")
    plotly_template = _apply_theme(theme_mode)

    db_path = Path(db_path_raw).expanduser()
    if not db_path.exists():
        st.error(f"Database path does not exist: {db_path}")
        st.stop()

    existing_objects = _get_existing_objects(db_path)
    has_platform_quality_cache = "dash_cache_platform_quality_daily_latest" in existing_objects
    has_quality_view = "vw_mart_dashboard_platform_quality_daily" in existing_objects
    has_symbol_deviation_cache = "dash_cache_symbol_deviation_bucket" in existing_objects
    has_symbol_deviation_view = "vw_mart_dashboard_symbol_deviation_bucket" in existing_objects
    has_symbol_observed_quality_view = "vw_mart_dashboard_symbol_observed_quality_base" in existing_objects
    has_cleansed_market = "cleansed_market" in existing_objects

    if not has_cleansed_market:
        st.error("Missing required table: cleansed_market")
        st.stop()

    with st.sidebar:
        if has_platform_quality_cache:
            st.success("Platform quality source: cache table")
        elif has_quality_view:
            st.warning("Platform quality source: mart view fallback (no cache table)")
        else:
            st.warning("Platform quality source unavailable (missing mart view and cache table)")

        if has_symbol_deviation_cache:
            st.success("Symbol deviation source: cache table")
        elif has_symbol_deviation_view:
            st.warning("Symbol deviation source: mart view fallback (no cache table)")
        else:
            st.warning("Symbol deviation source: raw fallback from cleansed_market")

        if has_symbol_observed_quality_view:
            st.success("Observed quality source: mart view")
        else:
            st.warning("Observed quality source: raw fallback from cleansed_market")

    if "dash_cache_refresh_metadata" in existing_objects:
        cache_meta_df = _query_dataframe(db_path, SQL_CACHE_METADATA)
        if not cache_meta_df.empty:
            meta_row = cache_meta_df.iloc[0]
            with st.sidebar:
                st.caption(f"Last cache refresh (UTC): {meta_row['refresh_ts_utc']}")
                st.caption(
                    "Cache rows: "
                    f"platform={int(meta_row['platform_rows'])}, "
                    f"deviation={int(meta_row['deviation_rows'])}, "
                    f"curve={int(meta_row['curve_rows'])}, "
                    f"symbols={int(meta_row['symbol_rows'])}"
                )

    runs_df = _query_dataframe(db_path, SQL_CLEANSED_RUNS)
    if runs_df.empty:
        st.error("No cleansed runs found in cleansed_market.")
        st.stop()

    with st.sidebar:
        run_ids = runs_df["run_id"].astype(str).tolist()
        selected_run_id = st.selectbox("Cleansing Run", options=run_ids, index=0)

    run_range_df = _query_dataframe(
        db_path,
        SQL_RUN_RANGE,
        params={"run_id": selected_run_id},
    )
    if run_range_df.empty:
        st.error("Failed to load selected run range.")
        st.stop()

    run_range_row = run_range_df.iloc[0]
    run_min_ts = _to_utc_timestamp(run_range_row["min_bucket_start_utc"])
    run_max_ts = _to_utc_timestamp(run_range_row["max_bucket_start_utc"])
    if run_min_ts is None or run_max_ts is None:
        st.error("Selected run has no valid bucket timestamps.")
        st.stop()

    min_dt = _to_naive_utc_datetime(run_min_ts)
    max_dt = _to_naive_utc_datetime(run_max_ts)
    default_start_dt = max(min_dt, max_dt - timedelta(hours=1))

    with st.sidebar:
        st.caption(
            "Run coverage (UTC): "
            f"{run_min_ts.strftime('%Y-%m-%d %H:%M:%S')} -> {run_max_ts.strftime('%Y-%m-%d %H:%M:%S')}"
        )
        st.caption(
            "Run rows: "
            f"total={int(run_range_row['row_count'])}, "
            f"valid={int(run_range_row['valid_row_count'])}"
        )
        if min_dt < max_dt:
            window_start_dt, window_end_dt = st.slider(
                "Analysis Window (UTC)",
                min_value=min_dt,
                max_value=max_dt,
                value=(default_start_dt, max_dt),
                step=timedelta(minutes=1),
                format="YYYY-MM-DD HH:mm:ss",
            )
        else:
            window_start_dt = min_dt
            window_end_dt = max_dt
            st.caption("Run has a single bucket timestamp; window selection is fixed.")

    window_start_utc = _naive_utc_datetime_to_iso(window_start_dt)
    window_end_utc = _naive_utc_datetime_to_iso(window_end_dt)

    observed_quality_query = (
        SQL_SYMBOL_OBSERVED_QUALITY_WINDOW_VIEW
        if has_symbol_observed_quality_view
        else SQL_SYMBOL_OBSERVED_QUALITY_WINDOW_RAW
    )
    symbol_observed_quality_df = _query_dataframe(
        db_path,
        observed_quality_query,
        params={
            "run_id": selected_run_id,
            "window_start_utc": window_start_utc,
            "window_end_utc": window_end_utc,
        },
    )
    if not symbol_observed_quality_df.empty:
        symbol_observed_quality_df["symbol"] = symbol_observed_quality_df["symbol"].astype(str)
        symbol_observed_quality_df["exchange_id"] = symbol_observed_quality_df["exchange_id"].astype(str)
        symbol_observed_quality_df["observed_quality_band"] = (
            symbol_observed_quality_df["observed_quality_band"].astype(str)
        )
        symbol_observed_quality_df["observed_vs_max_pct"] = pd.to_numeric(
            symbol_observed_quality_df["observed_vs_max_pct"], errors="coerce"
        )
        symbol_observed_quality_df["observed_quality_band_order"] = pd.to_numeric(
            symbol_observed_quality_df["observed_quality_band_order"], errors="coerce"
        )

    with st.sidebar:
        selected_observed_quality_bands = st.multiselect(
            "Observed Coverage Quality Bands",
            options=list(OBSERVED_QUALITY_BANDS),
            default=list(OBSERVED_QUALITY_BANDS),
            help=(
                "Per symbol, compare observed points of each exchange against "
                "the maximum observed points across exchanges in the selected window."
            ),
        )

    if symbol_observed_quality_df.empty:
        filtered_symbol_observed_quality_df = symbol_observed_quality_df.copy()
    else:
        filtered_symbol_observed_quality_df = symbol_observed_quality_df[
            symbol_observed_quality_df["observed_quality_band"].isin(selected_observed_quality_bands)
        ].copy()

    run_symbols_df = _query_dataframe(
        db_path,
        SQL_SYMBOLS_BY_RUN,
        params={"run_id": selected_run_id},
    )
    run_symbol_count = int(len(run_symbols_df))

    symbols_df = _query_dataframe(
        db_path,
        SQL_SYMBOLS_BY_RUN_WINDOW,
        params={
            "run_id": selected_run_id,
            "window_start_utc": window_start_utc,
            "window_end_utc": window_end_utc,
        },
    )
    if symbols_df.empty:
        symbols_df = run_symbols_df

    symbols = symbols_df["symbol"].astype(str).tolist()
    if not symbol_observed_quality_df.empty:
        observed_symbol_set = set(
            filtered_symbol_observed_quality_df["symbol"].astype(str).unique().tolist()
        )
        symbols = [symbol_value for symbol_value in symbols if symbol_value in observed_symbol_set]

    if not symbols:
        st.warning(
            "No symbols found for selected run/window and observed quality category filter."
        )
        st.stop()

    symbol_from_query = _get_symbol_query_param()
    selected_symbol_index = symbols.index(symbol_from_query) if symbol_from_query in symbols else 0
    with st.sidebar:
        selected_symbol = st.selectbox("Symbol", options=symbols, index=selected_symbol_index)

    if selected_symbol != symbol_from_query:
        _set_symbol_query_param(selected_symbol)

    selected_symbol_observed_quality_df = pd.DataFrame()
    observed_quality_allowed_exchanges: set[str] = set()
    if not filtered_symbol_observed_quality_df.empty:
        selected_symbol_observed_quality_df = filtered_symbol_observed_quality_df[
            filtered_symbol_observed_quality_df["symbol"] == selected_symbol
        ].copy()
        if not selected_symbol_observed_quality_df.empty:
            selected_symbol_observed_quality_df = selected_symbol_observed_quality_df.sort_values(
                by=["observed_vs_max_pct", "exchange_id"],
                ascending=[False, True],
            ).reset_index(drop=True)
            observed_quality_allowed_exchanges = set(
                selected_symbol_observed_quality_df["exchange_id"].astype(str).tolist()
            )

    with st.sidebar:
        if not symbol_observed_quality_df.empty:
            st.caption(
                "Observed quality rows (active bands): "
                f"{len(filtered_symbol_observed_quality_df)}"
            )
            st.caption(
                "Observed quality symbols (active bands): "
                f"{int(filtered_symbol_observed_quality_df['symbol'].nunique())}"
            )
            st.caption(
                f"Selected symbol exchanges (active bands): {len(observed_quality_allowed_exchanges)}"
            )

    if has_platform_quality_cache:
        platform_quality_df = _query_dataframe(db_path, SQL_PLATFORM_QUALITY_DAILY_CACHE)
    elif has_quality_view:
        platform_quality_df = _query_dataframe(db_path, SQL_PLATFORM_QUALITY_DAILY_VIEW)
    else:
        platform_quality_df = pd.DataFrame()

    query_params = {
        "run_id": selected_run_id,
        "symbol": selected_symbol,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
    }
    price_curve_df = _query_dataframe(db_path, SQL_PRICE_CURVE_WINDOW, params=query_params)
    if observed_quality_allowed_exchanges:
        price_curve_df = price_curve_df[
            price_curve_df["exchange_id"].astype(str).isin(observed_quality_allowed_exchanges)
        ].copy()
    elif not symbol_observed_quality_df.empty:
        price_curve_df = price_curve_df.iloc[0:0].copy()

    price_deviation_window_df = _build_deviation_from_curve(price_curve_df)

    symbol_violin_params = {
        "run_id": selected_run_id,
        "window_start_utc": window_start_utc,
        "window_end_utc": window_end_utc,
    }
    if has_symbol_deviation_cache:
        symbol_violin_df = _query_dataframe(
            db_path,
            SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW_CACHE,
            params=symbol_violin_params,
        )
    elif has_symbol_deviation_view:
        symbol_violin_df = _query_dataframe(
            db_path,
            SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW_VIEW,
            params=symbol_violin_params,
        )
    else:
        symbol_violin_df = _query_dataframe(
            db_path,
            SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW_RAW,
            params=symbol_violin_params,
        )
    if not symbol_observed_quality_df.empty:
        observed_symbol_set = set(
            filtered_symbol_observed_quality_df["symbol"].astype(str).unique().tolist()
        )
        symbol_violin_df = symbol_violin_df[
            symbol_violin_df["symbol"].astype(str).isin(observed_symbol_set)
        ].copy()

    st.subheader("Symbol Start Page: Price Deviation Violin Grid")
    st.caption(
        "Each violin shows the distribution of bucket-level percentage close-price deviation across exchanges "
        "for the selected run and UTC window."
    )
    if symbol_violin_df.empty:
        st.info("No aligned multi-exchange points available for violin plots in the selected run/window.")
    else:
        symbol_stats_df = (
            symbol_violin_df.groupby("symbol", as_index=False)
            .agg(
                point_count=("price_diff_pct", "size"),
                avg_price_diff_pct=("price_diff_pct", "mean"),
                max_price_diff_pct=("price_diff_pct", "max"),
            )
            .sort_values(by=["max_price_diff_pct", "symbol"], ascending=[False, True])
        )
        symbol_stats_df = symbol_stats_df.reset_index(drop=True)
        symbol_stats_df["rank_max_diff_desc"] = symbol_stats_df.index + 1

        page_size_options = [6, 12, 24, 48, 96, "Alle"]
        total_symbols = len(symbol_stats_df)
        control_col_1, control_col_2, _ = st.columns([1.8, 1.0, 7.2], gap="small")
        with control_col_1:
            st.caption("Symbols per page")
            page_size_label = st.selectbox(
                "Symbols per page",
                options=page_size_options,
                index=0,
                label_visibility="collapsed",
            )

        if page_size_label == "Alle":
            page_count = 1
            current_page_default = 1
        else:
            page_size_value = int(page_size_label)
            page_count = max(1, (total_symbols + page_size_value - 1) // page_size_value)
            current_page_default = 1

        with control_col_2:
            st.caption("Page")
            current_page = st.number_input(
                "Page",
                min_value=1,
                max_value=page_count,
                value=current_page_default,
                step=1,
                disabled=(page_size_label == "Alle"),
                label_visibility="collapsed",
            )

        if page_size_label == "Alle":
            paged_symbols_df = symbol_stats_df
            current_page = 1
        else:
            start_idx = (int(current_page) - 1) * page_size_value
            end_idx = start_idx + page_size_value
            paged_symbols_df = symbol_stats_df.iloc[start_idx:end_idx].copy()

        if not paged_symbols_df.empty:
            first_symbol_on_page = str(paged_symbols_df.iloc[0]["symbol"])
            page_signature = "|".join(
                [
                    str(selected_run_id),
                    window_start_utc,
                    window_end_utc,
                    str(page_size_label),
                    str(int(current_page)),
                ]
            )
            last_page_signature = str(st.session_state.get(PAGINATION_SIGNATURE_STATE_KEY, ""))
            if page_signature != last_page_signature:
                st.session_state[PAGINATION_SIGNATURE_STATE_KEY] = page_signature
                if selected_symbol != first_symbol_on_page:
                    _set_symbol_query_param(first_symbol_on_page)
                    st.rerun()

        st.caption(
            f"Sorted by max deviation descending | Page {int(current_page)}/{int(page_count)} | "
            f"Visible symbols: {len(paged_symbols_df)}"
        )

        columns_per_row = 3
        grid_columns = st.columns(columns_per_row)
        for idx, symbol in enumerate(paged_symbols_df["symbol"].astype(str).tolist()):
            with grid_columns[idx % columns_per_row]:
                symbol_points_df = symbol_violin_df[symbol_violin_df["symbol"] == symbol]
                stat_row = paged_symbols_df[paged_symbols_df["symbol"] == symbol].iloc[0]
                st.markdown(
                    f"**#{int(stat_row['rank_max_diff_desc'])} {symbol}**"
                )
                violin_figure = px.violin(
                    symbol_points_df,
                    y="price_diff_pct",
                    points=False,
                    box=True,
                )
                violin_figure.update_layout(
                    template=plotly_template,
                    height=220,
                    margin={"l": 20, "r": 20, "t": 5, "b": 10},
                    showlegend=False,
                    xaxis_title=None,
                    yaxis_title="Diff %",
                )
                st.plotly_chart(
                    violin_figure,
                    width="stretch",
                    config=PLOTLY_CHART_CONFIG,
                    key=f"violin_{selected_run_id}_{symbol}_{idx}",
                )
                st.caption(
                    f"n={int(stat_row['point_count'])} | "
                    f"avg={float(stat_row['avg_price_diff_pct']):.4f}% | "
                    f"max={float(stat_row['max_price_diff_pct']):.4f}%"
                )
                if st.button(f"Open {symbol}", key=f"open_symbol_{symbol}"):
                    _set_symbol_query_param(symbol)
                    st.rerun()
        with st.expander("Symbol Deviation Stats (Table)"):
            st.dataframe(
                symbol_stats_df[
                    [
                        "rank_max_diff_desc",
                        "symbol",
                        "point_count",
                        "avg_price_diff_pct",
                        "max_price_diff_pct",
                    ]
                ],
                width="stretch",
                hide_index=True,
            )

    st.subheader("Snapshot")
    metric_col_1, metric_col_2, metric_col_3, metric_col_4, metric_col_5 = st.columns(5)

    latest_kpi_date = (
        str(platform_quality_df["kpi_date_utc"].iloc[0]) if not platform_quality_df.empty else "n/a"
    )
    curve_points = len(price_curve_df)
    curve_exchanges = int(price_curve_df["exchange_id"].nunique()) if not price_curve_df.empty else 0
    deviation_points = len(price_deviation_window_df)

    metric_col_1.metric("Selected Symbol", selected_symbol)
    metric_col_2.metric("Selected Run", selected_run_id)
    metric_col_3.metric("Run Symbols", run_symbol_count)
    metric_col_4.metric("Curve Exchanges", curve_exchanges)
    metric_col_5.metric("Aligned Diff Points", deviation_points)
    st.caption(
        f"Window UTC: {window_start_utc} -> {window_end_utc} | "
        f"Curve points: {curve_points} | Platform KPI date: {latest_kpi_date} | "
        f"Observed bands: {', '.join(selected_observed_quality_bands) if selected_observed_quality_bands else 'none'}"
    )

    tab_curve, tab_deviation, tab_quality = st.tabs(
        ["Price Curve (Window)", "Price Deviation (Window)", "Platform Quality"]
    )

    with tab_curve:
        st.write("Price development for selected symbol in selected UTC window.")
        if price_curve_df.empty:
            st.warning("No curve points available in selected run/window for this symbol.")
        else:
            curve_table_df = price_curve_df[
                [
                    "run_id",
                    "exchange_id",
                    "symbol",
                    "bucket_start_utc",
                    "bucket_epoch_s",
                    "price_close",
                    "fill_method",
                ]
            ].copy()
            curve_table_df["exchange_id"] = curve_table_df["exchange_id"].astype(str)
            curve_table_df["fill_method"] = curve_table_df["fill_method"].fillna("n/a").astype(str)
            curve_table_df["bucket_epoch_s"] = pd.to_numeric(
                curve_table_df["bucket_epoch_s"], errors="coerce"
            )
            curve_table_df["price_close"] = pd.to_numeric(
                curve_table_df["price_close"], errors="coerce"
            )
            curve_table_df = curve_table_df.dropna(subset=["bucket_epoch_s", "price_close"])
            curve_table_df["bucket_epoch_s"] = curve_table_df["bucket_epoch_s"].astype(int)

            all_exchange_values = sorted(curve_table_df["exchange_id"].unique().tolist())
            all_fill_method_values = sorted(curve_table_df["fill_method"].unique().tolist())

            curve_filter_keys = {
                "exchange_values": f"curve_filter_exchange_values_{selected_run_id}_{selected_symbol}",
                "exchange_text": f"curve_filter_exchange_text_{selected_run_id}_{selected_symbol}",
                "fill_values": f"curve_filter_fill_values_{selected_run_id}_{selected_symbol}",
                "fill_text": f"curve_filter_fill_text_{selected_run_id}_{selected_symbol}",
                "epoch_expr": f"curve_filter_epoch_expr_{selected_run_id}_{selected_symbol}",
                "price_expr": f"curve_filter_price_expr_{selected_run_id}_{selected_symbol}",
            }

            if st.button(
                "Reset Filters",
                key=f"curve_filter_reset_{selected_run_id}_{selected_symbol}",
            ):
                for state_key in curve_filter_keys.values():
                    st.session_state.pop(state_key, None)
                st.rerun()

            st.caption(
                "Column-style filters (Excel-like): graph and table use the same filtered rows."
            )
            filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
            with filter_col_1:
                selected_exchange_values = st.multiselect(
                    "exchange_id values",
                    options=all_exchange_values,
                    default=all_exchange_values,
                    key=curve_filter_keys["exchange_values"],
                )
                exchange_filter_text = st.text_input(
                    "exchange_id contains (comma OR)",
                    value="",
                    key=curve_filter_keys["exchange_text"],
                )
            with filter_col_2:
                selected_fill_method_values = st.multiselect(
                    "fill_method values",
                    options=all_fill_method_values,
                    default=all_fill_method_values,
                    key=curve_filter_keys["fill_values"],
                )
                fill_method_filter_text = st.text_input(
                    "fill_method contains (comma OR)",
                    value="",
                    key=curve_filter_keys["fill_text"],
                )
            with filter_col_3:
                bucket_epoch_expr = st.text_input(
                    "bucket_epoch_s criteria (comma OR)",
                    value="",
                    placeholder="e.g. 1700000000-1700003600,>=1700010000,<1700020000",
                    key=curve_filter_keys["epoch_expr"],
                )
            with filter_col_4:
                price_close_expr = st.text_input(
                    "price_close criteria (comma OR)",
                    value="",
                    placeholder="e.g. 42000-43000,>=44000,<41000",
                    key=curve_filter_keys["price_expr"],
                )

            filtered_curve_df = curve_table_df.copy()
            if selected_exchange_values:
                filtered_curve_df = filtered_curve_df[
                    filtered_curve_df["exchange_id"].isin(selected_exchange_values)
                ]
            else:
                filtered_curve_df = filtered_curve_df.iloc[0:0]

            exchange_terms = _parse_contains_terms(exchange_filter_text)
            filtered_curve_df = _apply_contains_filter(
                filtered_curve_df,
                "exchange_id",
                exchange_terms,
            )

            if selected_fill_method_values:
                filtered_curve_df = filtered_curve_df[
                    filtered_curve_df["fill_method"].isin(selected_fill_method_values)
                ]
            else:
                filtered_curve_df = filtered_curve_df.iloc[0:0]

            fill_terms = _parse_contains_terms(fill_method_filter_text)
            filtered_curve_df = _apply_contains_filter(
                filtered_curve_df,
                "fill_method",
                fill_terms,
            )

            epoch_criteria, invalid_epoch_tokens = _parse_numeric_criteria(bucket_epoch_expr)
            if invalid_epoch_tokens:
                st.warning(
                    "Invalid bucket_epoch_s criteria ignored: "
                    + ", ".join(invalid_epoch_tokens)
                )
            filtered_curve_df = _apply_numeric_criteria_filter(
                filtered_curve_df,
                "bucket_epoch_s",
                epoch_criteria,
            )

            price_criteria, invalid_price_tokens = _parse_numeric_criteria(price_close_expr)
            if invalid_price_tokens:
                st.warning(
                    "Invalid price_close criteria ignored: "
                    + ", ".join(invalid_price_tokens)
                )
            filtered_curve_df = _apply_numeric_criteria_filter(
                filtered_curve_df,
                "price_close",
                price_criteria,
            )

            if filtered_curve_df.empty:
                st.warning("No rows match the active column filters.")
            else:
                price_curve_plot = filtered_curve_df.copy()
                price_curve_plot["bucket_start_utc"] = pd.to_datetime(
                    price_curve_plot["bucket_start_utc"],
                    utc=True,
                    errors="coerce",
                )
                price_curve_plot = price_curve_plot.dropna(subset=["bucket_start_utc"])
                price_curve_plot["exchange_id"] = price_curve_plot["exchange_id"].astype(str)
                visible_exchange_count = int(price_curve_plot["exchange_id"].nunique())
                st.caption(
                    f"Visible exchanges after filters: {visible_exchange_count} | "
                    f"Filtered rows: {len(filtered_curve_df)}"
                )
                curve_figure = px.line(
                    price_curve_plot,
                    x="bucket_start_utc",
                    y="price_close",
                    color="exchange_id",
                    template=plotly_template,
                )
                curve_figure.update_layout(
                    height=360,
                    margin={"l": 20, "r": 20, "t": 20, "b": 10},
                    xaxis_title="UTC Timestamp",
                    yaxis_title="Close Price",
                    legend_title="Exchange",
                )
                st.plotly_chart(
                    curve_figure,
                    width="stretch",
                    config=PLOTLY_CHART_CONFIG,
                    key=f"curve_{selected_run_id}_{selected_symbol}",
                )
                st.dataframe(
                    filtered_curve_df,
                    width="stretch",
                    hide_index=True,
                )
                with st.expander("Exchange Coverage (Filtered)"):
                    filtered_exchange_point_counts = (
                        filtered_curve_df.groupby("exchange_id", as_index=False)
                        .size()
                        .rename(columns={"size": "point_count"})
                        .sort_values(by=["point_count", "exchange_id"], ascending=[False, True])
                    )
                    st.dataframe(filtered_exchange_point_counts, width="stretch", hide_index=True)
                with st.expander("Exchange Coverage (Unfiltered Window)"):
                    exchange_point_counts = (
                        curve_table_df.groupby("exchange_id", as_index=False)
                        .size()
                        .rename(columns={"size": "point_count"})
                        .sort_values(by=["point_count", "exchange_id"], ascending=[False, True])
                    )
                    st.dataframe(exchange_point_counts, width="stretch", hide_index=True)

    with tab_deviation:
        st.write("Price deviation across exchanges for selected symbol and selected UTC window.")
        if price_deviation_window_df.empty:
            st.warning("No aligned deviation points available (need at least two exchanges per timestamp).")
        else:
            deviation_table_df = price_deviation_window_df.copy()
            deviation_table_df["max_diff_exchange_pair"] = (
                deviation_table_df["max_diff_exchange_pair"].fillna("n/a").astype(str)
            )
            deviation_table_df["bucket_start_utc"] = deviation_table_df["bucket_start_utc"].astype(str)

            numeric_deviation_columns = [
                "bucket_epoch_s",
                "exchange_count",
                "max_price",
                "min_price",
                "price_diff_abs",
                "price_diff_pct",
            ]
            for numeric_column in numeric_deviation_columns:
                deviation_table_df[numeric_column] = pd.to_numeric(
                    deviation_table_df[numeric_column], errors="coerce"
                )
            deviation_table_df = deviation_table_df.dropna(subset=numeric_deviation_columns)
            deviation_table_df["bucket_epoch_s"] = deviation_table_df["bucket_epoch_s"].astype(int)
            deviation_table_df["exchange_count"] = deviation_table_df["exchange_count"].astype(int)

            all_pair_values = sorted(deviation_table_df["max_diff_exchange_pair"].unique().tolist())
            deviation_filter_keys = {
                "pair_values": f"deviation_filter_pair_values_{selected_run_id}_{selected_symbol}",
                "pair_text": f"deviation_filter_pair_text_{selected_run_id}_{selected_symbol}",
                "bucket_start_text": f"deviation_filter_bucket_start_text_{selected_run_id}_{selected_symbol}",
                "bucket_epoch_expr": f"deviation_filter_bucket_epoch_expr_{selected_run_id}_{selected_symbol}",
                "exchange_count_expr": f"deviation_filter_exchange_count_expr_{selected_run_id}_{selected_symbol}",
                "max_price_expr": f"deviation_filter_max_price_expr_{selected_run_id}_{selected_symbol}",
                "min_price_expr": f"deviation_filter_min_price_expr_{selected_run_id}_{selected_symbol}",
                "price_diff_abs_expr": f"deviation_filter_price_diff_abs_expr_{selected_run_id}_{selected_symbol}",
                "price_diff_pct_expr": f"deviation_filter_price_diff_pct_expr_{selected_run_id}_{selected_symbol}",
            }

            if st.button(
                "Reset Deviation Filters",
                key=f"deviation_filter_reset_{selected_run_id}_{selected_symbol}",
            ):
                for state_key in deviation_filter_keys.values():
                    st.session_state.pop(state_key, None)
                st.rerun()

            st.caption(
                "Column-style filters (Excel-like): graphs and table use the same filtered rows."
            )
            dev_filter_col_1, dev_filter_col_2, dev_filter_col_3 = st.columns(3)
            with dev_filter_col_1:
                selected_pair_values = st.multiselect(
                    "max_diff_exchange_pair values",
                    options=all_pair_values,
                    default=all_pair_values,
                    key=deviation_filter_keys["pair_values"],
                )
                pair_filter_text = st.text_input(
                    "max_diff_exchange_pair contains (comma OR)",
                    value="",
                    key=deviation_filter_keys["pair_text"],
                )
            with dev_filter_col_2:
                bucket_start_filter_text = st.text_input(
                    "bucket_start_utc contains (comma OR)",
                    value="",
                    key=deviation_filter_keys["bucket_start_text"],
                )
                bucket_epoch_expr = st.text_input(
                    "bucket_epoch_s criteria (comma OR)",
                    value="",
                    placeholder="e.g. 1700000000-1700003600,>=1700010000,<1700020000",
                    key=deviation_filter_keys["bucket_epoch_expr"],
                )
            with dev_filter_col_3:
                exchange_count_expr = st.text_input(
                    "exchange_count criteria (comma OR)",
                    value="",
                    placeholder="e.g. 2,>=3,2-5",
                    key=deviation_filter_keys["exchange_count_expr"],
                )

            dev_num_col_1, dev_num_col_2, dev_num_col_3, dev_num_col_4 = st.columns(4)
            with dev_num_col_1:
                max_price_expr = st.text_input(
                    "max_price criteria (comma OR)",
                    value="",
                    placeholder="e.g. >=42000,42000-43000",
                    key=deviation_filter_keys["max_price_expr"],
                )
            with dev_num_col_2:
                min_price_expr = st.text_input(
                    "min_price criteria (comma OR)",
                    value="",
                    placeholder="e.g. >=41000,41000-42500",
                    key=deviation_filter_keys["min_price_expr"],
                )
            with dev_num_col_3:
                price_diff_abs_expr = st.text_input(
                    "price_diff_abs criteria (comma OR)",
                    value="",
                    placeholder="e.g. >=50,10-200",
                    key=deviation_filter_keys["price_diff_abs_expr"],
                )
            with dev_num_col_4:
                price_diff_pct_expr = st.text_input(
                    "price_diff_pct criteria (comma OR)",
                    value="",
                    placeholder="e.g. >=0.1,0.05-1.0",
                    key=deviation_filter_keys["price_diff_pct_expr"],
                )

            filtered_deviation_df = deviation_table_df.copy()
            if selected_pair_values:
                filtered_deviation_df = filtered_deviation_df[
                    filtered_deviation_df["max_diff_exchange_pair"].isin(selected_pair_values)
                ]
            else:
                filtered_deviation_df = filtered_deviation_df.iloc[0:0]

            pair_terms = _parse_contains_terms(pair_filter_text)
            filtered_deviation_df = _apply_contains_filter(
                filtered_deviation_df,
                "max_diff_exchange_pair",
                pair_terms,
            )

            bucket_start_terms = _parse_contains_terms(bucket_start_filter_text)
            filtered_deviation_df = _apply_contains_filter(
                filtered_deviation_df,
                "bucket_start_utc",
                bucket_start_terms,
            )

            column_criteria_map = [
                ("bucket_epoch_s", bucket_epoch_expr),
                ("exchange_count", exchange_count_expr),
                ("max_price", max_price_expr),
                ("min_price", min_price_expr),
                ("price_diff_abs", price_diff_abs_expr),
                ("price_diff_pct", price_diff_pct_expr),
            ]
            for column_name, expression in column_criteria_map:
                criteria, invalid_tokens = _parse_numeric_criteria(expression)
                if invalid_tokens:
                    st.warning(
                        f"Invalid {column_name} criteria ignored: " + ", ".join(invalid_tokens)
                    )
                filtered_deviation_df = _apply_numeric_criteria_filter(
                    filtered_deviation_df,
                    column_name,
                    criteria,
                )

            if filtered_deviation_df.empty:
                st.warning("No rows match the active deviation filters.")
                st.dataframe(filtered_deviation_df, width="stretch", hide_index=True)
            else:
                row_max = filtered_deviation_df.sort_values(
                    by=["price_diff_abs", "bucket_epoch_s"],
                    ascending=[False, True],
                ).iloc[0]
                dev_col_1, dev_col_2, dev_col_3, dev_col_4 = st.columns(4)
                dev_col_1.metric(
                    "Max Diff (%)",
                    _format_float(filtered_deviation_df["price_diff_pct"].max(), decimals=4),
                )
                dev_col_2.metric(
                    "Max Diff (Abs)",
                    _format_float(filtered_deviation_df["price_diff_abs"].max(), decimals=8),
                )
                dev_col_3.metric(
                    "Avg Diff (%)",
                    _format_float(filtered_deviation_df["price_diff_pct"].mean(), decimals=4),
                )
                dev_col_4.metric("Compared Points", int(len(filtered_deviation_df)))
                st.write(f"Max spread exchange pair: `{row_max['max_diff_exchange_pair']}`")
                st.write(f"Max spread timestamp (UTC): `{row_max['bucket_start_utc']}`")

                deviation_plot = filtered_deviation_df.copy()
                deviation_plot["bucket_start_utc"] = pd.to_datetime(
                    deviation_plot["bucket_start_utc"],
                    utc=True,
                    errors="coerce",
                )
                deviation_plot = deviation_plot.dropna(subset=["bucket_start_utc"])
                deviation_dual_figure = make_subplots(specs=[[{"secondary_y": True}]])
                deviation_dual_figure.add_trace(
                    go.Scatter(
                        x=deviation_plot["bucket_start_utc"],
                        y=deviation_plot["price_diff_abs"],
                        mode="lines",
                        name="Price Diff (Abs)",
                    ),
                    secondary_y=False,
                )
                deviation_dual_figure.add_trace(
                    go.Scatter(
                        x=deviation_plot["bucket_start_utc"],
                        y=deviation_plot["price_diff_pct"],
                        mode="lines",
                        name="Price Diff (%)",
                    ),
                    secondary_y=True,
                )
                deviation_dual_figure.update_layout(
                    template=plotly_template,
                    height=360,
                    margin={"l": 20, "r": 20, "t": 20, "b": 10},
                    xaxis_title="UTC Timestamp",
                    legend_title="Metric",
                )
                deviation_dual_figure.update_yaxes(
                    title_text="Price Diff (Abs)",
                    secondary_y=False,
                )
                deviation_dual_figure.update_yaxes(
                    title_text="Price Diff (%)",
                    secondary_y=True,
                )
                st.plotly_chart(
                    deviation_dual_figure,
                    width="stretch",
                    config=PLOTLY_CHART_CONFIG,
                    key=f"deviation_dual_{selected_run_id}_{selected_symbol}",
                )
                st.dataframe(filtered_deviation_df, width="stretch", hide_index=True)

    with tab_quality:
        st.write("Daily platform quality snapshot (all exchanges).")
        st.info(
            "Ranking logic (best quality first): default_quality_score = weighted score with latency caps "
            "(min 1000 ms, avg 10000 ms, max 600000 ms). "
            "Weights: avg latency 35%, max latency 30%, min latency 15%, update frequency 10%, "
            "disconnect count 8%, symbols covered 2%. "
            "default_quality_rank = 1 means best score."
        )
        st.write(
            "Observed coverage quality (selected symbol): observed points per exchange "
            "vs maximum observed points of that symbol in the selected run/window."
        )
        if symbol_observed_quality_df.empty:
            st.warning("No observed quality rows available for selected run/window.")
        elif selected_symbol_observed_quality_df.empty:
            st.warning(
                "No exchange of selected symbol matches the active observed quality categories."
            )
        else:
            observed_quality_display_df = selected_symbol_observed_quality_df[
                [
                    "symbol",
                    "exchange_id",
                    "total_points",
                    "observed_points",
                    "max_observed_points_for_symbol",
                    "observed_vs_max_pct",
                    "observed_quality_band",
                ]
            ].copy()
            for numeric_column in [
                "total_points",
                "observed_points",
                "max_observed_points_for_symbol",
                "observed_vs_max_pct",
            ]:
                observed_quality_display_df[numeric_column] = pd.to_numeric(
                    observed_quality_display_df[numeric_column], errors="coerce"
                )

            oq_col_1, oq_col_2, oq_col_3, oq_col_4 = st.columns(4)
            oq_col_1.metric("Exchanges (Symbol)", int(len(observed_quality_display_df)))
            oq_col_2.metric(
                "Max Observed vs Max (%)",
                _format_float(observed_quality_display_df["observed_vs_max_pct"].max(), decimals=2),
            )
            oq_col_3.metric(
                "Median Observed vs Max (%)",
                _format_float(observed_quality_display_df["observed_vs_max_pct"].median(), decimals=2),
            )
            oq_col_4.metric(
                "Min Observed vs Max (%)",
                _format_float(observed_quality_display_df["observed_vs_max_pct"].min(), decimals=2),
            )

            observed_quality_bar_df = observed_quality_display_df.sort_values(
                by=["observed_vs_max_pct", "exchange_id"], ascending=[False, True]
            )
            observed_quality_figure = px.bar(
                observed_quality_bar_df,
                x="exchange_id",
                y="observed_vs_max_pct",
                color="observed_quality_band",
                category_orders={"observed_quality_band": list(OBSERVED_QUALITY_BANDS)},
                template=plotly_template,
            )
            observed_quality_figure.update_layout(
                height=320,
                margin={"l": 20, "r": 20, "t": 20, "b": 10},
                xaxis_title="Exchange",
                yaxis_title="Observed vs Symbol Max (%)",
                legend_title="Observed Quality Band",
            )
            observed_quality_figure.update_yaxes(range=[0, 100])
            st.plotly_chart(
                observed_quality_figure,
                width="stretch",
                config=PLOTLY_CHART_CONFIG,
                key=f"quality_observed_band_{selected_run_id}_{selected_symbol}",
            )
            st.dataframe(observed_quality_display_df, width="stretch", hide_index=True)

        if platform_quality_df.empty:
            st.warning(
                "No platform quality data available. "
                "Run mart views and cache build to populate the quality snapshot."
            )
        else:
            quality_display = platform_quality_df[
                [
                    "exchange_id",
                    "default_quality_score",
                    "default_quality_rank",
                    "symbols_covered",
                    "avg_latency_ms",
                    "min_latency_ms",
                    "max_latency_ms",
                    "update_frequency_hz",
                    "disconnect_count",
                ]
            ].copy()
            quality_display["exchange_id"] = quality_display["exchange_id"].astype(str)
            numeric_quality_columns = [
                "default_quality_score",
                "default_quality_rank",
                "symbols_covered",
                "avg_latency_ms",
                "min_latency_ms",
                "max_latency_ms",
                "update_frequency_hz",
                "disconnect_count",
            ]
            for numeric_column in numeric_quality_columns:
                quality_display[numeric_column] = pd.to_numeric(
                    quality_display[numeric_column], errors="coerce"
                )
            quality_display = quality_display.dropna(subset=numeric_quality_columns)
            quality_display["default_quality_rank"] = quality_display["default_quality_rank"].astype(int)
            quality_display["symbols_covered"] = quality_display["symbols_covered"].astype(int)
            quality_display["disconnect_count"] = quality_display["disconnect_count"].astype(int)

            all_quality_exchange_values = sorted(quality_display["exchange_id"].unique().tolist())
            quality_filter_keys = {
                "exchange_values": f"quality_filter_exchange_values_{selected_run_id}_{selected_symbol}",
                "exchange_text": f"quality_filter_exchange_text_{selected_run_id}_{selected_symbol}",
                "score_expr": f"quality_filter_score_expr_{selected_run_id}_{selected_symbol}",
                "rank_expr": f"quality_filter_rank_expr_{selected_run_id}_{selected_symbol}",
                "symbols_expr": f"quality_filter_symbols_expr_{selected_run_id}_{selected_symbol}",
                "avg_latency_expr": f"quality_filter_avg_latency_expr_{selected_run_id}_{selected_symbol}",
                "min_latency_expr": f"quality_filter_min_latency_expr_{selected_run_id}_{selected_symbol}",
                "max_latency_expr": f"quality_filter_max_latency_expr_{selected_run_id}_{selected_symbol}",
                "update_hz_expr": f"quality_filter_update_hz_expr_{selected_run_id}_{selected_symbol}",
                "disconnect_expr": f"quality_filter_disconnect_expr_{selected_run_id}_{selected_symbol}",
            }

            if st.button(
                "Reset Quality Filters",
                key=f"quality_filter_reset_{selected_run_id}_{selected_symbol}",
            ):
                for state_key in quality_filter_keys.values():
                    st.session_state.pop(state_key, None)
                st.rerun()

            st.caption(
                "Column-style filters (Excel-like): charts and table use the same filtered rows."
            )
            quality_filter_col_1, quality_filter_col_2, quality_filter_col_3 = st.columns(3)
            with quality_filter_col_1:
                selected_quality_exchange_values = st.multiselect(
                    "exchange_id values",
                    options=all_quality_exchange_values,
                    default=all_quality_exchange_values,
                    key=quality_filter_keys["exchange_values"],
                )
                quality_exchange_filter_text = st.text_input(
                    "exchange_id contains (comma OR)",
                    value="",
                    key=quality_filter_keys["exchange_text"],
                )
            with quality_filter_col_2:
                quality_rank_expr = st.text_input(
                    "default_quality_rank criteria (comma OR)",
                    value="",
                    placeholder="e.g. 1,<=3,2-5",
                    key=quality_filter_keys["rank_expr"],
                )
                quality_score_expr = st.text_input(
                    "default_quality_score criteria (comma OR)",
                    value="",
                    placeholder="e.g. >=70,60-90",
                    key=quality_filter_keys["score_expr"],
                )
            with quality_filter_col_3:
                symbols_covered_expr = st.text_input(
                    "symbols_covered criteria (comma OR)",
                    value="",
                    placeholder="e.g. >=10,5-25",
                    key=quality_filter_keys["symbols_expr"],
                )
                disconnect_count_expr = st.text_input(
                    "disconnect_count criteria (comma OR)",
                    value="",
                    placeholder="e.g. 0,>=1,0-3",
                    key=quality_filter_keys["disconnect_expr"],
                )

            quality_filter_num_col_1, quality_filter_num_col_2, quality_filter_num_col_3, quality_filter_num_col_4 = st.columns(4)
            with quality_filter_num_col_1:
                avg_latency_expr = st.text_input(
                    "avg_latency_ms criteria (comma OR)",
                    value="",
                    placeholder="e.g. <5000,1000-8000",
                    key=quality_filter_keys["avg_latency_expr"],
                )
            with quality_filter_num_col_2:
                min_latency_expr = st.text_input(
                    "min_latency_ms criteria (comma OR)",
                    value="",
                    placeholder="e.g. <1000,100-2000",
                    key=quality_filter_keys["min_latency_expr"],
                )
            with quality_filter_num_col_3:
                max_latency_expr = st.text_input(
                    "max_latency_ms criteria (comma OR)",
                    value="",
                    placeholder="e.g. <60000,5000-120000",
                    key=quality_filter_keys["max_latency_expr"],
                )
            with quality_filter_num_col_4:
                update_frequency_expr = st.text_input(
                    "update_frequency_hz criteria (comma OR)",
                    value="",
                    placeholder="e.g. >0.1,0.05-1.0",
                    key=quality_filter_keys["update_hz_expr"],
                )

            filtered_quality_df = quality_display.copy()
            if selected_quality_exchange_values:
                filtered_quality_df = filtered_quality_df[
                    filtered_quality_df["exchange_id"].isin(selected_quality_exchange_values)
                ]
            else:
                filtered_quality_df = filtered_quality_df.iloc[0:0]

            quality_exchange_terms = _parse_contains_terms(quality_exchange_filter_text)
            filtered_quality_df = _apply_contains_filter(
                filtered_quality_df,
                "exchange_id",
                quality_exchange_terms,
            )

            quality_criteria_map = [
                ("default_quality_rank", quality_rank_expr),
                ("default_quality_score", quality_score_expr),
                ("symbols_covered", symbols_covered_expr),
                ("disconnect_count", disconnect_count_expr),
                ("avg_latency_ms", avg_latency_expr),
                ("min_latency_ms", min_latency_expr),
                ("max_latency_ms", max_latency_expr),
                ("update_frequency_hz", update_frequency_expr),
            ]
            for column_name, expression in quality_criteria_map:
                criteria, invalid_tokens = _parse_numeric_criteria(expression)
                if invalid_tokens:
                    st.warning(
                        f"Invalid {column_name} criteria ignored: " + ", ".join(invalid_tokens)
                    )
                filtered_quality_df = _apply_numeric_criteria_filter(
                    filtered_quality_df,
                    column_name,
                    criteria,
                )

            if filtered_quality_df.empty:
                st.warning("No rows match the active platform quality filters.")
                st.dataframe(filtered_quality_df, width="stretch", hide_index=True)
            else:
                quality_by_disconnect = filtered_quality_df.sort_values(
                    by=["disconnect_count", "exchange_id"],
                    ascending=[False, True],
                )
                st.caption(
                    f"Visible exchanges after filters: {int(filtered_quality_df['exchange_id'].nunique())} | "
                    f"Filtered rows: {len(filtered_quality_df)}"
                )
                st.dataframe(filtered_quality_df, width="stretch", hide_index=True)
                st.write("Disconnects by exchange (sorted descending).")
                disconnect_figure = px.bar(
                    quality_by_disconnect,
                    x="exchange_id",
                    y="disconnect_count",
                    template=plotly_template,
                )
                disconnect_figure.update_layout(
                    height=320,
                    margin={"l": 20, "r": 20, "t": 20, "b": 10},
                    xaxis_title="Exchange",
                    yaxis_title="Disconnect Count",
                    showlegend=False,
                )
                st.plotly_chart(
                    disconnect_figure,
                    width="stretch",
                    config=PLOTLY_CHART_CONFIG,
                    key="quality_disconnect_bar",
                )
                latency_long_df = filtered_quality_df.melt(
                    id_vars=["exchange_id"],
                    value_vars=["min_latency_ms", "max_latency_ms"],
                    var_name="latency_type",
                    value_name="latency_ms",
                )
                latency_figure = px.bar(
                    latency_long_df,
                    x="exchange_id",
                    y="latency_ms",
                    color="latency_type",
                    barmode="group",
                    template=plotly_template,
                )
                latency_figure.update_layout(
                    height=320,
                    margin={"l": 20, "r": 20, "t": 20, "b": 10},
                    xaxis_title="Exchange",
                    yaxis_title="Latency (ms)",
                    legend_title="Latency Metric",
                )
                st.plotly_chart(
                    latency_figure,
                    width="stretch",
                    config=PLOTLY_CHART_CONFIG,
                    key="quality_latency_bar",
                )


if __name__ == "__main__":
    main()
