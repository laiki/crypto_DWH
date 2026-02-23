#!/usr/bin/env python3
"""
Dashboard MVP for mart-based crypto KPI visualization.

Panels:
- Price curve (last 24h, Binance baseline) for selected symbol.
- Price deviation snapshot (daily, selected symbol).
- Platform quality snapshot (daily, all exchanges).
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


DEFAULT_DB_PATH = Path("data/core/core_kpi.db")
REQUIRED_VIEWS = (
    "vw_mart_dashboard_platform_quality_daily",
    "vw_mart_dashboard_price_deviation_daily",
    "vw_mart_dashboard_price_curve_24h_binance",
)

SQL_LIST_SYMBOLS = """
SELECT symbol
FROM (
    SELECT symbol FROM vw_mart_dashboard_price_curve_24h_binance
    UNION
    SELECT symbol FROM vw_mart_dashboard_price_deviation_daily
)
WHERE symbol IS NOT NULL
ORDER BY symbol ASC;
"""

SQL_PLATFORM_QUALITY_DAILY = """
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
    default_quality_rank
FROM vw_mart_dashboard_platform_quality_daily
WHERE kpi_date_utc = (SELECT kpi_date_utc FROM latest_day)
ORDER BY default_quality_rank ASC, exchange_id ASC;
"""

SQL_PRICE_DEVIATION_DAILY = """
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
  AND symbol = :symbol
ORDER BY default_deviation_rank ASC, symbol ASC;
"""

SQL_PRICE_CURVE_24H = """
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
"""


def _query_dataframe(db_path: Path, query: str, params: dict[str, object] | None = None) -> pd.DataFrame:
    with sqlite3.connect(str(db_path)) as connection:
        return pd.read_sql_query(query, connection, params=params)


def _missing_required_views(db_path: Path) -> list[str]:
    query = "SELECT name FROM sqlite_master WHERE type = 'view';"
    views_df = _query_dataframe(db_path, query)
    existing = set(views_df["name"].astype(str).tolist())
    return sorted(view_name for view_name in REQUIRED_VIEWS if view_name not in existing)


def _format_float(value: object, decimals: int = 3) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):.{decimals}f}"


def main() -> None:
    st.set_page_config(page_title="Crypto KPI Dashboard MVP", layout="wide")
    st.title("Crypto KPI Dashboard MVP")
    st.caption("Source: SQLite Core DB with mart dashboard views")

    with st.sidebar:
        st.header("Configuration")
        db_path_raw = st.text_input("Core SQLite DB path", str(DEFAULT_DB_PATH))

    db_path = Path(db_path_raw).expanduser()
    if not db_path.exists():
        st.error(f"Database path does not exist: {db_path}")
        st.stop()

    missing_views = _missing_required_views(db_path)
    if missing_views:
        st.error("Required mart views are missing in the selected database.")
        st.write("Missing views:")
        st.code("\n".join(missing_views))
        st.write("Apply mart views with:")
        st.code(f"sqlite3 {db_path} \".read scripts/5_marts/mart_dashboard_views.sql\"")
        st.stop()

    symbols_df = _query_dataframe(db_path, SQL_LIST_SYMBOLS)
    symbols = symbols_df["symbol"].astype(str).tolist()
    if not symbols:
        st.warning("No symbols found in mart views. Please check upstream data and mart refresh.")
        st.stop()

    with st.sidebar:
        selected_symbol = st.selectbox("Symbol", options=symbols, index=0)

    platform_quality_df = _query_dataframe(db_path, SQL_PLATFORM_QUALITY_DAILY)
    price_deviation_df = _query_dataframe(
        db_path,
        SQL_PRICE_DEVIATION_DAILY,
        params={"symbol": selected_symbol},
    )
    price_curve_df = _query_dataframe(
        db_path,
        SQL_PRICE_CURVE_24H,
        params={"symbol": selected_symbol},
    )

    st.subheader("Snapshot")
    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)

    latest_kpi_date = (
        str(platform_quality_df["kpi_date_utc"].iloc[0]) if not platform_quality_df.empty else "n/a"
    )
    total_disconnects = (
        int(platform_quality_df["disconnect_count"].fillna(0).sum()) if not platform_quality_df.empty else 0
    )
    exchanges_count = int(platform_quality_df["exchange_id"].nunique()) if not platform_quality_df.empty else 0
    curve_points = len(price_curve_df.index)

    metric_col_1.metric("Selected Symbol", selected_symbol)
    metric_col_2.metric("KPI Date (UTC)", latest_kpi_date)
    metric_col_3.metric("Exchanges in Snapshot", exchanges_count)
    metric_col_4.metric("Total Disconnects", total_disconnects)
    st.caption(f"Price curve points for selected symbol: {curve_points}")

    tab_curve, tab_deviation, tab_quality = st.tabs(
        ["Price Curve 24h", "Price Deviation", "Platform Quality"]
    )

    with tab_curve:
        st.write("Binance baseline from latest cleansing run.")
        if price_curve_df.empty:
            st.warning("No 24h price curve points available for selected symbol.")
        else:
            price_curve_plot = price_curve_df.copy()
            price_curve_plot["bucket_start_utc"] = pd.to_datetime(
                price_curve_plot["bucket_start_utc"],
                utc=True,
                errors="coerce",
            )
            price_curve_plot = price_curve_plot.dropna(subset=["bucket_start_utc"])
            st.line_chart(
                price_curve_plot.set_index("bucket_start_utc")["price_close"],
                use_container_width=True,
            )
            st.dataframe(
                price_curve_df[
                    ["run_id", "exchange_id", "symbol", "bucket_start_utc", "price_close", "point_index_asc"]
                ],
                use_container_width=True,
                hide_index=True,
            )

    with tab_deviation:
        st.write("Daily deviation snapshot for selected symbol.")
        if price_deviation_df.empty:
            st.warning("No daily price deviation data available for selected symbol.")
        else:
            row = price_deviation_df.iloc[0]
            dev_col_1, dev_col_2, dev_col_3, dev_col_4 = st.columns(4)
            dev_col_1.metric("Max Diff (%)", _format_float(row["max_price_diff_pct"], decimals=4))
            dev_col_2.metric("Max Diff (Abs)", _format_float(row["max_price_diff_abs"], decimals=8))
            dev_col_3.metric("Avg Diff (%)", _format_float(row["avg_price_diff_pct"], decimals=4))
            dev_col_4.metric("Compared Points", int(row["aligned_points_compared"]))
            st.write(f"Max spread exchange pair: `{row['max_diff_exchange_pair']}`")
            st.write(f"Max spread timestamp (UTC): `{row['max_diff_bucket_start_utc']}`")
            st.dataframe(price_deviation_df, use_container_width=True, hide_index=True)

    with tab_quality:
        st.write("Daily platform quality snapshot (all exchanges).")
        if platform_quality_df.empty:
            st.warning("No platform quality data available.")
        else:
            quality_display = platform_quality_df[
                [
                    "exchange_id",
                    "symbols_covered",
                    "avg_latency_ms",
                    "min_latency_ms",
                    "max_latency_ms",
                    "update_frequency_hz",
                    "disconnect_count",
                    "default_quality_rank",
                ]
            ]
            st.dataframe(quality_display, use_container_width=True, hide_index=True)
            st.bar_chart(
                quality_display.set_index("exchange_id")[["min_latency_ms", "max_latency_ms"]],
                use_container_width=True,
            )
            st.bar_chart(
                quality_display.set_index("exchange_id")[["disconnect_count"]],
                use_container_width=True,
            )


if __name__ == "__main__":
    main()
