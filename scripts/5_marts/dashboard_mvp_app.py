#!/usr/bin/env python3
"""
Dashboard MVP for mart-based crypto KPI visualization.

Panels:
- Price curve (last 24h, Binance baseline) for selected symbol.
- Price deviation snapshot (daily, selected symbol).
- Platform quality snapshot (daily, all exchanges).
"""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pandas as pd
import streamlit as st


DEFAULT_DB_PATH = Path("data/core/core_kpi.db")
CACHE_TABLES = (
    "dash_cache_platform_quality_daily_latest",
    "dash_cache_price_deviation_daily_latest",
    "dash_cache_price_curve_24h_binance_latest",
    "dash_cache_symbols",
    "dash_cache_refresh_metadata",
)

SQL_LIST_SYMBOLS_CACHE = """
SELECT symbol
FROM dash_cache_symbols
WHERE symbol IS NOT NULL
ORDER BY symbol ASC;
"""

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

SQL_PRICE_DEVIATION_DAILY_CACHE = """
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
FROM dash_cache_price_deviation_daily_latest
WHERE symbol = :symbol
ORDER BY default_deviation_rank ASC, symbol ASC;
"""

SQL_PRICE_CURVE_24H_CACHE = """
SELECT
    run_id,
    exchange_id,
    symbol,
    bucket_start_utc,
    price_close,
    point_index_asc
FROM dash_cache_price_curve_24h_binance_latest
WHERE symbol = :symbol
ORDER BY point_index_asc ASC;
"""

SQL_EXISTING_TABLES = "SELECT name FROM sqlite_master WHERE type = 'table';"
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


def _has_cache_tables(db_path: Path) -> bool:
    tables_df = _query_dataframe(db_path, SQL_EXISTING_TABLES)
    existing = set(tables_df["name"].astype(str).tolist())
    return all(table_name in existing for table_name in CACHE_TABLES)


def _format_float(value: object, decimals: int = 3) -> str:
    if pd.isna(value):
        return "n/a"
    return f"{float(value):.{decimals}f}"


def main() -> None:
    st.set_page_config(page_title="Crypto KPI Dashboard MVP", layout="wide")
    st.title("Crypto KPI Dashboard MVP")
    st.caption("Source: precomputed dashboard cache tables in SQLite Core DB")

    with st.sidebar:
        st.header("Configuration")
        db_path_raw = st.text_input("Core SQLite DB path", str(DEFAULT_DB_PATH))
        if st.button("Refresh Query Cache"):
            st.cache_data.clear()
            st.success("Query cache cleared.")

    db_path = Path(db_path_raw).expanduser()
    if not db_path.exists():
        st.error(f"Database path does not exist: {db_path}")
        st.stop()

    if not _has_cache_tables(db_path):
        st.error("Dashboard cache tables are missing. The dashboard runs only in cache mode.")
        st.write("Prepare cache tables with:")
        st.code(f"python scripts/5_marts/build_dashboard_cache.py --db-path \"{db_path}\"")
        st.stop()

    with st.sidebar:
        st.success("Data mode: cache tables (required)")

    cache_meta_df = _query_dataframe(db_path, SQL_CACHE_METADATA)
    if not cache_meta_df.empty:
        meta_row = cache_meta_df.iloc[0]
        with st.sidebar:
            st.caption(f"Last cache refresh (UTC): {meta_row['refresh_ts_utc']}")
            st.caption(
                "Rows: "
                f"platform={int(meta_row['platform_rows'])}, "
                f"deviation={int(meta_row['deviation_rows'])}, "
                f"curve={int(meta_row['curve_rows'])}, "
                f"symbols={int(meta_row['symbol_rows'])}"
            )

    symbols_df = _query_dataframe(db_path, SQL_LIST_SYMBOLS_CACHE)
    symbols = symbols_df["symbol"].astype(str).tolist()
    if not symbols:
        st.warning("No symbols found in cache tables. Please rebuild dashboard cache.")
        st.stop()

    with st.sidebar:
        selected_symbol = st.selectbox("Symbol", options=symbols, index=0)

    platform_quality_df = _query_dataframe(db_path, SQL_PLATFORM_QUALITY_DAILY_CACHE)
    price_deviation_df = _query_dataframe(
        db_path,
        SQL_PRICE_DEVIATION_DAILY_CACHE,
        params={"symbol": selected_symbol},
    )
    price_curve_df = _query_dataframe(
        db_path,
        SQL_PRICE_CURVE_24H_CACHE,
        params={"symbol": selected_symbol},
    )

    st.subheader("Snapshot")
    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)

    latest_kpi_date = (
        str(platform_quality_df["kpi_date_utc"].iloc[0]) if not platform_quality_df.empty else "n/a"
    )
    exchanges_count = int(platform_quality_df["exchange_id"].nunique()) if not platform_quality_df.empty else 0
    curve_points = len(price_curve_df.index)
    top_disconnect_exchange = "n/a"
    top_disconnect_count = 0
    if not platform_quality_df.empty:
        disconnect_sorted = platform_quality_df.sort_values(
            by=["disconnect_count", "exchange_id"],
            ascending=[False, True],
        )
        top_disconnect_exchange = str(disconnect_sorted["exchange_id"].iloc[0])
        top_disconnect_count = int(disconnect_sorted["disconnect_count"].iloc[0])

    metric_col_1.metric("Selected Symbol", selected_symbol)
    metric_col_2.metric("KPI Date (UTC)", latest_kpi_date)
    metric_col_3.metric("Exchanges in Snapshot", exchanges_count)
    metric_col_4.metric("Top Disconnect Exchange", f"{top_disconnect_exchange} ({top_disconnect_count})")
    st.caption(f"Price curve points for selected symbol: {curve_points}")

    tab_curve, tab_deviation, tab_quality = st.tabs(
        ["Price Curve 24h", "Price Deviation", "Platform Quality"]
    )

    with tab_curve:
        st.write("Binance baseline from latest cleansing run.")
        if price_curve_df.empty:
            st.warning(
                "No 24h price curve points available for selected symbol in cache. "
                "If recent data exists, refresh mart views and rebuild dashboard cache."
            )
            st.code(f"python scripts/5_marts/build_dashboard_cache.py --db-path \"{db_path}\"")
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
        st.info(
            "Ranking logic (best quality first): default_quality_score = average of 6 normalized components: "
            "lower min_latency_ms, lower avg_latency_ms, lower max_latency_ms, higher update_frequency_hz, "
            "lower disconnect_count, higher symbols_covered. "
            "default_quality_rank = 1 means best score."
        )
        if platform_quality_df.empty:
            st.warning("No platform quality data available.")
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
            ]
            quality_by_disconnect = quality_display.sort_values(
                by=["disconnect_count", "exchange_id"],
                ascending=[False, True],
            )
            st.dataframe(quality_display, use_container_width=True, hide_index=True)
            st.write("Disconnects by exchange (sorted descending).")
            st.bar_chart(
                quality_by_disconnect.set_index("exchange_id")[["disconnect_count"]],
                use_container_width=True,
            )
            st.bar_chart(
                quality_display.set_index("exchange_id")[["min_latency_ms", "max_latency_ms"]],
                use_container_width=True,
            )


if __name__ == "__main__":
    main()
