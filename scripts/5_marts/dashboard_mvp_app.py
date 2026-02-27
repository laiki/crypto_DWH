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
import streamlit as st


DEFAULT_DB_PATH = Path("data/core/core_kpi.db")

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

SQL_PRICE_DEVIATION_WINDOW = """
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

SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW = """
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


def main() -> None:
    st.set_page_config(page_title="Crypto KPI Dashboard MVP", layout="wide")
    st.title("Crypto KPI Dashboard MVP")
    st.caption("Source: SQLite Core DB (dynamic window analysis on cleansed data)")

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

    existing_objects = _get_existing_objects(db_path)
    has_platform_quality_cache = "dash_cache_platform_quality_daily_latest" in existing_objects
    has_quality_view = "vw_mart_dashboard_platform_quality_daily" in existing_objects
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
        symbols_df = _query_dataframe(
            db_path,
            SQL_SYMBOLS_BY_RUN,
            params={"run_id": selected_run_id},
        )

    symbols = symbols_df["symbol"].astype(str).tolist()
    if not symbols:
        st.warning("No symbols found for selected run/window.")
        st.stop()

    symbol_from_query = _get_symbol_query_param()
    selected_symbol_index = symbols.index(symbol_from_query) if symbol_from_query in symbols else 0
    with st.sidebar:
        selected_symbol = st.selectbox("Symbol", options=symbols, index=selected_symbol_index)

    if selected_symbol != symbol_from_query:
        _set_symbol_query_param(selected_symbol)

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
    price_deviation_window_df = _query_dataframe(db_path, SQL_PRICE_DEVIATION_WINDOW, params=query_params)
    symbol_violin_df = _query_dataframe(
        db_path,
        SQL_SYMBOL_DEVIATION_VIOLIN_WINDOW,
        params={
            "run_id": selected_run_id,
            "window_start_utc": window_start_utc,
            "window_end_utc": window_end_utc,
        },
    )

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
        columns_per_row = 4
        grid_columns = st.columns(columns_per_row)
        for idx, symbol in enumerate(symbol_stats_df["symbol"].astype(str).tolist()):
            with grid_columns[idx % columns_per_row]:
                symbol_points_df = symbol_violin_df[symbol_violin_df["symbol"] == symbol]
                st.markdown(f"**{symbol}**")
                violin_figure = px.violin(
                    symbol_points_df,
                    y="price_diff_pct",
                    points=False,
                    box=True,
                )
                violin_figure.update_layout(
                    height=220,
                    margin={"l": 20, "r": 20, "t": 5, "b": 10},
                    showlegend=False,
                    xaxis_title=None,
                    yaxis_title="Diff %",
                )
                st.plotly_chart(
                    violin_figure,
                    use_container_width=True,
                    config={"displayModeBar": False},
                )
                stat_row = symbol_stats_df[symbol_stats_df["symbol"] == symbol].iloc[0]
                st.caption(
                    f"n={int(stat_row['point_count'])} | "
                    f"avg={float(stat_row['avg_price_diff_pct']):.4f}% | "
                    f"max={float(stat_row['max_price_diff_pct']):.4f}%"
                )
                if st.button(f"Open {symbol}", key=f"open_symbol_{symbol}"):
                    _set_symbol_query_param(symbol)
                    st.rerun()
        with st.expander("Symbol Deviation Stats (Table)"):
            st.dataframe(symbol_stats_df, use_container_width=True, hide_index=True)

    st.subheader("Snapshot")
    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)

    latest_kpi_date = (
        str(platform_quality_df["kpi_date_utc"].iloc[0]) if not platform_quality_df.empty else "n/a"
    )
    curve_points = len(price_curve_df)
    curve_exchanges = int(price_curve_df["exchange_id"].nunique()) if not price_curve_df.empty else 0
    deviation_points = len(price_deviation_window_df)

    metric_col_1.metric("Selected Symbol", selected_symbol)
    metric_col_2.metric("Selected Run", selected_run_id)
    metric_col_3.metric("Curve Exchanges", curve_exchanges)
    metric_col_4.metric("Aligned Diff Points", deviation_points)
    st.caption(
        f"Window UTC: {window_start_utc} -> {window_end_utc} | "
        f"Curve points: {curve_points} | Platform KPI date: {latest_kpi_date}"
    )

    tab_curve, tab_deviation, tab_quality = st.tabs(
        ["Price Curve (Window)", "Price Deviation (Window)", "Platform Quality"]
    )

    with tab_curve:
        st.write("Price development for selected symbol in selected UTC window.")
        if price_curve_df.empty:
            st.warning("No curve points available in selected run/window for this symbol.")
        else:
            exchange_point_counts = (
                price_curve_df.groupby("exchange_id", as_index=False)
                .size()
                .rename(columns={"size": "point_count"})
                .sort_values(by=["point_count", "exchange_id"], ascending=[False, True])
            )
            available_exchanges = exchange_point_counts["exchange_id"].astype(str).tolist()
            selected_curve_exchanges = st.multiselect(
                "Visible Exchanges",
                options=available_exchanges,
                default=available_exchanges,
            )
            if not selected_curve_exchanges:
                st.warning("Select at least one exchange to render the curve.")
                st.dataframe(exchange_point_counts, use_container_width=True, hide_index=True)
            else:
                filtered_curve_df = price_curve_df[
                    price_curve_df["exchange_id"].astype(str).isin(selected_curve_exchanges)
                ].copy()
                price_curve_plot = price_curve_df.copy()
                price_curve_plot["bucket_start_utc"] = pd.to_datetime(
                    price_curve_plot["bucket_start_utc"],
                    utc=True,
                    errors="coerce",
                )
                price_curve_plot = price_curve_plot.dropna(subset=["bucket_start_utc"])
                price_curve_plot = price_curve_plot[
                    price_curve_plot["exchange_id"].astype(str).isin(selected_curve_exchanges)
                ]
                curve_wide = price_curve_plot.pivot_table(
                    index="bucket_start_utc",
                    columns="exchange_id",
                    values="price_close",
                    aggfunc="last",
                )
                st.caption(
                    f"Visible exchanges: {len(selected_curve_exchanges)} / {len(available_exchanges)} "
                    f"(for symbol `{selected_symbol}` in selected window)."
                )
                st.line_chart(
                    curve_wide,
                    use_container_width=True,
                )
                st.dataframe(
                    filtered_curve_df[
                        [
                            "run_id",
                            "exchange_id",
                            "symbol",
                            "bucket_start_utc",
                            "bucket_epoch_s",
                            "price_close",
                            "fill_method",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )
                with st.expander("Exchange Coverage in Window"):
                    st.dataframe(exchange_point_counts, use_container_width=True, hide_index=True)

    with tab_deviation:
        st.write("Price deviation across exchanges for selected symbol and selected UTC window.")
        if price_deviation_window_df.empty:
            st.warning("No aligned deviation points available (need at least two exchanges per timestamp).")
        else:
            row_max = price_deviation_window_df.sort_values(
                by=["price_diff_abs", "bucket_epoch_s"],
                ascending=[False, True],
            ).iloc[0]
            dev_col_1, dev_col_2, dev_col_3, dev_col_4 = st.columns(4)
            dev_col_1.metric(
                "Max Diff (%)",
                _format_float(price_deviation_window_df["price_diff_pct"].max(), decimals=4),
            )
            dev_col_2.metric(
                "Max Diff (Abs)",
                _format_float(price_deviation_window_df["price_diff_abs"].max(), decimals=8),
            )
            dev_col_3.metric(
                "Avg Diff (%)",
                _format_float(price_deviation_window_df["price_diff_pct"].mean(), decimals=4),
            )
            dev_col_4.metric("Compared Points", int(len(price_deviation_window_df)))
            st.write(f"Max spread exchange pair: `{row_max['max_diff_exchange_pair']}`")
            st.write(f"Max spread timestamp (UTC): `{row_max['bucket_start_utc']}`")

            deviation_plot = price_deviation_window_df.copy()
            deviation_plot["bucket_start_utc"] = pd.to_datetime(
                deviation_plot["bucket_start_utc"],
                utc=True,
                errors="coerce",
            )
            deviation_plot = deviation_plot.dropna(subset=["bucket_start_utc"])
            st.line_chart(
                deviation_plot.set_index("bucket_start_utc")[["price_diff_abs"]],
                use_container_width=True,
            )
            st.line_chart(
                deviation_plot.set_index("bucket_start_utc")[["price_diff_pct"]],
                use_container_width=True,
            )
            st.dataframe(price_deviation_window_df, use_container_width=True, hide_index=True)

    with tab_quality:
        st.write("Daily platform quality snapshot (all exchanges).")
        st.info(
            "Ranking logic (best quality first): default_quality_score = weighted score with latency caps "
            "(min 1000 ms, avg 10000 ms, max 600000 ms). "
            "Weights: avg latency 35%, max latency 30%, min latency 15%, update frequency 10%, "
            "disconnect count 8%, symbols covered 2%. "
            "default_quality_rank = 1 means best score."
        )
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
