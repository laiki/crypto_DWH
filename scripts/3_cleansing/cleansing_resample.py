#!/usr/bin/env python3
"""
Build a 1-minute cleansed time series from staging market ticks.

This script aligns each (exchange_id, symbol) stream into fixed-size time bins and
applies observed / forward_fill / interpolation / missing rules with quality flags.
"""

from __future__ import annotations

import argparse
import json
import logging
import multiprocessing as mp
import sqlite3
import sys
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


LOGGER = logging.getLogger("cleansing_resample")


@dataclass
class PairProfile:
    exchange_id: str
    symbol: str
    min_epoch_s: int
    max_epoch_s: int
    source_tick_count: int


@dataclass
class PairSummary:
    exchange_id: str
    symbol: str
    source_tick_count: int
    bins_total: int
    bins_observed: int
    bins_forward_fill: int
    bins_interpolation: int
    bins_missing: int


@dataclass
class DQSummary:
    exchange_id: str
    symbol: str
    source_row_count: int
    null_ingestion_ts_count: int
    invalid_ingestion_ts_count: int
    null_effective_price_count: int
    non_positive_effective_price_count: int
    rows_with_negative_values_count: int
    bid_gt_ask_count: int
    duplicate_tick_count: int
    outlier_return_count: int
    issue_counter_sum: int


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create a cleansed fixed-interval series from staging market_ticks with "
            "observed/forward_fill/interpolation/missing flags using a single-writer output model."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--input-db",
        required=True,
        help="Path to staging SQLite database that contains market_ticks.",
    )
    parser.add_argument(
        "--output-db",
        default=None,
        help=(
            "Path to output cleansing SQLite database. "
            "If omitted and input is under a directory named staging, uses "
            "<staging_parent>/cleansing/cleaned_<input_name>_<bin_seconds>s.db. "
            "Otherwise uses <input_db_dir>/cleansing/cleaned_<input_name>_<bin_seconds>s.db."
        ),
    )
    parser.add_argument(
        "--bin-seconds",
        type=int,
        default=60,
        help="Target bin size in seconds.",
    )
    parser.add_argument(
        "--max-forward-fill-s",
        type=int,
        default=300,
        help="Maximum allowed age for forward fill values.",
    )
    parser.add_argument(
        "--stale-after-s",
        type=int,
        default=90,
        help="Rows with age_seconds above this threshold are marked as stale.",
    )
    parser.add_argument(
        "--fill-method",
        choices=["forward_fill", "interpolation"],
        default="forward_fill",
        help=(
            "Fill strategy for empty bins. "
            "interpolation = linear interpolation between previous and next value "
            "when both are within max_forward_fill_s, otherwise fallback to forward_fill behavior."
        ),
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Optional comma-separated exchange filter.",
    )
    parser.add_argument(
        "--symbols",
        default=None,
        help="Optional comma-separated symbol filter (for example BTC/USDT,ETH/USDT).",
    )
    parser.add_argument(
        "--enable-dq",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Run data-quality checks and persist DQ summary tables.",
    )
    parser.add_argument(
        "--dq-outlier-threshold-pct",
        type=float,
        default=0.2,
        help="Outlier threshold on absolute return for observed cleansed bins (for example 0.2 = 20%%).",
    )
    parser.add_argument(
        "--pair-limit",
        type=int,
        default=None,
        help="Optional limit of pairs to process after filtering.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for pair processing. Output writes always use a single writer.",
    )
    parser.add_argument(
        "--worker-queue-size",
        type=int,
        default=128,
        help=(
            "Max queue size for worker result messages. "
            "Use 0 for unbounded queue."
        ),
    )
    parser.add_argument(
        "--insert-batch-size",
        type=int,
        default=5000,
        help="Batch size for inserts into output DB.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level.",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="Optional log file path. Default: <output_db_name>.log derived from <output_db_name>.json.",
    )
    return parser.parse_args()


def normalize_csv_values(raw: str | None, *, to_lower: bool = False) -> list[str]:
    if raw is None:
        return []
    values = [item.strip() for item in raw.split(",")]
    values = [item for item in values if item]
    if to_lower:
        values = [item.lower() for item in values]
    seen: set[str] = set()
    ordered: list[str] = []
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        ordered.append(value)
    return ordered


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def epoch_to_utc_iso(epoch_s: int) -> str:
    return datetime.fromtimestamp(epoch_s, tz=timezone.utc).isoformat(timespec="seconds")


def floor_epoch(epoch_s: int, bin_seconds: int) -> int:
    return (epoch_s // bin_seconds) * bin_seconds


def default_output_path(input_db_path: Path, bin_seconds: int) -> Path:
    suffix = input_db_path.suffix if input_db_path.suffix else ".db"
    filename = f"cleaned_{input_db_path.stem}_{bin_seconds}s{suffix}"
    staging_ancestor = next(
        (parent for parent in [input_db_path.parent, *input_db_path.parent.parents] if parent.name.lower() == "staging"),
        None,
    )
    if staging_ancestor is not None:
        output_dir = staging_ancestor.parent / "cleansing"
    else:
        output_dir = input_db_path.parent / "cleansing"
    return output_dir / filename


def default_json_path(output_db_path: Path) -> Path:
    return Path(str(output_db_path) + ".json")


def default_log_path_from_output_db(output_db_path: Path) -> Path:
    return default_json_path(output_db_path).with_suffix(".log")


def configure_logging(log_level: str, log_file: Path) -> None:
    log_file.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger()
    logger.handlers.clear()
    logger.setLevel(logging.DEBUG)
    selected_level = getattr(logging, log_level)
    file_level = selected_level if selected_level >= logging.INFO else logging.INFO

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(selected_level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)


def assert_input_schema(connection: sqlite3.Connection) -> None:
    row = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_ticks' LIMIT 1"
    ).fetchone()
    if row is None:
        raise SystemExit("Input DB does not contain required table: market_ticks")


def ensure_output_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS cleansed_market (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            bucket_start_utc TEXT NOT NULL,
            bucket_epoch_s INTEGER NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            bin_seconds INTEGER NOT NULL,
            max_forward_fill_s INTEGER NOT NULL,
            fill_method_mode TEXT NOT NULL,
            price REAL,
            source_ingestion_ts_utc TEXT,
            source_ingestion_epoch_s INTEGER,
            tick_count_in_bin INTEGER NOT NULL,
            fill_method TEXT NOT NULL,
            age_seconds REAL,
            is_stale INTEGER NOT NULL,
            is_missing INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_cleansed_market_pair_time
            ON cleansed_market(exchange_id, symbol, bucket_epoch_s);

        CREATE INDEX IF NOT EXISTS idx_cleansed_market_run
            ON cleansed_market(run_id);

        CREATE TABLE IF NOT EXISTS cleansing_pair_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            source_tick_count INTEGER NOT NULL,
            bins_total INTEGER NOT NULL,
            bins_observed INTEGER NOT NULL,
            bins_forward_fill INTEGER NOT NULL,
            bins_interpolation INTEGER NOT NULL DEFAULT 0,
            bins_missing INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_cleansing_pair_summary_run
            ON cleansing_pair_summary(run_id);

        CREATE TABLE IF NOT EXISTS cleansing_run_metadata (
            run_id TEXT PRIMARY KEY,
            created_utc TEXT NOT NULL,
            source_db_path TEXT NOT NULL,
            output_db_path TEXT NOT NULL,
            workers INTEGER NOT NULL,
            bin_seconds INTEGER NOT NULL,
            max_forward_fill_s INTEGER NOT NULL,
            stale_after_s INTEGER NOT NULL,
            fill_method_mode TEXT NOT NULL DEFAULT 'forward_fill',
            dq_enabled INTEGER NOT NULL DEFAULT 1,
            dq_outlier_threshold_pct REAL NOT NULL DEFAULT 0.2,
            exchange_filter_csv TEXT,
            symbol_filter_csv TEXT,
            total_pairs INTEGER NOT NULL,
            total_rows INTEGER NOT NULL,
            observed_rows INTEGER NOT NULL,
            forward_fill_rows INTEGER NOT NULL,
            interpolation_rows INTEGER NOT NULL DEFAULT 0,
            missing_rows INTEGER NOT NULL,
            runtime_seconds REAL NOT NULL
        );

        CREATE TABLE IF NOT EXISTS cleansing_dq_summary (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            run_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            source_row_count INTEGER NOT NULL,
            null_ingestion_ts_count INTEGER NOT NULL,
            invalid_ingestion_ts_count INTEGER NOT NULL,
            null_effective_price_count INTEGER NOT NULL,
            non_positive_effective_price_count INTEGER NOT NULL,
            rows_with_negative_values_count INTEGER NOT NULL,
            bid_gt_ask_count INTEGER NOT NULL,
            duplicate_tick_count INTEGER NOT NULL,
            outlier_return_count INTEGER NOT NULL,
            issue_counter_sum INTEGER NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_cleansing_dq_summary_run
            ON cleansing_dq_summary(run_id);

        CREATE INDEX IF NOT EXISTS idx_cleansing_dq_summary_pair
            ON cleansing_dq_summary(run_id, exchange_id, symbol);

        CREATE TABLE IF NOT EXISTS cleansing_dq_run_summary (
            run_id TEXT PRIMARY KEY,
            created_utc TEXT NOT NULL,
            pair_count INTEGER NOT NULL,
            pairs_with_any_issue INTEGER NOT NULL,
            total_issue_counter_sum INTEGER NOT NULL,
            total_negative_value_rows INTEGER NOT NULL,
            total_outlier_return_count INTEGER NOT NULL
        );
        """
    )
    connection.commit()


def build_pair_filter_clause(exchanges: list[str], symbols: list[str]) -> tuple[str, list[Any]]:
    conditions = ["ingestion_ts_utc IS NOT NULL", "COALESCE(price, last) IS NOT NULL"]
    params: list[Any] = []
    if exchanges:
        placeholders = ",".join("?" for _ in exchanges)
        conditions.append(f"lower(exchange_id) IN ({placeholders})")
        params.extend(exchanges)
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        conditions.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    return " AND ".join(conditions), params


def build_scope_filter_clause(exchanges: list[str], symbols: list[str]) -> tuple[str, list[Any]]:
    conditions = [
        "exchange_id IS NOT NULL",
        "symbol IS NOT NULL",
        "trim(exchange_id) <> ''",
        "trim(symbol) <> ''",
    ]
    params: list[Any] = []
    if exchanges:
        placeholders = ",".join("?" for _ in exchanges)
        conditions.append(f"lower(exchange_id) IN ({placeholders})")
        params.extend(exchanges)
    if symbols:
        placeholders = ",".join("?" for _ in symbols)
        conditions.append(f"symbol IN ({placeholders})")
        params.extend(symbols)
    return " AND ".join(conditions), params


def load_pair_profiles(
    connection: sqlite3.Connection,
    where_clause: str,
    params: list[Any],
    pair_limit: int | None,
) -> list[PairProfile]:
    query = (
        "SELECT "
        "  exchange_id, "
        "  symbol, "
        "  MIN(CAST(strftime('%s', ingestion_ts_utc) AS INTEGER)) AS min_epoch_s, "
        "  MAX(CAST(strftime('%s', ingestion_ts_utc) AS INTEGER)) AS max_epoch_s, "
        "  COUNT(*) AS source_tick_count "
        "FROM market_ticks "
        f"WHERE {where_clause} "
        "GROUP BY exchange_id, symbol "
        "ORDER BY exchange_id, symbol"
    )
    rows = connection.execute(query, params).fetchall()
    profiles: list[PairProfile] = []
    for row in rows:
        min_epoch_s = row[2]
        max_epoch_s = row[3]
        if min_epoch_s is None or max_epoch_s is None:
            continue
        profiles.append(
            PairProfile(
                exchange_id=str(row[0]),
                symbol=str(row[1]),
                min_epoch_s=int(min_epoch_s),
                max_epoch_s=int(max_epoch_s),
                source_tick_count=int(row[4]),
            )
        )
    if pair_limit is not None and pair_limit > 0:
        return profiles[:pair_limit]
    return profiles


def collect_source_dq_summaries(
    connection: sqlite3.Connection,
    scope_where_clause: str,
    scope_params: list[Any],
) -> list[DQSummary]:
    query = (
        "WITH scoped AS ("
        "  SELECT "
        "    exchange_id, "
        "    symbol, "
        "    ingestion_ts_utc, "
        "    COALESCE(price, last) AS effective_price, "
        "    price, "
        "    bid, "
        "    ask, "
        "    last, "
        "    base_volume, "
        "    quote_volume "
        "  FROM market_ticks "
        f"  WHERE {scope_where_clause}"
        "), "
        "dup AS ("
        "  SELECT "
        "    exchange_id, "
        "    symbol, "
        "    SUM(cnt - 1) AS duplicate_tick_count "
        "  FROM ("
        "    SELECT "
        "      exchange_id, "
        "      symbol, "
        "      ingestion_ts_utc, "
        "      effective_price, "
        "      COUNT(*) AS cnt "
        "    FROM scoped "
        "    GROUP BY exchange_id, symbol, ingestion_ts_utc, effective_price "
        "    HAVING COUNT(*) > 1"
        "  ) grouped_dups "
        "  GROUP BY exchange_id, symbol"
        "), "
        "agg AS ("
        "  SELECT "
        "    exchange_id, "
        "    symbol, "
        "    COUNT(*) AS source_row_count, "
        "    SUM(CASE WHEN ingestion_ts_utc IS NULL THEN 1 ELSE 0 END) AS null_ingestion_ts_count, "
        "    SUM(CASE WHEN ingestion_ts_utc IS NOT NULL AND strftime('%s', ingestion_ts_utc) IS NULL THEN 1 ELSE 0 END) AS invalid_ingestion_ts_count, "
        "    SUM(CASE WHEN effective_price IS NULL THEN 1 ELSE 0 END) AS null_effective_price_count, "
        "    SUM(CASE WHEN effective_price IS NOT NULL AND effective_price <= 0 THEN 1 ELSE 0 END) AS non_positive_effective_price_count, "
        "    SUM(CASE "
        "      WHEN (price IS NOT NULL AND price < 0) "
        "        OR (bid IS NOT NULL AND bid < 0) "
        "        OR (ask IS NOT NULL AND ask < 0) "
        "        OR (last IS NOT NULL AND last < 0) "
        "        OR (base_volume IS NOT NULL AND base_volume < 0) "
        "        OR (quote_volume IS NOT NULL AND quote_volume < 0) "
        "      THEN 1 ELSE 0 END"
        "    ) AS rows_with_negative_values_count, "
        "    SUM(CASE WHEN bid IS NOT NULL AND ask IS NOT NULL AND bid > ask THEN 1 ELSE 0 END) AS bid_gt_ask_count "
        "  FROM scoped "
        "  GROUP BY exchange_id, symbol"
        ") "
        "SELECT "
        "  agg.exchange_id, "
        "  agg.symbol, "
        "  agg.source_row_count, "
        "  agg.null_ingestion_ts_count, "
        "  agg.invalid_ingestion_ts_count, "
        "  agg.null_effective_price_count, "
        "  agg.non_positive_effective_price_count, "
        "  agg.rows_with_negative_values_count, "
        "  agg.bid_gt_ask_count, "
        "  COALESCE(dup.duplicate_tick_count, 0) AS duplicate_tick_count "
        "FROM agg "
        "LEFT JOIN dup "
        "  ON agg.exchange_id = dup.exchange_id AND agg.symbol = dup.symbol "
        "ORDER BY agg.exchange_id, agg.symbol"
    )
    rows = connection.execute(query, scope_params).fetchall()

    summaries: list[DQSummary] = []
    for row in rows:
        dq = DQSummary(
            exchange_id=str(row[0]),
            symbol=str(row[1]),
            source_row_count=int(row[2]),
            null_ingestion_ts_count=int(row[3]),
            invalid_ingestion_ts_count=int(row[4]),
            null_effective_price_count=int(row[5]),
            non_positive_effective_price_count=int(row[6]),
            rows_with_negative_values_count=int(row[7]),
            bid_gt_ask_count=int(row[8]),
            duplicate_tick_count=int(row[9]),
            outlier_return_count=0,
            issue_counter_sum=0,
        )
        summaries.append(dq)
    return summaries


def collect_outlier_counts_from_cleansed(
    connection: sqlite3.Connection,
    run_id: str,
    outlier_threshold_pct: float,
) -> dict[tuple[str, str], int]:
    query = (
        "WITH ordered AS ("
        "  SELECT "
        "    exchange_id, "
        "    symbol, "
        "    bucket_epoch_s, "
        "    price, "
        "    LAG(price) OVER (PARTITION BY exchange_id, symbol ORDER BY bucket_epoch_s) AS prev_price "
        "  FROM cleansed_market "
        "  WHERE run_id = ? AND fill_method = 'observed' AND price IS NOT NULL"
        "), "
        "flagged AS ("
        "  SELECT "
        "    exchange_id, "
        "    symbol, "
        "    CASE "
        "      WHEN prev_price IS NOT NULL "
        "       AND prev_price <> 0 "
        "       AND ABS((price - prev_price) / prev_price) > ? "
        "      THEN 1 ELSE 0 END AS is_outlier "
        "  FROM ordered"
        ") "
        "SELECT exchange_id, symbol, SUM(is_outlier) AS outlier_return_count "
        "FROM flagged "
        "GROUP BY exchange_id, symbol"
    )
    rows = connection.execute(query, (run_id, outlier_threshold_pct)).fetchall()
    return {
        (str(row[0]), str(row[1])): int(row[2] or 0)
        for row in rows
    }


def finalize_dq_issue_counters(summaries: list[DQSummary]) -> None:
    for item in summaries:
        item.issue_counter_sum = (
            item.null_ingestion_ts_count
            + item.invalid_ingestion_ts_count
            + item.null_effective_price_count
            + item.non_positive_effective_price_count
            + item.rows_with_negative_values_count
            + item.bid_gt_ask_count
            + item.duplicate_tick_count
            + item.outlier_return_count
        )


def insert_dq_summary(connection: sqlite3.Connection, run_id: str, summaries: list[DQSummary]) -> None:
    connection.execute("DELETE FROM cleansing_dq_summary WHERE run_id = ?", (run_id,))
    if summaries:
        payload = [
            (
                run_id,
                item.exchange_id,
                item.symbol,
                item.source_row_count,
                item.null_ingestion_ts_count,
                item.invalid_ingestion_ts_count,
                item.null_effective_price_count,
                item.non_positive_effective_price_count,
                item.rows_with_negative_values_count,
                item.bid_gt_ask_count,
                item.duplicate_tick_count,
                item.outlier_return_count,
                item.issue_counter_sum,
            )
            for item in summaries
        ]
        connection.executemany(
            """
            INSERT INTO cleansing_dq_summary (
                run_id,
                exchange_id,
                symbol,
                source_row_count,
                null_ingestion_ts_count,
                invalid_ingestion_ts_count,
                null_effective_price_count,
                non_positive_effective_price_count,
                rows_with_negative_values_count,
                bid_gt_ask_count,
                duplicate_tick_count,
                outlier_return_count,
                issue_counter_sum
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            payload,
        )
    connection.commit()


def insert_dq_run_summary(connection: sqlite3.Connection, run_id: str, summaries: list[DQSummary]) -> None:
    pair_count = len(summaries)
    pairs_with_any_issue = sum(1 for item in summaries if item.issue_counter_sum > 0)
    total_issue_counter_sum = sum(item.issue_counter_sum for item in summaries)
    total_negative_value_rows = sum(item.rows_with_negative_values_count for item in summaries)
    total_outlier_return_count = sum(item.outlier_return_count for item in summaries)
    connection.execute(
        """
        INSERT OR REPLACE INTO cleansing_dq_run_summary (
            run_id,
            created_utc,
            pair_count,
            pairs_with_any_issue,
            total_issue_counter_sum,
            total_negative_value_rows,
            total_outlier_return_count
        )
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            utc_now_iso(),
            pair_count,
            pairs_with_any_issue,
            total_issue_counter_sum,
            total_negative_value_rows,
            total_outlier_return_count,
        ),
    )
    connection.commit()


def insert_rows_chunk(
    connection: sqlite3.Connection,
    rows: list[tuple[Any, ...]],
) -> None:
    if not rows:
        return
    connection.executemany(
        """
        INSERT INTO cleansed_market (
            run_id,
            bucket_start_utc,
            bucket_epoch_s,
            exchange_id,
            symbol,
            bin_seconds,
            max_forward_fill_s,
            fill_method_mode,
            price,
            source_ingestion_ts_utc,
            source_ingestion_epoch_s,
            tick_count_in_bin,
            fill_method,
            age_seconds,
            is_stale,
            is_missing
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        rows,
    )
    connection.commit()


def query_rows_as_dicts(
    connection: sqlite3.Connection,
    query: str,
    params: tuple[Any, ...],
) -> list[dict[str, Any]]:
    cursor = connection.execute(query, params)
    columns = [item[0] for item in cursor.description or []]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def write_run_json_export(
    connection: sqlite3.Connection,
    output_db_path: Path,
    run_id: str,
) -> Path:
    metadata_rows = query_rows_as_dicts(
        connection=connection,
        query="SELECT * FROM cleansing_run_metadata WHERE run_id = ?",
        params=(run_id,),
    )
    pair_summary_rows = query_rows_as_dicts(
        connection=connection,
        query=(
            "SELECT * FROM cleansing_pair_summary "
            "WHERE run_id = ? "
            "ORDER BY exchange_id, symbol"
        ),
        params=(run_id,),
    )
    cleansed_rows = query_rows_as_dicts(
        connection=connection,
        query=(
            "SELECT * FROM cleansed_market "
            "WHERE run_id = ? "
            "ORDER BY exchange_id, symbol, bucket_epoch_s"
        ),
        params=(run_id,),
    )
    dq_summary_rows = query_rows_as_dicts(
        connection=connection,
        query=(
            "SELECT * FROM cleansing_dq_summary "
            "WHERE run_id = ? "
            "ORDER BY exchange_id, symbol"
        ),
        params=(run_id,),
    )
    dq_run_rows = query_rows_as_dicts(
        connection=connection,
        query="SELECT * FROM cleansing_dq_run_summary WHERE run_id = ?",
        params=(run_id,),
    )

    payload = {
        "contract_name": "cleansing_run_export",
        "contract_version": "1.0.0",
        "created_utc": utc_now_iso(),
        "run_id": run_id,
        "output_db_path": str(output_db_path),
        "tables": {
            "cleansing_run_metadata": metadata_rows,
            "cleansing_pair_summary": pair_summary_rows,
            "cleansed_market": cleansed_rows,
            "cleansing_dq_summary": dq_summary_rows,
            "cleansing_dq_run_summary": dq_run_rows,
        },
    }

    json_path = default_json_path(output_db_path)
    json_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")
    return json_path


def process_pair(
    source_connection: sqlite3.Connection,
    profile: PairProfile,
    *,
    run_id: str,
    bin_seconds: int,
    max_forward_fill_s: int,
    stale_after_s: int,
    fill_method_mode: str,
    emit_batch_size: int,
    emit_rows: Callable[[list[tuple[Any, ...]]], None],
) -> PairSummary:
    query = (
        "SELECT "
        "  id, "
        "  ingestion_ts_utc, "
        "  CAST(strftime('%s', ingestion_ts_utc) AS INTEGER) AS ts_epoch_s, "
        "  COALESCE(price, last) AS effective_price "
        "FROM market_ticks "
        "WHERE exchange_id = ? AND symbol = ? "
        "  AND ingestion_ts_utc IS NOT NULL "
        "  AND COALESCE(price, last) IS NOT NULL "
        "ORDER BY ingestion_ts_utc, id"
    )
    cursor = source_connection.execute(query, (profile.exchange_id, profile.symbol))

    start_epoch_s = floor_epoch(profile.min_epoch_s, bin_seconds)
    end_epoch_s = floor_epoch(profile.max_epoch_s, bin_seconds)

    current_bin_epoch_s = start_epoch_s
    current_bin_tick_count = 0
    current_bin_last_price: float | None = None
    current_bin_last_ts_utc: str | None = None
    current_bin_last_ts_epoch_s: int | None = None

    last_known_price: float | None = None
    last_known_ts_utc: str | None = None
    last_known_ts_epoch_s: int | None = None

    rows_to_emit: list[tuple[Any, ...]] = []
    bins_total = 0
    bins_observed = 0
    bins_forward_fill = 0
    bins_interpolation = 0
    bins_missing = 0

    def _emit_if_needed(force: bool = False) -> None:
        if not rows_to_emit:
            return
        if not force and len(rows_to_emit) < emit_batch_size:
            return
        emit_rows(list(rows_to_emit))
        rows_to_emit.clear()

    def _finalize_current_bin(next_tick_epoch_s: int | None = None, next_tick_price: float | None = None) -> None:
        nonlocal current_bin_tick_count
        nonlocal current_bin_last_price
        nonlocal current_bin_last_ts_utc
        nonlocal current_bin_last_ts_epoch_s
        nonlocal last_known_price
        nonlocal last_known_ts_utc
        nonlocal last_known_ts_epoch_s
        nonlocal bins_total
        nonlocal bins_observed
        nonlocal bins_forward_fill
        nonlocal bins_interpolation
        nonlocal bins_missing

        bucket_ts_utc = epoch_to_utc_iso(current_bin_epoch_s)
        price: float | None = None
        source_ts_utc: str | None = None
        source_ts_epoch_s: int | None = None
        fill_method = "missing"
        age_seconds: float | None = None
        is_stale = 1
        is_missing = 1
        tick_count_in_bin = current_bin_tick_count

        if current_bin_tick_count > 0 and current_bin_last_price is not None and current_bin_last_ts_epoch_s is not None:
            price = current_bin_last_price
            source_ts_utc = current_bin_last_ts_utc
            source_ts_epoch_s = current_bin_last_ts_epoch_s
            fill_method = "observed"
            age_seconds = 0.0
            is_stale = 0
            is_missing = 0
            bins_observed += 1

            last_known_price = current_bin_last_price
            last_known_ts_utc = current_bin_last_ts_utc
            last_known_ts_epoch_s = current_bin_last_ts_epoch_s
        elif last_known_price is not None and last_known_ts_epoch_s is not None:
            candidate_age = float(current_bin_epoch_s - last_known_ts_epoch_s)
            forward_fill_possible = candidate_age <= float(max_forward_fill_s)
            interpolation_possible = (
                fill_method_mode == "interpolation"
                and forward_fill_possible
                and next_tick_epoch_s is not None
                and next_tick_price is not None
                and next_tick_epoch_s > current_bin_epoch_s
                and next_tick_epoch_s > last_known_ts_epoch_s
                and float(next_tick_epoch_s - current_bin_epoch_s) <= float(max_forward_fill_s)
            )

            if interpolation_possible:
                interpolation_span_s = float(next_tick_epoch_s - last_known_ts_epoch_s)
                ratio = float(current_bin_epoch_s - last_known_ts_epoch_s) / interpolation_span_s
                interpolated_price = float(last_known_price) + (
                    float(next_tick_price) - float(last_known_price)
                ) * ratio
                prev_age = float(current_bin_epoch_s - last_known_ts_epoch_s)
                next_age = float(next_tick_epoch_s - current_bin_epoch_s)
                nearest_age = min(prev_age, next_age)

                price = interpolated_price
                source_ts_utc = None
                source_ts_epoch_s = None
                fill_method = "interpolation"
                age_seconds = nearest_age
                is_stale = 1 if nearest_age > float(stale_after_s) else 0
                is_missing = 0
                bins_interpolation += 1
            elif forward_fill_possible:
                price = last_known_price
                source_ts_utc = last_known_ts_utc
                source_ts_epoch_s = last_known_ts_epoch_s
                fill_method = "forward_fill"
                age_seconds = candidate_age
                is_stale = 1 if candidate_age > float(stale_after_s) else 0
                is_missing = 0
                bins_forward_fill += 1
            else:
                bins_missing += 1
        else:
            bins_missing += 1

        bins_total += 1
        rows_to_emit.append(
            (
                run_id,
                bucket_ts_utc,
                current_bin_epoch_s,
                profile.exchange_id,
                profile.symbol,
                bin_seconds,
                max_forward_fill_s,
                fill_method_mode,
                price,
                source_ts_utc,
                source_ts_epoch_s,
                tick_count_in_bin,
                fill_method,
                age_seconds,
                is_stale,
                is_missing,
            )
        )

        current_bin_tick_count = 0
        current_bin_last_price = None
        current_bin_last_ts_utc = None
        current_bin_last_ts_epoch_s = None

    for _, ingestion_ts_utc, ts_epoch_s, effective_price in cursor:
        if ts_epoch_s is None:
            continue
        tick_epoch_s = int(ts_epoch_s)
        tick_bin_epoch_s = floor_epoch(tick_epoch_s, bin_seconds)
        tick_price = float(effective_price)

        while current_bin_epoch_s < tick_bin_epoch_s:
            _finalize_current_bin(next_tick_epoch_s=tick_epoch_s, next_tick_price=tick_price)
            _emit_if_needed()
            current_bin_epoch_s += bin_seconds

        current_bin_tick_count += 1
        current_bin_last_price = tick_price
        current_bin_last_ts_utc = str(ingestion_ts_utc)
        current_bin_last_ts_epoch_s = tick_epoch_s

    while current_bin_epoch_s <= end_epoch_s:
        _finalize_current_bin()
        _emit_if_needed()
        current_bin_epoch_s += bin_seconds

    _emit_if_needed(force=True)

    return PairSummary(
        exchange_id=profile.exchange_id,
        symbol=profile.symbol,
        source_tick_count=profile.source_tick_count,
        bins_total=bins_total,
        bins_observed=bins_observed,
        bins_forward_fill=bins_forward_fill,
        bins_interpolation=bins_interpolation,
        bins_missing=bins_missing,
    )


def worker_process_loop(
    worker_id: int,
    source_db_path: str,
    work_queue: mp.Queue[Any],
    result_queue: mp.Queue[Any],
    run_id: str,
    bin_seconds: int,
    max_forward_fill_s: int,
    stale_after_s: int,
    fill_method_mode: str,
    insert_batch_size: int,
) -> None:
    source_connection = sqlite3.connect(f"file:{Path(source_db_path).resolve()}?mode=ro", uri=True)
    try:
        while True:
            payload = work_queue.get()
            if payload is None:
                break
            profile = PairProfile(**payload)

            def _emit_rows(rows: list[tuple[Any, ...]]) -> None:
                result_queue.put(
                    {
                        "kind": "rows",
                        "rows": rows,
                    }
                )

            summary = process_pair(
                source_connection=source_connection,
                profile=profile,
                run_id=run_id,
                bin_seconds=bin_seconds,
                max_forward_fill_s=max_forward_fill_s,
                stale_after_s=stale_after_s,
                fill_method_mode=fill_method_mode,
                emit_batch_size=insert_batch_size,
                emit_rows=_emit_rows,
            )
            result_queue.put(
                {
                    "kind": "summary",
                    "summary": asdict(summary),
                }
            )
        result_queue.put(
            {
                "kind": "worker_done",
                "worker_id": worker_id,
            }
        )
    except Exception as exc:  # noqa: BLE001
        result_queue.put(
            {
                "kind": "worker_error",
                "worker_id": worker_id,
                "error": repr(exc),
            }
        )
    finally:
        source_connection.close()


def run_pairs_parallel(
    *,
    source_db_path: Path,
    output_connection: sqlite3.Connection,
    pair_profiles: list[PairProfile],
    workers: int,
    worker_queue_size: int,
    run_id: str,
    bin_seconds: int,
    max_forward_fill_s: int,
    stale_after_s: int,
    fill_method_mode: str,
    insert_batch_size: int,
) -> list[PairSummary]:
    ctx = mp.get_context("spawn")
    work_queue: mp.Queue[Any] = ctx.Queue()
    if worker_queue_size > 0:
        result_queue: mp.Queue[Any] = ctx.Queue(maxsize=worker_queue_size)
    else:
        result_queue = ctx.Queue()

    processes: list[mp.Process] = []
    effective_workers = min(workers, max(1, len(pair_profiles)))
    for worker_id in range(effective_workers):
        process = ctx.Process(
            target=worker_process_loop,
            args=(
                worker_id,
                str(source_db_path),
                work_queue,
                result_queue,
                run_id,
                bin_seconds,
                max_forward_fill_s,
                stale_after_s,
                fill_method_mode,
                insert_batch_size,
            ),
            name=f"cleansing-worker-{worker_id}",
        )
        process.start()
        processes.append(process)

    for profile in pair_profiles:
        work_queue.put(asdict(profile))
    for _ in range(effective_workers):
        work_queue.put(None)

    summaries: list[PairSummary] = []
    completed_workers = 0
    while completed_workers < effective_workers:
        message = result_queue.get()
        kind = message.get("kind")
        if kind == "rows":
            rows = message.get("rows", [])
            if rows:
                insert_rows_chunk(output_connection, rows)
            continue
        if kind == "summary":
            summary_payload = message.get("summary", {})
            summaries.append(PairSummary(**summary_payload))
            continue
        if kind == "worker_done":
            completed_workers += 1
            continue
        if kind == "worker_error":
            worker_id = message.get("worker_id", "?")
            error = message.get("error", "unknown worker error")
            for process in processes:
                if process.is_alive():
                    process.terminate()
            for process in processes:
                process.join(timeout=5)
            raise SystemExit(f"Worker {worker_id} failed: {error}")
        raise SystemExit(f"Unexpected worker message: {message!r}")

    for process in processes:
        process.join()
        if process.exitcode not in (0, None):
            raise SystemExit(
                f"Worker process exited with non-zero code: pid={process.pid}, exitcode={process.exitcode}"
            )

    return summaries


def insert_pair_summaries(connection: sqlite3.Connection, run_id: str, summaries: list[PairSummary]) -> None:
    if not summaries:
        return
    payload = [
        (
            run_id,
            item.exchange_id,
            item.symbol,
            item.source_tick_count,
            item.bins_total,
            item.bins_observed,
            item.bins_forward_fill,
            item.bins_interpolation,
            item.bins_missing,
        )
        for item in summaries
    ]
    connection.executemany(
        """
        INSERT INTO cleansing_pair_summary (
            run_id,
            exchange_id,
            symbol,
            source_tick_count,
            bins_total,
            bins_observed,
            bins_forward_fill,
            bins_interpolation,
            bins_missing
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    connection.commit()


def insert_run_metadata(
    connection: sqlite3.Connection,
    *,
    run_id: str,
    source_db_path: Path,
    output_db_path: Path,
    workers: int,
    bin_seconds: int,
    max_forward_fill_s: int,
    stale_after_s: int,
    fill_method_mode: str,
    dq_enabled: bool,
    dq_outlier_threshold_pct: float,
    exchanges_filter: list[str],
    symbols_filter: list[str],
    summaries: list[PairSummary],
    runtime_seconds: float,
) -> None:
    total_rows = sum(item.bins_total for item in summaries)
    observed_rows = sum(item.bins_observed for item in summaries)
    forward_fill_rows = sum(item.bins_forward_fill for item in summaries)
    interpolation_rows = sum(item.bins_interpolation for item in summaries)
    missing_rows = sum(item.bins_missing for item in summaries)
    connection.execute(
        """
        INSERT OR REPLACE INTO cleansing_run_metadata (
            run_id,
            created_utc,
            source_db_path,
            output_db_path,
            workers,
            bin_seconds,
            max_forward_fill_s,
            stale_after_s,
            fill_method_mode,
            dq_enabled,
            dq_outlier_threshold_pct,
            exchange_filter_csv,
            symbol_filter_csv,
            total_pairs,
            total_rows,
            observed_rows,
            forward_fill_rows,
            interpolation_rows,
            missing_rows,
            runtime_seconds
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_id,
            utc_now_iso(),
            str(source_db_path),
            str(output_db_path),
            workers,
            bin_seconds,
            max_forward_fill_s,
            stale_after_s,
            fill_method_mode,
            1 if dq_enabled else 0,
            dq_outlier_threshold_pct,
            ",".join(exchanges_filter),
            ",".join(symbols_filter),
            len(summaries),
            total_rows,
            observed_rows,
            forward_fill_rows,
            interpolation_rows,
            missing_rows,
            runtime_seconds,
        ),
    )
    connection.commit()


def main() -> None:
    args = parse_args()
    if args.bin_seconds <= 0:
        raise SystemExit("--bin-seconds must be > 0.")
    if args.max_forward_fill_s < 0:
        raise SystemExit("--max-forward-fill-s must be >= 0.")
    if args.stale_after_s < 0:
        raise SystemExit("--stale-after-s must be >= 0.")
    if args.insert_batch_size <= 0:
        raise SystemExit("--insert-batch-size must be > 0.")
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0.")
    if args.worker_queue_size < 0:
        raise SystemExit("--worker-queue-size must be >= 0.")
    if args.dq_outlier_threshold_pct <= 0:
        raise SystemExit("--dq-outlier-threshold-pct must be > 0.")

    source_db_path = Path(args.input_db)
    if not source_db_path.exists():
        raise SystemExit(f"Input DB not found: {source_db_path}")

    output_db_path = Path(args.output_db) if args.output_db else default_output_path(source_db_path, args.bin_seconds)
    output_db_path.parent.mkdir(parents=True, exist_ok=True)

    default_json_output_path = default_json_path(output_db_path)
    log_file_path = Path(args.log_file) if args.log_file else default_log_path_from_output_db(output_db_path)
    configure_logging(args.log_level, log_file_path)
    LOGGER.info("File logging enabled: %s", log_file_path)

    exchanges_filter = normalize_csv_values(args.exchanges, to_lower=True)
    symbols_filter = normalize_csv_values(args.symbols, to_lower=False)

    run_id = f"cleansing_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}_{args.bin_seconds}s"
    started = time.monotonic()

    source_connection = sqlite3.connect(f"file:{source_db_path.resolve()}?mode=ro", uri=True)
    output_connection = sqlite3.connect(str(output_db_path))
    try:
        assert_input_schema(source_connection)
        ensure_output_schema(output_connection)

        scope_where_clause, scope_where_params = build_scope_filter_clause(exchanges_filter, symbols_filter)
        dq_summaries: list[DQSummary] = []
        if args.enable_dq:
            dq_summaries = collect_source_dq_summaries(
                connection=source_connection,
                scope_where_clause=scope_where_clause,
                scope_params=scope_where_params,
            )
            LOGGER.info("DQ scope pairs discovered: %s", len(dq_summaries))

        where_clause, where_params = build_pair_filter_clause(exchanges_filter, symbols_filter)
        pair_profiles = load_pair_profiles(
            connection=source_connection,
            where_clause=where_clause,
            params=where_params,
            pair_limit=args.pair_limit,
        )
        if not pair_profiles:
            LOGGER.warning("No exchange/symbol pairs found after filters. Nothing to process.")
            insert_run_metadata(
                connection=output_connection,
                run_id=run_id,
                source_db_path=source_db_path,
                output_db_path=output_db_path,
                workers=args.workers,
                bin_seconds=args.bin_seconds,
                max_forward_fill_s=args.max_forward_fill_s,
                stale_after_s=args.stale_after_s,
                fill_method_mode=args.fill_method,
                dq_enabled=args.enable_dq,
                dq_outlier_threshold_pct=args.dq_outlier_threshold_pct,
                exchanges_filter=exchanges_filter,
                symbols_filter=symbols_filter,
                summaries=[],
                runtime_seconds=time.monotonic() - started,
            )
            if args.enable_dq:
                finalize_dq_issue_counters(dq_summaries)
                insert_dq_summary(output_connection, run_id, dq_summaries)
                insert_dq_run_summary(output_connection, run_id, dq_summaries)
            json_output_path = write_run_json_export(
                connection=output_connection,
                output_db_path=output_db_path,
                run_id=run_id,
            )
            LOGGER.info("Run JSON export: %s", json_output_path)
            return

        LOGGER.info("Pairs selected for cleansing: %s", len(pair_profiles))
        summaries: list[PairSummary]
        if args.workers == 1:
            summaries = []
            for index, profile in enumerate(pair_profiles, start=1):
                LOGGER.info(
                    "Processing pair %s/%s: %s | source_ticks=%s | min_epoch=%s | max_epoch=%s",
                    index,
                    len(pair_profiles),
                    f"{profile.exchange_id}:{profile.symbol}",
                    profile.source_tick_count,
                    profile.min_epoch_s,
                    profile.max_epoch_s,
                )

                def _emit_rows_sequential(rows: list[tuple[Any, ...]]) -> None:
                    insert_rows_chunk(output_connection, rows)

                summary = process_pair(
                    source_connection=source_connection,
                    profile=profile,
                    run_id=run_id,
                    bin_seconds=args.bin_seconds,
                    max_forward_fill_s=args.max_forward_fill_s,
                    stale_after_s=args.stale_after_s,
                    fill_method_mode=args.fill_method,
                    emit_batch_size=args.insert_batch_size,
                    emit_rows=_emit_rows_sequential,
                )
                summaries.append(summary)
        else:
            LOGGER.info(
                "Running parallel pair processing with %s workers and single-writer queue model.",
                args.workers,
            )
            summaries = run_pairs_parallel(
                source_db_path=source_db_path,
                output_connection=output_connection,
                pair_profiles=pair_profiles,
                workers=args.workers,
                worker_queue_size=args.worker_queue_size,
                run_id=run_id,
                bin_seconds=args.bin_seconds,
                max_forward_fill_s=args.max_forward_fill_s,
                stale_after_s=args.stale_after_s,
                fill_method_mode=args.fill_method,
                insert_batch_size=args.insert_batch_size,
            )

        insert_pair_summaries(output_connection, run_id, summaries)

        if args.enable_dq:
            outlier_counts = collect_outlier_counts_from_cleansed(
                connection=output_connection,
                run_id=run_id,
                outlier_threshold_pct=args.dq_outlier_threshold_pct,
            )
            if outlier_counts:
                dq_map: dict[tuple[str, str], DQSummary] = {
                    (item.exchange_id, item.symbol): item for item in dq_summaries
                }
                for key, count in outlier_counts.items():
                    if key in dq_map:
                        dq_map[key].outlier_return_count = count
            finalize_dq_issue_counters(dq_summaries)
            insert_dq_summary(output_connection, run_id, dq_summaries)
            insert_dq_run_summary(output_connection, run_id, dq_summaries)

        runtime_seconds = time.monotonic() - started
        insert_run_metadata(
            connection=output_connection,
            run_id=run_id,
            source_db_path=source_db_path,
            output_db_path=output_db_path,
            workers=args.workers,
            bin_seconds=args.bin_seconds,
            max_forward_fill_s=args.max_forward_fill_s,
            stale_after_s=args.stale_after_s,
            fill_method_mode=args.fill_method,
            dq_enabled=args.enable_dq,
            dq_outlier_threshold_pct=args.dq_outlier_threshold_pct,
            exchanges_filter=exchanges_filter,
            symbols_filter=symbols_filter,
            summaries=summaries,
            runtime_seconds=runtime_seconds,
        )
        json_output_path = write_run_json_export(
            connection=output_connection,
            output_db_path=output_db_path,
            run_id=run_id,
        )

        total_rows = sum(item.bins_total for item in summaries)
        observed_rows = sum(item.bins_observed for item in summaries)
        forward_fill_rows = sum(item.bins_forward_fill for item in summaries)
        interpolation_rows = sum(item.bins_interpolation for item in summaries)
        missing_rows = sum(item.bins_missing for item in summaries)
        LOGGER.info("Cleansing complete.")
        LOGGER.info("Run ID: %s", run_id)
        LOGGER.info("Output DB: %s", output_db_path)
        LOGGER.info("Fill method mode: %s", args.fill_method)
        LOGGER.info("DQ enabled: %s", "yes" if args.enable_dq else "no")
        LOGGER.info("Total rows: %s", total_rows)
        LOGGER.info("Run JSON export: %s", json_output_path)
        LOGGER.info(
            "Row mix | observed=%s | forward_fill=%s | interpolation=%s | missing=%s",
            observed_rows,
            forward_fill_rows,
            interpolation_rows,
            missing_rows,
        )
        if args.enable_dq:
            total_dq_issue_counter_sum = sum(item.issue_counter_sum for item in dq_summaries)
            total_negative_rows = sum(item.rows_with_negative_values_count for item in dq_summaries)
            total_outliers = sum(item.outlier_return_count for item in dq_summaries)
            LOGGER.info(
                "DQ summary | issue_counter_sum=%s | negative_rows=%s | outlier_returns=%s | threshold=%.4f",
                total_dq_issue_counter_sum,
                total_negative_rows,
                total_outliers,
                args.dq_outlier_threshold_pct,
            )
        LOGGER.info("Runtime seconds: %.2f", runtime_seconds)
    finally:
        source_connection.close()
        output_connection.close()


if __name__ == "__main__":
    try:
        mp.freeze_support()
        main()
    except KeyboardInterrupt:
        sys.exit(130)
