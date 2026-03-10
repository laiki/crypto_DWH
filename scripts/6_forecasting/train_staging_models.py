#!/usr/bin/env python
"""
Train regression models from staging history and register model artifacts.

Pipeline:
1) Resolve one or more staging DB inputs.
2) Build per-series staging training frames (resampled to target cadence).
3) Train Ridge + HistGradientBoosting models for two horizons:
   - 1 * bin_seconds
   - N * bin_seconds (configurable multiple)
4) Evaluate with holdout plus TimeSeriesSplit.
5) Persist model artifacts and model/training metadata.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import glob
import json
import math
import multiprocessing
import re
import sqlite3
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

MISSING_DEPENDENCY_MESSAGE: str | None = None
try:
    import joblib
    import numpy as np
    import pandas as pd
    import pandas_ta as pta
    from sklearn.base import clone
    from sklearn.ensemble import HistGradientBoostingRegressor
    from sklearn.linear_model import Ridge
    from sklearn.metrics import mean_absolute_error
    from sklearn.metrics import mean_squared_error
    from sklearn.metrics import r2_score
    from sklearn.model_selection import TimeSeriesSplit
    from sklearn.pipeline import Pipeline
    from sklearn.preprocessing import StandardScaler
except Exception as exc:  # noqa: BLE001
    missing_name = getattr(exc, "name", None)
    if missing_name:
        MISSING_DEPENDENCY_MESSAGE = (
            "Missing dependency "
            f"'{missing_name}' for interpreter '{sys.executable}'. "
            "Install dependencies with: "
            f"'{sys.executable} -m pip install -r requirements.txt'"
        )
    else:
        MISSING_DEPENDENCY_MESSAGE = (
            "Failed to import forecasting runtime dependencies "
            f"for interpreter '{sys.executable}': {exc!r}"
        )


LOGGER_NAME = "train_staging_models"


@dataclass(frozen=True)
class SeriesScope:
    exchange_id: str
    symbol: str
    cleansing_rows: int


@dataclass(frozen=True)
class PairWorkerTask:
    exchange_id: str
    symbol: str
    cleansing_rows: int
    staging_db_paths: tuple[str, ...]
    training_cutoff_utc: str
    bin_seconds: int
    lag_steps: tuple[int, ...]
    rolling_windows: tuple[int, ...]
    min_train_rows: int
    test_size: float
    cv_splits: int
    ridge_alpha: float
    hgb_max_iter: int
    hgb_learning_rate: float
    horizons_steps: tuple[int, ...]
    horizons_seconds: tuple[int, ...]
    training_run_id: str
    model_dir: str


def log(level: str, message: str) -> None:
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"{timestamp} | {level} | {LOGGER_NAME} | {message}", flush=True)


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def format_hhmmss(total_seconds: float) -> str:
    safe_seconds = max(0, int(total_seconds))
    hours = safe_seconds // 3600
    minutes = (safe_seconds % 3600) // 60
    seconds = safe_seconds % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def estimate_eta_seconds(*, elapsed_seconds: float, completed_items: int, total_items: int) -> float | None:
    if completed_items <= 0 or total_items <= 0:
        return None
    rate = float(completed_items) / max(elapsed_seconds, 1e-9)
    if rate <= 0.0:
        return None
    remaining = max(0, total_items - completed_items)
    return float(remaining) / rate


def parse_int_csv(raw_value: str, *, arg_name: str) -> list[int]:
    values: list[int] = []
    for token in raw_value.split(","):
        item = token.strip()
        if not item:
            continue
        try:
            value = int(item)
        except ValueError as exc:
            raise SystemExit(f"Invalid integer in {arg_name}: {item!r}") from exc
        if value <= 0:
            raise SystemExit(f"Values in {arg_name} must be > 0, got {value}.")
        values.append(value)
    if not values:
        raise SystemExit(f"{arg_name} must contain at least one integer value.")
    return sorted(set(values))


def parse_csv_values(raw_value: str | None) -> list[str]:
    if raw_value is None:
        return []
    values: list[str] = []
    seen: set[str] = set()
    for token in raw_value.split(","):
        item = token.strip()
        if not item:
            continue
        key = item.lower()
        if key in seen:
            continue
        seen.add(key)
        values.append(item)
    return values


def split_exact_and_like_tokens(values: list[str]) -> tuple[list[str], list[str]]:
    exact_tokens: list[str] = []
    like_tokens: list[str] = []
    for value in values:
        if "%" in value or "_" in value:
            like_tokens.append(value)
        else:
            exact_tokens.append(value)
    return exact_tokens, like_tokens


def build_case_insensitive_symbol_clause(
    *,
    column_name: str,
    symbols: list[str],
) -> tuple[str, list[Any]]:
    if not symbols:
        return "", []

    exact_symbols, like_symbols = split_exact_and_like_tokens(symbols)
    clause_parts: list[str] = []
    params: list[Any] = []

    if exact_symbols:
        exact_values = [item.lower() for item in exact_symbols]
        placeholders = ",".join("?" for _ in exact_values)
        clause_parts.append(f"lower({column_name}) IN ({placeholders})")
        params.extend(exact_values)

    if like_symbols:
        like_conditions = []
        for pattern in like_symbols:
            like_conditions.append(f"lower({column_name}) LIKE ? ESCAPE '\\'")
            params.append(pattern.lower())
        clause_parts.append("(" + " OR ".join(like_conditions) + ")")

    if not clause_parts:
        return "", []
    if len(clause_parts) == 1:
        return clause_parts[0], params
    return "(" + " OR ".join(clause_parts) + ")", params


def safe_name(value: str) -> str:
    normalized = re.sub(r"[^A-Za-z0-9._-]+", "_", value.strip())
    return normalized.strip("_") or "value"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Train regression models from staging history and register "
            "model artifacts plus evaluation metadata."
        ),
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--staging-db",
        default="data/staging/latest_staging.db",
        help="Staging DB input used for leakage-safe training history reads. Accepts a single .db file, a directory, or a glob pattern.",
    )
    parser.add_argument(
        "--training-cutoff-utc",
        default=None,
        help="Optional UTC cutoff for training history. Only staging rows before this timestamp are used. If omitted, all available staging history is used.",
    )
    parser.add_argument(
        "--forecast-db",
        default="data/core/core_kpi.db",
        help="SQLite DB path where training registry tables are written.",
    )
    parser.add_argument(
        "--model-dir",
        default="data/forecasting/models",
        help="Directory for persisted model artifact files.",
    )
    parser.add_argument(
        "--bin-seconds",
        type=int,
        default=60,
        help="Training cadence in seconds used for staging resampling.",
    )
    parser.add_argument(
        "--secondary-horizon-multiple",
        type=int,
        default=30,
        help="Secondary forecast horizon as integer multiple of bin-seconds.",
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help=(
            "Optional comma-separated symbol filter for training/inference scope. "
            "Supports exact symbols and SQL LIKE patterns (%% and _), case-insensitive "
            "(for example BTC/USDT,eth/usdt,%%btc/%%)."
        ),
    )
    parser.add_argument(
        "--exchange-id",
        default=None,
        help="Optional exchange_id filter for training/inference scope.",
    )
    parser.add_argument(
        "--limit-series",
        type=int,
        default=None,
        help="Optional maximum number of exchange/symbol series to process.",
    )
    parser.add_argument(
        "--lag-steps",
        default="1,2,3,5,10,15,30",
        help="Comma-separated lag steps used as model features.",
    )
    parser.add_argument(
        "--rolling-windows",
        default="3,5,10,30",
        help="Comma-separated rolling window sizes for lagged mean/std features.",
    )
    parser.add_argument(
        "--min-train-rows",
        type=int,
        default=300,
        help="Minimum rows after feature engineering required to train a model.",
    )
    parser.add_argument(
        "--test-size",
        type=float,
        default=0.2,
        help="Holdout ratio from tail of training frame.",
    )
    parser.add_argument(
        "--cv-splits",
        type=int,
        default=5,
        help="Target number of TimeSeriesSplit folds.",
    )
    parser.add_argument(
        "--ridge-alpha",
        type=float,
        default=1.0,
        help="Ridge regularization strength.",
    )
    parser.add_argument(
        "--hgb-max-iter",
        type=int,
        default=300,
        help="HistGradientBoosting maximum iterations.",
    )
    parser.add_argument(
        "--hgb-learning-rate",
        type=float,
        default=0.05,
        help="HistGradientBoosting learning rate.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=1,
        help="Number of worker processes for per-pair training/inference tasks.",
    )
    parser.add_argument(
        "--progress",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable periodic progress and ETA logging.",
    )
    parser.add_argument(
        "--progress-interval-seconds",
        type=int,
        default=20,
        help="Minimum interval between periodic progress logs.",
    )
    parser.add_argument(
        "--log-pair-details",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log per-pair start lines.",
    )
    parser.add_argument(
        "--log-skip-details",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="Log per-skip warning lines.",
    )
    return parser.parse_args()


def validate_args(args: argparse.Namespace) -> None:
    if args.secondary_horizon_multiple <= 1:
        raise SystemExit("--secondary-horizon-multiple must be > 1.")
    if args.bin_seconds <= 0:
        raise SystemExit("--bin-seconds must be > 0.")
    if args.limit_series is not None and args.limit_series <= 0:
        raise SystemExit("--limit-series must be > 0 when set.")
    if not 0.01 <= args.test_size <= 0.5:
        raise SystemExit("--test-size must be within [0.01, 0.5].")
    if args.cv_splits <= 0:
        raise SystemExit("--cv-splits must be > 0.")
    if args.min_train_rows < 60:
        raise SystemExit("--min-train-rows must be >= 60.")
    if args.workers <= 0:
        raise SystemExit("--workers must be > 0.")
    if args.progress_interval_seconds <= 0:
        raise SystemExit("--progress-interval-seconds must be > 0.")


def resolve_file(path_raw: str) -> Path:
    candidate = Path(path_raw).expanduser()
    if candidate.is_absolute():
        path = candidate
    else:
        path = (Path.cwd() / candidate).resolve()
    return path


def resolve_staging_db_paths(path_raw: str) -> list[Path]:
    has_glob_syntax = any(token in path_raw for token in ("*", "?", "["))
    if has_glob_syntax:
        matches = sorted(Path(item).resolve() for item in glob.glob(path_raw, recursive=True))
        db_paths = [path for path in matches if path.is_file() and path.suffix == ".db"]
        if not db_paths:
            raise SystemExit(f"No staging DB files matched pattern: {path_raw}")
        return db_paths

    candidate = resolve_file(path_raw)
    if candidate.is_file():
        if candidate.suffix != ".db":
            raise SystemExit(f"Staging input is not a .db file: {candidate}")
        return [candidate]
    if candidate.is_dir():
        db_paths = sorted(path for path in candidate.glob("*.db") if path.is_file())
        if not db_paths:
            raise SystemExit(f"No staging DB files found in directory: {candidate}")
        return db_paths
    raise SystemExit(f"Staging DB input does not exist: {candidate}")


def resolve_training_cutoff_utc(*, requested_cutoff_utc: str | None, staging_max_utc: str | None) -> str:
    if requested_cutoff_utc:
        return requested_cutoff_utc
    if staging_max_utc is None:
        raise SystemExit("No staging ingestion timestamps found in market_ticks. Cannot derive training cutoff.")
    staging_max_ts = pd.to_datetime(staging_max_utc, utc=True, errors="coerce")
    if pd.isna(staging_max_ts):
        raise SystemExit(f"Could not parse staging max timestamp: {staging_max_utc}")
    return (staging_max_ts + pd.Timedelta(seconds=1)).isoformat()


def load_scope_pairs_from_staging(
    staging_db_paths: list[Path],
    *,
    exchange_id_filter: str | None,
    symbol_filters: list[str],
    training_cutoff_utc: str,
) -> list[SeriesScope]:
    cutoff_ts = pd.to_datetime(training_cutoff_utc, utc=True, errors="coerce")
    if pd.isna(cutoff_ts):
        raise SystemExit(f"Could not parse --training-cutoff-utc: {training_cutoff_utc}")
    cutoff_epoch_s = int(cutoff_ts.timestamp())
    price_expr = staging_price_sql_expr()
    aggregated_counts: dict[tuple[str, str], int] = {}

    for staging_db_path in staging_db_paths:
        connection = sqlite3.connect(f"file:{staging_db_path}?mode=ro", uri=True)
        try:
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_ticks' LIMIT 1"
            ).fetchone()
            if table_exists is None:
                continue
            conditions = [
                "ingestion_ts_epoch_s < ?",
                f"{price_expr} IS NOT NULL",
            ]
            params: list[Any] = [cutoff_epoch_s]
            if exchange_id_filter:
                conditions.append("lower(exchange_id) = ?")
                params.append(exchange_id_filter.lower())
            symbol_clause, symbol_params = build_case_insensitive_symbol_clause(
                column_name="symbol",
                symbols=symbol_filters,
            )
            if symbol_clause:
                conditions.append(symbol_clause)
                params.extend(symbol_params)

            rows = connection.execute(
                f"""
                SELECT exchange_id, symbol, COUNT(*) AS row_count
                FROM market_ticks
                WHERE {" AND ".join(conditions)}
                GROUP BY exchange_id, symbol
                """,
                params,
            ).fetchall()
        finally:
            connection.close()

        for exchange_id, symbol, row_count in rows:
            key = (str(exchange_id), str(symbol))
            aggregated_counts[key] = aggregated_counts.get(key, 0) + int(row_count)

    result = [
        SeriesScope(exchange_id=exchange_id, symbol=symbol, cleansing_rows=row_count)
        for (exchange_id, symbol), row_count in aggregated_counts.items()
    ]
    result.sort(key=lambda item: (-item.cleansing_rows, item.exchange_id, item.symbol))
    return result
def resolve_cleansing_run_id(connection: sqlite3.Connection, requested: str | None) -> str:
    if requested:
        row = connection.execute(
            "SELECT run_id FROM cleansed_market WHERE run_id = ? LIMIT 1",
            (requested,),
        ).fetchone()
        if row is None:
            raise SystemExit(f"Cleansing run_id not found: {requested}")
        return str(row[0])

    row = connection.execute(
        """
        SELECT run_id
        FROM cleansed_market
        GROUP BY run_id
        ORDER BY run_id DESC
        LIMIT 1
        """
    ).fetchone()
    if row is None:
        raise SystemExit("No cleansing run_id found in cleansed_market.")
    return str(row[0])


def resolve_training_cutoff(connection: sqlite3.Connection, cleansing_run_id: str) -> str:
    row = connection.execute(
        """
        SELECT MIN(bucket_start_utc)
        FROM cleansed_market
        WHERE run_id = ?
        """,
        (cleansing_run_id,),
    ).fetchone()
    if row is None or row[0] is None:
        raise SystemExit(f"No cleansing timestamps found for run_id={cleansing_run_id}.")
    return str(row[0])


def derive_bin_seconds(connection: sqlite3.Connection, cleansing_run_id: str) -> int | None:
    df = pd.read_sql_query(
        """
        SELECT DISTINCT bucket_epoch_s
        FROM cleansed_market
        WHERE run_id = ?
        ORDER BY bucket_epoch_s ASC
        """,
        connection,
        params=(cleansing_run_id,),
    )
    if df.empty:
        return None
    epochs = pd.to_numeric(df["bucket_epoch_s"], errors="coerce").dropna().astype(int).to_numpy()
    if len(epochs) < 2:
        return None
    deltas = np.diff(epochs)
    positive = [int(delta) for delta in deltas if int(delta) > 0]
    if not positive:
        return None
    counts = Counter(positive)
    best_count = max(counts.values())
    winners = sorted(delta for delta, count in counts.items() if count == best_count)
    return int(winners[0])


def load_scope_pairs(
    connection: sqlite3.Connection,
    *,
    cleansing_run_id: str,
    exchange_id_filter: str | None,
    symbol_filters: list[str],
) -> list[SeriesScope]:
    conditions = [
        "run_id = ?",
        "price IS NOT NULL",
        "is_missing = 0",
        "is_stale = 0",
    ]
    params: list[Any] = [cleansing_run_id]
    if exchange_id_filter:
        conditions.append("exchange_id = ?")
        params.append(exchange_id_filter)
    symbol_clause, symbol_params = build_case_insensitive_symbol_clause(
        column_name="symbol",
        symbols=symbol_filters,
    )
    if symbol_clause:
        conditions.append(symbol_clause)
        params.extend(symbol_params)

    rows = connection.execute(
        f"""
        SELECT
            exchange_id,
            symbol,
            COUNT(*) AS row_count
        FROM cleansed_market
        WHERE {" AND ".join(conditions)}
        GROUP BY exchange_id, symbol
        ORDER BY row_count DESC, exchange_id ASC, symbol ASC
        """,
        params,
    ).fetchall()
    result: list[SeriesScope] = []
    for row in rows:
        result.append(
            SeriesScope(
                exchange_id=str(row[0]),
                symbol=str(row[1]),
                cleansing_rows=int(row[2]),
            )
        )
    return result


def staging_price_sql_expr() -> str:
    return (
        "COALESCE("
        "price, "
        "last, "
        "CASE WHEN bid IS NOT NULL AND ask IS NOT NULL THEN (bid + ask) / 2.0 ELSE NULL END"
        ")"
    )


def load_staging_series_before_cutoff_single(
    staging_db_path: Path,
    *,
    exchange_id: str,
    symbol: str,
    cutoff_utc: str | None,
) -> pd.DataFrame:
    cutoff_epoch_s: int | None = None
    if cutoff_utc is not None:
        cutoff_ts = pd.to_datetime(cutoff_utc, utc=True, errors="coerce")
        if pd.isna(cutoff_ts):
            return pd.DataFrame(columns=["ingestion_ts_utc", "price_value"])
        cutoff_epoch_s = int(cutoff_ts.timestamp())
    exchange_norm = exchange_id.lower()
    symbol_norm = symbol.lower()

    price_expr = staging_price_sql_expr()
    conditions = [
        "exchange_id_norm = ?",
        "symbol_norm = ?",
        f"{price_expr} IS NOT NULL",
    ]
    params: list[Any] = [exchange_norm, symbol_norm]
    if cutoff_epoch_s is not None:
        conditions.append("ingestion_ts_epoch_s < ?")
        params.append(cutoff_epoch_s)
    sql = f"""
        SELECT
            ingestion_ts_utc,
            {price_expr} AS price_value
        FROM market_ticks
        WHERE {" AND ".join(conditions)}
        ORDER BY ingestion_ts_epoch_s ASC
    """
    connection = sqlite3.connect(f"file:{staging_db_path}?mode=ro", uri=True)
    try:
        table_exists = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_ticks' LIMIT 1"
        ).fetchone()
        if table_exists is None:
            return pd.DataFrame(columns=["ingestion_ts_utc", "price_value"])
        merged = pd.read_sql_query(sql, connection, params=params)
    finally:
        connection.close()

    if merged.empty:
        return pd.DataFrame(columns=["ingestion_ts_utc", "price_value"])
    merged["ingestion_ts_utc"] = pd.to_datetime(merged["ingestion_ts_utc"], utc=True, errors="coerce")
    merged["price_value"] = pd.to_numeric(merged["price_value"], errors="coerce")
    merged = merged.dropna(subset=["ingestion_ts_utc", "price_value"])
    merged = merged.sort_values(by=["ingestion_ts_utc"], ascending=[True])
    merged = merged.drop_duplicates(subset=["ingestion_ts_utc"], keep="last")
    return merged.reset_index(drop=True)


def load_staging_series_before_cutoff(
    staging_db_paths: list[Path],
    *,
    exchange_id: str,
    symbol: str,
    cutoff_utc: str | None,
) -> pd.DataFrame:
    frames = [
        load_staging_series_before_cutoff_single(
            staging_db_path,
            exchange_id=exchange_id,
            symbol=symbol,
            cutoff_utc=cutoff_utc,
        )
        for staging_db_path in staging_db_paths
    ]
    non_empty_frames = [frame for frame in frames if not frame.empty]
    if not non_empty_frames:
        return pd.DataFrame(columns=["ingestion_ts_utc", "price_value"])
    merged = pd.concat(non_empty_frames, ignore_index=True)
    merged["ingestion_ts_utc"] = pd.to_datetime(merged["ingestion_ts_utc"], utc=True, errors="coerce")
    merged["price_value"] = pd.to_numeric(merged["price_value"], errors="coerce")
    merged = merged.dropna(subset=["ingestion_ts_utc", "price_value"])
    merged = merged.sort_values(by=["ingestion_ts_utc"], ascending=[True])
    merged = merged.drop_duplicates(subset=["ingestion_ts_utc"], keep="last")
    return merged.reset_index(drop=True)


def summarize_staging_time_window(staging_db_paths: list[Path]) -> tuple[str | None, str | None]:
    min_epoch_values: list[int] = []
    max_epoch_values: list[int] = []
    for staging_db_path in staging_db_paths:
        connection = sqlite3.connect(f"file:{staging_db_path}?mode=ro", uri=True)
        try:
            table_exists = connection.execute(
                "SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_ticks' LIMIT 1"
            ).fetchone()
            if table_exists is None:
                continue
            row = connection.execute(
                """
                SELECT MIN(ingestion_ts_epoch_s), MAX(ingestion_ts_epoch_s)
                FROM market_ticks
                """
            ).fetchone()
        finally:
            connection.close()
        if row is None:
            continue
        if row[0] is not None:
            min_epoch_values.append(int(row[0]))
        if row[1] is not None:
            max_epoch_values.append(int(row[1]))

    min_utc = (
        datetime.fromtimestamp(min(min_epoch_values), tz=timezone.utc).isoformat(timespec="seconds")
        if min_epoch_values
        else None
    )
    max_utc = (
        datetime.fromtimestamp(max(max_epoch_values), tz=timezone.utc).isoformat(timespec="seconds")
        if max_epoch_values
        else None
    )
    return min_utc, max_utc


def resample_staging_series(staging_series: pd.DataFrame, *, bin_seconds: int) -> pd.DataFrame:
    if staging_series.empty:
        return pd.DataFrame(columns=["bucket_start_utc", "bucket_epoch_s", "price"])
    indexed = staging_series.set_index("ingestion_ts_utc").sort_index()
    resampled = indexed["price_value"].resample(f"{bin_seconds}s").last().dropna()
    if resampled.empty:
        return pd.DataFrame(columns=["bucket_start_utc", "bucket_epoch_s", "price"])
    output = resampled.reset_index().rename(columns={"ingestion_ts_utc": "bucket_start_utc", "price_value": "price"})
    output["bucket_epoch_s"] = (output["bucket_start_utc"].astype("int64") // 10**9).astype(int)
    output["price"] = pd.to_numeric(output["price"], errors="coerce")
    output = output.dropna(subset=["price"])
    output = output[output["price"] > 0.0]
    return output.reset_index(drop=True)


def load_cleansing_series(
    connection: sqlite3.Connection,
    *,
    cleansing_run_id: str,
    exchange_id: str,
    symbol: str,
) -> pd.DataFrame:
    df = pd.read_sql_query(
        """
        SELECT
            bucket_start_utc,
            bucket_epoch_s,
            price
        FROM cleansed_market
        WHERE run_id = :run_id
          AND exchange_id = :exchange_id
          AND symbol = :symbol
          AND price IS NOT NULL
          AND is_missing = 0
          AND is_stale = 0
        ORDER BY bucket_epoch_s ASC
        """,
        connection,
        params={
            "run_id": cleansing_run_id,
            "exchange_id": exchange_id,
            "symbol": symbol,
        },
    )
    if df.empty:
        return df
    df["bucket_start_utc"] = pd.to_datetime(df["bucket_start_utc"], utc=True, errors="coerce")
    df["bucket_epoch_s"] = pd.to_numeric(df["bucket_epoch_s"], errors="coerce")
    df["price"] = pd.to_numeric(df["price"], errors="coerce")
    df = df.dropna(subset=["bucket_start_utc", "bucket_epoch_s", "price"]).copy()
    df["bucket_epoch_s"] = df["bucket_epoch_s"].astype(int)
    df = df[df["price"] > 0.0].copy()
    df = df.sort_values(by=["bucket_epoch_s"], ascending=[True]).drop_duplicates(
        subset=["bucket_epoch_s"], keep="last"
    )
    return df.reset_index(drop=True)


def build_feature_table(base_series: pd.DataFrame, *, lag_steps: list[int], rolling_windows: list[int]) -> pd.DataFrame:
    working = base_series.copy()
    working = working.sort_values(by=["bucket_epoch_s"], ascending=[True]).reset_index(drop=True)
    if working.empty:
        return working

    minute_of_day = (
        working["bucket_start_utc"].dt.hour.astype(float) * 60.0 + working["bucket_start_utc"].dt.minute.astype(float)
    )
    working["tod_sin"] = np.sin(2.0 * np.pi * minute_of_day / 1440.0)
    working["tod_cos"] = np.cos(2.0 * np.pi * minute_of_day / 1440.0)
    working["dow_sin"] = np.sin(2.0 * np.pi * working["bucket_start_utc"].dt.dayofweek.astype(float) / 7.0)
    working["dow_cos"] = np.cos(2.0 * np.pi * working["bucket_start_utc"].dt.dayofweek.astype(float) / 7.0)

    for lag in lag_steps:
        working[f"lag_{lag}"] = working["price"].shift(lag)

    lagged_price = working["price"].shift(1)
    for window in rolling_windows:
        working[f"roll_mean_{window}"] = lagged_price.rolling(window=window).mean()
        working[f"roll_std_{window}"] = lagged_price.rolling(window=window).std()
        working[f"ta_sma_{window}"] = pta.sma(lagged_price, length=window)
        working[f"ta_ema_{window}"] = pta.ema(lagged_price, length=window)

    working["ta_rsi_14"] = pta.rsi(lagged_price, length=14)
    working["ta_roc_10"] = pta.roc(lagged_price, length=10)
    bbands = pta.bbands(lagged_price, length=20, std=2.0)
    if bbands is not None and not bbands.empty:
        lower_col = next((column for column in bbands.columns if column.startswith("BBL_")), None)
        middle_col = next((column for column in bbands.columns if column.startswith("BBM_")), None)
        upper_col = next((column for column in bbands.columns if column.startswith("BBU_")), None)
        bandwidth_col = next((column for column in bbands.columns if column.startswith("BBB_")), None)
        if lower_col:
            working["ta_bbands_lower_20_2"] = bbands[lower_col]
        if middle_col:
            working["ta_bbands_middle_20_2"] = bbands[middle_col]
        if upper_col:
            working["ta_bbands_upper_20_2"] = bbands[upper_col]
        if bandwidth_col:
            working["ta_bbands_bandwidth_20_2"] = bbands[bandwidth_col]

    if {1, 5}.issubset(set(lag_steps)):
        working["momentum_lag1_lag5"] = working["lag_1"] - working["lag_5"]
    if {1, 10}.issubset(set(lag_steps)):
        working["momentum_lag1_lag10"] = working["lag_1"] - working["lag_10"]

    return working


def feature_columns_from_table(feature_table: pd.DataFrame) -> list[str]:
    excluded = {"bucket_start_utc", "bucket_epoch_s", "price"}
    return [column for column in feature_table.columns if column not in excluded]


def safe_mape(y_true: np.ndarray, y_pred: np.ndarray, *, epsilon: float = 1e-9) -> float | None:
    mask = np.abs(y_true) > epsilon
    if not np.any(mask):
        return None
    ratio = np.abs((y_true[mask] - y_pred[mask]) / y_true[mask])
    return float(np.mean(ratio) * 100.0)


def compute_metrics(y_true: np.ndarray, y_pred: np.ndarray) -> dict[str, float | None]:
    mae = float(mean_absolute_error(y_true, y_pred))
    rmse = float(math.sqrt(mean_squared_error(y_true, y_pred)))
    mape = safe_mape(y_true, y_pred)
    r2 = float(r2_score(y_true, y_pred))
    return {"mae": mae, "rmse": rmse, "mape": mape, "r2": r2}


def metric_mean(metric_rows: list[dict[str, float | None]], name: str) -> float | None:
    values = [row[name] for row in metric_rows if row[name] is not None]
    if not values:
        return None
    return float(np.mean(values))


def evaluate_time_series_cv(
    model: Any,
    X_train: pd.DataFrame,
    y_train: pd.Series,
    requested_splits: int,
) -> tuple[int, list[dict[str, float | None]], dict[str, float | None]]:
    max_possible_splits = len(X_train) - 1
    effective_splits = min(requested_splits, max_possible_splits)
    if effective_splits < 2:
        return 0, [], {"mae": None, "rmse": None, "mape": None, "r2": None}

    splitter = TimeSeriesSplit(n_splits=effective_splits)
    fold_metrics: list[dict[str, float | None]] = []
    for fold_index, (fit_idx, valid_idx) in enumerate(splitter.split(X_train), start=1):
        fold_model = clone(model)
        fold_model.fit(X_train.iloc[fit_idx], y_train.iloc[fit_idx])
        pred = fold_model.predict(X_train.iloc[valid_idx]).astype(float)
        metrics = compute_metrics(y_train.iloc[valid_idx].to_numpy(dtype=float), pred)
        metrics["fold_index"] = float(fold_index)
        fold_metrics.append(metrics)

    mean_metrics = {
        "mae": metric_mean(fold_metrics, "mae"),
        "rmse": metric_mean(fold_metrics, "rmse"),
        "mape": metric_mean(fold_metrics, "mape"),
        "r2": metric_mean(fold_metrics, "r2"),
    }
    return effective_splits, fold_metrics, mean_metrics


def create_models_from_hyperparams(
    *,
    ridge_alpha: float,
    hgb_max_iter: int,
    hgb_learning_rate: float,
) -> dict[str, Any]:
    ridge = Pipeline(
        steps=[
            ("scaler", StandardScaler()),
            ("model", Ridge(alpha=ridge_alpha)),
        ]
    )
    hgb = HistGradientBoostingRegressor(
        loss="squared_error",
        learning_rate=hgb_learning_rate,
        max_iter=hgb_max_iter,
        max_depth=6,
        min_samples_leaf=20,
        random_state=42,
    )
    return {
        "ridge": ridge,
        "hist_gradient_boosting": hgb,
    }


def create_models(args: argparse.Namespace) -> dict[str, Any]:
    return create_models_from_hyperparams(
        ridge_alpha=float(args.ridge_alpha),
        hgb_max_iter=int(args.hgb_max_iter),
        hgb_learning_rate=float(args.hgb_learning_rate),
    )


def ensure_forecast_tables(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS forecast_training_runs (
            training_run_id TEXT PRIMARY KEY,
            started_utc TEXT NOT NULL,
            finished_utc TEXT,
            status TEXT NOT NULL,
            staging_glob TEXT NOT NULL,
            staging_db_count INTEGER NOT NULL,
            cleansing_db_path TEXT NOT NULL,
            cleansing_run_id TEXT NOT NULL,
            forecast_db_path TEXT NOT NULL,
            training_cutoff_utc TEXT NOT NULL,
            bin_seconds INTEGER NOT NULL,
            secondary_horizon_multiple INTEGER NOT NULL,
            lag_steps_csv TEXT NOT NULL,
            rolling_windows_csv TEXT NOT NULL,
            series_total INTEGER NOT NULL DEFAULT 0,
            series_trained INTEGER NOT NULL DEFAULT 0,
            models_trained INTEGER NOT NULL DEFAULT 0,
            predictions_written INTEGER NOT NULL DEFAULT 0,
            notes_json TEXT
        );

        CREATE TABLE IF NOT EXISTS forecast_model_registry (
            model_id INTEGER PRIMARY KEY AUTOINCREMENT,
            training_run_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            model_name TEXT NOT NULL,
            horizon_steps INTEGER NOT NULL,
            horizon_seconds INTEGER NOT NULL,
            artifact_path TEXT NOT NULL,
            feature_config_json TEXT NOT NULL,
            train_rows INTEGER NOT NULL,
            test_rows INTEGER NOT NULL,
            cv_fold_count INTEGER NOT NULL,
            holdout_mae REAL,
            holdout_rmse REAL,
            holdout_mape REAL,
            holdout_r2 REAL,
            cv_mean_mae REAL,
            cv_mean_rmse REAL,
            cv_mean_mape REAL,
            cv_mean_r2 REAL,
            created_utc TEXT NOT NULL,
            status TEXT NOT NULL,
            error_message TEXT,
            FOREIGN KEY (training_run_id) REFERENCES forecast_training_runs(training_run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_forecast_model_registry_scope
            ON forecast_model_registry(training_run_id, exchange_id, symbol, model_name, horizon_seconds);

        CREATE TABLE IF NOT EXISTS forecast_predictions (
            prediction_id INTEGER PRIMARY KEY AUTOINCREMENT,
            model_id INTEGER NOT NULL,
            training_run_id TEXT NOT NULL,
            cleansing_run_id TEXT NOT NULL,
            exchange_id TEXT NOT NULL,
            symbol TEXT NOT NULL,
            model_name TEXT NOT NULL,
            horizon_seconds INTEGER NOT NULL,
            forecast_generated_bucket_start_utc TEXT NOT NULL,
            forecast_generated_bucket_epoch_s INTEGER NOT NULL,
            target_bucket_start_utc TEXT NOT NULL,
            target_bucket_epoch_s INTEGER NOT NULL,
            predicted_price REAL NOT NULL,
            actual_price REAL,
            abs_error REAL,
            generated_at_utc TEXT NOT NULL,
            FOREIGN KEY (model_id) REFERENCES forecast_model_registry(model_id),
            FOREIGN KEY (training_run_id) REFERENCES forecast_training_runs(training_run_id)
        );

        CREATE INDEX IF NOT EXISTS idx_forecast_predictions_dashboard
            ON forecast_predictions(cleansing_run_id, exchange_id, symbol, model_name, horizon_seconds, target_bucket_epoch_s);

        CREATE VIEW IF NOT EXISTS model_artifacts AS
        SELECT
            model_id,
            training_run_id,
            exchange_id,
            symbol,
            model_name,
            horizon_steps,
            horizon_seconds,
            artifact_path,
            feature_config_json,
            train_rows,
            test_rows,
            holdout_r2 AS test_score_r2,
            holdout_mae,
            holdout_rmse,
            holdout_mape,
            cv_mean_r2,
            cv_mean_mae,
            cv_mean_rmse,
            cv_mean_mape,
            created_utc,
            status,
            error_message
        FROM forecast_model_registry;
        """
    )
    existing_columns = {
        str(row[1]) for row in connection.execute("PRAGMA table_info('forecast_model_registry')").fetchall()
    }
    if "holdout_r2" not in existing_columns:
        connection.execute("ALTER TABLE forecast_model_registry ADD COLUMN holdout_r2 REAL")
    if "cv_mean_r2" not in existing_columns:
        connection.execute("ALTER TABLE forecast_model_registry ADD COLUMN cv_mean_r2 REAL")
    connection.execute("DROP VIEW IF EXISTS model_artifacts")
    connection.execute(
        """
        CREATE VIEW model_artifacts AS
        SELECT
            model_id,
            training_run_id,
            exchange_id,
            symbol,
            model_name,
            horizon_steps,
            horizon_seconds,
            artifact_path,
            feature_config_json,
            train_rows,
            test_rows,
            holdout_r2 AS test_score_r2,
            holdout_mae,
            holdout_rmse,
            holdout_mape,
            cv_mean_r2,
            cv_mean_mae,
            cv_mean_rmse,
            cv_mean_mape,
            created_utc,
            status,
            error_message
        FROM forecast_model_registry
        """
    )
    connection.commit()


def insert_training_run_start(
    connection: sqlite3.Connection,
    *,
    training_run_id: str,
    args: argparse.Namespace,
    staging_db_count: int,
    staging_input: str,
    cleansing_db_path: Path,
    cleansing_run_id: str,
    forecast_db_path: Path,
    training_cutoff_utc: str,
    bin_seconds: int,
    lag_steps: list[int],
    rolling_windows: list[int],
    series_total: int,
) -> None:
    connection.execute(
        """
        INSERT INTO forecast_training_runs (
            training_run_id,
            started_utc,
            status,
            staging_glob,
            staging_db_count,
            cleansing_db_path,
            cleansing_run_id,
            forecast_db_path,
            training_cutoff_utc,
            bin_seconds,
            secondary_horizon_multiple,
            lag_steps_csv,
            rolling_windows_csv,
            series_total
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            training_run_id,
            utc_now_iso(),
            "running",
            staging_input,
            staging_db_count,
            str(cleansing_db_path),
            cleansing_run_id,
            str(forecast_db_path),
            training_cutoff_utc,
            int(bin_seconds),
            int(args.secondary_horizon_multiple),
            ",".join(str(value) for value in lag_steps),
            ",".join(str(value) for value in rolling_windows),
            int(series_total),
        ),
    )
    connection.commit()


def update_training_run_finish(
    connection: sqlite3.Connection,
    *,
    training_run_id: str,
    status: str,
    series_trained: int,
    models_trained: int,
    predictions_written: int,
    notes: dict[str, Any],
) -> None:
    connection.execute(
        """
        UPDATE forecast_training_runs
        SET
            finished_utc = ?,
            status = ?,
            series_trained = ?,
            models_trained = ?,
            predictions_written = ?,
            notes_json = ?
        WHERE training_run_id = ?
        """,
        (
            utc_now_iso(),
            status,
            int(series_trained),
            int(models_trained),
            int(predictions_written),
            json.dumps(notes, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            training_run_id,
        ),
    )
    connection.commit()


def make_model_artifact_path(
    model_dir: Path,
    *,
    training_run_id: str,
    exchange_id: str,
    symbol: str,
    model_name: str,
    horizon_seconds: int,
) -> Path:
    safe_exchange = safe_name(exchange_id)
    safe_symbol = safe_name(symbol)
    filename = f"{safe_name(model_name)}_h{horizon_seconds}s.joblib"
    model_path = model_dir / training_run_id / safe_exchange / safe_symbol / filename
    model_path.parent.mkdir(parents=True, exist_ok=True)
    return model_path


def insert_model_registry_row(
    connection: sqlite3.Connection,
    *,
    training_run_id: str,
    exchange_id: str,
    symbol: str,
    model_name: str,
    horizon_steps: int,
    horizon_seconds: int,
    artifact_path: Path,
    feature_config: dict[str, Any],
    train_rows: int,
    test_rows: int,
    cv_fold_count: int,
    holdout_metrics: dict[str, float | None],
    cv_mean_metrics: dict[str, float | None],
    status: str,
    error_message: str | None = None,
) -> int:
    cursor = connection.execute(
        """
        INSERT INTO forecast_model_registry (
            training_run_id,
            exchange_id,
            symbol,
            model_name,
            horizon_steps,
            horizon_seconds,
            artifact_path,
            feature_config_json,
            train_rows,
            test_rows,
            cv_fold_count,
            holdout_mae,
            holdout_rmse,
            holdout_mape,
            holdout_r2,
            cv_mean_mae,
            cv_mean_rmse,
            cv_mean_mape,
            cv_mean_r2,
            created_utc,
            status,
            error_message
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            training_run_id,
            exchange_id,
            symbol,
            model_name,
            int(horizon_steps),
            int(horizon_seconds),
            str(artifact_path),
            json.dumps(feature_config, ensure_ascii=True, separators=(",", ":"), sort_keys=True),
            int(train_rows),
            int(test_rows),
            int(cv_fold_count),
            holdout_metrics["mae"],
            holdout_metrics["rmse"],
            holdout_metrics["mape"],
            holdout_metrics["r2"],
            cv_mean_metrics["mae"],
            cv_mean_metrics["rmse"],
            cv_mean_metrics["mape"],
            cv_mean_metrics["r2"],
            utc_now_iso(),
            status,
            error_message,
        ),
    )
    connection.commit()
    return int(cursor.lastrowid)


def build_training_frame(
    feature_table: pd.DataFrame,
    feature_columns: list[str],
    *,
    horizon_steps: int,
) -> tuple[pd.DataFrame, pd.Series]:
    working = feature_table.copy()
    working["target_price"] = working["price"].shift(-horizon_steps)
    cleaned = working.dropna(subset=feature_columns + ["target_price"]).copy()
    X = cleaned[feature_columns].astype(float)
    y = cleaned["target_price"].astype(float)
    return X, y


def build_cleansing_inference_frame(
    feature_table: pd.DataFrame,
    feature_columns: list[str],
    *,
    horizon_seconds: int,
) -> pd.DataFrame:
    working = feature_table.copy()
    cleaned = working.dropna(subset=feature_columns).copy()
    if cleaned.empty:
        return pd.DataFrame()
    cleaned = cleaned.reset_index(drop=True)
    cleaned["target_bucket_epoch_s"] = cleaned["bucket_epoch_s"] + int(horizon_seconds)
    cleaned["target_bucket_start_utc"] = pd.to_datetime(
        cleaned["target_bucket_epoch_s"], unit="s", utc=True
    ).dt.strftime("%Y-%m-%dT%H:%M:%S%z")
    return cleaned


def process_pair_task(task: PairWorkerTask) -> dict[str, Any]:
    model_templates = create_models_from_hyperparams(
        ridge_alpha=float(task.ridge_alpha),
        hgb_max_iter=int(task.hgb_max_iter),
        hgb_learning_rate=float(task.hgb_learning_rate),
    )
    model_names = list(model_templates.keys())
    models_per_pair = len(task.horizons_steps) * len(model_names)

    result: dict[str, Any] = {
        "exchange_id": task.exchange_id,
        "symbol": task.symbol,
        "pair_key": f"{task.exchange_id}:{task.symbol}",
        "pair_trained_models": 0,
        "model_slots_processed": 0,
        "model_slots_failed": 0,
        "model_slots_skipped": 0,
        "skipped_series_item": None,
        "model_results": [],
    }

    staging_raw = load_staging_series_before_cutoff(
        [Path(path_raw) for path_raw in task.staging_db_paths],
        exchange_id=task.exchange_id,
        symbol=task.symbol,
        cutoff_utc=task.training_cutoff_utc,
    )
    staging_resampled = resample_staging_series(staging_raw, bin_seconds=int(task.bin_seconds))
    if len(staging_resampled) < int(task.min_train_rows):
        result["skipped_series_item"] = {
            "exchange_id": task.exchange_id,
            "symbol": task.symbol,
            "reason": f"insufficient_staging_rows:{len(staging_resampled)}",
        }
        result["model_slots_processed"] = models_per_pair
        result["model_slots_skipped"] = models_per_pair
        return result

    feature_table_staging = build_feature_table(
        staging_resampled,
        lag_steps=list(task.lag_steps),
        rolling_windows=list(task.rolling_windows),
    )
    feature_columns = feature_columns_from_table(feature_table_staging)
    if not feature_columns:
        result["skipped_series_item"] = {
            "exchange_id": task.exchange_id,
            "symbol": task.symbol,
            "reason": "no_features",
        }
        result["model_slots_processed"] = models_per_pair
        result["model_slots_skipped"] = models_per_pair
        return result

    for horizon_step, horizon_seconds in zip(task.horizons_steps, task.horizons_seconds):
        X_all, y_all = build_training_frame(
            feature_table_staging,
            feature_columns,
            horizon_steps=int(horizon_step),
        )
        if len(X_all) < int(task.min_train_rows):
            result["model_slots_processed"] += len(model_names)
            result["model_slots_skipped"] += len(model_names)
            continue

        split_index = int(math.floor(len(X_all) * (1.0 - float(task.test_size))))
        split_index = max(1, min(len(X_all) - 1, split_index))
        X_train = X_all.iloc[:split_index]
        y_train = y_all.iloc[:split_index]
        X_test = X_all.iloc[split_index:]
        y_test = y_all.iloc[split_index:]

        for model_name, base_model in model_templates.items():
            feature_config = {
                "bin_seconds": int(task.bin_seconds),
                "horizon_steps": int(horizon_step),
                "horizon_seconds": int(horizon_seconds),
                "lag_steps": list(task.lag_steps),
                "rolling_windows": list(task.rolling_windows),
                "feature_columns": feature_columns,
                "training_cutoff_utc": task.training_cutoff_utc,
            }
            try:
                cv_fold_count, _, cv_mean_metrics = evaluate_time_series_cv(
                    base_model,
                    X_train,
                    y_train,
                    requested_splits=int(task.cv_splits),
                )
                eval_model = clone(base_model)
                eval_model.fit(X_train, y_train)
                y_test_pred = eval_model.predict(X_test).astype(float)
                holdout_metrics = compute_metrics(y_test.to_numpy(dtype=float), y_test_pred)

                production_model = clone(base_model)
                production_model.fit(X_all, y_all)

                model_path = make_model_artifact_path(
                    model_dir=Path(task.model_dir),
                    training_run_id=task.training_run_id,
                    exchange_id=task.exchange_id,
                    symbol=task.symbol,
                    model_name=model_name,
                    horizon_seconds=int(horizon_seconds),
                )
                joblib.dump(
                    {
                        "model": production_model,
                        "metadata": feature_config,
                    },
                    model_path,
                )

                result["model_results"].append(
                    {
                        "status": "trained",
                        "exchange_id": task.exchange_id,
                        "symbol": task.symbol,
                        "model_name": model_name,
                        "horizon_steps": int(horizon_step),
                        "horizon_seconds": int(horizon_seconds),
                        "artifact_path": str(model_path),
                        "feature_config": feature_config,
                        "train_rows": int(len(X_train)),
                        "test_rows": int(len(X_test)),
                        "cv_fold_count": int(cv_fold_count),
                        "holdout_metrics": holdout_metrics,
                        "cv_mean_metrics": cv_mean_metrics,
                        "prediction_rows": [],
                        "error_message": None,
                    }
                )
                result["pair_trained_models"] += 1
                result["model_slots_processed"] += 1
            except Exception as exc:  # noqa: BLE001
                result["model_results"].append(
                    {
                        "status": "failed",
                        "exchange_id": task.exchange_id,
                        "symbol": task.symbol,
                        "model_name": model_name,
                        "horizon_steps": int(horizon_step),
                        "horizon_seconds": int(horizon_seconds),
                        "artifact_path": "",
                        "feature_config": feature_config,
                        "train_rows": 0,
                        "test_rows": 0,
                        "cv_fold_count": 0,
                        "holdout_metrics": {"mae": None, "rmse": None, "mape": None, "r2": None},
                        "cv_mean_metrics": {"mae": None, "rmse": None, "mape": None, "r2": None},
                        "prediction_rows": [],
                        "error_message": repr(exc),
                    }
                )
                result["model_slots_processed"] += 1
                result["model_slots_failed"] += 1

    return result


def build_pair_worker_exception_result(
    *,
    task: PairWorkerTask,
    model_names: list[str],
    error_message: str,
) -> dict[str, Any]:
    model_results: list[dict[str, Any]] = []
    for horizon_step, horizon_seconds in zip(task.horizons_steps, task.horizons_seconds):
        for model_name in model_names:
            model_results.append(
                {
                    "status": "failed",
                    "exchange_id": task.exchange_id,
                    "symbol": task.symbol,
                    "model_name": model_name,
                    "horizon_steps": int(horizon_step),
                    "horizon_seconds": int(horizon_seconds),
                    "artifact_path": "",
                    "feature_config": {
                        "bin_seconds": int(task.bin_seconds),
                        "horizon_steps": int(horizon_step),
                        "horizon_seconds": int(horizon_seconds),
                        "lag_steps": list(task.lag_steps),
                        "rolling_windows": list(task.rolling_windows),
                        "feature_columns": [],
                        "training_cutoff_utc": task.training_cutoff_utc,
                    },
                    "train_rows": 0,
                    "test_rows": 0,
                    "cv_fold_count": 0,
                    "holdout_metrics": {"mae": None, "rmse": None, "mape": None, "r2": None},
                    "cv_mean_metrics": {"mae": None, "rmse": None, "mape": None, "r2": None},
                    "prediction_rows": [],
                    "error_message": error_message,
                }
            )
    model_slots = len(model_results)
    return {
        "exchange_id": task.exchange_id,
        "symbol": task.symbol,
        "pair_key": f"{task.exchange_id}:{task.symbol}",
        "pair_trained_models": 0,
        "model_slots_processed": model_slots,
        "model_slots_failed": model_slots,
        "model_slots_skipped": 0,
        "skipped_series_item": None,
        "model_results": model_results,
    }


def log_progress(
    *,
    total_pairs: int,
    processed_pairs: int,
    total_model_slots: int,
    processed_model_slots: int,
    trained_models: int,
    failed_models: int,
    skipped_model_slots: int,
    predictions_written: int,
    started_monotonic: float,
    last_pair: str,
) -> None:
    elapsed_seconds = max(0.0, time.monotonic() - started_monotonic)
    pair_pct = (processed_pairs / total_pairs * 100.0) if total_pairs > 0 else 0.0
    model_pct = (processed_model_slots / total_model_slots * 100.0) if total_model_slots > 0 else 0.0
    eta_seconds = estimate_eta_seconds(
        elapsed_seconds=elapsed_seconds,
        completed_items=processed_pairs,
        total_items=total_pairs,
    )
    eta_txt = format_hhmmss(eta_seconds) if eta_seconds is not None else "n/a"
    log(
        "INFO",
        "Progress "
        f"pairs {processed_pairs}/{total_pairs} ({pair_pct:.1f}%) | "
        f"model_slots {processed_model_slots}/{total_model_slots} ({model_pct:.1f}%) | "
        f"trained={trained_models} failed={failed_models} skipped={skipped_model_slots} "
        f"predictions={predictions_written} | "
        f"elapsed={format_hhmmss(elapsed_seconds)} eta={eta_txt} | "
        f"last_pair={last_pair}",
    )


def main() -> None:
    args = parse_args()
    validate_args(args)
    if MISSING_DEPENDENCY_MESSAGE is not None:
        raise SystemExit(MISSING_DEPENDENCY_MESSAGE)

    lag_steps = parse_int_csv(args.lag_steps, arg_name="--lag-steps")
    rolling_windows = parse_int_csv(args.rolling_windows, arg_name="--rolling-windows")
    symbol_filters = parse_csv_values(args.symbols)

    staging_db_paths = resolve_staging_db_paths(args.staging_db)

    forecast_db_path = resolve_file(args.forecast_db)
    forecast_db_path.parent.mkdir(parents=True, exist_ok=True)
    model_dir = resolve_file(args.model_dir)
    model_dir.mkdir(parents=True, exist_ok=True)
    staging_min_utc, staging_max_utc = summarize_staging_time_window(staging_db_paths)
    if staging_min_utc is None:
        raise SystemExit("No staging ingestion timestamps found in market_ticks. Cannot train forecasting models.")

    training_cutoff_utc = resolve_training_cutoff_utc(
        requested_cutoff_utc=args.training_cutoff_utc,
        staging_max_utc=staging_max_utc,
    )
    bin_seconds = int(args.bin_seconds)
    scope_pairs = load_scope_pairs_from_staging(
        staging_db_paths,
        exchange_id_filter=args.exchange_id,
        symbol_filters=symbol_filters,
        training_cutoff_utc=training_cutoff_utc,
    )
    if args.limit_series is not None:
        scope_pairs = scope_pairs[: args.limit_series]

    if not scope_pairs:
        raise SystemExit("No staging exchange/symbol pairs found for selected filters.")

    log("INFO", f"Training cutoff UTC: {training_cutoff_utc}")
    log("INFO", f"Bin seconds: {bin_seconds}")
    log("INFO", f"Scope pairs: {len(scope_pairs)}")
    log("INFO", f"Staging input: {args.staging_db}")
    log("INFO", f"Resolved staging DB count: {len(staging_db_paths)}")
    log("INFO", f"Staging time window UTC: min={staging_min_utc or '-'} max={staging_max_utc or '-'}")

    horizons_steps = [1, int(args.secondary_horizon_multiple)]
    horizons_seconds = [bin_seconds * step for step in horizons_steps]
    log("INFO", f"Training horizons seconds: {horizons_seconds}")

    training_run_id = "forecast_train_" + datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%SZ")

    forecast_conn = sqlite3.connect(str(forecast_db_path))
    try:
        ensure_forecast_tables(forecast_conn)
        insert_training_run_start(
            forecast_conn,
            training_run_id=training_run_id,
            args=args,
            staging_db_count=len(staging_db_paths),
            staging_input=args.staging_db,
            cleansing_db_path=Path("-"),
            cleansing_run_id="-",
            forecast_db_path=forecast_db_path,
            training_cutoff_utc=training_cutoff_utc,
            bin_seconds=bin_seconds,
            lag_steps=lag_steps,
            rolling_windows=rolling_windows,
            series_total=len(scope_pairs),
        )

        models = create_models(args)
        series_trained = 0
        models_trained = 0
        predictions_written = 0
        skipped_series: list[dict[str, Any]] = []
        failed_models: list[dict[str, Any]] = []
        failed_model_slots = 0
        skipped_model_slots = 0
        processed_pairs = 0
        processed_model_slots = 0
        models_per_pair = len(horizons_steps) * len(models)
        total_pairs = len(scope_pairs)
        total_model_slots = total_pairs * models_per_pair
        progress_started_monotonic = time.monotonic()
        last_progress_logged_monotonic = progress_started_monotonic - float(args.progress_interval_seconds)
        last_completed_pair = "-"

        def maybe_log_progress(*, force: bool = False) -> None:
            nonlocal last_progress_logged_monotonic
            if not args.progress:
                return
            now_monotonic = time.monotonic()
            if (not force) and (
                now_monotonic - last_progress_logged_monotonic < float(args.progress_interval_seconds)
            ):
                return
            log_progress(
                total_pairs=total_pairs,
                processed_pairs=processed_pairs,
                total_model_slots=total_model_slots,
                processed_model_slots=processed_model_slots,
                trained_models=models_trained,
                failed_models=failed_model_slots,
                skipped_model_slots=skipped_model_slots,
                predictions_written=predictions_written,
                started_monotonic=progress_started_monotonic,
                last_pair=last_completed_pair,
            )
            last_progress_logged_monotonic = now_monotonic

        maybe_log_progress(force=True)

        model_names = list(models.keys())
        worker_tasks = [
            PairWorkerTask(
                exchange_id=pair.exchange_id,
                symbol=pair.symbol,
                cleansing_rows=int(pair.cleansing_rows),
                staging_db_paths=tuple(str(path) for path in staging_db_paths),
                training_cutoff_utc=training_cutoff_utc,
                bin_seconds=int(bin_seconds),
                lag_steps=tuple(lag_steps),
                rolling_windows=tuple(rolling_windows),
                min_train_rows=int(args.min_train_rows),
                test_size=float(args.test_size),
                cv_splits=int(args.cv_splits),
                ridge_alpha=float(args.ridge_alpha),
                hgb_max_iter=int(args.hgb_max_iter),
                hgb_learning_rate=float(args.hgb_learning_rate),
                horizons_steps=tuple(int(step) for step in horizons_steps),
                horizons_seconds=tuple(int(seconds) for seconds in horizons_seconds),
                training_run_id=training_run_id,
                model_dir=str(model_dir),
            )
            for pair in scope_pairs
        ]
        log("INFO", f"Worker processes: {args.workers}")

        def persist_pair_result(result: dict[str, Any]) -> None:
            nonlocal series_trained
            nonlocal models_trained
            nonlocal predictions_written
            nonlocal failed_model_slots
            nonlocal skipped_model_slots
            nonlocal processed_pairs
            nonlocal processed_model_slots
            nonlocal last_completed_pair

            processed_pairs += 1
            processed_model_slots += int(result.get("model_slots_processed", 0))
            failed_model_slots += int(result.get("model_slots_failed", 0))
            skipped_model_slots += int(result.get("model_slots_skipped", 0))
            last_completed_pair = str(result.get("pair_key", "-"))

            skipped_item = result.get("skipped_series_item")
            if isinstance(skipped_item, dict):
                skipped_series.append(skipped_item)
                if args.log_skip_details:
                    log(
                        "WARNING",
                        f"Skip pair exchange={skipped_item.get('exchange_id')} "
                        f"symbol={skipped_item.get('symbol')} reason={skipped_item.get('reason')}",
                    )

            if int(result.get("pair_trained_models", 0)) > 0:
                series_trained += 1

            for model_result in result.get("model_results", []):
                status = str(model_result.get("status", "")).lower()
                holdout_metrics = model_result.get("holdout_metrics") or {
                    "mae": None,
                    "rmse": None,
                    "mape": None,
                    "r2": None,
                }
                cv_mean_metrics = model_result.get("cv_mean_metrics") or {
                    "mae": None,
                    "rmse": None,
                    "mape": None,
                    "r2": None,
                }
                exchange_id = str(model_result.get("exchange_id", result.get("exchange_id", "")))
                symbol = str(model_result.get("symbol", result.get("symbol", "")))
                model_name = str(model_result.get("model_name", "unknown_model"))
                horizon_steps = int(model_result.get("horizon_steps", 0))
                horizon_seconds = int(model_result.get("horizon_seconds", 0))
                error_message = model_result.get("error_message")
                feature_config = model_result.get("feature_config") or {
                    "bin_seconds": int(bin_seconds),
                    "horizon_steps": horizon_steps,
                    "horizon_seconds": horizon_seconds,
                    "lag_steps": lag_steps,
                    "rolling_windows": rolling_windows,
                    "feature_columns": [],
                    "training_cutoff_utc": training_cutoff_utc,
                }

                insert_model_registry_row(
                    forecast_conn,
                    training_run_id=training_run_id,
                    exchange_id=exchange_id,
                    symbol=symbol,
                    model_name=model_name,
                    horizon_steps=horizon_steps,
                    horizon_seconds=horizon_seconds,
                    artifact_path=Path(str(model_result.get("artifact_path", ""))),
                    feature_config=feature_config,
                    train_rows=int(model_result.get("train_rows", 0)),
                    test_rows=int(model_result.get("test_rows", 0)),
                    cv_fold_count=int(model_result.get("cv_fold_count", 0)),
                    holdout_metrics=holdout_metrics,
                    cv_mean_metrics=cv_mean_metrics,
                    status="trained" if status == "trained" else "failed",
                    error_message=str(error_message) if error_message is not None else None,
                )

                if status != "trained":
                    failed_models.append(
                        {
                            "exchange_id": exchange_id,
                            "symbol": symbol,
                            "model_name": model_name,
                            "horizon_seconds": horizon_seconds,
                            "error": str(error_message),
                        }
                    )
                    if args.log_skip_details:
                        log(
                            "ERROR",
                            f"Model failed exchange={exchange_id} symbol={symbol} "
                            f"model={model_name} horizon_s={horizon_seconds} error={error_message}",
                        )
                    continue

                models_trained += 1
                if args.log_pair_details:
                    holdout_rmse = holdout_metrics.get("rmse")
                    holdout_rmse_txt = f"{holdout_rmse:.8f}" if holdout_rmse is not None else "n/a"
                    log(
                        "INFO",
                        f"Trained model exchange={exchange_id} symbol={symbol} "
                        f"model={model_name} horizon_s={horizon_seconds} "
                        f"holdout_rmse={holdout_rmse_txt}",
                    )

            maybe_log_progress()

        if args.workers == 1:
            for idx, task in enumerate(worker_tasks, start=1):
                if args.log_pair_details:
                    log(
                        "INFO",
                        f"Pair {idx}/{len(worker_tasks)} exchange={task.exchange_id} "
                        f"symbol={task.symbol} staging_rows={task.cleansing_rows}",
                    )
                try:
                    pair_result = process_pair_task(task)
                except Exception as exc:  # noqa: BLE001
                    pair_result = build_pair_worker_exception_result(
                        task=task,
                        model_names=model_names,
                        error_message=f"worker_exception:{exc!r}",
                    )
                persist_pair_result(pair_result)
        else:
            with concurrent.futures.ProcessPoolExecutor(
                max_workers=int(args.workers),
                mp_context=multiprocessing.get_context("spawn"),
            ) as executor:
                future_to_task: dict[concurrent.futures.Future[dict[str, Any]], PairWorkerTask] = {}
                for idx, task in enumerate(worker_tasks, start=1):
                    if args.log_pair_details:
                        log(
                            "INFO",
                            f"Queue pair {idx}/{len(worker_tasks)} exchange={task.exchange_id} "
                            f"symbol={task.symbol} staging_rows={task.cleansing_rows}",
                        )
                    future = executor.submit(process_pair_task, task)
                    future_to_task[future] = task

                for future in concurrent.futures.as_completed(future_to_task):
                    task = future_to_task[future]
                    try:
                        pair_result = future.result()
                    except Exception as exc:  # noqa: BLE001
                        pair_result = build_pair_worker_exception_result(
                            task=task,
                            model_names=model_names,
                            error_message=f"worker_exception:{exc!r}",
                        )
                    persist_pair_result(pair_result)

        maybe_log_progress(force=True)

        status = "completed"
        notes = {
            "skipped_series_count": len(skipped_series),
            "failed_model_count": len(failed_models),
            "skipped_series": skipped_series[:200],
            "failed_models": failed_models[:200],
            "horizons_seconds": horizons_seconds,
        }
        update_training_run_finish(
            forecast_conn,
            training_run_id=training_run_id,
            status=status,
            series_trained=series_trained,
            models_trained=models_trained,
            predictions_written=predictions_written,
            notes=notes,
        )
    finally:
        forecast_conn.close()

    log(
        "INFO",
        f"Training run finished id={training_run_id} "
        f"forecast_db={forecast_db_path} model_dir={model_dir}",
    )


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
