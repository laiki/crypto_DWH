#!/usr/bin/env python
"""
Redis Stream consumer and VAULT writer runtime.

Compatibility note:
- this file remains as a backward-compatible entrypoint
- the product command path is `redis_stream_to_vault_writer.py`
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

REDIS_IMPORT_ERROR: Exception | None = None
try:
    import redis
except Exception as exc:  # pragma: no cover - import environment dependent
    redis = None
    REDIS_IMPORT_ERROR = exc


LOGGER = logging.getLogger("redis_stream_to_vault_writer")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Consume Redis Stream events and write into VAULT partitions.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis connection URL.")
    parser.add_argument("--stream", default="ingest:events:v1", help="Redis source stream.")
    parser.add_argument("--group", default="cg.vault_writer", help="Redis consumer group.")
    parser.add_argument("--consumer", default=f"writer-{os.getpid()}", help="Consumer name in the group.")
    parser.add_argument("--dlq-stream", default="ingest:events:dlq:v1", help="Dead-letter stream key.")
    parser.add_argument(
        "--stream-maxlen",
        type=int,
        default=50_000,
        help="Approximate maxlen for the main stream short-lived operational buffer.",
    )
    parser.add_argument(
        "--dlq-maxlen",
        type=int,
        default=10_000,
        help="Approximate maxlen for the DLQ short-lived operational buffer.",
    )
    parser.add_argument("--vault-root", default="data/vault2_redis", help="Output VAULT root directory.")
    parser.add_argument("--vault-layer", default="ingestion", help="Partition layer label.")
    parser.add_argument("--batch-size", type=int, default=200, help="XREADGROUP batch size.")
    parser.add_argument("--block-ms", type=int, default=3000, help="XREADGROUP block timeout ms.")
    parser.add_argument("--max-retries", type=int, default=3, help="Retry attempts before DLQ handoff.")
    parser.add_argument("--retry-sleep-s", type=float, default=0.25, help="Sleep between retries.")
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="Run duration in seconds. 0 means infinite.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Console log level.",
    )
    return parser.parse_args()


def configure_logging(level_name: str) -> None:
    logging.basicConfig(
        level=getattr(logging, level_name),
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def ensure_redis_available() -> None:
    if redis is not None:
        return
    cause = f"{type(REDIS_IMPORT_ERROR).__name__}: {REDIS_IMPORT_ERROR}"
    raise SystemExit(
        "Failed to import 'redis'. "
        f"interpreter={os.path.realpath(os.sys.executable)} | cause={cause}. "
        "Install with: python -m pip install -U redis"
    ) from REDIS_IMPORT_ERROR


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def normalize_text(value: Any) -> str:
    if not isinstance(value, str):
        return ""
    return value.strip()


def normalize_text_lower(value: Any) -> str:
    return normalize_text(value).lower()


def symbol_asset(symbol: str) -> str:
    if not symbol:
        return ""
    return normalize_text_lower(symbol.split("/", 1)[0])


def ms_to_utc_iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000.0, tz=timezone.utc).isoformat(timespec="milliseconds")


def ensure_manifest_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        CREATE TABLE IF NOT EXISTS partition_registry (
            partition_id TEXT PRIMARY KEY,
            layer TEXT NOT NULL,
            db_path TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            ts_min_epoch_s INTEGER NOT NULL,
            ts_max_epoch_s INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            created_utc TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_partition_registry_layer_time
            ON partition_registry(layer, ts_min_epoch_s, ts_max_epoch_s);

        CREATE TABLE IF NOT EXISTS partition_pairs (
            partition_id TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            symbol_norm TEXT NOT NULL,
            asset_norm TEXT NOT NULL,
            ts_min_epoch_s INTEGER NOT NULL,
            ts_max_epoch_s INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            PRIMARY KEY (partition_id, exchange_id_norm, symbol_norm)
        );

        CREATE INDEX IF NOT EXISTS idx_partition_pairs_scope
            ON partition_pairs(exchange_id_norm, symbol_norm, ts_min_epoch_s, ts_max_epoch_s);

        CREATE TABLE IF NOT EXISTS processed_event_keys (
            dedup_key TEXT PRIMARY KEY,
            event_type TEXT NOT NULL,
            first_seen_utc TEXT NOT NULL,
            source_stream_id TEXT NOT NULL
        );
        """
    )
    connection.commit()


def ensure_partition_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        PRAGMA journal_mode=WAL;
        PRAGMA synchronous=NORMAL;

        CREATE TABLE IF NOT EXISTS market_ticks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ingestion_ts_utc TEXT NOT NULL,
            ingestion_ts_epoch_s INTEGER NOT NULL,
            exchange_id TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            symbol TEXT NOT NULL,
            symbol_norm TEXT NOT NULL,
            asset_norm TEXT NOT NULL,
            market_type TEXT,
            price REAL,
            bid REAL,
            ask REAL,
            last REAL,
            open REAL,
            high REAL,
            low REAL,
            base_volume REAL,
            quote_volume REAL,
            exchange_ts_ms INTEGER,
            exchange_ts_utc TEXT,
            raw_json TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_market_ticks_pair_ts
            ON market_ticks(exchange_id_norm, symbol_norm, ingestion_ts_epoch_s);

        CREATE TABLE IF NOT EXISTS connection_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_ts_utc TEXT NOT NULL,
            event_ts_epoch_s INTEGER NOT NULL,
            exchange_id TEXT NOT NULL,
            exchange_id_norm TEXT NOT NULL,
            symbol TEXT,
            symbol_norm TEXT,
            asset_norm TEXT,
            event_type TEXT NOT NULL,
            details_json TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_connection_events_ex_ts
            ON connection_events(exchange_id_norm, event_ts_epoch_s);
        """
    )
    connection.commit()


def to_int_ms(value: Any, fallback_ms: int) -> int:
    if isinstance(value, (int, float)):
        return int(value)
    return int(fallback_ms)


def partition_info(vault_root: Path, vault_layer: str, exchange_id_norm: str, reference_ms: int) -> tuple[str, Path]:
    dt = datetime.fromtimestamp(reference_ms / 1000.0, tz=timezone.utc)
    date_label = dt.strftime("%Y-%m-%d")
    hour_label = dt.strftime("%H")
    compact = dt.strftime("%Y%m%d_%H")
    partition_id = f"{vault_layer}:{exchange_id_norm}:{compact}"
    db_path = (
        vault_root
        / vault_layer
        / f"exchange={exchange_id_norm}"
        / f"date={date_label}"
        / f"hour={hour_label}"
        / f"part_{exchange_id_norm}_{compact}.db"
    )
    return partition_id, db_path


def load_event(fields: dict[str, Any], message_id: str) -> dict[str, Any]:
    payload = fields.get("event_json")
    if payload is None:
        raise ValueError(f"stream message has no event_json field | id={message_id}")
    if isinstance(payload, bytes):
        payload = payload.decode("utf-8", errors="replace")
    event = json.loads(str(payload))
    if not isinstance(event, dict):
        raise ValueError("event_json payload is not an object")
    return event


def validate_event(event: dict[str, Any]) -> None:
    required = [
        "event_id",
        "event_type",
        "schema_version",
        "exchange_id",
        "symbol",
        "symbol_norm",
        "asset_norm",
        "event_ts_ms",
        "ingestion_ts_ms",
        "dedup_key",
        "payload",
    ]
    missing = [key for key in required if key not in event]
    if missing:
        raise ValueError(f"missing required keys: {missing}")
    if event["event_type"] not in {"tick", "connection_event", "heartbeat"}:
        raise ValueError(f"unsupported event_type={event['event_type']}")
    if not isinstance(event.get("payload"), dict):
        raise ValueError("payload must be an object")


def upsert_manifest_for_partition(
    manifest_connection: sqlite3.Connection,
    *,
    partition_id: str,
    layer: str,
    db_path: Path,
    exchange_id_norm: str,
) -> None:
    with sqlite3.connect(str(db_path)) as partition_connection:
        stats = partition_connection.execute(
            """
            WITH all_ts AS (
                SELECT ingestion_ts_epoch_s AS ts FROM market_ticks
                UNION ALL
                SELECT event_ts_epoch_s AS ts FROM connection_events
            )
            SELECT COALESCE(MIN(ts), 0), COALESCE(MAX(ts), 0), COUNT(*)
            FROM all_ts
            """
        ).fetchone()
        ts_min = int(stats[0]) if stats else 0
        ts_max = int(stats[1]) if stats else 0
        row_count = int(stats[2]) if stats else 0

        pairs = partition_connection.execute(
            """
            WITH pair_rows AS (
                SELECT exchange_id_norm, symbol_norm, asset_norm, ingestion_ts_epoch_s AS ts
                FROM market_ticks
                UNION ALL
                SELECT exchange_id_norm, symbol_norm, asset_norm, event_ts_epoch_s AS ts
                FROM connection_events
                WHERE symbol_norm IS NOT NULL AND symbol_norm <> ''
            )
            SELECT
                exchange_id_norm,
                symbol_norm,
                asset_norm,
                MIN(ts) AS ts_min,
                MAX(ts) AS ts_max,
                COUNT(*) AS row_count
            FROM pair_rows
            GROUP BY exchange_id_norm, symbol_norm, asset_norm
            """
        ).fetchall()

    manifest_connection.execute(
        """
        INSERT INTO partition_registry (
            partition_id,
            layer,
            db_path,
            exchange_id_norm,
            ts_min_epoch_s,
            ts_max_epoch_s,
            row_count,
            created_utc
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(partition_id) DO UPDATE SET
            layer=excluded.layer,
            db_path=excluded.db_path,
            exchange_id_norm=excluded.exchange_id_norm,
            ts_min_epoch_s=excluded.ts_min_epoch_s,
            ts_max_epoch_s=excluded.ts_max_epoch_s,
            row_count=excluded.row_count
        """,
        (
            partition_id,
            layer,
            str(db_path),
            exchange_id_norm,
            ts_min,
            ts_max,
            row_count,
            utc_now_iso(),
        ),
    )

    manifest_connection.execute("DELETE FROM partition_pairs WHERE partition_id = ?", (partition_id,))
    for row in pairs:
        manifest_connection.execute(
            """
            INSERT INTO partition_pairs (
                partition_id,
                exchange_id_norm,
                symbol_norm,
                asset_norm,
                ts_min_epoch_s,
                ts_max_epoch_s,
                row_count
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                partition_id,
                str(row[0]),
                str(row[1]),
                str(row[2]),
                int(row[3]),
                int(row[4]),
                int(row[5]),
            ),
        )
    manifest_connection.commit()


def register_dedup_key(
    manifest_connection: sqlite3.Connection,
    *,
    dedup_key: str,
    event_type: str,
    source_stream_id: str,
) -> bool:
    manifest_connection.execute(
        """
        INSERT OR IGNORE INTO processed_event_keys (
            dedup_key,
            event_type,
            first_seen_utc,
            source_stream_id
        )
        VALUES (?, ?, ?, ?)
        """,
        (dedup_key, event_type, utc_now_iso(), source_stream_id),
    )
    manifest_connection.commit()
    changed = int(manifest_connection.execute("SELECT changes()").fetchone()[0])
    return changed > 0


def write_tick_event(partition_connection: sqlite3.Connection, event: dict[str, Any]) -> None:
    payload = event["payload"]
    ingestion_ts_ms = to_int_ms(event.get("ingestion_ts_ms"), int(time.time() * 1000))
    ingestion_epoch_s = int(ingestion_ts_ms // 1000)
    partition_connection.execute(
        """
        INSERT INTO market_ticks (
            ingestion_ts_utc,
            ingestion_ts_epoch_s,
            exchange_id,
            exchange_id_norm,
            symbol,
            symbol_norm,
            asset_norm,
            market_type,
            price,
            bid,
            ask,
            last,
            open,
            high,
            low,
            base_volume,
            quote_volume,
            exchange_ts_ms,
            exchange_ts_utc,
            raw_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ms_to_utc_iso(ingestion_ts_ms),
            ingestion_epoch_s,
            normalize_text(event.get("exchange_id")),
            normalize_text_lower(event.get("exchange_id")),
            normalize_text(event.get("symbol")),
            normalize_text_lower(event.get("symbol_norm") or event.get("symbol")),
            normalize_text_lower(event.get("asset_norm") or symbol_asset(str(event.get("symbol", "")))),
            normalize_text(payload.get("market_type")),
            payload.get("price"),
            payload.get("bid"),
            payload.get("ask"),
            payload.get("last"),
            payload.get("open"),
            payload.get("high"),
            payload.get("low"),
            payload.get("base_volume"),
            payload.get("quote_volume"),
            payload.get("exchange_ts_ms"),
            ms_to_utc_iso(int(payload.get("exchange_ts_ms"))) if isinstance(payload.get("exchange_ts_ms"), (int, float)) else None,
            normalize_text(payload.get("raw_json") or json.dumps(payload, ensure_ascii=True, default=str)),
        ),
    )


def write_connection_event(partition_connection: sqlite3.Connection, event: dict[str, Any]) -> None:
    payload = event["payload"]
    event_ts_ms = to_int_ms(event.get("event_ts_ms"), int(time.time() * 1000))
    partition_connection.execute(
        """
        INSERT INTO connection_events (
            event_ts_utc,
            event_ts_epoch_s,
            exchange_id,
            exchange_id_norm,
            symbol,
            symbol_norm,
            asset_norm,
            event_type,
            details_json
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            ms_to_utc_iso(event_ts_ms),
            int(event_ts_ms // 1000),
            normalize_text(event.get("exchange_id")),
            normalize_text_lower(event.get("exchange_id")),
            normalize_text(event.get("symbol")),
            normalize_text_lower(event.get("symbol_norm") or event.get("symbol")),
            normalize_text_lower(event.get("asset_norm") or symbol_asset(str(event.get("symbol", "")))),
            normalize_text(payload.get("event_name") or event.get("event_type")),
            normalize_text(payload.get("details_json") or json.dumps(payload, ensure_ascii=True, default=str)),
        ),
    )


def write_event_to_partition(
    *,
    vault_root: Path,
    vault_layer: str,
    event: dict[str, Any],
) -> tuple[str, Path]:
    exchange_id_norm = normalize_text_lower(event.get("exchange_id"))
    if not exchange_id_norm:
        raise ValueError("event.exchange_id is empty")
    event_ts_ms = to_int_ms(event.get("event_ts_ms"), int(time.time() * 1000))
    partition_id, partition_path = partition_info(vault_root, vault_layer, exchange_id_norm, event_ts_ms)
    partition_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(partition_path)) as partition_connection:
        ensure_partition_schema(partition_connection)
        event_type = normalize_text(event.get("event_type"))
        if event_type == "tick":
            write_tick_event(partition_connection, event)
        elif event_type in {"connection_event", "heartbeat"}:
            write_connection_event(partition_connection, event)
        else:
            raise ValueError(f"unsupported event_type={event_type}")
        partition_connection.commit()
    return partition_id, partition_path


def publish_dlq(
    redis_client: redis.Redis,
    *,
    dlq_stream: str,
    dlq_maxlen: int,
    source_stream: str,
    source_message_id: str,
    event: dict[str, Any] | None,
    error_message: str,
) -> None:
    dlq_payload = {
        "failed_at_utc": utc_now_iso(),
        "source_stream": source_stream,
        "source_message_id": source_message_id,
        "error_message": error_message,
        "event_json": event,
    }
    redis_client.xadd(
        dlq_stream,
        {"dlq_json": json.dumps(dlq_payload, ensure_ascii=True, default=str, separators=(",", ":"))},
        maxlen=dlq_maxlen,
        approximate=True,
    )


def ensure_group(redis_client: redis.Redis, stream: str, group: str) -> None:
    try:
        redis_client.xgroup_create(stream, group, id="$", mkstream=True)
        LOGGER.info("Created consumer group | stream=%s group=%s", stream, group)
    except redis.exceptions.ResponseError as exc:
        if "BUSYGROUP" in str(exc):
            return
        raise


def run(args: argparse.Namespace) -> None:
    ensure_redis_available()
    if args.max_retries <= 0:
        raise SystemExit("--max-retries must be > 0")

    redis_client = redis.Redis.from_url(args.redis_url, decode_responses=True)
    redis_client.ping()
    ensure_group(redis_client, args.stream, args.group)

    vault_root = Path(args.vault_root)
    manifest_path = vault_root / "meta" / "vault_manifest.db"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(manifest_path)) as manifest_connection:
        ensure_manifest_schema(manifest_connection)

        started = time.monotonic()
        consumed = 0
        written = 0
        duplicated = 0
        dlq_count = 0
        LOGGER.info(
            "Starting consumer | stream=%s group=%s consumer=%s vault_root=%s",
            args.stream,
            args.group,
            args.consumer,
            vault_root,
        )

        while True:
            if args.duration_seconds > 0 and (time.monotonic() - started) >= args.duration_seconds:
                LOGGER.info("Duration reached. Stopping consumer.")
                break

            response = redis_client.xreadgroup(
                groupname=args.group,
                consumername=args.consumer,
                streams={args.stream: ">"},
                count=args.batch_size,
                block=args.block_ms,
            )
            if not response:
                continue

            for stream_key, messages in response:
                for message_id, fields in messages:
                    consumed += 1
                    event_for_dlq: dict[str, Any] | None = None
                    last_error = "unknown"
                    processed_ok = False
                    for attempt in range(1, args.max_retries + 1):
                        try:
                            event = load_event(fields, message_id)
                            event_for_dlq = event
                            validate_event(event)
                            dedup_key = normalize_text(event.get("dedup_key"))
                            if not dedup_key:
                                raise ValueError("dedup_key is empty")

                            was_inserted = register_dedup_key(
                                manifest_connection,
                                dedup_key=dedup_key,
                                event_type=normalize_text(event.get("event_type")),
                                source_stream_id=message_id,
                            )
                            if not was_inserted:
                                duplicated += 1
                                redis_client.xack(args.stream, args.group, message_id)
                                processed_ok = True
                                break

                            partition_id, partition_path = write_event_to_partition(
                                vault_root=vault_root,
                                vault_layer=args.vault_layer,
                                event=event,
                            )
                            upsert_manifest_for_partition(
                                manifest_connection,
                                partition_id=partition_id,
                                layer=args.vault_layer,
                                db_path=partition_path,
                                exchange_id_norm=normalize_text_lower(event.get("exchange_id")),
                            )
                            redis_client.xack(args.stream, args.group, message_id)
                            written += 1
                            processed_ok = True
                            break
                        except Exception as exc:  # noqa: BLE001
                            last_error = f"{type(exc).__name__}: {exc}"
                            LOGGER.warning(
                                "Processing failed | stream_id=%s attempt=%s/%s error=%s",
                                message_id,
                                attempt,
                                args.max_retries,
                                last_error,
                            )
                            if attempt < args.max_retries:
                                time.sleep(max(0.0, args.retry_sleep_s))

                    if not processed_ok:
                        publish_dlq(
                            redis_client,
                            dlq_stream=args.dlq_stream,
                            dlq_maxlen=args.dlq_maxlen,
                            source_stream=args.stream,
                            source_message_id=message_id,
                            event=event_for_dlq,
                            error_message=last_error,
                        )
                        redis_client.xack(args.stream, args.group, message_id)
                        dlq_count += 1
                        LOGGER.error("Moved event to DLQ | stream_id=%s error=%s", message_id, last_error)

            if (written + duplicated + dlq_count) > 0 and (written + duplicated + dlq_count) % 100 == 0:
                LOGGER.info(
                    "Progress | consumed=%s written=%s duplicated=%s dlq=%s",
                    consumed,
                    written,
                    duplicated,
                    dlq_count,
                )

        LOGGER.info(
            "Consumer stopped | consumed=%s written=%s duplicated=%s dlq=%s",
            consumed,
            written,
            duplicated,
            dlq_count,
        )


def main() -> None:
    args = parse_args()
    configure_logging(args.log_level)
    run(args)


if __name__ == "__main__":
    main()
