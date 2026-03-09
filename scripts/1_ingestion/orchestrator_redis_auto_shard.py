#!/usr/bin/env python
"""
Auto-sharding orchestrator for Redis-stream ingestion workers.

What this script does:
1. Profiles exchange traffic load over a short sampling window.
2. Computes a load score per exchange.
3. Distributes exchanges across publisher workers with greedy bin packing.
4. Spawns and supervises Redis publisher worker processes automatically.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path
from typing import Any

import ccxt.pro as ccxtpro

from orchestrator_auto_shard import (  # noqa: PLC2701
    DEFAULT_EXCLUDED_EXCHANGES,
    WorkerPlan,
    configure_logging_with_file,
    ensure_ccxtpro_available,
    load_plan_json,
    parse_exchange_list,
    parse_symbol_filters,
    pick_worker_count,
    profile_exchanges,
    resolve_excluded_exchanges,
    supervise_workers,
    write_plan_json,
)


LOGGER = logging.getLogger("orchestrator_redis_auto_shard")


def parse_args() -> argparse.Namespace:
    default_excluded_csv = ", ".join(sorted(DEFAULT_EXCLUDED_EXCHANGES))
    parser = argparse.ArgumentParser(
        description="Profile exchange traffic, auto-shard exchanges, and spawn Redis publisher worker processes.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--plan-input", default=None, help="Optional existing plan JSON path.")
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated list of exchange IDs. Default: all ccxt.pro exchanges after default exclusions.",
    )
    parser.add_argument(
        "--exchanges-exclude",
        default=None,
        help=(
            "Comma-separated list of exchange IDs additionally excluded from profiling and execution. "
            f"Default excluded: {default_excluded_csv}."
        ),
    )
    parser.add_argument("--workers", type=int, default=None, help="Fixed worker count. Default: auto.")
    parser.add_argument("--max-auto-workers", type=int, default=8, help="Maximum worker count in auto mode.")
    parser.add_argument("--profile-seconds", type=float, default=30.0, help="Sampling window per exchange in seconds.")
    parser.add_argument("--profile-concurrency", type=int, default=4, help="How many exchanges to profile in parallel.")
    parser.add_argument(
        "--profile-request-timeout-s",
        type=float,
        default=8.0,
        help="Timeout for a single websocket receive during profiling.",
    )
    parser.add_argument(
        "--profile-symbol-limit",
        type=int,
        default=25,
        help="Max symbols used during profiling per exchange (-1 for all).",
    )
    parser.add_argument("--only-spot", action="store_true", help="Use spot markets only.")
    parser.add_argument("--only-watch-tickers", action="store_true", help="Use only exchanges with watchTickers support.")
    parser.add_argument(
        "--max-symbols-per-exchange",
        type=int,
        default=100,
        help="Optional symbol cap per exchange passed to worker publishers. <=0 means all.",
    )
    parser.add_argument(
        "--symbols",
        "--symbol",
        dest="symbols",
        default=None,
        help=(
            "Optional comma-separated symbol filter. Supports exact symbols and SQL LIKE patterns "
            "(%% and _), case-insensitive."
        ),
    )
    parser.add_argument(
        "--max-symbol-streams-per-exchange",
        type=int,
        default=25,
        help="Only for watch_ticker fallback: cap concurrent symbol streams per exchange. <=0 means all.",
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis connection URL.")
    parser.add_argument("--stream", default="ingest:events:v1", help="Redis stream key.")
    parser.add_argument(
        "--stream-maxlen",
        type=int,
        default=50_000,
        help="Approximate max stream length for the short-lived operational buffer.",
    )
    parser.add_argument("--logs-dir", default="logs", help="Directory for worker log files.")
    parser.add_argument("--plan-output", default="data/redis_orchestrator_plan.json", help="Path to write plan JSON.")
    parser.add_argument("--dry-run", action="store_true", help="Only profile and create plan. Do not spawn workers.")
    parser.add_argument(
        "--worker-log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level forwarded to worker publisher processes.",
    )
    parser.add_argument("--queue-size", type=int, default=200000, help="Forwarded worker queue size.")
    parser.add_argument(
        "--exchange-start-delay-s",
        type=float,
        default=0.2,
        help="Forwarded worker delay between exchange worker starts.",
    )
    parser.add_argument(
        "--reconnect-base-s",
        type=float,
        default=1.0,
        help="Forwarded worker reconnect backoff base.",
    )
    parser.add_argument(
        "--reconnect-max-s",
        type=float,
        default=30.0,
        help="Forwarded worker reconnect backoff max.",
    )
    parser.add_argument("--redis-retry-s", type=float, default=1.0, help="Forwarded worker Redis retry sleep.")
    parser.add_argument(
        "--producer-id-prefix",
        default=None,
        help="Optional producer id prefix forwarded to worker publishers.",
    )
    parser.add_argument("--print-every", type=int, default=100, help="Forwarded worker publish log interval.")
    parser.add_argument(
        "--duration-seconds",
        type=float,
        default=0.0,
        help="Forwarded worker run duration. 0 means infinite.",
    )
    parser.add_argument("--restart-base-s", type=float, default=2.0, help="Worker process restart backoff base.")
    parser.add_argument("--restart-max-s", type=float, default=60.0, help="Worker process restart backoff max.")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Orchestrator log level.",
    )
    parser.add_argument(
        "--log-file",
        default="logs/orchestrator_redis_auto_shard.log",
        help="Orchestrator log file path.",
    )
    return parser.parse_args()


def build_shard_plan(
    runnable_profiles: list[Any],
    workers: int,
    args: argparse.Namespace,
    publisher_script_path: Path,
) -> list[WorkerPlan]:
    logs_dir = Path(args.logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    bins: list[WorkerPlan] = []
    for worker_id in range(workers):
        bins.append(
            WorkerPlan(
                worker_id=worker_id,
                exchanges=[],
                score_total=0.0,
                db_path=args.redis_url,
                command=[],
                log_path=str(logs_dir / f"redis_worker_{worker_id}.log"),
            )
        )

    for profile in sorted(runnable_profiles, key=lambda item: item.score, reverse=True):
        target = min(bins, key=lambda item: item.score_total)
        target.exchanges.append(profile.exchange_id)
        target.score_total += profile.score

    for plan in bins:
        command = [
            sys.executable,
            str(publisher_script_path),
            "--redis-url",
            args.redis_url,
            "--stream",
            args.stream,
            "--stream-maxlen",
            str(args.stream_maxlen),
            "--exchanges",
            ",".join(plan.exchanges),
            "--log-level",
            args.worker_log_level,
            "--queue-size",
            str(args.queue_size),
            "--exchange-start-delay-s",
            str(args.exchange_start_delay_s),
            "--reconnect-base-s",
            str(args.reconnect_base_s),
            "--reconnect-max-s",
            str(args.reconnect_max_s),
            "--redis-retry-s",
            str(args.redis_retry_s),
            "--print-every",
            str(args.print_every),
            "--duration-seconds",
            str(args.duration_seconds),
        ]
        if args.only_spot:
            command.append("--only-spot")
        if args.only_watch_tickers:
            command.append("--only-watch-tickers")
        if args.max_symbols_per_exchange is not None:
            command.extend(["--max-symbols-per-exchange", str(args.max_symbols_per_exchange)])
        if args.max_symbol_streams_per_exchange is not None:
            command.extend(["--max-symbol-streams-per-exchange", str(args.max_symbol_streams_per_exchange)])
        if args.symbols:
            command.extend(["--symbols", args.symbols])
        if args.producer_id_prefix:
            command.extend(["--producer-id-prefix", args.producer_id_prefix])
        plan.command = command

    return [plan for plan in bins if plan.exchanges]


def main() -> None:
    args = parse_args()
    ensure_ccxtpro_available()
    configure_logging_with_file(args.log_level, Path(args.log_file))
    LOGGER.info("File logging enabled: %s", args.log_file)

    if args.plan_input:
        plan_input_path = Path(args.plan_input)
        worker_plans_sorted = load_plan_json(plan_input_path, Path(args.logs_dir))
        LOGGER.info("Loaded %s worker plans from %s.", len(worker_plans_sorted), plan_input_path)
        if args.dry_run:
            LOGGER.info("Dry-run mode active with --plan-input. No workers spawned.")
            return
        supervise_workers(worker_plans_sorted, args)
        return

    available_exchanges = list(getattr(ccxtpro, "exchanges", []))
    requested_exchanges = parse_exchange_list(args.exchanges, available_exchanges)
    excluded_exchanges = resolve_excluded_exchanges(
        args.exchanges_exclude,
        available_exchanges,
        DEFAULT_EXCLUDED_EXCHANGES,
    )
    selected_exchanges = [exchange_id for exchange_id in requested_exchanges if exchange_id not in excluded_exchanges]
    excluded_from_selection = sorted(set(requested_exchanges) - set(selected_exchanges))
    if not selected_exchanges:
        raise SystemExit("No valid exchanges available after exclusions.")

    symbol_filters = parse_symbol_filters(args.symbols)
    LOGGER.info("Profiling %s exchanges for Redis publisher sharding...", len(selected_exchanges))
    profiles = asyncio.run(profile_exchanges(selected_exchanges, args, symbol_filters))

    runnable_profiles = [profile for profile in profiles if profile.error is None]
    if not runnable_profiles:
        raise SystemExit("No runnable exchanges left after profiling.")

    worker_count = pick_worker_count(
        exchange_count=len(runnable_profiles),
        requested_workers=args.workers,
        max_auto_workers=args.max_auto_workers,
    )
    publisher_script_path = Path(__file__).resolve().parent / "ccxt_to_redis_stream.py"
    worker_plans = build_shard_plan(runnable_profiles, worker_count, args, publisher_script_path)
    worker_plans_sorted = sorted(worker_plans, key=lambda item: item.worker_id)

    for plan in worker_plans_sorted:
        LOGGER.info(
            "Redis worker %s plan | exchanges=%s | score_total=%.1f",
            plan.worker_id,
            len(plan.exchanges),
            plan.score_total,
        )

    plan_output = Path(args.plan_output)
    write_plan_json(profiles, worker_plans_sorted, excluded_from_selection, plan_output)
    LOGGER.info("Plan written to %s", plan_output)

    if args.dry_run:
        LOGGER.info("Dry-run mode active. No workers spawned.")
        return

    supervise_workers(worker_plans_sorted, args)


if __name__ == "__main__":
    main()
