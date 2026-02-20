#!/usr/bin/env python3
"""
Auto-sharding orchestrator for websocket ingestion workers.

What this script does:
1. Profiles exchange traffic load over a short sampling window.
2. Computes a load score per exchange.
3. Distributes exchanges across workers with greedy bin packing.
4. Spawns and supervises ingestion worker processes automatically.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

try:
    import ccxt.pro as ccxtpro
except ImportError as exc:
    raise SystemExit(
        "ccxt.pro is not installed. Please install it (e.g. pip install ccxt)."
    ) from exc

from ingestion_common import (
    DEFAULT_EXCLUDED_EXCHANGES,
    configure_logging_with_file,
    is_terminal_exclusion_error,
    iter_tickers,
    parse_exchange_list,
    resolve_excluded_exchanges,
    select_symbols,
    supports_ws_flag,
    terminal_error_reason,
)


LOGGER = logging.getLogger("orchestrator_auto_shard")


@dataclass
class ExchangeProfile:
    exchange_id: str
    mode: str
    symbols_total: int
    symbols_profiled: int
    ticks: int
    bytes_total: int
    reconnects: int
    duration_s: float
    error: str | None
    terminal_excluded: bool
    ticks_per_sec: float
    bytes_per_sec: float
    reconnects_per_min: float
    score: float


@dataclass
class WorkerPlan:
    worker_id: int
    exchanges: list[str]
    score_total: float
    db_path: str
    command: list[str]
    log_path: str


class _ProfileAccumulator:
    def __init__(self) -> None:
        self.ticks = 0
        self.bytes_total = 0
        self.reconnects = 0

    def add_ticker(self, ticker: dict[str, Any]) -> None:
        self.ticks += 1
        # Approximate transfer size from serialized payload.
        self.bytes_total += len(json.dumps(ticker, ensure_ascii=True, default=str, separators=(",", ":")))

    def add_reconnect(self) -> None:
        self.reconnects += 1


class TerminalProfileError(Exception):
    def __init__(self, reason: str, details: str) -> None:
        super().__init__(details)
        self.reason = reason
        self.details = details


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Profile exchange traffic, auto-shard exchanges, and spawn ingestion worker processes."
    )
    parser.add_argument(
        "--exchanges",
        default=None,
        help="Comma-separated list of exchange IDs. Default: all ccxt.pro exchanges.",
    )
    parser.add_argument(
        "--exchanges-exclude",
        default=None,
        help="Comma-separated list of exchange IDs to additionally exclude from profiling and execution.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="Fixed worker count. Default: auto.",
    )
    parser.add_argument(
        "--max-auto-workers",
        type=int,
        default=8,
        help="Maximum worker count in auto mode.",
    )
    parser.add_argument(
        "--profile-seconds",
        type=float,
        default=30.0,
        help="Sampling window per exchange in seconds.",
    )
    parser.add_argument(
        "--profile-concurrency",
        type=int,
        default=4,
        help="How many exchanges to profile in parallel.",
    )
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
    parser.add_argument(
        "--only-spot",
        action="store_true",
        help="Use spot markets only.",
    )
    parser.add_argument(
        "--only-watch-tickers",
        action="store_true",
        help="Use only exchanges with watchTickers capability.",
    )
    parser.add_argument(
        "--max-symbols-per-exchange",
        type=int,
        default=None,
        help="Optional symbol cap per exchange passed to workers.",
    )
    parser.add_argument(
        "--max-symbol-streams-per-exchange",
        type=int,
        default=None,
        help="Optional watch_ticker symbol stream cap passed to workers.",
    )
    parser.add_argument(
        "--worker-db-template",
        default="data/worker_{worker_id}_crypto_ws_ticks.db",
        help="Output DB path template per worker.",
    )
    parser.add_argument(
        "--logs-dir",
        default="logs",
        help="Directory for worker log files.",
    )
    parser.add_argument(
        "--plan-output",
        default="data/orchestrator_plan.json",
        help="Path to write profiling and shard plan JSON.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only profile and create plan. Do not spawn worker processes.",
    )
    parser.add_argument(
        "--worker-log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Log level forwarded to worker ingestion processes.",
    )
    parser.add_argument("--queue-size", type=int, default=200000, help="Forwarded worker queue size.")
    parser.add_argument("--batch-size", type=int, default=500, help="Forwarded worker SQLite insert batch size.")
    parser.add_argument(
        "--flush-interval-s",
        type=float,
        default=1.0,
        help="Forwarded worker SQLite flush interval.",
    )
    parser.add_argument(
        "--exchange-start-delay-s",
        type=float,
        default=0.2,
        help="Forwarded worker delay between exchange worker starts.",
    )
    parser.add_argument("--reconnect-base-s", type=float, default=1.0, help="Forwarded worker reconnect backoff base.")
    parser.add_argument("--reconnect-max-s", type=float, default=30.0, help="Forwarded worker reconnect backoff max.")
    parser.add_argument(
        "--restart-base-s",
        type=float,
        default=2.0,
        help="Worker process restart backoff base.",
    )
    parser.add_argument(
        "--restart-max-s",
        type=float,
        default=60.0,
        help="Worker process restart backoff max.",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Orchestrator log level.",
    )
    parser.add_argument(
        "--log-file",
        default="logs/orchestrator_auto_shard.log",
        help="Orchestrator log file path.",
    )
    return parser.parse_args()


def limit_symbols(symbols: list[str], profile_symbol_limit: int) -> list[str]:
    if profile_symbol_limit < 0:
        return symbols
    return symbols[:profile_symbol_limit]


async def profile_watch_tickers(
    exchange: Any,
    symbols: list[str],
    deadline: float,
    request_timeout_s: float,
    acc: _ProfileAccumulator,
) -> None:
    use_symbol_list = True
    reconnect_backoff = 0.5
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        timeout_s = min(request_timeout_s, remaining)
        try:
            if use_symbol_list:
                payload = await asyncio.wait_for(exchange.watch_tickers(symbols), timeout=timeout_s)
            else:
                payload = await asyncio.wait_for(exchange.watch_tickers(), timeout=timeout_s)
            for ticker in iter_tickers(payload):
                acc.add_ticker(ticker)
            reconnect_backoff = 0.5
        except TypeError:
            if use_symbol_list:
                use_symbol_list = False
                continue
            acc.add_reconnect()
            await asyncio.sleep(min(reconnect_backoff, max(0.0, deadline - time.monotonic())))
            reconnect_backoff = min(reconnect_backoff * 2.0, 5.0)
        except asyncio.TimeoutError:
            # No update within timeout. Continue sampling.
            continue
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                raise TerminalProfileError(terminal_error_reason(exc), repr(exc)) from exc
            acc.add_reconnect()
            await asyncio.sleep(min(reconnect_backoff, max(0.0, deadline - time.monotonic())))
            reconnect_backoff = min(reconnect_backoff * 2.0, 5.0)


async def _profile_watch_ticker_symbol(
    exchange: Any,
    symbol: str,
    deadline: float,
    request_timeout_s: float,
    acc: _ProfileAccumulator,
    terminal_error_queue: asyncio.Queue[TerminalProfileError],
) -> None:
    reconnect_backoff = 0.5
    while time.monotonic() < deadline:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        timeout_s = min(request_timeout_s, remaining)
        try:
            ticker = await asyncio.wait_for(exchange.watch_ticker(symbol), timeout=timeout_s)
            if isinstance(ticker, dict):
                acc.add_ticker(ticker)
            reconnect_backoff = 0.5
        except asyncio.TimeoutError:
            continue
        except Exception as exc:  # noqa: BLE001
            if is_terminal_exclusion_error(exc):
                await terminal_error_queue.put(TerminalProfileError(terminal_error_reason(exc), repr(exc)))
                return
            acc.add_reconnect()
            await asyncio.sleep(min(reconnect_backoff, max(0.0, deadline - time.monotonic())))
            reconnect_backoff = min(reconnect_backoff * 2.0, 5.0)


async def profile_watch_ticker(
    exchange: Any,
    symbols: list[str],
    deadline: float,
    request_timeout_s: float,
    acc: _ProfileAccumulator,
) -> None:
    terminal_error_queue: asyncio.Queue[TerminalProfileError] = asyncio.Queue(maxsize=1)
    tasks = [
        asyncio.create_task(
            _profile_watch_ticker_symbol(
                exchange=exchange,
                symbol=symbol,
                deadline=deadline,
                request_timeout_s=request_timeout_s,
                acc=acc,
                terminal_error_queue=terminal_error_queue,
            ),
            name=f"profile:{symbol}",
        )
        for symbol in symbols
    ]
    try:
        while time.monotonic() < deadline:
            if not terminal_error_queue.empty():
                raise await terminal_error_queue.get()
            if all(task.done() for task in tasks):
                return
            await asyncio.sleep(0.1)
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


def make_profile(
    exchange_id: str,
    mode: str,
    symbols_total: int,
    symbols_profiled: int,
    acc: _ProfileAccumulator,
    duration_s: float,
    error: str | None,
    terminal_excluded: bool,
) -> ExchangeProfile:
    safe_duration = max(duration_s, 1e-9)
    ticks_per_sec = acc.ticks / safe_duration
    bytes_per_sec = acc.bytes_total / safe_duration
    reconnects_per_min = acc.reconnects / max(safe_duration / 60.0, 1e-9)
    score = bytes_per_sec + 2000.0 * reconnects_per_min
    if error is None:
        score = max(score, 1.0)
    else:
        score = 0.0
    return ExchangeProfile(
        exchange_id=exchange_id,
        mode=mode,
        symbols_total=symbols_total,
        symbols_profiled=symbols_profiled,
        ticks=acc.ticks,
        bytes_total=acc.bytes_total,
        reconnects=acc.reconnects,
        duration_s=duration_s,
        error=error,
        terminal_excluded=terminal_excluded,
        ticks_per_sec=ticks_per_sec,
        bytes_per_sec=bytes_per_sec,
        reconnects_per_min=reconnects_per_min,
        score=score,
    )


async def profile_exchange(exchange_id: str, args: argparse.Namespace) -> ExchangeProfile:
    exchange_cls = getattr(ccxtpro, exchange_id, None)
    if exchange_cls is None:
        return make_profile(
            exchange_id=exchange_id,
            mode="unavailable",
            symbols_total=0,
            symbols_profiled=0,
            acc=_ProfileAccumulator(),
            duration_s=0.0,
            error="exchange class not found in ccxt.pro",
            terminal_excluded=True,
        )

    exchange = exchange_cls({"enableRateLimit": True, "newUpdates": True})
    start = time.monotonic()
    acc = _ProfileAccumulator()
    mode = "unknown"
    symbols_total = 0
    symbols_profiled = 0
    error: str | None = None
    terminal_excluded = False

    try:
        markets = await exchange.load_markets()
        symbols = select_symbols(markets, args.only_spot, args.max_symbols_per_exchange)
        symbols_total = len(symbols)
        symbols_for_profile = limit_symbols(symbols, args.profile_symbol_limit)
        symbols_profiled = len(symbols_for_profile)

        watch_tickers_supported = supports_ws_flag(exchange.has.get("watchTickers"))
        watch_ticker_supported = supports_ws_flag(exchange.has.get("watchTicker"))

        if args.only_watch_tickers and not watch_tickers_supported:
            mode = "excluded_no_watch_tickers"
            error = "watchTickers not supported"
            terminal_excluded = True
            return make_profile(
                exchange_id=exchange_id,
                mode=mode,
                symbols_total=symbols_total,
                symbols_profiled=symbols_profiled,
                acc=acc,
                duration_s=time.monotonic() - start,
                error=error,
                terminal_excluded=terminal_excluded,
            )

        if watch_tickers_supported:
            mode = "watch_tickers"
            deadline = time.monotonic() + args.profile_seconds
            await profile_watch_tickers(
                exchange=exchange,
                symbols=symbols_for_profile,
                deadline=deadline,
                request_timeout_s=args.profile_request_timeout_s,
                acc=acc,
            )
        elif watch_ticker_supported:
            mode = "watch_ticker"
            if not symbols_for_profile:
                error = "no active symbols available for profiling"
            else:
                deadline = time.monotonic() + args.profile_seconds
                await profile_watch_ticker(
                    exchange=exchange,
                    symbols=symbols_for_profile,
                    deadline=deadline,
                    request_timeout_s=args.profile_request_timeout_s,
                    acc=acc,
                )
        else:
            mode = "unsupported"
            error = "watchTickers and watchTicker are not supported"
            terminal_excluded = True

    except TerminalProfileError as exc:
        error = f"{exc.reason}: {exc.details}"
        terminal_excluded = True
    except Exception as exc:  # noqa: BLE001
        error = repr(exc)
        terminal_excluded = is_terminal_exclusion_error(exc)
    finally:
        try:
            await exchange.close()
        except Exception:  # noqa: BLE001
            pass

    duration_s = time.monotonic() - start
    return make_profile(
        exchange_id=exchange_id,
        mode=mode,
        symbols_total=symbols_total,
        symbols_profiled=symbols_profiled,
        acc=acc,
        duration_s=duration_s,
        error=error,
        terminal_excluded=terminal_excluded,
    )


async def profile_exchanges(exchange_ids: list[str], args: argparse.Namespace) -> list[ExchangeProfile]:
    semaphore = asyncio.Semaphore(max(1, args.profile_concurrency))

    async def _run(exchange_id: str) -> ExchangeProfile:
        async with semaphore:
            LOGGER.info("Profiling exchange %s...", exchange_id)
            profile = await profile_exchange(exchange_id, args)
            if profile.error is None:
                LOGGER.info(
                    "Profiled %s | mode=%s | ticks=%s | bytes/s=%.1f | score=%.1f",
                    exchange_id,
                    profile.mode,
                    profile.ticks,
                    profile.bytes_per_sec,
                    profile.score,
                )
            else:
                LOGGER.warning(
                    "Profile failed for %s | mode=%s | terminal_excluded=%s | error=%s",
                    exchange_id,
                    profile.mode,
                    profile.terminal_excluded,
                    profile.error,
                )
            return profile

    return await asyncio.gather(*(_run(exchange_id) for exchange_id in exchange_ids))


def pick_worker_count(exchange_count: int, requested_workers: int | None, max_auto_workers: int) -> int:
    if requested_workers is not None:
        return max(1, requested_workers)
    cpu_count = os.cpu_count() or 2
    auto = min(max_auto_workers, cpu_count, max(1, exchange_count))
    return max(1, auto)


def build_shard_plan(
    runnable_profiles: list[ExchangeProfile],
    workers: int,
    args: argparse.Namespace,
    ingestion_script_path: Path,
) -> list[WorkerPlan]:
    bins: list[WorkerPlan] = []
    logs_dir = Path(args.logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)

    for worker_id in range(workers):
        db_path = args.worker_db_template.format(worker_id=worker_id)
        log_path = str(logs_dir / f"worker_{worker_id}.log")
        bins.append(
            WorkerPlan(
                worker_id=worker_id,
                exchanges=[],
                score_total=0.0,
                db_path=db_path,
                command=[],
                log_path=log_path,
            )
        )

    for profile in sorted(runnable_profiles, key=lambda item: item.score, reverse=True):
        target = min(bins, key=lambda item: item.score_total)
        target.exchanges.append(profile.exchange_id)
        target.score_total += profile.score

    for plan in bins:
        command = [
            sys.executable,
            str(ingestion_script_path),
            "--db-path",
            plan.db_path,
            "--exchanges",
            ",".join(plan.exchanges),
            "--log-level",
            args.worker_log_level,
            "--queue-size",
            str(args.queue_size),
            "--batch-size",
            str(args.batch_size),
            "--flush-interval-s",
            str(args.flush_interval_s),
            "--exchange-start-delay-s",
            str(args.exchange_start_delay_s),
            "--reconnect-base-s",
            str(args.reconnect_base_s),
            "--reconnect-max-s",
            str(args.reconnect_max_s),
        ]
        if args.only_spot:
            command.append("--only-spot")
        if args.only_watch_tickers:
            command.append("--only-watch-tickers")
        if args.max_symbols_per_exchange is not None:
            command.extend(["--max-symbols-per-exchange", str(args.max_symbols_per_exchange)])
        if args.max_symbol_streams_per_exchange is not None:
            command.extend(["--max-symbol-streams-per-exchange", str(args.max_symbol_streams_per_exchange)])
        plan.command = command

    # Remove empty workers so no process is spawned with empty exchange list.
    return [plan for plan in bins if plan.exchanges]


def write_plan_json(
    profiles: list[ExchangeProfile],
    worker_plans: list[WorkerPlan],
    excluded_from_selection: list[str],
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "excluded_from_selection": excluded_from_selection,
        "profiles": [asdict(profile) for profile in profiles],
        "workers": [asdict(worker) for worker in worker_plans],
    }
    output_path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")


def terminate_process(process: subprocess.Popen[Any], timeout_s: float = 10.0) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=timeout_s)


def supervise_workers(worker_plans: list[WorkerPlan], args: argparse.Namespace) -> None:
    runtimes: dict[int, dict[str, Any]] = {}
    stop_requested = False

    def _request_stop(*_: Any) -> None:
        nonlocal stop_requested
        if not stop_requested:
            LOGGER.info("Stop signal received. Terminating workers...")
            stop_requested = True

    signal.signal(signal.SIGINT, _request_stop)
    signal.signal(signal.SIGTERM, _request_stop)

    for plan in worker_plans:
        log_file = Path(plan.log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handle = log_file.open("a", encoding="utf-8")
        process = subprocess.Popen(  # noqa: S603
            plan.command,
            stdout=handle,
            stderr=subprocess.STDOUT,
            cwd=str(Path(__file__).resolve().parent.parent),
        )
        runtimes[plan.worker_id] = {
            "plan": plan,
            "process": process,
            "log_handle": handle,
            "restart_count": 0,
            "next_restart_ts": 0.0,
        }
        LOGGER.info(
            "Spawned worker %s | exchanges=%s | pid=%s | log=%s",
            plan.worker_id,
            len(plan.exchanges),
            process.pid,
            plan.log_path,
        )

    try:
        while not stop_requested:
            for worker_id, runtime in runtimes.items():
                process: subprocess.Popen[Any] = runtime["process"]
                return_code = process.poll()
                if return_code is None:
                    continue
                now = time.monotonic()
                if now < runtime["next_restart_ts"]:
                    continue

                backoff = min(args.restart_base_s * (2 ** runtime["restart_count"]), args.restart_max_s)
                runtime["restart_count"] += 1
                runtime["next_restart_ts"] = now + backoff

                LOGGER.warning(
                    "Worker %s exited with code %s. Restarting in %.1fs.",
                    worker_id,
                    return_code,
                    backoff,
                )

                handle = runtime["log_handle"]
                if not handle.closed:
                    handle.flush()

                time.sleep(backoff)
                if stop_requested:
                    break
                plan = runtime["plan"]
                new_process = subprocess.Popen(  # noqa: S603
                    plan.command,
                    stdout=handle,
                    stderr=subprocess.STDOUT,
                    cwd=str(Path(__file__).resolve().parent.parent),
                )
                runtime["process"] = new_process
                LOGGER.info("Worker %s restarted with pid=%s.", worker_id, new_process.pid)

            time.sleep(1.0)
    finally:
        for runtime in runtimes.values():
            process: subprocess.Popen[Any] = runtime["process"]
            handle = runtime["log_handle"]
            terminate_process(process)
            if not handle.closed:
                handle.flush()
                handle.close()


def main() -> None:
    args = parse_args()
    configure_logging_with_file(args.log_level, Path(args.log_file))
    LOGGER.info("File logging enabled: %s", args.log_file)

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

    if excluded_from_selection:
        LOGGER.info(
            "Exchanges excluded from selection (%s): %s",
            len(excluded_from_selection),
            ", ".join(excluded_from_selection),
        )

    LOGGER.info("Profiling %s exchanges...", len(selected_exchanges))
    profiles = asyncio.run(profile_exchanges(selected_exchanges, args))

    runnable_profiles = [profile for profile in profiles if profile.error is None]
    excluded_profiles = [profile for profile in profiles if profile.error is not None]

    for profile in excluded_profiles:
        LOGGER.warning(
            "Exchange excluded from execution: %s | mode=%s | terminal=%s | error=%s",
            profile.exchange_id,
            profile.mode,
            profile.terminal_excluded,
            profile.error,
        )

    if not runnable_profiles:
        raise SystemExit("No runnable exchanges left after profiling.")

    worker_count = pick_worker_count(
        exchange_count=len(runnable_profiles),
        requested_workers=args.workers,
        max_auto_workers=args.max_auto_workers,
    )

    ingestion_script_path = Path(__file__).resolve().parent / "ingest_all_exchanges_ws.py"
    worker_plans = build_shard_plan(runnable_profiles, worker_count, args, ingestion_script_path)
    worker_plans_sorted = sorted(worker_plans, key=lambda item: item.worker_id)

    for plan in worker_plans_sorted:
        LOGGER.info(
            "Worker %s plan | exchanges=%s | score_total=%.1f",
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
