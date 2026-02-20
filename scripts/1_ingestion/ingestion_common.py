#!/usr/bin/env python3
"""
Shared configuration and helper functions for ingestion scripts.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Iterable

try:
    from ccxt.base.errors import AuthenticationError as CcxtAuthenticationError
except Exception:  # noqa: BLE001
    CcxtAuthenticationError = None


DEFAULT_EXCLUDED_EXCHANGES = {
    "alpaca",
    "arkham",
    "bequant",
    "bitfinex",
    "bitmex",
    "bitopro",
    "blockchaincom",
    "oxfun",
    "probit",
}

TERMINAL_EXCLUSION_ERROR_NAMES = {
    "authenticationerror",
    "exchangenotavailable",
    "requesttimeout",
    "exchangeerror",
}


def iter_tickers(payload: Any) -> Iterable[dict[str, Any]]:
    if isinstance(payload, dict):
        if "symbol" in payload:
            yield payload
            return
        for value in payload.values():
            if isinstance(value, dict) and value.get("symbol"):
                yield value
        return
    if isinstance(payload, list):
        for value in payload:
            if isinstance(value, dict) and value.get("symbol"):
                yield value


def select_symbols(
    markets: dict[str, dict[str, Any]],
    only_spot: bool,
    max_symbols: int | None,
) -> list[str]:
    symbols: list[str] = []
    for symbol, market in markets.items():
        if not isinstance(market, dict):
            continue
        if market.get("active") is False:
            continue
        if only_spot and not market.get("spot", False):
            continue
        symbols.append(symbol)
    symbols = sorted(set(symbols))
    if max_symbols is not None and max_symbols > 0:
        symbols = symbols[:max_symbols]
    return symbols


def supports_ws_flag(flag_value: Any) -> bool:
    return flag_value is True


def error_class_name(exc: Exception) -> str:
    return exc.__class__.__name__


def is_auth_error(exc: Exception) -> bool:
    if CcxtAuthenticationError is not None and isinstance(exc, CcxtAuthenticationError):
        return True
    error_name = error_class_name(exc).lower()
    if "authenticationerror" in error_name:
        return True
    message = str(exc).lower()
    auth_markers = (
        'requires "apikey" credential',
        "api key",
        "not authenticated",
        "authentication failed",
        "invalid api",
    )
    return any(marker in message for marker in auth_markers)


def is_terminal_exclusion_error(exc: Exception) -> bool:
    if is_auth_error(exc):
        return True
    return error_class_name(exc).lower() in TERMINAL_EXCLUSION_ERROR_NAMES


def terminal_error_reason(exc: Exception) -> str:
    return error_class_name(exc)


def parse_exchange_list(exchange_arg: str | None, available_exchanges: list[str]) -> list[str]:
    if not exchange_arg:
        return list(available_exchanges)
    selected = [item.strip().lower() for item in exchange_arg.split(",") if item.strip()]
    filtered = [exchange_id for exchange_id in selected if exchange_id in available_exchanges]
    return list(dict.fromkeys(filtered))


def parse_explicit_exchange_list(exchange_arg: str | None, available_exchanges: list[str]) -> list[str]:
    if not exchange_arg:
        return []
    selected = [item.strip().lower() for item in exchange_arg.split(",") if item.strip()]
    filtered = [exchange_id for exchange_id in selected if exchange_id in available_exchanges]
    return list(dict.fromkeys(filtered))


def resolve_excluded_exchanges(
    explicit_exclude_arg: str | None,
    available_exchanges: list[str],
    default_excluded: set[str] | None = None,
) -> set[str]:
    explicit_excluded = set(parse_explicit_exchange_list(explicit_exclude_arg, available_exchanges))
    defaults = set(default_excluded if default_excluded is not None else DEFAULT_EXCLUDED_EXCHANGES)
    return defaults | explicit_excluded


def configure_logging_with_file(
    log_level_name: str,
    log_file_path: Path | str,
    fmt: str = "%(asctime)s %(levelname)s %(name)s: %(message)s",
) -> None:
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)
    root_logger.setLevel(logging.DEBUG)

    console_level = getattr(logging, log_level_name.upper(), logging.INFO)
    file_level = logging.DEBUG if console_level == logging.DEBUG else logging.INFO

    formatter = logging.Formatter(fmt)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(console_level)
    console_handler.setFormatter(formatter)

    log_path = Path(log_file_path)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setLevel(file_level)
    file_handler.setFormatter(formatter)

    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)
