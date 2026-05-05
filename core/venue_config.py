"""
Load venue-level adapter config (account currency + FX conversion rules).

The admin panel writes these configs to `adapter_admin/adapters_config/*.json`.
At backtest time we need to know:
  * which currency the account reports PnL in (account_base_currency)
  * how to convert non-base-currency PnL back to the base (fx_conversion)

A backtest receives a bar_type_str like "USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL".
We parse the venue ("FOREX_MS") from the InstrumentId, scan the configs dir for
the matching venue, and return that config dict. Returns None when no matching
adapter is configured — callers treat that as "use built-in USD defaults".
"""

from __future__ import annotations

import json
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_ADAPTERS_CONFIG_DIR = _PROJECT_ROOT / "adapter_admin" / "adapters_config"


def venue_from_bar_type(bar_type_str: str) -> str | None:
    """Extract the venue from a bar type string.

    "USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL" -> "FOREX_MS".
    "BTCUSD.BINANCE-1-DAY-LAST-EXTERNAL"    -> "BINANCE".
    """
    if not bar_type_str:
        return None
    instrument_id_part = bar_type_str.split("-", 1)[0]  # "USDJPY.FOREX_MS"
    if "." not in instrument_id_part:
        return None
    return instrument_id_part.split(".", 1)[1].strip() or None


def load_adapter_config_for_venue(
    venue: str | None,
    configs_dir: Path | str | None = None,
) -> dict | None:
    """Return the adapter config dict for `venue`, or None if not found.

    Matches on config["venue"] (case-insensitive) rather than filename so
    renaming the adapter file doesn't break backtests.
    """
    if not venue:
        return None
    config_dir = Path(configs_dir) if configs_dir is not None else _ADAPTERS_CONFIG_DIR
    if not config_dir.exists():
        return None
    target = venue.upper()
    for f in config_dir.glob("*.json"):
        try:
            config = json.loads(f.read_text(encoding="utf-8"))
        except Exception:
            continue
        if (config.get("venue") or "").upper() == target:
            return config
    return None


def load_adapter_config_for_bar_type(
    bar_type_str: str,
    configs_dir: Path | str | None = None,
) -> dict | None:
    """Convenience: parse the venue from a bar type and load the config."""
    return load_adapter_config_for_venue(venue_from_bar_type(bar_type_str), configs_dir)


def symbol_from_bar_type(bar_type_str: str) -> str | None:
    """Extract the bare symbol from a bar type string.

    "USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL" -> "USDJPY".
    """
    if not bar_type_str:
        return None
    instrument_id_part = bar_type_str.split("-", 1)[0]  # "USDJPY.FOREX_MS"
    if "." not in instrument_id_part:
        return instrument_id_part or None
    return instrument_id_part.split(".", 1)[0].strip() or None


def load_instrument_config(
    symbol: str | None,
    venue: str | None,
    configs_dir: Path | str | None = None,
) -> dict | None:
    """Return per-instrument settings from the venue's adapter config.

    Looks up ``config["instruments"][symbol]`` (case-insensitive on symbol).
    Expected shape: ``{"lot_size": <number>, "trade_size": <cap>}``.
    Returns None when the venue config is missing, lacks an ``instruments``
    block, or doesn't list this symbol.
    """
    if not symbol:
        return None
    cfg = load_adapter_config_for_venue(venue, configs_dir)
    if not cfg:
        return None
    instruments = cfg.get("instruments") or {}
    target = symbol.upper()
    for key, value in instruments.items():
        if isinstance(key, str) and key.upper() == target:
            return value if isinstance(value, dict) else None
    return None


def load_instrument_config_for_bar_type(
    bar_type_str: str,
    configs_dir: Path | str | None = None,
) -> dict | None:
    """Convenience: parse symbol + venue from a bar type and load the config."""
    return load_instrument_config(
        symbol_from_bar_type(bar_type_str),
        venue_from_bar_type(bar_type_str),
        configs_dir,
    )
