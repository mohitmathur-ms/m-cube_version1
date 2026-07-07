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
import logging
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


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


def account_currency_code_for_venue(
    venue: str | None,
    configs_dir: Path | str | None = None,
) -> str:
    """Account base-currency CODE (e.g. "INR", "USD") for ``venue``, read from the
    adapter config's ``account_base_currency``.

    Defaults to "USD" when no adapter or currency is configured — preserving the
    historical USD-account behaviour for venues that don't declare one (FX/crypto),
    while INR-quoted venues (e.g. NIFTY_FUTURES_MS, which declares
    ``account_base_currency: "INR"``) now get an INR account so their account
    report reflects real INR balances instead of a frozen USD one.
    """
    cfg = load_adapter_config_for_venue(venue, configs_dir)
    code = (cfg or {}).get("account_base_currency") or "USD"
    return str(code).strip().upper() or "USD"


def account_currency_code_for_bar_type(
    bar_type_str: str,
    configs_dir: Path | str | None = None,
) -> str:
    """Same as :func:`account_currency_code_for_venue`, parsing the venue from a
    bar type string."""
    return account_currency_code_for_venue(venue_from_bar_type(bar_type_str), configs_dir)


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


def _venue_config_path(
    venue: str | None,
    configs_dir: Path | str | None = None,
) -> Path | None:
    """Return the path of the config file whose ``venue`` field matches.

    Matches on ``config["venue"]`` (case-insensitive), like
    :func:`load_adapter_config_for_venue`, so the lookup is independent of the
    filename. Returns ``None`` when no venue config matches.
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
            return f
    return None


def update_venue_session_window(
    venue: str | None,
    session_start_time: str | None,
    session_end_time: str | None,
    configs_dir: Path | str | None = None,
    mode: str = "union",
) -> dict | None:
    """Persist the venue's daily session window into its adapter config.

    ``session_start_time`` / ``session_end_time`` are zero-padded ``"HH:MM:SS"``
    UTC strings derived from data (see
    :func:`core.csv_loader.session_window_from_df`). Zero-padded times compare
    lexicographically the same as chronologically, so plain string min/max works.

    ``mode`` controls how the new window combines with any window already stored:

    * ``"union"`` (default) — *widen*: ``start = min(existing, new)``,
      ``end = max(existing, new)``. Used by the per-instrument ingest hook,
      where each load only sees one instrument's slice of the venue.
    * ``"set"`` — *overwrite* with the new window verbatim. Used by the
      full-catalog refresh (see :func:`core.session_windows.refresh_all_venue_session_windows`),
      whose ``(start, end)`` already covers every bar type on the venue and is
      therefore authoritative — this lets the window shrink if data was removed.

    The write is best-effort: any failure (no matching venue config,
    unreadable/locked JSON) is logged and swallowed — it never breaks ingest or
    request handling. Returns the updated config dict, or ``None`` when no write
    happened (no config, no input, or the stored window already matches).
    """
    if not venue or not session_start_time or not session_end_time:
        return None

    path = _venue_config_path(venue, configs_dir)
    if path is None:
        logger.debug("session_window: no adapter config for venue %s; skipping", venue)
        return None

    try:
        config = json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        logger.warning("session_window: could not read %s: %s", path.name, e)
        return None

    existing_start = config.get("session_start_time")
    existing_end = config.get("session_end_time")

    if mode == "set":
        new_start, new_end = session_start_time, session_end_time
    else:
        new_start = min(existing_start, session_start_time) if existing_start else session_start_time
        new_end = max(existing_end, session_end_time) if existing_end else session_end_time

    if new_start == existing_start and new_end == existing_end:
        return None  # already up to date — no write

    config["session_start_time"] = new_start
    config["session_end_time"] = new_end
    config["updated_at"] = datetime.now().isoformat()

    try:
        path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    except Exception as e:
        logger.warning("session_window: could not write %s: %s", path.name, e)
        return None

    logger.info(
        "session_window: %s -> %s..%s", venue, new_start, new_end
    )
    return config


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
