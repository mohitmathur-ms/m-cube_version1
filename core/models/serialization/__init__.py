"""Dict ↔ dataclass conversion + legacy-schema migration.

Turns a ``PortfolioConfig`` into a plain dict and back. ``portfolio_from_dict``
is schema-drift tolerant (``_filter_known_fields`` ignores unknown keys / fills
missing ones with defaults) and runs the legacy ``trade_size`` → ``lots``
migration (``_migrate_legacy_trade_size``) before constructing each slot.

"""

from __future__ import annotations

import dataclasses
from dataclasses import asdict

from core.models.exit_config import ExitConfig
from core.models.portfolio_config import PortfolioConfig
from core.models.strategy_slot_config import StrategySlotConfig


def portfolio_to_dict(config: PortfolioConfig) -> dict:
    return asdict(config)


def _filter_known_fields(data: dict, dataclass_type) -> dict:
    """Return only the keys in ``data`` that are fields of ``dataclass_type``.

    Defensive against schema drift in either direction: a UI that sends new
    fields the server doesn't know about gets a clean, ignore-the-extras load
    instead of a TypeError; an older payload missing newer fields gets the
    dataclass defaults filled in. Avoids the brittle `Class(**raw_data)` that
    crashes on the first mismatch.
    """
    known = {f.name for f in dataclasses.fields(dataclass_type)}
    return {k: v for k, v in data.items() if k in known}


def _migrate_legacy_trade_size(slot_data: dict) -> None:
    """Translate the deprecated slot-level ``trade_size`` into ``lots``.

    Older portfolio JSON had a single ``trade_size`` field on each slot that
    conflated the instrument's lot size with the user's multiplier (e.g.
    ``trade_size: 1000`` on an FX slot meant "1000 base-currency units").
    The new model has admin-configured ``lot_size`` on the instrument and
    user-configured ``lots`` on the slot, so ``order_qty = lot_size × lots``.

    To preserve the exact order quantity of legacy portfolios, derive
    ``lots = trade_size / lot_size`` (where lot_size comes from the venue
    config for the slot's symbol). When the venue config is missing or has no
    entry for this symbol, lot_size defaults to 1, so ``lots = trade_size`` —
    which gives the same per-bar quantity the legacy code path produced.
    Mutates ``slot_data`` in place. No-op when ``trade_size`` is absent or
    ``lots`` is already set explicitly.
    """
    if "trade_size" not in slot_data or "lots" in slot_data:
        slot_data.pop("trade_size", None)
        return
    legacy = slot_data.pop("trade_size")
    # Local import keeps models free of a top-level core.venue_config
    # dependency (avoids a circular import path during test collection).
    try:
        from core.venue_config import load_instrument_config_for_bar_type
        inst_cfg = load_instrument_config_for_bar_type(slot_data.get("bar_type_str", "")) or {}
    except Exception:
        inst_cfg = {}
    lot_size = float(inst_cfg.get("lot_size") or 1) or 1.0
    slot_data["lots"] = float(legacy) / lot_size


def portfolio_from_dict(data: dict) -> PortfolioConfig:
    # Don't mutate the caller's dict.
    data = dict(data)
    slots_data = data.pop("slots", [])
    slots = []
    for raw_slot in slots_data:
        slot_data = dict(raw_slot)
        _migrate_legacy_trade_size(slot_data)
        exit_data = slot_data.pop("exit_config", {}) or {}
        exit_config = ExitConfig(**_filter_known_fields(exit_data, ExitConfig))
        slots.append(StrategySlotConfig(
            exit_config=exit_config,
            **_filter_known_fields(slot_data, StrategySlotConfig),
        ))
    return PortfolioConfig(slots=slots, **_filter_known_fields(data, PortfolioConfig))
