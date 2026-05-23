"""Runtime order-quantity resolution for a slot.

``effective_slot_qty`` is the single place that materializes the four sizing
tiers (admin instrument ``lot_size``, admin ``trade_size`` cap, user-slot
``lots``, per-user ``multiplier``) into the final order quantity the engine
uses. Uses lazy imports of ``core.venue_config`` / ``core.users`` to keep the
models package free of a top-level dependency on them.
"""

from __future__ import annotations

from typing import Optional


def effective_slot_qty(
    slot: "StrategySlotConfig",
    user_id: Optional[str] = None,
) -> float:
    """Compute the runtime order quantity for a slot.

    Resolves
    ``min(instrument.lot_size × slot.lots × user.multiplier,
    instrument.trade_size_cap)`` by reading the per-symbol admin config
    from the slot's ``bar_type_str`` and the per-user multiplier from
    ``config/users.json``.

    When the venue/symbol has no config entry, lot_size defaults to 1 (so
    the raw quantity is just ``slot.lots × multiplier``) and the cap is
    unbounded. When ``user_id`` is None or unknown, multiplier defaults to
    1.0 so legacy single-user call paths behave identically.

    This is the single place that materializes the four sizing tiers
    (admin-instrument lot_size, admin-instrument trade_size cap, user-slot
    lots, per-user multiplier) into one number. ``backtest_runner`` calls
    it when constructing each strategy's runtime config, so strategies
    themselves only ever see a final pre-resolved quantity.

    **Cap order matters.** The user multiplier is applied BEFORE the
    admin cap so the cap is a true hard ceiling — a runaway multiplier
    can't bust through. Documented as a deliberate authority boundary:
    admin policy wins over user preference.
    """
    try:
        from core.venue_config import load_instrument_config_for_bar_type
        inst_cfg = load_instrument_config_for_bar_type(slot.bar_type_str) or {}
    except Exception:
        inst_cfg = {}
    try:
        from core.users import get_multiplier
        multiplier = get_multiplier(user_id)
    except Exception:
        multiplier = 1.0
    lot_size = float(inst_cfg.get("lot_size") or 1) or 1.0
    raw = float(slot.lots) * lot_size * multiplier
    cap_raw = inst_cfg.get("trade_size")
    if cap_raw:
        cap = float(cap_raw)
        if cap > 0:
            return min(raw, cap)
    return raw
