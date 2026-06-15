"""Bar-type string reasoning: BID/ASK/MID fill pairing, the leg's
aggregation-target external bar type, and slot grouping by engine key."""

from __future__ import annotations

from nautilus_trader.model.data import BarType


def _aggregate_target_for_slot(slot, base_bar_type) -> str:
    """Plain EXTERNAL bar type the leg's first strategy timeframe aggregates to,
    or "" when none is selected or it equals the base. Mirrors the logic in
    ``config_from_exit`` so the raw (non-ManagedExit) strategy path — used when a
    leg has no SL/TP/squareoff/RBO/run_on_days — aggregates identically to the
    managed path instead of silently dropping the aggregation."""
    from core.aggregator import external_from_composite
    sbts = [str(s) for s in (getattr(slot, "strategy_bar_types", None) or []) if s]
    if not sbts:
        return ""
    ext = external_from_composite(sbts[0])
    if not ext:
        return ""
    try:
        if BarType.from_str(ext) != base_bar_type:
            return ext
    except Exception:  # noqa: BLE001 — malformed → no aggregation
        return ""
    return ""


def _group_slots(
    enabled_slots: list,
    capitals_by_slot_id: dict[str, float],
    default_start_date: str | None,
    default_end_date: str | None,
    custom_strategies_dir: str | None,
) -> list[list[tuple]]:
    """Group slots that share the same (bar_type, start, end, custom_strategies_dir).

    Each group is a list of (slot, capital) tuples. Groups of size 1 are still
    emitted — callers decide whether to treat them as shared-engine or fall
    back to the per-slot engine path.

    Grouping key intentionally excludes strategy_name / strategy_params / trade_size
    — those legitimately differ across the strategies that should share an engine.
    """
    groups: dict[tuple, list[tuple]] = {}
    for slot in enabled_slots:
        start = slot.start_date or default_start_date
        end = slot.end_date or default_end_date
        key = (slot.bar_type_str, start, end, custom_strategies_dir)
        groups.setdefault(key, []).append((slot, capitals_by_slot_id[slot.slot_id]))
    return list(groups.values())


def _pair_bid_ask_bar_type(bt_str: str) -> list[str]:
    """Return additional bar type strings needed for realistic fills.

    Nautilus's matching engine needs both quote sides to fill FX market
    orders. A strategy subscribed only to BID sees no fills unless ASK is
    also loaded (and vice versa). MID slots need both ASK and BID so the
    engine can fill at real spread prices instead of the midpoint.
    LAST bar types don't need a pair.
    """
    if "-BID-" in bt_str:
        return [bt_str.replace("-BID-", "-ASK-", 1)]
    if "-ASK-" in bt_str:
        return [bt_str.replace("-ASK-", "-BID-", 1)]
    if "-MID-" in bt_str:
        return [
            bt_str.replace("-MID-", "-ASK-", 1),
            bt_str.replace("-MID-", "-BID-", 1),
        ]
    return []


def _same_ts_sort_key(bar):
    """Project-wide same-timestamp ordering: **quotes before MID** (ASK, then BID,
    then MID) at each ``ts_init``. Deterministic by contract (spec §4.1/§4.2 — an
    exit's conservative fill should see the current minute's bid/ask before the MID
    trigger), replacing the old incidental load-order. Non-FX (LAST) single-stream
    bars get a neutral rank (no same-ts pairing). Use as ``bars.sort(key=...)`` in
    every path that feeds the engine so all engines agree on the order."""
    s = str(bar.bar_type)
    rank = 3
    if "-ASK-" in s:
        rank = 0
    elif "-BID-" in s:
        rank = 1
    elif "-MID-" in s:
        rank = 2
    return (bar.ts_init, rank)


def _normalize_primary_to_mid(bt_str: str) -> str:
    """Force an FX bid/ask primary to its MID variant.

    The signal / SL-TP-trigger decision should run on the unbiased **midpoint**,
    not a single quote side (ASK-only / BID-only bakes a half-spread skew into the
    indicators even though trades go both ways). Fills stay direction-correct
    regardless (BUY→ASK, SELL→BID) via the matching engine + the §4.2 logic, so the
    primary only carries the *decision* price. ``-ASK-`` / ``-BID-`` → ``-MID-``;
    ``-MID-`` and crypto ``-LAST-`` (no midpoint) are returned unchanged.
    """
    if "-ASK-" in bt_str:
        return bt_str.replace("-ASK-", "-MID-", 1)
    if "-BID-" in bt_str:
        return bt_str.replace("-BID-", "-MID-", 1)
    return bt_str
