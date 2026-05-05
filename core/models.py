"""
Portfolio configuration models.

Dataclasses for portfolio, strategy slot, and exit management configuration.
Includes JSON serialization helpers for saving/loading portfolios.
"""

from __future__ import annotations

import dataclasses
import json
import uuid
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional


@dataclass
class ExitConfig:
    """Exit management settings for a strategy slot."""

    # Stop Loss
    stop_loss_type: str = "none"  # "none", "percentage", "points", "trailing"
    stop_loss_value: float = 0.0
    trailing_sl_step: float = 0.0
    trailing_sl_offset: float = 0.0

    # Target / Take Profit
    target_type: str = "none"  # "none", "percentage", "points"
    target_value: float = 0.0

    # Target Locking
    target_lock_trigger: Optional[float] = None
    target_lock_minimum: Optional[float] = None

    # SL Wait (bars to confirm SL before executing)
    sl_wait_bars: int = 0

    # On Target/SL Actions
    on_sl_action: str = "close"      # "close", "re_execute", "reverse"
    on_target_action: str = "close"  # "close", "re_execute", "reverse"
    max_re_executions: int = 0

    # Leg-level square-off (most specific). HH:MM, e.g. "17:00".
    # When set, every day at this local time the position is force-closed and
    # no new entries are allowed until the next session.
    squareoff_time: Optional[str] = None
    squareoff_tz: Optional[str] = None  # IANA name, e.g. "America/New_York"

    def has_exit_management(self) -> bool:
        return (
            self.stop_loss_type != "none"
            or self.target_type != "none"
            or self.squareoff_time is not None
        )


@dataclass
class StrategySlotConfig:
    """One strategy instance within a portfolio."""

    slot_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    strategy_name: str = "EMA Cross"
    strategy_params: dict = field(default_factory=dict)
    bar_type_str: str = ""
    # Sizing multiplier (number of contracts). Combined with the instrument's
    # admin-configured lot_size at order-submit time:
    #   order_qty = min(instrument.lot_size × slot.lots, instrument.trade_size_cap)
    # FX example: lot_size=100_000 (one standard lot), lots=0.01 → 1000 base ccy.
    # Older portfolio JSON used a single ``trade_size`` field that conflated lot
    # size and multiplier; ``portfolio_from_dict`` migrates those values
    # automatically (see _migrate_legacy_trade_size).
    lots: float = 1.0
    allocation_pct: float = 0.0  # Capital allocation percentage (used when mode is "percentage")
    exit_config: ExitConfig = field(default_factory=ExitConfig)
    enabled: bool = True
    start_date: Optional[str] = None  # ISO date like "2024-01-01"
    end_date: Optional[str] = None    # ISO date like "2024-12-31"
    # Slot/strategy-level square-off override. Falls back to portfolio-level
    # if None. Leg-level (ExitConfig.squareoff_time) wins over both.
    squareoff_time: Optional[str] = None
    squareoff_tz: Optional[str] = None

    @property
    def display_name(self) -> str:
        instrument = self.bar_type_str.split(".")[0] if self.bar_type_str else "N/A"
        sl_desc = ""
        if self.exit_config.stop_loss_type == "percentage":
            sl_desc = f" | SL: {self.exit_config.stop_loss_value}%"
        elif self.exit_config.stop_loss_type == "points":
            sl_desc = f" | SL: {self.exit_config.stop_loss_value}pts"
        elif self.exit_config.stop_loss_type == "trailing":
            sl_desc = f" | SL: trailing {self.exit_config.trailing_sl_offset}%"
        tp_desc = ""
        if self.exit_config.target_type == "percentage":
            tp_desc = f" | TP: {self.exit_config.target_value}%"
        elif self.exit_config.target_type == "points":
            tp_desc = f" | TP: {self.exit_config.target_value}pts"
        return f"{self.strategy_name} on {instrument}{sl_desc}{tp_desc}"


@dataclass
class PortfolioConfig:
    """Groups multiple strategy slots with portfolio-level settings."""

    name: str = "New Portfolio"
    description: str = ""
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    updated_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    starting_capital: float = 100_000.0
    max_loss: Optional[float] = None
    max_profit: Optional[float] = None
    allocation_mode: str = "equal"  # "equal" or "percentage"
    start_date: Optional[str] = None  # ISO date applied to all slots unless a slot overrides it
    end_date: Optional[str] = None
    # Portfolio-level default square-off time. Slot or leg level can override.
    squareoff_time: Optional[str] = None  # "HH:MM"
    squareoff_tz: Optional[str] = None    # IANA name, e.g. "America/New_York"
    # Day-of-week filter for backtest. None / empty list means all 7 days allowed.
    # Values: subset of ["MON","TUE","WED","THU","FRI","SAT","SUN"] (case-insensitive,
    # full names like "Monday" also accepted). When set, bars on excluded weekdays
    # are dropped before the engine sees them, so trades cannot fire on those days.
    # Weekday is computed from each bar's UTC ts_event.
    run_on_days: Optional[list[str]] = None
    # Intra-day entry window. Both endpoints in HH:MM format, UTC.
    # When either is set, bars outside [entry_start_time, entry_end_time] are
    # dropped before the engine sees them. Caveat: bars dropped at the tail
    # mean the strategy can't process exits past entry_end_time, so set
    # ``squareoff_time`` to the same time as ``entry_end_time`` if you need
    # forced closes at end-of-window.
    entry_start_time: Optional[str] = None  # "HH:MM" UTC, e.g. "09:30"
    entry_end_time: Optional[str] = None    # "HH:MM" UTC, e.g. "16:00"

    # Range Breakout (RBO). When rbo_enabled, the entry timing for all enabled
    # slots is gated by a per-day state machine: range is built during
    # [range_monitoring_start, range_monitoring_end], frozen at the end of
    # that window, then breakouts above/below the range during
    # [rbo_entry_start, rbo_entry_end] arm strategy entries. Spec:
    # 5. Logics/rbo_logics.html. All times are HH:MM:SS UTC. range_buffer is
    # an integer number of MINUTES (NOT a price buffer) — extends entry_end
    # only until the first side fires, then collapses back per spec P6.
    rbo_enabled: bool = False
    range_monitoring_start: Optional[str] = None  # P2
    range_monitoring_end: Optional[str] = None    # P3 (UI auto-syncs rbo_entry_start to this)
    rbo_entry_start: Optional[str] = None         # P4
    rbo_entry_end: Optional[str] = None           # P5
    rbo_range_buffer: int = 0                     # P6 — minutes
    # P7. Backend-validated values: "Any" / "RangeHigh" / "RangeLow".
    # Options-only values "C_OnHigh_P_OnLow" / "P_OnHigh_C_OnLow" are silently
    # downgraded to "Any" with a warning log when used on FX/crypto.
    rbo_entry_at: str = "Any"
    # P8. Only "Underlying" is implemented; other values disable RBO with an
    # error log per spec validation rules.
    rbo_monitoring: str = "Underlying"
    rbo_cancel_other_side: bool = False           # P9

    # Other Settings tab. Spec: 5. Logics/Other_Settings_Logic.html.
    # delay_between_legs_sec: post-re-execution delay. Spec defines this at
    #   PORTFOLIO level (after a portfolio SL/TP triggers ReExecute). Our
    #   slot-independent architecture has no portfolio-level SL/TP that fires
    #   re-execution, so we apply this at the SLOT level: after a slot's own
    #   SL/TP triggers re_execute (per ExitConfig.on_sl_action / .on_target_action
    #   = "re_execute"), block the next entry on that slot until N seconds of
    #   bar-time have elapsed. Default 0 = no delay (matches spec).
    delay_between_legs_sec: int = 0
    # on_sl_action_on / on_target_action_on: filter that decides whether the
    # configured exit action fires when the SL/TP hit was the FIXED level vs
    # a TRAILING level that was moved by trailing_sl_step / target_lock.
    # Spec semantics defined for portfolio-level SL/TP; we adapt at slot
    # level using a was_trailed flag in ManagedExitStrategy.
    #   Values: "OnSL_N_Trailing_Both" (default — fire on any SL hit),
    #           "OnSL_Only" (suppress action when SL was trailed),
    #           "OnSL_Trailing_Only" (suppress action when SL was the fixed
    #            initial value). Same shape for on_target_action_on with
    #           "OnTarget_*" prefixes.
    # Note: OnTarget_Trailing_Only is effectively suppress-all on FX/crypto
    # because we have no trailing-target concept distinct from fixed TP
    # (target_lock exits route through SL path, not TP path).
    on_sl_action_on: str = "OnSL_N_Trailing_Both"
    on_target_action_on: str = "OnTarget_N_Trailing_Both"
    # Stored but UNUSED for FX/crypto — both are options-specific (Straddle)
    # or spec-missing (Trail Wait Trade). Kept on the schema so user-saved
    # values survive round-trips for when these features land.
    straddle_width_multiplier: float = 0.0
    trail_wait_trade: bool = False

    # Portfolio-level Stoploss & Target. Spec: 5. Logics/portfolio_sl_tgt.html.
    # Applied post-hoc in _merge_portfolio_results via _apply_portfolio_clip:
    # the unified equity curve is walked in time order, fixed/trailing SL & TP
    # are evaluated each tick (spec §8 evaluation order), and trades after the
    # clip timestamp are dropped from the merged outputs. All values are raw
    # PnL amounts (positive numbers; SL fires at PnL ≤ −value, TP at PnL ≥
    # value). Defaults all "off" — feature is opt-in via the *_enabled flags.
    # Type fields accept only the universal value for FX/crypto; options-only
    # values (Combined Premium, Underlying Movement, etc.) are silently
    # downgraded with a warning by _resolve_pf_stoploss / _resolve_pf_target.

    # ── Stoploss Settings (spec §1) ──
    pf_sl_enabled: bool = False
    pf_sl_type: str = "Combined Loss"  # only universal value for FX/crypto
    pf_sl_value: float = 0.0
    pf_sl_action: str = "SqOff"        # SqOff | ReExecute (others options-only)
    pf_sl_delay_sec: int = 0
    pf_sl_reexecute_count: int = 0     # 0 = unlimited per spec §1.7
    pf_sl_sqoff_only_loss_legs: bool = False    # spec §1.9
    pf_sl_sqoff_only_profit_legs: bool = False  # spec §1.10 (mutually exclusive with above)

    # ── Trailing SL Settings (spec §2) ──
    pf_sl_trail_enabled: bool = False
    pf_sl_trail_every: float = 0.0  # ratchet step in PnL
    pf_sl_trail_by: float = 0.0     # SL tightens by this per step

    # ── Move SL to Cost (spec §3) — applied per-slot via ManagedExitStrategy ──
    move_sl_enabled: bool = False
    move_sl_safety_sec: int = 0
    move_sl_action: str = "Move Only for Profitable Legs"
    move_sl_trail_after: bool = False
    move_sl_no_buy_legs: bool = False  # adapted: skip move-to-cost on LONG positions
    move_sl_hit_on_leg_sl: bool = False     # cross-slot trigger applied post-hoc
    move_sl_hit_on_leg_target: bool = False # same shape, on any slot's target

    # ── Target Settings (spec §4) ──
    pf_tgt_enabled: bool = False
    pf_tgt_type: str = "Combined Profit"  # only universal value for FX/crypto
    pf_tgt_value: float = 0.0
    pf_tgt_action: str = "SqOff"
    pf_tgt_delay_sec: int = 0
    pf_tgt_reexecute_count: int = 0  # 0 = unlimited

    # ── Trailing Target Settings (spec §5) ──
    pf_tgt_trail_enabled: bool = False
    pf_tgt_trail_lock_min_profit: float = 0.0
    pf_tgt_trail_when_profit_reach: float = 0.0
    pf_tgt_trail_every: float = 0.0
    pf_tgt_trail_by: float = 0.0

    # ── ReExecute Tab (spec: 5. Logics/ReExecute_Logics.html) ──
    # P1 (no_reexec_sl_cost): WIRED for FX/crypto. When True, suppresses
    #   re_execute action when the slot's SL was previously raised to entry
    #   via Move SL to Cost (current_position._move_sl_fired_this_position).
    # P2 (no_wait_trade_reexec): no-op for FX/crypto — we have no per-leg
    #   "Wait & Trade" pre-entry delay concept. Stored for round-trip only.
    # P3 (no_strike_change_reexec): options-only — strikes (ATM, ATM±N)
    #   don't exist for FX/crypto. Stored, marked gray in UI.
    # P4 (no_reentry_after_end): effectively always-on in our system —
    #   entry_end_time pre-filters bars; re-executions can't fire past it.
    #   Stored for round-trip; tooltip explains.
    # P5 (no_reentry_sl_cost): no-op — we don't have a separate ReEntry
    #   action that waits for price recovery (P1 covers our use case).
    no_reexec_sl_cost: bool = False
    no_wait_trade_reexec: bool = False
    no_strike_change_reexec: bool = False
    no_reentry_after_end: bool = False
    no_reentry_sl_cost: bool = True  # spec default ON

    # ── Exit Settings Tab (spec: 5. Logics/Exit_Settings_Logics.html) ──
    # exit_order_type: only "MARKET" is implemented (spec confirms even for
    #   options). "Limit" / "SL_Limit" are not implemented; spec blocks them
    #   at portfolio save in the original engine.
    # exit_sell_first: options-only multi-leg ordering (close SELL legs
    #   before BUY legs to reduce delta). FX/crypto slots are independent.
    # on_portfolio_complete: only "None" is wired. The 3 cross-portfolio
    #   actions need cross-portfolio infrastructure that doesn't exist.
    exit_order_type: str = "MARKET"
    exit_sell_first: bool = True
    on_portfolio_complete: str = "None"

    # ── Monitoring Tab (no spec doc found in 5. Logics/) ──
    # All 6 fields are evaluation-frequency settings (Realtime / MinuteClose
    # / Interval) for live-trading monitoring; backtest processes every bar
    # in order so there's no analogue. Stored for round-trip only.
    leg_target_monitoring: str = "Realtime"
    leg_trailing_monitoring: str = "Realtime"
    leg_sl_monitoring: str = "Realtime"
    leg_sl_trailing_monitoring: str = "Realtime"
    combined_target_monitoring: str = "Realtime"
    combined_sl_monitoring: str = "Realtime"

    slots: list[StrategySlotConfig] = field(default_factory=list)

    def add_slot(self, slot: StrategySlotConfig) -> None:
        self.slots.append(slot)
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def remove_slot(self, slot_id: str) -> None:
        self.slots = [s for s in self.slots if s.slot_id != slot_id]
        self.updated_at = datetime.now(timezone.utc).isoformat()

    @property
    def enabled_slots(self) -> list[StrategySlotConfig]:
        return [s for s in self.slots if s.enabled]


def resolve_squareoff(
    portfolio: "PortfolioConfig", slot: "StrategySlotConfig"
) -> tuple[Optional[str], Optional[str]]:
    """Resolve effective (squareoff_time, squareoff_tz) for a slot.

    Priority: leg (ExitConfig) > slot > portfolio. Each level is taken
    independently — e.g. a slot may set only the time and inherit the tz
    from the portfolio. Returns (None, None) if disabled at every level.
    """
    levels = (slot.exit_config, slot, portfolio)
    time = next((getattr(lvl, "squareoff_time", None) for lvl in levels
                 if getattr(lvl, "squareoff_time", None)), None)
    tz = next((getattr(lvl, "squareoff_tz", None) for lvl in levels
               if getattr(lvl, "squareoff_tz", None)), None)
    return time, tz


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
    # Local import keeps models.py free of a top-level core.venue_config
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


def save_portfolio(config: PortfolioConfig, directory: str = "portfolios") -> Path:
    dir_path = Path(directory)
    dir_path.mkdir(parents=True, exist_ok=True)
    file_path = dir_path / f"{config.name}.json"
    config.updated_at = datetime.now(timezone.utc).isoformat()
    with open(file_path, "w") as f:
        json.dump(portfolio_to_dict(config), f, indent=2)
    return file_path


def load_portfolio(name: str, directory: str = "portfolios") -> PortfolioConfig:
    file_path = Path(directory) / f"{name}.json"
    with open(file_path) as f:
        data = json.load(f)
    return portfolio_from_dict(data)


def list_portfolios(directory: str = "portfolios") -> list[str]:
    dir_path = Path(directory)
    if not dir_path.exists():
        return []
    return sorted(p.stem for p in dir_path.glob("*.json"))


def delete_portfolio(name: str, directory: str = "portfolios") -> bool:
    file_path = Path(directory) / f"{name}.json"
    if file_path.exists():
        file_path.unlink()
        return True
    return False
