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
    trade_size: float = 1.0
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


def portfolio_from_dict(data: dict) -> PortfolioConfig:
    # Don't mutate the caller's dict.
    data = dict(data)
    slots_data = data.pop("slots", [])
    slots = []
    for raw_slot in slots_data:
        slot_data = dict(raw_slot)
        exit_data = slot_data.pop("exit_config", {}) or {}
        exit_config = ExitConfig(**_filter_known_fields(exit_data, ExitConfig))
        slots.append(StrategySlotConfig(
            exit_config=exit_config,
            **_filter_known_fields(slot_data, StrategySlotConfig),
        ))
    return PortfolioConfig(slots=slots, **_filter_known_fields(data, PortfolioConfig))


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
