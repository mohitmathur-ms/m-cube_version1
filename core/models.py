"""
Portfolio configuration models.

Dataclasses for portfolio, strategy slot, and exit management configuration.
Includes JSON serialization helpers for saving/loading portfolios.
"""

from __future__ import annotations

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

    def has_exit_management(self) -> bool:
        return self.stop_loss_type != "none" or self.target_type != "none"


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


def portfolio_to_dict(config: PortfolioConfig) -> dict:
    return asdict(config)


def portfolio_from_dict(data: dict) -> PortfolioConfig:
    slots_data = data.pop("slots", [])
    slots = []
    for slot_data in slots_data:
        exit_data = slot_data.pop("exit_config", {})
        exit_config = ExitConfig(**exit_data)
        slots.append(StrategySlotConfig(exit_config=exit_config, **slot_data))
    return PortfolioConfig(slots=slots, **data)


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
