# Multi-Strategy Portfolio System - Setup Guide

Complete guide to set up the multi-strategy portfolio management, exit management (SL/TP/trailing), and combined backtesting system on a new device or project.

---

## Table of Contents

1. [Prerequisites](#1-prerequisites)
2. [Project Structure](#2-project-structure)
3. [New Files to Add](#3-new-files-to-add)
4. [Files to Modify](#4-files-to-modify)
5. [Create Required Directories](#5-create-required-directories)
6. [Architecture Overview](#6-architecture-overview)
7. [Configuration Reference](#7-configuration-reference)
8. [Running the Application](#8-running-the-application)
9. [Running the Test Suite](#9-running-the-test-suite)
10. [Troubleshooting](#10-troubleshooting)

---

## 1. Prerequisites

### Python Dependencies

The system uses the same dependencies as the base NautilusTrader dashboard. No new packages are required:

```
nautilus_trader>=1.225.0
streamlit>=1.30
plotly>=5.18
pandas>=2.3
numpy>=1.26
pyarrow>=23.0
```

### Existing Project Requirements

Your project must already have the base NautilusTrader dashboard working:

- `app.py` - Main Streamlit entry point
- `core/strategies.py` - Strategy registry with `STRATEGY_REGISTRY` dict
- `core/backtest_runner.py` - Single-strategy `run_backtest()` function
- `core/instrument_factory.py` - `create_instrument()` function
- `core/nautilus_loader.py` - Catalog loading utilities
- `pages/1_load_data.py` through `pages/4_tearsheet.py` - Existing UI pages
- `catalog/` directory with loaded instrument data

---

## 2. Project Structure

After setup, your project should look like this:

```
project_root/
├── app.py                          # MODIFIED - Add navigation for pages 5-6
├── catalog/                        # Existing - ParquetDataCatalog storage
├── portfolios/                     # NEW - JSON portfolio configs saved here
├── core/
│   ├── strategies.py               # Existing - EMA Cross, RSI, Bollinger
│   ├── backtest_runner.py          # MODIFIED - Add run_portfolio_backtest()
│   ├── instrument_factory.py       # Existing - No changes
│   ├── nautilus_loader.py          # Existing - No changes
│   ├── csv_loader.py               # Existing - No changes
│   ├── models.py                   # NEW - Portfolio/Slot/Exit dataclasses
│   ├── signals.py                  # NEW - Extracted signal functions
│   ├── managed_strategy.py         # NEW - ManagedExitStrategy wrapper
│   └── templates.py                # NEW - Predefined portfolio templates
├── pages/
│   ├── 1_load_data.py              # Existing - No changes
│   ├── 2_view_data.py              # Existing - No changes
│   ├── 3_backtest.py               # MODIFIED - Add exit settings expander
│   ├── 4_tearsheet.py              # Existing - No changes
│   ├── 5_portfolio.py              # NEW - Portfolio management UI
│   └── 6_portfolio_tearsheet.py    # NEW - Portfolio analytics
└── tests/
    └── test_portfolio_e2e.py       # NEW - End-to-end test suite
```

---

## 3. New Files to Add

### 3.1 `core/models.py` - Data Models

This file defines the three core dataclasses and JSON serialization:

- **`ExitConfig`** - Stop loss, take profit, trailing, target locking, SL wait, on-action settings
- **`StrategySlotConfig`** - One strategy instance: name, params, bar type, trade size, exit config
- **`PortfolioConfig`** - Groups slots with portfolio-level settings (capital, max loss/profit)
- **Helpers** - `save_portfolio()`, `load_portfolio()`, `list_portfolios()`, `delete_portfolio()`

Create the file at `core/models.py`:

```python
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
    exit_config: ExitConfig = field(default_factory=ExitConfig)
    enabled: bool = True

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
```

### 3.2 `core/signals.py` - Signal Functions

Extracts entry signal logic from strategies into pure functions. The `ManagedExitStrategy` uses these to decouple entry logic from exit management.

Copy the file directly from the project: `core/signals.py`

**Key contents:**
- `ema_cross_signal()` - Buy when fast EMA > slow EMA
- `rsi_signal()` - Buy when RSI oversold, sell when overbought
- `bollinger_signal()` - Buy at lower band, sell at upper band
- `SIGNAL_REGISTRY` - Maps strategy names to signal functions + indicator specs

### 3.3 `core/managed_strategy.py` - Exit Management Strategy

The core engine component. Wraps any signal logic with SL/TP/trailing/target locking.

Copy the file directly from the project: `core/managed_strategy.py`

**Key classes:**
- `ManagedExitConfig(StrategyConfig)` - Frozen config with all exit parameters
- `ManagedExitStrategy(Strategy)` - On each bar: check exits first (SL, TP, trailing, target lock, SL wait), then check entries via signal function
- `config_from_exit()` - Helper to build `ManagedExitConfig` from an `ExitConfig` dataclass

**How exit logic works (in `on_bar`):**

```
IF in a position:
  1. Update highest profit seen
  2. Check target locking (if profit >= trigger, move SL to lock level)
  3. Update trailing SL (for every step% profit, trail SL by offset%)
  4. Check SL hit (conservative: SL checked before TP)
     - If sl_wait_bars > 0: wait N bars before confirming
  5. Check TP hit
  6. On exit: execute on_sl_action or on_target_action (close/re_execute/reverse)

IF flat:
  Run signal function for entry, compute initial SL/TP on fill
```

### 3.4 `core/templates.py` - Portfolio Templates

Predefined portfolio configurations for quick start.

Copy the file directly from the project: `core/templates.py`

**Templates provided:**
- **Trend Following** - EMA Cross on up to 3 instruments with trailing SL 5%, trail step 3%, offset 1.5%, TP 15%
- **Mean Reversion** - RSI + Bollinger with tight SL 3% / TP 5%
- **Diversified** - One of each strategy type, SL 5% / TP 10%
- **Conservative** - Single EMA Cross, wide SL 8%, TP 12%, target locking at $500/$200

### 3.5 `pages/5_portfolio.py` - Portfolio Management Page

The main portfolio UI with 4 sections:

1. **Portfolio Settings** - Name, capital, max loss/profit, load/save/template
2. **Strategy Slots** - Add/remove/configure slots with strategy params + exit config
3. **Actions** - Save, run backtest, export/import JSON
4. **Results Preview** - Summary metrics + per-strategy breakdown table

Copy the file directly from the project: `pages/5_portfolio.py`

### 3.6 `pages/6_portfolio_tearsheet.py` - Portfolio Tearsheet

Combined analytics page with 7 visualization sections:

1. Portfolio Summary (6 metric cards)
2. Combined Equity Curve + Drawdown (Plotly subplot)
3. Per-Strategy Equity Curves (overlaid lines)
4. Strategy P&L Contribution (bar chart)
5. Per-Strategy Metrics Table
6. Trade P&L Distribution (per strategy, expandable)
7. Strategy Correlation Heatmap

Copy the file directly from the project: `pages/6_portfolio_tearsheet.py`

### 3.7 `tests/test_portfolio_e2e.py` - Test Suite

28 end-to-end tests covering all components with synthetic data. No external data needed.

Copy the file directly from the project: `tests/test_portfolio_e2e.py`

---

## 4. Files to Modify

### 4.1 `core/backtest_runner.py` - Add Portfolio Backtest

**Add these imports** at the top of the file:

```python
import numpy as np

from core.models import PortfolioConfig, StrategySlotConfig
from core.managed_strategy import ManagedExitStrategy, config_from_exit
```

**Add `run_portfolio_backtest()` function** after the existing `run_backtest()`:

Key implementation details:
- Collects all unique bar types and instruments from enabled slots
- Adds each unique venue once (using the instrument's real venue name)
- Adds each unique instrument and bar data once (avoids duplicates)
- For slots WITH exit config: creates `ManagedExitStrategy`
- For slots WITHOUT exit config: creates raw strategy from `STRATEGY_REGISTRY`
- **Critical**: Filters strategy_params to only include keys valid for the selected strategy (prevents stale params when user switches strategy type in UI)
- After `engine.run()`, reads actual strategy IDs from `engine.trader.strategies()` (NautilusTrader reassigns IDs internally)
- Extracts per-strategy results by filtering positions/orders by `strategy_id`

**Add `_extract_portfolio_results()` helper** that returns:
- Portfolio-level: combined P&L, equity curve, max drawdown, win rate
- Per-strategy: individual P&L, trades, wins/losses, trade_pnls list
- Portfolio stop flags: whether max_loss or max_profit was breached

### 4.2 `pages/3_backtest.py` - Add Exit Settings

Add an **"Exit Management (SL / TP / Trailing)"** expander between the strategy parameters section and the Run Backtest button. When exit settings are configured, the page creates a `ManagedExitStrategy` instead of the raw strategy.

### 4.3 `app.py` - Update Navigation

Change the 4-column layout to a 2-row x 3-column layout. Add cards for:
- **5. Portfolio** - linking to `pages/5_portfolio.py`
- **6. Portfolio Tearsheet** - linking to `pages/6_portfolio_tearsheet.py`

Update the sidebar description to list all 6 pages.

---

## 5. Create Required Directories

```bash
mkdir portfolios
mkdir tests
```

The `portfolios/` directory stores saved portfolio JSON configurations. It is created automatically by `save_portfolio()` if it doesn't exist, but creating it upfront avoids confusion.

---

## 6. Architecture Overview

### Data Flow

```
┌─────────────────────────────────────────────────────┐
│                   Streamlit UI                       │
│  ┌──────────────┐  ┌──────────────┐  ┌────────────┐│
│  │ Page 5:      │  │ Page 3:      │  │ Page 6:    ││
│  │ Portfolio    │  │ Backtest     │  │ Portfolio  ││
│  │ Management   │  │ (+ Exit)     │  │ Tearsheet  ││
│  └──────┬───────┘  └──────┬───────┘  └─────▲──────┘│
└─────────┼──────────────────┼────────────────┼───────┘
          │                  │                │
          ▼                  ▼                │
┌─────────────────┐  ┌──────────────┐        │
│ PortfolioConfig │  │ ExitConfig   │        │
│ (models.py)     │  │ (models.py)  │        │
└────────┬────────┘  └──────┬───────┘        │
         │                  │                │
         ▼                  ▼                │
┌────────────────────────────────────┐       │
│ run_portfolio_backtest()           │       │
│ (backtest_runner.py)               │       │
│                                    │       │
│  ┌─────────────────────────────┐   │       │
│  │ For each enabled slot:      │   │       │
│  │  IF has_exit_management:    │   │       │
│  │    → ManagedExitStrategy    │   │       │
│  │  ELSE:                      │   │       │
│  │    → Raw Strategy           │   │       │
│  └─────────────────────────────┘   │       │
│                                    │       │
│  BacktestEngine.run()              │       │
│  → Extract per-strategy results    │───────┘
└────────────────────────────────────┘
```

### ManagedExitStrategy Internals

```
ManagedExitStrategy
├── on_start()
│   ├── Create indicators from SIGNAL_REGISTRY
│   ├── Register indicators for bar type
│   └── Subscribe to bars
│
├── on_bar(bar)
│   ├── IF in position:
│   │   ├── Update highest profit
│   │   ├── Check target locking
│   │   ├── Update trailing SL
│   │   ├── Check SL hit (with optional wait)
│   │   ├── Check TP hit
│   │   └── Handle exit action (close/re_execute/reverse)
│   │
│   └── IF flat:
│       ├── Run signal function (ema_cross/rsi/bollinger)
│       └── Submit entry + compute initial SL/TP
│
└── on_stop()
    ├── Cancel all orders
    └── Close all positions
```

### Exit Types Reference

| SL Type | How It Works |
|---------|-------------|
| `percentage` | SL = entry_price +/- (entry_price * value / 100) |
| `points` | SL = entry_price +/- value |
| `trailing` | Initial SL same as percentage. For every `step`% profit increase, trail SL by `offset`%. SL never moves backward. |

| TP Type | How It Works |
|---------|-------------|
| `percentage` | TP = entry_price +/- (entry_price * value / 100) |
| `points` | TP = entry_price +/- value |

| Feature | How It Works |
|---------|-------------|
| Target Locking | When highest profit >= trigger, move SL to entry_price +/- lock_minimum |
| SL Wait | SL must be breached for N consecutive bars before confirming |
| On SL/TP: `close` | Close position, no further action |
| On SL/TP: `re_execute` | Close position, allow re-entry on next signal |
| On SL/TP: `reverse` | Close position, immediately open opposite side |

---

## 7. Configuration Reference

### Portfolio JSON Format

Saved to `portfolios/{name}.json`:

```json
{
  "name": "My Portfolio",
  "description": "3-strategy diversified portfolio",
  "created_at": "2026-04-15T08:00:00+00:00",
  "updated_at": "2026-04-15T08:30:00+00:00",
  "starting_capital": 200000.0,
  "max_loss": 10000.0,
  "max_profit": null,
  "slots": [
    {
      "slot_id": "a1b2c3d4",
      "strategy_name": "EMA Cross",
      "strategy_params": {
        "fast_ema_period": 10,
        "slow_ema_period": 30
      },
      "bar_type_str": "BTCUSD.CRYPTO-1-DAY-LAST-EXTERNAL",
      "trade_size": 1.0,
      "exit_config": {
        "stop_loss_type": "trailing",
        "stop_loss_value": 5.0,
        "trailing_sl_step": 3.0,
        "trailing_sl_offset": 1.5,
        "target_type": "percentage",
        "target_value": 15.0,
        "target_lock_trigger": null,
        "target_lock_minimum": null,
        "sl_wait_bars": 0,
        "on_sl_action": "close",
        "on_target_action": "close",
        "max_re_executions": 0
      },
      "enabled": true
    }
  ]
}
```

### Programmatic Usage

```python
from core.models import PortfolioConfig, StrategySlotConfig, ExitConfig
from core.backtest_runner import run_portfolio_backtest

portfolio = PortfolioConfig(
    name="My Portfolio",
    starting_capital=200000.0,
    max_loss=10000.0,
    slots=[
        StrategySlotConfig(
            strategy_name="EMA Cross",
            strategy_params={"fast_ema_period": 10, "slow_ema_period": 30},
            bar_type_str="BTCUSD.CRYPTO-1-DAY-LAST-EXTERNAL",
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="trailing",
                stop_loss_value=5.0,
                trailing_sl_step=3.0,
                trailing_sl_offset=1.5,
                target_type="percentage",
                target_value=15.0,
            ),
        ),
        StrategySlotConfig(
            strategy_name="RSI Mean Reversion",
            strategy_params={"rsi_period": 14, "overbought": 70.0, "oversold": 30.0},
            bar_type_str="ETHUSD.CRYPTO-1-DAY-LAST-EXTERNAL",
            trade_size=1.0,
            exit_config=ExitConfig(
                stop_loss_type="percentage",
                stop_loss_value=3.0,
                target_type="percentage",
                target_value=5.0,
                on_sl_action="re_execute",
                max_re_executions=3,
            ),
        ),
    ],
)

results = run_portfolio_backtest("./catalog", portfolio)

print(f"Total P&L: ${results['total_pnl']:,.2f}")
print(f"Win Rate: {results['win_rate']:.1f}%")
print(f"Max Drawdown: {results['max_drawdown']:.2f}%")
for sid, sr in results["per_strategy"].items():
    print(f"  {sr['display_name']}: ${sr['pnl']:,.2f} ({sr['trades']} trades)")
```

---

## 8. Running the Application

### Start the dashboard

```bash
cd project_root
streamlit run app.py
```

Open http://localhost:8501 in your browser.

### Quick workflow

1. **Load Data** (Page 1) - Import at least 2-3 instruments (BTC, ETH, SOL)
2. **Portfolio** (Page 5) - Create a portfolio, add strategy slots with exit management
3. **Run Backtest** - Click "Run Portfolio Backtest" on Page 5
4. **Analyze** (Page 6) - View combined equity, per-strategy breakdown, correlation

---

## 9. Running the Test Suite

The test suite generates its own synthetic data and runs 28 tests covering all components.

```bash
cd project_root
python -c "import sys; sys.path.insert(0, '.'); from tests.test_portfolio_e2e import run_all_tests; run_all_tests()"
```

### What the tests cover

| Group | Count | Description |
|-------|-------|-------------|
| Data Models | 5 | ExitConfig, StrategySlotConfig, PortfolioConfig, JSON roundtrip, save/load/delete |
| Signals | 4 | All 3 signal functions + registry completeness |
| Single Strategy | 3 | EMA Cross, RSI, Bollinger baselines |
| Portfolio (no exit) | 2 | 1-slot and 3-slot portfolios without exit management |
| Exit Management | 6 | Percentage SL, percentage TP, SL+TP, trailing SL, SL wait, target locking |
| On-Actions | 2 | Re-execute on SL, reverse on TP |
| Multi-Strategy | 3 | Mixed exits, max_loss limit, disabled slot |
| Templates | 2 | Config generation, template backtest |
| Results Structure | 1 | All output keys present and valid |

Expected output:

```
RESULTS: 28 passed, 0 failed
```

---

## 10. Troubleshooting

### "Cannot add an Instrument object without first adding its associated venue"

The venue name in `add_venue()` must match the venue embedded in the instrument's ID. For example, if your instrument is `BTCUSD.CRYPTO`, you must call `engine.add_venue(venue=Venue("CRYPTO"), ...)`.

The portfolio runner handles this automatically by reading the venue from the instrument.

### "Unexpected keyword argument" when running portfolio backtest

This happens when a slot's `strategy_params` dict contains keys from a previously selected strategy (e.g., user switched from EMA Cross to RSI in the UI but old params remain). The runner filters params to only include valid keys for the current strategy. If you hit this on an older version, update `core/backtest_runner.py` to include the param filtering:

```python
valid_param_keys = set(registry_entry["params"].keys())
filtered_params = {k: v for k, v in slot.strategy_params.items() if k in valid_param_keys}
```

### Venue name "SYNTH" causes `'client' argument was None`

NautilusTrader reserves certain venue names. Avoid using `SYNTH` as a venue name. Use `SIM`, `CRYPTO`, `YAHOO`, `TEST`, `BINANCE`, or other standard names instead.

### 0 trades in portfolio backtest

- Verify the instrument has enough data for indicators to initialize (e.g., 20+ bars for EMA-20)
- Check that strategy_id mapping is using actual IDs from `engine.trader.strategies()` after running (not the config-provided IDs, which NautilusTrader reassigns)
- For NETTING OMS: two strategies on the same instrument will share positions and may cancel each other out. Use different instruments per slot to avoid this.

### Streamlit not picking up code changes

Streamlit hot-reloads on file save, but sometimes you need to:
- Refresh the browser (F5)
- Or restart Streamlit: `Ctrl+C` then `streamlit run app.py`

### Portfolio JSON import fails

Ensure the JSON was exported from the same version of the system. The schema is defined by `PortfolioConfig` / `ExitConfig` dataclasses. If fields were added or renamed between versions, update the JSON manually or re-create the portfolio.

---

## Adding New Strategies

To add a new strategy to the portfolio system:

1. **Define the strategy** in `core/strategies.py` (Strategy class + Config class + add to `STRATEGY_REGISTRY`)
2. **Add a signal function** in `core/signals.py` (pure function + add to `SIGNAL_REGISTRY`)
3. That's it - the portfolio UI, backtest runner, and managed exit strategy automatically pick up anything in the registries.

Example for a MACD strategy:

```python
# In core/signals.py - add signal function
def macd_signal(macd_value, signal_value, is_flat, is_long, is_short):
    if macd_value > signal_value:
        if is_flat or is_short:
            return OrderSide.BUY
    elif macd_value < signal_value:
        if is_flat or is_long:
            return OrderSide.SELL
    return None

# In SIGNAL_REGISTRY - add entry
"MACD": {
    "signal_fn": macd_signal,
    "indicators": {
        "macd": {"class": MovingAverageConvergenceDivergence, "param_key": "fast_period", "default": 12},
    },
    "extract_args": lambda indicators, params, close: {
        "macd_value": indicators["macd"].value,
        "signal_value": indicators["macd"].signal,
    },
}
```
