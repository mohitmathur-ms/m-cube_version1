"""
Custom strategy loader: validate, dynamically load, and manage user-uploaded strategy files.

Each custom strategy .py file must export 5 module-level constants:
    STRATEGY_NAME: str
    STRATEGY_CLASS: type (subclass of Strategy)
    CONFIG_CLASS: type (subclass of StrategyConfig, frozen=True)
    DESCRIPTION: str
    PARAMS: dict
"""

from __future__ import annotations

import importlib.util
import inspect
import logging
import re
import sys
from pathlib import Path

from nautilus_trader.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

from core.strategies import STRATEGY_REGISTRY

logger = logging.getLogger(__name__)

REQUIRED_EXPORTS = ["STRATEGY_NAME", "STRATEGY_CLASS", "CONFIG_CLASS", "DESCRIPTION", "PARAMS"]
REQUIRED_CONFIG_FIELDS = {"instrument_id", "bar_type", "trade_size"}
OPTIONAL_CONFIG_FIELDS = {"extra_bar_types"}
REQUIRED_STRATEGY_METHODS = {"on_start", "on_bar", "on_stop"}
MAX_STRATEGY_NAME_LENGTH = 100


def sanitize_filename(name: str) -> str:
    """Sanitize a filename: keep alphanumeric, underscore, hyphen, and dot only."""
    sanitized = re.sub(r"[^\w\-.]", "_", name)
    # Collapse multiple underscores
    sanitized = re.sub(r"_+", "_", sanitized)
    return sanitized


def _load_module_from_file(file_path: Path):
    """Dynamically load a Python module from a file path."""
    module_name = f"custom_strategy_{file_path.stem}"
    # Remove previously loaded version to pick up changes
    if module_name in sys.modules:
        del sys.modules[module_name]

    spec = importlib.util.spec_from_file_location(module_name, str(file_path))
    if spec is None or spec.loader is None:
        raise ValueError(f"Cannot create module spec from file: {file_path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_and_load_strategy(file_path: Path) -> dict:
    """
    Validate and dynamically load a custom strategy from a .py file.

    Returns a registry entry dict compatible with STRATEGY_REGISTRY,
    with an additional "_strategy_name" key holding the STRATEGY_NAME value.
    Raises ValueError with all validation errors collected.
    """
    # --- Phase 1: File-level checks ---
    if not file_path.exists():
        raise ValueError(f"Strategy file not found: {file_path}")
    if file_path.stat().st_size == 0:
        raise ValueError("Strategy file is empty.")

    # --- Phase 2: Load module ---
    try:
        module = _load_module_from_file(file_path)
    except SyntaxError as e:
        raise ValueError(f"Syntax error in strategy file: {e.msg} (line {e.lineno})") from e
    except ImportError as e:
        mod_name = getattr(e, "name", str(e))
        raise ValueError(
            f"Import error: '{mod_name}' not found. Ensure all dependencies are installed."
        ) from e
    except Exception as e:
        raise ValueError(f"Error loading strategy file: {e}") from e

    errors: list[str] = []

    # --- Phase 3: Check required exports ---
    for name in REQUIRED_EXPORTS:
        if not hasattr(module, name):
            errors.append(f"Missing required export: {name}")
    if errors:
        raise ValueError("Validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

    strategy_name = getattr(module, "STRATEGY_NAME")
    strategy_class = getattr(module, "STRATEGY_CLASS")
    config_class = getattr(module, "CONFIG_CLASS")
    description = getattr(module, "DESCRIPTION")
    params = getattr(module, "PARAMS")

    # --- Phase 4: Type checks ---
    if not isinstance(strategy_name, str):
        errors.append(f"STRATEGY_NAME must be a string, got {type(strategy_name).__name__}")
    elif len(strategy_name.strip()) == 0:
        errors.append("STRATEGY_NAME cannot be empty")
    elif len(strategy_name) > MAX_STRATEGY_NAME_LENGTH:
        errors.append(f"STRATEGY_NAME too long (max {MAX_STRATEGY_NAME_LENGTH} chars)")

    if not isinstance(description, str):
        errors.append(f"DESCRIPTION must be a string, got {type(description).__name__}")

    if not isinstance(params, dict):
        errors.append(f"PARAMS must be a dict, got {type(params).__name__}")

    if not inspect.isclass(config_class):
        errors.append(f"CONFIG_CLASS must be a class, got {type(config_class).__name__}")
    elif not issubclass(config_class, StrategyConfig):
        errors.append("CONFIG_CLASS must inherit from StrategyConfig")

    if not inspect.isclass(strategy_class):
        errors.append(f"STRATEGY_CLASS must be a class, got {type(strategy_class).__name__}")
    elif not issubclass(strategy_class, Strategy):
        errors.append("STRATEGY_CLASS must inherit from Strategy")

    # --- Phase 5: Config field and strategy method validation ---
    if inspect.isclass(config_class) and issubclass(config_class, StrategyConfig):
        # Collect all annotations from the MRO
        config_annotations = {}
        for cls in reversed(config_class.__mro__):
            config_annotations.update(getattr(cls, "__annotations__", {}))
        # Also check model_fields if available (msgspec/pydantic)
        model_fields = set(getattr(config_class, "model_fields", {}).keys())
        all_config_fields = set(config_annotations.keys()) | model_fields

        for field in REQUIRED_CONFIG_FIELDS:
            if field not in all_config_fields:
                errors.append(f"CONFIG_CLASS must have '{field}' field")

    if inspect.isclass(strategy_class) and issubclass(strategy_class, Strategy):
        for method in REQUIRED_STRATEGY_METHODS:
            if method not in strategy_class.__dict__:
                errors.append(f"STRATEGY_CLASS must define {method}() method")

    # --- Phase 6: PARAMS dict validation ---
    if isinstance(params, dict) and inspect.isclass(config_class) and issubclass(config_class, StrategyConfig):
        # Gather config field names for cross-referencing
        config_annotations = {}
        for cls in reversed(config_class.__mro__):
            config_annotations.update(getattr(cls, "__annotations__", {}))
        model_fields = set(getattr(config_class, "model_fields", {}).keys())
        all_config_fields = set(config_annotations.keys()) | model_fields

        for key, param_info in params.items():
            if not isinstance(param_info, dict):
                errors.append(f"Param '{key}' must be a dict, got {type(param_info).__name__}")
                continue

            if "label" not in param_info:
                errors.append(f"Param '{key}' missing required key 'label'")
            elif not isinstance(param_info["label"], str):
                errors.append(f"Param '{key}' label must be a string")

            if "default" not in param_info:
                errors.append(f"Param '{key}' missing required key 'default'")
                continue

            default = param_info["default"]
            if not isinstance(default, (int, float, bool)):
                errors.append(
                    f"Param '{key}' default must be int, float, or bool, got {type(default).__name__}"
                )
                continue

            # Numeric params (int/float but not bool) need min/max
            if isinstance(default, (int, float)) and not isinstance(default, bool):
                if "min" not in param_info:
                    errors.append(f"Param '{key}' is numeric but missing 'min'")
                if "max" not in param_info:
                    errors.append(f"Param '{key}' is numeric but missing 'max'")
                if "min" in param_info and "max" in param_info:
                    if param_info["min"] > param_info["max"]:
                        errors.append(
                            f"Param '{key}': min ({param_info['min']}) must be <= max ({param_info['max']})"
                        )
                    elif not (param_info["min"] <= default <= param_info["max"]):
                        errors.append(
                            f"Param '{key}': default ({default}) outside range "
                            f"[{param_info['min']}, {param_info['max']}]"
                        )

            # Cross-reference with config fields
            if key not in all_config_fields:
                available = sorted(all_config_fields - REQUIRED_CONFIG_FIELDS)
                errors.append(
                    f"Param '{key}' not found in CONFIG_CLASS fields. Available: {available}"
                )

    if errors:
        raise ValueError("Validation failed:\n" + "\n".join(f"  - {e}" for e in errors))

    return {
        "strategy_class": strategy_class,
        "config_class": config_class,
        "description": description,
        "params": params,
        "_strategy_name": strategy_name,
    }


def load_all_custom_strategies(directory: Path) -> tuple[dict[str, dict], list[str]]:
    """
    Load all valid custom strategies from a directory.

    Returns (strategies_dict, warnings_list).
    """
    strategies: dict[str, dict] = {}
    warnings: list[str] = []

    if not directory.exists():
        return strategies, warnings

    seen_names: dict[str, str] = {}  # strategy_name -> filename

    for py_file in sorted(directory.glob("*.py")):
        if py_file.name.startswith("__"):
            continue

        try:
            entry = validate_and_load_strategy(py_file)
        except PermissionError:
            warnings.append(f"Permission denied: {py_file.name}")
            continue
        except ValueError as e:
            warnings.append(f"{py_file.name}: {e}")
            continue
        except Exception as e:
            warnings.append(f"{py_file.name}: Unexpected error: {e}")
            continue

        strat_name = entry.pop("_strategy_name", py_file.stem)

        if strat_name in seen_names:
            warnings.append(
                f"Duplicate strategy name '{strat_name}' in {py_file.name} "
                f"(already defined in {seen_names[strat_name]}), skipping"
            )
            continue

        seen_names[strat_name] = py_file.name
        strategies[strat_name] = entry

    return strategies, warnings


def get_merged_registry(custom_dir: Path) -> tuple[dict, list[str]]:
    """
    Merge built-in STRATEGY_REGISTRY with custom strategies.

    Returns (merged_registry, warnings_list).
    """
    merged = dict(STRATEGY_REGISTRY)
    custom_strategies, warnings = load_all_custom_strategies(custom_dir)

    for name, entry in custom_strategies.items():
        if name in merged:
            # Name collision with built-in: prefix with "(Custom) "
            prefixed = f"(Custom) {name}"
            warnings.append(
                f"Strategy name '{name}' conflicts with built-in. Renamed to '{prefixed}'."
            )
            merged[prefixed] = entry
        else:
            merged[name] = entry

    return merged, warnings


def get_strategy_template() -> str:
    """Return a complete, well-commented custom strategy template."""
    return '''"""
Custom Strategy Template for NautilusTrader Crypto Dashboard
=============================================================

Instructions:
1. Rename this file to something descriptive (e.g., my_vwap_strategy.py)
2. Implement your strategy logic in the Strategy class
3. Define your configurable parameters in the Config class and PARAMS dict
4. Upload this file on the Backtest page

Required exports (do not remove):
    STRATEGY_NAME  - Display name for the UI
    CONFIG_CLASS   - Your config class (must inherit StrategyConfig)
    STRATEGY_CLASS - Your strategy class (must inherit Strategy)
    DESCRIPTION    - One-line description
    PARAMS         - Dict of configurable parameters for the UI
"""

from __future__ import annotations

from decimal import Decimal

from nautilus_trader.config import PositiveInt
from nautilus_trader.config import StrategyConfig
from nautilus_trader.indicators import ExponentialMovingAverage
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from nautilus_trader.model.enums import TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.instruments import Instrument
from nautilus_trader.trading.strategy import Strategy


# =============================================================================
# 1. CONFIG CLASS
# =============================================================================
# - Must inherit from StrategyConfig with frozen=True
# - Must include these 3 required fields (system-injected, do not remove):
#       instrument_id: InstrumentId
#       bar_type: BarType
#       trade_size: Decimal
# - Add your custom parameters below the required fields
# - Supported param types: PositiveInt, int, float, bool, Decimal

class MyStrategyConfig(StrategyConfig, frozen=True):
    # --- Required fields (do not remove) ---
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    extra_bar_types: list[BarType] | None = None  # Optional: additional bar types (e.g., BID + ASK)

    # --- Your custom parameters below ---
    ema_period: PositiveInt = 20
    threshold: float = 0.5
    use_filter: bool = False


# =============================================================================
# 2. STRATEGY CLASS
# =============================================================================
# - Must inherit from Strategy
# - Must define these methods:
#       __init__(self, config)  - Initialize indicators and state
#       on_start(self)          - Register indicators, load instrument, subscribe
#       on_bar(self, bar)       - Your trading logic (called on each bar)
#       on_stop(self)           - Cleanup: cancel orders, close positions

class MyStrategy(Strategy):
    """Example custom strategy using EMA with threshold filter."""

    def __init__(self, config: MyStrategyConfig) -> None:
        super().__init__(config)
        self.instrument: Instrument | None = None
        # Initialize your indicators here
        self.ema = ExponentialMovingAverage(config.ema_period)

    def on_start(self) -> None:
        """Called when the strategy starts. Register indicators and subscribe to data."""
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return

        # Register indicators so they auto-update with each bar
        self.register_indicator_for_bars(self.config.bar_type, self.ema)

        # Subscribe to bar data
        self.subscribe_bars(self.config.bar_type)

        # Subscribe to extra bar types if provided (e.g., BID + ASK for forex)
        if self.config.extra_bar_types:
            for bt in self.config.extra_bar_types:
                self.subscribe_bars(bt)

    def on_bar(self, bar: Bar) -> None:
        """Called on each new bar. Implement your trading logic here."""
        # Wait until all indicators have enough data
        if not self.indicators_initialized():
            return

        close = float(bar.close)
        ema_val = self.ema.value

        # Example logic: buy when price crosses above EMA + threshold
        if close > ema_val * (1 + self.config.threshold / 100):
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.BUY)
            elif self.portfolio.is_net_short(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.BUY)

        # Sell when price crosses below EMA - threshold
        elif close < ema_val * (1 - self.config.threshold / 100):
            if self.portfolio.is_flat(self.config.instrument_id):
                self._submit_order(OrderSide.SELL)
            elif self.portfolio.is_net_long(self.config.instrument_id):
                self.close_all_positions(self.config.instrument_id)
                self._submit_order(OrderSide.SELL)

    def _submit_order(self, side: OrderSide) -> None:
        """Helper to submit a market order."""
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=side,
            quantity=self.instrument.make_qty(self.config.trade_size),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)

    def on_stop(self) -> None:
        """Called when the strategy stops. Clean up orders and positions."""
        self.cancel_all_orders(self.config.instrument_id)
        self.close_all_positions(self.config.instrument_id)


# =============================================================================
# 3. REQUIRED EXPORTS (do not remove)
# =============================================================================

STRATEGY_NAME = "My Custom Strategy"

CONFIG_CLASS = MyStrategyConfig

STRATEGY_CLASS = MyStrategy

DESCRIPTION = "Example: Buy/sell when price crosses EMA +/- threshold percentage."

PARAMS = {
    # Integer parameter: requires label, default, min, max
    "ema_period": {
        "label": "EMA Period",
        "min": 2,
        "max": 200,
        "default": 20,
    },
    # Float parameter: requires label, default, min, max
    "threshold": {
        "label": "Threshold (%)",
        "min": 0.1,
        "max": 10.0,
        "default": 0.5,
    },
    # Boolean parameter: requires label, default only (no min/max)
    "use_filter": {
        "label": "Use Filter",
        "default": False,
    },
}
'''


def get_strategy_guidelines() -> str:
    """Return markdown guidelines for writing custom strategies."""
    return """
## Custom Strategy Guidelines

### File Structure

Your `.py` file must export **5 module-level constants**:

| Export | Type | Description |
|--------|------|-------------|
| `STRATEGY_NAME` | `str` | Display name in the UI (max 100 chars) |
| `CONFIG_CLASS` | class | Config class inheriting `StrategyConfig` with `frozen=True` |
| `STRATEGY_CLASS` | class | Strategy class inheriting `Strategy` |
| `DESCRIPTION` | `str` | One-line description of what the strategy does |
| `PARAMS` | `dict` | Parameter definitions for the UI (see format below) |

---

### Required Config Fields

Your `CONFIG_CLASS` **must** include these 3 fields (they are injected by the system):

```python
class MyConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId    # Auto-set from selected instrument
    bar_type: BarType              # Auto-set from selected bar type
    trade_size: Decimal = Decimal("1")  # Configurable in UI
    extra_bar_types: list[BarType] | None = None  # Optional: auto-set when multiple bar types selected
    # ... your custom parameters below
```

### Multi-Bar Support (e.g., BID + ASK for Forex)

If you select multiple bar types for the same instrument (e.g., BID and ASK), the first becomes `bar_type` and the rest are passed as `extra_bar_types`. In `on_bar()`, use `bar.bar_type` to distinguish:

```python
def on_bar(self, bar: Bar) -> None:
    if "BID" in str(bar.bar_type):
        self.bid_close = float(bar.close)
    elif "ASK" in str(bar.bar_type):
        self.ask_close = float(bar.close)
```

---

### Required Strategy Methods

Your `STRATEGY_CLASS` **must** define these methods:

| Method | Purpose |
|--------|---------|
| `__init__(self, config)` | Initialize indicators and instance variables |
| `on_start(self)` | Register indicators, load instrument, subscribe to bars |
| `on_bar(self, bar)` | Trading logic - called on every new bar |
| `on_stop(self)` | Cancel all orders, close all positions |

---

### PARAMS Dict Format

```python
PARAMS = {
    # Integer parameter
    "my_period": {
        "label": "My Period",      # Required: display label
        "default": 14,             # Required: default value
        "min": 2,                  # Required for int/float
        "max": 200,                # Required for int/float
    },
    # Float parameter
    "my_threshold": {
        "label": "Threshold",
        "default": 0.5,
        "min": 0.0,
        "max": 10.0,
    },
    # Boolean parameter (no min/max needed)
    "use_filter": {
        "label": "Use Filter",
        "default": False,
    },
}
```

**Rules:**
- Each param key must match a field name in your `CONFIG_CLASS`
- `label` (str) and `default` (int, float, or bool) are always required
- `min` and `max` are required for numeric (int/float) params
- `default` must be within `[min, max]` range
- `min` must be `<=` `max`
- Boolean params only need `label` and `default`

---

### Available NautilusTrader Indicators

Common indicators you can import from `nautilus_trader.indicators`:

| Indicator | Import | Constructor |
|-----------|--------|-------------|
| EMA | `ExponentialMovingAverage` | `ExponentialMovingAverage(period)` |
| SMA | `SimpleMovingAverage` | `SimpleMovingAverage(period)` |
| RSI | `RelativeStrengthIndex` | `RelativeStrengthIndex(period)` |
| Bollinger Bands | `BollingerBands` | `BollingerBands(period, k)` |
| MACD | `MovingAverageConvergenceDivergence` | `MACD(fast, slow, signal)` |
| ATR | `AverageTrueRange` | `AverageTrueRange(period)` |
| Stochastic | `Stochastics` | `Stochastics(period_k, period_d)` |
| ADX | `AverageDirectionalIndex` | `AverageDirectionalIndex(period)` |

Register indicators in `on_start()`:
```python
self.register_indicator_for_bars(self.config.bar_type, self.my_indicator)
```

---

### Common Trading Patterns

**Submitting orders:**
```python
order = self.order_factory.market(
    instrument_id=self.config.instrument_id,
    order_side=OrderSide.BUY,  # or OrderSide.SELL
    quantity=self.instrument.make_qty(self.config.trade_size),
    time_in_force=TimeInForce.GTC,
)
self.submit_order(order)
```

**Checking position state:**
```python
self.portfolio.is_flat(self.config.instrument_id)       # No position
self.portfolio.is_net_long(self.config.instrument_id)    # Long position
self.portfolio.is_net_short(self.config.instrument_id)   # Short position
```

**Managing positions:**
```python
self.close_all_positions(self.config.instrument_id)  # Close all
self.cancel_all_orders(self.config.instrument_id)    # Cancel pending
```

**Accessing bar data:**
```python
close = float(bar.close)
high = float(bar.high)
low = float(bar.low)
open_price = float(bar.open)
volume = float(bar.volume)
```

---

### Important Notes

1. Always call `super().__init__(config)` in your strategy's `__init__`
2. Always check `self.indicators_initialized()` at the start of `on_bar` before trading
3. Always load the instrument in `on_start`: `self.instrument = self.cache.instrument(self.config.instrument_id)`
4. Always clean up in `on_stop`: cancel orders and close positions
5. Use `self.log.info(...)`, `self.log.warning(...)`, `self.log.error(...)` for logging
6. Config classes must use `frozen=True` for immutability
"""
