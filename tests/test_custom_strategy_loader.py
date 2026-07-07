"""
Comprehensive tests for custom_strategy_loader module.

Covers: file-level validation, missing exports, type validation, inheritance,
PARAMS dict validation, happy paths, load_all_custom_strategies, get_merged_registry,
get_strategy_template, and filename sanitization.
"""

from __future__ import annotations

import sys
import textwrap
from pathlib import Path

import pytest

# Ensure project root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.custom_strategy_loader import (
    get_merged_registry,
    get_strategy_template,
    load_all_custom_strategies,
    sanitize_filename,
    validate_and_load_strategy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write(tmp_path: Path, code: str, name: str = "strat.py") -> Path:
    """Write Python source to a file in tmp_path and return its Path."""
    p = tmp_path / name
    p.write_text(textwrap.dedent(code), encoding="utf-8")
    return p


# A valid minimal strategy source reused across tests.
VALID_STRATEGY_SRC = """\
from __future__ import annotations
from decimal import Decimal
from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class GoodConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    my_period: PositiveInt = 14

class GoodStrategy(Strategy):
    def __init__(self, config: GoodConfig) -> None:
        super().__init__(config)
    def on_start(self) -> None:
        pass
    def on_bar(self, bar: Bar) -> None:
        pass
    def on_stop(self) -> None:
        pass

STRATEGY_NAME = "Good Strategy"
CONFIG_CLASS = GoodConfig
STRATEGY_CLASS = GoodStrategy
DESCRIPTION = "A valid test strategy."
PARAMS = {
    "my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},
}
"""


# ===========================================================================
# Group 1: File-Level Validation
# ===========================================================================

class TestFileLevelValidation:
    def test_load_nonexistent_file(self, tmp_path: Path):
        with pytest.raises(ValueError, match="not found"):
            validate_and_load_strategy(tmp_path / "nonexistent.py")

    def test_load_empty_file(self, tmp_path: Path):
        p = _write(tmp_path, "", "empty.py")
        with pytest.raises(ValueError, match="empty"):
            validate_and_load_strategy(p)

    def test_load_syntax_error_file(self, tmp_path: Path):
        p = _write(tmp_path, "def bad(\n", "syntax_err.py")
        with pytest.raises(ValueError, match="Syntax error"):
            validate_and_load_strategy(p)

    def test_load_import_error_file(self, tmp_path: Path):
        p = _write(tmp_path, "import nonexistent_module_xyz_123\n", "import_err.py")
        with pytest.raises(ValueError, match="Import error"):
            validate_and_load_strategy(p)

    def test_load_runtime_error_file(self, tmp_path: Path):
        p = _write(tmp_path, "raise RuntimeError('boom')\n", "runtime_err.py")
        with pytest.raises(ValueError, match="Error loading"):
            validate_and_load_strategy(p)


# ===========================================================================
# Group 2: Missing Exports
# ===========================================================================

class TestMissingExports:
    def test_missing_strategy_name(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace('STRATEGY_NAME = "Good Strategy"', "")
        p = _write(tmp_path, code, "no_name.py")
        with pytest.raises(ValueError, match="Missing required export: STRATEGY_NAME"):
            validate_and_load_strategy(p)

    def test_missing_strategy_class(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace("STRATEGY_CLASS = GoodStrategy", "")
        p = _write(tmp_path, code, "no_class.py")
        with pytest.raises(ValueError, match="Missing required export: STRATEGY_CLASS"):
            validate_and_load_strategy(p)

    def test_missing_config_class(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace("CONFIG_CLASS = GoodConfig", "")
        p = _write(tmp_path, code, "no_config.py")
        with pytest.raises(ValueError, match="Missing required export: CONFIG_CLASS"):
            validate_and_load_strategy(p)

    def test_missing_description(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace('DESCRIPTION = "A valid test strategy."', "")
        p = _write(tmp_path, code, "no_desc.py")
        with pytest.raises(ValueError, match="Missing required export: DESCRIPTION"):
            validate_and_load_strategy(p)

    def test_missing_params(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace(
            'PARAMS = {\n    "my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},\n}',
            "",
        )
        p = _write(tmp_path, code, "no_params.py")
        with pytest.raises(ValueError, match="Missing required export: PARAMS"):
            validate_and_load_strategy(p)

    def test_missing_multiple_exports(self, tmp_path: Path):
        code = (
            VALID_STRATEGY_SRC
            .replace('STRATEGY_NAME = "Good Strategy"', "")
            .replace('DESCRIPTION = "A valid test strategy."', "")
        )
        p = _write(tmp_path, code, "no_multi.py")
        with pytest.raises(ValueError, match="STRATEGY_NAME") as exc_info:
            validate_and_load_strategy(p)
        assert "DESCRIPTION" in str(exc_info.value)


# ===========================================================================
# Group 3: Type Validation
# ===========================================================================

class TestTypeValidation:
    def test_strategy_name_not_string(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace('STRATEGY_NAME = "Good Strategy"', "STRATEGY_NAME = 123")
        p = _write(tmp_path, code, "name_int.py")
        with pytest.raises(ValueError, match="must be a string"):
            validate_and_load_strategy(p)

    def test_strategy_name_empty(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace('STRATEGY_NAME = "Good Strategy"', 'STRATEGY_NAME = "  "')
        p = _write(tmp_path, code, "name_empty.py")
        with pytest.raises(ValueError, match="cannot be empty"):
            validate_and_load_strategy(p)

    def test_strategy_name_too_long(self, tmp_path: Path):
        long_name = "A" * 101
        code = VALID_STRATEGY_SRC.replace('STRATEGY_NAME = "Good Strategy"', f'STRATEGY_NAME = "{long_name}"')
        p = _write(tmp_path, code, "name_long.py")
        with pytest.raises(ValueError, match="too long"):
            validate_and_load_strategy(p)

    def test_description_not_string(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace('DESCRIPTION = "A valid test strategy."', "DESCRIPTION = 42")
        p = _write(tmp_path, code, "desc_int.py")
        with pytest.raises(ValueError, match="DESCRIPTION must be a string"):
            validate_and_load_strategy(p)

    def test_params_not_dict(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace(
            'PARAMS = {\n    "my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},\n}',
            "PARAMS = [1, 2, 3]",
        )
        p = _write(tmp_path, code, "params_list.py")
        with pytest.raises(ValueError, match="PARAMS must be a dict"):
            validate_and_load_strategy(p)

    def test_config_class_not_class(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace("CONFIG_CLASS = GoodConfig", 'CONFIG_CLASS = "not_a_class"')
        p = _write(tmp_path, code, "cfg_str.py")
        with pytest.raises(ValueError, match="CONFIG_CLASS must be a class"):
            validate_and_load_strategy(p)

    def test_strategy_class_not_class(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace("STRATEGY_CLASS = GoodStrategy", "STRATEGY_CLASS = 999")
        p = _write(tmp_path, code, "strat_int.py")
        with pytest.raises(ValueError, match="STRATEGY_CLASS must be a class"):
            validate_and_load_strategy(p)


# ===========================================================================
# Group 4: Inheritance Validation
# ===========================================================================

class TestInheritanceValidation:
    def test_config_not_subclass_of_strategy_config(self, tmp_path: Path):
        code = """\
from nautilus_trader.trading.strategy import Strategy
from nautilus_trader.model.data import Bar

class BadConfig:
    pass

class GoodStrategy(Strategy):
    def __init__(self, config):
        pass
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "Bad Config"
CONFIG_CLASS = BadConfig
STRATEGY_CLASS = GoodStrategy
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "bad_cfg.py")
        with pytest.raises(ValueError, match="must inherit from StrategyConfig"):
            validate_and_load_strategy(p)

    def test_strategy_not_subclass_of_strategy(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

class GoodConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

class BadStrategy:
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "Bad Strategy"
CONFIG_CLASS = GoodConfig
STRATEGY_CLASS = BadStrategy
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "bad_strat.py")
        with pytest.raises(ValueError, match="must inherit from Strategy"):
            validate_and_load_strategy(p)

    def test_config_missing_instrument_id(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.trading.strategy import Strategy

class BadConfig(StrategyConfig, frozen=True):
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "Missing InstrumentId"
CONFIG_CLASS = BadConfig
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "no_iid.py")
        with pytest.raises(ValueError, match="instrument_id"):
            validate_and_load_strategy(p)

    def test_config_missing_bar_type(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class BadConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    trade_size: Decimal = Decimal("1")

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "Missing BarType"
CONFIG_CLASS = BadConfig
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "no_bt.py")
        with pytest.raises(ValueError, match="bar_type"):
            validate_and_load_strategy(p)

    def test_config_missing_trade_size(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class BadConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "Missing TradeSize"
CONFIG_CLASS = BadConfig
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "no_ts.py")
        with pytest.raises(ValueError, match="trade_size"):
            validate_and_load_strategy(p)

    def test_strategy_missing_on_start(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class Cfg(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "No on_start"
CONFIG_CLASS = Cfg
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "no_on_start.py")
        with pytest.raises(ValueError, match="on_start"):
            validate_and_load_strategy(p)

    def test_strategy_missing_on_bar(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class Cfg(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_start(self):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "No on_bar"
CONFIG_CLASS = Cfg
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "no_on_bar.py")
        with pytest.raises(ValueError, match="on_bar"):
            validate_and_load_strategy(p)

    def test_strategy_missing_on_stop(self, tmp_path: Path):
        code = """\
from decimal import Decimal
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class Cfg(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass

STRATEGY_NAME = "No on_stop"
CONFIG_CLASS = Cfg
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {}
"""
        p = _write(tmp_path, code, "no_on_stop.py")
        with pytest.raises(ValueError, match="on_stop"):
            validate_and_load_strategy(p)


# ===========================================================================
# Group 5: PARAMS Validation
# ===========================================================================

class TestParamsValidation:
    def _make_code(self, params_str: str, extra_field: str = "") -> str:
        """Build strategy source with custom PARAMS and optional extra config field."""
        return f"""\
from __future__ import annotations
from decimal import Decimal
from nautilus_trader.config import PositiveInt, StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.trading.strategy import Strategy

class Cfg(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    trade_size: Decimal = Decimal("1")
    my_period: PositiveInt = 14
    my_float: float = 0.5
    my_bool: bool = False
    {extra_field}

class Strat(Strategy):
    def __init__(self, config):
        super().__init__(config)
    def on_start(self):
        pass
    def on_bar(self, bar):
        pass
    def on_stop(self):
        pass

STRATEGY_NAME = "Test"
CONFIG_CLASS = Cfg
STRATEGY_CLASS = Strat
DESCRIPTION = "test"
PARAMS = {params_str}
"""

    def test_param_missing_label(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"default": 14, "min": 2, "max": 200}}'))
        with pytest.raises(ValueError, match="missing required key 'label'"):
            validate_and_load_strategy(p)

    def test_param_missing_default(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "Period", "min": 2, "max": 200}}'))
        with pytest.raises(ValueError, match="missing required key 'default'"):
            validate_and_load_strategy(p)

    def test_param_label_not_string(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": 42, "default": 14, "min": 2, "max": 200}}'))
        with pytest.raises(ValueError, match="label must be a string"):
            validate_and_load_strategy(p)

    def test_numeric_param_missing_min(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "P", "default": 14, "max": 200}}'))
        with pytest.raises(ValueError, match="missing 'min'"):
            validate_and_load_strategy(p)

    def test_numeric_param_missing_max(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "P", "default": 14, "min": 2}}'))
        with pytest.raises(ValueError, match="missing 'max'"):
            validate_and_load_strategy(p)

    def test_param_min_greater_than_max(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "P", "default": 14, "min": 200, "max": 2}}'))
        with pytest.raises(ValueError, match="min .* must be <= max"):
            validate_and_load_strategy(p)

    def test_param_default_below_min(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "P", "default": 1, "min": 2, "max": 200}}'))
        with pytest.raises(ValueError, match="default .* outside range"):
            validate_and_load_strategy(p)

    def test_param_default_above_max(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "P", "default": 999, "min": 2, "max": 200}}'))
        with pytest.raises(ValueError, match="default .* outside range"):
            validate_and_load_strategy(p)

    def test_param_key_not_in_config(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"nonexistent": {"label": "X", "default": 5, "min": 1, "max": 10}}'))
        with pytest.raises(ValueError, match="not found in CONFIG_CLASS fields"):
            validate_and_load_strategy(p)

    def test_param_unsupported_default_type(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code('{"my_period": {"label": "P", "default": "hello"}}'))
        with pytest.raises(ValueError, match="must be int, float, or bool"):
            validate_and_load_strategy(p)

    def test_empty_params_dict(self, tmp_path: Path):
        p = _write(tmp_path, self._make_code("{}"))
        result = validate_and_load_strategy(p)
        assert result["params"] == {}

    def test_bool_param_with_min_max(self, tmp_path: Path):
        """Bool params with extra min/max keys should be silently accepted."""
        p = _write(tmp_path, self._make_code(
            '{"my_bool": {"label": "Flag", "default": False, "min": 0, "max": 1}}'
        ))
        result = validate_and_load_strategy(p)
        assert "my_bool" in result["params"]


# ===========================================================================
# Group 6: Happy Path
# ===========================================================================

class TestHappyPath:
    def test_valid_strategy_loads_successfully(self, tmp_path: Path):
        p = _write(tmp_path, VALID_STRATEGY_SRC)
        result = validate_and_load_strategy(p)
        assert result["description"] == "A valid test strategy."
        assert "strategy_class" in result
        assert "config_class" in result
        assert "params" in result

    def test_valid_strategy_with_int_params(self, tmp_path: Path):
        p = _write(tmp_path, VALID_STRATEGY_SRC)
        result = validate_and_load_strategy(p)
        assert result["params"]["my_period"]["default"] == 14

    def test_valid_strategy_with_float_params(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace(
            "my_period: PositiveInt = 14",
            "my_period: PositiveInt = 14\n    my_threshold: float = 0.5",
        ).replace(
            '"my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},',
            '"my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},\n'
            '    "my_threshold": {"label": "Threshold", "min": 0.0, "max": 5.0, "default": 0.5},',
        )
        p = _write(tmp_path, code, "float_param.py")
        result = validate_and_load_strategy(p)
        assert result["params"]["my_threshold"]["default"] == 0.5

    def test_valid_strategy_with_bool_params(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace(
            "my_period: PositiveInt = 14",
            "my_period: PositiveInt = 14\n    use_filter: bool = False",
        ).replace(
            '"my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},',
            '"my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},\n'
            '    "use_filter": {"label": "Use Filter", "default": False},',
        )
        p = _write(tmp_path, code, "bool_param.py")
        result = validate_and_load_strategy(p)
        assert result["params"]["use_filter"]["default"] is False

    def test_valid_strategy_with_no_params(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace(
            'PARAMS = {\n    "my_period": {"label": "My Period", "min": 2, "max": 200, "default": 14},\n}',
            "PARAMS = {}",
        )
        p = _write(tmp_path, code, "no_params.py")
        result = validate_and_load_strategy(p)
        assert result["params"] == {}


# ===========================================================================
# Group 7: load_all_custom_strategies
# ===========================================================================

class TestLoadAllCustomStrategies:
    def test_empty_directory(self, tmp_path: Path):
        strategies, warnings = load_all_custom_strategies(tmp_path)
        assert strategies == {}
        assert warnings == []

    def test_single_valid_file(self, tmp_path: Path):
        _write(tmp_path, VALID_STRATEGY_SRC, "good.py")
        strategies, warnings = load_all_custom_strategies(tmp_path)
        assert "Good Strategy" in strategies
        assert warnings == []

    def test_multiple_valid_files(self, tmp_path: Path):
        _write(tmp_path, VALID_STRATEGY_SRC, "good1.py")
        code2 = VALID_STRATEGY_SRC.replace('STRATEGY_NAME = "Good Strategy"', 'STRATEGY_NAME = "Second Strategy"')
        _write(tmp_path, code2, "good2.py")
        strategies, warnings = load_all_custom_strategies(tmp_path)
        assert len(strategies) == 2
        assert "Good Strategy" in strategies
        assert "Second Strategy" in strategies

    def test_mix_valid_and_invalid(self, tmp_path: Path):
        _write(tmp_path, VALID_STRATEGY_SRC, "good.py")
        _write(tmp_path, "raise Exception('boom')\n", "bad.py")
        strategies, warnings = load_all_custom_strategies(tmp_path)
        assert "Good Strategy" in strategies
        assert len(warnings) == 1
        assert "bad.py" in warnings[0]

    def test_duplicate_strategy_names(self, tmp_path: Path):
        _write(tmp_path, VALID_STRATEGY_SRC, "first.py")
        _write(tmp_path, VALID_STRATEGY_SRC, "second.py")
        strategies, warnings = load_all_custom_strategies(tmp_path)
        assert len(strategies) == 1  # first wins
        assert any("Duplicate" in w for w in warnings)

    def test_skips_dunder_files(self, tmp_path: Path):
        _write(tmp_path, "x = 1\n", "__init__.py")
        _write(tmp_path, VALID_STRATEGY_SRC, "good.py")
        strategies, warnings = load_all_custom_strategies(tmp_path)
        assert len(strategies) == 1

    def test_nonexistent_directory(self, tmp_path: Path):
        strategies, warnings = load_all_custom_strategies(tmp_path / "does_not_exist")
        assert strategies == {}
        assert warnings == []


# ===========================================================================
# Group 8: get_merged_registry
# ===========================================================================

class TestGetMergedRegistry:
    def test_no_custom_strategies(self, tmp_path: Path):
        merged, warnings = get_merged_registry(tmp_path)
        # Should have the 5 built-in strategies
        assert "EMA Cross" in merged
        assert "RSI Mean Reversion" in merged
        assert "Bollinger Bands" in merged
        assert "4 Moving Averages" in merged
        assert "Range Breakout" in merged
        assert len(merged) == 5

    def test_with_custom_strategies(self, tmp_path: Path):
        _write(tmp_path, VALID_STRATEGY_SRC, "custom.py")
        merged, warnings = get_merged_registry(tmp_path)
        assert "Good Strategy" in merged
        assert len(merged) == 6  # 5 built-in + 1 custom

    def test_name_collision_with_builtin(self, tmp_path: Path):
        code = VALID_STRATEGY_SRC.replace('STRATEGY_NAME = "Good Strategy"', 'STRATEGY_NAME = "EMA Cross"')
        _write(tmp_path, code, "collision.py")
        merged, warnings = get_merged_registry(tmp_path)
        assert "(Custom) EMA Cross" in merged
        assert "EMA Cross" in merged  # built-in preserved
        assert any("conflicts" in w for w in warnings)

    def test_custom_strategies_dir_missing(self, tmp_path: Path):
        merged, warnings = get_merged_registry(tmp_path / "nonexistent")
        assert len(merged) == 5  # just built-ins


# ===========================================================================
# Group 9: get_strategy_template
# ===========================================================================

class TestGetStrategyTemplate:
    def test_template_is_valid_python(self):
        template = get_strategy_template()
        # Should not raise SyntaxError
        compile(template, "<template>", "exec")

    def test_template_has_all_exports(self):
        template = get_strategy_template()
        for export in ["STRATEGY_NAME", "STRATEGY_CLASS", "CONFIG_CLASS", "DESCRIPTION", "PARAMS"]:
            assert export in template

    def test_template_passes_validation(self, tmp_path: Path):
        template = get_strategy_template()
        p = tmp_path / "template_test.py"
        p.write_text(template, encoding="utf-8")
        result = validate_and_load_strategy(p)
        assert result["description"] == "Example: Buy/sell when price crosses EMA +/- threshold percentage."
        assert "strategy_class" in result


# ===========================================================================
# Group 10: Filename Sanitization
# ===========================================================================

class TestFilenameSanitization:
    def test_filename_with_spaces(self):
        assert sanitize_filename("my strategy.py") == "my_strategy.py"

    def test_filename_with_special_chars(self):
        result = sanitize_filename("my@strategy#v2!.py")
        assert "@" not in result
        assert "#" not in result
        assert "!" not in result
        assert result.endswith(".py")

    def test_filename_already_clean(self):
        assert sanitize_filename("good_strategy.py") == "good_strategy.py"

    def test_filename_with_multiple_spaces(self):
        result = sanitize_filename("my   strategy   file.py")
        # Multiple underscores collapsed
        assert "___" not in result
        assert result.endswith(".py")

    def test_filename_hyphen_preserved(self):
        assert sanitize_filename("my-strategy.py") == "my-strategy.py"
