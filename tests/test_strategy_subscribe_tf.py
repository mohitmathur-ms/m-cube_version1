"""Unit tests for the base-timeframe / strategy-subscribe-timeframe split:
NautilusTrader composite bar types the strategy subscribes to alongside the
base bar type (which stays the catalog data fed to the engine).
"""

from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import InstrumentId

from core.models import (
    ExitConfig, StrategySlotConfig, build_composite_bar_type,
    portfolio_from_dict, portfolio_to_dict,
)
from core.managed_strategy import config_from_exit


# ── build_composite_bar_type ────────────────────────────────────────────────

def test_composite_5min_from_1min_base():
    bt = build_composite_bar_type("EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL", "5-MINUTE")
    assert bt == "EURUSD.FOREX_MS-5-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL"
    # And it must be a parseable Nautilus bar type.
    parsed = BarType.from_str(bt)
    assert str(parsed) == bt


def test_composite_15min_keeps_price_type():
    bt = build_composite_bar_type("USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL", "15-MINUTE")
    assert bt == "USDJPY.FOREX_MS-15-MINUTE-MID-INTERNAL@1-MINUTE-EXTERNAL"


def test_composite_same_tf_is_just_the_base():
    # sub TF == base TF → no aggregation, returns the plain base bar type.
    base = "EURUSD.FOREX_MS-5-MINUTE-BID-EXTERNAL"
    assert build_composite_bar_type(base, "5-MINUTE") == base


def test_composite_malformed_base_returns_empty():
    assert build_composite_bar_type("garbage", "5-MINUTE") == ""
    assert build_composite_bar_type("EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL", "") == ""


# ── StrategySlotConfig — strategy_bar_types field ───────────────────────────

def test_slot_strategy_bar_types_default_empty():
    s = StrategySlotConfig()
    assert s.strategy_bar_types == []


def test_strategy_bar_types_roundtrip():
    pf = portfolio_from_dict({
        "name": "t",
        "slots": [{
            "strategy_name": "EMA Cross",
            "bar_type_str": "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL",
            "strategy_bar_types": [
                "EURUSD.FOREX_MS-5-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL",
                "EURUSD.FOREX_MS-15-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL",
            ],
            "exit_config": {},
        }],
    })
    s = pf.slots[0]
    assert s.bar_type_str == "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL"
    assert len(s.strategy_bar_types) == 2
    rt = portfolio_to_dict(pf)
    assert rt["slots"][0]["strategy_bar_types"][0].endswith("@1-MINUTE-EXTERNAL")
    # Old JSON without the field loads with an empty list.
    old = portfolio_from_dict({"name": "t2", "slots": [
        {"strategy_name": "EMA Cross", "exit_config": {}}]})
    assert old.slots[0].strategy_bar_types == []


# ── config_from_exit — base / strategy-timeframe split ──────────────────────
#
# When a leg selects strategy timeframe(s): config.bar_type becomes the FIRST
# composite (the stream the strategy logic runs on); config.subscribe_bar_types
# holds any FURTHER composites. The base bar type is NOT subscribed — the
# simulated exchange processes engine.add_data (the base) and fills orders
# against it regardless of strategy subscriptions.

_BASE = "EURUSD.SIM-1-MINUTE-MID-EXTERNAL"
_C5 = "EURUSD.SIM-5-MINUTE-MID-INTERNAL@1-MINUTE-EXTERNAL"
_C15 = "EURUSD.SIM-15-MINUTE-MID-INTERNAL@1-MINUTE-EXTERNAL"


def _cfg(subscribe_bar_types=None):
    return config_from_exit(
        exit_config=ExitConfig(), signal_name="EMA Cross", signal_params={},
        instrument_id=InstrumentId.from_str("EURUSD.SIM"),
        bar_type=BarType.from_str(_BASE),
        trade_size=1000, subscribe_bar_types=subscribe_bar_types,
    )


def test_config_no_strategy_tf_keeps_base():
    cfg = _cfg(None)
    assert str(cfg.bar_type) == _BASE
    assert cfg.subscribe_bar_types == []
    cfg2 = _cfg([])
    assert str(cfg2.bar_type) == _BASE and cfg2.subscribe_bar_types == []


def test_config_one_strategy_tf_becomes_signal_bar_type():
    cfg = _cfg([_C5])
    # The strategy operates on the 5-min composite; no extra subscriptions
    # (the base is NOT subscribed — add_data drives matching).
    assert str(cfg.bar_type) == _C5
    assert cfg.subscribe_bar_types == []


def test_config_multiple_strategy_tfs_first_is_primary():
    cfg = _cfg([_C5, _C15])
    assert str(cfg.bar_type) == _C5            # first = primary signal stream
    assert cfg.subscribe_bar_types == [_C15]   # further composite only


def test_config_subscribe_bar_types_drops_falsy():
    cfg = _cfg([_C5, "", None])
    assert str(cfg.bar_type) == _C5
    assert cfg.subscribe_bar_types == []
