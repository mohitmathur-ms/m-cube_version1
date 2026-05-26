"""Behavioural tests for the native-bracket exit path (``_USE_NATIVE_BRACKET``).

Covers the two fixes made to ``ManagedExitStrategy._submit_bracket``:

* **L-2** — a native bracket's resting SL/TP children must carry exit-reason tags
  whose prefix matches the Python exit path (``"Stop Loss: …"`` / ``"Take Profit: …"``)
  so the orderbook's EXIT REASON column (``report_generator`` splits on the first
  ``":"``) is consistent across both exit modes. Asserted end-to-end in a real
  ``BacktestEngine``.

* **L-1** — when the SL/TP trigger prices can't be computed (e.g. a cold ATR on the
  first signal, since ``_classify_exit_mode`` admits ATR legs on the multiplier alone),
  ``_submit_bracket`` must NOT leave the leg as ``native_bracket`` with a bare market
  entry — that would skip the Python ``_check_exits`` and leave the position with no
  resting SL/TP and no monitoring. It must fall back onto the Python exit path. Asserted
  as a focused unit test on ``_submit_bracket`` with the engine-bound calls stubbed,
  matching the standalone-strategy pattern in ``test_multi_tf_exits.py``.
"""
from __future__ import annotations

from decimal import Decimal

from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.config import LoggingConfig, RiskEngineConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar, BarSpecification, BarType
from nautilus_trader.model.enums import (
    AccountType,
    AggregationSource,
    BarAggregation,
    OmsType,
    OrderSide,
    OrderType,
    PriceType,
)
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestInstrumentProvider

from core.managed_strategy import ManagedExitStrategy, config_from_exit
from core.models import ExitConfig

_MIN_NS = 60_000_000_000
SL_POINTS = 0.00100
TP_POINTS = 0.00200

# Rising warmup (EMA fast>=slow -> BUY once initialised) then ONE sharp drop that
# pierces the stop. (open, high, low, close) per minute.
BARS = [
    (1.10000, 1.10002, 1.09999, 1.10000),
    (1.10000, 1.10007, 1.09999, 1.10005),
    (1.10005, 1.10012, 1.10004, 1.10010),
    (1.10010, 1.10017, 1.10009, 1.10015),  # EMA initialised -> BUY; entry here
    (1.10015, 1.10017, 1.09800, 1.09850),  # sharp drop -> SL trigger fires
]


# ── L-2: native-bracket children carry matching exit-reason tags ─────────────

def test_native_bracket_children_carry_exit_reason_tags(monkeypatch):
    """A plain SL+TP leg under the flag enters via a native bracket whose
    STOP_MARKET / LIMIT children are tagged ``"Stop Loss: …"`` / ``"Take Profit: …"``
    — the prefix the orderbook's EXIT REASON column consumes."""
    monkeypatch.setenv("_USE_NATIVE_BRACKET", "1")  # read at config_from_exit time

    inst = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    bt = BarType(inst.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                 AggregationSource.EXTERNAL)
    t0 = 1_700_000_000_000_000_000

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId("NB-TAG-001"), logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True), run_analysis=False,
    ))
    engine.add_venue(
        venue=inst.id.venue, oms_type=OmsType.NETTING, account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)], base_currency=USD,
        default_leverage=Decimal(1), bar_adaptive_high_low_ordering=True,
    )
    engine.add_instrument(inst)
    engine.add_data([
        Bar(bar_type=bt, open=inst.make_price(o), high=inst.make_price(h),
            low=inst.make_price(l), close=inst.make_price(c),
            volume=inst.make_qty(1_000_000), ts_event=t0 + i * _MIN_NS,
            ts_init=t0 + i * _MIN_NS)
        for i, (o, h, l, c) in enumerate(BARS)
    ])

    ec = ExitConfig(stop_loss_type="points", stop_loss_value=SL_POINTS,
                    target_type="points", target_value=TP_POINTS)
    cfg = config_from_exit(ec, "EMA Cross", {"fast_ema_period": 2, "slow_ema_period": 3},
                           inst.id, bt, 1_000_000)
    assert cfg.exit_mode == "native_bracket"  # precondition: the path under test
    engine.add_strategy(ManagedExitStrategy(cfg))
    engine.run()

    # First bracket (the leg may re-enter after the SL fires; assert on trade #1).
    order_lists = list(engine.cache.order_lists())
    first_ol = min(order_lists, key=lambda ol: min(o.ts_init for o in ol.orders))
    by_type = {o.order_type: o for o in first_ol.orders}
    stop = by_type[OrderType.STOP_MARKET]
    tp = by_type[OrderType.LIMIT]

    def _tag(order) -> str:
        tags = order.tags
        return str(tags[0]) if tags else ""

    # The EXIT REASON column is tag.split(":", 1)[0] — assert the prefix matches
    # the Python path's labels.
    assert _tag(stop).split(":", 1)[0] == "Stop Loss"
    assert _tag(tp).split(":", 1)[0] == "Take Profit"
    engine.dispose()


# ── L-1: uncomputable levels fall back onto the Python exit path ─────────────

def _flat_native_strategy():
    """A native-bracket leg in flat state with engine-bound calls stubbed."""
    inst = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    bt = BarType(inst.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
                 AggregationSource.EXTERNAL)
    cfg = config_from_exit(
        exit_config=ExitConfig(stop_loss_type="points", stop_loss_value=SL_POINTS,
                               target_type="points", target_value=TP_POINTS),
        signal_name="EMA Cross", signal_params={},
        instrument_id=inst.id, bar_type=bt, trade_size=1000,
    )
    strat = ManagedExitStrategy(cfg)
    strat._exit_mode = "native_bracket"  # the path under test

    # self.log is a read-only Actor attribute (a real logger usable unregistered),
    # so we don't stub it — the warning just writes to the log system.
    calls = {"market": 0, "bracket": 0}
    strat._submit_order = lambda *a, **k: calls.__setitem__("market", calls["market"] + 1)
    strat.submit_order_list = lambda *a, **k: calls.__setitem__("bracket", calls["bracket"] + 1)
    return strat, calls


def test_fallback_when_levels_unavailable_switches_to_python_exit():
    """When _compute_sl_tp can't produce usable levels, the leg must switch to the
    Python exit path (so _on_primary_bar keeps monitoring it) and submit a plain
    market entry — never a bracket and never an unmanaged native leg."""
    strat, calls = _flat_native_strategy()
    strat._compute_sl_tp = lambda is_buy, ref: (0.0, 0.0)  # cold-ATR style failure

    strat._submit_bracket(OrderSide.BUY, 1.10000)

    assert strat._exit_mode == "python"   # monitored by _check_exits hereafter
    assert calls["market"] == 1           # plain market entry submitted
    assert calls["bracket"] == 0          # no native bracket submitted
