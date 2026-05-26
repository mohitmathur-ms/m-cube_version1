"""End-to-end test of the native-bracket production wiring in ManagedExitStrategy.

Exercises the REAL code path (config_from_exit classification + the
ManagedExitStrategy entry/exit routing) inside a real BacktestEngine — not a
standalone harness. Two parts:

  Part A — classification: config_from_exit assigns exit_mode correctly
            (plain SL+TP close-only -> native_bracket under _USE_NATIVE_BRACKET=1;
             complex legs / flag off -> python).
  Part B — engine run: a plain-SL/TP EMA-Cross leg (flag on) enters via a native
            bracket (MARKET + STOP_MARKET + LIMIT sharing one order_list_id) and
            the SL fills at the trigger price when a later 1-min bar pierces it.

    venv\\Scripts\\python.exe scripts\\sim_native_bracket_e2e.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

os.environ["_USE_NATIVE_BRACKET"] = "1"  # set before config_from_exit is called

from nautilus_trader.backtest.engine import BacktestEngine, BacktestEngineConfig
from nautilus_trader.config import LoggingConfig, RiskEngineConfig
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import Bar, BarSpecification, BarType
from nautilus_trader.model.enums import (
    AccountType,
    AggregationSource,
    BarAggregation,
    OmsType,
    OrderType,
    PriceType,
)
from nautilus_trader.model.identifiers import TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestInstrumentProvider

from core.managed_strategy import ManagedExitStrategy, config_from_exit
from core.models import ExitConfig

PIP = 0.0001
_MIN_NS = 60_000_000_000

# Rising warmup (EMA fast>=slow -> BUY once initialised) then ONE sharp drop that
# pierces the stop. Kept to 5 bars so exactly one entry+exit occurs (no further
# bars => no EMA re-entry). (open, high, low, close) per minute.
BARS = [
    (1.10000, 1.10002, 1.09999, 1.10000),
    (1.10000, 1.10007, 1.09999, 1.10005),
    (1.10005, 1.10012, 1.10004, 1.10010),
    (1.10010, 1.10017, 1.10009, 1.10015),  # EMA initialised -> BUY; entry here
    (1.10015, 1.10017, 1.09800, 1.09850),  # sharp drop -> SL trigger fires
]
SL_POINTS = 0.00100
TP_POINTS = 0.00200


def part_a() -> bool:
    inst = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    bt = BarType(inst.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)

    def mk(ec: ExitConfig):
        return config_from_exit(ec, "EMA Cross", {}, inst.id, bt, 1_000_000).exit_mode

    plain = ExitConfig(stop_loss_type="points", stop_loss_value=SL_POINTS,
                       target_type="points", target_value=TP_POINTS)
    trailing = ExitConfig(stop_loss_type="trailing", stop_loss_value=1.0,
                          target_type="points", target_value=TP_POINTS)
    sl_only = ExitConfig(stop_loss_type="points", stop_loss_value=SL_POINTS)
    reexec = ExitConfig(stop_loss_type="points", stop_loss_value=SL_POINTS,
                        target_type="points", target_value=TP_POINTS, on_sl_action="re_execute")

    checks = [
        ("plain SL+TP, flag on  -> native_bracket", mk(plain) == "native_bracket"),
        ("trailing SL           -> python", mk(trailing) == "python"),
        ("SL only (no TP)       -> python", mk(sl_only) == "python"),
        ("on_sl_action=re_execute-> python", mk(reexec) == "python"),
    ]
    os.environ["_USE_NATIVE_BRACKET"] = "0"
    checks.append(("plain SL+TP, flag OFF  -> python", mk(plain) == "python"))
    os.environ["_USE_NATIVE_BRACKET"] = "1"

    print("  Part A — config_from_exit classification")
    ok = True
    for label, passed in checks:
        ok &= passed
        print(f"    [{'PASS' if passed else 'FAIL'}] {label}")
    return ok


def part_b() -> bool:
    inst = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    bt = BarType(inst.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
    t0 = 1_700_000_000_000_000_000

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId("E2E-001"), logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True), run_analysis=False,
    ))
    engine.add_venue(
        venue=inst.id.venue, oms_type=OmsType.NETTING, account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)], base_currency=USD, default_leverage=Decimal(1),
        bar_adaptive_high_low_ordering=True,
    )
    engine.add_instrument(inst)
    bars = [
        Bar(bar_type=bt, open=inst.make_price(o), high=inst.make_price(h),
            low=inst.make_price(l), close=inst.make_price(c),
            volume=inst.make_qty(1_000_000), ts_event=t0 + i * _MIN_NS, ts_init=t0 + i * _MIN_NS)
        for i, (o, h, l, c) in enumerate(BARS)
    ]
    engine.add_data(bars)

    ec = ExitConfig(stop_loss_type="points", stop_loss_value=SL_POINTS,
                    target_type="points", target_value=TP_POINTS)
    cfg = config_from_exit(ec, "EMA Cross", {"fast_ema_period": 2, "slow_ema_period": 3},
                           inst.id, bt, 1_000_000)
    engine.add_strategy(ManagedExitStrategy(cfg))
    engine.run()

    # Isolate the FIRST bracket (the strategy may re-enter after the SL fires;
    # we assert on trade #1). Pick the order list whose entry was submitted first.
    order_lists = list(engine.cache.order_lists())
    first_ol = min(order_lists, key=lambda ol: min(o.ts_init for o in ol.orders))
    by_type = {}
    for o in first_ol.orders:
        by_type[o.order_type] = o
    entry = by_type.get(OrderType.MARKET)
    stop = by_type.get(OrderType.STOP_MARKET)
    tp = by_type.get(OrderType.LIMIT)

    has_bracket = entry is not None and stop is not None and tp is not None
    entry_fill = float(entry.avg_px) if entry and entry.avg_px is not None else None
    sl_trigger = float(stop.trigger_price) if stop is not None else None
    sl_fill = float(stop.avg_px) if stop and stop.avg_px is not None else None
    drop_close = BARS[-1][3]  # what a Python market-close exit would fill at

    # The stop fills AT its trigger (move-through), not at the distant drop-bar
    # close a Python market-close would suffer.
    slip_vs_trigger = abs(sl_fill - sl_trigger) / PIP if (sl_fill and sl_trigger) else float("nan")
    fill_at_trigger = slip_vs_trigger <= 0.5
    mkt_baseline_pips = abs(drop_close - sl_trigger) / PIP if sl_trigger else float("nan")
    much_better = sl_fill is not None and abs(sl_fill - sl_trigger) < abs(drop_close - sl_trigger)

    print("\n  Part B — engine run (exit_mode=%s); first bracket =" % cfg.exit_mode, first_ol.id)
    print(f"    bracket orders: {[str(k) for k in by_type]}")
    print(f"    entry MARKET fill = {entry_fill:.5f}")
    print(f"    SL trigger price  = {sl_trigger:.5f}   SL fill = {sl_fill:.5f}   "
          f"slip-vs-trigger = {slip_vs_trigger:.2f} pips")
    print(f"    (a Python market-close would fill ~{drop_close:.5f}, "
          f"{mkt_baseline_pips:.1f} pips from the stop)")
    print(f"    [{'PASS' if has_bracket else 'FAIL'}] bracket = MARKET + STOP_MARKET + LIMIT (one order_list)")
    print(f"    [{'PASS' if fill_at_trigger else 'FAIL'}] SL filled AT trigger price (<=0.5 pip)")
    print(f"    [{'PASS' if much_better else 'FAIL'}] bracket fill far closer to stop than market-close baseline")
    engine.dispose()
    return bool(has_bracket and fill_at_trigger and much_better)


def main() -> None:
    print("=" * 74)
    print("  Native-bracket production wiring — end-to-end test (_USE_NATIVE_BRACKET=1)")
    print("=" * 74)
    a = part_a()
    b = part_b()
    print("\n  RESULT:", "PASS" if (a and b) else "FAIL")
    if not (a and b):
        sys.exit(1)


if __name__ == "__main__":
    main()
