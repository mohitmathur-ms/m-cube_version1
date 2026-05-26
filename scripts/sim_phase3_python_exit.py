"""Phase 3 simulation — Python SL check per-1-min vs deferred-to-5-min.

Signal is built by the repo's ``core.aggregator.BarAggregator`` (custom, NOT a
native composite). Exit uses the PYTHON state machine (a market close on breach,
as m-cube does today). We run the same data twice:

  flag ON  (manage_exits_on_base_bar=True)  -> SL checked on EVERY 1-min base bar
  flag OFF (today's behaviour)              -> SL checked only on the emitted 5-min bar

Proves the flag makes the Python exit fire on the MID-WINDOW 1-min bar where the
breach happens, instead of being deferred to the next 5-min window close — and
quantifies the slippage reduction. (This path is for legs using non-native exit
features that a bracket cannot express.)

    venv\\Scripts\\python.exe scripts\\sim_phase3_python_exit.py
"""
from __future__ import annotations

import os
import sys
from decimal import Decimal

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

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
    PriceType,
    TimeInForce,
)
from nautilus_trader.model.identifiers import InstrumentId, TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

from core.aggregator import BarAggregator

PIP = 0.0001
_MIN_NS = 60_000_000_000
_WIN_NS = 300_000_000_000
SL_OFFSET = 0.00100  # SL = signal close - 10 pips

# (open, high, low, close). Window1 = m0..m4 (signal close 1.10025 -> SL 1.09925).
# Entry on m5. SL breached mid-window at m7. Price keeps dropping to m10 so the
# deferred (5-min) exit fills materially worse.
BARS = [
    (1.10020, 1.10030, 1.10015, 1.10022),  # m0
    (1.10022, 1.10028, 1.10018, 1.10024),  # m1
    (1.10024, 1.10030, 1.10020, 1.10025),  # m2
    (1.10025, 1.10032, 1.10021, 1.10026),  # m3
    (1.10026, 1.10030, 1.10022, 1.10025),  # m4  window-1 close 1.10025
    (1.10025, 1.10030, 1.10020, 1.10025),  # m5  emit window1 -> enter; entry fill 1.10025
    (1.10025, 1.10028, 1.10010, 1.10020),  # m6
    (1.10020, 1.10022, 1.09890, 1.09950),  # m7  low 1.09890 < SL 1.09925 (breach)
    (1.09950, 1.09955, 1.09850, 1.09870),  # m8
    (1.09870, 1.09875, 1.09800, 1.09810),  # m9
    (1.09810, 1.09815, 1.09790, 1.09800),  # m10 emit window2 -> deferred exit fires; fill 1.09800
    (1.09800, 1.09805, 1.09790, 1.09800),  # m11 settle
]


class Phase3Config(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    base_bar_type: BarType
    target_bar_type: BarType
    qty: int
    sl_offset: float
    manage_exits_on_base_bar: bool


class Phase3Strategy(Strategy):
    def __init__(self, config: Phase3Config) -> None:
        super().__init__(config)
        self._agg: BarAggregator | None = None
        self._entered = False
        self._exited = False
        self.sl_level: float | None = None
        self.fills: list[tuple[str, float, int]] = []

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self._agg = BarAggregator(self.instrument, self.config.target_bar_type, "5-MINUTE")
        self.subscribe_bars(self.config.base_bar_type)

    def _close(self) -> None:
        order = self.order_factory.market(
            instrument_id=self.config.instrument_id, order_side=OrderSide.SELL,
            quantity=self.instrument.make_qty(self.config.qty), time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)
        self._exited = True

    def on_bar(self, bar: Bar) -> None:
        agg_bar = self._agg.on_bar(bar)

        # EXIT path on every 1-min base bar (flag ON only).
        if (self.config.manage_exits_on_base_bar and self._entered and not self._exited
                and float(bar.low) <= self.sl_level):
            self._close()

        if agg_bar is None:
            return

        # SIGNAL/entry on the emitted 5-min bar.
        if not self._entered:
            self.sl_level = float(agg_bar.close) - self.config.sl_offset
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id, order_side=OrderSide.BUY,
                quantity=self.instrument.make_qty(self.config.qty), time_in_force=TimeInForce.GTC,
            )
            self.submit_order(order)
            self._entered = True
        # Deferred EXIT on the 5-min bar (flag OFF — today's behaviour).
        elif (not self.config.manage_exits_on_base_bar and not self._exited
              and float(agg_bar.low) <= self.sl_level):
            self._close()

    def on_order_filled(self, event) -> None:
        side = "BUY" if event.order_side == OrderSide.BUY else "SELL"
        self.fills.append((side, float(event.last_px), event.ts_event))


def run(flag_on: bool):
    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    base_bt = BarType(instrument.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
    target_bt = BarType(instrument.id, BarSpecification(5, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
    t0 = (1_700_000_000_000_000_000 // _WIN_NS) * _WIN_NS

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId("SIMP3-001"), logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True), run_analysis=False,
    ))
    engine.add_venue(
        venue=instrument.id.venue, oms_type=OmsType.NETTING, account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)], base_currency=USD, default_leverage=Decimal(1),
    )
    engine.add_instrument(instrument)
    bars = [
        Bar(bar_type=base_bt, open=instrument.make_price(o), high=instrument.make_price(h),
            low=instrument.make_price(l), close=instrument.make_price(c),
            volume=instrument.make_qty(1_000_000), ts_event=t0 + i * _MIN_NS, ts_init=t0 + i * _MIN_NS)
        for i, (o, h, l, c) in enumerate(BARS)
    ]
    engine.add_data(bars)
    strat = Phase3Strategy(Phase3Config(
        instrument_id=instrument.id, base_bar_type=base_bt, target_bar_type=target_bt,
        qty=1_000_000, sl_offset=SL_OFFSET, manage_exits_on_base_bar=flag_on,
    ))
    engine.add_strategy(strat)
    engine.run()
    sells = [(p, ts) for s, p, ts in strat.fills if s == "SELL"]
    exit_px, exit_ts = sells[0] if sells else (float("nan"), None)
    exit_min = (exit_ts - t0) // _MIN_NS if exit_ts is not None else None
    slip = (exit_px - strat.sl_level) / PIP
    engine.dispose()
    return exit_min, exit_px, slip


def main() -> None:
    on_min, on_px, on_slip = run(flag_on=True)
    off_min, off_px, off_slip = run(flag_on=False)

    print("=" * 74)
    print("  Phase 3 — Python SL on a custom-aggregator 5-min signal (SL=1.09925)")
    print("=" * 74)
    print(f"  {'mode':<34}{'exit minute':<13}{'fill':<11}{'slip(pips)':<11}")
    print("  " + "-" * 70)
    print(f"  {'flag ON  (check every 1-min bar)':<34}{on_min!s:<13}{on_px:<11.5f}{on_slip:<+11.1f}")
    print(f"  {'flag OFF (check on 5-min bar only)':<34}{off_min!s:<13}{off_px:<11.5f}{off_slip:<+11.1f}")

    earlier = on_min is not None and off_min is not None and on_min < off_min
    on_mid_window = on_min is not None and (on_min % 5 != 0)
    off_on_boundary = off_min is not None and (off_min % 5 == 0)
    tighter = abs(on_slip) < abs(off_slip)

    print("\n  Assertions:")
    print(f"    (a) flag ON exits earlier than flag OFF       : {'PASS' if earlier else 'FAIL'}  ({on_min} < {off_min})")
    print(f"    (b) flag ON exit is MID-WINDOW (min %% 5 != 0)  : {'PASS' if on_mid_window else 'FAIL'}")
    print(f"    (c) flag OFF exit deferred to 5-min boundary  : {'PASS' if off_on_boundary else 'FAIL'}")
    print(f"    (d) flag ON slippage tighter than flag OFF    : {'PASS' if tighter else 'FAIL'}  ({abs(on_slip):.1f} < {abs(off_slip):.1f} pips)")

    if not (earlier and on_mid_window and off_on_boundary and tighter):
        sys.exit(1)
    print("\n  PHASE 3 SIM: PASS")


if __name__ == "__main__":
    main()
