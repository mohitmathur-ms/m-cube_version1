"""Phase 2 simulation — native bracket SL/TP, signal from custom BarAggregator.

The 5-min SIGNAL is built by the repo's ``core.aggregator.BarAggregator`` (NOT a
native composite bar). On the first emitted 5-min bar the strategy enters a native
``order_factory.bracket()`` (MARKET entry + STOP_MARKET SL + LIMIT TP). The
matching engine then sweeps every 1-minute base bar's O->H->L->C and fires the SL.

Proves: the SL fires on a MID-WINDOW 1-minute bar (timestamp NOT on a 5-min
boundary) and fills at the trigger price (slippage ~ 0) — per-1-min exit accuracy
with a 5-min custom-aggregator signal.

    venv\\Scripts\\python.exe scripts\\sim_phase2_bracket.py
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
SL_OFFSET = 0.00100   # 10 pips below signal close
TP_OFFSET = 0.00200   # 20 pips above signal close

# (open, high, low, close) per minute. Window 1 = minutes 0-4 (signal close
# 1.10025). Entry fills on minute 5. SL breached MID-WINDOW at minute 7.
BARS = [
    (1.10020, 1.10030, 1.10015, 1.10022),  # m0
    (1.10022, 1.10028, 1.10018, 1.10024),  # m1
    (1.10024, 1.10030, 1.10020, 1.10025),  # m2
    (1.10025, 1.10032, 1.10021, 1.10026),  # m3
    (1.10026, 1.10030, 1.10022, 1.10025),  # m4  window-1 close = 1.10025
    (1.10025, 1.10030, 1.10020, 1.10025),  # m5  emit window1 -> enter; entry fill 1.10025
    (1.10025, 1.10028, 1.10010, 1.10020),  # m6  no SL breach (SL=1.09925)
    (1.10020, 1.10022, 1.09890, 1.09950),  # m7  low 1.09890 < SL -> move-through fill 1.09925
    (1.09950, 1.09960, 1.09940, 1.09950),  # m8  settle
    (1.09950, 1.09960, 1.09940, 1.09950),  # m9  settle
]


class Phase2Config(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    base_bar_type: BarType
    target_bar_type: BarType
    qty: int
    sl_offset: float
    tp_offset: float


class Phase2Strategy(Strategy):
    def __init__(self, config: Phase2Config) -> None:
        super().__init__(config)
        self._agg: BarAggregator | None = None
        self._entered = False
        self.sl_level: float | None = None
        self.tp_level: float | None = None
        self.fills: list[tuple[str, float, int]] = []  # (side, price, ts_event)

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self._agg = BarAggregator(self.instrument, self.config.target_bar_type, "5-MINUTE")
        self.subscribe_bars(self.config.base_bar_type)

    def on_bar(self, bar: Bar) -> None:
        agg_bar = self._agg.on_bar(bar)
        if agg_bar is None:
            return
        if self._entered:
            return
        # Compute SL/TP from the 5-min SIGNAL bar close (mirrors production).
        sig_close = float(agg_bar.close)
        self.sl_level = sig_close - self.config.sl_offset
        self.tp_level = sig_close + self.config.tp_offset
        bracket = self.order_factory.bracket(
            instrument_id=self.config.instrument_id,
            order_side=OrderSide.BUY,
            quantity=self.instrument.make_qty(self.config.qty),
            sl_trigger_price=self.instrument.make_price(self.sl_level),
            tp_price=self.instrument.make_price(self.tp_level),
        )
        self.submit_order_list(bracket)
        self._entered = True

    def on_order_filled(self, event) -> None:
        side = "BUY" if event.order_side == OrderSide.BUY else "SELL"
        self.fills.append((side, float(event.last_px), event.ts_event))


def main() -> None:
    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    base_bt = BarType(instrument.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
    target_bt = BarType(instrument.id, BarSpecification(5, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
    t0 = (1_700_000_000_000_000_000 // _WIN_NS) * _WIN_NS

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId("SIMP2-001"),
        logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True),
        run_analysis=False,
    ))
    engine.add_venue(
        venue=instrument.id.venue, oms_type=OmsType.NETTING, account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)], base_currency=USD, default_leverage=Decimal(1),
        bar_adaptive_high_low_ordering=True,
    )
    engine.add_instrument(instrument)
    bars = [
        Bar(bar_type=base_bt, open=instrument.make_price(o), high=instrument.make_price(h),
            low=instrument.make_price(l), close=instrument.make_price(c),
            volume=instrument.make_qty(1_000_000), ts_event=t0 + i * _MIN_NS, ts_init=t0 + i * _MIN_NS)
        for i, (o, h, l, c) in enumerate(BARS)
    ]
    engine.add_data(bars)
    strat = Phase2Strategy(Phase2Config(
        instrument_id=instrument.id, base_bar_type=base_bt, target_bar_type=target_bt,
        qty=1_000_000, sl_offset=SL_OFFSET, tp_offset=TP_OFFSET,
    ))
    engine.add_strategy(strat)
    engine.run()

    buys = [(p, ts) for s, p, ts in strat.fills if s == "BUY"]
    sells = [(p, ts) for s, p, ts in strat.fills if s == "SELL"]
    entry_px, entry_ts = buys[0] if buys else (float("nan"), None)
    exit_px, exit_ts = sells[0] if sells else (float("nan"), None)
    entry_min = (entry_ts - t0) // _MIN_NS if entry_ts is not None else None
    exit_min = (exit_ts - t0) // _MIN_NS if exit_ts is not None else None

    print("=" * 74)
    print("  Phase 2 — native bracket SL/TP on a custom-aggregator 5-min signal")
    print("=" * 74)
    print(f"  signal close (5-min) -> SL={strat.sl_level:.5f}  TP={strat.tp_level:.5f}")
    print(f"  entry  : fill={entry_px:.5f}  at minute {entry_min} from t0 (5-min boundary)")
    print(f"  SL exit: fill={exit_px:.5f}  at minute {exit_min} from t0")

    slip = (exit_px - strat.sl_level) / PIP if (exit_px == exit_px and strat.sl_level) else float("nan")
    exit_mid_window = exit_min is not None and (exit_min % 5 != 0)
    fill_at_trigger = abs(exit_px - strat.sl_level) < 1e-9
    entry_on_boundary = entry_min is not None and (entry_min % 5 == 0)

    print("\n  Assertions:")
    print(f"    (a) entry/signal on a 5-min boundary          : {'PASS' if entry_on_boundary else 'FAIL'}")
    print(f"    (b) SL exit fired MID-WINDOW (minute %% 5 != 0) : {'PASS' if exit_mid_window else 'FAIL'}  (minute {exit_min})")
    print(f"    (c) SL filled at trigger (slippage ~ 0)        : {'PASS' if fill_at_trigger else 'FAIL'}  (slip {slip:+.1f} pips)")
    engine.dispose()

    if not (entry_on_boundary and exit_mid_window and fill_at_trigger):
        sys.exit(1)
    print("\n  PHASE 2 SIM: PASS")


if __name__ == "__main__":
    main()
