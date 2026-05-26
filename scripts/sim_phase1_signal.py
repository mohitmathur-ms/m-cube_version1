"""Phase 1 simulation — custom BarAggregator builds the 5-min SIGNAL from 1-min.

Proves (with the REAL BacktestEngine + the repo's own
``core.aggregator.BarAggregator``, NOT native composite bars):
  (a) the emitted 5-min bar OHLC = first-open / max-high / min-low / last-close
      of each 1-minute window, and
  (b) the entry/signal fires only on a 5-minute boundary (ts %% 300s == 0) even
      though the engine is fed 1-minute data.

    venv\\Scripts\\python.exe scripts\\sim_phase1_signal.py
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

_MIN_NS = 60_000_000_000
_WIN_NS = 300_000_000_000  # 5 minutes

# Three 1-min windows of 5 bars each, with deliberately distinct O/H/L/C so the
# aggregated OHLC is unambiguous. (open, high, low, close) per minute.
WINDOWS = [
    [  # window 1
        (1.10000, 1.10010, 1.09990, 1.10005),
        (1.10005, 1.10040, 1.10000, 1.10020),
        (1.10020, 1.10025, 1.09980, 1.10010),  # window low 1.09980
        (1.10010, 1.10050, 1.10005, 1.10030),  # window high 1.10050
        (1.10030, 1.10035, 1.10015, 1.10025),  # window close 1.10025
    ],
    [  # window 2
        (1.10025, 1.10060, 1.10020, 1.10040),
        (1.10040, 1.10070, 1.10030, 1.10055),  # window high 1.10070
        (1.10055, 1.10060, 1.10010, 1.10030),  # window low 1.10010
        (1.10030, 1.10045, 1.10025, 1.10035),
        (1.10035, 1.10050, 1.10020, 1.10045),  # window close 1.10045
    ],
    [  # window 3 (only its first bar is fed during the run → flushes window 2)
        (1.10045, 1.10048, 1.10040, 1.10046),
    ],
]


def _expected(window):
    return (
        window[0][0],                       # open  = first open
        max(b[1] for b in window),          # high  = max high
        min(b[2] for b in window),          # low   = min low
        window[-1][3],                      # close = last close
    )


class Phase1Config(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    base_bar_type: BarType
    target_bar_type: BarType
    qty: int


class Phase1Strategy(Strategy):
    def __init__(self, config: Phase1Config) -> None:
        super().__init__(config)
        self._agg: BarAggregator | None = None
        self._entered = False
        self.emitted: list[Bar] = []
        self.entry_signal_ts: int | None = None

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self._agg = BarAggregator(self.instrument, self.config.target_bar_type, "5-MINUTE")
        self.subscribe_bars(self.config.base_bar_type)

    def on_bar(self, bar: Bar) -> None:
        # Feed every BASE 1-min bar to the custom aggregator. Signal/entry runs
        # only when a 5-min window emits — mirrors ManagedExitStrategy aggregating.
        agg_bar = self._agg.on_bar(bar)
        if agg_bar is None:
            return
        self.emitted.append(agg_bar)
        if not self._entered:
            self.entry_signal_ts = agg_bar.ts_event
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=self.instrument.make_qty(self.config.qty),
                time_in_force=TimeInForce.GTC,
            )
            self.submit_order(order)
            self._entered = True


def main() -> None:
    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    base_bt = BarType(instrument.id, BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)
    target_bt = BarType(instrument.id, BarSpecification(5, BarAggregation.MINUTE, PriceType.LAST), AggregationSource.EXTERNAL)

    # epoch-aligned 5-min boundary so window_start floors cleanly
    t0 = (1_700_000_000_000_000_000 // _WIN_NS) * _WIN_NS

    engine = BacktestEngine(config=BacktestEngineConfig(
        trader_id=TraderId("SIMP1-001"),
        logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True),
        run_analysis=False,
    ))
    engine.add_venue(
        venue=instrument.id.venue, oms_type=OmsType.NETTING, account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)], base_currency=USD, default_leverage=Decimal(1),
    )
    engine.add_instrument(instrument)

    bars: list[Bar] = []
    minute = 0
    for win in WINDOWS:
        for (o, h, l, c) in win:
            ts = t0 + minute * _MIN_NS
            bars.append(Bar(
                bar_type=base_bt,
                open=instrument.make_price(o), high=instrument.make_price(h),
                low=instrument.make_price(l), close=instrument.make_price(c),
                volume=instrument.make_qty(1_000_000), ts_event=ts, ts_init=ts,
            ))
            minute += 1
    engine.add_data(bars)

    strat = Phase1Strategy(Phase1Config(
        instrument_id=instrument.id, base_bar_type=base_bt, target_bar_type=target_bt, qty=1_000_000,
    ))
    engine.add_strategy(strat)
    engine.run()

    print("=" * 74)
    print("  Phase 1 — custom BarAggregator 5-min signal from 1-min data")
    print("=" * 74)
    print(f"  fed {len(bars)} one-minute bars; aggregator emitted {len(strat.emitted)} five-minute bars\n")

    print(f"  {'Window':<8}{'O/H/L/C emitted':<44}{'expected':<22}{'ok':<4}")
    print("  " + "-" * 72)
    all_ohlc_ok = True
    for i, agg in enumerate(strat.emitted):
        exp = _expected(WINDOWS[i])
        got = (float(agg.open), float(agg.high), float(agg.low), float(agg.close))
        ok = all(abs(a - b) < 1e-9 for a, b in zip(got, exp))
        all_ohlc_ok &= ok
        got_s = "/".join(f"{x:.5f}" for x in got)
        exp_s = "/".join(f"{x:.5f}" for x in exp)
        print(f"  {i + 1:<8}{got_s:<44}{exp_s:<22}{'YES' if ok else 'NO':<4}")

    # Boundary checks (relative to t0, which is an epoch 5-min boundary).
    emit_offsets = [(a.ts_event - t0) for a in strat.emitted]
    boundary_ok = all(off % _WIN_NS == 0 for off in emit_offsets)
    sig_off = (strat.entry_signal_ts - t0) if strat.entry_signal_ts is not None else None
    entry_on_boundary = sig_off is not None and sig_off % _WIN_NS == 0

    print("\n  Emitted 5-min timestamps (minutes from t0): "
          + ", ".join(str(off // _MIN_NS) for off in emit_offsets))
    print(f"  Entry/signal at minute {sig_off // _MIN_NS if sig_off is not None else 'NONE'} from t0")
    print("\n  Assertions:")
    print(f"    (a) emitted 5-min OHLC == first/max/min/last per window : {'PASS' if all_ohlc_ok else 'FAIL'}")
    print(f"    (b) every emitted 5-min bar lands on a 5-min boundary    : {'PASS' if boundary_ok else 'FAIL'}")
    print(f"    (c) entry/signal lands on a 5-min boundary               : {'PASS' if entry_on_boundary else 'FAIL'}")
    engine.dispose()

    if not (all_ohlc_ok and boundary_ok and entry_on_boundary):
        sys.exit(1)
    print("\n  PHASE 1 SIM: PASS")


if __name__ == "__main__":
    main()
