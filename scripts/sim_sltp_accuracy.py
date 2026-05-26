"""Phase 0 — empirical SL/TP accuracy simulation (real BacktestEngine).

Compares two exit-firing mechanisms on a fixed synthetic 1-minute bar series,
running the ACTUAL NautilusTrader matching engine (not a hand-trace):

  Scenario 1 "market"  — strategy checks bar high/low vs stored SL/TP in on_bar
                          and submits a MARKET close on breach (what m-cube does
                          today via core/managed_strategy.py).
  Scenario 2 "bracket" — strategy enters via order_factory.bracket() (entry
                          MARKET + STOP_MARKET SL + LIMIT TP); no Python check.

Three cases (LONG EUR/USD, entry 1.10000, SL 1.09900, TP 1.10200):
  B move-through SL · C gap-down SL · D both levels in one bar

Run both with bar_adaptive_high_low_ordering False and True. Prints per-case and
summary accuracy tables. Read-only: touches no production code.

    venv\\Scripts\\python.exe scripts\\sim_sltp_accuracy.py
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
    PriceType,
    TimeInForce,
)
from nautilus_trader.model.identifiers import InstrumentId, TraderId
from nautilus_trader.model.objects import Money
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.trading.config import StrategyConfig
from nautilus_trader.trading.strategy import Strategy

PIP = 0.0001
ENTRY = 1.10000
SL = 1.09900
TP = 1.10200

# case name -> (open, high, low, close) of the single post-entry case bar
CASES = {
    "B move-through SL": (1.10020, 1.10040, 1.09890, 1.09930),
    "C gap-down SL": (1.09850, 1.09870, 1.09800, 1.09820),
    "D both in one bar": (1.10000, 1.10260, 1.09850, 1.10100),
}

_MIN_NS = 60_000_000_000


class SimConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    bar_type: BarType
    scenario: str  # "market" | "bracket"
    sl_level: float
    tp_level: float
    qty: int


class SimStrategy(Strategy):
    def __init__(self, config: SimConfig) -> None:
        super().__init__(config)
        self._entered = False
        self._exited = False
        self.fills: list[tuple[str, float]] = []  # (side, price)

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        self.subscribe_bars(self.config.bar_type)

    def on_bar(self, bar: Bar) -> None:
        if not self._entered:
            self._enter()
            self._entered = True
            return
        if self._exited or self.config.scenario != "market":
            return
        # Scenario 1: Python check on the completed bar, then MARKET close.
        # Check TP before SL (an L2 if-order detail; fill is the bar close
        # either way when both levels are inside one bar).
        if float(bar.high) >= self.config.tp_level or float(bar.low) <= self.config.sl_level:
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.SELL,
                quantity=self.instrument.make_qty(self.config.qty),
                time_in_force=TimeInForce.GTC,
            )
            self.submit_order(order)
            self._exited = True

    def _enter(self) -> None:
        qty = self.instrument.make_qty(self.config.qty)
        if self.config.scenario == "market":
            order = self.order_factory.market(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=qty,
                time_in_force=TimeInForce.GTC,
            )
            self.submit_order(order)
        else:
            bracket = self.order_factory.bracket(
                instrument_id=self.config.instrument_id,
                order_side=OrderSide.BUY,
                quantity=qty,
                sl_trigger_price=self.instrument.make_price(self.config.sl_level),
                tp_price=self.instrument.make_price(self.config.tp_level),
            )
            self.submit_order_list(bracket)

    def on_order_filled(self, event) -> None:
        side = "BUY" if event.order_side == OrderSide.BUY else "SELL"
        self.fills.append((side, float(event.last_px)))


def _bar(bt: BarType, inst, ts: int, o, h, l, c) -> Bar:
    return Bar(
        bar_type=bt,
        open=inst.make_price(o),
        high=inst.make_price(h),
        low=inst.make_price(l),
        close=inst.make_price(c),
        volume=inst.make_qty(1_000_000),
        ts_event=ts,
        ts_init=ts,
    )


def run_case(case_ohlc, scenario: str, adaptive: bool) -> tuple[float, float]:
    """Return (entry_fill, exit_fill) for one isolated episode."""
    instrument = TestInstrumentProvider.default_fx_ccy("EUR/USD")
    bt = BarType(
        instrument.id,
        BarSpecification(1, BarAggregation.MINUTE, PriceType.LAST),
        AggregationSource.EXTERNAL,
    )
    engine = BacktestEngine(
        config=BacktestEngineConfig(
            trader_id=TraderId("SIM-001"),
            logging=LoggingConfig(bypass_logging=True),
            risk_engine=RiskEngineConfig(bypass=True),
            run_analysis=False,
        )
    )
    engine.add_venue(
        venue=instrument.id.venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(1_000_000, USD)],
        base_currency=USD,
        default_leverage=Decimal(1),
        bar_adaptive_high_low_ordering=adaptive,
    )
    engine.add_instrument(instrument)

    o, h, l, c = case_ohlc
    t0 = 1_700_000_000_000_000_000
    bars = [
        _bar(bt, instrument, t0, ENTRY, ENTRY, ENTRY, ENTRY),  # entry bar
        _bar(bt, instrument, t0 + _MIN_NS, o, h, l, c),         # case bar
        _bar(bt, instrument, t0 + 2 * _MIN_NS, c, c, c, c),     # settle bar
    ]
    engine.add_data(bars)

    strat = SimStrategy(
        SimConfig(
            instrument_id=instrument.id,
            bar_type=bt,
            scenario=scenario,
            sl_level=SL,
            tp_level=TP,
            qty=1_000_000,
        )
    )
    engine.add_strategy(strat)
    engine.run()

    buys = [p for s, p in strat.fills if s == "BUY"]
    sells = [p for s, p in strat.fills if s == "SELL"]
    entry_fill = buys[0] if buys else float("nan")
    exit_fill = sells[0] if sells else float("nan")
    engine.dispose()
    return entry_fill, exit_fill


def _label_and_slip(entry_fill: float, exit_fill: float) -> tuple[str, float, float]:
    """Label the exit (TP if fill above entry else SL) and compute slippage pips."""
    if exit_fill != exit_fill:  # nan
        return "NONE", float("nan"), float("nan")
    is_tp = exit_fill >= entry_fill
    intended = TP if is_tp else SL
    slip = (exit_fill - intended) / PIP
    return ("TP" if is_tp else "SL"), intended, slip


def main() -> None:
    scenarios = [("Scenario 1 market", "market"), ("Scenario 2 bracket", "bracket")]
    for adaptive in (False, True):
        print("\n" + "=" * 78)
        print(f"  bar_adaptive_high_low_ordering = {adaptive}")
        print("=" * 78)
        print(f"  LONG EUR/USD  entry={ENTRY:.5f}  SL={SL:.5f}  TP={TP:.5f}  (pip={PIP})")
        print("-" * 78)
        header = f"{'Case':<20}{'Scenario':<20}{'Exit':<6}{'Intended':<11}{'Fill':<11}{'Slip(pips)':<11}"
        print(header)
        print("-" * 78)
        slips: dict[str, list[float]] = {s[0]: [] for s in scenarios}
        for case_name, ohlc in CASES.items():
            for sc_label, sc in scenarios:
                entry_fill, exit_fill = run_case(ohlc, sc, adaptive)
                exit_type, intended, slip = _label_and_slip(entry_fill, exit_fill)
                slips[sc_label].append(abs(slip))
                print(
                    f"{case_name:<20}{sc_label:<20}{exit_type:<6}"
                    f"{intended:<11.5f}{exit_fill:<11.5f}{slip:<+11.1f}"
                )
            print("-" * 78)
        print("\n  Summary (mean |slip|, worst |slip|):")
        for sc_label, _ in scenarios:
            vals = slips[sc_label]
            mean = sum(vals) / len(vals)
            worst = max(vals)
            print(f"    {sc_label:<22} mean={mean:6.2f} pips   worst={worst:6.2f} pips")


if __name__ == "__main__":
    main()
