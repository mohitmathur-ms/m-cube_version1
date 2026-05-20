"""
Engine-pipeline sniffers for the aggregation_reports feature.

Three views of the same backtest data stream, captured at sequential
stages of NautilusTrader's bar dispatch:

  raw_engine_sniffer  : Bars sitting in ``engine.data`` BEFORE ``engine.run()``,
                        filtered to AggregationSource.EXTERNAL. Dumped via
                        :func:`dump_raw_engine`.
  sniffer_data_engine : Bars the ``DataEngine`` dispatches at runtime to
                        subscribed strategies (EXTERNAL). Captured in
                        :class:`BarSniffer.on_bar`, written in ``on_stop``.
  sniffer_strategy    : Bars after Nautilus's TimeBarAggregator produces
                        INTERNAL aggregations and they arrive at the
                        strategy. Same sniffer, different bucket, same write.

All three CSVs share a single 12-column schema (:data:`SNIFFER_COLUMNS`).

Reference implementation: ipynb/engine_dispatch_verification.ipynb cells 30-31.
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

import pandas as pd

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.config import StrategyConfig
from nautilus_trader.model.data import Bar, BarType
from nautilus_trader.model.enums import AggregationSource
from nautilus_trader.trading.strategy import Strategy


logger = logging.getLogger(__name__)


SNIFFER_COLUMNS: tuple[str, ...] = (
    "stream",
    "price_type",
    "bar_type",
    "ts_event_ns",
    "ts_init_ns",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "ts_event",
    "ts_init",
)


def bar_to_row(stream: str, bar: Bar) -> dict:
    bt = bar.bar_type
    return {
        "stream": stream,
        "price_type": bt.spec.price_type.name,
        "bar_type": str(bt),
        "ts_event_ns": int(bar.ts_event),
        "ts_init_ns": int(bar.ts_init),
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
        "ts_event": pd.Timestamp(bar.ts_event, unit="ns", tz="UTC").isoformat(),
        "ts_init": pd.Timestamp(bar.ts_init, unit="ns", tz="UTC").isoformat(),
    }


def _write_rows(path: str | Path, rows: list[dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=list(SNIFFER_COLUMNS))
        w.writeheader()
        w.writerows(rows)


def dump_raw_engine(engine: BacktestEngine, csv_path: str | Path) -> int:
    """Snapshot ``engine.data`` to CSV before ``engine.run()``.

    Filters to ``Bar`` items with ``AggregationSource.EXTERNAL`` (the raw
    bars the engine will dispatch), writes the 12-column schema, and
    returns the row count.
    """
    rows = [
        bar_to_row("raw_engine", d)
        for d in engine.data
        if isinstance(d, Bar)
        and d.bar_type.aggregation_source == AggregationSource.EXTERNAL
    ]
    _write_rows(csv_path, rows)
    return len(rows)


class BarSnifferConfig(StrategyConfig, frozen=True):
    """Configuration for :class:`BarSniffer`.

    ``bar_types`` accepts BarType objects (notebook style) or strings; the
    sniffer converts strings via ``BarType.from_str`` in ``on_start``.
    Strings are preferred when this config is constructed across a process
    boundary (e.g. a ProcessPoolExecutor worker) where BarType pickling
    is more brittle than plain strings.
    """

    bar_types: tuple[str, ...]
    csv_engine_path: str
    csv_strategy_path: str


class BarSniffer(Strategy):
    """Records every ``on_bar``; buckets EXTERNAL vs INTERNAL; flushes two
    CSVs in ``on_stop``."""

    def __init__(self, config: BarSnifferConfig) -> None:
        super().__init__(config)
        self._engine_rows: list[dict] = []
        self._strategy_rows: list[dict] = []

    def on_start(self) -> None:
        bts = [BarType.from_str(bt) if isinstance(bt, str) else bt
               for bt in self.config.bar_types]
        for bt in sorted(bts, key=lambda b: b.aggregation_source.value):
            self.subscribe_bars(bt)

    def on_bar(self, bar: Bar) -> None:
        if bar.bar_type.aggregation_source == AggregationSource.EXTERNAL:
            self._engine_rows.append(bar_to_row("sniffer_engine", bar))
        else:
            self._strategy_rows.append(bar_to_row("sniffer_strategy", bar))

    def on_stop(self) -> None:
        try:
            _write_rows(self.config.csv_engine_path, self._engine_rows)
            _write_rows(self.config.csv_strategy_path, self._strategy_rows)
        except Exception:
            logger.exception("BarSniffer.on_stop: CSV write failed")
