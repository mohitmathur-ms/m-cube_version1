"""
Engine-pipeline sniffers for the aggregation_reports feature.

Three views of the same backtest data stream, captured at sequential
stages of NautilusTrader's bar dispatch:

  raw_engine_sniffer  : Bars sitting in ``engine.data`` BEFORE ``engine.run()``,
                        filtered to AggregationSource.EXTERNAL. Snapshotted via
                        :meth:`BarSniffer.prime_raw_engine` and written in
                        ``on_stop`` (so it carries the event columns too).
  sniffer_data_engine : Bars the ``DataEngine`` dispatches at runtime to
                        subscribed strategies (EXTERNAL). Captured in
                        :class:`BarSniffer.on_bar`, written in ``on_stop``.
  sniffer_strategy    : Bars after Nautilus's TimeBarAggregator produces
                        INTERNAL aggregations and they arrive at the
                        strategy. Same sniffer, different bucket, same write.

All three CSVs share a single schema (:data:`SNIFFER_COLUMNS`): the original
12 bar columns plus 9 event columns recording when a signal fired
(order submitted, with the ``core.signals`` reason text) and when the OMS
executed an order (order filled). Events whose timestamp aligns to a bar fill
that bar's event columns; unaligned events are appended as their own
``signal_fire`` / ``order_exec`` rows (see :func:`_apply_events`).

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
from nautilus_trader.model.events import OrderFilled
from nautilus_trader.trading.strategy import Strategy


logger = logging.getLogger(__name__)


# The original 12-column bar schema plus 9 event columns. The event columns are
# appended at the end so existing readers (which select columns by name) are
# unaffected. They are populated on the bar row whose ts_event matches a
# signal-fire / OMS-fill timestamp; events that don't align to a bar row in a
# given CSV are written as their own rows (stream="signal_fire"/"order_exec").
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
    # --- signal-fire (order-submitted) columns ---
    "signal_side",
    "signal_reason",
    "signal_strategy_id",
    "signal_fire_ts_ns",
    "signal_fire_ts",
    # --- OMS execution (order-filled) columns ---
    "order_exec_side",
    "order_exec_qty",
    "order_exec_price",
    "order_exec_strategy_id",
    "order_exec_ts_ns",
    "order_exec_ts",
)

# Columns introduced by the event-merge step; default to "" on every bar row.
_EVENT_COLUMNS: tuple[str, ...] = (
    "signal_side",
    "signal_reason",
    "signal_strategy_id",
    "signal_fire_ts_ns",
    "signal_fire_ts",
    "order_exec_side",
    "order_exec_qty",
    "order_exec_price",
    "order_exec_strategy_id",
    "order_exec_ts_ns",
    "order_exec_ts",
)


def _ts_iso(ts_ns: int) -> str:
    return pd.Timestamp(ts_ns, unit="ns", tz="UTC").isoformat()


def bar_to_row(stream: str, bar: Bar) -> dict:
    bt = bar.bar_type
    row = {
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
    for col in _EVENT_COLUMNS:
        row[col] = ""
    return row


def _write_rows(path: str | Path, rows: list[dict]) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with open(p, "w", newline="", encoding="utf-8") as fh:
        # extrasaction="ignore" is defensive; rows always carry the full schema.
        w = csv.DictWriter(
            fh, fieldnames=list(SNIFFER_COLUMNS), extrasaction="ignore"
        )
        w.writeheader()
        w.writerows(rows)


def _capture_external_rows(engine: BacktestEngine, stream: str) -> list[dict]:
    """Snapshot the EXTERNAL ``Bar`` items in ``engine.data`` (the raw bars the
    engine will dispatch) into the 21-column row schema."""
    return [
        bar_to_row(stream, d)
        for d in engine.data
        if isinstance(d, Bar)
        and d.bar_type.aggregation_source == AggregationSource.EXTERNAL
    ]


def dump_raw_engine(engine: BacktestEngine, csv_path: str | Path) -> int:
    """Snapshot ``engine.data`` to CSV before ``engine.run()``.

    Retained for external/notebook use. The backtest runner no longer calls
    this — :class:`BarSniffer` now owns the raw-engine snapshot so it can be
    written *after* the run with the signal/order event columns merged in
    (see :meth:`BarSniffer.prime_raw_engine`).
    """
    rows = _capture_external_rows(engine, "raw_engine")
    _write_rows(csv_path, rows)
    return len(rows)


def build_event_maps(cache) -> tuple[dict[int, list[dict]], dict[int, list[dict]]]:
    """Read order activity from the engine cache and bucket it by timestamp.

    Returns ``(signals_by_ts, fills_by_ts)`` where:

    * ``signals_by_ts[ts_ns]`` — one entry per order *submitted* at ``ts_ns``
      (``order.ts_init``, which equals the bar ``ts_event`` at signal time).
      Carries the order side and the ``tags`` reason string (the
      ``core.signals`` text for entries, or the exit reason for SL/TP/squareoff
      closes).
    * ``fills_by_ts[ts_ns]`` — one entry per ``OrderFilled`` event, keyed by the
      exact OMS fill timestamp (``event.ts_event``), with side / qty / price.

    All failures are swallowed and logged so a report-pipeline error never
    propagates into the backtest.
    """
    signals_by_ts: dict[int, list[dict]] = {}
    fills_by_ts: dict[int, list[dict]] = {}
    try:
        orders = cache.orders()
    except Exception:
        logger.exception("build_event_maps: cache.orders() failed")
        return signals_by_ts, fills_by_ts

    for order in orders:
        try:
            strategy_id = str(order.strategy_id)
        except Exception:
            strategy_id = ""

        try:
            ts_init = int(order.ts_init)
            tags = getattr(order, "tags", None)
            reason = "; ".join(tags) if tags else ""
            signals_by_ts.setdefault(ts_init, []).append({
                "side": order.side.name,
                "reason": reason,
                "strategy_id": strategy_id,
            })
        except Exception:
            logger.exception("build_event_maps: failed to read order submit")

        try:
            for ev in order.events:
                if not isinstance(ev, OrderFilled):
                    continue
                ts_fill = int(ev.ts_event)
                fills_by_ts.setdefault(ts_fill, []).append({
                    "side": ev.order_side.name,
                    "qty": float(ev.last_qty),
                    "price": float(ev.last_px),
                    "strategy_id": strategy_id,
                })
        except Exception:
            logger.exception("build_event_maps: failed to read order fills")

    return signals_by_ts, fills_by_ts


def _signal_event_row(ts_ns: int, sig: dict) -> dict:
    row = {c: "" for c in SNIFFER_COLUMNS}
    row["stream"] = "signal_fire"
    row["ts_event_ns"] = ts_ns
    row["ts_event"] = _ts_iso(ts_ns)
    row["signal_side"] = sig["side"]
    row["signal_reason"] = sig["reason"]
    row["signal_strategy_id"] = sig.get("strategy_id", "")
    row["signal_fire_ts_ns"] = ts_ns
    row["signal_fire_ts"] = _ts_iso(ts_ns)
    return row


def _fill_event_row(ts_ns: int, fill: dict) -> dict:
    row = {c: "" for c in SNIFFER_COLUMNS}
    row["stream"] = "order_exec"
    row["ts_event_ns"] = ts_ns
    row["ts_event"] = _ts_iso(ts_ns)
    row["order_exec_side"] = fill["side"]
    row["order_exec_qty"] = fill["qty"]
    row["order_exec_price"] = fill["price"]
    row["order_exec_strategy_id"] = fill.get("strategy_id", "")
    row["order_exec_ts_ns"] = ts_ns
    row["order_exec_ts"] = _ts_iso(ts_ns)
    return row


def _apply_events(
    rows: list[dict],
    signals_by_ts: dict[int, list[dict]],
    fills_by_ts: dict[int, list[dict]],
) -> list[dict]:
    """Merge signal-fire and OMS-fill events into a list of bar rows.

    Each cell holds a single value — events are never packed. The rule below is
    applied independently for signals and for fills:

    * If exactly **one** event of that type occurs at a timestamp that matches a
      bar row, its columns are filled inline on that bar row.
    * Otherwise — more than one event share the timestamp, *or* no bar row
      exists at it (e.g. an EXTERNAL fill on the aggregated ``sniffer_strategy``
      CSV) — each event is emitted as its own ``signal_fire`` / ``order_exec``
      row, single-valued and attributed via ``*_strategy_id``.

    The combined list is sorted by ``ts_event_ns`` so event rows interleave
    with the bars chronologically.
    """
    bar_ts = set()
    for row in rows:
        ts = row.get("ts_event_ns")
        if ts is None:
            continue
        ts = int(ts)
        bar_ts.add(ts)
        sigs = signals_by_ts.get(ts)
        if sigs and len(sigs) == 1:
            s = sigs[0]
            row["signal_side"] = s["side"]
            row["signal_reason"] = s["reason"]
            row["signal_strategy_id"] = s.get("strategy_id", "")
            row["signal_fire_ts_ns"] = ts
            row["signal_fire_ts"] = _ts_iso(ts)
        fills = fills_by_ts.get(ts)
        if fills and len(fills) == 1:
            f = fills[0]
            row["order_exec_side"] = f["side"]
            row["order_exec_qty"] = f["qty"]
            row["order_exec_price"] = f["price"]
            row["order_exec_strategy_id"] = f.get("strategy_id", "")
            row["order_exec_ts_ns"] = ts
            row["order_exec_ts"] = _ts_iso(ts)

    # Emit one row per event for everything not handled inline above:
    # unaligned timestamps, or timestamps with more than one event of that type.
    extra: list[dict] = []
    for ts, sigs in signals_by_ts.items():
        if ts in bar_ts and len(sigs) == 1:
            continue
        for s in sigs:
            extra.append(_signal_event_row(ts, s))
    for ts, fills in fills_by_ts.items():
        if ts in bar_ts and len(fills) == 1:
            continue
        for f in fills:
            extra.append(_fill_event_row(ts, f))

    if not extra:
        return rows
    combined = rows + extra
    combined.sort(key=lambda r: int(r.get("ts_event_ns") or 0))
    return combined


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
    csv_raw_engine_path: str = ""


class BarSniffer(Strategy):
    """Records every ``on_bar``; buckets EXTERNAL vs INTERNAL. In ``on_stop`` it
    merges signal-fire / OMS-fill events from the cache into all three row sets
    (raw_engine, data_engine, strategy) and flushes the three CSVs."""

    def __init__(self, config: BarSnifferConfig) -> None:
        super().__init__(config)
        self._raw_rows: list[dict] = []
        self._engine_rows: list[dict] = []
        self._strategy_rows: list[dict] = []

    def prime_raw_engine(self, engine: BacktestEngine) -> None:
        """Snapshot the EXTERNAL bars in ``engine.data`` *before* the run.

        The rows are held in memory and written in :meth:`on_stop` so they can
        carry the signal/order event columns. Swallows its own errors.
        """
        try:
            self._raw_rows = _capture_external_rows(engine, "raw_engine")
        except Exception:
            logger.exception("BarSniffer.prime_raw_engine: capture failed")

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
            signals_by_ts, fills_by_ts = build_event_maps(self.cache)
            raw_rows = _apply_events(self._raw_rows, signals_by_ts, fills_by_ts)
            engine_rows = _apply_events(
                self._engine_rows, signals_by_ts, fills_by_ts
            )
            strategy_rows = _apply_events(
                self._strategy_rows, signals_by_ts, fills_by_ts
            )
            if self.config.csv_raw_engine_path:
                _write_rows(self.config.csv_raw_engine_path, raw_rows)
            _write_rows(self.config.csv_engine_path, engine_rows)
            _write_rows(self.config.csv_strategy_path, strategy_rows)
        except Exception:
            logger.exception("BarSniffer.on_stop: CSV write failed")


def attach_bar_sniffer(
    engine: BacktestEngine,
    bar_types: tuple[str, ...] | list[str],
    paths: dict,
) -> BarSniffer:
    """Build a :class:`BarSniffer` for all three sniffer CSVs, prime its
    raw-engine snapshot from ``engine.data`` (pre-run), and register it.

    ``paths`` is the dict returned by
    ``aggregation_report.derive_sniffer_paths_for_bar_type`` (keys
    ``raw_engine_sniffer`` / ``sniffer_data_engine`` / ``sniffer_strategy``).
    Returns the sniffer instance; callers may ignore it (``on_stop`` writes the
    CSVs).
    """
    sniffer = BarSniffer(BarSnifferConfig(
        bar_types=tuple(bar_types),
        csv_engine_path=str(paths["sniffer_data_engine"]),
        csv_strategy_path=str(paths["sniffer_strategy"]),
        csv_raw_engine_path=str(paths["raw_engine_sniffer"]),
    ))
    sniffer.prime_raw_engine(engine)
    engine.add_strategy(sniffer)
    return sniffer
