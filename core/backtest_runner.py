"""
Backtest runner: configure and execute backtests using NautilusTrader's BacktestEngine.

Loads data from the ParquetDataCatalog, runs a selected strategy, and returns results.
"""

from __future__ import annotations

import dataclasses
from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.backtest.config import BacktestVenueConfig
from nautilus_trader.backtest.config import BacktestDataConfig
from nautilus_trader.backtest.config import BacktestRunConfig
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model import TraderId
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

import numpy as np

from core.strategies import STRATEGY_REGISTRY
from core.models import (
    PortfolioConfig,
    StrategySlotConfig,
    effective_portfolio_squareoff,
    effective_slot_qty,
)
from core.managed_strategy import (
    ManagedExitStrategy,
    advance_trailing_target,
    config_from_exit,
)
from core.fx_rates import FxRateResolver, parse_money_string
from core.venue_config import load_adapter_config_for_bar_type

import contextlib
import functools
import time as _time_mod


@contextlib.contextmanager
def _phase(label: str, bag: dict | None):
    """Record phase wall-time into ``bag[label]`` when profiling is active.

    No-op when ``bag is None``; callers pass ``None`` in the hot path so
    non-profiling runs pay only the cost of a context-manager enter/exit.
    """
    if bag is None:
        yield
        return
    t0 = _time_mod.perf_counter()
    try:
        yield
    finally:
        bag[label] = bag.get(label, 0.0) + (_time_mod.perf_counter() - t0)


def _config_supports_extra_bar_types(config_class) -> bool:
    """Cheap memoized check for whether a strategy config accepts extra_bar_types.

    Walks the MRO once per class, stashes the result on the class itself so
    repeated slot runs with the same config class skip the MRO walk entirely.
    """
    cached = config_class.__dict__.get("_supports_extra_bar_types")
    if cached is not None:
        return cached
    for cls in reversed(config_class.__mro__):
        if "extra_bar_types" in getattr(cls, "__annotations__", {}):
            config_class._supports_extra_bar_types = True
            return True
    config_class._supports_extra_bar_types = False
    return False


def _config_supports_aggregate_to(config_class) -> bool:
    """Memoized check for whether a strategy config accepts aggregate_to_bar_type."""
    cached = config_class.__dict__.get("_supports_aggregate_to")
    if cached is not None:
        return cached
    for cls in reversed(config_class.__mro__):
        if "aggregate_to_bar_type" in getattr(cls, "__annotations__", {}):
            config_class._supports_aggregate_to = True
            return True
    config_class._supports_aggregate_to = False
    return False


def _aggregate_target_for_slot(slot, base_bar_type) -> str:
    """Plain EXTERNAL bar type the leg's first strategy timeframe aggregates to,
    or "" when none is selected or it equals the base. Mirrors the logic in
    ``config_from_exit`` so the raw (non-ManagedExit) strategy path — used when a
    leg has no SL/TP/squareoff/RBO/run_on_days — aggregates identically to the
    managed path instead of silently dropping the aggregation."""
    from core.aggregator import external_from_composite
    sbts = [str(s) for s in (getattr(slot, "strategy_bar_types", None) or []) if s]
    if not sbts:
        return ""
    ext = external_from_composite(sbts[0])
    if not ext:
        return ""
    try:
        if BarType.from_str(ext) != base_bar_type:
            return ext
    except Exception:  # noqa: BLE001 — malformed → no aggregation
        return ""
    return ""


def _group_slots(
    enabled_slots: list,
    capitals_by_slot_id: dict[str, float],
    default_start_date: str | None,
    default_end_date: str | None,
    custom_strategies_dir: str | None,
) -> list[list[tuple]]:
    """Group slots that share the same (bar_type, start, end, custom_strategies_dir).

    Each group is a list of (slot, capital) tuples. Groups of size 1 are still
    emitted — callers decide whether to treat them as shared-engine or fall
    back to the per-slot engine path.

    Grouping key intentionally excludes strategy_name / strategy_params / trade_size
    — those legitimately differ across the strategies that should share an engine.
    """
    groups: dict[tuple, list[tuple]] = {}
    for slot in enabled_slots:
        start = slot.start_date or default_start_date
        end = slot.end_date or default_end_date
        key = (slot.bar_type_str, start, end, custom_strategies_dir)
        groups.setdefault(key, []).append((slot, capitals_by_slot_id[slot.slot_id]))
    return list(groups.values())


def _pair_bid_ask_bar_type(bt_str: str) -> list[str]:
    """Return additional bar type strings needed for realistic fills.

    Nautilus's matching engine needs both quote sides to fill FX market
    orders. A strategy subscribed only to BID sees no fills unless ASK is
    also loaded (and vice versa). MID slots need both ASK and BID so the
    engine can fill at real spread prices instead of the midpoint.
    LAST bar types don't need a pair.
    """
    if "-BID-" in bt_str:
        return [bt_str.replace("-BID-", "-ASK-", 1)]
    if "-ASK-" in bt_str:
        return [bt_str.replace("-ASK-", "-BID-", 1)]
    if "-MID-" in bt_str:
        return [
            bt_str.replace("-MID-", "-ASK-", 1),
            bt_str.replace("-MID-", "-BID-", 1),
        ]
    return []


_DAY_NAME_TO_WEEKDAY = {
    "MON": 0, "MONDAY": 0,
    "TUE": 1, "TUESDAY": 1,
    "WED": 2, "WEDNESDAY": 2,
    "THU": 3, "THURSDAY": 3,
    "FRI": 4, "FRIDAY": 4,
    "SAT": 5, "SATURDAY": 5,
    "SUN": 6, "SUNDAY": 6,
}


def _allowed_weekdays(run_on_days) -> set | None:
    """Resolve a portfolio's run_on_days into a set of weekday integers.

    Returns None when no filter should apply (input is None — meaning
    "all 7 days are fine"). Returns a set of int weekdays (0=Mon..6=Sun)
    when the filter is active. Returns an empty set when the input is
    a non-None list whose entries don't match any known day name —
    callers should treat that as "no days are allowed" and short-circuit.
    """
    if run_on_days is None:
        return None
    if not isinstance(run_on_days, (list, tuple, set)):
        return None
    allowed: set = set()
    for day in run_on_days:
        if not isinstance(day, str):
            continue
        wd = _DAY_NAME_TO_WEEKDAY.get(day.strip().upper())
        if wd is not None:
            allowed.add(wd)
    return allowed


# 1970-01-01 (UNIX epoch) was a Thursday — Python weekday() = 3.
_NANOS_PER_DAY = 86_400_000_000_000
_EPOCH_WEEKDAY = 3


def _filter_bars_by_weekday(bars: list, allowed_weekdays: set | None) -> tuple[list, int]:
    """Drop bars whose UTC weekday isn't in allowed_weekdays.

    Returns (kept_bars, dropped_count). When allowed_weekdays is None,
    returns the input list unchanged with dropped=0. Uses an integer
    modulo on ts_event nanoseconds to avoid the per-bar pandas-timestamp
    cost — for a year of 1-min FX bars that's the difference between a
    20 ms filter and a 2 second filter.
    """
    if allowed_weekdays is None:
        return bars, 0
    if not allowed_weekdays:
        return [], len(bars)
    if not bars:
        return bars, 0

    kept = []
    for bar in bars:
        days = bar.ts_event // _NANOS_PER_DAY
        weekday = (_EPOCH_WEEKDAY + days) % 7
        if weekday in allowed_weekdays:
            kept.append(bar)
    return kept, len(bars) - len(kept)


def _filter_bars_after_ns(bars: list, cutoff_ns: int) -> tuple[list, int]:
    """Drop bars with ``ts_event < cutoff_ns`` — keep only bars at/after it.

    Used by the portfolio ReExecute replay (spec §1.2 / §2.4): a replay
    segment re-runs every slot from the clip timestamp onward, so each slot
    starts flat at ``cutoff_ns``. ``cutoff_ns <= 0`` is a no-op (the normal
    full-range run). Returns ``(kept_bars, dropped_count)``.
    """
    if cutoff_ns <= 0 or not bars:
        return bars, 0
    kept = [b for b in bars if b.ts_event >= cutoff_ns]
    return kept, len(bars) - len(kept)


_NANOS_PER_MINUTE = 60_000_000_000


def _hhmm_to_minute(s: str | None) -> int | None:
    """Parse a 'HH:MM' or 'HH:MM:SS' string into a minute-of-day integer.

    Returns None for None/empty/malformed input. Doesn't raise — bad input
    just disables that endpoint of the filter. Range is 0..1439 inclusive.
    """
    if not s or not isinstance(s, str):
        return None
    parts = s.strip().split(":")
    if len(parts) < 2:
        return None
    try:
        h = int(parts[0])
        m = int(parts[1])
    except ValueError:
        return None
    if not (0 <= h <= 23) or not (0 <= m <= 59):
        return None
    return h * 60 + m


def _is_intraday_bar_type(bt_str: str) -> bool:
    """Return True if the bar type is intraday granularity (minute / hour / second).

    Bar type format: ``INSTRUMENT.VENUE-N-AGGREGATION-PRICE-SOURCE``
    e.g. ``BTCUSD.BINANCE-1-DAY-LAST-EXTERNAL`` -> DAY (not intraday)
         ``EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL`` -> MINUTE (intraday)

    The intra-day entry window (entry_start_time / entry_end_time) only
    makes sense for intraday bars — daily/weekly/monthly bars have a single
    ts_event per period (typically 00:00 UTC of the period start) which would
    be unconditionally inside or outside any HH:MM window. Applying the filter
    to daily bars on an equity-hours window (e.g. 09:30..16:15) drops every
    bar.
    """
    if not bt_str:
        return False
    upper = bt_str.upper()
    return ("-SECOND-" in upper) or ("-MINUTE-" in upper) or ("-HOUR-" in upper)


def _filter_bars_by_time_of_day(
    bars: list,
    start_hhmm: str | None,
    end_hhmm: str | None,
) -> tuple[list, int]:
    """Drop bars whose UTC time-of-day falls outside [start_hhmm, end_hhmm].

    Both endpoints are inclusive. Either may be None — in which case that
    side of the window is unbounded (start=00:00 or end=23:59 effectively).
    When both are None, returns input unchanged.

    Window is in UTC. To use a non-UTC window, the caller would convert
    bar timestamps first; we don't pull pandas in here for the same
    perf reason as ``_filter_bars_by_weekday``.

    Returns (kept_bars, dropped_count).
    """
    start_min = _hhmm_to_minute(start_hhmm)
    end_min = _hhmm_to_minute(end_hhmm)
    if start_min is None and end_min is None:
        return bars, 0
    if not bars:
        return bars, 0

    # Normalize unbounded sides to full-day extremes
    lo = start_min if start_min is not None else 0
    hi = end_min if end_min is not None else (24 * 60 - 1)

    if lo > hi:
        # Inverted window (e.g. start=22:00, end=02:00) — treat as wrap-around
        # i.e. keep bars in [lo, 24*60) ∪ [0, hi].
        kept = []
        for bar in bars:
            intra = (bar.ts_event % _NANOS_PER_DAY) // _NANOS_PER_MINUTE
            if intra >= lo or intra <= hi:
                kept.append(bar)
        return kept, len(bars) - len(kept)

    kept = []
    for bar in bars:
        intra = (bar.ts_event % _NANOS_PER_DAY) // _NANOS_PER_MINUTE
        if lo <= intra <= hi:
            kept.append(bar)
    return kept, len(bars) - len(kept)


# ─────────────────────────────────────────────────────────────────────────────
# Path B (BacktestNode) helpers — opt-in via _USE_BACKTEST_NODE=1.
# See nautilus_path_a_to_path_b_migration.html for the full design rationale.
# ─────────────────────────────────────────────────────────────────────────────

def _path_b_active() -> bool:
    """True when the env flag opting into the BacktestNode pipeline is set."""
    import os
    return os.environ.get("_USE_BACKTEST_NODE") == "1"


def _sec_to_hms(sec: int) -> str:
    """Seconds-of-day → HH:MM:SS. Inverse of _hms_to_sec."""
    sec = max(0, min(86399, int(sec)))
    return f"{sec // 3600:02d}:{(sec // 60) % 60:02d}:{sec % 60:02d}"


def _chunk_data_configs_for_path_b(
    catalog_path: str,
    instrument_id_str: str,
    bar_type_strs: list[str],
    start_date: str,
    end_date: str,
    entry_start_time: str | None,
    entry_end_time: str | None,
    run_on_days: list | None,
    rbo_settings: "_RBOSettings | None" = None,
) -> list[BacktestDataConfig]:
    """One BacktestDataConfig per allowed day, bounded by the entry window.

    The Nautilus high-level API takes ``BacktestRunConfig.data`` as a list of
    ``BacktestDataConfig`` entries, each with its own ``start_time``/``end_time``.
    By emitting one entry per allowed (day, bar_type) we can express both
    ``run_on_days`` (skip excluded weekdays) and the recurring intraday
    ``entry_start_time``/``entry_end_time`` window — neither of which a single
    contiguous data config can represent.

    All days are walked in UTC. Excluded weekdays are dropped. For each
    included day, ``start_time`` becomes ``YYYY-MM-DDTHH:MM:SS+00:00`` using
    the entry window endpoints (defaulting to 00:00:00 .. 23:59:59.999999
    when one side is unbounded).

    When ``rbo_settings`` is provided, the per-day window is widened to the
    union of the existing entry window and ``[monitoring_start, entry_end +
    buffer]`` so the in-strategy RBO state machine sees the bars it needs to
    build the range and detect breakouts.

    Days with no catalog data are silently skipped by Nautilus — no need to
    pre-filter. ``ValueError`` is raised only if the filter combination
    yields zero configs (e.g. entry window with start > end).
    """
    allowed_weekdays = _allowed_weekdays(run_on_days)
    if allowed_weekdays is not None and not allowed_weekdays:
        raise ValueError(
            "Chunked Path B yielded zero data configs — run_on_days excludes "
            "every weekday."
        )

    win_start = entry_start_time or "00:00:00"
    win_end = entry_end_time or "23:59:59.999999"

    if rbo_settings is not None:
        # Widen to cover the RBO load needs: monitoring window at the start,
        # entry_end + buffer at the tail. Take the union with whatever
        # entry_window the user already set (which we only narrow further
        # never expand). Times are seconds-of-day; convert and string-compare.
        cur_start_sec = _hms_to_sec(win_start)
        cur_end_sec = _hms_to_sec(win_end.split(".")[0])  # strip fractional sec
        new_start_sec = min(cur_start_sec, rbo_settings.monitoring_start_sec)
        new_end_sec = max(
            cur_end_sec,
            rbo_settings.entry_end_sec + rbo_settings.range_buffer_sec,
        )
        win_start = _sec_to_hms(new_start_sec)
        win_end = _sec_to_hms(new_end_sec)

    start = pd.Timestamp(start_date, tz="UTC").normalize()
    end = pd.Timestamp(end_date, tz="UTC").normalize()

    configs: list[BacktestDataConfig] = []
    cur = start
    one_day = pd.Timedelta(days=1)
    while cur <= end:
        if allowed_weekdays is None or cur.weekday() in allowed_weekdays:
            day_str = cur.strftime("%Y-%m-%d")
            configs.append(BacktestDataConfig(
                catalog_path=catalog_path,
                data_cls="nautilus_trader.model.data:Bar",
                instrument_id=instrument_id_str,
                bar_types=bar_type_strs,
                start_time=f"{day_str}T{win_start}+00:00",
                end_time=f"{day_str}T{win_end}+00:00",
            ))
        cur = cur + one_day

    if not configs:
        raise ValueError(
            "Chunked Path B yielded zero data configs — check start/end dates."
        )
    return configs


def _build_run_config(
    catalog_path: str,
    instrument_id,
    bar_type_strs: list[str],
    venue,
    starting_capital: float,
    start_date: str | None,
    end_date: str | None,
    trader_id: str = "BACKTESTER-001",
    chunk_size: int | None = None,
    oms_type: str = "NETTING",
    entry_start_time: str | None = None,
    entry_end_time: str | None = None,
    run_on_days: list | None = None,
    rbo_settings: "_RBOSettings | None" = None,
) -> BacktestRunConfig:
    """Build the three Nautilus config dataclasses and bundle them.

    Single source of the Path B venue/data/engine wiring. Used by all three
    node-based variants (run_backtest_node, _run_single_slot_node,
    _run_slot_group_node) so the configs stay consistent across sites.

    When ``run_on_days`` is set, OR an entry window is set on an intraday bar
    type, this function emits one ``BacktestDataConfig`` per allowed day via
    ``_chunk_data_configs_for_path_b`` to honour the filter. Otherwise it
    emits a single contiguous data config for bit-exact parity with Path A.

    Notes on the Path A → Path B mapping:
      - ``starting_balances`` is ``list[str]`` not ``list[Money]`` (configs are
        msgspec-serialisable).
      - ``base_currency`` is a string ``"USD"`` not the Currency object.
      - ``default_leverage`` is a float ``1.0`` not ``Decimal(1)``.
      - The instrument is loaded from the catalog automatically — no explicit
        ``add_instrument`` call required.
      - ``chunk_size=None`` keeps Path B in load-everything-at-once mode for
        bit-exact parity with Path A. Pass an int (e.g. 100_000) once you've
        verified parity to opt into row-chunked streaming.
    """
    venue_cfg = BacktestVenueConfig(
        name=str(venue),
        oms_type=oms_type,
        account_type="MARGIN",
        starting_balances=[f"{starting_capital} USD"],
        base_currency="USD",
        default_leverage=1.0,
    )

    has_run_on_days = run_on_days is not None
    has_entry_window = bool(entry_start_time) or bool(entry_end_time)
    primary_bt = bar_type_strs[0] if bar_type_strs else None
    # Entry window is a no-op for non-intraday bars (ts_event is at midnight),
    # so don't bother chunking by it in that case. run_on_days is still
    # load-bearing on daily bars and forces chunking regardless.
    entry_window_effective = has_entry_window and (
        primary_bt is None or _is_intraday_bar_type(primary_bt)
    )
    needs_chunking = has_run_on_days or entry_window_effective

    if needs_chunking:
        if not start_date or not end_date:
            raise ValueError(
                "Path B chunking requires concrete start_date and end_date "
                "to enumerate allowed days."
            )
        data_cfgs = _chunk_data_configs_for_path_b(
            catalog_path=catalog_path,
            instrument_id_str=str(instrument_id),
            bar_type_strs=bar_type_strs,
            start_date=start_date,
            end_date=end_date,
            entry_start_time=entry_start_time if entry_window_effective else None,
            entry_end_time=entry_end_time if entry_window_effective else None,
            run_on_days=run_on_days,
            rbo_settings=rbo_settings,
        )
    else:
        # Construct full-day ISO timestamps so end_date is inclusive of the
        # final calendar day, matching Path A (_cached_catalog_bars: +1d -1ns)
        # and the chunked branch above (win_end = "23:59:59.999999"). Bare
        # date strings here would be interpreted by Nautilus as midnight
        # start of end_date, silently dropping all bars on the final day —
        # a Path A vs B parity bug.
        data_cfgs = [BacktestDataConfig(
            catalog_path=catalog_path,
            # Nautilus resolves data_cls via path.rsplit(":", 1) — must use
            # "module.path:ClassName" format. A dot before the class name
            # raises ValueError("not enough values to unpack") in node.build().
            data_cls="nautilus_trader.model.data:Bar",
            instrument_id=str(instrument_id),
            bar_types=bar_type_strs,
            start_time=f"{start_date}T00:00:00+00:00" if start_date else None,
            end_time=f"{end_date}T23:59:59.999999+00:00" if end_date else None,
        )]

    from nautilus_trader.config import RiskEngineConfig
    engine_cfg = BacktestEngineConfig(
        trader_id=TraderId(trader_id),
        logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True),
        run_analysis=False,
    )
    return BacktestRunConfig(
        venues=[venue_cfg],
        data=data_cfgs,
        engine=engine_cfg,
        chunk_size=chunk_size,
    )


# ─────────────────────────────────────────────────────────────────────────────
# RBO (Range Breakout) — portfolio-level breakout-gated entry.
# Spec: 5. Logics/rbo_logics.html. Wired through ManagedExitStrategy: each
# slot's strategy maintains its own per-day state machine over its own bar
# type (the spec's "monitoring = Underlying"). Path-A and Path-B both run
# unchanged — RBO is applied at strategy-build time, not engine-build time.
# ─────────────────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _RBOSettings:
    """Validated, time-parsed RBO configuration ready for ManagedExitStrategy.

    All HH:MM:SS portfolio fields are pre-converted to seconds-of-day so the
    strategy's hot path on every bar is integer comparisons only — no string
    parsing per tick.
    """
    monitoring_start_sec: int
    monitoring_end_sec: int
    entry_start_sec: int
    entry_end_sec: int
    range_buffer_sec: int  # rbo_range_buffer minutes → seconds
    entry_at: str  # "Any" / "RangeHigh" / "RangeLow" — already-downgraded
    cancel_other_side: bool


def _hms_to_sec(hms: str) -> int:
    """HH:MM[:SS] → seconds-of-day. ValueError on malformed input — fail loud."""
    parts = hms.split(":")
    h = int(parts[0])
    m = int(parts[1]) if len(parts) > 1 else 0
    s = int(parts[2]) if len(parts) > 2 else 0
    return h * 3600 + m * 60 + s


def add_one_hour(t: str | None) -> str | None:
    """Shift an "HH:MM" / "HH:MM:SS" local time forward by one hour.

    Implements the spec's ``add_one_hour()`` for the Winter Time Adjustment
    (execution_logic_target.html §9). ``None`` / empty / malformed input is
    returned unchanged so callers can apply it unconditionally. A shift that
    would cross midnight is clamped to end-of-day (23:59[:59]) rather than
    wrapping — session times never legitimately wrap, and clamping preserves
    "late square-off" intent instead of silently moving it to 00:xx.
    """
    if not t:
        return t
    parts = str(t).split(":")
    try:
        h = int(parts[0])
        m = int(parts[1])
        s = int(parts[2]) if len(parts) > 2 else None
    except (ValueError, IndexError):
        return t
    h += 1
    if h > 23:
        h, m = 23, 59
        if s is not None:
            s = 59
    if s is not None:
        return f"{h:02d}:{m:02d}:{s:02d}"
    return f"{h:02d}:{m:02d}"


def _apply_winter_time(portfolio) -> bool:
    """Apply the Winter Time Adjustment to all configured local times in place.

    Spec execution_logic_target.html §9: when winter time is in effect for a
    US-listed instrument, the engine shifts the raw configured times by +1 hour
    before applying them. We mutate the (per-run, freshly-loaded) portfolio's
    intraday time fields — entry window, portfolio/MIS square-off, RBO windows,
    and every slot/leg square-off override — so all downstream paths (Path A
    per-slot, grouped, Path B) see the shifted values uniformly. Dates are not
    touched. Returns True when a shift was applied (for logging). No-op unless
    ``portfolio.winter_time_adjust`` is set.
    """
    if not getattr(portfolio, "winter_time_adjust", False):
        return False
    portfolio.entry_start_time = add_one_hour(portfolio.entry_start_time)
    portfolio.entry_end_time = add_one_hour(portfolio.entry_end_time)
    portfolio.squareoff_time = add_one_hour(portfolio.squareoff_time)
    portfolio.mis_squareoff_time = add_one_hour(portfolio.mis_squareoff_time)
    portfolio.range_monitoring_start = add_one_hour(portfolio.range_monitoring_start)
    portfolio.range_monitoring_end = add_one_hour(portfolio.range_monitoring_end)
    portfolio.rbo_entry_start = add_one_hour(portfolio.rbo_entry_start)
    portfolio.rbo_entry_end = add_one_hour(portfolio.rbo_entry_end)
    for slot in portfolio.slots:
        if getattr(slot, "squareoff_time", None):
            slot.squareoff_time = add_one_hour(slot.squareoff_time)
        ec = getattr(slot, "exit_config", None)
        if ec is not None and getattr(ec, "squareoff_time", None):
            ec.squareoff_time = add_one_hour(ec.squareoff_time)
    return True


def _resolve_rbo(portfolio) -> tuple[_RBOSettings | None, str | None]:
    """Validate portfolio.rbo_* fields per rbo_logics.html.

    Returns (settings, message):
      - (None, None)           → RBO disabled, no error.
      - (None, error_message)  → RBO requested but invalid; caller falls back
                                 to standard time-based entry per spec.
      - (settings, None)       → Valid; ready to wire into ManagedExitConfig.
      - (settings, warning)    → Valid but with a downgrade — e.g. options-only
                                 entry_at value silently coerced to "Any" for
                                 FX/crypto (spec assumes options).
    """
    if not getattr(portfolio, "rbo_enabled", False):
        return None, None

    if not portfolio.range_monitoring_start or not portfolio.range_monitoring_end:
        return None, "RBO Monitoring times missing"

    if portfolio.rbo_monitoring != "Underlying":
        return None, "RBO Monitoring must be set to 'Underlying'"

    entry_at = portfolio.rbo_entry_at or "Any"
    warning: str | None = None
    if entry_at in ("C_OnHigh_P_OnLow", "P_OnHigh_C_OnLow"):
        warning = (
            f"rbo_entry_at='{entry_at}' is options-only; downgraded to 'Any' "
            f"for FX/crypto. (Spec rbo_logics.html P7 — Call/Put routing has "
            f"no analogue without options legs.)"
        )
        entry_at = "Any"
    elif entry_at not in ("Any", "RangeHigh", "RangeLow"):
        return None, f"Invalid rbo_entry_at: '{entry_at}'"

    # Per spec P4: rbo_entry_start defaults to range_monitoring_end (no quiet gap).
    entry_start = portfolio.rbo_entry_start or portfolio.range_monitoring_end
    entry_end = portfolio.rbo_entry_end or "16:15:00"

    return _RBOSettings(
        monitoring_start_sec=_hms_to_sec(portfolio.range_monitoring_start),
        monitoring_end_sec=_hms_to_sec(portfolio.range_monitoring_end),
        entry_start_sec=_hms_to_sec(entry_start),
        entry_end_sec=_hms_to_sec(entry_end),
        range_buffer_sec=int(portfolio.rbo_range_buffer or 0) * 60,
        entry_at=entry_at,
        cancel_other_side=bool(portfolio.rbo_cancel_other_side),
    ), warning


# ─────────────────────────────────────────────────────────────────────────────
# Other Settings tab. Spec: 5. Logics/Other_Settings_Logic.html.
# Wired through ManagedExitStrategy at slot level (the spec is portfolio-level
# but we adapt to our slot-independent architecture). delay_between_legs_sec
# gates re-entries; on_sl_action_on / on_target_action_on filter the configured
# exit action based on whether the SL/TP was the fixed level or a trailing one.
# ─────────────────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _OtherSettings:
    """Validated Other Settings for ManagedExitStrategy.

    All fields default-equivalent to off, so a slot wired with a freshly-built
    instance behaves exactly like the legacy pre-Other-Settings code path.
    """
    delay_between_legs_sec: int = 0
    on_sl_action_on: str = "OnSL_N_Trailing_Both"
    on_target_action_on: str = "OnTarget_N_Trailing_Both"


_VALID_ON_SL_ACTION_ON = ("OnSL_N_Trailing_Both", "OnSL_Only", "OnSL_Trailing_Only")
_VALID_ON_TARGET_ACTION_ON = ("OnTarget_N_Trailing_Both", "OnTarget_Only", "OnTarget_Trailing_Only")


def _resolve_other_settings(portfolio) -> tuple[_OtherSettings, list[str]]:
    """Validate portfolio Other Settings fields and produce a ready-to-use struct.

    Returns (settings, warnings). ``warnings`` is empty unless the user has set
    options-only fields (Straddle Width Multiplier) or invalid enum values; we
    surface those via stdout in run_portfolio_backtest so they're visible in
    the worker log.
    """
    warnings: list[str] = []

    delay = int(getattr(portfolio, "delay_between_legs_sec", 0) or 0)
    if delay < 0:
        warnings.append(f"delay_between_legs_sec={delay} clamped to 0 (negative)")
        delay = 0

    on_sl = getattr(portfolio, "on_sl_action_on", "OnSL_N_Trailing_Both") or "OnSL_N_Trailing_Both"
    if on_sl not in _VALID_ON_SL_ACTION_ON:
        warnings.append(
            f"on_sl_action_on={on_sl!r} not in {_VALID_ON_SL_ACTION_ON} — "
            f"falling back to default 'OnSL_N_Trailing_Both'."
        )
        on_sl = "OnSL_N_Trailing_Both"

    on_tgt = getattr(portfolio, "on_target_action_on", "OnTarget_N_Trailing_Both") or "OnTarget_N_Trailing_Both"
    if on_tgt not in _VALID_ON_TARGET_ACTION_ON:
        warnings.append(
            f"on_target_action_on={on_tgt!r} not in {_VALID_ON_TARGET_ACTION_ON} — "
            f"falling back to default 'OnTarget_N_Trailing_Both'."
        )
        on_tgt = "OnTarget_N_Trailing_Both"

    # Options-only fields — surface a warning if user set non-default values.
    swm = float(getattr(portfolio, "straddle_width_multiplier", 0.0) or 0.0)
    if swm != 0.0:
        warnings.append(
            f"straddle_width_multiplier={swm} is options-only (CE/PE strike "
            f"override); ignored for FX/crypto. See Other_Settings_Logic.html."
        )

    if getattr(portfolio, "trail_wait_trade", False):
        warnings.append(
            "trail_wait_trade=True has no documented spec; the field is stored "
            "but not yet wired. No effect on this run."
        )

    return _OtherSettings(
        delay_between_legs_sec=delay,
        on_sl_action_on=on_sl,
        on_target_action_on=on_tgt,
    ), warnings


# ─────────────────────────────────────────────────────────────────────────────
# Portfolio-level Stoploss / Target. Spec: 5. Logics/portfolio_sl_tgt.html.
# Applied post-hoc in _merge_portfolio_results via _apply_portfolio_clip.
# Move SL to Cost is applied per-slot through ManagedExitStrategy (separate
# wiring) — captured in _MoveSLConfig below for that path.
# ─────────────────────────────────────────────────────────────────────────────


@dataclasses.dataclass(frozen=True)
class _PfStoplossSettings:
    """Validated portfolio-level Stoploss config ready for _apply_portfolio_clip.

    All values pre-validated and non-negative. Default-equivalent to "off"
    when ``enabled=False`` — caller may treat this as None semantically.
    """
    enabled: bool = False
    value: float = 0.0
    action: str = "SqOff"  # SqOff | ReExecute | cross-portfolio variants
    delay_sec: int = 0
    reexecute_count: int = 0  # 0 = unlimited per spec §1.7
    sqoff_only_loss_legs: bool = False
    sqoff_only_profit_legs: bool = False
    trail_enabled: bool = False
    trail_every: float = 0.0
    trail_by: float = 0.0
    target_portfolio: str = ""  # spec §2.1(m) — only used for cross-portfolio actions
    # SL type (spec §2.1): "Combined Loss" (PnL-based) | "Underlying Movement"
    # (price-cross) | "Loss and Underlying Range" (hybrid). Underlying-based
    # types default the underlying to the portfolio's primary instrument.
    sl_type: str = "Combined Loss"
    underlying_below: float = 0.0
    underlying_above: float = 0.0


@dataclasses.dataclass(frozen=True)
class _PfTargetSettings:
    """Validated portfolio-level Target config."""
    enabled: bool = False
    # Target type (spec §5.1): "Combined Profit" (PnL-based) or
    # "Underlying Movement" (primary-instrument price crosses ``value``).
    tgt_type: str = "Combined Profit"
    value: float = 0.0
    action: str = "SqOff"
    delay_sec: int = 0
    reexecute_count: int = 0
    trail_enabled: bool = False
    trail_lock_min_profit: float = 0.0
    trail_when_profit_reach: float = 0.0
    trail_every: float = 0.0
    trail_by: float = 0.0
    target_portfolio: str = ""  # spec §2.4(d)


@dataclasses.dataclass(frozen=True)
class _MoveSLConfig:
    """Per-slot Move SL to Cost config (spec §3, adapted to slot-independent
    architecture: each slot raises its own SL to entry once safety_sec elapsed
    and position is in profit). Threaded through config_from_exit into
    ManagedExitConfig fields move_sl_*.

    ``no_reexec_sl_cost`` (ReExecute_Logics.html P1) is bundled here because
    it modifies Move-SL behaviour: when True, the re_execute action is
    suppressed if the SL that fired had been raised to entry by Move SL to
    Cost. It's meaningful only in conjunction with Move SL to Cost — when
    Move SL is disabled the flag is benignly always-no-op (the strategy's
    _move_sl_fired_this_position flag never flips True).
    """
    enabled: bool = False
    safety_sec: int = 0
    action: str = "Move Only for Profitable Legs"
    trail_after: bool = False
    no_buy_legs: bool = False
    no_reexec_sl_cost: bool = False
    # ReExecute_Logics.html P2 / P3 / P5 — wired regardless of move_sl_enabled.
    # no_wait_trade_reexec: skip the slot re-execution delay on re-executions.
    # no_reentry_sl_cost: block the re_entry action when SL was moved to cost.
    # no_reentry_after_end: block ReExecute / ReEntry past Portfolio End Time.
    no_wait_trade_reexec: bool = False
    no_reentry_sl_cost: bool = True
    no_reentry_after_end: bool = False
    # LTP-buffer variant (spec §3 action v3, leg-level): on a losing leg, slide
    # SL toward LTP by this buffer (price units). 0 = exit at current price.
    ltp_buffer: float = 0.0
    # Hit-On-Leg cross-slot triggers (spec §3 1.3(f)/(g)).
    hit_on_leg_sl: bool = False
    hit_on_leg_target: bool = False
    # Portfolio-aggregate Move SL trigger (spec §2.3). agg_pnl_* mirror the
    # PortfolioConfig fields. agg_trigger_ns / preseeded_bus are the pass-2
    # inputs the two-pass runner injects (0 / None during pass 1 and whenever
    # the feature is off — both are benign no-ops in ManagedExitStrategy).
    agg_pnl_enabled: bool = False
    agg_pnl_threshold: float = 0.0
    agg_pnl_direction: str = "loss"
    agg_trigger_ns: int = 0
    preseeded_bus: dict | None = None


# Underlying-based SL types are now universal for FX/crypto — D5 "underlying
# defaults to self" makes them apply against the portfolio's primary
# instrument price. Only the premium-based types remain options-only.
_VALID_PF_SL_TYPES_FX = (
    "Combined Loss", "Underlying Movement", "Loss and Underlying Range",
)
_UNDERLYING_PF_SL_TYPES = ("Underlying Movement", "Loss and Underlying Range")
# Underlying-based Target is now universal (D5 "underlying = self"), mirroring
# the SL side. Only the premium-based types remain options-only.
_VALID_PF_TGT_TYPES_FX = ("Combined Profit", "Underlying Movement")
_UNDERLYING_PF_TGT_TYPES = ("Underlying Movement",)
_OPTIONS_ONLY_PF_SL_TYPES = ("Combined Premium", "Absolute Combined Premium")
_OPTIONS_ONLY_PF_TGT_TYPES = ("Combined Premium", "Absolute Combined Premium")
_VALID_PF_ACTIONS_FX = (
    "SqOff", "ReExecute",
    # ReExecute-family — all three drive the config-driven portfolio ReExecute
    # replay (spec §2.4): slots re-run flat from the clip timestamp and the
    # segment is spliced on. The "at Entry Price" variants replay as plain
    # ReExecute (the FX adaptation the spec marks ⚙️ — price-wait re-entry at a
    # specific level lives at the leg level, spec §1.2(d)).
    "ReExecute at Entry Price",
    "ReExecute Same Contract at EntryPrice",
    # Cross-portfolio actions (spec §2.1(h)/(i)/(j)). Each clip in this
    # portfolio also writes an event to the cross-portfolio bus; the named
    # target portfolio reads it at its next run start.
    "SqOff Other Portfolio",
    "Execute Other Portfolio",
    "Start Other Portfolio",
)
_OPTIONS_ONLY_PF_ACTIONS: tuple[str, ...] = ()
_CROSS_PORTFOLIO_ACTIONS = (
    "SqOff Other Portfolio", "Execute Other Portfolio", "Start Other Portfolio",
)

# Cross-portfolio event bus. Keyed by target_portfolio name; each entry is a
# list of pending events (action, source_portfolio, ts_iso). When portfolio B
# runs, `consume_cross_portfolio_events("B")` pops its queue and the runner
# applies the requested action(s) at run start.
_CROSS_PORTFOLIO_EVENT_BUS: dict[str, list[dict]] = {}


def publish_cross_portfolio_event(target_portfolio: str, action: str,
                                  source_portfolio: str, ts_iso: str) -> None:
    """Append a cross-portfolio event for later consumption by target_portfolio."""
    if not target_portfolio:
        return
    _CROSS_PORTFOLIO_EVENT_BUS.setdefault(target_portfolio, []).append(
        {"action": action, "source": source_portfolio, "ts": ts_iso}
    )


def consume_cross_portfolio_events(target_portfolio: str) -> list[dict]:
    """Pop all pending events targeting this portfolio. Returns [] if none."""
    if not target_portfolio:
        return []
    return _CROSS_PORTFOLIO_EVENT_BUS.pop(target_portfolio, []) or []


def clear_cross_portfolio_bus() -> None:
    """Reset the cross-portfolio bus (used between orchestrator sessions)."""
    _CROSS_PORTFOLIO_EVENT_BUS.clear()
_VALID_MOVE_SL_ACTIONS_FX = ("Move Only for Profitable Legs",
                             "Move SL for All Legs Despite Loss/Profit",
                             "Move SL to LTP + Buffer for Loss Making Legs")
_OPTIONS_ONLY_MOVE_SL_ACTIONS: tuple[str, ...] = ()

# D2 rename (locked): "SameStrike" → "Same Contract". Futures use contract month, not strike.
# Translate legacy config strings on load so existing portfolios keep working.
_LEGACY_PF_ACTION_ALIASES = {
    "ReExecute SameStrike at EntryPrice": "ReExecute Same Contract at EntryPrice",
}


def _normalize_pf_action(action: str | None) -> str:
    """Translate legacy action names (D2) so old configs still resolve correctly."""
    if not action:
        return "SqOff"
    return _LEGACY_PF_ACTION_ALIASES.get(action, action)


# ReExecute-family actions all trigger clip-then-replay. The "at Entry Price"
# variants currently fall back to immediate ReExecute because leg-level
# price-wait re-entry is not yet wired (spec §1.2 1.2(d)).
_REEXECUTE_FAMILY_ACTIONS = (
    "ReExecute",
    "ReExecute at Entry Price",
    "ReExecute Same Contract at EntryPrice",
)


def _is_reexec_action(action: str) -> bool:
    return action in _REEXECUTE_FAMILY_ACTIONS


def _resolve_pf_stoploss(portfolio) -> tuple[_PfStoplossSettings, list[str]]:
    """Validate portfolio.pf_sl_* fields per portfolio_sl_tgt.html §1-§2.

    Returns (settings, warnings). settings.enabled=False when the user hasn't
    enabled the feature OR validation falls back to safe defaults. Warnings
    surface options-only types/actions silently downgraded to FX/crypto
    universals.
    """
    warnings: list[str] = []
    if not getattr(portfolio, "pf_sl_enabled", False):
        return _PfStoplossSettings(enabled=False), warnings

    sl_type = getattr(portfolio, "pf_sl_type", "Combined Loss") or "Combined Loss"
    if sl_type in _OPTIONS_ONLY_PF_SL_TYPES:
        warnings.append(
            f"pf_sl_type={sl_type!r} is options-only (premium/underlying-based); "
            f"downgraded to 'Combined Loss' for FX/crypto. Spec §1.2."
        )
        sl_type = "Combined Loss"
    elif sl_type not in _VALID_PF_SL_TYPES_FX:
        warnings.append(
            f"Invalid pf_sl_type={sl_type!r} — falling back to 'Combined Loss'."
        )
        sl_type = "Combined Loss"

    action = _normalize_pf_action(getattr(portfolio, "pf_sl_action", "SqOff"))
    if action in _OPTIONS_ONLY_PF_ACTIONS:
        warnings.append(
            f"pf_sl_action={action!r} requires cross-portfolio dispatch infrastructure "
            f"that is not yet wired; closing local portfolio only (SqOff fallback)."
        )
        action = "SqOff"
    elif action not in _VALID_PF_ACTIONS_FX:
        warnings.append(f"Invalid pf_sl_action={action!r} — falling back to 'SqOff'.")
        action = "SqOff"

    value = max(0.0, float(getattr(portfolio, "pf_sl_value", 0.0) or 0.0))
    delay = max(0, int(getattr(portfolio, "pf_sl_delay_sec", 0) or 0))
    reexec = max(0, int(getattr(portfolio, "pf_sl_reexecute_count", 0) or 0))

    sqoff_loss = bool(getattr(portfolio, "pf_sl_sqoff_only_loss_legs", False))
    sqoff_profit = bool(getattr(portfolio, "pf_sl_sqoff_only_profit_legs", False))
    if sqoff_loss and sqoff_profit:
        warnings.append(
            "pf_sl_sqoff_only_loss_legs and pf_sl_sqoff_only_profit_legs are "
            "mutually exclusive (spec §1.10) — disabling both."
        )
        sqoff_loss = sqoff_profit = False

    trail_enabled = bool(getattr(portfolio, "pf_sl_trail_enabled", False))
    trail_every = max(0.0, float(getattr(portfolio, "pf_sl_trail_every", 0.0) or 0.0))
    trail_by = max(0.0, float(getattr(portfolio, "pf_sl_trail_by", 0.0) or 0.0))
    if trail_enabled and (trail_every == 0 or trail_by == 0):
        warnings.append(
            "pf_sl_trail_enabled=True but trail_every=0 or trail_by=0 — trailing "
            "SL will have no effect."
        )

    target_pf = str(getattr(portfolio, "pf_sl_target_portfolio", "") or "")
    if action in _CROSS_PORTFOLIO_ACTIONS and not target_pf:
        warnings.append(
            f"pf_sl_action={action!r} requires pf_sl_target_portfolio to be set — "
            f"downgrading to 'SqOff' (local close)."
        )
        action = "SqOff"

    # Underlying-based SL bounds (spec §2.1). Only meaningful for the two
    # underlying SL types; for "Combined Loss" they're inert.
    u_below = max(0.0, float(getattr(portfolio, "pf_sl_underlying_below", 0.0) or 0.0))
    u_above = max(0.0, float(getattr(portfolio, "pf_sl_underlying_above", 0.0) or 0.0))
    if sl_type == "Underlying Movement" and value <= 0:
        warnings.append(
            "pf_sl_type='Underlying Movement' needs a non-zero pf_sl_value "
            "(the underlying price level to fire at) — SL disabled."
        )
        return _PfStoplossSettings(enabled=False), warnings
    if sl_type == "Loss and Underlying Range" and u_below <= 0 and u_above <= 0:
        warnings.append(
            "pf_sl_type='Loss and Underlying Range' needs pf_sl_underlying_below "
            "or pf_sl_underlying_above to be set — SL disabled."
        )
        return _PfStoplossSettings(enabled=False), warnings

    return _PfStoplossSettings(
        enabled=True, value=value, action=action, delay_sec=delay,
        reexecute_count=reexec,
        sqoff_only_loss_legs=sqoff_loss,
        sqoff_only_profit_legs=sqoff_profit,
        trail_enabled=trail_enabled,
        trail_every=trail_every, trail_by=trail_by,
        target_portfolio=target_pf,
        sl_type=sl_type,
        underlying_below=u_below,
        underlying_above=u_above,
    ), warnings


def _resolve_pf_target(portfolio) -> tuple[_PfTargetSettings, list[str]]:
    """Validate portfolio.pf_tgt_* fields per portfolio_sl_tgt.html §4-§5."""
    warnings: list[str] = []
    if not getattr(portfolio, "pf_tgt_enabled", False):
        return _PfTargetSettings(enabled=False), warnings

    tgt_type = getattr(portfolio, "pf_tgt_type", "Combined Profit") or "Combined Profit"
    if tgt_type in _OPTIONS_ONLY_PF_TGT_TYPES:
        warnings.append(
            f"pf_tgt_type={tgt_type!r} is options-only; downgraded to "
            f"'Combined Profit' for FX/crypto. Spec §4.2."
        )
        tgt_type = "Combined Profit"
    elif tgt_type not in _VALID_PF_TGT_TYPES_FX:
        warnings.append(
            f"Invalid pf_tgt_type={tgt_type!r} — falling back to 'Combined Profit'."
        )
        tgt_type = "Combined Profit"

    action = _normalize_pf_action(getattr(portfolio, "pf_tgt_action", "SqOff"))
    if action in _OPTIONS_ONLY_PF_ACTIONS:
        warnings.append(
            f"pf_tgt_action={action!r} requires cross-portfolio dispatch infrastructure "
            f"that is not yet wired; closing local portfolio only (SqOff fallback)."
        )
        action = "SqOff"
    elif action not in _VALID_PF_ACTIONS_FX:
        warnings.append(f"Invalid pf_tgt_action={action!r} — falling back to 'SqOff'.")
        action = "SqOff"

    value = max(0.0, float(getattr(portfolio, "pf_tgt_value", 0.0) or 0.0))
    delay = max(0, int(getattr(portfolio, "pf_tgt_delay_sec", 0) or 0))
    reexec = max(0, int(getattr(portfolio, "pf_tgt_reexecute_count", 0) or 0))

    trail_enabled = bool(getattr(portfolio, "pf_tgt_trail_enabled", False))
    trail_lock = max(0.0, float(getattr(portfolio, "pf_tgt_trail_lock_min_profit", 0.0) or 0.0))
    trail_reach = max(0.0, float(getattr(portfolio, "pf_tgt_trail_when_profit_reach", 0.0) or 0.0))
    trail_every = max(0.0, float(getattr(portfolio, "pf_tgt_trail_every", 0.0) or 0.0))
    trail_by = max(0.0, float(getattr(portfolio, "pf_tgt_trail_by", 0.0) or 0.0))
    if trail_enabled and trail_reach < trail_lock:
        warnings.append(
            f"pf_tgt_trail_when_profit_reach={trail_reach} < lock_min_profit={trail_lock} — "
            f"per spec §5.3 the activation threshold should be >= lock_min_profit."
        )

    target_pf = str(getattr(portfolio, "pf_tgt_target_portfolio", "") or "")
    if action in _CROSS_PORTFOLIO_ACTIONS and not target_pf:
        warnings.append(
            f"pf_tgt_action={action!r} requires pf_tgt_target_portfolio to be set — "
            f"downgrading to 'SqOff' (local close)."
        )
        action = "SqOff"

    # Underlying-Movement Target (spec §5.1) needs a non-zero price level
    # (pf_tgt_value is the underlying price to fire at, not a PnL amount).
    if tgt_type == "Underlying Movement" and value <= 0:
        warnings.append(
            "pf_tgt_type='Underlying Movement' needs a non-zero pf_tgt_value "
            "(the underlying price level to fire at) — Target disabled."
        )
        return _PfTargetSettings(enabled=False), warnings

    return _PfTargetSettings(
        enabled=True, tgt_type=tgt_type, value=value, action=action, delay_sec=delay,
        reexecute_count=reexec,
        trail_enabled=trail_enabled,
        trail_lock_min_profit=trail_lock,
        trail_when_profit_reach=trail_reach,
        trail_every=trail_every, trail_by=trail_by,
        target_portfolio=target_pf,
    ), warnings


def _resolve_move_sl_to_cost(portfolio) -> tuple[_MoveSLConfig, list[str]]:
    """Validate portfolio.move_sl_* fields per portfolio_sl_tgt.html §3.

    Returns the per-slot config (threaded into ManagedExitConfig). Default-off
    is the no-op state.
    """
    warnings: list[str] = []
    # ReExecute gating flags are read regardless of move_sl_enabled — see dataclass docs.
    no_reexec_sl_cost = bool(getattr(portfolio, "no_reexec_sl_cost", False))
    no_wait_trade_reexec = bool(getattr(portfolio, "no_wait_trade_reexec", False))
    no_reentry_sl_cost = bool(getattr(portfolio, "no_reentry_sl_cost", True))
    no_reentry_after_end = bool(getattr(portfolio, "no_reentry_after_end", False))

    # Portfolio-aggregate Move SL trigger (spec §2.3). Read regardless of
    # move_sl_enabled — it is an independent portfolio-level trigger, not a
    # sub-option of the per-slot Move SL to Cost feature.
    agg_pnl_enabled = bool(getattr(portfolio, "move_sl_agg_pnl_enabled", False))
    agg_pnl_threshold = float(getattr(portfolio, "move_sl_agg_pnl_threshold", 0.0) or 0.0)
    agg_pnl_direction = str(getattr(portfolio, "move_sl_agg_pnl_direction", "loss") or "loss").lower()
    if agg_pnl_direction not in ("loss", "profit"):
        warnings.append(
            f"Invalid move_sl_agg_pnl_direction={agg_pnl_direction!r} — falling back to 'loss'."
        )
        agg_pnl_direction = "loss"
    if agg_pnl_enabled and agg_pnl_threshold <= 0:
        warnings.append(
            "move_sl_agg_pnl_enabled but threshold <= 0 - aggregate Move SL trigger disabled."
        )
        agg_pnl_enabled = False

    if not getattr(portfolio, "move_sl_enabled", False):
        return _MoveSLConfig(
            enabled=False, no_reexec_sl_cost=no_reexec_sl_cost,
            no_wait_trade_reexec=no_wait_trade_reexec,
            no_reentry_sl_cost=no_reentry_sl_cost,
            no_reentry_after_end=no_reentry_after_end,
            agg_pnl_enabled=agg_pnl_enabled,
            agg_pnl_threshold=agg_pnl_threshold,
            agg_pnl_direction=agg_pnl_direction,
        ), warnings

    action = getattr(portfolio, "move_sl_action", "Move Only for Profitable Legs") \
        or "Move Only for Profitable Legs"
    if action in _OPTIONS_ONLY_MOVE_SL_ACTIONS:
        warnings.append(
            f"move_sl_action={action!r} (LTP + Buffer variant) is options-flavoured; "
            f"downgraded to 'Move Only for Profitable Legs' for FX/crypto."
        )
        action = "Move Only for Profitable Legs"
    elif action not in _VALID_MOVE_SL_ACTIONS_FX:
        warnings.append(
            f"Invalid move_sl_action={action!r} — falling back to 'Move Only for Profitable Legs'."
        )
        action = "Move Only for Profitable Legs"

    safety = max(0, int(getattr(portfolio, "move_sl_safety_sec", 0) or 0))
    ltp_buffer = max(0.0, float(getattr(portfolio, "move_sl_ltp_buffer", 0.0) or 0.0))

    return _MoveSLConfig(
        enabled=True, safety_sec=safety, action=action,
        trail_after=bool(getattr(portfolio, "move_sl_trail_after", False)),
        no_buy_legs=bool(getattr(portfolio, "move_sl_no_buy_legs", False)),
        no_reexec_sl_cost=no_reexec_sl_cost,
        no_wait_trade_reexec=no_wait_trade_reexec,
        no_reentry_sl_cost=no_reentry_sl_cost,
        no_reentry_after_end=no_reentry_after_end,
        ltp_buffer=ltp_buffer,
        hit_on_leg_sl=bool(getattr(portfolio, "move_sl_hit_on_leg_sl", False)),
        hit_on_leg_target=bool(getattr(portfolio, "move_sl_hit_on_leg_target", False)),
        agg_pnl_enabled=agg_pnl_enabled,
        agg_pnl_threshold=agg_pnl_threshold,
        agg_pnl_direction=agg_pnl_direction,
    ), warnings


@dataclasses.dataclass(frozen=True)
class _AggCoordination:
    """Pass-1 → pass-2 hand-off for the portfolio-aggregate Move SL trigger
    (spec §2.3). ``agg_trigger_ns`` is the UTC-ns timestamp at which the
    combined portfolio P&L first crossed the configured threshold (0 = never).
    ``event_bus`` is the union of every leg's pass-1 SL/target hit timestamps,
    ``{slot_id: {"sl_ns","tgt_ns"}}`` — pre-seeded into each pass-2 worker so
    the cross-slot Hit-On-Leg trigger works across process boundaries.
    """
    agg_trigger_ns: int = 0
    agg_trigger_ts: str | None = None
    event_bus: dict = dataclasses.field(default_factory=dict)
    logs: tuple[str, ...] = ()


def _compute_agg_coordination(portfolio, pass1_results: dict, move_sl) -> _AggCoordination:
    """Build the pass-2 coordination payload from pass-1 slot results.

    Merges every slot's per-bar equity curve into one combined-P&L timeline,
    finds the first timestamp the combined P&L crosses ``agg_pnl_threshold``
    in the configured direction, and unions every leg's SL/target hit
    timestamps into a cross-process event bus. Pure function of pass-1 output
    — keeps the two-pass run deterministic.
    """
    logs: list[str] = []

    # Cross-process event bus: union of pass-1 per-leg SL/target hits.
    event_bus: dict[str, dict[str, int]] = {}
    for r in pass1_results.values():
        if not r:
            continue
        sid = r.get("slot_id")
        ev = r.get("leg_exit_events") or {}
        if not sid or not ev:
            continue
        entry = event_bus.setdefault(str(sid), {})
        for k, v in ev.items():
            try:
                iv = int(v)
            except (TypeError, ValueError):
                continue
            if iv > entry.get(k, 0):
                entry[k] = iv

    agg_trigger_ns = 0
    agg_trigger_ts: str | None = None
    if move_sl.agg_pnl_enabled and move_sl.agg_pnl_threshold > 0:
        curves = [
            r.get("equity_curve_ts", [])
            for r in pass1_results.values()
            if r and r.get("equity_curve_ts")
        ]
        merged = _merge_equity_curves(curves)
        start_cap = float(getattr(portfolio, "starting_capital", 0.0) or 0.0)
        thr = abs(float(move_sl.agg_pnl_threshold))
        is_loss = move_sl.agg_pnl_direction == "loss"
        for pt in merged:
            ts_iso = pt.get("timestamp")
            if not ts_iso:
                continue  # seed point carries no timestamp
            pnl = float(pt.get("balance", start_cap)) - start_cap
            crossed = (pnl <= -thr) if is_loss else (pnl >= thr)
            if crossed:
                agg_trigger_ns = _ts_iso_to_ns(ts_iso)
                agg_trigger_ts = ts_iso
                break
        if agg_trigger_ns:
            logs.append(
                f"aggregate {move_sl.agg_pnl_direction} trigger: combined PnL crossed "
                f"{thr:g} at {agg_trigger_ts} — open legs move SL to cost from there"
            )
        else:
            logs.append(
                f"aggregate {move_sl.agg_pnl_direction} trigger: combined PnL never "
                f"crossed {thr:g} — no aggregate Move SL applied"
            )
    if event_bus:
        logs.append(
            f"cross-slot bus: {len(event_bus)} slot(s) published SL/target hits to pass 2"
        )
    return _AggCoordination(
        agg_trigger_ns=agg_trigger_ns,
        agg_trigger_ts=agg_trigger_ts,
        event_bus=event_bus,
        logs=tuple(logs),
    )


@dataclasses.dataclass(frozen=True)
class _ClipResult:
    """Outcome of _apply_portfolio_clip — drives report-level enforcement
    of portfolio Stoploss/Target. ``clip_ts is None`` means no enforcement
    fired (or feature disabled). ``clipped_slots`` is the set of slot_ids
    whose trades after clip_ts should be dropped — for full SqOff that's
    every enabled slot, for selective SqOff it's only the matching subset.
    """
    clip_ts: str | None = None  # ISO UTC string from equity_curve_ts (FIRST clip)
    clip_reason: str | None = None  # STOPLOSS | STOPLOSS_TRAIL | TARGET | TARGET_TRAIL
    clip_action: str | None = None  # SqOff | ReExecute
    clipped_slots: tuple[str, ...] = ()
    would_reexecute: bool = False
    reexec_count: int = 0
    logs: tuple[str, ...] = ()
    # All clip events fired during this portfolio run, in chronological order.
    # For non-ReExecute actions there's at most one entry. For ReExecute,
    # honours `pf_sl_reexecute_count` (0 = unlimited; otherwise capped).
    # Each tuple: (clip_ts_iso, reason, action).
    clip_events: tuple[tuple[str, str, str], ...] = ()


def _ts_iso_to_ns(ts_iso: str | None) -> int:
    """Convert an ISO timestamp string from equity_curve_ts to UTC nanoseconds.
    Returns 0 for the seed/None entry. The merged equity curve uses ISO with
    explicit '+00:00' offset (per _build_equity_curve_from_account)."""
    if not ts_iso:
        return 0
    try:
        return pd.Timestamp(ts_iso).value
    except Exception:
        return 0


def _apply_portfolio_clip(
    equity_curve_ts: list[dict],
    starting_capital: float,
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None = None,  # slot_id -> pnl_at_clip (for selective sqoff)
    slot_curves: dict | None = None,       # slot_id -> equity_curve_ts (selective at clip ts)
) -> _ClipResult:
    """Walk the unified equity curve in time order and decide where the
    portfolio-level Stoploss/Target would have triggered. Returns a
    _ClipResult that the caller uses to drop post-clip trades from the
    merged outputs.

    Spec evaluation order (portfolio_sl_tgt.html §8) per tick:
      1. Trailing SL ratchet
      2. Fixed SL check
      3. Trailing Target activate / ratchet
      4. Fixed Target check
    Delay confirmation: when a hit fires, defer clip by N seconds; cancel
    if the condition clears before delay expires (oscillation guard, §1.6).
    Trailing-Target SqOff always uses local SqOff regardless of action (§5).
    """
    if not pf_sl.enabled and not pf_tgt.enabled:
        return _ClipResult()
    if len(equity_curve_ts) < 2:
        return _ClipResult()  # need at least one real tick after the seed

    logs: list[str] = []

    # Trailing SL state
    sl_current = pf_sl.value if pf_sl.enabled else 0.0
    sl_trail_anchor = 0.0
    # Trailing Target state
    tgt_trail_active = False
    tgt_floor = 0.0
    tgt_trail_anchor = 0.0
    # Delay state — shared between SL and TGT per spec §4.6
    pending_reason: str | None = None  # STOPLOSS | STOPLOSS_TRAIL | TARGET | TARGET_TRAIL
    pending_at_ns: int = 0
    pending_clip_ts: str | None = None

    def _condition_holds(reason: str, pnl: float) -> bool:
        """Re-check whether the same condition still holds for a pending clip."""
        if reason == "STOPLOSS":
            return pf_sl.enabled and pnl <= -sl_current
        if reason == "STOPLOSS_TRAIL":
            return pf_sl.enabled and pf_sl.trail_enabled and pnl <= -sl_current
        if reason == "TARGET":
            return pf_tgt.enabled and pnl >= pf_tgt.value
        if reason == "TARGET_TRAIL":
            return tgt_trail_active and pnl <= tgt_floor
        return False

    # Cumulative ReExecute clip events. When pf_sl_action == "ReExecute" and
    # the cap allows, each SL clip resets trailing/pending state and continues
    # walking instead of stopping. reexecute_count=0 means unlimited;
    # otherwise the (count+1)-th SL hit ends the run.
    clip_events_acc: list[tuple[str, str, str]] = []
    reexec_cap = int(getattr(pf_sl, "reexecute_count", 0) or 0)

    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue  # seed entry
        balance = float(pt.get("balance", starting_capital))
        pnl = balance - starting_capital
        ts_ns = _ts_iso_to_ns(ts)

        # ── Step 1: Trailing SL ratchet ──
        if pf_sl.enabled and pf_sl.trail_enabled and pf_sl.trail_every > 0:
            gain = pnl - sl_trail_anchor
            if gain >= pf_sl.trail_every:
                steps = int(gain / pf_sl.trail_every)
                old_sl = sl_current
                sl_current = max(0.0, sl_current - steps * pf_sl.trail_by)
                sl_trail_anchor += steps * pf_sl.trail_every
                if sl_current != old_sl:
                    logs.append(
                        f"TRAIL_SL_UPDATED | Steps={steps} | Combined_SL={sl_current:.2f} | PnL={pnl:.2f}"
                    )

        # ── Step 2: Fixed SL check ──
        sl_hit_now = pf_sl.enabled and pnl <= -sl_current
        # Distinguish trailed-SL from fixed-SL hit (analogous to spec)
        sl_reason = "STOPLOSS_TRAIL" if (sl_hit_now and pf_sl.trail_enabled and sl_current < pf_sl.value) else "STOPLOSS"

        # ── Step 3: Trailing Target activate / ratchet ──
        if pf_tgt.enabled and pf_tgt.trail_enabled:
            if not tgt_trail_active and pnl >= pf_tgt.trail_when_profit_reach and pf_tgt.trail_when_profit_reach > 0:
                tgt_trail_active = True
                tgt_floor = pf_tgt.trail_lock_min_profit
                tgt_trail_anchor = pf_tgt.trail_when_profit_reach
                logs.append(
                    f"TRAIL_TARGET_ACTIVATED | Lock={tgt_floor:.2f} | WhenReach={tgt_trail_anchor:.2f}"
                )
            if tgt_trail_active and pf_tgt.trail_every > 0:
                gain = pnl - tgt_trail_anchor
                if gain >= pf_tgt.trail_every:
                    steps = int(gain / pf_tgt.trail_every)
                    old_floor = tgt_floor
                    tgt_floor += steps * pf_tgt.trail_by
                    tgt_trail_anchor += steps * pf_tgt.trail_every
                    if tgt_floor != old_floor:
                        logs.append(
                            f"TRAIL_TARGET_UPDATED | current_stop={tgt_floor:.2f}"
                        )

        # Trailing-Target exit (always SqOff per spec §5)
        tgt_trail_hit_now = tgt_trail_active and pnl <= tgt_floor

        # ── Step 4: Fixed Target check ──
        tgt_hit_now = pf_tgt.enabled and pnl >= pf_tgt.value and pf_tgt.value > 0

        # ── Delay confirmation handling ──
        # Determine if any new condition fires at this tick
        new_hit_reason: str | None = None
        if sl_hit_now:
            new_hit_reason = sl_reason
        elif tgt_trail_hit_now:
            new_hit_reason = "TARGET_TRAIL"
        elif tgt_hit_now:
            new_hit_reason = "TARGET"

        def _fire_clip(clip_ts_iso: str, reason: str):
            """Handle a confirmed clip. Returns a `_ClipResult` to stop the
            walk, or None to continue (when action=ReExecute and cap allows)."""
            action_local = pf_sl.action if reason.startswith("STOPLOSS") else (
                "SqOff" if reason == "TARGET_TRAIL" else pf_tgt.action
            )
            # Spec: all ReExecute-family actions replay (subject to cap).
            if _is_reexec_action(action_local) and (reexec_cap == 0 or len(clip_events_acc) < reexec_cap):
                clip_events_acc.append((clip_ts_iso, reason, action_local))
                logs.append(
                    f"PORTFOLIO_REEXECUTE_REPLAY | Reason={reason} | "
                    f"ReExec#{len(clip_events_acc)}/{reexec_cap or 'unlimited'}"
                )
                return None  # Continue walking
            # Final clip — stop the walk.
            return _build_clip_result(
                clip_ts=clip_ts_iso, reason=reason,
                pf_sl=pf_sl, pf_tgt=pf_tgt,
                slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                prior_events=tuple(clip_events_acc),
                slot_curves=slot_curves,
            )

        if pending_reason is not None:
            # Has the condition cleared?
            if not _condition_holds(pending_reason, pnl):
                logs.append(f"PORTFOLIO_DELAY_CLEARED | Condition persistent=False | Reason={pending_reason}")
                pending_reason = None
                pending_at_ns = 0
                pending_clip_ts = None
            else:
                # Condition still holds; check if delay elapsed
                delay_sec = pf_sl.delay_sec if pending_reason.startswith("STOPLOSS") else pf_tgt.delay_sec
                if delay_sec == 0 or (ts_ns - pending_at_ns) >= delay_sec * 1_000_000_000:
                    result = _fire_clip(pending_clip_ts, pending_reason)
                    if result is not None:
                        return result
                    # ReExecute replay: reset trail/pending state and continue.
                    sl_trail_anchor = pnl
                    tgt_trail_active = False
                    tgt_floor = 0.0
                    tgt_trail_anchor = 0.0
                    pending_reason = None
                    pending_at_ns = 0
                    pending_clip_ts = None

        if new_hit_reason is not None and pending_reason is None:
            delay_sec = pf_sl.delay_sec if new_hit_reason.startswith("STOPLOSS") else pf_tgt.delay_sec
            if delay_sec > 0:
                pending_reason = new_hit_reason
                pending_at_ns = ts_ns
                pending_clip_ts = ts
                logs.append(
                    f"PORTFOLIO_DELAY_PENDING | Reason={new_hit_reason} | Delay={delay_sec}s"
                )
            else:
                # No delay — clip immediately (or replay if ReExecute + under cap)
                result = _fire_clip(ts, new_hit_reason)
                if result is not None:
                    return result
                # ReExecute replay: reset trail state and continue.
                sl_trail_anchor = pnl
                tgt_trail_active = False
                tgt_floor = 0.0
                tgt_trail_anchor = 0.0

    # Walked the whole curve without a final (non-replayed) clip.
    # Surface any accumulated ReExecute replays for the caller.
    return _ClipResult(logs=tuple(logs), clip_events=tuple(clip_events_acc))


def _slot_pnl_at_ts(curve: list[dict] | None, clip_ns: int) -> float:
    """Per-slot PnL as of ``clip_ns`` — the slot's balance at the last equity
    point at-or-before the clip timestamp, minus its seed capital."""
    if not curve:
        return 0.0
    seed = float(curve[0].get("balance", 0.0) or 0.0)
    last = seed
    for pt in curve:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        if _ts_iso_to_ns(ts) <= clip_ns:
            last = float(pt.get("balance", last) or last)
        else:
            break
    return last - seed


def _build_clip_result(
    clip_ts: str,
    reason: str,
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    logs: list,
    prior_events: tuple[tuple[str, str, str], ...] = (),
    slot_curves: dict | None = None,
) -> _ClipResult:
    """Construct the ClipResult, applying selective SqOff filtering and
    classifying the action. Trailing-Target hits ignore configured action
    (always SqOff per spec §5).

    When ``slot_curves`` (slot_id → equity_curve_ts) is supplied, the
    loss/profit selective filter uses each slot's PnL **at the clip timestamp**
    rather than its end-of-run PnL — spec-accurate per §1.9-§1.10."""
    if reason.startswith("STOPLOSS"):
        action = pf_sl.action
        sqoff_loss = pf_sl.sqoff_only_loss_legs
        sqoff_profit = pf_sl.sqoff_only_profit_legs
    else:
        action = "SqOff" if reason == "TARGET_TRAIL" else pf_tgt.action
        sqoff_loss = False  # selective filters are SL-only per spec §1.9-§1.10
        sqoff_profit = False

    # Prefer per-slot PnL evaluated at the clip timestamp when slot curves are
    # available; fall back to the end-of-run proxy in slot_pnl_at_clip.
    if slot_curves and (sqoff_loss or sqoff_profit):
        _clip_ns = _ts_iso_to_ns(clip_ts)
        slot_pnl_at_clip = {
            sid: _slot_pnl_at_ts(curve, _clip_ns) for sid, curve in slot_curves.items()
        }

    # Determine which slots are clipped
    if slot_pnl_at_clip is not None and (sqoff_loss or sqoff_profit):
        if sqoff_loss:
            clipped = tuple(sid for sid, p in slot_pnl_at_clip.items() if p < 0)
            filter_label = "LOSS_LEGS_ONLY"
        else:
            clipped = tuple(sid for sid, p in slot_pnl_at_clip.items() if p > 0)
            filter_label = "PROFIT_LEGS_ONLY"
        logs = list(logs) + [f"PORTFOLIO_PARTIAL_SQOFF | Reason={reason} | Filter={filter_label}"]
    else:
        # Full SqOff — every slot in slot_pnl_at_clip (or empty if not provided)
        clipped = tuple(slot_pnl_at_clip.keys()) if slot_pnl_at_clip else ()
        logs = list(logs) + [f"PORTFOLIO_SQOFF | Reason={reason}"]

    would_reexec = _is_reexec_action(action)
    final_events = prior_events + ((clip_ts, reason, action),)
    total_reexec = len(prior_events) + (1 if would_reexec else 0)
    if would_reexec:
        logs.append(
            f"PORTFOLIO_REEXECUTE_FINAL | Reason={reason} | TotalReExec={total_reexec} (cap reached → stop)"
        )

    # Cross-portfolio dispatch (spec §2.1(h)/(i)/(j) and §2.4 mirror). The
    # event is queued for the target portfolio; the runner consumes it at
    # its next start. Action name is normalised to a single-word verb the
    # consumer recognises.
    if action in _CROSS_PORTFOLIO_ACTIONS:
        target_pf = pf_sl.target_portfolio if reason.startswith("STOPLOSS") else pf_tgt.target_portfolio
        verb_map = {
            "SqOff Other Portfolio": "sqoff",
            "Execute Other Portfolio": "execute",
            "Start Other Portfolio": "start",
        }
        verb = verb_map.get(action, "sqoff")
        if target_pf:
            publish_cross_portfolio_event(target_pf, verb, "", clip_ts)
            logs.append(
                f"CROSS_PORTFOLIO_DISPATCH | Action={action!r} | Target={target_pf!r} | Verb={verb}"
            )

    return _ClipResult(
        clip_ts=clip_ts, clip_reason=reason, clip_action=action,
        clipped_slots=clipped,
        would_reexecute=would_reexec, reexec_count=total_reexec,
        logs=tuple(logs),
        clip_events=final_events,
    )


def _build_underlying_curve(engine, bar_type) -> list[dict]:
    """Extract the (timestamp, close) price series the engine processed.

    Used by the portfolio-level "Underlying Movement" / "Loss and Underlying
    Range" SL types (spec §2.1). The underlying defaults to the slot's own
    instrument (D5 "underlying = self"). Returns ``[]`` when bars can't be
    read — the caller degrades to "underlying SL not enforced" with a warning.
    """
    try:
        bars = engine.cache.bars(bar_type)
    except Exception:
        return []
    if not bars:
        return []
    out: list[dict] = []
    for b in bars:
        try:
            out.append({
                "timestamp": pd.Timestamp(b.ts_event, unit="ns", tz="UTC").isoformat(),
                "close": float(b.close),
            })
        except Exception:
            continue
    # cache.bars() returns most-recent-first; sort ascending by ts.
    out.sort(key=lambda p: p["timestamp"])
    return out


def _underlying_sl_clip(
    underlying_curve: list[dict] | None,
    equity_curve_ts: list[dict],
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    starting_capital: float,
    slot_curves: dict | None = None,
) -> _ClipResult:
    """Portfolio SL for the underlying-price-based types (spec §2.1).

    • "Underlying Movement"      → fire when the primary instrument price
      crosses ``pf_sl.value``.
    • "Loss and Underlying Range" → fire when combined PnL ≤ −value AND the
      underlying price is outside [underlying_below, underlying_above].

    ``Delay (sec)`` shifts the clip timestamp forward by that many seconds
    (a confirmed-after-N-seconds approximation). Returns a ``_ClipResult``;
    ``clip_ts is None`` means the SL never fired.
    """
    logs: list[str] = []
    if not underlying_curve:
        logs.append(
            "UNDERLYING_SL_SKIPPED | no underlying price series available "
            "(grouped / Path-B run, or missing data) — underlying SL not enforced"
        )
        return _ClipResult(logs=tuple(logs))

    # Underlying price series, ascending by ts.
    u: list[tuple[int, float]] = []
    for pt in underlying_curve:
        ts_ns = _ts_iso_to_ns(pt.get("timestamp"))
        if ts_ns:
            u.append((ts_ns, float(pt.get("close", 0.0) or 0.0)))
    u.sort()
    if not u:
        return _ClipResult(logs=tuple(logs))

    # Combined-PnL step series from the merged equity curve.
    eq: list[tuple[int, float]] = []
    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        eq.append((_ts_iso_to_ns(ts), float(pt.get("balance", starting_capital)) - starting_capital))
    eq.sort()

    is_movement = pf_sl.sl_type == "Underlying Movement"
    level = pf_sl.value
    below = pf_sl.underlying_below
    above = pf_sl.underlying_above
    delay_ns = int(pf_sl.delay_sec) * 1_000_000_000

    prev_close: float | None = None
    eq_idx = 0
    cur_pnl = 0.0
    for ts_ns, close in u:
        while eq_idx < len(eq) and eq[eq_idx][0] <= ts_ns:
            cur_pnl = eq[eq_idx][1]
            eq_idx += 1

        hit = False
        if is_movement:
            if prev_close is not None and (
                (prev_close <= level <= close) or (prev_close >= level >= close)
            ):
                hit = True
        else:  # Loss and Underlying Range
            range_breached = (below > 0 and close <= below) or (above > 0 and close >= above)
            if cur_pnl <= -level and range_breached:
                hit = True
        prev_close = close

        if hit:
            clip_ns = ts_ns + delay_ns
            clip_iso = pd.Timestamp(clip_ns, unit="ns", tz="UTC").isoformat()
            logs.append(
                f"UNDERLYING_SL_HIT | type={pf_sl.sl_type!r} | underlying={close:.5f} "
                f"| pnl={cur_pnl:.2f} | clip_ts={clip_iso}"
            )
            return _build_clip_result(
                clip_ts=clip_iso, reason="STOPLOSS",
                pf_sl=pf_sl, pf_tgt=pf_tgt,
                slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                slot_curves=slot_curves,
            )

    return _ClipResult(logs=tuple(logs))


def _user_sl_clip(
    equity_curve_ts: list[dict],
    starting_capital: float,
    cum_user_pnl: float,
    eff_max_loss: float | None,
    user_trail_sl: dict | None,
    all_slot_ids: list[str],
    scope_label: str = "USER",
) -> _ClipResult:
    """User-level (or tag-level) SL clip (spec §3 Level 3 / §6 / §11).

    Walks the merged equity curve in combined-PnL terms (this portfolio's PnL
    plus ``cum_user_pnl`` — the cumulative PnL the scope has accrued from
    earlier portfolios). The Max-Loss cap — optionally ratcheted tighter each
    bar by the Trailing SL — is a real force-sqoff: at the first breaching bar
    the whole portfolio is clipped (every slot), and post-clip trades are
    dropped by the caller.

    ``scope_label`` ("USER" or "TAG") only flavours the clip reason / log
    strings so the two tiers are distinguishable downstream; the arithmetic is
    identical. The tag tier reuses this exact walk one level down (spec §11).
    """
    if eff_max_loss is None:
        return _ClipResult()
    u_sl = abs(float(eff_max_loss))
    anchor = 0.0
    logs: list[str] = []
    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        combined = float(pt.get("balance", starting_capital)) - starting_capital + cum_user_pnl
        if user_trail_sl:
            gain = combined - anchor
            if gain >= user_trail_sl["every"]:
                steps = int(gain / user_trail_sl["every"])
                u_sl = max(0.0, u_sl - steps * user_trail_sl["by"])
                anchor += steps * user_trail_sl["every"]
        if combined <= -u_sl:
            ratcheted = bool(user_trail_sl) and u_sl < abs(float(eff_max_loss))
            reason = f"{scope_label}_TRAIL_STOPLOSS" if ratcheted else f"{scope_label}_STOPLOSS"
            logs.append(
                f"{scope_label}_SL_HIT | reason={reason} | combined_pnl={combined:.2f} "
                f"| effective_sl={u_sl:.2f} | clip_ts={ts}"
            )
            return _ClipResult(
                clip_ts=ts, clip_reason=reason, clip_action="SqOff",
                clipped_slots=tuple(all_slot_ids), logs=tuple(logs),
            )
    return _ClipResult()


def _underlying_tgt_clip(
    underlying_curve: list[dict] | None,
    equity_curve_ts: list[dict],
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    starting_capital: float,
    slot_curves: dict | None = None,
) -> _ClipResult:
    """Portfolio Target for the "Underlying Movement" type (spec §5.1).

    Mirror of the ``is_movement`` branch of ``_underlying_sl_clip`` on the
    profit side: fires the first time the primary instrument's price crosses
    ``pf_tgt.value``. ``Delay (sec)`` shifts the clip timestamp forward.
    Returns a ``_ClipResult``; ``clip_ts is None`` means the Target never fired.
    """
    logs: list[str] = []
    if not underlying_curve:
        logs.append(
            "UNDERLYING_TGT_SKIPPED | no underlying price series available "
            "(grouped / Path-B run, or missing data) — underlying Target not enforced"
        )
        return _ClipResult(logs=tuple(logs))

    u: list[tuple[int, float]] = []
    for pt in underlying_curve:
        ts_ns = _ts_iso_to_ns(pt.get("timestamp"))
        if ts_ns:
            u.append((ts_ns, float(pt.get("close", 0.0) or 0.0)))
    u.sort()
    if not u:
        return _ClipResult(logs=tuple(logs))

    level = pf_tgt.value
    delay_ns = int(pf_tgt.delay_sec) * 1_000_000_000
    prev_close: float | None = None
    for ts_ns, close in u:
        hit = (
            prev_close is not None
            and ((prev_close <= level <= close) or (prev_close >= level >= close))
        )
        prev_close = close
        if hit:
            clip_ns = ts_ns + delay_ns
            clip_iso = pd.Timestamp(clip_ns, unit="ns", tz="UTC").isoformat()
            logs.append(
                f"UNDERLYING_TGT_HIT | underlying={close:.5f} | level={level:.5f} "
                f"| clip_ts={clip_iso}"
            )
            return _build_clip_result(
                clip_ts=clip_iso, reason="TARGET",
                pf_sl=pf_sl, pf_tgt=pf_tgt,
                slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                slot_curves=slot_curves,
            )

    return _ClipResult(logs=tuple(logs))


def _user_tgt_clip(
    equity_curve_ts: list[dict],
    starting_capital: float,
    cum_user_pnl: float,
    eff_max_profit: float | None,
    user_trail_tgt: dict | None,
    all_slot_ids: list[str],
    scope_label: str = "USER",
) -> _ClipResult:
    """User-level (or tag-level) Target clip (spec §3 / target §6 / §11).

    Mirror of ``_user_sl_clip`` on the profit side. Walks the merged equity
    curve in combined-PnL terms (this portfolio's PnL plus the user's
    cumulative PnL from earlier portfolios). Two ceilings, checked per bar:

      * **Max Profit** — fixed cap; first bar combined PnL ≥ cap force-sqoffs
        every slot.
      * **Trailing Target / Profit-Lock** — once combined PnL reaches the
        activation threshold a floor is locked and ratcheted up; a fall back
        to the floor force-sqoffs every slot.

    Fixed Max Profit is checked before the trailing lock within a bar so the
    hard ceiling always wins a tie. Returns a ``_ClipResult``; ``clip_ts is
    None`` means neither ceiling fired.
    """
    if eff_max_profit is None and not user_trail_tgt:
        return _ClipResult()
    cap = abs(float(eff_max_profit)) if eff_max_profit is not None else None
    logs: list[str] = []
    tt_active = False
    tt_stop = 0.0
    tt_anchor = 0.0
    for pt in equity_curve_ts:
        ts = pt.get("timestamp")
        if ts is None:
            continue
        combined = float(pt.get("balance", starting_capital)) - starting_capital + cum_user_pnl

        # Fixed Max Profit ceiling.
        if cap is not None and cap > 0 and combined >= cap:
            logs.append(
                f"{scope_label}_TARGET_HIT | reason={scope_label}_TARGET | combined_pnl={combined:.2f} "
                f"| max_profit={cap:.2f} | clip_ts={ts}"
            )
            return _ClipResult(
                clip_ts=ts, clip_reason=f"{scope_label}_TARGET", clip_action="SqOff",
                clipped_slots=tuple(all_slot_ids), logs=tuple(logs),
            )

        # Trailing Target / Profit-Lock — reuses the leg-level pure ratchet.
        if user_trail_tgt:
            tt_active, tt_stop, tt_anchor, tt_hit = advance_trailing_target(
                tt_active, tt_stop, tt_anchor, combined,
                user_trail_tgt["when_reach"], user_trail_tgt["lock"],
                user_trail_tgt["every"], user_trail_tgt["by"],
            )
            if tt_hit:
                logs.append(
                    f"{scope_label}_TARGET_HIT | reason={scope_label}_TRAIL_TARGET | combined_pnl={combined:.2f} "
                    f"| locked_floor={tt_stop:.2f} | clip_ts={ts}"
                )
                return _ClipResult(
                    clip_ts=ts, clip_reason=f"{scope_label}_TRAIL_TARGET", clip_action="SqOff",
                    clipped_slots=tuple(all_slot_ids), logs=tuple(logs),
                )
    return _ClipResult()


def _earliest_clip(*results: _ClipResult) -> _ClipResult:
    """Return the _ClipResult with the earliest non-None clip_ts.

    Logs from every result are merged onto the winner so nothing is lost.
    When no result fired, returns the first with merged logs.
    """
    merged_logs: tuple[str, ...] = ()
    for r in results:
        merged_logs += r.logs
    fired = [r for r in results if r.clip_ts is not None]
    if not fired:
        return _ClipResult(logs=merged_logs)
    winner = min(fired, key=lambda r: _ts_iso_to_ns(r.clip_ts))
    return dataclasses.replace(winner, logs=merged_logs)


def _path_b_supports_filters(
    run_on_days: list | None,
    entry_start_time: str | None,
    entry_end_time: str | None,
    bar_type_str: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> bool:
    """True when the configured filters can be honoured under Path B.

    Three regimes:

    1. **No filters** (no run_on_days, no entry window) → Path B is trivially
       fine — single contiguous BacktestDataConfig.
    2. **Entry window only, on a non-intraday bar type** → window is a runtime
       no-op (daily/weekly bars have ts_event at midnight; see
       ``_is_intraday_bar_type``), so still single contiguous, still fine.
    3. **Any other filter combination** → ``_build_run_config`` honours the
       filter by emitting one BacktestDataConfig per allowed day (see
       ``_chunk_data_configs_for_path_b``). That requires concrete
       ``start_date`` and ``end_date`` to enumerate days; without them we
       fall back to Path A.

    When this returns False, callers must fall back to Path A so the run
    remains correct rather than silently ignoring user-configured filters.
    """
    has_run_on_days = run_on_days is not None
    has_entry_window = bool(entry_start_time) or bool(entry_end_time)

    if not has_run_on_days and not has_entry_window:
        return True

    if (
        not has_run_on_days
        and has_entry_window
        and bar_type_str
        and not _is_intraday_bar_type(bar_type_str)
    ):
        return True

    # Need to chunk; chunking enumerates days, so we need bounds.
    return bool(start_date) and bool(end_date)


@functools.lru_cache(maxsize=4)
def _cached_catalog_bars(catalog_path: str, bt_str: str, start_iso: str | None, end_iso: str | None):
    """Worker-local bar cache.

    Multiple slots sharing the same (bar_type, date range) within the same
    worker process hit this cache and skip the parquet read. Cache lives for
    the worker's lifetime (ProcessPoolExecutor keeps workers alive between tasks).

    maxsize sized for one slot's working set: MID + up to three extra TFs.
    A year of 1-min bars is ~75 MB, so cap holds peak per-worker cache
    footprint near ~300 MB instead of growing toward several GB. Cache
    misses cost ~0.5 s of parquet read per year-long slot — rounding error
    against engine.run() time.
    """
    catalog = ParquetDataCatalog(catalog_path)
    start_arg = pd.Timestamp(start_iso, tz="UTC") if start_iso else None
    end_arg = (
        pd.Timestamp(end_iso, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    ) if end_iso else None
    return catalog.bars(bar_types=[bt_str], start=start_arg, end=end_arg)


def run_backtest_node(
    catalog_path: str,
    bar_type_str: str | list[str],
    strategy_name: str,
    strategy_params: dict,
    trade_size: float = 0.01,
    starting_capital: float = 100_000.0,
    registry: dict | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    user_id: str | None = None,
    aggregate_to_bar_type: str = "",
) -> dict:
    """Path B (BacktestNode) variant of run_backtest.

    Public surface is identical to run_backtest. The engine is constructed
    by ``BacktestNode`` from a ``BacktestRunConfig`` instead of being wired
    by hand. After ``node.build()`` we attach the strategy imperatively (the
    same line as Path A) because custom strategies loaded via importlib
    aren't ``ImportableStrategyConfig``-friendly.

    Result extraction is unchanged — engines retrieved from the node expose
    the same ``trader.generate_*`` and ``kernel.cache.*`` APIs as engines
    constructed directly.
    """
    # Normalize bar types and auto-pair BID/ASK like Path A does
    if isinstance(bar_type_str, str):
        bar_type_strs = [bar_type_str]
    else:
        bar_type_strs = list(bar_type_str)
    paired_strs: list[str] = []
    for bt in bar_type_strs:
        paired_strs.extend(_pair_bid_ask_bar_type(bt))
    all_bar_types = bar_type_strs + paired_strs

    primary_bar_type = BarType.from_str(bar_type_strs[0])
    instrument_id = primary_bar_type.instrument_id
    extra_bar_types = [BarType.from_str(bt) for bt in bar_type_strs[1:]] or None

    # Build the run config and node
    run_cfg = _build_run_config(
        catalog_path=catalog_path,
        instrument_id=instrument_id,
        bar_type_strs=all_bar_types,
        venue=instrument_id.venue,
        starting_capital=starting_capital,
        start_date=start_date,
        end_date=end_date,
    )
    node = BacktestNode(configs=[run_cfg])
    # build() constructs the engine and (if catalog has the data) loads
    # instruments from it. After this we can fetch the engine handle.
    node.build()
    engine = node.get_engine(run_cfg.id)
    if engine is None:
        raise RuntimeError(
            f"BacktestNode.get_engine({run_cfg.id!r}) returned None — "
            f"catalog at {catalog_path!r} may be missing data for "
            f"{instrument_id} in range {start_date}..{end_date}"
        )

    # Build + attach strategy (identical to Path A from this point)
    registry_entry = (registry or STRATEGY_REGISTRY)[strategy_name]
    config_class = registry_entry["config_class"]
    # Apply per-user multiplier same way the portfolio path does (admin
    # cap doesn't apply here — standalone path takes raw trade_size as the
    # final order quantity, so we just scale it by the user's multiplier).
    from core.users import get_multiplier as _get_multiplier
    eff_trade_size = float(trade_size) * _get_multiplier(user_id)
    config_kwargs = {
        "instrument_id": instrument_id,
        "bar_type": primary_bar_type,
        "trade_size": Decimal(str(eff_trade_size)),
        **strategy_params,
    }
    if extra_bar_types and _config_supports_extra_bar_types(config_class):
        config_kwargs["extra_bar_types"] = extra_bar_types
    if aggregate_to_bar_type and _config_supports_aggregate_to(config_class):
        config_kwargs["aggregate_to_bar_type"] = aggregate_to_bar_type
    strategy_config = config_class(**config_kwargs)
    strategy = registry_entry["strategy_class"](strategy_config)
    engine.add_strategy(strategy)

    # Run via the node — dispose_on_completion=True (default) cleans up.
    node.run()

    # Result extraction uses the engine handle directly. The node still
    # holds a reference until disposed, so the engine's reports/cache are
    # still accessible here.
    adapter_cfg = load_adapter_config_for_bar_type(bar_type_strs[0])
    fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)
    results = _extract_results(engine, starting_capital, fx_resolver)

    return results


def run_backtest(
    catalog_path: str,
    bar_type_str: str | list[str],
    strategy_name: str,
    strategy_params: dict,
    trade_size: float = 0.01,
    starting_capital: float = 100_000.0,
    registry: dict | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    user_id: str | None = None,
    aggregate_to_bar_type: str = "",
) -> dict:
    """
    Run a backtest using data from the catalog.

    Parameters
    ----------
    catalog_path : str
        Path to the ParquetDataCatalog.
    bar_type_str : str or list[str]
        BarType string(s), e.g. "BTCUSD.YAHOO-1-DAY-LAST-EXTERNAL"
        or ["EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL", "EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL"].
    strategy_name : str
        Name of strategy from STRATEGY_REGISTRY.
    strategy_params : dict
        Strategy-specific parameters.
    trade_size : float
        Position size per trade.
    starting_capital : float
        Starting account balance in USD.
    start_date : str, optional
        Start date for filtering bars (e.g. "2022-01-01").
    end_date : str, optional
        End date for filtering bars (e.g. "2024-12-31").

    Returns
    -------
    dict
        Results including trades, account info, and performance metrics.
    """
    # Path B opt-in. Single backtests have no run_on_days / entry_window
    # filters at this signature, so the route to Path B is unconditional.
    if _path_b_active():
        return run_backtest_node(
            catalog_path=catalog_path,
            bar_type_str=bar_type_str,
            strategy_name=strategy_name,
            strategy_params=strategy_params,
            trade_size=trade_size,
            starting_capital=starting_capital,
            registry=registry,
            start_date=start_date,
            end_date=end_date,
            user_id=user_id,
        )

    # Normalize to list
    if isinstance(bar_type_str, str):
        bar_type_strs = [bar_type_str]
    else:
        bar_type_strs = list(bar_type_str)

    # Auto-pair: for each user-supplied bar type, also load its ASK/BID
    # counterpart(s) so the matching engine can fill at real spread prices.
    paired_strs: list[str] = []
    for bt in bar_type_strs:
        paired_strs.extend(_pair_bid_ask_bar_type(bt))

    # Route through the worker-local LRU so a second strategy on the same
    # (instrument, date range) inside the same worker skips the parquet read.
    catalog = ParquetDataCatalog(catalog_path)
    bars = []
    missing_pairs: list[str] = []
    for bt_str in bar_type_strs + paired_strs:
        try:
            cached = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
        except Exception:
            cached = []
            if bt_str in paired_strs:
                missing_pairs.append(bt_str)
        if not cached and bt_str in paired_strs:
            missing_pairs.append(bt_str)
        bars.extend(cached)
    missing_pairs = list(dict.fromkeys(missing_pairs))

    if not bars:
        date_info = f" in range {start_date or 'start'} to {end_date or 'end'}"
        raise ValueError(f"No bars found in catalog for {bar_type_strs}{date_info}")

    # Primary bar type (first) determines instrument
    primary_bar_type = BarType.from_str(bar_type_strs[0])
    instrument_id = primary_bar_type.instrument_id

    # Extra bar types (if any)
    extra_bar_types = [BarType.from_str(bt) for bt in bar_type_strs[1:]] or None

    # Load instrument
    instrument = next(
        (inst for inst in catalog.instruments() if inst.id == instrument_id),
        None,
    )

    if instrument is None:
        raise ValueError(f"No instrument found for {instrument_id} in catalog")

    # Create engine with the same perf-tuning as _run_single_slot
    from nautilus_trader.config import RiskEngineConfig
    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(bypass_logging=True),
        risk_engine=RiskEngineConfig(bypass=True),
        run_analysis=False,
    )
    engine = BacktestEngine(config=engine_config)

    # Add venue
    venue = instrument_id.venue
    engine.add_venue(
        venue=venue,
        oms_type=OmsType.NETTING,
        account_type=AccountType.MARGIN,
        starting_balances=[Money(starting_capital, USD)],
        base_currency=USD,
        default_leverage=Decimal(1),
    )

    # Add instrument and data. Drop the local bar list after add_data so we
    # don't hold a second copy alongside the engine's internal buffer.
    engine.add_instrument(instrument)
    engine.add_data(bars)
    del bars

    # Create strategy
    registry_entry = (registry or STRATEGY_REGISTRY)[strategy_name]
    config_class = registry_entry["config_class"]

    # Apply per-user multiplier (matches Path B branch + portfolio path).
    from core.users import get_multiplier as _get_multiplier
    eff_trade_size = float(trade_size) * _get_multiplier(user_id)

    config_kwargs = {
        "instrument_id": instrument_id,
        "bar_type": primary_bar_type,
        "trade_size": Decimal(str(eff_trade_size)),
        **strategy_params,
    }

    # Only pass extra_bar_types if the config class supports it and there are extras
    if extra_bar_types and _config_supports_extra_bar_types(config_class):
        config_kwargs["extra_bar_types"] = extra_bar_types

    # Custom streaming aggregation: aggregate the base stream up to a coarser
    # EXTERNAL bar type in-strategy (replaces Nautilus internal aggregation).
    if aggregate_to_bar_type and _config_supports_aggregate_to(config_class):
        config_kwargs["aggregate_to_bar_type"] = aggregate_to_bar_type

    strategy_config = config_class(**config_kwargs)
    strategy = registry_entry["strategy_class"](strategy_config)

    engine.add_strategy(strategy)

    # Run backtest
    engine.run()

    # Build an FX rate resolver from the venue's adapter config. Non-base
    # currency PnL (e.g. JPY from USDJPY) gets converted to the account base
    # currency during report extraction — otherwise it silently stays in the
    # position's native currency and the account balance never moves.
    adapter_cfg = load_adapter_config_for_bar_type(bar_type_strs[0])
    fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)

    # Extract results
    results = _extract_results(engine, starting_capital, fx_resolver)

    if missing_pairs:
        results["warning"] = (
            f"ASK/BID bar data not found in catalog ({', '.join(missing_pairs)}). "
            f"Fills will use MID prices — spread cost is not reflected in results."
        )

    engine.dispose()

    return results


def _run_single_backtest_task(
    catalog_path: str,
    bar_type_str,
    strategy_name: str,
    strategy_params: dict,
    trade_size: float,
    starting_capital: float,
    start_date: str | None,
    end_date: str | None,
    custom_strategies_dir: str | None,
    user_id: str | None = None,
) -> dict:
    """Top-level worker for ProcessPoolExecutor.

    Custom strategy classes (loaded dynamically via importlib) are not picklable
    across process boundaries, so each worker reloads the merged registry from
    the custom strategies directory before invoking run_backtest.

    ``user_id`` is forwarded to ``run_backtest`` so the per-user multiplier
    is applied inside the worker process (which re-reads ``users.json`` via
    ``core.users.get_multiplier``).
    """
    import time as _time
    registry = None
    if custom_strategies_dir:
        from core.custom_strategy_loader import get_merged_registry
        registry, _ = get_merged_registry(Path(custom_strategies_dir))

    t0 = _time.time()
    result = run_backtest(
        catalog_path=catalog_path,
        bar_type_str=bar_type_str,
        strategy_name=strategy_name,
        strategy_params=strategy_params,
        trade_size=trade_size,
        starting_capital=starting_capital,
        registry=registry,
        start_date=start_date,
        end_date=end_date,
        user_id=user_id,
    )
    # Reported back to the parent so run_backtest_stream can persist it to
    # the runtime-history file and improve LPT estimates on the next run.
    result["elapsed_seconds"] = round(_time.time() - t0, 3)
    return result


def _worker_init_ignore_sigint() -> None:
    """ProcessPoolExecutor initializer: make workers ignore SIGINT.

    Without this, a console Ctrl+C is delivered to every process in the
    group; the worker mid-engine.run() raises KeyboardInterrupt from
    inside Nautilus's Cython engine, then races interpreter shutdown
    against Nautilus's Rust daemon threads writing to stderr — fatal.
    Ignoring SIGINT here lets the parent handle Ctrl+C and shut the
    pool down cleanly.
    """
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)


def _run_single_slot_node(
    catalog_path: str,
    slot: StrategySlotConfig,
    capital: float,
    custom_strategies_dir: str | None,
    slot_index: int,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
    default_squareoff_time: str | None = None,
    default_squareoff_tz: str | None = None,
    default_run_on_days: list | None = None,
    default_entry_start_time: str | None = None,
    default_entry_end_time: str | None = None,
    default_rbo_settings: "_RBOSettings | None" = None,
    default_other_settings: "_OtherSettings | None" = None,
    default_move_sl_settings: "_MoveSLConfig | None" = None,
    user_id: str | None = None,
) -> dict:
    """Path B variant of _run_single_slot.

    The signature mirrors Path A's so this is a drop-in for ProcessPoolExecutor.
    Only the engine construction layer changes — strategy building, exit-config
    wrapping, phase timing, and result extraction are identical to Path A.

    Filters (run_on_days, entry window) are NOT applied here. The caller
    (the gate at the top of _run_single_slot) is responsible for routing to
    Path A when filters are configured.
    """
    import os
    import time as _time
    _t_slot_start = _time.time()

    phase_times: dict | None = {} if os.environ.get("_PROFILE_PHASES") == "1" else None

    with _phase("registry_load", phase_times):
        if custom_strategies_dir:
            from core.custom_strategy_loader import get_merged_registry
            registry, _ = get_merged_registry(Path(custom_strategies_dir))
        else:
            registry = STRATEGY_REGISTRY

    # Resolve date range — slot-level override beats portfolio default.
    start_date = slot.start_date or default_start_date
    end_date = slot.end_date or default_end_date

    # Compute the same auto-paired bar type list Path A would use.
    bar_type_strs_to_load = [slot.bar_type_str]
    paired_strs = _pair_bid_ask_bar_type(slot.bar_type_str)
    bar_type_strs_to_load.extend(paired_strs)

    # Detect missing paired data up-front (Path A learned this from the
    # catalog read; we have to peek at the catalog ourselves to surface
    # the warning, since BacktestDataConfig silently ignores missing files).
    missing_pairs: list[str] = []
    catalog = ParquetDataCatalog(catalog_path)
    for bt_str in paired_strs:
        try:
            sample = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
        except Exception:
            sample = []
        if not sample:
            missing_pairs.append(bt_str)
    missing_pairs = list(dict.fromkeys(missing_pairs))

    primary_bt = BarType.from_str(slot.bar_type_str)
    instrument_id = primary_bt.instrument_id

    # Resolve effective squareoff (same priority chain as Path A)
    eff_squareoff_time = (
        slot.exit_config.squareoff_time
        or slot.squareoff_time
        or default_squareoff_time
    )
    eff_squareoff_tz = (
        slot.exit_config.squareoff_tz
        or slot.squareoff_tz
        or default_squareoff_tz
    )

    node = None
    try:
        with _phase("engine_build", phase_times):
            run_cfg = _build_run_config(
                catalog_path=catalog_path,
                instrument_id=instrument_id,
                bar_type_strs=bar_type_strs_to_load,
                venue=instrument_id.venue,
                starting_capital=capital,
                start_date=start_date,
                end_date=end_date,
                trader_id=f"SLOT-{slot_index:03d}",
                entry_start_time=default_entry_start_time,
                entry_end_time=default_entry_end_time,
                run_on_days=default_run_on_days,
                rbo_settings=default_rbo_settings,
            )
            node = BacktestNode(configs=[run_cfg])
            node.build()
            engine = node.get_engine(run_cfg.id)
            if engine is None:
                raise ValueError(
                    f"BacktestNode failed to build an engine for {slot.bar_type_str} "
                    f"in range {start_date or 'start'}..{end_date or 'end'}"
                )

        with _phase("strategy_build", phase_times):
            # RBO needs ManagedExitStrategy (state machine lives there) — force
            # the wrapped path even if the slot has no exit-management or
            # squareoff configured. Otherwise the raw signal class would run
            # ungated, defeating the point of enabling RBO.
            slot_qty = effective_slot_qty(slot, user_id)
            if slot.exit_config.has_exit_management() or eff_squareoff_time or default_rbo_settings is not None:
                managed_config = config_from_exit(
                    exit_config=slot.exit_config,
                    signal_name=slot.strategy_name,
                    signal_params=slot.strategy_params,
                    instrument_id=instrument_id,
                    bar_type=primary_bt,
                    trade_size=slot_qty,
                    squareoff_time=eff_squareoff_time,
                    squareoff_tz=eff_squareoff_tz,
                    rbo_settings=default_rbo_settings,
                    other_settings=default_other_settings,
                    move_sl_settings=default_move_sl_settings,
                    # Per-slot path: own process, no in-process siblings — the
                    # cross-slot bus is empty here (cross-process events arrive
                    # via the two-pass preseeded_bus inside _MoveSLConfig).
                    subscribe_bar_types=getattr(slot, "strategy_bar_types", None),
                    portfolio_id="",
                    slot_id=slot.slot_id,
                )
                strategy = ManagedExitStrategy(managed_config)
            else:
                if slot.strategy_name not in registry:
                    raise ValueError(f"Unknown strategy: {slot.strategy_name}")
                registry_entry = registry[slot.strategy_name]
                config_class = registry_entry["config_class"]
                valid_param_keys = set(registry_entry["params"].keys())
                filtered_params = {k: v for k, v in slot.strategy_params.items() if k in valid_param_keys}
                config_kwargs = {
                    "instrument_id": instrument_id,
                    "bar_type": primary_bt,
                    "trade_size": Decimal(str(slot_qty)),
                    **filtered_params,
                }
                _agg_to = _aggregate_target_for_slot(slot, primary_bt)
                if _agg_to and _config_supports_aggregate_to(config_class):
                    config_kwargs["aggregate_to_bar_type"] = _agg_to
                strategy_config = config_class(**config_kwargs)
                strategy = registry_entry["strategy_class"](strategy_config)

            engine.add_strategy(strategy)

        with _phase("engine_run", phase_times):
            node.run()

        with _phase("fx_resolver_build", phase_times):
            adapter_cfg = load_adapter_config_for_bar_type(slot.bar_type_str)
            fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)

        with _phase("extract_results", phase_times):
            results = _extract_results(engine, capital, fx_resolver)

        # Slot metadata + telemetry (mirrors Path A)
        results["slot_id"] = slot.slot_id
        results["display_name"] = slot.display_name
        results["strategy_name"] = slot.strategy_name
        results["bar_type"] = slot.bar_type_str
        results["allocated_capital"] = capital
        results["elapsed_seconds"] = round(_time.time() - _t_slot_start, 3)
        results["worker_pid"] = os.getpid()
        results["path_b"] = True

        if missing_pairs:
            results["warning"] = (
                f"ASK/BID bar data not found in catalog ({', '.join(missing_pairs)}). "
                f"Fills will use MID prices — spread cost is not reflected in results."
            )

        ci = _cached_catalog_bars.cache_info()
        results["cache_hits"] = ci.hits
        results["cache_misses"] = ci.misses
        results["cache_currsize"] = ci.currsize
        try:
            import psutil as _psutil
            results["worker_rss_mb"] = round(_psutil.Process(os.getpid()).memory_info().rss / 1e6, 1)
        except Exception:
            results["worker_rss_mb"] = None

        if phase_times is not None:
            results["phase_times"] = {k: round(v, 4) for k, v in phase_times.items()}
    finally:
        # Default dispose_on_completion=True already disposes engines after
        # node.run(). Explicit dispose() here is defensive against early
        # exits before run() (e.g. exception during strategy build).
        if node is not None:
            try:
                node.dispose()
            except BaseException:
                pass

    return results


def _run_single_slot(
    catalog_path: str,
    slot: StrategySlotConfig,
    capital: float,
    custom_strategies_dir: str | None,
    slot_index: int,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
    default_squareoff_time: str | None = None,
    default_squareoff_tz: str | None = None,
    default_run_on_days: list | None = None,
    default_entry_start_time: str | None = None,
    default_entry_end_time: str | None = None,
    default_rbo_settings: "_RBOSettings | None" = None,
    default_other_settings: "_OtherSettings | None" = None,
    default_move_sl_settings: "_MoveSLConfig | None" = None,
    user_id: str | None = None,
    default_capture_underlying: bool = False,
    default_replay_cutoff_ns: int = 0,
    default_vwap_fill: bool = False,
) -> dict:
    """Run a single strategy slot in its own engine.

    Top-level and picklable so it can run under ProcessPoolExecutor.
    Rebuilds the merged registry inside the worker because custom strategy
    classes loaded via importlib are not picklable across processes.
    """
    # Path B opt-in. The gate now allows Path B for run_on_days / intraday
    # entry-window filters too — _build_run_config honours them by emitting
    # one BacktestDataConfig per allowed day. Slot-level dates win over
    # portfolio defaults (matches Path A's resolution at line ~1085).
    _gate_start_date = slot.start_date or default_start_date
    _gate_end_date = slot.end_date or default_end_date
    if _path_b_active() and _path_b_supports_filters(
        default_run_on_days, default_entry_start_time, default_entry_end_time,
        bar_type_str=slot.bar_type_str,
        start_date=_gate_start_date,
        end_date=_gate_end_date,
    ):
        return _run_single_slot_node(
            catalog_path=catalog_path,
            slot=slot,
            capital=capital,
            custom_strategies_dir=custom_strategies_dir,
            slot_index=slot_index,
            default_start_date=default_start_date,
            default_end_date=default_end_date,
            default_squareoff_time=default_squareoff_time,
            default_squareoff_tz=default_squareoff_tz,
            default_run_on_days=default_run_on_days,
            default_entry_start_time=default_entry_start_time,
            default_entry_end_time=default_entry_end_time,
            default_rbo_settings=default_rbo_settings,
            default_other_settings=default_other_settings,
            default_move_sl_settings=default_move_sl_settings,
            user_id=user_id,
        )

    import os
    import time as _time
    _t_slot_start = _time.time()

    # Phase-timing bag: populated only when _PROFILE_PHASES=1, else None so
    # wrappers are no-ops. Attached to results at the end if non-None.
    phase_times: dict | None = {} if os.environ.get("_PROFILE_PHASES") == "1" else None

    with _phase("registry_load", phase_times):
        if custom_strategies_dir:
            from core.custom_strategy_loader import get_merged_registry
            registry, _ = get_merged_registry(Path(custom_strategies_dir))
        else:
            registry = STRATEGY_REGISTRY

    with _phase("catalog_init", phase_times):
        catalog = ParquetDataCatalog(catalog_path)

    # Resolve date range: slot-level override wins, else fall back to the
    # portfolio-level default. Lets users pick a custom range once at the
    # portfolio level without touching every slot.
    start_date = slot.start_date or default_start_date
    end_date = slot.end_date or default_end_date

    # Load bars and instrument. Bars are served from the worker-local LRU cache
    # so a second slot on the same worker with same (bar_type, start, end) skips
    # the parquet read entirely.
    with _phase("instruments_scan", phase_times):
        instrument_map = {inst.id: inst for inst in catalog.instruments()}
    with _phase("bars_load", phase_times):
        # Load the slot's primary bar type + its BID/ASK pair(s) if any exist.
        # Nautilus's matching engine needs the opposite quote side to fill FX
        # market orders. MID slots load both ASK and BID so the engine fills
        # at real spread prices instead of the midpoint.
        bar_type_strs_to_load = [slot.bar_type_str]
        paired_strs = _pair_bid_ask_bar_type(slot.bar_type_str)
        bar_type_strs_to_load.extend(paired_strs)

        all_bars = []
        missing_pairs: list[str] = []
        for bt_str in bar_type_strs_to_load:
            try:
                cached = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
            except Exception:
                cached = []
                if bt_str in paired_strs:
                    missing_pairs.append(bt_str)
            if not cached and bt_str in paired_strs:
                missing_pairs.append(bt_str)
            all_bars.extend(cached)
        # De-duplicate in case both except and empty-check fire for the same str
        missing_pairs = list(dict.fromkeys(missing_pairs))

    # Day-of-week filter (portfolio.run_on_days). NO LONGER pre-filters the
    # bar list — bars stay in the engine on every weekday so Nautilus's
    # internal aggregators see a continuous input stream. (Pre-filtering
    # produced a gap that made TimeBarAggregator emit synthetic stale-close
    # bars across excluded days, on which composite-bar strategies could fire
    # phantom signals.) The day filter is now enforced inside the strategy
    # by ManagedExitStrategy's per-bar gate — see allowed_weekdays in
    # core/managed_strategy.py. The resolved weekday set is threaded through
    # config_from_exit a few lines below. None = no filter; empty set =
    # portfolio explicitly disabled all weekdays — strategy never enters.
    allowed_weekdays = _allowed_weekdays(default_run_on_days)
    bars_filtered_by_run_on_days = 0  # always 0 now; kept for result-dict compat

    # Portfolio ReExecute replay (spec §2.4): a replay segment re-runs this
    # slot from the clip timestamp onward, so drop every bar before the
    # cutoff — the slot starts flat there. 0 = normal full-range run.
    if default_replay_cutoff_ns > 0:
        all_bars, _bars_dropped_pre_cutoff = _filter_bars_after_ns(
            all_bars, default_replay_cutoff_ns
        )

    # Intra-day entry window (portfolio.entry_start_time / .entry_end_time).
    # Both endpoints UTC and inclusive. Either may be None for unbounded.
    # Skipped for non-intraday bar types (daily/weekly/monthly) since their
    # ts_event is at the period start — applying an HH:MM window to daily
    # bars would unconditionally drop every bar.
    bars_filtered_by_entry_window = 0
    entry_window_skipped_reason: str | None = None
    # Managed slots gate entries internally (spec §9), so the entry-window
    # END side is NOT pre-dropped for them — post-window bars are kept so
    # SL/Target keep being monitored until squareoff. Raw (non-managed)
    # strategies have no exit management, so they keep the full pre-filter.
    _eff_sqoff_for_mgmt = (
        slot.exit_config.squareoff_time or slot.squareoff_time or default_squareoff_time
    )
    _slot_is_managed = bool(
        slot.exit_config.has_exit_management() or _eff_sqoff_for_mgmt
        or default_rbo_settings is not None
    )
    with _phase("entry_window_filter", phase_times):
        if (default_entry_start_time or default_entry_end_time) and not _is_intraday_bar_type(slot.bar_type_str):
            entry_window_skipped_reason = (
                f"bar type {slot.bar_type_str} is not intraday — entry window ignored"
            )
        else:
            _filter_end = None if _slot_is_managed else default_entry_end_time
            all_bars, bars_filtered_by_entry_window = _filter_bars_by_time_of_day(
                all_bars, default_entry_start_time, _filter_end
            )

    if not all_bars:
        msg = f"No bars in date range {start_date or 'start'}..{end_date or 'end'} for {slot.bar_type_str}"
        extra = []
        if bars_filtered_by_run_on_days:
            extra.append(f"run_on_days dropped {bars_filtered_by_run_on_days}")
        if bars_filtered_by_entry_window:
            extra.append(f"entry window dropped {bars_filtered_by_entry_window}")
        if extra:
            msg += f" (after filters: {'; '.join(extra)})"
        raise ValueError(msg)

    primary_bt = BarType.from_str(slot.bar_type_str)
    instrument_id = primary_bt.instrument_id
    instrument = instrument_map.get(instrument_id)
    if instrument is None:
        raise ValueError(f"No instrument found for {slot.bar_type_str}")

    extra_bar_types = None

    # Create engine with aggressive per-run performance tuning:
    #   - bypass_logging: skip all kernel/strategy log emission
    #   - RiskEngineConfig(bypass=True): skip per-order risk checks (OK for controlled backtests)
    #   - run_analysis=False: skip built-in post-run analytics; we compute our own metrics
    from nautilus_trader.config import RiskEngineConfig

    # try/finally guarantees engine.dispose() even on BaseException (SystemExit,
    # CancelledError). Workers ignore SIGINT via the pool initializer, so KI
    # shouldn't fire here, but this is the belt-and-suspenders contract.
    engine = None
    try:
        with _phase("engine_build", phase_times):
            engine = BacktestEngine(config=BacktestEngineConfig(
                trader_id=TraderId(f"SLOT-{slot_index:03d}"),
                logging=LoggingConfig(bypass_logging=True),
                risk_engine=RiskEngineConfig(bypass=True),
                run_analysis=False,
            ))

            venue = instrument_id.venue
            engine.add_venue(
                venue=venue,
                oms_type=OmsType.NETTING,
                account_type=AccountType.MARGIN,
                starting_balances=[Money(capital, USD)],
                base_currency=USD,
                default_leverage=Decimal(1),
            )
            engine.add_instrument(instrument)
            engine.add_data(all_bars)
            # VWAP proxy-fill (spec §4.2): index ASK/BID bars by ts before the
            # bar list is dropped, so _extract_results can reprice SL/Target
            # exits. None when the flag is off or no ASK/BID data is present.
            vwap_lookup = (
                _build_vwap_lookup(all_bars)
                if (default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1")
                else None
            )
            # Free the bar list reference; Nautilus has copied into its internal cache.
            del all_bars

        # Resolve effective squareoff: leg (ExitConfig) > slot > portfolio default.
        # Done here (not inside config_from_exit) so the routing decision below can
        # also see whether squareoff is set even when the slot has no SL/TP.
        eff_squareoff_time = (
            slot.exit_config.squareoff_time
            or slot.squareoff_time
            or default_squareoff_time
        )
        eff_squareoff_tz = (
            slot.exit_config.squareoff_tz
            or slot.squareoff_tz
            or default_squareoff_tz
        )

        with _phase("strategy_build", phase_times):
            reg = registry
            # ManagedExitStrategy is required when SL/TP/trailing OR squareoff
            # is set, OR when RBO is active — squareoff alone (no SL/TP) needs
            # the wrapper because raw strategy classes don't know how to
            # time-close, and RBO needs it because the gate state machine
            # lives there. The day-of-week gate (allowed_weekdays) also lives
            # in the wrapper, so any active run_on_days filter forces this
            # path too — otherwise a raw strategy would ignore the filter now
            # that we no longer pre-filter bars from the engine.
            slot_qty = effective_slot_qty(slot, user_id)
            if (slot.exit_config.has_exit_management() or eff_squareoff_time
                    or default_rbo_settings is not None
                    or allowed_weekdays is not None):
                managed_config = config_from_exit(
                    exit_config=slot.exit_config,
                    signal_name=slot.strategy_name,
                    signal_params=slot.strategy_params,
                    instrument_id=instrument_id,
                    bar_type=primary_bt,
                    trade_size=slot_qty,
                    rbo_settings=default_rbo_settings,
                    other_settings=default_other_settings,
                    move_sl_settings=default_move_sl_settings,
                    squareoff_time=eff_squareoff_time,
                    squareoff_tz=eff_squareoff_tz,
                    # Intraday entry window (spec §9) — gated inside the
                    # strategy so post-window bars still drive exit checks.
                    # Only meaningful for intraday bar types.
                    entry_start_time=(default_entry_start_time
                                      if _is_intraday_bar_type(slot.bar_type_str) else None),
                    entry_end_time=(default_entry_end_time
                                    if _is_intraday_bar_type(slot.bar_type_str) else None),
                    # Day-of-week filter (portfolio.run_on_days) — gated inside
                    # the strategy so bars stay in the engine stream and
                    # internal aggregators don't see gaps. ``None`` (no
                    # filter) and the empty-set case (explicit no-days) are
                    # both forwarded to ManagedExitConfig as-is.
                    allowed_weekdays=(None if allowed_weekdays is None
                                      else sorted(allowed_weekdays)),
                    # Per-slot runs each get their own process/engine, so the
                    # module-level cross-slot bus has no siblings to reach —
                    # an empty portfolio_id routes to the standalone bus.
                    # (Cross-slot wiring is meaningful only in _run_slot_group.)
                    subscribe_bar_types=getattr(slot, "strategy_bar_types", None),
                    portfolio_id="",
                    slot_id=slot.slot_id,
                )
                strategy = ManagedExitStrategy(managed_config)
            else:
                if slot.strategy_name not in reg:
                    raise ValueError(f"Unknown strategy: {slot.strategy_name}")

                registry_entry = reg[slot.strategy_name]
                config_class = registry_entry["config_class"]
                valid_param_keys = set(registry_entry["params"].keys())
                filtered_params = {k: v for k, v in slot.strategy_params.items() if k in valid_param_keys}

                config_kwargs = {
                    "instrument_id": instrument_id,
                    "bar_type": primary_bt,
                    "trade_size": Decimal(str(slot_qty)),
                    **filtered_params,
                }
                if extra_bar_types and _config_supports_extra_bar_types(config_class):
                    config_kwargs["extra_bar_types"] = extra_bar_types
                _agg_to = _aggregate_target_for_slot(slot, primary_bt)
                if _agg_to and _config_supports_aggregate_to(config_class):
                    config_kwargs["aggregate_to_bar_type"] = _agg_to

                strategy_config = config_class(**config_kwargs)
                strategy = registry_entry["strategy_class"](strategy_config)

            engine.add_strategy(strategy)

        with _phase("engine_run", phase_times):
            engine.run()

        with _phase("fx_resolver_build", phase_times):
            # FX resolver built from the slot's venue config (see run_backtest for rationale).
            adapter_cfg = load_adapter_config_for_bar_type(slot.bar_type_str)
            fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)

        with _phase("extract_results", phase_times):
            results = _extract_results(engine, capital, fx_resolver, vwap_lookup)
            # Capture the underlying price series for portfolio-level
            # Underlying-Movement / Loss-and-Range SL (spec §2.1). Only built
            # when the portfolio actually uses an underlying SL type — keeps
            # the result dict small for the common case. engine is still live
            # here (disposed in the finally below).
            if default_capture_underlying:
                results["underlying_curve"] = _build_underlying_curve(engine, primary_bt)
            # Per-leg SL/target hit timestamps (spec §2.3) — surfaced so the
            # two-pass aggregate-Move-SL runner can pre-seed pass 2's bus.
            # Empty {} for raw (non-ManagedExit) strategies.
            results["leg_exit_events"] = dict(
                getattr(strategy, "_exit_events_self", {}) or {}
            )

        # Add slot metadata
        results["slot_id"] = slot.slot_id
        results["display_name"] = slot.display_name
        results["strategy_name"] = slot.strategy_name
        results["bar_type"] = slot.bar_type_str
        results["allocated_capital"] = capital
        results["elapsed_seconds"] = round(_time.time() - _t_slot_start, 3)
        results["worker_pid"] = os.getpid()

        # Warn if paired ASK/BID data was unavailable — fills will use MID prices
        if missing_pairs:
            results["warning"] = (
                f"ASK/BID bar data not found in catalog ({', '.join(missing_pairs)}). "
                f"Fills will use MID prices — spread cost is not reflected in results."
            )

        # Surface day-of-week filter telemetry so users can see the rule
        # actually applied. ``run_on_days`` is None when no filter was set.
        if default_run_on_days is not None:
            results["run_on_days"] = list(default_run_on_days)
            results["bars_filtered_by_run_on_days"] = bars_filtered_by_run_on_days

        # Surface intra-day entry-window telemetry, same pattern as above.
        if default_entry_start_time or default_entry_end_time:
            results["entry_start_time"] = default_entry_start_time
            results["entry_end_time"] = default_entry_end_time
            results["bars_filtered_by_entry_window"] = bars_filtered_by_entry_window
            if entry_window_skipped_reason:
                results["entry_window_skipped"] = entry_window_skipped_reason
                # Append to existing warning if there is one, otherwise create.
                _existing = results.get("warning")
                _add = f"Entry window not applied: {entry_window_skipped_reason}."
                results["warning"] = f"{_existing} {_add}" if _existing else _add

        # Per-worker cache + RSS telemetry. Helps decide whether the LRU is
        # earning its keep on a given workload, and whether per-worker memory
        # is approaching a budget that warrants memory-aware eviction.
        ci = _cached_catalog_bars.cache_info()
        results["cache_hits"] = ci.hits
        results["cache_misses"] = ci.misses
        results["cache_currsize"] = ci.currsize
        try:
            import psutil as _psutil
            results["worker_rss_mb"] = round(_psutil.Process(os.getpid()).memory_info().rss / 1e6, 1)
        except Exception:
            results["worker_rss_mb"] = None

        if phase_times is not None:
            results["phase_times"] = {k: round(v, 4) for k, v in phase_times.items()}
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except BaseException:
                pass

    return results


def _extract_slot_from_group_reports(
    positions_report,
    fills_report,
    strategy_id: str,
    slot,
    capital: float,
    fx_resolver,
    vwap_lookup: dict | None = None,
) -> dict:
    """Build one slot's result dict by filtering a shared-engine's reports by strategy_id.

    Output shape matches ``_run_single_slot`` so ``_merge_portfolio_results``
    can consume it identically. Equity curve is synthesized from this slot's
    position closes (running balance = capital + cumulative realized PnL) since
    the shared engine only has one account-balance history.

    ``vwap_lookup`` enables the VWAP proxy-fill model on this slot's filtered
    reports (see ``_apply_vwap_fill``); ``None`` leaves fills unchanged.
    """
    slot_id_str = str(strategy_id)

    # Filter the engine's full report to just this slot's strategy_id
    slot_positions = pd.DataFrame()
    if positions_report is not None and not positions_report.empty \
            and "strategy_id" in positions_report.columns:
        mask = positions_report["strategy_id"].astype(str) == slot_id_str
        slot_positions = positions_report.loc[mask].copy()

    slot_fills = pd.DataFrame()
    if fills_report is not None and not fills_report.empty \
            and "strategy_id" in fills_report.columns:
        mask = fills_report["strategy_id"].astype(str) == slot_id_str
        slot_fills = fills_report.loc[mask].copy()

    # VWAP proxy-fill (spec §4.2): reprice this slot's SL/Target exits before
    # any per-slot metric is derived from slot_positions.
    vwap_fill_adjustments = 0
    if vwap_lookup:
        try:
            vwap_fill_adjustments = _apply_vwap_fill(
                slot_positions, slot_fills, vwap_lookup,
            )
        except Exception:
            vwap_fill_adjustments = 0

    # Per-trade realized PnL in base currency
    pnl_col = _pick_col(slot_positions, ["realized_pnl", "RealizedPnl", "pnl"]) if not slot_positions.empty else None
    ts_col = _pick_col(slot_positions, ["ts_closed", "ts_last", "ts_init"]) if not slot_positions.empty else None

    pnl_values: list[float] = []
    if pnl_col and not slot_positions.empty:
        # Sort by close timestamp so the synthetic equity curve is monotonic in time
        if ts_col:
            slot_positions = slot_positions.sort_values(ts_col, kind="stable").reset_index(drop=True)
        pnl_values = _base_values_from_report(slot_positions, pnl_col, ts_col, fx_resolver)

    trades = len(pnl_values)
    wins = sum(1 for p in pnl_values if p > 0)
    losses = sum(1 for p in pnl_values if p < 0)
    flat_trades = max(trades - wins - losses, 0)
    total_realized = float(sum(pnl_values))
    final_balance = capital + total_realized
    total_return_pct = (total_realized / capital) * 100 if capital > 0 else 0.0
    win_rate = (wins / trades * 100) if trades > 0 else 0.0
    # Decisive win rate excludes flat (zero-PnL) trades from the denominator.
    # See _extract_results for rationale.
    _decisive_n = wins + losses
    decisive_win_rate = (wins / _decisive_n * 100) if _decisive_n > 0 else None

    # Day-based win percentage — aggregate PnL by calendar date
    winning_days = 0
    losing_days = 0
    total_days = 0
    win_pct_days = 0.0
    loss_pct_days = 0.0
    daily_pnl: dict[str, float] = {}
    # Use entry time (ts_init) for daily grouping to match the HTML report which
    # groups by ENTRY TIME.  Fall back to ts_col (close time) if ts_init absent.
    entry_ts_col = _pick_col(slot_positions, ["ts_init"]) if not slot_positions.empty else None
    daily_ts_col = entry_ts_col or ts_col
    if pnl_values and daily_ts_col and not slot_positions.empty:
        ts_values = slot_positions[daily_ts_col].tolist()
        for i, pnl_val in enumerate(pnl_values):
            ts_raw = ts_values[i]
            try:
                dt = pd.Timestamp(ts_raw, unit="ns", tz="UTC") if ts_raw is not None and pd.notna(ts_raw) else None
            except (TypeError, ValueError):
                try:
                    dt = pd.Timestamp(ts_raw)
                except Exception:
                    dt = None
            day_key = dt.strftime("%Y-%m-%d") if dt is not None else "unknown"
            daily_pnl[day_key] = daily_pnl.get(day_key, 0.0) + pnl_val
        total_days = len(daily_pnl)
        winning_days = sum(1 for v in daily_pnl.values() if v > 0)
        losing_days = sum(1 for v in daily_pnl.values() if v < 0)
        win_pct_days = (winning_days / total_days * 100) if total_days > 0 else 0.0
        loss_pct_days = (losing_days / total_days * 100) if total_days > 0 else 0.0

    # Synthetic equity curve — starting point plus running sum at each close ts.
    # Use the same {"timestamp": iso_str, "balance": float} shape as
    # _build_equity_curve_from_account so _merge_equity_curves can combine
    # per-slot curves from both paths identically.
    equity_curve_ts: list[dict] = [{"timestamp": None, "balance": capital}]
    if pnl_values and ts_col:
        running = capital
        ts_values = slot_positions[ts_col].tolist()
        for i, pnl_val in enumerate(pnl_values):
            running += pnl_val
            ts_raw = ts_values[i]
            try:
                ts_iso = pd.Timestamp(ts_raw, unit="ns", tz="UTC").isoformat() if ts_raw is not None and pd.notna(ts_raw) else None
            except (TypeError, ValueError):
                try:
                    ts_iso = pd.Timestamp(ts_raw).isoformat()
                except Exception:
                    ts_iso = None
            equity_curve_ts.append({"timestamp": ts_iso, "balance": running})

    # Max drawdown from the equity curve
    balances = [pt["balance"] for pt in equity_curve_ts]
    peak = balances[0]
    max_dd = 0.0
    for val in balances:
        if val > peak:
            peak = val
        dd = ((val - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    return {
        "slot_id": slot.slot_id,
        "display_name": slot.display_name,
        "strategy_name": slot.strategy_name,
        "bar_type": slot.bar_type_str,
        "allocated_capital": capital,
        "starting_capital": capital,
        "final_balance": final_balance,
        "total_pnl": total_realized,
        "pnl": total_realized,
        "total_return_pct": total_return_pct,
        "total_trades": trades,
        "trades": trades,
        "wins": wins,
        "losses": losses,
        "flat_trades": flat_trades,
        "win_rate": win_rate,
        "decisive_win_rate": decisive_win_rate,
        "total_days": total_days,
        "winning_days": winning_days,
        "losing_days": losing_days,
        "win_pct_days": win_pct_days,
        "loss_pct_days": loss_pct_days,
        "daily_pnl": daily_pnl,
        "max_drawdown": max_dd,
        "equity_curve": balances,
        "equity_curve_ts": equity_curve_ts,
        "positions_report": positions_report_with_base(slot_positions, fx_resolver),
        "fills_report": slot_fills,
        "account_report": None,  # shared in a group, not per-slot
        "vwap_fill_applied": bool(vwap_fill_adjustments),
        "vwap_fill_adjustments": vwap_fill_adjustments,
    }


def _run_slot_group_node(
    catalog_path: str,
    group: list,
    custom_strategies_dir: str | None,
    group_index: int,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
    default_squareoff_time: str | None = None,
    default_squareoff_tz: str | None = None,
    default_run_on_days: list | None = None,
    default_entry_start_time: str | None = None,
    default_entry_end_time: str | None = None,
    default_rbo_settings: "_RBOSettings | None" = None,
    default_other_settings: "_OtherSettings | None" = None,
    default_move_sl_settings: "_MoveSLConfig | None" = None,
    user_id: str | None = None,
    portfolio_name: str = "",
) -> list[dict]:
    """Path B variant of _run_slot_group.

    All slots in the group share one ``BacktestNode`` / one engine. Each slot's
    strategy is attached imperatively after ``node.build()`` with its own
    ``order_id_tag`` so positions stay distinguishable in the post-run reports.

    Like _run_single_slot_node, this skips run_on_days / entry-window filters
    — the caller is responsible for routing to Path A when those are set.
    """
    import os
    import time as _time
    _t_group_start = _time.time()

    phase_times: dict | None = {} if os.environ.get("_PROFILE_PHASES") == "1" else None

    if not group:
        return []

    primary_slot = group[0][0]
    primary_bar_type_str = primary_slot.bar_type_str
    start_date = primary_slot.start_date or default_start_date
    end_date = primary_slot.end_date or default_end_date

    with _phase("registry_load", phase_times):
        if custom_strategies_dir:
            from core.custom_strategy_loader import get_merged_registry
            registry, _ = get_merged_registry(Path(custom_strategies_dir))
        else:
            registry = STRATEGY_REGISTRY

    # Auto-pair BID/ASK and detect missing pairs (same surface as Path A so
    # each slot result still gets a clear warning when fills will use MID).
    bar_type_strs_to_load = [primary_bar_type_str]
    paired_strs = _pair_bid_ask_bar_type(primary_bar_type_str)
    bar_type_strs_to_load.extend(paired_strs)

    missing_pairs: list[str] = []
    for bt_str in paired_strs:
        try:
            sample = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
        except Exception:
            sample = []
        if not sample:
            missing_pairs.append(bt_str)
    missing_pairs = list(dict.fromkeys(missing_pairs))

    primary_bt = BarType.from_str(primary_bar_type_str)
    instrument_id = primary_bt.instrument_id
    total_capital = float(sum(capital for _, capital in group))

    node = None
    expected_tags: list[str] = []
    try:
        with _phase("engine_build", phase_times):
            run_cfg = _build_run_config(
                catalog_path=catalog_path,
                instrument_id=instrument_id,
                bar_type_strs=bar_type_strs_to_load,
                venue=instrument_id.venue,
                starting_capital=total_capital,
                start_date=start_date,
                end_date=end_date,
                trader_id=f"GROUP-{group_index:03d}",
                oms_type="HEDGING",  # see _run_slot_group for rationale
                entry_start_time=default_entry_start_time,
                entry_end_time=default_entry_end_time,
                run_on_days=default_run_on_days,
                rbo_settings=default_rbo_settings,
            )
            node = BacktestNode(configs=[run_cfg])
            node.build()
            engine = node.get_engine(run_cfg.id)
            if engine is None:
                raise ValueError(
                    f"BacktestNode failed to build a group engine for "
                    f"{primary_bar_type_str} in range "
                    f"{start_date or 'start'}..{end_date or 'end'}"
                )

        with _phase("strategy_build", phase_times):
            for i, (slot, _capital) in enumerate(group):
                order_tag = f"{group_index:03d}-{i:03d}"
                expected_tags.append(order_tag)

                eff_squareoff_time = (
                    slot.exit_config.squareoff_time
                    or slot.squareoff_time
                    or default_squareoff_time
                )
                eff_squareoff_tz = (
                    slot.exit_config.squareoff_tz
                    or slot.squareoff_tz
                    or default_squareoff_tz
                )

                slot_qty = effective_slot_qty(slot, user_id)
                if slot.exit_config.has_exit_management() or eff_squareoff_time or default_rbo_settings is not None:
                    managed_config = config_from_exit(
                        exit_config=slot.exit_config,
                        signal_name=slot.strategy_name,
                        signal_params=slot.strategy_params,
                        instrument_id=instrument_id,
                        bar_type=primary_bt,
                        trade_size=slot_qty,
                        order_id_tag=order_tag,
                        squareoff_time=eff_squareoff_time,
                        squareoff_tz=eff_squareoff_tz,
                        rbo_settings=default_rbo_settings,
                        other_settings=default_other_settings,
                        move_sl_settings=default_move_sl_settings,
                        # Intraday entry window — only passed (so the strategy
                        # gates entries internally) when the whole group is
                        # managed and its post-window bars were kept.
                        entry_start_time=(default_entry_start_time
                                          if (_group_all_managed
                                              and _is_intraday_bar_type(primary_bar_type_str))
                                          else None),
                        entry_end_time=(default_entry_end_time
                                        if (_group_all_managed
                                            and _is_intraday_bar_type(primary_bar_type_str))
                                        else None),
                        # Shared-engine group: all slots in this process share
                        # one cross-slot bus keyed by the portfolio name.
                        subscribe_bar_types=getattr(slot, "strategy_bar_types", None),
                        portfolio_id=portfolio_name,
                        slot_id=slot.slot_id,
                    )
                    strategy = ManagedExitStrategy(managed_config)
                else:
                    if slot.strategy_name not in registry:
                        raise ValueError(f"Unknown strategy: {slot.strategy_name}")
                    registry_entry = registry[slot.strategy_name]
                    config_class = registry_entry["config_class"]
                    valid_param_keys = set(registry_entry["params"].keys())
                    filtered_params = {k: v for k, v in slot.strategy_params.items() if k in valid_param_keys}
                    config_kwargs = {
                        "instrument_id": instrument_id,
                        "bar_type": primary_bt,
                        "trade_size": Decimal(str(slot_qty)),
                        "order_id_tag": order_tag,
                        **filtered_params,
                    }
                    _agg_to = _aggregate_target_for_slot(slot, primary_bt)
                    if _agg_to and _config_supports_aggregate_to(config_class):
                        config_kwargs["aggregate_to_bar_type"] = _agg_to
                    strategy_config = config_class(**config_kwargs)
                    strategy = registry_entry["strategy_class"](strategy_config)

                engine.add_strategy(strategy)

        with _phase("engine_run", phase_times):
            node.run()

        with _phase("fx_resolver_build", phase_times):
            adapter_cfg = load_adapter_config_for_bar_type(primary_bar_type_str)
            fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)

        with _phase("extract_results", phase_times):
            fills_report = None
            try:
                fills_report = engine.trader.generate_order_fills_report()
            except Exception:
                pass
            positions_report = None
            try:
                positions_report = engine.trader.generate_positions_report()
            except Exception:
                pass

            actual_strategies = engine.trader.strategies()
            actual_strategy_ids = [str(s.id) for s in actual_strategies]

            slot_results: list[dict] = []
            group_elapsed = round(_time.time() - _t_group_start, 3)
            for i, (slot, capital) in enumerate(group):
                strategy_id = (
                    actual_strategy_ids[i] if i < len(actual_strategy_ids)
                    else f"ManagedExitStrategy-{expected_tags[i]}"
                )
                r = _extract_slot_from_group_reports(
                    positions_report, fills_report, strategy_id, slot, capital, fx_resolver,
                )
                r["elapsed_seconds"] = group_elapsed
                r["worker_pid"] = os.getpid()
                r["group_index"] = group_index
                r["group_size"] = len(group)
                r["group_strategy_id"] = strategy_id
                r["path_b"] = True
                if phase_times is not None:
                    r["phase_times"] = {k: round(v, 4) for k, v in phase_times.items()}

                ci = _cached_catalog_bars.cache_info()
                r["cache_hits"] = ci.hits
                r["cache_misses"] = ci.misses
                r["cache_currsize"] = ci.currsize
                try:
                    import psutil as _psutil
                    r["worker_rss_mb"] = round(_psutil.Process(os.getpid()).memory_info().rss / 1e6, 1)
                except Exception:
                    r["worker_rss_mb"] = None

                if missing_pairs:
                    r["warning"] = (
                        f"ASK/BID bar data not found in catalog ({', '.join(missing_pairs)}). "
                        f"Fills will use MID prices — spread cost is not reflected in results."
                    )

                slot_results.append(r)
    finally:
        if node is not None:
            try:
                node.dispose()
            except BaseException:
                pass

    return slot_results


def _run_slot_group(
    catalog_path: str,
    group: list,
    custom_strategies_dir: str | None,
    group_index: int,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
    default_squareoff_time: str | None = None,
    default_squareoff_tz: str | None = None,
    default_run_on_days: list | None = None,
    default_entry_start_time: str | None = None,
    default_entry_end_time: str | None = None,
    default_rbo_settings: "_RBOSettings | None" = None,
    default_other_settings: "_OtherSettings | None" = None,
    default_move_sl_settings: "_MoveSLConfig | None" = None,
    user_id: str | None = None,
    portfolio_name: str = "",
    default_replay_cutoff_ns: int = 0,
    default_vwap_fill: bool = False,
) -> list[dict]:
    """Run a group of slots sharing (bar_type, date_range) in ONE BacktestEngine.

    Each slot becomes a strategy instance with its own ``order_id_tag`` so
    Nautilus assigns it a unique ``strategy_id``. Orders route through the
    shared account (safe because strategies use fixed ``trade_size``, not
    balance-derived sizing). Per-slot P&L is extracted post-run by filtering
    ``positions_report`` on ``strategy_id``.

    Returns a list of per-slot result dicts in the same shape as
    ``_run_single_slot`` output — ``_merge_portfolio_results`` can consume
    them identically.

    Size-1 groups are allowed but callers are free to short-circuit to
    ``_run_single_slot`` for that case.
    """
    # Path B opt-in. Group's primary bar_type and date range are shared
    # across all slots by construction (groups are formed by
    # (bar_type, date_range)), so it's safe to use the primary slot's values
    # for both the non-intraday exemption and the chunking date bounds.
    _primary_slot = group[0][0] if group else None
    _group_primary_bar_type = _primary_slot.bar_type_str if _primary_slot else None
    _gate_start_date = (_primary_slot.start_date if _primary_slot else None) or default_start_date
    _gate_end_date = (_primary_slot.end_date if _primary_slot else None) or default_end_date
    if _path_b_active() and _path_b_supports_filters(
        default_run_on_days, default_entry_start_time, default_entry_end_time,
        bar_type_str=_group_primary_bar_type,
        start_date=_gate_start_date,
        end_date=_gate_end_date,
    ):
        return _run_slot_group_node(
            catalog_path=catalog_path,
            group=group,
            custom_strategies_dir=custom_strategies_dir,
            group_index=group_index,
            default_start_date=default_start_date,
            default_end_date=default_end_date,
            default_squareoff_time=default_squareoff_time,
            default_squareoff_tz=default_squareoff_tz,
            default_run_on_days=default_run_on_days,
            default_entry_start_time=default_entry_start_time,
            default_entry_end_time=default_entry_end_time,
            default_rbo_settings=default_rbo_settings,
            default_other_settings=default_other_settings,
            default_move_sl_settings=default_move_sl_settings,
            user_id=user_id,
            portfolio_name=portfolio_name,
        )

    import os
    import time as _time
    _t_group_start = _time.time()

    phase_times: dict | None = {} if os.environ.get("_PROFILE_PHASES") == "1" else None

    if not group:
        return []

    # All slots in the group share bar_type + date range (by construction)
    primary_slot = group[0][0]
    primary_bar_type_str = primary_slot.bar_type_str
    start_date = primary_slot.start_date or default_start_date
    end_date = primary_slot.end_date or default_end_date

    with _phase("registry_load", phase_times):
        if custom_strategies_dir:
            from core.custom_strategy_loader import get_merged_registry
            registry, _ = get_merged_registry(Path(custom_strategies_dir))
        else:
            registry = STRATEGY_REGISTRY

    with _phase("catalog_init", phase_times):
        catalog = ParquetDataCatalog(catalog_path)

    with _phase("instruments_scan", phase_times):
        instrument_map = {inst.id: inst for inst in catalog.instruments()}

    with _phase("bars_load", phase_times):
        # BID/ASK auto-pair (same as _run_single_slot): Nautilus's matching
        # engine needs the opposite quote side to fill FX market orders.
        bar_type_strs_to_load = [primary_bar_type_str]
        paired_strs = _pair_bid_ask_bar_type(primary_bar_type_str)
        bar_type_strs_to_load.extend(paired_strs)
        all_bars = []
        missing_pairs: list[str] = []
        for bt_str in bar_type_strs_to_load:
            try:
                cached = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
            except Exception:
                cached = []
                if bt_str in paired_strs:
                    missing_pairs.append(bt_str)
            if not cached and bt_str in paired_strs:
                missing_pairs.append(bt_str)
            all_bars.extend(cached)
        missing_pairs = list(dict.fromkeys(missing_pairs))

    # Day-of-week filter mirroring _run_single_slot. NO LONGER pre-filters —
    # bars stay continuous so internal aggregators don't see gaps; each slot's
    # strategy enforces the filter via ManagedExitConfig.allowed_weekdays
    # (passed through config_from_exit below). See _run_single_slot for the
    # full rationale.
    allowed_weekdays = _allowed_weekdays(default_run_on_days)
    bars_filtered_by_run_on_days = 0  # always 0 now; kept for result-dict compat

    # Portfolio ReExecute replay cutoff (spec §2.4) — mirrors _run_single_slot.
    if default_replay_cutoff_ns > 0:
        all_bars, _bars_dropped_pre_cutoff = _filter_bars_after_ns(
            all_bars, default_replay_cutoff_ns
        )

    # Intra-day entry window filter (mirrors _run_single_slot). Skipped when
    # the group's primary bar type is non-intraday (daily/weekly/monthly).
    # The END side is pre-dropped only when the group is NOT entirely managed
    # — if every slot is managed, post-window bars are kept so each strategy
    # can gate entries internally while still monitoring exits (spec §9).
    bars_filtered_by_entry_window = 0
    entry_window_skipped_reason: str | None = None
    _group_all_managed = all(
        (slot.exit_config.has_exit_management()
         or slot.exit_config.squareoff_time or slot.squareoff_time
         or default_squareoff_time or default_rbo_settings is not None)
        for slot, _cap in group
    )
    with _phase("entry_window_filter", phase_times):
        if (default_entry_start_time or default_entry_end_time) and not _is_intraday_bar_type(primary_bar_type_str):
            entry_window_skipped_reason = (
                f"bar type {primary_bar_type_str} is not intraday — entry window ignored"
            )
        else:
            _filter_end = None if _group_all_managed else default_entry_end_time
            all_bars, bars_filtered_by_entry_window = _filter_bars_by_time_of_day(
                all_bars, default_entry_start_time, _filter_end
            )

    if not all_bars:
        msg = (
            f"No bars in date range {start_date or 'start'}..{end_date or 'end'} "
            f"for group primary bar_type {primary_bar_type_str}"
        )
        extra = []
        if bars_filtered_by_run_on_days:
            extra.append(f"run_on_days dropped {bars_filtered_by_run_on_days}")
        if bars_filtered_by_entry_window:
            extra.append(f"entry window dropped {bars_filtered_by_entry_window}")
        if extra:
            msg += f" (after filters: {'; '.join(extra)})"
        raise ValueError(msg)

    primary_bt = BarType.from_str(primary_bar_type_str)
    instrument_id = primary_bt.instrument_id
    instrument = instrument_map.get(instrument_id)
    if instrument is None:
        raise ValueError(f"No instrument found for {primary_bar_type_str}")

    from nautilus_trader.config import RiskEngineConfig

    # try/finally guarantees engine.dispose() even on BaseException (SystemExit,
    # CancelledError). Workers ignore SIGINT via the pool initializer, so KI
    # shouldn't fire here, but this is the belt-and-suspenders contract.
    engine = None
    try:
        with _phase("engine_build", phase_times):
            engine = BacktestEngine(config=BacktestEngineConfig(
                trader_id=TraderId(f"GROUP-{group_index:03d}"),
                logging=LoggingConfig(bypass_logging=True),
                risk_engine=RiskEngineConfig(bypass=True),
                run_analysis=False,
            ))
            total_capital = float(sum(capital for _, capital in group))
            venue = instrument_id.venue
            # HEDGING (not NETTING) so each strategy's positions are tracked
            # independently. NETTING would merge all strategies' orders on the
            # same (venue, instrument) into a single position record — breaks
            # per-strategy round-trip accounting.
            engine.add_venue(
                venue=venue,
                oms_type=OmsType.HEDGING,
                account_type=AccountType.MARGIN,
                starting_balances=[Money(total_capital, USD)],
                base_currency=USD,
                default_leverage=Decimal(1),
            )
            engine.add_instrument(instrument)
            engine.add_data(all_bars)
            # VWAP proxy-fill (spec §4.2): index ASK/BID bars before the list
            # is dropped, so each slot's _extract_slot_from_group_reports can
            # reprice SL/Target exits. None when the flag is off / no ASK-BID.
            vwap_lookup = (
                _build_vwap_lookup(all_bars)
                if (default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1")
                else None
            )
            del all_bars

        # Build and attach N strategies with deterministic unique order_id_tags
        expected_tags: list[str] = []
        with _phase("strategy_build", phase_times):
            for i, (slot, _capital) in enumerate(group):
                order_tag = f"{group_index:03d}-{i:03d}"
                expected_tags.append(order_tag)

                # Resolve effective squareoff per slot — same priority chain as
                # _run_single_slot: leg > slot > portfolio default.
                eff_squareoff_time = (
                    slot.exit_config.squareoff_time
                    or slot.squareoff_time
                    or default_squareoff_time
                )
                eff_squareoff_tz = (
                    slot.exit_config.squareoff_tz
                    or slot.squareoff_tz
                    or default_squareoff_tz
                )

                slot_qty = effective_slot_qty(slot, user_id)
                # Force ManagedExitStrategy when a run_on_days filter is active
                # (gate lives in the wrapper). Mirrors the wrapper-condition in
                # _run_single_slot.
                if (slot.exit_config.has_exit_management() or eff_squareoff_time
                        or default_rbo_settings is not None
                        or allowed_weekdays is not None):
                    managed_config = config_from_exit(
                        exit_config=slot.exit_config,
                        signal_name=slot.strategy_name,
                        signal_params=slot.strategy_params,
                        instrument_id=instrument_id,
                        bar_type=primary_bt,
                        trade_size=slot_qty,
                        order_id_tag=order_tag,
                        squareoff_time=eff_squareoff_time,
                        squareoff_tz=eff_squareoff_tz,
                        rbo_settings=default_rbo_settings,
                        other_settings=default_other_settings,
                        move_sl_settings=default_move_sl_settings,
                        # Day-of-week filter (portfolio.run_on_days) — gated
                        # inside the strategy so bars stay continuous for the
                        # internal aggregator. Same pattern as _run_single_slot.
                        allowed_weekdays=(None if allowed_weekdays is None
                                          else sorted(allowed_weekdays)),
                        # Shared-engine group: all slots in this process share
                        # one cross-slot bus keyed by the portfolio name.
                        subscribe_bar_types=getattr(slot, "strategy_bar_types", None),
                        portfolio_id=portfolio_name,
                        slot_id=slot.slot_id,
                    )
                    strategy = ManagedExitStrategy(managed_config)
                else:
                    if slot.strategy_name not in registry:
                        raise ValueError(f"Unknown strategy: {slot.strategy_name}")

                    registry_entry = registry[slot.strategy_name]
                    config_class = registry_entry["config_class"]
                    valid_param_keys = set(registry_entry["params"].keys())
                    filtered_params = {k: v for k, v in slot.strategy_params.items() if k in valid_param_keys}

                    config_kwargs = {
                        "instrument_id": instrument_id,
                        "bar_type": primary_bt,
                        "trade_size": Decimal(str(slot_qty)),
                        "order_id_tag": order_tag,
                        **filtered_params,
                    }
                    _agg_to = _aggregate_target_for_slot(slot, primary_bt)
                    if _agg_to and _config_supports_aggregate_to(config_class):
                        config_kwargs["aggregate_to_bar_type"] = _agg_to
                    strategy_config = config_class(**config_kwargs)
                    strategy = registry_entry["strategy_class"](strategy_config)

                engine.add_strategy(strategy)

        with _phase("engine_run", phase_times):
            engine.run()

        with _phase("fx_resolver_build", phase_times):
            # Single FX resolver for the whole group — same bar_type → same venue → same adapter cfg
            adapter_cfg = load_adapter_config_for_bar_type(primary_bar_type_str)
            fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)

        with _phase("extract_results", phase_times):
            fills_report = None
            try:
                fills_report = engine.trader.generate_order_fills_report()
            except Exception:
                pass
            positions_report = None
            try:
                positions_report = engine.trader.generate_positions_report()
            except Exception:
                pass

            # Fetch actual strategy_ids from the engine in insertion order (confirmed
            # via nautilus_trader/trading/trader.py — strategies() returns dict values
            # which preserve insertion order).
            actual_strategies = engine.trader.strategies()
            actual_strategy_ids = [str(s.id) for s in actual_strategies]

            slot_results: list[dict] = []
            group_elapsed = round(_time.time() - _t_group_start, 3)
            for i, (slot, capital) in enumerate(group):
                strategy_id = (
                    actual_strategy_ids[i] if i < len(actual_strategy_ids)
                    else f"ManagedExitStrategy-{expected_tags[i]}"
                )
                r = _extract_slot_from_group_reports(
                    positions_report, fills_report, strategy_id, slot, capital, fx_resolver,
                    vwap_lookup,
                )
                # Per-leg SL/target hit timestamps (spec §2.3) — read from the
                # strategy instance (same insertion index as strategy_id) so
                # the two-pass aggregate-Move-SL runner can pre-seed pass 2.
                if i < len(actual_strategies):
                    r["leg_exit_events"] = dict(
                        getattr(actual_strategies[i], "_exit_events_self", {}) or {}
                    )
                r["elapsed_seconds"] = group_elapsed  # group-level wall time; per-slot isn't meaningful in a shared run
                r["worker_pid"] = os.getpid()
                r["group_index"] = group_index
                r["group_size"] = len(group)
                r["group_strategy_id"] = strategy_id
                if phase_times is not None:
                    r["phase_times"] = {k: round(v, 4) for k, v in phase_times.items()}

                # Per-worker cache telemetry (same as _run_single_slot)
                ci = _cached_catalog_bars.cache_info()
                r["cache_hits"] = ci.hits
                r["cache_misses"] = ci.misses
                r["cache_currsize"] = ci.currsize
                try:
                    import psutil as _psutil
                    r["worker_rss_mb"] = round(_psutil.Process(os.getpid()).memory_info().rss / 1e6, 1)
                except Exception:
                    r["worker_rss_mb"] = None

                if missing_pairs:
                    r["warning"] = (
                        f"ASK/BID bar data not found in catalog ({', '.join(missing_pairs)}). "
                        f"Fills will use MID prices — spread cost is not reflected in results."
                    )

                if default_run_on_days is not None:
                    r["run_on_days"] = list(default_run_on_days)
                    r["bars_filtered_by_run_on_days"] = bars_filtered_by_run_on_days

                if default_entry_start_time or default_entry_end_time:
                    r["entry_start_time"] = default_entry_start_time
                    r["entry_end_time"] = default_entry_end_time
                    r["bars_filtered_by_entry_window"] = bars_filtered_by_entry_window
                    if entry_window_skipped_reason:
                        r["entry_window_skipped"] = entry_window_skipped_reason
                        _existing = r.get("warning")
                        _add = f"Entry window not applied: {entry_window_skipped_reason}."
                        r["warning"] = f"{_existing} {_add}" if _existing else _add

                slot_results.append(r)
    finally:
        if engine is not None:
            try:
                engine.dispose()
            except BaseException:
                pass

    return slot_results


def _positions_pnl_series(df) -> "pd.Series":
    """Numeric realized-PnL series for a positions report.

    Prefers the FX-converted base-currency column added by
    ``positions_report_with_base`` (``realized_pnl_<CCY>``); falls back to
    parsing the Nautilus Money-string ``realized_pnl`` ("X.XX CCY").
    Returns an empty float Series when the report has no usable column.
    """
    if df is None or df.empty:
        return pd.Series([], dtype=float)
    base_col = next(
        (c for c in df.columns if c.startswith("realized_pnl_") and c != "realized_pnl_"),
        None,
    )
    if base_col is not None:
        return pd.to_numeric(df[base_col], errors="coerce").fillna(0.0)
    if "realized_pnl" in df.columns:
        return df["realized_pnl"].map(
            lambda x: float(str(x).split(" ")[0]) if x and " " in str(x) else 0.0
        )
    return pd.Series([0.0] * len(df), dtype=float)


def _per_strategy_breakdown(positions, slot_to_strategy_id: dict | None) -> dict:
    """Per-slot ``{pnl, trades, wins, losses, trade_pnls}`` from a positions
    report, keyed by slot_id.

    Used by the ReExecute splice to recover each slot's *pre-clip* stats from
    the truncated head positions report — ``positions`` rows carry the run's
    ``strategy_id``, which ``slot_to_strategy_id`` maps back to a slot_id.
    """
    out: dict = {}
    if positions is None or positions.empty or "strategy_id" not in positions.columns:
        return out
    inv = {str(sid): slot for slot, sid in (slot_to_strategy_id or {}).items()}
    pnl = _positions_pnl_series(positions).reset_index(drop=True)
    sids = positions["strategy_id"].astype(str).reset_index(drop=True)
    for i in range(len(sids)):
        slot_id = inv.get(sids.iloc[i])
        if slot_id is None:
            continue
        p = float(pnl.iloc[i])
        d = out.setdefault(slot_id, {"pnl": 0.0, "trades": 0, "wins": 0,
                                     "losses": 0, "trade_pnls": []})
        d["pnl"] += p
        d["trades"] += 1
        d["trade_pnls"].append(p)
        if p > 0:
            d["wins"] += 1
        elif p < 0:
            d["losses"] += 1
    return out


def _splice_merged_results(head: dict, tail: dict, clip_ns: int,
                           starting_capital: float) -> dict:
    """Splice a pass-1 merged result (``head``) with a ReExecute replay
    segment (``tail``) at ``clip_ns``.

    The head contributes every fill/position strictly before the clip; the
    tail — already bar-cutoff-filtered to start flat at the clip — contributes
    the whole post-clip regime. Top-level portfolio metrics (PnL, trades,
    win/loss, equity curve, drawdown) AND the per-slot ``per_strategy`` block
    are re-derived from the spliced reports — the head's pre-clip per-slot
    stats (mapped via its ``slot_to_strategy_id``) plus the tail segment's.
    """
    def _before(df, col):
        if df is None or getattr(df, "empty", True) or col not in df.columns:
            return df.iloc[0:0] if df is not None else pd.DataFrame()
        ts_int = pd.to_datetime(df[col], errors="coerce", utc=True).astype("int64")
        return df.loc[ts_int < clip_ns]

    hf = _before(head.get("fills_report"), "ts_init")
    hp = _before(head.get("positions_report"), "ts_opened")
    tf = tail.get("fills_report")
    tp = tail.get("positions_report")
    fills = pd.concat([d for d in (hf, tf) if d is not None and not d.empty],
                      ignore_index=True) if (hf is not None or tf is not None) else pd.DataFrame()
    positions = pd.concat([d for d in (hp, tp) if d is not None and not d.empty],
                          ignore_index=True) if (hp is not None or tp is not None) else pd.DataFrame()

    pnl = _positions_pnl_series(positions)
    total_pnl = float(pnl.sum())
    total_trades = int(len(positions))
    wins = int((pnl > 0).sum())
    losses = int((pnl < 0).sum())
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0.0
    final_balance = starting_capital + total_pnl
    total_return_pct = (total_pnl / starting_capital * 100) if starting_capital > 0 else 0.0

    # Equity-curve splice: head points before the clip, then the tail curve
    # shifted to continue from the head's balance at the clip.
    head_eq = head.get("equity_curve_ts") or []
    tail_eq = tail.get("equity_curve_ts") or []
    kept_head = [p for p in head_eq
                 if p.get("timestamp") is None or _ts_iso_to_ns(p["timestamp"]) < clip_ns]
    head_bal_at_clip = next(
        (p["balance"] for p in reversed(kept_head) if p.get("timestamp") is not None),
        starting_capital,
    )
    shift = head_bal_at_clip - starting_capital
    spliced_eq = list(kept_head) + [
        {"timestamp": p["timestamp"], "balance": float(p.get("balance", starting_capital)) + shift}
        for p in tail_eq if p.get("timestamp") is not None
    ]
    balances = [float(p.get("balance", starting_capital)) for p in spliced_eq]
    # Running-peak drawdown over the spliced balance series.
    max_dd = 0.0
    peak = balances[0] if balances else starting_capital
    for b in balances:
        peak = max(peak, b)
        if peak > 0:
            max_dd = max(max_dd, (peak - b) / peak * 100)

    # Per-slot breakdown: head's pre-clip stats (recovered from the truncated
    # head positions via its slot_to_strategy_id) + the tail segment's stats.
    head_pre = _per_strategy_breakdown(hp, head.get("slot_to_strategy_id"))
    head_per = head.get("per_strategy") or {}
    tail_per = tail.get("per_strategy") or {}
    combined_per: dict = {}
    for slot_id in set(head_pre) | set(tail_per):
        h = head_pre.get(slot_id)
        t = tail_per.get(slot_id) or {}
        meta = tail_per.get(slot_id) or head_per.get(slot_id) or {}
        s_pnl = (h["pnl"] if h else 0.0) + float(t.get("pnl", 0.0))
        s_trades = (h["trades"] if h else 0) + int(t.get("trades", 0))
        s_wins = (h["wins"] if h else 0) + int(t.get("wins", 0))
        s_losses = (h["losses"] if h else 0) + int(t.get("losses", 0))
        s_tpnls = (h["trade_pnls"] if h else []) + list(t.get("trade_pnls", []))
        combined_per[slot_id] = {
            "display_name": meta.get("display_name", ""),
            "strategy_name": meta.get("strategy_name", ""),
            "bar_type": meta.get("bar_type", ""),
            "pnl": s_pnl,
            "trades": s_trades,
            "wins": s_wins,
            "losses": s_losses,
            "win_rate": (s_wins / s_trades * 100) if s_trades > 0 else 0.0,
            "trade_pnls": s_tpnls,
        }

    out = dict(tail)
    out.update(
        fills_report=fills,
        positions_report=positions,
        total_pnl=total_pnl,
        final_balance=final_balance,
        total_return_pct=total_return_pct,
        total_trades=total_trades,
        wins=wins,
        losses=losses,
        win_rate=win_rate,
        equity_curve_ts=spliced_eq,
        equity_curve=balances,
        max_drawdown=max_dd,
        per_strategy=combined_per,
    )
    return out


def run_portfolio_backtest(
    catalog_path: str,
    portfolio: PortfolioConfig,
    custom_strategies_dir: str | None = None,
    on_slot_complete=None,
    user_id: str | None = None,
) -> dict:
    """
    Run a portfolio backtest with multiple strategy slots in parallel.

    Each slot runs in its own engine with allocated capital.
    Results are merged into portfolio-level metrics.

    ``user_id`` threads through to ``effective_slot_qty`` so the per-user
    multiplier (from ``config/users.json``) scales every slot's order
    quantity. None preserves single-user behavior (multiplier=1.0).
    """
    import os
    from concurrent.futures import ProcessPoolExecutor, as_completed

    # Reset the cross-slot event bus for this portfolio so a prior run's
    # SL/target events don't leak into the new run (spec §3 1.3(f)/(g)).
    from core.managed_strategy import clear_cross_slot_bus
    pf_name = getattr(portfolio, "name", "") or "_standalone_"
    clear_cross_slot_bus(pf_name)

    # Cross-portfolio dispatch consumption (spec §2.1(h)/(i)/(j) + §2.4
    # mirror). Pending events were written by an earlier portfolio's clip.
    pending_events = consume_cross_portfolio_events(pf_name)
    pending_sqoff = any(e["action"] == "sqoff" for e in pending_events)
    pending_execute = any(e["action"] == "execute" for e in pending_events)
    if pending_events:
        for e in pending_events:
            print(f"[XPF] {pf_name!r} consumed {e['action']!r} from clip @ {e.get('ts')}")
    if pending_sqoff:
        # SqOff Other Portfolio from a prior run: short-circuit this run.
        # Return a zero-trade result that downstream callers handle the same
        # way they handle an immediately-clipped portfolio.
        print(f"[XPF] {pf_name!r} suppressed by SqOff Other Portfolio event")
        return {
            "portfolio_name": pf_name,
            "starting_capital": getattr(portfolio, "starting_capital", 0.0),
            "final_balance": getattr(portfolio, "starting_capital", 0.0),
            "total_pnl": 0.0,
            "total_trades": 0,
            "wins": 0, "losses": 0,
            "max_loss_hit": False, "max_profit_hit": False,
            "per_strategy": {},
            "cross_portfolio_dispatch": {"suppressed_by": "sqoff", "events": pending_events},
        }
    # "execute"/"start" verbs just confirm normal run; armed_at_start handles
    # per-slot activation. The flag is surfaced in results for observability.

    enabled_slots = portfolio.enabled_slots

    if not enabled_slots:
        raise ValueError("No enabled strategy slots in portfolio")

    # Winter Time Adjustment (spec execution_logic_target.html §9). Shift all
    # configured local times +1h in place BEFORE resolving RBO / square-off /
    # entry-window, so every downstream path sees the adjusted values.
    if _apply_winter_time(portfolio):
        print(f"[WINTER] {pf_name!r}: configured times shifted +1h (winter_time_adjust)")

    # Resolve RBO once at the orchestrator. Failures fall back to standard
    # time-based entry per spec (rbo_logics.html validation rules); we surface
    # the message via print so it shows up in worker output even when the
    # caller doesn't pipe a logger.
    rbo_settings, rbo_msg = _resolve_rbo(portfolio)
    if rbo_msg:
        if rbo_settings is None:
            print(f"[RBO] disabled: {rbo_msg}")
        else:
            print(f"[RBO] warning: {rbo_msg}")

    # Resolve Other Settings (delay_between_legs, on_sl_action_on,
    # on_target_action_on, plus options-only fields). Spec:
    # 5. Logics/Other_Settings_Logic.html.
    other_settings, other_warnings = _resolve_other_settings(portfolio)
    for w in other_warnings:
        print(f"[OTHER] {w}")

    # Resolve Move SL to Cost (per-slot adaptation). Threaded into
    # ManagedExitConfig via config_from_exit alongside other slot params.
    # Spec: 5. Logics/portfolio_sl_tgt.html §3.
    move_sl_settings, move_sl_warnings = _resolve_move_sl_to_cost(portfolio)
    for w in move_sl_warnings:
        print(f"[MOVE_SL] {w}")

    # Conservative VWAP exit-fill model (spec §4.2 / §8.1). Enabled per-portfolio
    # via the saved config, or globally via the _USE_VWAP_FILL dev/parity flag.
    # Threaded into every slot worker as default_vwap_fill.
    vwap_fill_enabled = bool(getattr(portfolio, "vwap_exit_fill", False)) or (
        os.environ.get("_USE_VWAP_FILL", "0") == "1"
    )
    if vwap_fill_enabled:
        print(f"[VWAP_FILL] {pf_name!r}: conservative VWAP exit-fill enabled")

    # Calculate capital allocation per slot
    n = len(enabled_slots)
    capitals = {}
    if portfolio.allocation_mode == "percentage":
        for slot in enabled_slots:
            pct = slot.allocation_pct if slot.allocation_pct > 0 else (100.0 / n)
            capitals[slot.slot_id] = portfolio.starting_capital * pct / 100.0
    else:  # equal
        per_slot = portfolio.starting_capital / n
        for slot in enabled_slots:
            capitals[slot.slot_id] = per_slot

    # Run all slots in parallel (executor block lives in _run_all_slots below).

    # Raise cap from 8 → 32 so 16-core boxes actually utilize their cores.
    max_workers = min(n, (os.cpu_count() or 2), 32)

    # LPT scheduling: submit longest-expected slots first so shorter ones
    # can tail-fill behind them, minimizing max-worker-runtime imbalance.
    # History-aware: if we've run (bar_type, strategy) before, use the
    # observed per-day runtime instead of the span heuristic — this fixes
    # the USDJPY-tail case where three pairs share a span but one is 10x
    # heavier due to trade volume.
    from core import runtime_history
    history = runtime_history.load()

    def _span_days(slot):
        s = slot.start_date or portfolio.start_date
        e = slot.end_date or portfolio.end_date
        if s and e:
            try:
                return max(1, (pd.Timestamp(e) - pd.Timestamp(s)).days)
            except Exception:
                return 1
        return 1

    def _duration_estimate(slot):
        span = _span_days(slot)
        hist = runtime_history.estimate(history, slot.bar_type_str, slot.strategy_name, span)
        if hist is not None:
            return hist
        # Bollinger Bands empirically ran 15-20% slower than EMA/RSI in benchmarks.
        mult = 1.2 if "bollinger" in slot.strategy_name.lower() else 1.0
        return span * mult

    # Direction B: group slots that share (bar_type, start, end, custom_strategies_dir)
    # and submit one future per group. Size-1 groups still run via _run_single_slot
    # (zero-behavior-change fallback). Size-≥2 groups run in a shared engine via
    # _run_slot_group. Gated behind _USE_GROUPING env flag for safe rollout.
    use_grouping = os.environ.get("_USE_GROUPING", "0") == "1"

    # Capture each slot's underlying price series only when the portfolio uses
    # an underlying-price-based SL or Target type (spec §2.1 / §5.1) — otherwise
    # the result dict stays lean. Resolved here so it threads into slot workers.
    _capture_underlying = (
        bool(getattr(portfolio, "pf_sl_enabled", False))
        and str(getattr(portfolio, "pf_sl_type", "") or "") in _UNDERLYING_PF_SL_TYPES
    ) or (
        bool(getattr(portfolio, "pf_tgt_enabled", False))
        and str(getattr(portfolio, "pf_tgt_type", "") or "") in _UNDERLYING_PF_TGT_TYPES
    )

    if use_grouping:
        groups = _group_slots(
            enabled_slots, capitals,
            default_start_date=portfolio.start_date,
            default_end_date=portfolio.end_date,
            custom_strategies_dir=custom_strategies_dir,
        )
        # LPT at group level — sum member-slot durations so the longest group submits first
        def _group_duration(grp):
            return sum(_duration_estimate(slot) for slot, _cap in grp)
        sorted_groups = sorted(groups, key=_group_duration, reverse=True)
        # max_workers capped by n_groups (no point spawning more workers than groups)
        max_workers = min(len(sorted_groups), (os.cpu_count() or 2), 32)
    else:
        sorted_slots = sorted(enabled_slots, key=_duration_estimate, reverse=True)

    def _run_all_slots(active_move_sl, fire_callbacks: bool, replay_cutoff_ns: int = 0):
        """Submit every slot/group to a fresh ProcessPoolExecutor and collect
        results into ``{slot_id: result}``.

        The two-pass aggregate-Move-SL feature (spec §2.3) calls this twice
        with a different ``active_move_sl``; a normal run calls it once.
        ``fire_callbacks`` gates ``on_slot_complete`` and runtime-history
        recording so only the final (reported) pass drives the UI / history.
        ``replay_cutoff_ns`` > 0 makes every slot start flat at that timestamp
        — used by the portfolio ReExecute replay (spec §2.4).
        Returns ``(slot_results, errors)``.
        """
        slot_results: dict = {}
        errors: list = []
        # Portfolio-level squareoff routed through the helper so MIS product
        # type can supply a default when no explicit squareoff_time is set.
        # Slot/leg overrides still win at resolve-time inside the slot worker.
        _pf_sq_time, _pf_sq_tz = effective_portfolio_squareoff(portfolio)
        with ProcessPoolExecutor(
            max_workers=max_workers,
            initializer=_worker_init_ignore_sigint,
        ) as executor:
            futures = {}

            if use_grouping:
                # One future per group. Size-1 groups route to _run_single_slot (unchanged path);
                # size-≥2 groups route to _run_slot_group (new shared-engine path).
                for group_idx, group in enumerate(sorted_groups):
                    if len(group) == 1:
                        slot, capital = group[0]
                        future = executor.submit(
                            _run_single_slot,
                            catalog_path=catalog_path,
                            slot=slot,
                            capital=capital,
                            custom_strategies_dir=custom_strategies_dir,
                            slot_index=group_idx,
                            default_start_date=portfolio.start_date,
                            default_end_date=portfolio.end_date,
                            default_squareoff_time=_pf_sq_time,
                            default_squareoff_tz=_pf_sq_tz,
                            default_run_on_days=portfolio.run_on_days,
                            default_entry_start_time=portfolio.entry_start_time,
                            default_entry_end_time=portfolio.entry_end_time,
                            default_rbo_settings=rbo_settings,
                            default_other_settings=other_settings,
                            default_move_sl_settings=active_move_sl,
                            user_id=user_id,
                            default_capture_underlying=_capture_underlying,
                            default_replay_cutoff_ns=replay_cutoff_ns,
                            default_vwap_fill=vwap_fill_enabled,
                        )
                        futures[future] = ("single", [slot])
                    else:
                        future = executor.submit(
                            _run_slot_group,
                            catalog_path=catalog_path,
                            group=group,
                            custom_strategies_dir=custom_strategies_dir,
                            group_index=group_idx,
                            default_start_date=portfolio.start_date,
                            default_end_date=portfolio.end_date,
                            default_squareoff_time=_pf_sq_time,
                            default_squareoff_tz=_pf_sq_tz,
                            default_run_on_days=portfolio.run_on_days,
                            default_entry_start_time=portfolio.entry_start_time,
                            default_entry_end_time=portfolio.entry_end_time,
                            default_rbo_settings=rbo_settings,
                            default_other_settings=other_settings,
                            default_move_sl_settings=active_move_sl,
                            user_id=user_id,
                            portfolio_name=pf_name,
                            default_replay_cutoff_ns=replay_cutoff_ns,
                            default_vwap_fill=vwap_fill_enabled,
                        )
                        futures[future] = ("group", [slot for slot, _cap in group])
            else:
                for i, slot in enumerate(sorted_slots):
                    future = executor.submit(
                        _run_single_slot,
                        catalog_path=catalog_path,
                        slot=slot,
                        capital=capitals[slot.slot_id],
                        custom_strategies_dir=custom_strategies_dir,
                        slot_index=i,
                        default_start_date=portfolio.start_date,
                        default_end_date=portfolio.end_date,
                        default_squareoff_time=_pf_sq_time,
                        default_squareoff_tz=_pf_sq_tz,
                        default_run_on_days=portfolio.run_on_days,
                        default_entry_start_time=portfolio.entry_start_time,
                        default_entry_end_time=portfolio.entry_end_time,
                        default_rbo_settings=rbo_settings,
                        default_other_settings=other_settings,
                        default_move_sl_settings=active_move_sl,
                        user_id=user_id,
                        default_capture_underlying=_capture_underlying,
                        default_replay_cutoff_ns=replay_cutoff_ns,
                        default_vwap_fill=vwap_fill_enabled,
                    )
                    futures[future] = ("single", [slot])

            try:
                for future in as_completed(futures):
                    kind, slots_in_future = futures[future]
                    try:
                        result = future.result()
                        if kind == "group":
                            # _run_slot_group returns list[dict], one per slot in insertion order
                            for slot, r in zip(slots_in_future, result):
                                slot_results[slot.slot_id] = r
                                elapsed = r.get("elapsed_seconds")
                                if fire_callbacks and elapsed is not None:
                                    runtime_history.record(
                                        history, slot.bar_type_str, slot.strategy_name,
                                        float(elapsed), _span_days(slot),
                                    )
                                if fire_callbacks and on_slot_complete:
                                    try:
                                        on_slot_complete(slot.slot_id)
                                    except Exception:
                                        pass
                        else:
                            slot = slots_in_future[0]
                            slot_results[slot.slot_id] = result
                            elapsed = result.get("elapsed_seconds")
                            if fire_callbacks and elapsed is not None:
                                runtime_history.record(
                                    history, slot.bar_type_str, slot.strategy_name,
                                    float(elapsed), _span_days(slot),
                                )
                            if fire_callbacks and on_slot_complete:
                                try:
                                    on_slot_complete(slot.slot_id)
                                except Exception:
                                    pass
                    except Exception as e:
                        for slot in slots_in_future:
                            errors.append({
                                "slot_id": slot.slot_id,
                                "display_name": slot.display_name,
                                "error": str(e),
                            })
            except KeyboardInterrupt:
                # Parent main thread saw Ctrl+C. Cancel queued futures; in-flight
                # workers (which ignore SIGINT) finish their current engine.run()
                # and the pool drains cleanly. Re-raise so the caller sees the KI.
                executor.shutdown(wait=False, cancel_futures=True)
                raise
        return slot_results, errors

    # Two-pass portfolio-aggregate Move SL (spec §2.3). When active: pass 1
    # discovers the combined-P&L timeline + per-leg SL/target hits, the parent
    # computes the aggregate trigger ts and a cross-process event bus, then
    # pass 2 replays with those injected. ~2x runtime.
    #
    # Now a real per-portfolio feature: the two-pass runs whenever the
    # aggregate-P&L trigger or a cross-slot Hit-On-Leg trigger is configured on
    # the portfolio (cross-process Hit-On-Leg only works correctly via the
    # two-pass pre-seeded bus, since the in-process bus doesn't span workers).
    # When nothing is configured, agg_active stays False → single pass, zero
    # behavior change. The legacy _USE_PF_AGG_MOVE_SL env flag is still honoured
    # as a manual override for parity tooling.
    agg_active = (
        move_sl_settings.agg_pnl_enabled
        or move_sl_settings.hit_on_leg_sl
        or move_sl_settings.hit_on_leg_target
        or os.environ.get("_USE_PF_AGG_MOVE_SL", "0") == "1"
    )
    if not agg_active:
        slot_results, errors = _run_all_slots(move_sl_settings, fire_callbacks=True)
    else:
        print("[PF_AGG_MOVE_SL] two-pass active — running discovery pass 1")
        pass1_results, pass1_errors = _run_all_slots(move_sl_settings, fire_callbacks=False)
        coord = _compute_agg_coordination(portfolio, pass1_results, move_sl_settings)
        for log_line in coord.logs:
            print(f"[PF_AGG_MOVE_SL] {log_line}")
        # Pass-1 wrote live SL/target events into the per-process bus; clear it
        # so pass 2 starts from only the explicitly pre-seeded events.
        clear_cross_slot_bus(pf_name)
        move_sl_pass2 = dataclasses.replace(
            move_sl_settings,
            agg_trigger_ns=coord.agg_trigger_ns,
            preseeded_bus=coord.event_bus,
        )
        slot_results, errors = _run_all_slots(move_sl_pass2, fire_callbacks=True)
        # If pass 2 produced nothing, surface pass-1's errors for diagnostics.
        if not slot_results and not errors:
            errors = pass1_errors

    # Persist once after the whole run — cheap single JSON write.
    try:
        runtime_history.save(history)
    except Exception:
        pass

    if not slot_results and errors:
        raise ValueError(f"All slots failed: {errors}")

    # Merge results into portfolio-level metrics
    result = _merge_portfolio_results(portfolio, slot_results, capitals, errors, user_id=user_id)

    # ── Portfolio ReExecute replay (spec §2.4) ──────────────────────────────
    # When the portfolio SL/Target fires a ReExecute-family action (plain
    # ReExecute, "ReExecute at Entry Price", or "ReExecute Same Contract at
    # EntryPrice"), re-run every slot FLAT from the clip timestamp and splice
    # that segment onto the pre-clip trades — a genuine re-execution instead of
    # the default "keep trades" approximation. The segment is itself merged (so
    # it re-evaluates the portfolio limit) and the loop recurses on its first
    # ReExecute clip, up to the configured ReExecute count (0 = unlimited,
    # hard-capped at 50 to bound runtime).
    #
    # Now config-driven: the replay runs whenever a ReExecute-family action is
    # actually configured on the portfolio SL or Target (the entry-price
    # variants replay as plain ReExecute — the FX adaptation per spec, which
    # marks them ⚙️ since price-wait re-entry lives at the leg level §1.2(d)).
    # When no ReExecute action is configured, this is a no-op single pass. The
    # legacy _USE_PF_REEXEC_REPLAY env flag is still honoured as an override.
    _sl_set, _ = _resolve_pf_stoploss(portfolio)
    _tgt_set, _ = _resolve_pf_target(portfolio)
    _reexec_configured = (
        (_sl_set.enabled and _is_reexec_action(_sl_set.action))
        or (_tgt_set.enabled and _is_reexec_action(_tgt_set.action))
    )
    if _reexec_configured or os.environ.get("_USE_PF_REEXEC_REPLAY", "0") == "1":
        _cap = max(int(getattr(_sl_set, "reexecute_count", 0) or 0),
                   int(getattr(_tgt_set, "reexecute_count", 0) or 0))
        _cap = _cap if _cap > 0 else 50
        _replays = 0
        _last_clip_ns = 0
        while _replays < _cap:
            _events = result.get("pf_clip_events") or []
            _first = next((e for e in _events if _is_reexec_action(e[2])), None)
            if _first is None:
                break
            _clip_ns = _ts_iso_to_ns(_first[0])
            if _clip_ns <= 0 or _clip_ns <= _last_clip_ns:
                break  # no forward progress — guard against a degenerate loop
            print(f"[PF_REEXEC] replay #{_replays + 1}: re-running slots flat from {_first[0]}")
            _seg_results, _seg_errors = _run_all_slots(
                move_sl_settings, fire_callbacks=False, replay_cutoff_ns=_clip_ns,
            )
            if not _seg_results:
                break
            _seg_merged = _merge_portfolio_results(
                portfolio, _seg_results, capitals, _seg_errors, user_id=user_id,
            )
            result = _splice_merged_results(
                result, _seg_merged, _clip_ns, portfolio.starting_capital,
            )
            result["pf_reexec_replays"] = _replays + 1
            _last_clip_ns = _clip_ns
            _replays += 1
        if _replays > 0:
            print(f"[PF_REEXEC] spliced {_replays} replay segment(s)")

    return result


def _merge_portfolio_results(
    portfolio: PortfolioConfig,
    slot_results: dict,
    capitals: dict,
    errors: list,
    user_id: str | None = None,
) -> dict:
    """Merge individual slot results into portfolio-level metrics."""
    total_pnl = 0.0
    total_trades = 0
    total_wins = 0
    total_losses = 0
    total_flat = 0
    all_positions_reports = []
    all_fills_reports = []

    per_strategy = {}

    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if not r:
            continue

        slot_pnl = r["total_pnl"]
        total_pnl += slot_pnl
        total_trades += r["total_trades"]
        total_wins += r["wins"]
        total_losses += r["losses"]
        total_flat += r.get("flat_trades", max(r["total_trades"] - r["wins"] - r["losses"], 0))

        # Collect reports for merging
        if r.get("positions_report") is not None and not r["positions_report"].empty:
            all_positions_reports.append(r["positions_report"])
        if r.get("fills_report") is not None and not r["fills_report"].empty:
            all_fills_reports.append(r["fills_report"])

        # Extract trade PnLs from positions_report. Prefer the base-currency
        # column added by positions_report_with_base — falling back to the
        # native column means JPY pnl would be summed alongside USD pnl,
        # which is exactly the bug we fixed upstream.
        trade_pnls: list[float] = []
        pos_report = r.get("positions_report")
        if pos_report is not None and not pos_report.empty:
            base_col = next(
                (c for c in pos_report.columns if c.startswith("realized_pnl_")
                 and c not in ("realized_pnl_",)),
                None,
            )
            pnl_col = base_col or next(
                (c for c in ["realized_pnl", "RealizedPnl", "pnl"]
                 if c in pos_report.columns),
                None,
            )
            if pnl_col:
                trade_pnls = _extract_trade_pnls(pos_report, pnl_col)

        per_strategy[slot.slot_id] = {
            "display_name": r.get("display_name", slot.display_name),
            "strategy_name": r.get("strategy_name", slot.strategy_name),
            "bar_type": r.get("bar_type", slot.bar_type_str),
            "pnl": slot_pnl,
            "trades": r["total_trades"],
            "wins": r["wins"],
            "losses": r["losses"],
            "flat_trades": r.get("flat_trades", max(r["total_trades"] - r["wins"] - r["losses"], 0)),
            "win_rate": r["win_rate"],
            "decisive_win_rate": r.get("decisive_win_rate"),
            "total_days": r.get("total_days", 0),
            "winning_days": r.get("winning_days", 0),
            "losing_days": r.get("losing_days", 0),
            "win_pct_days": r.get("win_pct_days", 0.0),
            "loss_pct_days": r.get("loss_pct_days", 0.0),
            "trade_pnls": trade_pnls,
            "allocated_capital": capitals.get(slot.slot_id, 0),
            "elapsed_seconds": r.get("elapsed_seconds"),
            "worker_pid": r.get("worker_pid"),
            "cache_hits": r.get("cache_hits"),
            "cache_misses": r.get("cache_misses"),
            "cache_currsize": r.get("cache_currsize"),
            "worker_rss_mb": r.get("worker_rss_mb"),
            "warning": r.get("warning"),
            # True when this slot ran via BacktestNode (Path B). Falsy means
            # the slot stayed on Path A — either because _USE_BACKTEST_NODE
            # wasn't set, or the gate auto-fell-back due to filter config.
            "path_b": bool(r.get("path_b")),
            # VWAP proxy-fill provenance (spec §4.2) — count of SL/Target exit
            # fills repriced for this slot; 0 / False when _USE_VWAP_FILL off.
            "vwap_fill_applied": bool(r.get("vwap_fill_applied")),
            "vwap_fill_adjustments": int(r.get("vwap_fill_adjustments", 0) or 0),
        }

    # Merge equity curves — sum balances at each timestamp
    all_curves = []
    for r in slot_results.values():
        curve = r.get("equity_curve_ts", [])
        if curve:
            all_curves.append(curve)

    equity_curve_ts = _merge_equity_curves(all_curves)
    equity = [pt["balance"] for pt in equity_curve_ts] if equity_curve_ts else [portfolio.starting_capital]

    # Max drawdown from merged equity
    peak = equity[0] if equity else portfolio.starting_capital
    max_dd = 0.0
    for val in equity:
        if val > peak:
            peak = val
        dd = ((val - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    final_balance = portfolio.starting_capital + total_pnl
    total_return_pct = (total_pnl / portfolio.starting_capital) * 100 if portfolio.starting_capital > 0 else 0
    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0
    # Decisive win rate excludes flat trades (P&L rounded to zero) — see
    # _extract_results for rationale. ``None`` when no decisive trades exist
    # so the UI can distinguish "0% decisive" from "no signal yet".
    _decisive_n = total_wins + total_losses
    decisive_win_rate = (total_wins / _decisive_n * 100) if _decisive_n > 0 else None

    # Portfolio-level day-based win% — merge daily PnLs across all slots
    portfolio_daily_pnl: dict[str, float] = {}
    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if not r:
            continue
        for day_key, pv in r.get("daily_pnl", {}).items():
            portfolio_daily_pnl[day_key] = portfolio_daily_pnl.get(day_key, 0.0) + pv
    portfolio_total_days = len(portfolio_daily_pnl)
    portfolio_winning_days = sum(1 for v in portfolio_daily_pnl.values() if v > 0)
    portfolio_losing_days = sum(1 for v in portfolio_daily_pnl.values() if v < 0)
    portfolio_win_pct_days = (portfolio_winning_days / portfolio_total_days * 100) if portfolio_total_days > 0 else 0.0
    portfolio_loss_pct_days = (portfolio_losing_days / portfolio_total_days * 100) if portfolio_total_days > 0 else 0.0

    # Merge DataFrames
    merged_positions = pd.concat(all_positions_reports, ignore_index=True) if all_positions_reports else pd.DataFrame()
    merged_fills = pd.concat(all_fills_reports, ignore_index=True) if all_fills_reports else pd.DataFrame()

    # Global user-level caps (spec §3 Level 3). Prefer user-scoped limits from
    # config/users.json over the legacy portfolio-level fallback. PnL is the
    # sum of this portfolio's PnL plus any cumulative PnL the user has
    # accrued from prior portfolios in the same orchestrator run.
    from core.users import (
        get_user_max_loss, get_user_max_profit,
        get_user_cumulative_pnl, add_user_pnl, get_user_trailing_sl,
        get_user_trailing_target,
    )
    user_max_loss = get_user_max_loss(user_id) if user_id else None
    user_max_profit = get_user_max_profit(user_id) if user_id else None
    cum_user_pnl = get_user_cumulative_pnl(user_id) if user_id else 0.0
    combined_pnl = total_pnl + cum_user_pnl
    eff_max_loss = user_max_loss if user_max_loss is not None else portfolio.max_loss
    eff_max_profit = user_max_profit if user_max_profit is not None else portfolio.max_profit
    max_loss_hit = eff_max_loss is not None and combined_pnl <= -abs(eff_max_loss)
    max_profit_hit = eff_max_profit is not None and combined_pnl >= eff_max_profit

    # User-level SL (spec §3 Level 3 / execution_logic.html §6) — Max Loss,
    # optionally ratcheted tighter by the user Trailing SL. Resolved into a
    # real equity-curve clip below (_user_sl_clip); the result drives an
    # actual force-sqoff, not just a flag.
    user_trail_sl = get_user_trailing_sl(user_id) if user_id else None
    # User-level Target — Max Profit ceiling, optionally with a Trailing
    # Target / Profit-Lock (spec §6 target doc). Resolved into a real
    # equity-curve clip below (_user_tgt_clip), same as the SL side.
    user_trail_tgt = get_user_trailing_target(user_id) if user_id else None

    # Tag-level caps (spec §11) — the tier BETWEEN portfolio and user. A
    # portfolio carrying ``portfolio_tag`` shares a Max Loss / Max Profit /
    # trailing cap (defined in config/tags.json) with every other portfolio in
    # the same tag; the cap is evaluated on the tag's cumulative combined PnL,
    # exactly like the user tier one level up.
    from core.tags import (
        get_tag_max_loss, get_tag_max_profit, get_tag_trailing_sl,
        get_tag_trailing_target, get_tag_cumulative_pnl, add_tag_pnl,
    )
    pf_tag = getattr(portfolio, "portfolio_tag", None) or None
    tag_max_loss = get_tag_max_loss(pf_tag) if pf_tag else None
    tag_max_profit = get_tag_max_profit(pf_tag) if pf_tag else None
    tag_trail_sl = get_tag_trailing_sl(pf_tag) if pf_tag else None
    tag_trail_tgt = get_tag_trailing_target(pf_tag) if pf_tag else None
    cum_tag_pnl = get_tag_cumulative_pnl(pf_tag) if pf_tag else 0.0

    # Roll this portfolio's PnL into the user AND tag aggregators so subsequent
    # portfolios (or repeat runs in the same orchestrator session) see it.
    if user_id:
        add_user_pnl(user_id, total_pnl)
    if pf_tag:
        add_tag_pnl(pf_tag, total_pnl)

    # Portfolio-level Stoploss / Target post-hoc clip. Spec:
    # 5. Logics/portfolio_sl_tgt.html. Walks the merged equity curve, finds
    # the trigger point, drops post-clip trades from the merged outputs.
    pf_sl_settings, pf_sl_warnings = _resolve_pf_stoploss(portfolio)
    pf_tgt_settings, pf_tgt_warnings = _resolve_pf_target(portfolio)
    for w in pf_sl_warnings:
        print(f"[PF_SL] {w}")
    for w in pf_tgt_warnings:
        print(f"[PF_TGT] {w}")

    # User-level SL clip (spec §3 / §6) — evaluated regardless of portfolio SL.
    _user_clip_slot_ids = [
        s.slot_id for s in portfolio.enabled_slots if slot_results.get(s.slot_id)
    ]
    user_clip = _user_sl_clip(
        equity_curve_ts, portfolio.starting_capital, cum_user_pnl,
        eff_max_loss, user_trail_sl, _user_clip_slot_ids,
    )
    user_trail_sl_hit = user_clip.clip_reason == "USER_TRAIL_STOPLOSS"
    user_trail_sl_effective = None  # surfaced via the clip log when it fires
    # User-level Target clip — Max Profit ceiling + Trailing Target.
    user_tgt_clip = _user_tgt_clip(
        equity_curve_ts, portfolio.starting_capital, cum_user_pnl,
        eff_max_profit, user_trail_tgt, _user_clip_slot_ids,
    )
    user_trail_tgt_hit = user_tgt_clip.clip_reason == "USER_TRAIL_TARGET"

    # Tag-level SL + Target clips (spec §11) — same machinery one tier down,
    # evaluated on the tag's cumulative combined PnL. No-op when the portfolio
    # has no tag or the tag defines no caps.
    tag_clip = _user_sl_clip(
        equity_curve_ts, portfolio.starting_capital, cum_tag_pnl,
        tag_max_loss, tag_trail_sl, _user_clip_slot_ids, scope_label="TAG",
    ) if pf_tag else _ClipResult()
    tag_tgt_clip = _user_tgt_clip(
        equity_curve_ts, portfolio.starting_capital, cum_tag_pnl,
        tag_max_profit, tag_trail_tgt, _user_clip_slot_ids, scope_label="TAG",
    ) if pf_tag else _ClipResult()

    clip_result = _ClipResult()
    if (pf_sl_settings.enabled or pf_tgt_settings.enabled
            or user_clip.clip_ts is not None or user_tgt_clip.clip_ts is not None
            or tag_clip.clip_ts is not None or tag_tgt_clip.clip_ts is not None):
        # Compute per-slot final P&L for selective sqoff (used at clip-point
        # to decide which slots to clip — uses end-of-run P&L as a proxy for
        # P&L at clip_ts, which is close enough for v1 since selective sqoff
        # at the clip moment matters only for which slots survive past it).
        slot_pnl_at_clip = {
            slot.slot_id: float(slot_results[slot.slot_id]["total_pnl"])
            for slot in portfolio.enabled_slots
            if slot_results.get(slot.slot_id) is not None
        }
        # Per-slot equity curves let the selective SqOff filter use each slot's
        # PnL *at the clip timestamp* rather than the end-of-run proxy.
        slot_curves = {
            slot.slot_id: slot_results[slot.slot_id].get("equity_curve_ts", [])
            for slot in portfolio.enabled_slots
            if slot_results.get(slot.slot_id) is not None
        }
        # Underlying-based SL/Target types (spec §2.1 / §5.1) are evaluated
        # against the primary slot's price series, separately from the PnL
        # clip. For each side that is underlying-based we disable its branch
        # in _apply_portfolio_clip and run the dedicated underlying clip, then
        # keep whichever clip fires first.
        sl_is_underlying = (
            pf_sl_settings.enabled
            and pf_sl_settings.sl_type in _UNDERLYING_PF_SL_TYPES
        )
        tgt_is_underlying = (
            pf_tgt_settings.enabled
            and pf_tgt_settings.tgt_type in _UNDERLYING_PF_TGT_TYPES
        )
        if sl_is_underlying or tgt_is_underlying:
            underlying_curve = next(
                (slot_results[s.slot_id].get("underlying_curve")
                 for s in portfolio.enabled_slots
                 if slot_results.get(s.slot_id)
                 and slot_results[s.slot_id].get("underlying_curve")),
                None,
            )
            # The PnL clip handles whichever side is NOT underlying-based.
            pf_sl_for_clip = (dataclasses.replace(pf_sl_settings, enabled=False)
                              if sl_is_underlying else pf_sl_settings)
            pf_tgt_for_clip = (dataclasses.replace(pf_tgt_settings, enabled=False)
                               if tgt_is_underlying else pf_tgt_settings)
            candidates = [_apply_portfolio_clip(
                equity_curve_ts, portfolio.starting_capital,
                pf_sl_for_clip, pf_tgt_for_clip, slot_pnl_at_clip,
                slot_curves=slot_curves,
            )]
            if sl_is_underlying:
                candidates.append(_underlying_sl_clip(
                    underlying_curve, equity_curve_ts, pf_sl_settings,
                    pf_tgt_settings, slot_pnl_at_clip, portfolio.starting_capital,
                    slot_curves=slot_curves,
                ))
            if tgt_is_underlying:
                candidates.append(_underlying_tgt_clip(
                    underlying_curve, equity_curve_ts, pf_sl_settings,
                    pf_tgt_settings, slot_pnl_at_clip, portfolio.starting_capital,
                    slot_curves=slot_curves,
                ))
            clip_result = _earliest_clip(*candidates)
        else:
            clip_result = _apply_portfolio_clip(
                equity_curve_ts, portfolio.starting_capital,
                pf_sl_settings, pf_tgt_settings, slot_pnl_at_clip,
                slot_curves=slot_curves,
            )
        # Merge the portfolio, tag and user clips — whichever fires earliest
        # wins (spec §8 evaluation order). The tag tier (§11) sits between
        # portfolio and user; all are evaluated and the earliest breach clips.
        clip_result = _earliest_clip(
            clip_result, tag_clip, tag_tgt_clip, user_clip, user_tgt_clip,
        )
        for log_line in clip_result.logs:
            print(f"[PF_CLIP] {log_line}")

        if clip_result.clip_ts is not None and clip_result.clipped_slots:
            # Drop post-clip rows from merged_fills / merged_positions for the
            # clipped slot set. We match by trader_id (slot_to_trader_id is
            # built below — compute it inline here since we need it earlier).
            _slot_to_trader_pre = {}
            for slot in portfolio.enabled_slots:
                r = slot_results.get(slot.slot_id)
                if r and r.get("positions_report") is not None and not r["positions_report"].empty:
                    tids = r["positions_report"]["trader_id"].unique()
                    if len(tids) > 0:
                        _slot_to_trader_pre[slot.slot_id] = str(tids[0])

            clipped_traders = {
                _slot_to_trader_pre[sid] for sid in clip_result.clipped_slots
                if sid in _slot_to_trader_pre
            }
            clip_ns = _ts_iso_to_ns(clip_result.clip_ts)

            def _filter_post_clip(df: "pd.DataFrame") -> "pd.DataFrame":
                if df.empty or not clipped_traders:
                    return df
                if "trader_id" not in df.columns or "ts_init" not in df.columns:
                    return df
                # Drop rows where trader_id ∈ clipped_traders AND ts_init > clip_ns.
                # ts_init in the report is typically a pandas Timestamp object;
                # convert to int ns for comparison.
                ts_int = pd.to_datetime(df["ts_init"], errors="coerce", utc=True).astype("int64")
                mask = (df["trader_id"].astype(str).isin(clipped_traders)) & (ts_int > clip_ns)
                return df.loc[~mask].reset_index(drop=True)

            # v1 ReExecute is documented as "clip + flag, no replay" (see
            # _apply_portfolio_clip log line). For ReExecute, do NOT actually
            # drop trades — the flag is informational only, the trades really
            # happened. Filtering here would empty the orderbook/positions
            # whenever the clip fires near the start of the run (e.g. tight
            # pf_sl_value with bid/ask spread immediately tipping combined
            # PnL negative). SqOff still filters: post-clip trades genuinely
            # "shouldn't have happened" once the portfolio was squared off.
            if not clip_result.would_reexecute:
                merged_fills = _filter_post_clip(merged_fills)
                merged_positions = _filter_post_clip(merged_positions)

            # Recompute aggregate stats from the clipped positions (PnL/trades
            # for slots that were clipped). For v1 we only update the totals;
            # per-slot stats remain pre-clip (would require recomputing each
            # slot's win/loss from clipped merged_positions — defer).
            #
            # The "realized_pnl" column from Nautilus is a Money-string
            # ("-2.20 USD", "-321 JPY") — calling .sum() on it concatenates
            # rather than adds. Prefer the base-currency numeric column added
            # by positions_report_with_base (e.g. "realized_pnl_USD"), which
            # is FX-converted and float-typed. Same lookup pattern as
            # backtest_runner.py:2978-2991.
            if not merged_positions.empty:
                _base_col = next(
                    (c for c in merged_positions.columns
                     if c.startswith("realized_pnl_") and c != "realized_pnl_"),
                    None,
                )
                if _base_col is not None:
                    _clipped_pnl = float(merged_positions[_base_col].sum())
                elif "realized_pnl" in merged_positions.columns:
                    # Fallback: parse Money strings ("X.XX CCY" -> X.XX). This
                    # ignores cross-currency conversion and is only correct for
                    # single-currency catalogs. The base-currency column above
                    # is the right path; this branch exists for safety.
                    _clipped_pnl = sum(
                        float(str(x).split(" ")[0]) if x and " " in str(x) else 0.0
                        for x in merged_positions["realized_pnl"]
                    )
                else:
                    _clipped_pnl = total_pnl  # nothing to recompute
                if _clipped_pnl != total_pnl:
                    print(f"[PF_CLIP] Recomputed total_pnl after clip: {total_pnl:.2f} -> {_clipped_pnl:.2f}")
                total_pnl = _clipped_pnl
                final_balance = portfolio.starting_capital + total_pnl
                total_return_pct = (total_pnl / portfolio.starting_capital) * 100 if portfolio.starting_capital > 0 else 0
                total_trades = len(merged_positions)

    # Build slot_to_strategy_id mapping from positions_report
    slot_to_strategy_id = {}
    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if r and r.get("positions_report") is not None and not r["positions_report"].empty:
            sids = r["positions_report"]["strategy_id"].unique()
            if len(sids) > 0:
                slot_to_strategy_id[slot.slot_id] = str(sids[0])

    # Build slot_to_trader_id mapping — trader_id is unique per slot even when
    # multiple slots share the same strategy_id (e.g. grouped ManagedExitStrategy).
    slot_to_trader_id = {}
    for slot in portfolio.enabled_slots:
        r = slot_results.get(slot.slot_id)
        if r and r.get("positions_report") is not None and not r["positions_report"].empty:
            tids = r["positions_report"]["trader_id"].unique()
            if len(tids) > 0:
                slot_to_trader_id[slot.slot_id] = str(tids[0])

    # Portfolio-level Path B summary. "all" when every slot ran on Path B,
    # "none" when every slot stayed on Path A, "mixed" when some did and some
    # didn't (e.g. one slot had a filter that triggered Path A fallback).
    _slot_path_b_flags = [
        bool(r.get("path_b"))
        for r in slot_results.values()
        if r is not None
    ]
    if not _slot_path_b_flags:
        path_b_summary = "none"
    elif all(_slot_path_b_flags):
        path_b_summary = "all"
    elif any(_slot_path_b_flags):
        path_b_summary = "mixed"
    else:
        path_b_summary = "none"

    return {
        "starting_capital": portfolio.starting_capital,
        "final_balance": final_balance,
        "total_pnl": total_pnl,
        "total_return_pct": total_return_pct,
        "total_trades": total_trades,
        "wins": total_wins,
        "losses": total_losses,
        "flat_trades": total_flat,
        "win_rate": win_rate,
        "decisive_win_rate": decisive_win_rate,
        "path_b": path_b_summary,  # "all" | "mixed" | "none"
        "total_days": portfolio_total_days,
        "winning_days": portfolio_winning_days,
        "losing_days": portfolio_losing_days,
        "win_pct_days": portfolio_win_pct_days,
        "loss_pct_days": portfolio_loss_pct_days,
        "max_drawdown": max_dd,
        "equity_curve": equity,
        "equity_curve_ts": equity_curve_ts,
        "per_strategy": per_strategy,
        "max_loss_hit": max_loss_hit,
        "max_profit_hit": max_profit_hit,
        # User-level Trailing SL (spec execution_logic.html §6.1) — detection
        # flag + the ratcheted-tighter effective Max-Loss cap.
        "user_trail_sl_hit": user_trail_sl_hit,
        "user_trail_sl_effective": user_trail_sl_effective,
        # User-level Trailing Target / Profit-Lock (spec §6.1 target doc).
        "user_trail_tgt_hit": user_trail_tgt_hit,
        # Portfolio-level Stoploss/Target post-hoc clip (spec
        # 5. Logics/portfolio_sl_tgt.html). Null/empty when not enabled or
        # when the clip never triggered. clip_action is informational —
        # ReExecute is treated as clip+flag in v1, no actual replay.
        "pf_clip_ts": clip_result.clip_ts,
        "pf_clip_reason": clip_result.clip_reason,
        "pf_clip_action": clip_result.clip_action,
        "pf_clipped_slot_ids": list(clip_result.clipped_slots),
        "pf_would_reexecute": clip_result.would_reexecute,
        "pf_reexec_count": clip_result.reexec_count,
        # Chronological clip events (ts, reason, action) — drives the
        # ReExecute replay loop (_USE_PF_REEXEC_REPLAY). Empty when no clip.
        "pf_clip_events": list(clip_result.clip_events),
        "portfolio_name": portfolio.name,
        "allocation_mode": portfolio.allocation_mode,
        "fills_report": merged_fills,
        "positions_report": merged_positions,
        "account_report": None,
        "slot_to_strategy_id": slot_to_strategy_id,
        "slot_to_trader_id": slot_to_trader_id,
        "errors": errors,
        "warnings": [
            {"slot_id": sid, "display_name": info.get("display_name", sid), "warning": info["warning"]}
            for sid, info in per_strategy.items() if info.get("warning")
        ],
    }


def _extract_trade_pnls(pos_report: pd.DataFrame, pnl_col: str) -> list[float]:
    """Pull a list of float PnLs from a positions_report column.

    Replaces an iterrows() walk: extracting the column once with .tolist()
    avoids allocating one Series per row, which is the dominant cost on
    portfolios with thousands of trades.

    Tolerates the two shapes the column ever takes:
      * numeric (int / float) — used by `realized_pnl_<base>` after
        positions_report_with_base() has converted to base currency.
      * money-string ("123.45 USD", "0 JPY", or unparseable) — the raw
        Nautilus output. Unparseable cells fall through to 0.0.
    """
    values = pos_report[pnl_col].tolist()
    out: list[float] = []
    for val in values:
        if isinstance(val, (int, float)):
            out.append(float(val))
            continue
        try:
            out.append(float(str(val).split()[0]))
        except (ValueError, IndexError):
            out.append(0.0)
    return out


def _merge_equity_curves(curves: list[list[dict]]) -> list[dict]:
    """Merge multiple timestamped equity curves by summing balances at each timestamp.

    Each curve is a list of ``{"timestamp": iso_str, "balance": float}`` points.
    Output: one point per unique timestamp across all curves; the balance at
    each timestamp is the sum of every curve's most-recent balance at-or-before
    that timestamp (curves contribute 0.0 before their first point).
    """
    if not curves:
        return []
    if len(curves) == 1:
        return curves[0]

    # The dict-walk path here outperformed a pandas concat+ffill+sum
    # equivalent across every realistic input size (9-100 curves × 1k-10k
    # points): the vectorised version paid heavy concat/groupby/ffill
    # overhead that the small-N inner loop never recovered. Kept simple.
    all_timestamps = set()
    for curve in curves:
        for pt in curve:
            if pt.get("timestamp"):
                all_timestamps.add(pt["timestamp"])

    if not all_timestamps:
        return curves[0]

    sorted_ts = sorted(all_timestamps)

    # For each curve, build timestamp -> balance map with forward-fill
    curve_maps = []
    for curve in curves:
        ts_map = {}
        for pt in curve:
            if pt.get("timestamp"):
                ts_map[pt["timestamp"]] = pt["balance"]
        curve_maps.append(ts_map)

    # Merge: at each timestamp, sum the latest known balance from each curve
    merged = []
    last_balances = [0.0] * len(curves)
    for ts in sorted_ts:
        for i, ts_map in enumerate(curve_maps):
            if ts in ts_map:
                last_balances[i] = ts_map[ts]
        merged.append({"timestamp": ts, "balance": sum(last_balances)})

    return merged


def _build_equity_curve_from_account(accounts: list, starting_capital: float) -> list[dict]:
    """Build a timestamped equity curve from account state events.

    Returns list of {"timestamp": iso_str, "balance": float} dicts.
    """
    equity_curve_ts = [{"timestamp": None, "balance": starting_capital}]

    if not accounts:
        return equity_curve_ts

    account = accounts[0]
    try:
        events = account.events
    except Exception:
        return equity_curve_ts

    if not events:
        return equity_curve_ts

    # ts_iso -> index in `curve`, so dedup replacement is O(1) instead of an
    # O(n) reverse scan per duplicate.
    ts_to_idx: dict[str, int] = {}
    curve: list[dict] = []
    for event in events:
        ts_iso = pd.Timestamp(event.ts_event, unit="ns", tz="UTC").isoformat()
        # Hoisted try/except: failing `balances` iteration skips the event
        # without paying Python's per-iteration try-setup cost inside a sum.
        try:
            total = sum(float(bal.total) for bal in event.balances)
        except Exception:
            continue
        existing = ts_to_idx.get(ts_iso)
        if existing is not None:
            curve[existing]["balance"] = total
        else:
            ts_to_idx[ts_iso] = len(curve)
            curve.append({"timestamp": ts_iso, "balance": total})

    if curve:
        # Prepend starting point with the first event's timestamp if different
        if curve[0]["balance"] != starting_capital:
            first_ts = curve[0]["timestamp"]
            curve.insert(0, {"timestamp": first_ts, "balance": starting_capital})
        return curve

    return equity_curve_ts


def _extract_portfolio_results(
    engine: BacktestEngine,
    portfolio: PortfolioConfig,
    slot_strategy_map: dict,
) -> dict:
    """Extract portfolio-level and per-strategy results."""
    # Get actual strategy IDs from engine
    actual_strategies = engine.trader.strategies()
    actual_strategy_ids = [str(s.id) for s in actual_strategies]

    # Map slot_id -> actual strategy_id
    slot_to_actual = {}
    strategy_list = list(slot_strategy_map.items())
    for i, (slot_id, strategy) in enumerate(strategy_list):
        if i < len(actual_strategy_ids):
            slot_to_actual[slot_id] = actual_strategy_ids[i]

    # Get all positions (both closed and open)
    all_positions = engine.kernel.cache.positions()
    closed_positions = [p for p in all_positions if p.is_closed]
    open_positions = [p for p in all_positions if p.is_open]

    # Get final balance
    accounts = list(engine.kernel.cache.accounts())
    final_balance = portfolio.starting_capital
    if accounts:
        try:
            balance = accounts[0].balance_total(USD)
            if balance is not None:
                final_balance = float(balance)
        except Exception:
            pass

    total_pnl = final_balance - portfolio.starting_capital

    # Include unrealized P&L from open positions in total P&L
    unrealized_pnl = 0.0
    for pos in open_positions:
        try:
            unrealized_pnl += float(pos.unrealized_pnl(pos.last_price))
        except Exception:
            pass

    total_pnl_with_unrealized = total_pnl + unrealized_pnl
    total_return_pct = (total_pnl_with_unrealized / portfolio.starting_capital) * 100 if portfolio.starting_capital > 0 else 0

    # Generate reports for accurate trade counting and CSV export.
    # In NETTING mode, cache.positions() returns only 1 position per instrument,
    # but positions_report has the actual round-trip trades.
    positions_report = None
    fills_report = None
    account_report = None
    try:
        positions_report = engine.trader.generate_positions_report()
    except Exception:
        pass
    try:
        fills_report = engine.trader.generate_order_fills_report()
    except Exception:
        pass
    try:
        accs = list(engine.kernel.cache.accounts())
        if accs:
            venue = accs[0].id.get_issuer()
            account_report = engine.trader.generate_account_report(Venue(str(venue)))
    except Exception:
        pass

    # Portfolio-level stats — use positions_report for accurate trade counts
    all_pnls = []
    total_wins = 0
    total_losses = 0
    total_trades = 0

    # Build per-strategy PnL lookup from positions_report
    strategy_pnls = {}  # strategy_id -> list of pnl values

    if positions_report is not None and not positions_report.empty:
        pnl_col = None
        for col_name in ["realized_pnl", "RealizedPnl", "pnl"]:
            if col_name in positions_report.columns:
                pnl_col = col_name
                break

        strat_col = None
        for col_name in ["strategy_id", "StrategyId"]:
            if col_name in positions_report.columns:
                strat_col = col_name
                break

        if pnl_col:
            for _, row in positions_report.iterrows():
                try:
                    pnl_val = float(str(row[pnl_col]).split()[0])
                except (ValueError, IndexError):
                    pnl_val = 0.0
                all_pnls.append(pnl_val)
                total_trades += 1
                if pnl_val > 0:
                    total_wins += 1
                elif pnl_val < 0:
                    total_losses += 1

                # Track per-strategy
                if strat_col:
                    sid = str(row[strat_col])
                    strategy_pnls.setdefault(sid, []).append(pnl_val)
    else:
        # Fallback to cache positions
        for pos in closed_positions:
            try:
                pnl = float(pos.realized_pnl)
            except (TypeError, ValueError):
                pnl = 0.0
            all_pnls.append(pnl)
            total_trades += 1
            if pnl > 0:
                total_wins += 1
            elif pnl < 0:
                total_losses += 1

            sid = str(pos.strategy_id)
            strategy_pnls.setdefault(sid, []).append(pnl)

    # Count open positions as trades too
    for pos in open_positions:
        total_trades += 1
        try:
            pnl = float(pos.unrealized_pnl(pos.last_price))
        except Exception:
            pnl = 0.0
        all_pnls.append(pnl)
        if pnl > 0:
            total_wins += 1
        elif pnl < 0:
            total_losses += 1

        sid = str(pos.strategy_id)
        strategy_pnls.setdefault(sid, []).append(pnl)

    win_rate = (total_wins / total_trades * 100) if total_trades > 0 else 0

    # Build timestamped equity curve from account events
    actual_final = final_balance + unrealized_pnl
    equity_curve_ts = _build_equity_curve_from_account(accounts, portfolio.starting_capital)

    # Compute max drawdown from the timestamped equity curve
    balances = [pt["balance"] for pt in equity_curve_ts] if equity_curve_ts else [portfolio.starting_capital]
    peak = balances[0]
    max_dd = 0.0
    for val in balances:
        if val > peak:
            peak = val
        dd = ((val - peak) / peak) * 100 if peak > 0 else 0
        if dd < max_dd:
            max_dd = dd

    # Backward-compat equity_curve (just balance values)
    equity = balances

    # Per-strategy results using strategy_pnls from positions_report
    per_strategy = {}
    for slot in portfolio.enabled_slots:
        actual_sid = slot_to_actual.get(slot.slot_id)
        if not actual_sid:
            continue

        slot_pnls = strategy_pnls.get(actual_sid, [])
        slot_wins = sum(1 for p in slot_pnls if p > 0)
        slot_losses = sum(1 for p in slot_pnls if p < 0)
        slot_trades = len(slot_pnls)
        slot_pnl = sum(slot_pnls)

        per_strategy[slot.slot_id] = {
            "display_name": slot.display_name,
            "strategy_name": slot.strategy_name,
            "bar_type": slot.bar_type_str,
            "pnl": slot_pnl,
            "trades": slot_trades,
            "wins": slot_wins,
            "losses": slot_losses,
            "win_rate": (slot_wins / slot_trades * 100) if slot_trades > 0 else 0,
            "trade_pnls": slot_pnls,
        }

    # Global user-level caps (spec §3 Level 3). Same precedence rule as the
    # path-A site: user-level limits override portfolio-level when present.
    from core.users import (
        get_user_max_loss, get_user_max_profit,
        get_user_cumulative_pnl, add_user_pnl,
    )
    user_max_loss = get_user_max_loss(user_id) if user_id else None
    user_max_profit = get_user_max_profit(user_id) if user_id else None
    cum_user_pnl = get_user_cumulative_pnl(user_id) if user_id else 0.0
    combined_pnl = total_pnl_with_unrealized + cum_user_pnl
    eff_max_loss = user_max_loss if user_max_loss is not None else portfolio.max_loss
    eff_max_profit = user_max_profit if user_max_profit is not None else portfolio.max_profit
    max_loss_hit = eff_max_loss is not None and combined_pnl <= -abs(eff_max_loss)
    max_profit_hit = eff_max_profit is not None and combined_pnl >= eff_max_profit
    if user_id:
        add_user_pnl(user_id, total_pnl_with_unrealized)

    return {
        "starting_capital": portfolio.starting_capital,
        "final_balance": actual_final,
        "total_pnl": total_pnl_with_unrealized,
        "total_return_pct": total_return_pct,
        "total_trades": total_trades,
        "wins": total_wins,
        "losses": total_losses,
        "win_rate": win_rate,
        "max_drawdown": max_dd,
        "equity_curve": equity,
        "equity_curve_ts": equity_curve_ts,
        "per_strategy": per_strategy,
        "max_loss_hit": max_loss_hit,
        "max_profit_hit": max_profit_hit,
        "portfolio_name": portfolio.name,
        # Raw report DataFrames for CSV export
        "fills_report": fills_report,
        "positions_report": positions_report,
        "account_report": account_report,
        # Mapping of slot_id -> actual engine strategy_id
        "slot_to_strategy_id": slot_to_actual,
    }


def _extract_results(
    engine: BacktestEngine,
    starting_capital: float,
    fx_resolver: FxRateResolver | None = None,
    vwap_lookup: dict | None = None,
) -> dict:
    """Extract backtest results from the engine, converting per-position PnL
    into the account base currency via the supplied FX resolver.

    Without a resolver, results use engine-native numbers (identical to the
    pre-FX-aware behavior) — safe default for USD-only catalogs.

    ``vwap_lookup`` (from ``_build_vwap_lookup``) enables the VWAP proxy-fill
    model: SL/Target exit fills are repriced before any metric is computed.
    ``None`` (the default) leaves fills as the engine produced them.
    """
    trader = engine.trader

    # Generate reports (safe)
    fills_report = None
    try:
        fills_report = trader.generate_order_fills_report()
    except Exception:
        pass

    positions_report = None
    try:
        positions_report = trader.generate_positions_report()
    except Exception:
        pass

    # VWAP proxy-fill (spec §4.2): reprice SL/Target exits before any metric
    # is derived from positions_report, so win/loss, daily PnL, totals and the
    # base-currency column all reflect the adjusted fills.
    vwap_fill_adjustments = 0
    if vwap_lookup:
        try:
            vwap_fill_adjustments = _apply_vwap_fill(
                positions_report, fills_report, vwap_lookup,
            )
        except Exception:
            vwap_fill_adjustments = 0

    account_report = None
    try:
        # Try to get account report using the venue from cached accounts
        accounts = list(engine.kernel.cache.accounts())
        if accounts:
            venue = accounts[0].id.get_issuer()
            account_report = trader.generate_account_report(Venue(str(venue)))
    except Exception:
        pass

    # Realized cash on the account balance is unreliable here: when the engine
    # has no xrate source, JPY PnL from USDJPY never converts back to USD and
    # the USD balance stays frozen at the starting capital. Instead we rebuild
    # the total from per-position PnL, each converted via the FX resolver.
    accounts = list(engine.kernel.cache.accounts())

    # Include unrealized P&L from open positions, converted to base currency.
    # pos.unrealized_pnl returns a Money in the position's quote_currency — for
    # USDJPY that's JPY. Without conversion we'd be adding JPY to USD.
    all_positions = engine.kernel.cache.positions()
    open_positions = [p for p in all_positions if p.is_open]
    closed_positions = [p for p in all_positions if p.is_closed]

    unrealized_pnl = 0.0
    for pos in open_positions:
        unrealized_pnl += _position_unrealized_in_base(pos, fx_resolver)

    # Sum realized PnL across closed positions *in base currency*. Note: this
    # replaces the prior approach of reading `final_balance` from the USD
    # account, because the USD balance never moves when the engine has no
    # xrate source — the only trustworthy realized-PnL total is the sum of
    # per-position PnLs, each converted individually.
    realized_pnl_base = 0.0
    for pos in closed_positions:
        realized_pnl_base += _position_realized_in_base(pos, fx_resolver)

    # Prefer the positions_report for accurate trade counts (NETTING mode
    # collapses cache.positions() to 1 per instrument).
    report_realized_base = None
    if positions_report is not None and not positions_report.empty:
        report_realized_base = _positions_report_realized_in_base(
            positions_report, fx_resolver,
        )

    # Use the report-derived total when available (it captures every round
    # trip, not just the single NETTING position). Fall back to the cache
    # sum otherwise.
    total_realized_base = (
        report_realized_base if report_realized_base is not None
        else realized_pnl_base
    )

    total_pnl = total_realized_base + unrealized_pnl
    total_return_pct = (total_pnl / starting_capital) * 100 if starting_capital > 0 else 0

    # Count trades from order fills (round-trip analysis)
    orders = engine.kernel.cache.orders()
    filled_orders = [o for o in orders if o.is_closed]
    total_orders = len(filled_orders)

    wins = 0
    losses = 0
    total_trades = 0

    # In NETTING mode, cache.positions() returns only 1 position per instrument
    # (it gets reused for every open/close cycle). The positions_report has the
    # actual round-trip trades, so prefer that for accurate trade counting.
    if positions_report is not None and not positions_report.empty:
        pnl_col = None
        for col_name in ["realized_pnl", "RealizedPnl", "pnl"]:
            if col_name in positions_report.columns:
                pnl_col = col_name
                break
        ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"])

        if pnl_col:
            for _, row in positions_report.iterrows():
                pnl_val = _row_pnl_to_base(row, pnl_col, ts_col, fx_resolver)
                total_trades += 1
                if pnl_val > 0:
                    wins += 1
                elif pnl_val < 0:
                    losses += 1
        else:
            total_trades = len(positions_report)

    # Fallback to cache positions if positions_report was empty
    if total_trades == 0:
        for pos in closed_positions:
            pnl = _position_realized_in_base(pos, fx_resolver)
            total_trades += 1
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1

    # Count open positions
    for pos in open_positions:
        pnl = _position_unrealized_in_base(pos, fx_resolver)
        total_trades += 1
        if pnl > 0:
            wins += 1
        elif pnl < 0:
            losses += 1

    # Last resort fallback from fills count
    if total_trades == 0 and fills_report is not None and not fills_report.empty:
        total_trades = max(total_orders // 2, 1)
        if total_pnl > 0:
            wins = 1
        elif total_pnl < 0:
            losses = 1

    # ``flat_trades`` are closed positions whose realized P&L rounds to zero in
    # the account base currency. They are real trades — entry + exit both
    # filled — but the price moved by less than the sub-cent precision can
    # represent (or not at all). Surfacing this separately prevents the
    # "12,052 trades, 339 wins, 180 losses" confusion where the simple win
    # rate (wins / total) penalises the strategy for trades that didn't lose.
    flat_trades = max(total_trades - wins - losses, 0)
    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0
    # ``decisive_win_rate`` excludes flat trades from the denominator. Useful
    # when most trades are flat (e.g. a too-small trade_size relative to bar
    # noise) — gives the meaningful "of the trades that produced P&L, what
    # fraction were wins?" figure.
    _decisive_n = wins + losses
    decisive_win_rate = (wins / _decisive_n * 100) if _decisive_n > 0 else None

    # Day-based win percentage for single-strategy backtest
    # Use entry time (ts_init) for daily grouping to match the HTML report
    _daily_pnl: dict[str, float] = {}
    _daily_ts_col = _pick_col(positions_report, ["ts_init", "ts_closed", "ts_last"]) if positions_report is not None and not positions_report.empty else None
    _pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"]) if positions_report is not None and not positions_report.empty else None
    # _row_pnl_to_base needs the close-time column for FX conversion
    _close_ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"]) if positions_report is not None and not positions_report.empty else None
    if _daily_ts_col and _pnl_col and positions_report is not None and not positions_report.empty:
        for _, row in positions_report.iterrows():
            pnl_val = _row_pnl_to_base(row, _pnl_col, _close_ts_col, fx_resolver)
            ts_raw = row.get(_daily_ts_col)
            dt = _to_utc_ts(ts_raw)
            day_key = dt.strftime("%Y-%m-%d") if dt is not None else "unknown"
            _daily_pnl[day_key] = _daily_pnl.get(day_key, 0.0) + pnl_val
    _total_days = len(_daily_pnl)
    _winning_days = sum(1 for v in _daily_pnl.values() if v > 0)
    _losing_days = sum(1 for v in _daily_pnl.values() if v < 0)
    _win_pct_days = (_winning_days / _total_days * 100) if _total_days > 0 else 0.0
    _loss_pct_days = (_losing_days / _total_days * 100) if _total_days > 0 else 0.0

    # Build timestamped equity curve from account events. The engine emits
    # these in the account's base currency, so no per-event conversion is
    # needed — but the JPY-native unrealized PnL never hit the account, so
    # we stitch a final point reflecting the converted total.
    equity_curve_ts = _build_equity_curve_from_account(accounts, starting_capital)
    _ensure_final_equity_point(equity_curve_ts, starting_capital + total_pnl)

    return {
        "starting_capital": starting_capital,
        "final_balance": starting_capital + total_pnl,
        "total_pnl": total_pnl,
        "total_return_pct": total_return_pct,
        "total_orders": total_orders,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "flat_trades": flat_trades,
        "win_rate": win_rate,
        "decisive_win_rate": decisive_win_rate,
        "total_days": _total_days,
        "winning_days": _winning_days,
        "losing_days": _losing_days,
        "win_pct_days": _win_pct_days,
        "loss_pct_days": _loss_pct_days,
        "daily_pnl": _daily_pnl,
        "equity_curve_ts": equity_curve_ts,
        "fills_report": fills_report,
        "positions_report": positions_report_with_base(positions_report, fx_resolver),
        "account_report": account_report,
        "vwap_fill_applied": bool(vwap_fill_adjustments),
        "vwap_fill_adjustments": vwap_fill_adjustments,
    }


# ─── FX-aware PnL helpers ───────────────────────────────────────────────────

def _pick_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Return the first column in `candidates` that exists in `df`."""
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _to_utc_ts(raw) -> pd.Timestamp | None:
    """Best-effort conversion of a report timestamp cell to a UTC Timestamp."""
    if raw is None:
        return None
    try:
        ts = pd.Timestamp(raw)
    except Exception:
        return None
    if pd.isna(ts):
        return None
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts


def _position_realized_in_base(pos, fx_resolver: FxRateResolver | None) -> float:
    """Convert a closed position's realized PnL into the account base currency.

    Uses `pos.ts_closed` as the rate timestamp so 2015 fills convert at 2015
    rates and 2024 fills convert at 2024 rates.
    """
    try:
        money = pos.realized_pnl
    except Exception:
        return 0.0
    if money is None:
        return 0.0
    try:
        amount = float(money)
    except (TypeError, ValueError):
        return 0.0
    if fx_resolver is None or amount == 0:
        return amount
    ccy = getattr(money, "currency", None)
    ccy_code = str(getattr(ccy, "code", "") or "").upper()
    ts_ns = getattr(pos, "ts_closed", None) or getattr(pos, "ts_last", None)
    at = pd.Timestamp(ts_ns, unit="ns", tz="UTC") if ts_ns else None
    return fx_resolver.convert(amount, ccy_code, at)


def _position_unrealized_in_base(pos, fx_resolver: FxRateResolver | None) -> float:
    """Convert an open position's unrealized PnL into the account base currency.

    Uses `pos.ts_last` (last price timestamp) as the rate timestamp.
    """
    try:
        money = pos.unrealized_pnl(pos.last_price)
    except Exception:
        return 0.0
    if money is None:
        return 0.0
    try:
        amount = float(money)
    except (TypeError, ValueError):
        return 0.0
    if fx_resolver is None or amount == 0:
        return amount
    ccy = getattr(money, "currency", None)
    ccy_code = str(getattr(ccy, "code", "") or "").upper()
    ts_ns = getattr(pos, "ts_last", None) or getattr(pos, "ts_closed", None)
    at = pd.Timestamp(ts_ns, unit="ns", tz="UTC") if ts_ns else None
    return fx_resolver.convert(amount, ccy_code, at)


def _row_pnl_to_base(
    row,
    pnl_col: str,
    ts_col: str | None,
    fx_resolver: FxRateResolver | None,
) -> float:
    """Parse a positions_report PnL cell (e.g. '0 JPY') and convert to base."""
    amount, ccy = parse_money_string(row[pnl_col])
    if fx_resolver is None:
        return amount
    at = _to_utc_ts(row[ts_col]) if ts_col else None
    return fx_resolver.convert(amount, ccy, at)


def _base_values_from_report(
    positions_report: pd.DataFrame,
    pnl_col: str,
    ts_col: str | None,
    fx_resolver: FxRateResolver | None,
) -> list[float]:
    """Per-row base-currency PnL without paying iterrows' Series-per-row cost.

    Pulls the two underlying columns out as Python lists once, then walks them
    in a tight loop. Semantics identical to iterating with _row_pnl_to_base.
    """
    pnl_values = positions_report[pnl_col].tolist()
    ts_values = positions_report[ts_col].tolist() if ts_col else None
    out: list[float] = []
    if fx_resolver is None:
        for pnl_raw in pnl_values:
            amount, _ = parse_money_string(pnl_raw)
            out.append(amount)
        return out
    for i, pnl_raw in enumerate(pnl_values):
        amount, ccy = parse_money_string(pnl_raw)
        at = _to_utc_ts(ts_values[i]) if ts_values is not None else None
        out.append(fx_resolver.convert(amount, ccy, at))
    return out


def _positions_report_realized_in_base(
    positions_report: pd.DataFrame,
    fx_resolver: FxRateResolver | None,
) -> float | None:
    """Sum realized PnL from every row of a positions_report, in base currency.

    Returns None if the report has no recognizable PnL column — caller should
    fall back to the cache-positions sum.
    """
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    if pnl_col is None:
        return None
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"])
    return float(sum(_base_values_from_report(positions_report, pnl_col, ts_col, fx_resolver)))


def positions_report_with_base(
    positions_report: pd.DataFrame | None,
    fx_resolver: FxRateResolver | None,
) -> pd.DataFrame | None:
    """Return a copy of `positions_report` with an added `realized_pnl_base`
    column expressed in the account's base currency. No-op if no resolver or
    no conversion rules (report already in base currency).
    """
    if positions_report is None or positions_report.empty:
        return positions_report
    if fx_resolver is None:
        return positions_report
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    if pnl_col is None:
        return positions_report
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last", "ts_init"])
    base_ccy = fx_resolver.base_currency
    base_values = _base_values_from_report(positions_report, pnl_col, ts_col, fx_resolver)
    out = positions_report.copy()
    out[f"realized_pnl_{base_ccy.lower()}"] = base_values
    return out


def _ensure_final_equity_point(
    equity_curve_ts: list[dict],
    final_balance: float,
) -> None:
    """Append (or update) a final curve point matching `final_balance`.

    The engine-emitted equity curve reflects realized cash flow on the account,
    which misses FX-converted unrealized PnL. Stitch the corrected endpoint so
    downstream drawdown/return math sees the right terminal value.
    """
    if not equity_curve_ts:
        equity_curve_ts.append({"timestamp": None, "balance": float(final_balance)})
        return
    last = equity_curve_ts[-1]
    if abs(float(last.get("balance") or 0.0) - final_balance) < 1e-6:
        return
    equity_curve_ts.append({
        "timestamp": last.get("timestamp"),
        "balance": float(final_balance),
    })


# ─── VWAP proxy-fill model (spec §4.2 / 5. Logics/sl_tgt.html) ───────────────
#
# Gated by the _USE_VWAP_FILL env flag. When on, SL/Target exit fills are
# repriced to the conservative VWAP-proxy fill described in
# "5. Logics/sl_tgt.html" — exit_price = vwap if vwap > hit_price else
# hit_price. The catalog carries no intra-bar tick data, so the per-bar VWAP
# is *proxied* by the bar's typical price (H+L+C)/3 of the opposite quote
# side. All of this is runner-side post-run report surgery (same idiom as
# _apply_portfolio_clip) — the engine and ManagedExitStrategy are untouched.

# Close-order tag prefixes that mark an SL / Target exit (set by
# ManagedExitStrategy._handle_exit). Squareoff and entry fills are excluded.
_VWAP_SL_PREFIXES = ("Stop Loss", "Trailing SL", "Reverse on SL")
_VWAP_TP_PREFIXES = ("Take Profit", "Reverse on TP")


def _build_vwap_lookup(bars) -> dict | None:
    """Index ASK/BID bars by ts_event for the VWAP proxy-fill model.

    Returns ``{"ask": {ts_ns: (typical, high, low)}, "bid": {...}}`` where
    ``typical = (high + low + close) / 3`` is the per-bar VWAP proxy. Returns
    ``None`` when no ASK/BID bars are present (LAST / crypto-only slots) —
    callers treat ``None`` as "VWAP fill not applicable, leave fills as-is".
    """
    ask: dict[int, tuple] = {}
    bid: dict[int, tuple] = {}
    for bar in bars:
        bt = str(bar.bar_type)
        if "-ASK-" in bt:
            side = ask
        elif "-BID-" in bt:
            side = bid
        else:
            continue
        try:
            h = float(bar.high)
            l = float(bar.low)
            c = float(bar.close)
        except (TypeError, ValueError):
            continue
        side[int(bar.ts_event)] = ((h + l + c) / 3.0, h, l)
    if not ask and not bid:
        return None
    return {"ask": ask, "bid": bid}


def _vwap_normalize_tag(tg) -> str:
    """Unwrap a fills_report ``tags`` cell to its verbatim string.

    Nautilus stores tags as ``['Stop Loss: …']``; mirror report_generator's
    _normalize_tags so the prefix match sees the raw reason.
    """
    if isinstance(tg, (list, tuple)):
        return str(tg[0]) if tg else ""
    if tg is None:
        return ""
    return str(tg)


def _vwap_ts_to_ns(raw) -> int:
    """Best-effort conversion of a report timestamp cell to UTC nanoseconds.

    Handles nanosecond ints, pandas.Timestamp and ISO strings identically so
    a positions_report ``ts_closed`` matches a _build_vwap_lookup key (which
    is a raw ``bar.ts_event`` int)."""
    if raw is None:
        return 0
    try:
        if isinstance(raw, float) and pd.isna(raw):
            return 0
    except Exception:
        pass
    try:
        ts = pd.Timestamp(raw)
        if ts.tzinfo is None:
            ts = ts.tz_localize("UTC")
        return int(ts.value)
    except Exception:
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0


def _vwap_row_is_long(report, idx, sqty_col, entry_col, side_col):
    """Return True/False for a positions_report row's direction, or None.

    Prefers ``signed_qty`` (positive = long), then the ``entry`` order side,
    then the ``side`` position-side column."""
    if sqty_col:
        try:
            v = float(report.at[idx, sqty_col])
            if v > 0:
                return True
            if v < 0:
                return False
        except (TypeError, ValueError):
            pass
    if entry_col:
        e = str(report.at[idx, entry_col]).upper()
        if "BUY" in e:
            return True
        if "SELL" in e:
            return False
    if side_col:
        s = str(report.at[idx, side_col]).upper()
        if "LONG" in s:
            return True
        if "SHORT" in s:
            return False
    return None


def _vwap_adjust_pnl_cell(report, idx, pnl_col, delta: float) -> None:
    """Add ``delta`` (quote currency) to a realized_pnl cell, preserving its
    ``"<amount> <ccy>"`` Money-string shape so downstream parsing is unchanged."""
    amount, ccy = parse_money_string(report.at[idx, pnl_col])
    new_amount = amount + delta
    report.at[idx, pnl_col] = f"{new_amount} {ccy}" if ccy else new_amount


def _apply_vwap_fill(positions_report, fills_report, vwap_lookup) -> int:
    """Reprice SL/Target exit fills to the conservative VWAP-proxy price.

    Spec: 5. Logics/sl_tgt.html "SL & Target Fill Price Formula" —
    ``exit_price = vwap if vwap > hit_price else hit_price``. ``vwap`` is the
    opposite-quote-side per-bar typical price; ``hit_price`` is the
    trigger-side bar extreme:

      * long  leg (closed by selling at BID): vwap=bid typical,
        hit = ask_low (SL) / ask_high (Target)
      * short leg (closed by buying  at ASK): vwap=ask typical,
        hit = bid_high (SL) / bid_low (Target)

    Mutates ``positions_report``'s realized_pnl column in place (quote
    currency); the caller's existing FX conversion / metric code picks the
    change up. Returns the number of positions adjusted.
    """
    if not vwap_lookup or positions_report is None or fills_report is None:
        return 0
    if positions_report.empty or fills_report.empty:
        return 0
    pnl_col = _pick_col(positions_report, ["realized_pnl", "RealizedPnl", "pnl"])
    ts_col = _pick_col(positions_report, ["ts_closed", "ts_last"])
    close_col = _pick_col(positions_report, ["avg_px_close", "AvgPxClose", "avg_close"])
    if not pnl_col or not ts_col or not close_col:
        return 0

    # Map exit timestamp -> "sl" | "tp" from the close fills' structured tags.
    fill_ts_col = _pick_col(fills_report, ["ts_init", "ts_last", "ts_event"])
    tags_col = _pick_col(fills_report, ["tags", "Tags"])
    if not fill_ts_col or not tags_col:
        return 0
    exit_kind: dict[int, str] = {}
    for ts_raw, tg in zip(fills_report[fill_ts_col].tolist(),
                          fills_report[tags_col].tolist()):
        reason = _vwap_normalize_tag(tg).strip()
        if not reason:
            continue
        if reason.startswith(_VWAP_SL_PREFIXES):
            kind = "sl"
        elif reason.startswith(_VWAP_TP_PREFIXES):
            kind = "tp"
        else:
            continue
        ts_ns = _vwap_ts_to_ns(ts_raw)
        if ts_ns:
            exit_kind[ts_ns] = kind
    if not exit_kind:
        return 0

    ask = vwap_lookup.get("ask", {})
    bid = vwap_lookup.get("bid", {})
    qty_col = _pick_col(positions_report, ["peak_qty", "quantity", "Quantity"])
    sqty_col = _pick_col(positions_report, ["signed_qty", "SignedQty"])
    entry_col = _pick_col(positions_report, ["entry", "Entry"])
    side_col = _pick_col(positions_report, ["side", "Side"])

    adjusted = 0
    for idx in positions_report.index:
        ts_ns = _vwap_ts_to_ns(positions_report.at[idx, ts_col])
        kind = exit_kind.get(ts_ns)
        if kind is None:
            continue
        a = ask.get(ts_ns)
        b = bid.get(ts_ns)
        if a is None or b is None:
            continue  # one quote side missing for this bar — fall back
        was_long = _vwap_row_is_long(positions_report, idx, sqty_col, entry_col, side_col)
        if was_long is None:
            continue
        try:
            actual_px = float(positions_report.at[idx, close_col])
            qty = abs(float(positions_report.at[idx, qty_col])) if qty_col else 0.0
        except (TypeError, ValueError):
            continue
        if qty <= 0 or actual_px <= 0:
            continue
        ask_typ, ask_hi, ask_lo = a
        bid_typ, bid_hi, bid_lo = b
        if was_long:
            vwap = bid_typ
            hit = ask_lo if kind == "sl" else ask_hi
        else:
            vwap = ask_typ
            hit = bid_hi if kind == "sl" else bid_lo
        exit_px = vwap if vwap > hit else hit
        # Long pnl rises with the exit price; short pnl falls with it.
        delta = (exit_px - actual_px) * qty if was_long else (actual_px - exit_px) * qty
        if delta == 0.0:
            continue
        _vwap_adjust_pnl_cell(positions_report, idx, pnl_col, delta)
        adjusted += 1
    return adjusted
