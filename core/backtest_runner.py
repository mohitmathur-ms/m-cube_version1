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
from core.models import PortfolioConfig, StrategySlotConfig, effective_slot_qty
from core.managed_strategy import ManagedExitStrategy, config_from_exit
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
    action: str = "SqOff"  # SqOff | ReExecute
    delay_sec: int = 0
    reexecute_count: int = 0  # 0 = unlimited per spec §1.7
    sqoff_only_loss_legs: bool = False
    sqoff_only_profit_legs: bool = False
    trail_enabled: bool = False
    trail_every: float = 0.0
    trail_by: float = 0.0


@dataclasses.dataclass(frozen=True)
class _PfTargetSettings:
    """Validated portfolio-level Target config."""
    enabled: bool = False
    value: float = 0.0
    action: str = "SqOff"
    delay_sec: int = 0
    reexecute_count: int = 0
    trail_enabled: bool = False
    trail_lock_min_profit: float = 0.0
    trail_when_profit_reach: float = 0.0
    trail_every: float = 0.0
    trail_by: float = 0.0


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


_VALID_PF_SL_TYPES_FX = ("Combined Loss",)
_VALID_PF_TGT_TYPES_FX = ("Combined Profit",)
_OPTIONS_ONLY_PF_SL_TYPES = ("Combined Premium", "Absolute Combined Premium",
                             "Underlying Movement", "Loss and Underlying Range")
_OPTIONS_ONLY_PF_TGT_TYPES = ("Combined Premium", "Absolute Combined Premium",
                              "Underlying Movement")
_VALID_PF_ACTIONS_FX = ("SqOff", "ReExecute")
_OPTIONS_ONLY_PF_ACTIONS = ("SqOff Other Portfolio", "Execute Other Portfolio",
                            "Start Other Portfolio", "ReExecute at Entry Price",
                            "ReExecute SameStrike at EntryPrice")
_VALID_MOVE_SL_ACTIONS_FX = ("Move Only for Profitable Legs",
                             "Move SL for All Legs Despite Loss/Profit")
_OPTIONS_ONLY_MOVE_SL_ACTIONS = ("Move SL to LTP + Buffer for Loss Making Legs",)


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

    action = getattr(portfolio, "pf_sl_action", "SqOff") or "SqOff"
    if action in _OPTIONS_ONLY_PF_ACTIONS:
        warnings.append(
            f"pf_sl_action={action!r} requires options/cross-portfolio infrastructure "
            f"not implemented for FX/crypto; downgraded to 'SqOff'."
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

    return _PfStoplossSettings(
        enabled=True, value=value, action=action, delay_sec=delay,
        reexecute_count=reexec,
        sqoff_only_loss_legs=sqoff_loss,
        sqoff_only_profit_legs=sqoff_profit,
        trail_enabled=trail_enabled,
        trail_every=trail_every, trail_by=trail_by,
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

    action = getattr(portfolio, "pf_tgt_action", "SqOff") or "SqOff"
    if action in _OPTIONS_ONLY_PF_ACTIONS:
        warnings.append(
            f"pf_tgt_action={action!r} requires infrastructure not implemented "
            f"for FX/crypto; downgraded to 'SqOff'."
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

    return _PfTargetSettings(
        enabled=True, value=value, action=action, delay_sec=delay,
        reexecute_count=reexec,
        trail_enabled=trail_enabled,
        trail_lock_min_profit=trail_lock,
        trail_when_profit_reach=trail_reach,
        trail_every=trail_every, trail_by=trail_by,
    ), warnings


def _resolve_move_sl_to_cost(portfolio) -> tuple[_MoveSLConfig, list[str]]:
    """Validate portfolio.move_sl_* fields per portfolio_sl_tgt.html §3.

    Returns the per-slot config (threaded into ManagedExitConfig). Default-off
    is the no-op state.
    """
    warnings: list[str] = []
    # no_reexec_sl_cost is read regardless of move_sl_enabled — see dataclass docs.
    no_reexec_sl_cost = bool(getattr(portfolio, "no_reexec_sl_cost", False))

    if not getattr(portfolio, "move_sl_enabled", False):
        return _MoveSLConfig(enabled=False, no_reexec_sl_cost=no_reexec_sl_cost), warnings

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

    return _MoveSLConfig(
        enabled=True, safety_sec=safety, action=action,
        trail_after=bool(getattr(portfolio, "move_sl_trail_after", False)),
        no_buy_legs=bool(getattr(portfolio, "move_sl_no_buy_legs", False)),
        no_reexec_sl_cost=no_reexec_sl_cost,
    ), warnings


@dataclasses.dataclass(frozen=True)
class _ClipResult:
    """Outcome of _apply_portfolio_clip — drives report-level enforcement
    of portfolio Stoploss/Target. ``clip_ts is None`` means no enforcement
    fired (or feature disabled). ``clipped_slots`` is the set of slot_ids
    whose trades after clip_ts should be dropped — for full SqOff that's
    every enabled slot, for selective SqOff it's only the matching subset.
    """
    clip_ts: str | None = None  # ISO UTC string from equity_curve_ts
    clip_reason: str | None = None  # STOPLOSS | STOPLOSS_TRAIL | TARGET | TARGET_TRAIL
    clip_action: str | None = None  # SqOff | ReExecute
    clipped_slots: tuple[str, ...] = ()
    would_reexecute: bool = False
    reexec_count: int = 0
    logs: tuple[str, ...] = ()


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
                    # Fire — clip happens at the original pending_clip_ts
                    return _build_clip_result(
                        clip_ts=pending_clip_ts, reason=pending_reason,
                        pf_sl=pf_sl, pf_tgt=pf_tgt,
                        slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                    )

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
                # No delay — clip immediately
                return _build_clip_result(
                    clip_ts=ts, reason=new_hit_reason,
                    pf_sl=pf_sl, pf_tgt=pf_tgt,
                    slot_pnl_at_clip=slot_pnl_at_clip, logs=logs,
                )

    # Walked the whole curve without firing
    return _ClipResult(logs=tuple(logs))


def _build_clip_result(
    clip_ts: str,
    reason: str,
    pf_sl: "_PfStoplossSettings",
    pf_tgt: "_PfTargetSettings",
    slot_pnl_at_clip: dict | None,
    logs: list,
) -> _ClipResult:
    """Construct the ClipResult, applying selective SqOff filtering and
    classifying the action. Trailing-Target hits ignore configured action
    (always SqOff per spec §5)."""
    if reason.startswith("STOPLOSS"):
        action = pf_sl.action
        sqoff_loss = pf_sl.sqoff_only_loss_legs
        sqoff_profit = pf_sl.sqoff_only_profit_legs
    else:
        action = "SqOff" if reason == "TARGET_TRAIL" else pf_tgt.action
        sqoff_loss = False  # selective filters are SL-only per spec §1.9-§1.10
        sqoff_profit = False

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

    would_reexec = action == "ReExecute"
    if would_reexec:
        logs.append(f"PORTFOLIO_REEXECUTE | Reason={reason} | Count=1 (v1: clip + flag, no replay)")

    return _ClipResult(
        clip_ts=clip_ts, clip_reason=reason, clip_action=action,
        clipped_slots=clipped,
        would_reexecute=would_reexec, reexec_count=1 if would_reexec else 0,
        logs=tuple(logs),
    )


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

    # Day-of-week filter (portfolio.run_on_days). Applied after loading so
    # the LRU cache stays per-(bar_type, range) and isn't fragmented by the
    # day filter. None = no filter; empty set = portfolio explicitly disabled
    # all weekdays for this slot.
    allowed_weekdays = _allowed_weekdays(default_run_on_days)
    bars_filtered_by_run_on_days = 0
    with _phase("run_on_days_filter", phase_times):
        all_bars, bars_filtered_by_run_on_days = _filter_bars_by_weekday(
            all_bars, allowed_weekdays
        )

    # Intra-day entry window (portfolio.entry_start_time / .entry_end_time).
    # Both endpoints UTC and inclusive. Either may be None for unbounded.
    # Skipped for non-intraday bar types (daily/weekly/monthly) since their
    # ts_event is at the period start — applying an HH:MM window to daily
    # bars would unconditionally drop every bar.
    bars_filtered_by_entry_window = 0
    entry_window_skipped_reason: str | None = None
    with _phase("entry_window_filter", phase_times):
        if (default_entry_start_time or default_entry_end_time) and not _is_intraday_bar_type(slot.bar_type_str):
            entry_window_skipped_reason = (
                f"bar type {slot.bar_type_str} is not intraday — entry window ignored"
            )
        else:
            all_bars, bars_filtered_by_entry_window = _filter_bars_by_time_of_day(
                all_bars, default_entry_start_time, default_entry_end_time
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
            # lives there.
            slot_qty = effective_slot_qty(slot, user_id)
            if slot.exit_config.has_exit_management() or eff_squareoff_time or default_rbo_settings is not None:
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
            results = _extract_results(engine, capital, fx_resolver)

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
) -> dict:
    """Build one slot's result dict by filtering a shared-engine's reports by strategy_id.

    Output shape matches ``_run_single_slot`` so ``_merge_portfolio_results``
    can consume it identically. Equity curve is synthesized from this slot's
    position closes (running balance = capital + cumulative realized PnL) since
    the shared engine only has one account-balance history.
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

    # Day-of-week filter mirroring _run_single_slot. Applied per-group rather
    # than per-slot because all slots in the group share the same bars.
    allowed_weekdays = _allowed_weekdays(default_run_on_days)
    bars_filtered_by_run_on_days = 0
    with _phase("run_on_days_filter", phase_times):
        all_bars, bars_filtered_by_run_on_days = _filter_bars_by_weekday(
            all_bars, allowed_weekdays
        )

    # Intra-day entry window filter (mirrors _run_single_slot). Skipped when
    # the group's primary bar type is non-intraday (daily/weekly/monthly).
    bars_filtered_by_entry_window = 0
    entry_window_skipped_reason: str | None = None
    with _phase("entry_window_filter", phase_times):
        if (default_entry_start_time or default_entry_end_time) and not _is_intraday_bar_type(primary_bar_type_str):
            entry_window_skipped_reason = (
                f"bar type {primary_bar_type_str} is not intraday — entry window ignored"
            )
        else:
            all_bars, bars_filtered_by_entry_window = _filter_bars_by_time_of_day(
                all_bars, default_entry_start_time, default_entry_end_time
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

    enabled_slots = portfolio.enabled_slots

    if not enabled_slots:
        raise ValueError("No enabled strategy slots in portfolio")

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

    # Run all slots in parallel
    slot_results = {}
    errors = []

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
                        default_squareoff_time=portfolio.squareoff_time,
                        default_squareoff_tz=portfolio.squareoff_tz,
                        default_run_on_days=portfolio.run_on_days,
                        default_entry_start_time=portfolio.entry_start_time,
                        default_entry_end_time=portfolio.entry_end_time,
                        default_rbo_settings=rbo_settings,
                        default_other_settings=other_settings,
                        default_move_sl_settings=move_sl_settings,
                        user_id=user_id,
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
                        default_squareoff_time=portfolio.squareoff_time,
                        default_squareoff_tz=portfolio.squareoff_tz,
                        default_run_on_days=portfolio.run_on_days,
                        default_entry_start_time=portfolio.entry_start_time,
                        default_entry_end_time=portfolio.entry_end_time,
                        default_rbo_settings=rbo_settings,
                        default_other_settings=other_settings,
                        default_move_sl_settings=move_sl_settings,
                        user_id=user_id,
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
                    default_squareoff_time=portfolio.squareoff_time,
                    default_squareoff_tz=portfolio.squareoff_tz,
                    default_run_on_days=portfolio.run_on_days,
                    default_entry_start_time=portfolio.entry_start_time,
                    default_entry_end_time=portfolio.entry_end_time,
                    default_rbo_settings=rbo_settings,
                    default_other_settings=other_settings,
                    default_move_sl_settings=move_sl_settings,
                    user_id=user_id,
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
                            if elapsed is not None:
                                runtime_history.record(
                                    history, slot.bar_type_str, slot.strategy_name,
                                    float(elapsed), _span_days(slot),
                                )
                            if on_slot_complete:
                                try:
                                    on_slot_complete(slot.slot_id)
                                except Exception:
                                    pass
                    else:
                        slot = slots_in_future[0]
                        slot_results[slot.slot_id] = result
                        elapsed = result.get("elapsed_seconds")
                        if elapsed is not None:
                            runtime_history.record(
                                history, slot.bar_type_str, slot.strategy_name,
                                float(elapsed), _span_days(slot),
                            )
                        if on_slot_complete:
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

    # Persist once after the whole run — cheap single JSON write.
    try:
        runtime_history.save(history)
    except Exception:
        pass

    if not slot_results and errors:
        raise ValueError(f"All slots failed: {errors}")

    # Merge results into portfolio-level metrics
    return _merge_portfolio_results(portfolio, slot_results, capitals, errors)


def _merge_portfolio_results(
    portfolio: PortfolioConfig,
    slot_results: dict,
    capitals: dict,
    errors: list,
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

    # Portfolio stop flags (informational user-level caps, separate from the
    # full Stoploss/Target machinery below — analogous to spec §10 Global
    # User Limits).
    max_loss_hit = portfolio.max_loss is not None and total_pnl <= -abs(portfolio.max_loss)
    max_profit_hit = portfolio.max_profit is not None and total_pnl >= portfolio.max_profit

    # Portfolio-level Stoploss / Target post-hoc clip. Spec:
    # 5. Logics/portfolio_sl_tgt.html. Walks the merged equity curve, finds
    # the trigger point, drops post-clip trades from the merged outputs.
    pf_sl_settings, pf_sl_warnings = _resolve_pf_stoploss(portfolio)
    pf_tgt_settings, pf_tgt_warnings = _resolve_pf_target(portfolio)
    for w in pf_sl_warnings:
        print(f"[PF_SL] {w}")
    for w in pf_tgt_warnings:
        print(f"[PF_TGT] {w}")

    clip_result = _ClipResult()
    if pf_sl_settings.enabled or pf_tgt_settings.enabled:
        # Compute per-slot final P&L for selective sqoff (used at clip-point
        # to decide which slots to clip — uses end-of-run P&L as a proxy for
        # P&L at clip_ts, which is close enough for v1 since selective sqoff
        # at the clip moment matters only for which slots survive past it).
        slot_pnl_at_clip = {
            slot.slot_id: float(slot_results[slot.slot_id]["total_pnl"])
            for slot in portfolio.enabled_slots
            if slot_results.get(slot.slot_id) is not None
        }
        clip_result = _apply_portfolio_clip(
            equity_curve_ts, portfolio.starting_capital,
            pf_sl_settings, pf_tgt_settings, slot_pnl_at_clip,
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

    # Portfolio stop flags
    max_loss_hit = False
    max_profit_hit = False
    if portfolio.max_loss is not None and total_pnl_with_unrealized <= -abs(portfolio.max_loss):
        max_loss_hit = True
    if portfolio.max_profit is not None and total_pnl_with_unrealized >= portfolio.max_profit:
        max_profit_hit = True

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
) -> dict:
    """Extract backtest results from the engine, converting per-position PnL
    into the account base currency via the supplied FX resolver.

    Without a resolver, results use engine-native numbers (identical to the
    pre-FX-aware behavior) — safe default for USD-only catalogs.
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
