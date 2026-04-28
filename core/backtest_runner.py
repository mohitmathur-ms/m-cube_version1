"""
Backtest runner: configure and execute backtests using NautilusTrader's BacktestEngine.

Loads data from the ParquetDataCatalog, runs a selected strategy, and returns results.
"""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path

import pandas as pd

from nautilus_trader.backtest.engine import BacktestEngine
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
from core.models import PortfolioConfig, StrategySlotConfig
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


def _pair_bid_ask_bar_type(bt_str: str) -> str | None:
    """Return the opposite-side bar type string, or None if not BID/ASK.

    Nautilus's matching engine needs both quote sides to fill FX market
    orders. A strategy subscribed only to BID sees no fills unless ASK is
    also loaded (and vice versa). MID/LAST bar types don't need a pair.
    """
    if "-BID-" in bt_str:
        return bt_str.replace("-BID-", "-ASK-", 1)
    if "-ASK-" in bt_str:
        return bt_str.replace("-ASK-", "-BID-", 1)
    return None


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
    # Normalize to list
    if isinstance(bar_type_str, str):
        bar_type_strs = [bar_type_str]
    else:
        bar_type_strs = list(bar_type_str)

    # Route through the worker-local LRU so a second strategy on the same
    # (instrument, date range) inside the same worker skips the parquet read.
    catalog = ParquetDataCatalog(catalog_path)
    bars = []
    for bt_str in bar_type_strs:
        cached = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
        bars.extend(cached)

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

    config_kwargs = {
        "instrument_id": instrument_id,
        "bar_type": primary_bar_type,
        "trade_size": Decimal(str(trade_size)),
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
) -> dict:
    """Top-level worker for ProcessPoolExecutor.

    Custom strategy classes (loaded dynamically via importlib) are not picklable
    across process boundaries, so each worker reloads the merged registry from
    the custom strategies directory before invoking run_backtest.
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
) -> dict:
    """Run a single strategy slot in its own engine.

    Top-level and picklable so it can run under ProcessPoolExecutor.
    Rebuilds the merged registry inside the worker because custom strategy
    classes loaded via importlib are not picklable across processes.
    """
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
        # Load the slot's primary bar type + its BID/ASK pair if one exists.
        # Nautilus's matching engine needs the opposite quote side to fill FX
        # market orders. For MID/LAST slots _pair_bid_ask_bar_type returns None
        # and we load a single bar type as before.
        bar_type_strs_to_load = [slot.bar_type_str]
        pair = _pair_bid_ask_bar_type(slot.bar_type_str)
        if pair is not None:
            bar_type_strs_to_load.append(pair)

        all_bars = []
        for bt_str in bar_type_strs_to_load:
            try:
                cached = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
            except Exception:
                # Paired side absent from catalog is fine — primary-side failure
                # falls through to the empty-bars check below.
                cached = []
            all_bars.extend(cached)

    if not all_bars:
        raise ValueError(
            f"No bars in date range {start_date or 'start'}..{end_date or 'end'} for {slot.bar_type_str}"
        )

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
            # ManagedExitStrategy is required when SL/TP/trailing OR squareoff is
            # set — squareoff alone (no SL/TP) still needs the wrapper because the
            # raw strategy classes don't know how to time-close.
            if slot.exit_config.has_exit_management() or eff_squareoff_time:
                managed_config = config_from_exit(
                    exit_config=slot.exit_config,
                    signal_name=slot.strategy_name,
                    signal_params=slot.strategy_params,
                    instrument_id=instrument_id,
                    bar_type=primary_bt,
                    trade_size=slot.trade_size,
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
                    "trade_size": Decimal(str(slot.trade_size)),
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
    total_realized = float(sum(pnl_values))
    final_balance = capital + total_realized
    total_return_pct = (total_realized / capital) * 100 if capital > 0 else 0.0
    win_rate = (wins / trades * 100) if trades > 0 else 0.0

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
        "win_rate": win_rate,
        "max_drawdown": max_dd,
        "equity_curve": balances,
        "equity_curve_ts": equity_curve_ts,
        "positions_report": slot_positions,
        "fills_report": slot_fills,
        "account_report": None,  # shared in a group, not per-slot
    }


def _run_slot_group(
    catalog_path: str,
    group: list,
    custom_strategies_dir: str | None,
    group_index: int,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
    default_squareoff_time: str | None = None,
    default_squareoff_tz: str | None = None,
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
        pair = _pair_bid_ask_bar_type(primary_bar_type_str)
        if pair is not None:
            bar_type_strs_to_load.append(pair)
        all_bars = []
        for bt_str in bar_type_strs_to_load:
            try:
                cached = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
            except Exception:
                cached = []
            all_bars.extend(cached)

    if not all_bars:
        raise ValueError(
            f"No bars in date range {start_date or 'start'}..{end_date or 'end'} "
            f"for group primary bar_type {primary_bar_type_str}"
        )

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

                if slot.exit_config.has_exit_management() or eff_squareoff_time:
                    managed_config = config_from_exit(
                        exit_config=slot.exit_config,
                        signal_name=slot.strategy_name,
                        signal_params=slot.strategy_params,
                        instrument_id=instrument_id,
                        bar_type=primary_bt,
                        trade_size=slot.trade_size,
                        order_id_tag=order_tag,
                        squareoff_time=eff_squareoff_time,
                        squareoff_tz=eff_squareoff_tz,
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
                        "trade_size": Decimal(str(slot.trade_size)),
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
) -> dict:
    """
    Run a portfolio backtest with multiple strategy slots in parallel.

    Each slot runs in its own engine with allocated capital.
    Results are merged into portfolio-level metrics.
    """
    import os
    from concurrent.futures import ProcessPoolExecutor, as_completed

    enabled_slots = portfolio.enabled_slots

    if not enabled_slots:
        raise ValueError("No enabled strategy slots in portfolio")

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
            "win_rate": r["win_rate"],
            "trade_pnls": trade_pnls,
            "allocated_capital": capitals.get(slot.slot_id, 0),
            "elapsed_seconds": r.get("elapsed_seconds"),
            "worker_pid": r.get("worker_pid"),
            "cache_hits": r.get("cache_hits"),
            "cache_misses": r.get("cache_misses"),
            "cache_currsize": r.get("cache_currsize"),
            "worker_rss_mb": r.get("worker_rss_mb"),
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

    # Merge DataFrames
    merged_positions = pd.concat(all_positions_reports, ignore_index=True) if all_positions_reports else pd.DataFrame()
    merged_fills = pd.concat(all_fills_reports, ignore_index=True) if all_fills_reports else pd.DataFrame()

    # Portfolio stop flags
    max_loss_hit = portfolio.max_loss is not None and total_pnl <= -abs(portfolio.max_loss)
    max_profit_hit = portfolio.max_profit is not None and total_pnl >= portfolio.max_profit

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

    return {
        "starting_capital": portfolio.starting_capital,
        "final_balance": final_balance,
        "total_pnl": total_pnl,
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
        "allocation_mode": portfolio.allocation_mode,
        "fills_report": merged_fills,
        "positions_report": merged_positions,
        "account_report": None,
        "slot_to_strategy_id": slot_to_strategy_id,
        "slot_to_trader_id": slot_to_trader_id,
        "errors": errors,
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

    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0

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
        "win_rate": win_rate,
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
