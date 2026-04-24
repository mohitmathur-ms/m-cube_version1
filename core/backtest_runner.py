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

import functools


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


@functools.lru_cache(maxsize=4)
def _cached_catalog_bars(catalog_path: str, bt_str: str, start_iso: str | None, end_iso: str | None):
    """Worker-local bar cache.

    Multiple slots sharing the same (bar_type, date range) within the same
    worker process hit this cache and skip the parquet read. Cache lives for
    the worker's lifetime (ProcessPoolExecutor keeps workers alive between tasks).

    maxsize sized for one slot's working set: BID + ASK + up to two extra TFs.
    A year of 1-min bars is ~75 MB per side, so cap holds peak per-worker
    cache footprint near ~300 MB instead of growing toward several GB. Cache
    misses cost ~0.5 s of parquet read per year-long slot — rounding error
    against engine.run() time.
    """
    catalog = ParquetDataCatalog(catalog_path)
    start_arg = pd.Timestamp(start_iso, tz="UTC") if start_iso else None
    end_arg = (
        pd.Timestamp(end_iso, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)
    ) if end_iso else None
    return catalog.bars(bar_types=[bt_str], start=start_arg, end=end_arg)


def _auto_pair_bid_ask(bar_type_strs: list[str], catalog_path: str) -> list[str]:
    """Auto-detect BID/ASK bar types and include matching pair if it exists in catalog.

    Raises ValueError if a BID/ASK bar type is found but its matching pair is missing.
    """
    paired = list(bar_type_strs)
    for bt_str in bar_type_strs:
        if "-BID-" in bt_str:
            pair = bt_str.replace("-BID-", "-ASK-")
            missing_label = "ASK"
        elif "-ASK-" in bt_str:
            pair = bt_str.replace("-ASK-", "-BID-")
            missing_label = "BID"
        else:
            continue
        if pair not in paired:
            pair_dir = Path(catalog_path) / "data" / "bar" / pair
            if pair_dir.exists():
                paired.append(pair)
            else:
                instrument = bt_str.split("-")[0]
                raise ValueError(
                    f"Forex backtest requires both BID and ASK data. "
                    f"{missing_label} data is missing for {instrument}. "
                    f"Please upload the {missing_label} CSV file first."
                )
    return paired


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

    # Auto-pair BID/ASK: if BID selected, auto-load matching ASK (and vice versa)
    bar_type_strs = _auto_pair_bid_ask(bar_type_strs, catalog_path)

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


def _run_single_slot(
    catalog_path: str,
    slot: StrategySlotConfig,
    capital: float,
    custom_strategies_dir: str | None,
    slot_index: int,
    default_start_date: str | None = None,
    default_end_date: str | None = None,
) -> dict:
    """Run a single strategy slot in its own engine.

    Top-level and picklable so it can run under ProcessPoolExecutor.
    Rebuilds the merged registry inside the worker because custom strategy
    classes loaded via importlib are not picklable across processes.
    """
    import os
    import time as _time
    _t_slot_start = _time.time()

    if custom_strategies_dir:
        from core.custom_strategy_loader import get_merged_registry
        registry, _ = get_merged_registry(Path(custom_strategies_dir))
    else:
        registry = STRATEGY_REGISTRY

    # Auto-pair BID/ASK
    all_bt_strs = _auto_pair_bid_ask([slot.bar_type_str], catalog_path)

    catalog = ParquetDataCatalog(catalog_path)

    # Resolve date range: slot-level override wins, else fall back to the
    # portfolio-level default. Lets users pick a custom range once at the
    # portfolio level without touching every slot.
    start_date = slot.start_date or default_start_date
    end_date = slot.end_date or default_end_date

    # Load bars and instruments. Bars are served from the worker-local LRU cache
    # so a second slot on the same worker with same (bar_type, start, end) skips
    # the parquet read entirely.
    # Build instrument_id -> instrument map once so the per-bar-type lookup below
    # is O(1) instead of a full catalog scan per bar type.
    instrument_map = {inst.id: inst for inst in catalog.instruments()}
    all_bars = []
    instrument = None
    for bt_str in all_bt_strs:
        cached_bars = _cached_catalog_bars(catalog_path, bt_str, start_date, end_date)
        if not cached_bars:
            raise ValueError(
                f"No bars in date range {start_date or 'start'}..{end_date or 'end'} for {bt_str}"
            )
        # Shallow-copy so each engine receives its own list (Nautilus may mutate).
        all_bars.extend(cached_bars)

        if instrument is None:
            bt = BarType.from_str(bt_str)
            instrument = instrument_map.get(bt.instrument_id)

    if instrument is None:
        raise ValueError(f"No instrument found for {slot.bar_type_str}")

    primary_bt = BarType.from_str(slot.bar_type_str)
    instrument_id = primary_bt.instrument_id
    extra_bar_types = [BarType.from_str(s) for s in all_bt_strs if s != slot.bar_type_str] or None

    # Create engine with aggressive per-run performance tuning:
    #   - bypass_logging: skip all kernel/strategy log emission
    #   - RiskEngineConfig(bypass=True): skip per-order risk checks (OK for controlled backtests)
    #   - run_analysis=False: skip built-in post-run analytics; we compute our own metrics
    from nautilus_trader.config import RiskEngineConfig
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

    # Create strategy
    reg = registry
    if slot.exit_config.has_exit_management():
        managed_config = config_from_exit(
            exit_config=slot.exit_config,
            signal_name=slot.strategy_name,
            signal_params=slot.strategy_params,
            instrument_id=instrument_id,
            bar_type=primary_bt,
            trade_size=slot.trade_size,
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
    engine.run()

    # FX resolver built from the slot's venue config (see run_backtest for rationale).
    adapter_cfg = load_adapter_config_for_bar_type(slot.bar_type_str)
    fx_resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)

    # Extract results using existing function
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

    engine.dispose()

    return results


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

    sorted_slots = sorted(enabled_slots, key=_duration_estimate, reverse=True)

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {}
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
            )
            futures[future] = slot

        for future in as_completed(futures):
            slot = futures[future]
            try:
                result = future.result()
                slot_results[slot.slot_id] = result
                # Accumulate into the runtime history for next-run LPT.
                elapsed = result.get("elapsed_seconds")
                if elapsed is not None:
                    runtime_history.record(
                        history, slot.bar_type_str, slot.strategy_name,
                        float(elapsed), _span_days(slot),
                    )
            except Exception as e:
                errors.append({"slot_id": slot.slot_id, "display_name": slot.display_name,
                               "error": str(e)})
            # Invoke callback in the main process once each slot resolves
            if on_slot_complete:
                try:
                    on_slot_complete(slot.slot_id)
                except Exception:
                    pass

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
        trade_pnls = []
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
                for _, row in pos_report.iterrows():
                    val = row[pnl_col]
                    if isinstance(val, (int, float)):
                        trade_pnls.append(float(val))
                        continue
                    try:
                        trade_pnls.append(float(str(val).split()[0]))
                    except (ValueError, IndexError):
                        trade_pnls.append(0.0)

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
        "errors": errors,
    }


def _merge_equity_curves(curves: list[list[dict]]) -> list[dict]:
    """Merge multiple timestamped equity curves by summing balances at each timestamp."""
    if not curves:
        return []
    if len(curves) == 1:
        return curves[0]

    # Collect all timestamps and build per-curve balance lookup
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
