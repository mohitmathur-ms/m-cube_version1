"""Per-slot and grouped-engine execution (Path A and Path B), plus the
picklable ProcessPoolExecutor worker and its SIGINT-ignoring initializer."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
import pandas as pd
from nautilus_trader.backtest.engine import BacktestEngine
from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import BacktestEngineConfig
from nautilus_trader.config import LoggingConfig
from nautilus_trader.model import TraderId
from nautilus_trader.model.currencies import USD
from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import AccountType
from nautilus_trader.model.enums import OmsType
from nautilus_trader.model.objects import Money
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from core.strategies import STRATEGY_REGISTRY
from core.models import StrategySlotConfig
from core.models import effective_slot_qty
from core.models import normalize_strategy_bar_types
from core.managed_strategy import ManagedExitStrategy
from core.managed_strategy import config_from_exit
from core.fx_rates import FxRateResolver
from core.venue_config import load_adapter_config_for_bar_type

from core.backtest_runner.bar_filters import (
    _allowed_weekdays,
    _filter_bars_after_ns,
    _filter_bars_by_time_of_day,
    _is_intraday_bar_type,
)
from core.backtest_runner.bar_types import (
    _aggregate_target_for_slot,
    _pair_bid_ask_bar_type,
)
from core.backtest_runner.data_cache import _cached_catalog_bars
from core.backtest_runner.exit_fill import (
    _apply_directional_close_fill,
    _apply_vwap_fill,
    _build_close_lookup,
    _build_vwap_lookup,
    _session_start_minute,
)
from core.backtest_runner.other_settings import _OtherSettings
from core.backtest_runner.path_b import (
    _build_run_config,
    _path_b_active,
    _path_b_supports_filters,
)
from core.backtest_runner.portfolio_clip import _build_underlying_curve
from core.backtest_runner.portfolio_exit_config import _MoveSLConfig
from core.backtest_runner.profiling import (
    _config_supports_aggregate_to,
    _config_supports_extra_bar_types,
    _phase,
)
from core.backtest_runner.rbo import _RBOSettings
from core.backtest_runner.report_utils import _pick_col
from core.backtest_runner.results import (
    _base_values_from_report,
    _extract_results,
    positions_report_with_base,
)
from core.backtest_runner.single_backtest import run_backtest


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
    default_directional_fill: bool = False,
    default_reexec_entry_price: float = 0.0,
    default_reexec_entry_was_long: bool = True,
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

    # Validate the slot's strategy timeframe against its data feed. A strategy
    # timeframe that is not strictly coarser than the base bar type yields a
    # degenerate INTERNAL aggregation (NautilusTrader emits ~zero bars, so the
    # strategy silently trades nothing). Collapse same/finer timeframes to the
    # base feed and surface a warning for the finer case.
    eff_strategy_bar_types, _sbt_warnings = normalize_strategy_bar_types(
        slot.bar_type_str, getattr(slot, "strategy_bar_types", None) or [],
    )

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
            # VWAP fill (spec §4.2/§3): index ASK/BID bars with the session-
            # cumulative volume-weighted VWAP before the bar list is dropped, so
            # _extract_results can reprice SL/Target exits. The session reset
            # boundary comes from the venue's session_start_time (UTC). None when
            # the flag is off or no ASK/BID data is present.
            vwap_lookup = (
                _build_vwap_lookup(all_bars, _session_start_minute(slot.bar_type_str))
                if (default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1")
                else None
            )
            # Directional-close fill (spec §8.1): index ASK/BID closes so the
            # exit fill base can be repriced to the directional close. None when
            # off or no ASK/BID data. Composes with the VWAP fill — VWAP owns the
            # SL/Target leg exits (§4.2), the directional close is the base for
            # the rest (squareoff/EOD, §8.1) — so it is built even when VWAP is on.
            close_lookup = (
                _build_close_lookup(all_bars)
                if (default_directional_fill
                    or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1")
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
                    subscribe_bar_types=eff_strategy_bar_types,
                    portfolio_id="",
                    slot_id=slot.slot_id,
                    # Portfolio "ReExecute at Entry Price" (spec §5.2): the replay
                    # injects the slot's pre-clip entry price so the first re-entry
                    # waits for price to return to it. 0 = plain ReExecute.
                    reexec_entry_price=default_reexec_entry_price,
                    reexec_entry_was_long=default_reexec_entry_was_long,
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
            results = _extract_results(engine, capital, fx_resolver, vwap_lookup, close_lookup)
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

        # Surface any strategy-timeframe repair warnings (finer-than-feed case).
        if _sbt_warnings:
            _msg = " ".join(_sbt_warnings)
            results["warning"] = (
                f"{results['warning']} {_msg}" if results.get("warning") else _msg
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
    close_lookup: dict | None = None,
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

    # Directional-close exit fill (spec §8.1) — base for every exit; composes
    # with the VWAP fill (vwap_active skips the SL/Target exits VWAP owns).
    directional_fill_adjustments = 0
    if close_lookup:
        try:
            directional_fill_adjustments = _apply_directional_close_fill(
                slot_positions, close_lookup, slot_fills,
                vwap_active=bool(vwap_lookup),
            )
        except Exception:
            directional_fill_adjustments = 0

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
        "directional_fill_applied": bool(directional_fill_adjustments),
        "directional_fill_adjustments": directional_fill_adjustments,
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

    # Whether every slot in the group is managed (has exit management, a
    # squareoff, or RBO). Mirrors the same flag in Path A's _run_slot_group:
    # when the whole group is managed, post-entry-window bars are kept so each
    # strategy can gate entries internally while still monitoring exits.
    _group_all_managed = all(
        (slot.exit_config.has_exit_management()
         or slot.exit_config.squareoff_time or slot.squareoff_time
         or default_squareoff_time or default_rbo_settings is not None)
        for slot, _cap in group
    )

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
    default_directional_fill: bool = False,
    default_reexec_entry_prices: dict | None = None,
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
            # VWAP fill (spec §4.2/§3): index ASK/BID bars with the session-
            # cumulative volume-weighted VWAP before the list is dropped, so each
            # slot's _extract_slot_from_group_reports can reprice SL/Target exits.
            # Grouped slots share a bar type → one session start. None when the
            # flag is off / no ASK-BID.
            vwap_lookup = (
                _build_vwap_lookup(
                    all_bars,
                    _session_start_minute(group[0][0].bar_type_str) if group else 0,
                )
                if (default_vwap_fill or os.environ.get("_USE_VWAP_FILL", "0") == "1")
                else None
            )
            # Directional-close fill (spec §8.1) — sibling of vwap_lookup; built
            # even when VWAP is on (they compose on disjoint exit sets).
            close_lookup = (
                _build_close_lookup(all_bars)
                if (default_directional_fill
                    or os.environ.get("_USE_DIRECTIONAL_FILL", "0") == "1")
                else None
            )
            del all_bars

        # Build and attach N strategies with deterministic unique order_id_tags
        expected_tags: list[str] = []
        # Per-slot strategy-timeframe validation/repair (see _run_single_slot).
        # Keyed by slot_id so warnings can be surfaced on each slot's result.
        _group_sbt_eff: dict[str, list[str]] = {}
        _group_sbt_warnings: dict[str, list[str]] = {}
        for _slot_in_grp, _ in group:
            _eff, _warns = normalize_strategy_bar_types(
                _slot_in_grp.bar_type_str,
                getattr(_slot_in_grp, "strategy_bar_types", None) or [],
            )
            _group_sbt_eff[_slot_in_grp.slot_id] = _eff
            _group_sbt_warnings[_slot_in_grp.slot_id] = _warns

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
                        subscribe_bar_types=_group_sbt_eff.get(slot.slot_id, []),
                        portfolio_id=portfolio_name,
                        slot_id=slot.slot_id,
                        # Portfolio "ReExecute at Entry Price" (spec §5.2) — per
                        # slot pre-clip entry price injected by the replay.
                        reexec_entry_price=(default_reexec_entry_prices or {}).get(
                            slot.slot_id, (0.0, True))[0],
                        reexec_entry_was_long=(default_reexec_entry_prices or {}).get(
                            slot.slot_id, (0.0, True))[1],
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
                    vwap_lookup, close_lookup,
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

                # Surface strategy-timeframe repair warnings (finer-than-feed).
                _sw = _group_sbt_warnings.get(slot.slot_id) or []
                if _sw:
                    _msg = " ".join(_sw)
                    r["warning"] = (
                        f"{r['warning']} {_msg}" if r.get("warning") else _msg
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
