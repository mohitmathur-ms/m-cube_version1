"""Single-strategy entry points: run_backtest (Path A / Path B routing) and
the run_backtest_node Path B variant."""

from __future__ import annotations

from decimal import Decimal
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
from core.fx_rates import FxRateResolver
from core.venue_config import load_adapter_config_for_bar_type

from core.backtest_runner.bar_types import _pair_bid_ask_bar_type
from core.backtest_runner.data_cache import _cached_catalog_bars
from core.backtest_runner.path_b import (
    _build_run_config,
    _path_b_active,
)
from core.backtest_runner.profiling import (
    _config_supports_aggregate_to,
    _config_supports_extra_bar_types,
)
from core.backtest_runner.results import _extract_results


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
