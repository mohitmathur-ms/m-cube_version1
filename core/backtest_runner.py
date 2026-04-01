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

from core.strategies import STRATEGY_REGISTRY


def run_backtest(
    catalog_path: str,
    bar_type_str: str,
    strategy_name: str,
    strategy_params: dict,
    trade_size: float = 0.01,
    starting_capital: float = 100_000.0,
) -> dict:
    """
    Run a backtest using data from the catalog.

    Parameters
    ----------
    catalog_path : str
        Path to the ParquetDataCatalog.
    bar_type_str : str
        BarType string, e.g. "BTCUSD.YAHOO-1-DAY-LAST-EXTERNAL".
    strategy_name : str
        Name of strategy from STRATEGY_REGISTRY.
    strategy_params : dict
        Strategy-specific parameters.
    trade_size : float
        Position size per trade.
    starting_capital : float
        Starting account balance in USD.

    Returns
    -------
    dict
        Results including trades, account info, and performance metrics.
    """
    # Load data from catalog
    catalog = ParquetDataCatalog(catalog_path)
    bars = catalog.bars(bar_types=[bar_type_str])

    if not bars:
        raise ValueError(f"No bars found in catalog for {bar_type_str}")

    bar_type = BarType.from_str(bar_type_str)
    instrument_id = bar_type.instrument_id

    # Load instrument
    instruments = catalog.instruments()
    instrument = None
    for inst in instruments:
        if inst.id == instrument_id:
            instrument = inst
            break

    if instrument is None:
        raise ValueError(f"No instrument found for {instrument_id} in catalog")

    # Create engine
    engine_config = BacktestEngineConfig(
        trader_id=TraderId("BACKTESTER-001"),
        logging=LoggingConfig(log_level="WARNING"),
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

    # Add instrument and data
    engine.add_instrument(instrument)
    engine.add_data(bars)

    # Create strategy
    registry_entry = STRATEGY_REGISTRY[strategy_name]
    config_class = registry_entry["config_class"]

    config_kwargs = {
        "instrument_id": instrument_id,
        "bar_type": bar_type,
        "trade_size": Decimal(str(trade_size)),
        **strategy_params,
    }
    strategy_config = config_class(**config_kwargs)
    strategy = registry_entry["strategy_class"](strategy_config)

    engine.add_strategy(strategy)

    # Run backtest
    engine.run()
    engine.trader.stop()

    # Extract results
    results = _extract_results(engine, starting_capital)


    engine.dispose()

    return results


def _extract_results(engine: BacktestEngine, starting_capital: float) -> dict:
    """Extract backtest results from the engine."""
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

    # Get final balance
    accounts = list(engine.kernel.cache.accounts())
    final_balance = starting_capital
    if accounts:
        try:
            balance = accounts[0].balance_total(USD)
            if balance is not None:
                final_balance = float(balance)
        except Exception:
            pass

    total_pnl = final_balance - starting_capital
    total_return_pct = (total_pnl / starting_capital) * 100 if starting_capital > 0 else 0

    # Count trades from order fills (round-trip analysis)
    orders = engine.kernel.cache.orders()
    filled_orders = [o for o in orders if o.is_closed]
    total_orders = len(filled_orders)

    # In NETTING mode, positions net together so we can't reliably count
    # individual wins/losses from positions. Instead, derive from fills report.
    wins = 0
    losses = 0
    total_trades = 0

    if fills_report is not None and not fills_report.empty:
        total_trades = total_orders // 2

    positions = engine.kernel.cache.positions()
    closed_positions = [p for p in positions if p.is_closed]

    if len(closed_positions) > 1:
        for pos in closed_positions:
            pnl = float(pos.realized_pnl)
            if pnl > 0:
                wins += 1
            elif pnl < 0:
                losses += 1
        total_trades = wins + losses
    elif total_trades > 0:
        if total_pnl > 0:
            wins = 1
            losses = 0
        elif total_pnl < 0:
            wins = 0
            losses = 1
        total_trades = 1

    win_rate = (wins / total_trades * 100) if total_trades > 0 else 0


    return {
        "starting_capital": starting_capital,
        "final_balance": final_balance,
        "total_pnl": total_pnl,
        "total_return_pct": total_return_pct,
        "total_orders": total_orders,
        "total_trades": total_trades,
        "wins": wins,
        "losses": losses,
        "win_rate": win_rate,
        "fills_report": fills_report,
        "positions_report": positions_report,
        "account_report": account_report,
    }
