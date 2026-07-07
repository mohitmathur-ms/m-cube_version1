"""Configure and execute backtests using NautilusTrader's BacktestEngine.

Loads data from the ParquetDataCatalog, runs single-strategy and multi-slot
portfolio backtests with exit management, and returns result dicts. This
package decomposes the former monolithic ``core/backtest_runner.py`` into
single-responsibility components (see README.md); each subdirectory owns one
logical concern, with that concern's logic living directly in the directory's
``__init__.py``. The public API is unchanged after the split — every former
module-level symbol is re-exported here so ``from core.backtest_runner import X``
and ``core.backtest_runner.X`` continue to resolve for callers and tests.
"""

from __future__ import annotations

# Re-exported third-party / Nautilus names — preserve the former module
# namespace so ``core.backtest_runner.BacktestRunConfig`` (and siblings still
# referenced by verify_session_changes.py and other callers) keep resolving.
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
from core.models import PortfolioConfig
from core.models import StrategySlotConfig
from core.models import effective_portfolio_squareoff
from core.models import effective_slot_qty
from core.models import normalize_strategy_bar_types
from core.managed_strategy import ManagedExitStrategy
from core.managed_strategy import advance_trailing_target
from core.managed_strategy import config_from_exit
from core.fx_rates import FxRateResolver
from core.fx_rates import parse_money_string
from core.venue_config import load_adapter_config_for_bar_type
import contextlib
import functools
import time as _time_mod

from core.backtest_runner.bar_filters import (
    _DAY_NAME_TO_WEEKDAY,
    _EPOCH_WEEKDAY,
    _NANOS_PER_DAY,
    _NANOS_PER_MINUTE,
    _allowed_weekdays,
    _filter_bars_after_ns,
    _filter_bars_by_time_of_day,
    _filter_bars_by_weekday,
    _hhmm_to_minute,
    _is_intraday_bar_type,
)
from core.backtest_runner.bar_types import (
    _aggregate_target_for_slot,
    _group_slots,
    _pair_bid_ask_bar_type,
)
from core.backtest_runner.cross_portfolio import (
    _CROSS_PORTFOLIO_ACTIONS,
    _CROSS_PORTFOLIO_EVENT_BUS,
    _ENTRY_PRICE_REEXEC_ACTIONS,
    _LEGACY_PF_ACTION_ALIASES,
    _OPTIONS_ONLY_PF_ACTIONS,
    _REEXECUTE_FAMILY_ACTIONS,
    _VALID_PF_ACTIONS_FX,
    _is_entry_price_reexec,
    _is_reexec_action,
    _normalize_pf_action,
    clear_cross_portfolio_bus,
    consume_cross_portfolio_events,
    publish_cross_portfolio_event,
)
from core.backtest_runner.data_cache import (
    _cached_catalog_bars,
)
from core.backtest_runner.equity_curves import (
    _build_equity_curve_from_account,
    _ensure_final_equity_point,
    _merge_equity_curves,
)
from core.backtest_runner.exit_fill import (
    _VWAP_SL_PREFIXES,
    _VWAP_TP_PREFIXES,
    _apply_directional_close_fill,
    _apply_vwap_fill,
    _build_close_lookup,
    _build_vwap_lookup,
    _session_start_minute,
    _vwap_adjust_pnl_cell,
    _vwap_normalize_tag,
    _vwap_row_is_long,
    _vwap_session_bucket,
    _vwap_ts_to_ns,
)
from core.backtest_runner.orchestration import (
    run_portfolio_backtest,
)
from core.backtest_runner.other_settings import (
    _OtherSettings,
    _VALID_ON_SL_ACTION_ON,
    _VALID_ON_TARGET_ACTION_ON,
    _resolve_other_settings,
)
from core.backtest_runner.path_b import (
    _build_run_config,
    _chunk_data_configs_for_path_b,
    _path_b_active,
    _path_b_supports_filters,
    _sec_to_hms,
)
from core.backtest_runner.portfolio_clip import (
    _AggCoordination,
    _ClipResult,
    _apply_portfolio_clip,
    _build_clip_result,
    _build_underlying_curve,
    _compute_agg_coordination,
    _earliest_clip,
    _entry_at_clip,
    _slot_pnl_at_ts,
    _ts_iso_to_ns,
    _underlying_sl_clip,
    _underlying_tgt_clip,
    _user_sl_clip,
    _user_tgt_clip,
)
from core.backtest_runner.portfolio_exit_config import (
    _MoveSLConfig,
    _OPTIONS_ONLY_MOVE_SL_ACTIONS,
    _OPTIONS_ONLY_PF_SL_TYPES,
    _OPTIONS_ONLY_PF_TGT_TYPES,
    _PfStoplossSettings,
    _PfTargetSettings,
    _UNDERLYING_PF_SL_TYPES,
    _UNDERLYING_PF_TGT_TYPES,
    _VALID_MOVE_SL_ACTIONS_FX,
    _VALID_PF_SL_TYPES_FX,
    _VALID_PF_TGT_TYPES_FX,
    _resolve_move_sl_to_cost,
    _resolve_pf_stoploss,
    _resolve_pf_target,
)
from core.backtest_runner.portfolio_results import (
    _extract_portfolio_results,
    _extract_trade_pnls,
    _merge_portfolio_results,
    _per_strategy_breakdown,
    _positions_pnl_series,
    _splice_merged_results,
)
from core.backtest_runner.profiling import (
    _config_supports_aggregate_to,
    _config_supports_extra_bar_types,
    _phase,
)
from core.backtest_runner.rbo import (
    _RBOSettings,
    _apply_winter_time,
    _hms_to_sec,
    _resolve_rbo,
    add_one_hour,
)
from core.backtest_runner.report_utils import (
    _pick_col,
    _to_utc_ts,
)
from core.backtest_runner.results import (
    _base_values_from_report,
    _extract_results,
    _position_realized_in_base,
    _position_unrealized_in_base,
    _positions_report_realized_in_base,
    _row_pnl_to_base,
    positions_report_with_base,
)
from core.backtest_runner.single_backtest import (
    run_backtest,
    run_backtest_node,
)
from core.backtest_runner.slot_execution import (
    _extract_slot_from_group_reports,
    _run_single_backtest_task,
    _run_single_slot,
    _run_single_slot_node,
    _run_slot_group,
    _run_slot_group_node,
    _worker_init_ignore_sigint,
)

__all__ = [
    # Public API
    'run_backtest',
    'run_backtest_node',
    'run_portfolio_backtest',
    'positions_report_with_base',
    'publish_cross_portfolio_event',
    'consume_cross_portfolio_events',
    'clear_cross_portfolio_bus',
    'add_one_hour',
    # Re-exported internals (back-compat for tests / introspection)
    '_AggCoordination',
    '_CROSS_PORTFOLIO_ACTIONS',
    '_CROSS_PORTFOLIO_EVENT_BUS',
    '_ClipResult',
    '_DAY_NAME_TO_WEEKDAY',
    '_ENTRY_PRICE_REEXEC_ACTIONS',
    '_EPOCH_WEEKDAY',
    '_LEGACY_PF_ACTION_ALIASES',
    '_MoveSLConfig',
    '_NANOS_PER_DAY',
    '_NANOS_PER_MINUTE',
    '_OPTIONS_ONLY_MOVE_SL_ACTIONS',
    '_OPTIONS_ONLY_PF_ACTIONS',
    '_OPTIONS_ONLY_PF_SL_TYPES',
    '_OPTIONS_ONLY_PF_TGT_TYPES',
    '_OtherSettings',
    '_PfStoplossSettings',
    '_PfTargetSettings',
    '_RBOSettings',
    '_REEXECUTE_FAMILY_ACTIONS',
    '_UNDERLYING_PF_SL_TYPES',
    '_UNDERLYING_PF_TGT_TYPES',
    '_VALID_MOVE_SL_ACTIONS_FX',
    '_VALID_ON_SL_ACTION_ON',
    '_VALID_ON_TARGET_ACTION_ON',
    '_VALID_PF_ACTIONS_FX',
    '_VALID_PF_SL_TYPES_FX',
    '_VALID_PF_TGT_TYPES_FX',
    '_VWAP_SL_PREFIXES',
    '_VWAP_TP_PREFIXES',
    '_aggregate_target_for_slot',
    '_allowed_weekdays',
    '_apply_directional_close_fill',
    '_apply_portfolio_clip',
    '_apply_vwap_fill',
    '_apply_winter_time',
    '_base_values_from_report',
    '_build_clip_result',
    '_build_close_lookup',
    '_build_equity_curve_from_account',
    '_build_run_config',
    '_build_underlying_curve',
    '_build_vwap_lookup',
    '_cached_catalog_bars',
    '_chunk_data_configs_for_path_b',
    '_compute_agg_coordination',
    '_config_supports_aggregate_to',
    '_config_supports_extra_bar_types',
    '_earliest_clip',
    '_ensure_final_equity_point',
    '_entry_at_clip',
    '_extract_portfolio_results',
    '_extract_results',
    '_extract_slot_from_group_reports',
    '_extract_trade_pnls',
    '_filter_bars_after_ns',
    '_filter_bars_by_time_of_day',
    '_filter_bars_by_weekday',
    '_group_slots',
    '_hhmm_to_minute',
    '_hms_to_sec',
    '_is_entry_price_reexec',
    '_is_intraday_bar_type',
    '_is_reexec_action',
    '_merge_equity_curves',
    '_merge_portfolio_results',
    '_normalize_pf_action',
    '_pair_bid_ask_bar_type',
    '_path_b_active',
    '_path_b_supports_filters',
    '_per_strategy_breakdown',
    '_phase',
    '_pick_col',
    '_position_realized_in_base',
    '_position_unrealized_in_base',
    '_positions_pnl_series',
    '_positions_report_realized_in_base',
    '_resolve_move_sl_to_cost',
    '_resolve_other_settings',
    '_resolve_pf_stoploss',
    '_resolve_pf_target',
    '_resolve_rbo',
    '_row_pnl_to_base',
    '_run_single_backtest_task',
    '_run_single_slot',
    '_run_single_slot_node',
    '_run_slot_group',
    '_run_slot_group_node',
    '_sec_to_hms',
    '_session_start_minute',
    '_slot_pnl_at_ts',
    '_splice_merged_results',
    '_to_utc_ts',
    '_ts_iso_to_ns',
    '_underlying_sl_clip',
    '_underlying_tgt_clip',
    '_user_sl_clip',
    '_user_tgt_clip',
    '_vwap_adjust_pnl_cell',
    '_vwap_normalize_tag',
    '_vwap_row_is_long',
    '_vwap_session_bucket',
    '_vwap_ts_to_ns',
    '_worker_init_ignore_sigint',
    # Re-exported imports (preserve former module namespace)
    'AccountType',
    'BacktestDataConfig',
    'BacktestEngine',
    'BacktestEngineConfig',
    'BacktestNode',
    'BacktestRunConfig',
    'BacktestVenueConfig',
    'BarType',
    'Decimal',
    'FxRateResolver',
    'LoggingConfig',
    'ManagedExitStrategy',
    'Money',
    'OmsType',
    'ParquetDataCatalog',
    'Path',
    'PortfolioConfig',
    'STRATEGY_REGISTRY',
    'StrategySlotConfig',
    'TraderId',
    'USD',
    'Venue',
    '_time_mod',
    'advance_trailing_target',
    'config_from_exit',
    'contextlib',
    'dataclasses',
    'effective_portfolio_squareoff',
    'effective_slot_qty',
    'functools',
    'load_adapter_config_for_bar_type',
    'normalize_strategy_bar_types',
    'np',
    'parse_money_string',
    'pd',
]
