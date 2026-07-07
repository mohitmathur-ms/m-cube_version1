"""Portfolio configuration models.

Dataclasses for portfolio, strategy slot, and exit management configuration,
plus the helpers that resolve, (de)serialize, persist and size them.

This package decomposes the former monolithic ``core/models.py`` into
single-responsibility components (see README.md), each with its logic in the
subdirectory's ``__init__.py``. The public API is unchanged — every symbol that
``core/models.py`` exported is re-exported here, so ``from core.models import …``
keeps working identically.

The private helpers/constants (``_UNIT_SECONDS``, ``_timeframe_seconds``,
``_bar_type_timeframe``, ``_filter_known_fields``, ``_migrate_legacy_trade_size``)
are re-exported too, for backward compatibility with tests and introspection.
"""

from __future__ import annotations

from core.models.composite_bar_type import (
    build_composite_bar_type,
    normalize_strategy_bar_types,
)
from core.models.exit_config import ExitConfig
from core.models.leg_actions import (
    VALID_LEG_ACTIONS,
    parse_leg_actions,
    validate_leg_actions,
)
from core.models.persistence import (
    delete_portfolio,
    list_portfolios,
    load_portfolio,
    save_portfolio,
)
from core.models.portfolio_config import PortfolioConfig
from core.models.serialization import (
    _filter_known_fields,
    _migrate_legacy_trade_size,
    portfolio_from_dict,
    portfolio_to_dict,
)
from core.models.slot_sizing import effective_slot_qty
from core.models.squareoff import effective_portfolio_squareoff, resolve_squareoff
from core.models.strategy_slot_config import StrategySlotConfig
from core.models.timeframe_utils import (
    _UNIT_SECONDS,
    _bar_type_timeframe,
    _timeframe_seconds,
)

__all__ = [
    # ── Schema dataclasses ──
    "ExitConfig",
    "StrategySlotConfig",
    "PortfolioConfig",
    # ── Square-off resolution ──
    "effective_portfolio_squareoff",
    "resolve_squareoff",
    # ── Leg actions ──
    "VALID_LEG_ACTIONS",
    "parse_leg_actions",
    "validate_leg_actions",
    # ── Composite bar types ──
    "build_composite_bar_type",
    "normalize_strategy_bar_types",
    # ── Serialization ──
    "portfolio_to_dict",
    "portfolio_from_dict",
    # ── Persistence ──
    "save_portfolio",
    "load_portfolio",
    "list_portfolios",
    "delete_portfolio",
    # ── Slot sizing ──
    "effective_slot_qty",
    # ── Private helpers/constants (re-exported for tests/introspection) ──
    "_UNIT_SECONDS",
    "_timeframe_seconds",
    "_bar_type_timeframe",
    "_filter_known_fields",
    "_migrate_legacy_trade_size",
]
