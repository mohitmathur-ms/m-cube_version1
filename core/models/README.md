# `core/models`

The portfolio configuration schema and every helper that operates on it:
the three nested dataclasses (`ExitConfig` → `StrategySlotConfig` →
`PortfolioConfig`), square-off resolution, leg-action parsing/validation,
timeframe/composite bar-type utilities, JSON (de)serialization + legacy
migration, file persistence, and runtime slot sizing. This package was split
out of the former monolithic `core/models.py`; each subdirectory owns one
logical concern, with that concern's logic living directly in the directory's
`__init__.py`.

## Public API (unchanged after the split)

```python
from core.models import (
    ExitConfig, StrategySlotConfig, PortfolioConfig,        # schema dataclasses
    effective_portfolio_squareoff, resolve_squareoff,       # square-off
    VALID_LEG_ACTIONS, parse_leg_actions, validate_leg_actions,  # leg actions
    build_composite_bar_type, normalize_strategy_bar_types, # composite bar types
    portfolio_to_dict, portfolio_from_dict,                 # serialization
    save_portfolio, load_portfolio, list_portfolios, delete_portfolio,  # persistence
    effective_slot_qty,                                     # slot sizing
)
```

External consumers: `server.py` (the dataclasses + serialization + persistence
+ `validate_leg_actions` + `effective_portfolio_squareoff`),
`core/backtest_runner.py` (`effective_slot_qty`, `normalize_strategy_bar_types`,
`effective_portfolio_squareoff`, the dataclasses), `core/managed_strategy.py`
(`ExitConfig`, `parse_leg_actions`), `core/templates.py` (the dataclasses), and
the `scripts/` + `tests/` suites + `verify_session_changes.py`.

The private helpers/constants (`_UNIT_SECONDS`, `_timeframe_seconds`,
`_bar_type_timeframe`, `_filter_known_fields`, `_migrate_legacy_trade_size`)
are re-exported from the package root for backward compatibility with tests and
introspection.

> The original `core/models.py` is kept in the repo as a shadowed backup —
> Python imports this package directory in preference to the same-named module,
> so the split is what actually runs.

## Components

| Directory | Responsibility | Key symbols |
|---|---|---|
| `exit_config/` | Per-leg exit settings (SL/TP type+value, ATR sizing, trailing, target-lock, leg trailing-target, SL/Target wait, on-SL/Target actions + re-entry knobs, leg square-off). | `ExitConfig`, `has_exit_management` |
| `strategy_slot_config/` | One strategy instance: strategy + bar type + sizing (`lots`/`allocation_pct`) + embedded `ExitConfig` + per-slot date range & square-off. | `StrategySlotConfig`, `display_name` |
| `portfolio_config/` | Portfolio-level settings: capital, max-loss/profit, allocation, filters (run_on_days / entry window), winter-time, RBO, Other-Settings, portfolio SL/Target (+ trailing, Move-SL-to-Cost), ReExecute/Exit/Monitoring tabs, slot list. | `PortfolioConfig`, `add_slot`, `remove_slot`, `enabled_slots` |
| `squareoff/` | Resolve effective `(squareoff_time, squareoff_tz)` with leg > slot > portfolio precedence; MIS product supplies the portfolio default. | `effective_portfolio_squareoff`, `resolve_squareoff` |
| `leg_actions/` | Canonical valid leg-action list + splitter + combination validator (≤3, mutual-exclusion rules). | `VALID_LEG_ACTIONS`, `parse_leg_actions`, `validate_leg_actions` |
| `timeframe_utils/` | Low-level timeframe parsing: unit→seconds table, `"30-MINUTE"`→1800, pull `<step>-<unit>` from a bar type. | `_UNIT_SECONDS`, `_timeframe_seconds`, `_bar_type_timeframe` |
| `composite_bar_type/` | Build a NautilusTrader composite (`...-INTERNAL@<base>-EXTERNAL`) bar type; validate/repair a slot's strategy-subscribe timeframes vs its feed (coarser→keep, same→collapse, finer→drop). | `build_composite_bar_type`, `normalize_strategy_bar_types` |
| `serialization/` | Dict ↔ dataclass conversion; schema-drift-tolerant load; legacy `trade_size`→`lots` migration. | `portfolio_to_dict`, `portfolio_from_dict`, `_filter_known_fields`, `_migrate_legacy_trade_size` |
| `persistence/` | Save / load / list / delete portfolios as `<name>.json` on disk. | `save_portfolio`, `load_portfolio`, `list_portfolios`, `delete_portfolio` |
| `slot_sizing/` | Materialize the four sizing tiers (admin `lot_size`, admin `trade_size` cap, slot `lots`, user `multiplier`) into the final order quantity. | `effective_slot_qty` |

## Dependency graph (acyclic)

```
exit_config ──► strategy_slot_config ──► portfolio_config ─┐
                                                           ├─► serialization ──► persistence ─► __init__
timeframe_utils ──► composite_bar_type ────────────────────┘
squareoff ─────────────────────────────────────────────────► __init__
leg_actions ───────────────────────────────────────────────► __init__
slot_sizing ───────────────────────────────────────────────► __init__
```

`squareoff/` and `slot_sizing/` reference the dataclasses only via
forward-reference type hints / lazy runtime imports, so they carry no
import-time dependency on the schema components (keeps the graph acyclic).
`serialization/_migrate_legacy_trade_size` and `slot_sizing/effective_slot_qty`
import `core.venue_config` / `core.users` lazily inside the function body to
avoid a circular import during test collection.

## Hardcoded values

Magic constants and key defaults, by logical component (`{component} → {value}`):

- **`timeframe_utils`** → `_UNIT_SECONDS` table: `SECOND=1`, `MINUTE=60`,
  `HOUR=3600`, `DAY=86400`, `WEEK=604800`, `MONTH=2592000` (nominal 30-day
  month, used only for ordering).
- **`leg_actions`** → `VALID_LEG_ACTIONS = ("close", "re_execute", "reverse",
  "execute", "re_entry", "keep_leg_running")`; max combinable actions = `3`;
  empty/None action string defaults to `["close"]`.
- **`composite_bar_type`** → bar-type suffix literals `"INTERNAL"` / `"EXTERNAL"`
  (and the `@<step>-<unit>-EXTERNAL` composite-source spec); minimum bar-type
  parts = `5` in `build_composite_bar_type`, `3` in `_bar_type_timeframe`.
- **`squareoff`** → product sentinel `"MIS"` (the only product that supplies a
  square-off default).
- **`serialization`** → `lot_size` fallback = `1` when venue config is missing.
- **`slot_sizing`** → `lot_size` fallback = `1`; user `multiplier` fallback =
  `1.0`; cap applied only when `trade_size > 0`.
- **`persistence`** → default directory = `"portfolios"`; file extension =
  `.json`; JSON `indent=2`.
- **`exit_config`** (key defaults) → `exit_price_format="ohlcv"`,
  `stop_loss_type="none"`, `target_type="none"`,
  `on_sl_action="close"`, `on_target_action="close"`, `armed_at_start=True`;
  all numeric thresholds (SL/TP value, trailing, ATR period/multiplier, wait
  gates, re-execution/re-entry counts) default to `0` / `0.0`.
- **`strategy_slot_config`** (key defaults) → `strategy_name="EMA Cross"`,
  `lots=1.0`, `allocation_pct=0.0`, `enabled=True`, `slot_id` = first 8 chars of
  a `uuid4`.
- **`portfolio_config`** (key defaults) → `name="New Portfolio"`,
  `starting_capital=100000.0`, `allocation_mode="equal"`,
  `no_reentry_sl_cost=True` (spec default ON), `exit_order_type="MARKET"`,
  `exit_sell_first=True`, `on_portfolio_complete="None"`,
  `rbo_entry_at="Any"`, `rbo_monitoring="Underlying"`,
  `pf_sl_type="Combined Loss"`, `pf_tgt_type="Combined Profit"`,
  `pf_sl_action="SqOff"`, `pf_tgt_action="SqOff"`,
  `on_sl_action_on="OnSL_N_Trailing_Both"`,
  `on_target_action_on="OnTarget_N_Trailing_Both"`,
  `move_sl_action="Move Only for Profitable Legs"`,
  `move_sl_agg_pnl_direction="loss"`, all 6 `*_monitoring` fields = `"Realtime"`;
  re-execute counts `0 = unlimited`.
