# `core/backtest_runner`

The L3 portfolio runner / engine orchestrator: configures and executes
NautilusTrader backtests, runs both the single-strategy and the multi-slot
portfolio paths (per-slot, grouped-engine, and `BacktestNode` Path B variants),
applies bar filters / entry windows / RBO, resolves and enforces portfolio
stop-loss / target / Move-SL clips, converts cross-currency PnL to the account
base, and aggregates everything into result dicts. This package was split out of
the former monolithic `core/backtest_runner.py`; each subdirectory owns one
logical concern, with that concern's logic living directly in the directory's
`__init__.py`.

## Public API (unchanged after the split)

```python
from core.backtest_runner import (
    run_backtest, run_backtest_node,        # single-strategy entry points (Path A / Path B)
    run_portfolio_backtest,                 # multi-slot portfolio orchestrator
    positions_report_with_base,             # positions report with FX→base-currency PnL
    publish_cross_portfolio_event,          # cross-portfolio event bus
    consume_cross_portfolio_events,
    clear_cross_portfolio_bus,
    add_one_hour,                           # Winter Time Adjustment time-shift helper
)
```

External consumers: `server.py` (`run_backtest`, `run_portfolio_backtest`,
`_run_single_backtest_task`); `scripts/` (`run_portfolio_backtest`,
`_run_single_slot`, `_cached_catalog_bars`, `_pair_bid_ask_bar_type`);
`verify_session_changes.py` (`_allowed_weekdays`, `_filter_bars_by_weekday`,
`_filter_bars_by_time_of_day`, `_hhmm_to_minute`, `_is_intraday_bar_type`,
`_run_single_slot`, `_run_slot_group`, and `BacktestRunConfig`); `parity_check.py`
and the `tests/` suite (`run_portfolio_backtest`, `clear_cross_portfolio_bus`,
plus many internal helpers — clip/VWAP/equity-curve/splice functions and the
config dataclasses); `generate_subscription_excels.py` (`_pair_bid_ask_bar_type`).

Because so many callers reach into internals, **every** former module-level
symbol is re-exported from the package root and listed in `__all__` (split into a
curated *Public API* group and a *Re-exported internals* group), so both
`from core.backtest_runner import X` and `core.backtest_runner.X` keep resolving.
The root also re-exports the monolith's top-level third-party/Nautilus imports
(`BacktestEngine`, `BacktestNode`, `BacktestVenueConfig`, `BacktestDataConfig`,
`BacktestRunConfig`, …) to preserve the former module namespace.

## Components

| Directory | Responsibility | Key symbols |
|---|---|---|
| `profiling/` | Phase wall-time profiling context manager; memoized strategy-config introspection (`extra_bar_types` / `aggregate_to_bar_type`). | `_phase`, `_config_supports_extra_bar_types`, `_config_supports_aggregate_to` |
| `bar_types/` | Bar-type string reasoning: BID/ASK/MID fill pairing, leg aggregation target, slot grouping by engine key. | `_pair_bid_ask_bar_type`, `_aggregate_target_for_slot`, `_group_slots` |
| `bar_filters/` | Bar-list filters used by every path: `run_on_days` weekday gate, intraday entry-window time-of-day gate, ReExecute replay cutoff, and the HH:MM / granularity parsing helpers they share. | `_allowed_weekdays`, `_filter_bars_by_weekday`, `_filter_bars_by_time_of_day`, `_filter_bars_after_ns`, `_hhmm_to_minute`, `_is_intraday_bar_type` |
| `path_b/` | Path B (`BacktestNode`) wiring: env-flag check, per-day data-config chunking (run_on_days / entry-window / RBO), and the shared `BacktestRunConfig` builder. | `_path_b_active`, `_path_b_supports_filters`, `_chunk_data_configs_for_path_b`, `_build_run_config`, `_sec_to_hms` |
| `rbo/` | Range-Breakout config + Winter Time Adjustment: validated `_RBOSettings`, HH:MM[:SS] second parsing, `add_one_hour`, portfolio resolver/mutator. | `_RBOSettings`, `_resolve_rbo`, `_apply_winter_time`, `add_one_hour`, `_hms_to_sec` |
| `other_settings/` | The Other Settings tab: validated `_OtherSettings` (delay-between-legs, on-SL / on-Target action gating) and its resolver. | `_OtherSettings`, `_resolve_other_settings`, `_VALID_ON_SL_ACTION_ON`, `_VALID_ON_TARGET_ACTION_ON` |
| `portfolio_exit_config/` | Portfolio-level Stoploss / Target / Move-SL-to-Cost config: frozen settings dataclasses, FX/options type+action allow-lists, and the resolvers that validate and build them. | `_PfStoplossSettings`, `_PfTargetSettings`, `_MoveSLConfig`, `_resolve_pf_stoploss`, `_resolve_pf_target`, `_resolve_move_sl_to_cost` |
| `cross_portfolio/` | The cross-portfolio event bus (publish / consume / clear) and portfolio action normalisation (legacy aliases, ReExecute-family classifiers). | `publish_cross_portfolio_event`, `consume_cross_portfolio_events`, `clear_cross_portfolio_bus`, `_normalize_pf_action`, `_CROSS_PORTFOLIO_EVENT_BUS` |
| `portfolio_clip/` | Portfolio SL/Target clip detection + application: equity-curve walk, trailing/delay logic, selective square-off, underlying-price clips, and the aggregate-Move-SL pass-1→pass-2 coordination payload. | `_apply_portfolio_clip`, `_compute_agg_coordination`, `_build_clip_result`, `_build_underlying_curve`, `_underlying_sl_clip`, `_user_tgt_clip`, `_entry_at_clip`, `_ClipResult`, `_AggCoordination`, `_ts_iso_to_ns` |
| `equity_curves/` | Equity-curve primitives shared across the result layer: merge per-slot curves, build a curve from account snapshots, ensure a final point. | `_merge_equity_curves`, `_build_equity_curve_from_account`, `_ensure_final_equity_point` |
| `data_cache/` | Worker-local LRU cache around `ParquetDataCatalog` bar loads. | `_cached_catalog_bars` |
| `report_utils/` | Low-level report helpers shared by the results and exit-fill layers. | `_pick_col`, `_to_utc_ts` |
| `results/` | Single-engine result extraction with FX→base-currency conversion of realized/unrealized PnL. | `_extract_results`, `positions_report_with_base`, `_position_realized_in_base`, `_base_values_from_report` |
| `exit_fill/` | Conservative exit-fill models: session-cumulative VWAP re-pricing and the directional bid/ask close-fill model for SL/TP leg exits. | `_apply_vwap_fill`, `_build_vwap_lookup`, `_apply_directional_close_fill`, `_build_close_lookup`, `_vwap_session_bucket`, `_session_start_minute` |
| `single_backtest/` | Single-strategy entry points: `run_backtest` (Path A / B routing) and `run_backtest_node` (Path B). | `run_backtest`, `run_backtest_node` |
| `slot_execution/` | Per-slot and grouped-engine execution (Path A & B), plus the picklable `ProcessPoolExecutor` worker and its SIGINT-ignoring initializer. | `_run_single_slot`, `_run_single_slot_node`, `_run_slot_group`, `_run_slot_group_node`, `_extract_slot_from_group_reports`, `_run_single_backtest_task`, `_worker_init_ignore_sigint` |
| `portfolio_results/` | Portfolio-level aggregation: per-strategy breakdown, trade-PnL extraction, two-pass result splicing, the `_merge_portfolio_results` orchestrator, and the portfolio result-dict builder. | `_merge_portfolio_results`, `_extract_portfolio_results`, `_splice_merged_results`, `_per_strategy_breakdown`, `_positions_pnl_series`, `_extract_trade_pnls` |
| `orchestration/` | Top-level portfolio entry point. | `run_portfolio_backtest` |

## Dependency graph (acyclic)

```
profiling   bar_types   bar_filters   data_cache   report_utils   equity_curves   cross_portfolio   other_settings   rbo
                              │                          │              │                │                          │
                              └──────► path_b ◄──────────┼──────────────┼────────────────┘                          │
                                                         │              │                                           │
                                         exit_fill ──────┘              │            portfolio_exit_config ◄─────────┘
                                            │                           │                     │
                              results ◄─────┴───── equity_curves        │                     │
                                 │                                      │                     │
              single_backtest ◄──┤            portfolio_clip ◄──────────┴── cross_portfolio / exit_fill / report_utils
                 │               │                 │
                 └──► slot_execution ◄─────────────┘  (also: bar_filters, bar_types, data_cache, path_b, profiling, rbo, other_settings)
                              │
                     portfolio_results ◄── equity_curves, portfolio_clip, portfolio_exit_config
                              │
                       orchestration ──► __init__ (public API)
                       (+ bar_types, cross_portfolio, other_settings, rbo, portfolio_exit_config, portfolio_clip)
```

The cross-portfolio event bus (`_CROSS_PORTFOLIO_EVENT_BUS`) is a shared mutable
dict that lives only in `cross_portfolio/`; publishers/consumers import it (never
rebind it), so `clear_cross_portfolio_bus()` resets the single source. Two leaves
— `equity_curves/` and `report_utils/` — were factored out specifically to keep
the graph acyclic (they are imported by both the producer and consumer of the
otherwise-circular `results ↔ exit_fill` / `portfolio_clip ↔ portfolio_results`
relationships). Forward-reference type hints (e.g. `_RBOSettings`, `_MoveSLConfig`)
resolve to real imports here rather than quoted strings.

## Hardcoded values

Values baked into the package's `__init__.py` files with **no UI field** that
lets a user supply the real value. Override-path legend:

- **None** — only changeable by editing source.
- **physical** — fixed by time/calendar math; never configurable.
- **kwarg** — a function-argument default; overridable in code but nothing in the
  app passes it.
- **UI-backed** — listed only to clarify it is *not* truly hardcoded (the real
  value comes from the portfolio/slot editor; the literal is just a fallback).

### Engine / venue wiring (`single_backtest/`, `slot_execution/`, `path_b/`)
| Value | Literal | Meaning | Override |
|---|---|---|---|
| OMS type | `OmsType.NETTING` (single, per-slot, Path B) / `OmsType.HEDGING` (grouped-engine `_run_slot_group`) | Order-management semantics per engine. | None |
| Account type | `AccountType.MARGIN` | Backtest account type. | None |
| Leverage | `Decimal(1)` (Path A) / `1.0` (Path B) | 1× leverage on every venue. | None |
| Account base currency | `USD` (`Money(..., USD)`, `base_currency="USD"`) | Account is always USD; all PnL is converted to USD via `core/fx_rates`. | None |
| `trader_id` | `"BACKTESTER-001"` (single/node), `f"SLOT-{i:03d}"`, `f"GROUP-{i:03d}"` | Internal trader identifiers. | None |
| Engine logging/risk | `LoggingConfig(bypass_logging=True)`, `RiskEngineConfig(bypass=True)`, `run_analysis=False` | Backtest engine is run silent, with risk checks and built-in analysis disabled. | None |
| Path B `chunk_size` | `None` | Load-everything-at-once mode for bit-exact Path A parity. | kwarg |

### Concurrency (`orchestration/`)
| Value | Literal | Meaning | Override |
|---|---|---|---|
| Worker-pool cap | `min(n, os.cpu_count() or 2, 32)` | Max `ProcessPoolExecutor` workers (per-slot and per-group). | None |

### Time / calendar constants (`bar_filters/`, `path_b/`, `exit_fill/`)
| Value | Literal | Meaning | Override |
|---|---|---|---|
| `_NANOS_PER_DAY` | `86_400_000_000_000` | ns per day (fast weekday/time-of-day modulo). | physical |
| `_NANOS_PER_MINUTE` | `60_000_000_000` | ns per minute. | physical |
| `_EPOCH_WEEKDAY` | `3` | 1970-01-01 was a Thursday (weekday=3). | physical |
| Full-day bound | `24 * 60 - 1` (and `86399` clamp in `_sec_to_hms`) | Upper minute/second-of-day. | physical |
| Minutes per day | `1440` (`_vwap_session_bucket`) | Session-bucket rollover. | physical |
| Default entry window | `"00:00:00"` / `"23:59:59.999999"`; end-of-day `T23:59:59.999999` | Path B fallback when one window side is unset (the window endpoints themselves are UI-backed). | None (fallback) |
| RBO entry-end fallback | `"16:15:00"` (`_resolve_rbo`) | Used when `rbo_entry_end` is blank (the field itself is UI-backed). | None (fallback) |
| VWAP session-start default | `0` (midnight UTC) | Used when the venue adapter config has no `session_start_time`. | None (fallback) |

### Fixed spec enums / allow-lists (validation only)
| Value | Where | Meaning | Override |
|---|---|---|---|
| `_VALID_PF_SL_TYPES_FX`, `_VALID_PF_TGT_TYPES_FX`, `_UNDERLYING_PF_*`, `_OPTIONS_ONLY_PF_*` | `portfolio_exit_config/` | Accepted portfolio SL/Target type strings. | None |
| `_VALID_MOVE_SL_ACTIONS_FX` | `portfolio_exit_config/` | Accepted Move-SL action strings. | None |
| `_VALID_PF_ACTIONS_FX`, `_CROSS_PORTFOLIO_ACTIONS`, `_REEXECUTE_FAMILY_ACTIONS`, `_ENTRY_PRICE_REEXEC_ACTIONS`, `_LEGACY_PF_ACTION_ALIASES` | `cross_portfolio/` | Accepted/normalised portfolio action strings. | None |
| `_VALID_ON_SL_ACTION_ON`, `_VALID_ON_TARGET_ACTION_ON` | `other_settings/` | Accepted on-SL / on-Target gating modes. | None |
| `_VWAP_SL_PREFIXES`, `_VWAP_TP_PREFIXES`, `_DAY_NAME_TO_WEEKDAY` | `exit_fill/`, `bar_filters/` | Exit-leg tag prefixes; day-name → weekday map. | None |

> The UI **does** offer these enum values as dropdown options — but the accepted
> *set* (and its FX-vs-options gating) is defined here in source, not entered by
> the user.

### Not hardcoded (UI-backed defaults, listed for clarity)
| Value | Literal | Real source |
|---|---|---|
| `starting_capital` default | `100_000.0` | portfolio editor "starting capital" field |
| `trade_size` default | `0.01` | slot `lots` × instrument `lot_size` |
| bar types, dates, entry window, run_on_days, all SL/TP/RBO/Move-SL values | (function defaults) | the portfolio / slot editor |
