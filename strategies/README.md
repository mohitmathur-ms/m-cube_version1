# `strategies/`

Pure **entry strategies** (L1 in the backend layer cheat-sheet). Every module
here is auto-discovered at import time and registered into `STRATEGY_REGISTRY`,
which the dashboard reads to populate the strategy dropdown and parameter forms.
Each strategy file is one self-contained logical component: a
NautilusTrader `Strategy` subclass, its frozen `StrategyConfig`, and the five
registry constants.

Unlike `core/instrument_factory/` and `core/csv_loader/` — which were
monolithic files of *pure functions* split into one-concern-per-directory — the
strategies are mostly `Strategy` methods bound to instance state (`on_bar`,
`on_start`, …) and cannot be relocated out of their classes. So the strategy
files stay whole, and only the genuinely **pure, separable, shared** logic has
been extracted into the `_shared/` helper subpackage (mirroring the
subdirectory-per-concern layout of the precedent packages).

## Public API / registration contract

Each strategy module must export exactly these five module-level constants
(see `CLAUDE.md` → *Strategy plug-in contract*):

```python
STRATEGY_NAME: str     # registry key + UI label
STRATEGY_CLASS: type   # subclass of nautilus_trader.trading.strategy.Strategy
CONFIG_CLASS: type     # subclass of StrategyConfig, frozen=True
DESCRIPTION: str
PARAMS: dict           # UI metadata {param: {"label","min","max","default","type"}}
```

`strategies/__init__.py::_build_registry()` scans the package with
`pkgutil.iter_modules`, **skips any module/package whose name starts with `_`**
(so `_shared/` is never treated as a strategy), imports the rest, and registers
every module that defines `STRATEGY_NAME`. The result is exported as
`STRATEGY_REGISTRY`. `core/strategies.py` is a thin back-compat shim that
re-exports `STRATEGY_REGISTRY` plus four strategy/config classes.

> Adding exit management (SL/TP/trailing) for a strategy is a *separate* step —
> register a signal function in `core/signals.py::SIGNAL_REGISTRY`. The classes
> here only emit raw single-shot signals.

## Strategy components

| File | Strategy name | Signal rule | Key params |
|---|---|---|---|
| `ema_cross.py` | EMA Cross | BUY when fast EMA ≥ slow EMA; SELL on cross below. | `fast_ema_period`, `slow_ema_period` |
| `rsi_mean_reversion.py` | RSI Mean Reversion | BUY when RSI ≤ oversold; SELL when RSI ≥ overbought. | `rsi_period`, `overbought`, `oversold` |
| `bollinger_bands.py` | Bollinger Bands | BUY at/below lower band; SELL at/above upper band. | `bb_period`, `bb_std` |
| `four_ma.py` | 4 Moving Averages | BUY on MA1>MA2>MA3>MA4 alignment; SELL on reverse. SMA/EMA toggle. | `use_ema`, `ma1..ma4_period` |
| `range_breakout.py` | Range Breakout | Opening-range breakout with N-leg pyramid (target promotes next leg, stop doesn't); per-day state machine. **Largest/most complex module.** | times, buffers, `target_pct`/`stop_pct`, `num_legs`, per-leg lots & range-ends |

All five share the `core/aggregating_strategy.AggregatingStrategyMixin` base,
which lets them optionally aggregate the base bar stream up to a higher
timeframe (`aggregate_to_bar_type`).

## Shared helper components (`_shared/`)

| Directory | Responsibility | Key symbols |
|---|---|---|
| `_shared/time_windows/` | Intraday clock math: integer-HHMM → minute-of-day, and bar UTC-ns → local minute/date. Reusable by any time-gated strategy. | `hhmm_to_min`, `bar_to_local` |
| `_shared/entry_tags/` | Builders for the orderbook **"ENTRY DETAILED REASON"** order-tag string — one per strategy. Centralises the tag format (glyphs + precision). | `ema_reason`, `rsi_reason`, `bollinger_reason`, `four_ma_reason`, `rbo_reason` |
| `_shared/leg_state/` | `LegState` dataclass tracking one pyramid leg (active / entry price / ever-hit-target / entry count). | `LegState` |

`_shared/__init__.py` re-exports all of the above for convenience.

> `time_windows.hhmm_to_min` (integer `930` → 570) is intentionally distinct
> from `core/backtest_runner._hhmm_to_minute` (which parses `"HH:MM"` *strings*
> from the portfolio entry-window UI). Different input type, different caller.

## Dependency graph (acyclic)

```
core/aggregating_strategy ──► (all strategy files)

_shared/time_windows ─┐
_shared/entry_tags  ──┼─► range_breakout.py
_shared/leg_state   ──┘

_shared/entry_tags ──► ema_cross.py / rsi_mean_reversion.py
                       bollinger_bands.py / four_ma.py

(every strategy file) ──► strategies/__init__ (_build_registry) ──► STRATEGY_REGISTRY
```

`_shared/` imports nothing from the strategy files (one-way dependency), and the
auto-discovery scanner skips `_shared/` entirely.

---

## Hardcoded values

Literals are listed per logical component as `{value}` → meaning. Strategy
entries cover config defaults and the `PARAMS` `min`/`max`/`default` bounds.

### `ema_cross.py`
- `Decimal("1")` → default `trade_size`
- `10` → default `fast_ema_period` (PARAMS min `2`, max `100`)
- `20` → default `slow_ema_period` (PARAMS min `5`, max `200`)
- validation: `fast_ema_period < slow_ema_period` required

### `rsi_mean_reversion.py`
- `Decimal("1")` → default `trade_size`
- `14` → default `rsi_period` (PARAMS min `2`, max `50`)
- `70.0` → default `overbought` (PARAMS min `50.0`, max `95.0`)
- `30.0` → default `oversold` (PARAMS min `5.0`, max `50.0`)

### `bollinger_bands.py`
- `Decimal("1")` → default `trade_size`
- `20` → default `bb_period` (PARAMS min `5`, max `100`)
- `2.0` → default `bb_std` (PARAMS min `0.5`, max `4.0`)

### `four_ma.py`
- `Decimal("1")` → default `trade_size`
- `False` → default `use_ema` (use SMA unless toggled)
- `5` / `10` / `20` / `50` → default `ma1`/`ma2`/`ma3`/`ma4_period`
- PARAMS bounds: ma1–ma3 min `2` max `200`; ma4 min `2` max `500`

### `range_breakout.py`
- `MAX_LEGS = 10` → cap on pyramid legs (also `num_legs` PARAMS max)
- `Decimal("1")` → default `trade_size`
- default times (integer HHMM): `930` range start, `1030` range end, `1130` last-new-entry, `1515` squareoff (all PARAMS min `0`, max `2359`)
- `0` → default `timezone_offset_min` (PARAMS min `-720`, max `720`; IST = `330`)
- `0.1` → default `breakout_buffer_pct` (PARAMS min `0.0`, max `5.0`)
- `0` → default `breakout_buffer_mode` (0=percent, 1=points; PARAMS max `1`)
- `0.0` → default `breakout_buffer_pts` (PARAMS max `1_000_000.0`)
- `0` → default `range_monitoring_type` (0=realtime, 1=minute-close; PARAMS max `1`)
- `10.0` → default `target_pct`; `40.0` → default `stop_pct` (PARAMS min `0.1`, max `100.0`)
- `1` → default `target_stop_basis` (0=entry price, 1=range size; PARAMS max `1`)
- `False` → default `opposite_side_sl`
- `3` → default `num_legs`
- `1` → default `legN_lots` (legs 1–10; PARAMS min `0`, max `1000`)
- `0` → default `legN_range_end_hhmm` (0 = inherit shared; PARAMS max `2359`)
- `3` → default `max_reentries_per_leg` (PARAMS min `0`, max `10`)
- `True` → defaults for `enable_long`, `enable_short`
- `False` → defaults for `pessimistic_intra_bar_exits`, `one_side_entry_only`
- `0` → defaults for `first_entry_cutoff_minutes`, `reexecute_cutoff_minutes` (0=disabled; PARAMS min `0`, max `720`)
- `100.0` → percent divisor in buffer/target/stop math
- `float("inf")` / `float("-inf")` → triggers for a degenerate (zero-width) range, which disables that leg for the day

### `__init__.py` (registry)
- `"_"` → prefix that excludes a module/package from auto-discovery

### `_shared/time_windows/`
- `100`, `60` → HHMM decomposition (`(hhmm // 100) * 60 + (hhmm % 100)`)
- `1e9` → nanoseconds-per-second divisor for `bar.ts_event`

### `_shared/entry_tags/`
- `:.4f` → price/level precision in EMA, Bollinger, 4MA and RBO tags
- `:.2f` → RSI value precision
- glyphs `≥` / `≤` / `σ` and tag prefixes `"EMA Cross"`, `"RSI("`, `"Bollinger"`, `"4MA"`, `"RBO leg"` (must stay byte-stable — consumed by the orderbook column)

### `_shared/leg_state/`
- `False` / `0.0` / `0` → `LegState` field defaults (`active`, `entry_price`/`ever_hit_target`, `entry_count`)
