# Leg-Level SL & Target: Implementation Trace

> **Question answered:** *"When I enter leg-level Stop Loss (SL) and Target (TGT/TP)
> values in the portfolio UI, how are they actually implemented in the codebase?"*
>
> This report traces a single leg's SL/TGT value end-to-end — UI → JSON → schema →
> engine → exit — and then lists **every file and line** where SL and TGT are used.
> Line numbers were verified against the current source (`Path_A_LowAPI` branch).
>
> **Note on layout:** `CLAUDE.md` refers to `core/models.py`, but that module is now a
> **package** (`core/models/`) split into submodules — `exit_config/`,
> `strategy_slot_config/`, `portfolio_config/`, `serialization/`, `leg_actions/`.

---

## 1. Executive answer — the end-to-end path

When you type, say, `SL type = percentage, SL value = 1.0` and `Target type = points,
Target value = 50` into a leg, here is what happens:

1. The value is read from the **leg editor form** and stored on the slot's
   `exit_config` JSON object (frontend, `portfolio.js`).
2. Saving the portfolio POSTs the whole JSON (including every `exit_config`) to
   `/api/portfolios/save`, where `_validate_portfolio_sl()` validates the SL/Target
   actions before the file is written.
3. At backtest time the JSON is deserialized by `portfolio_from_dict()`, which builds
   an **`ExitConfig`** dataclass (the leaf of `leg → slot → portfolio`).
4. `config_from_exit()` maps that `ExitConfig` into a runtime **`ManagedExitConfig`**
   and canonicalizes the type names (`Premium → percentage`, `AbsolutePremium → points`).
5. When an entry order fills, **`ManagedExitStrategy.on_order_filled()`** converts your
   SL/TGT *value* into an **absolute trigger price** (`current_sl`, `current_tp`),
   applying the BUY-below / SELL-above sign rules and snapping to the instrument tick.
6. On every bar, **`_check_exits()`** checks whether the bar's high/low pierced
   `current_sl` / `current_tp` (plus trailing, target-lock, wait-gates), and when it
   does it calls **`_handle_exit()`**, which dispatches your `on_sl_action` /
   `on_target_action` (close / re-execute / reverse / …).
7. Separately, after the run, **`_apply_portfolio_clip()`** applies *portfolio-level*
   SL/TGT as an overlay on the aggregated equity curve (this is a different tier from
   the leg-level SL/TGT above).

```
UI leg editor (static/js/portfolio.js)
   │  slot.exit_config { stop_loss_type, stop_loss_value, target_type, target_value, … }
   ▼
POST /api/portfolios/save        ── server.py  (_validate_portfolio_sl)
   ▼
portfolio_from_dict()            ── core/models/serialization/__init__.py
   │  builds ExitConfig          ── core/models/exit_config/__init__.py
   ▼
config_from_exit()               ── core/managed_strategy.py:1779
   │  ExitConfig → ManagedExitConfig  (type canon: Premium→percentage …)
   ▼
ManagedExitStrategy.on_order_filled()   ── managed_strategy.py:1628
   │  value → absolute price: current_sl / current_tp
   ▼
ManagedExitStrategy._check_exits()      ── managed_strategy.py:1109   (every bar)
   │  bar high/low pierces price? → _handle_exit()
   ▼
_handle_exit()                          ── managed_strategy.py:1372
      dispatches on_sl_action / on_target_action

  ── (separate tier) portfolio-level overlay ──
_apply_portfolio_clip()                 ── core/backtest_runner.py:1376
```

---

## 2. Layer-by-layer line trace

### 2.1 Frontend — `static/js/portfolio.js`

The leg's SL/TGT lives on `slot.exit_config` (`ec` in the code). It is rendered in two
places (a compact inline leg-table row and a full leg modal), then read back on save.

| Line(s) | Element / symbol | Role |
|---|---|---|
| ~1784 | SL types `["none","percentage","points","trailing"]` | Inline-row SL dropdown options |
| ~1785 | Target types `["none","percentage","points"]` | Inline-row TGT dropdown options |
| ~1833–1836 | `leg-il-sltype/slval/tptype/tpval-${i}` | Inline-row SL type/value + TGT type/value inputs (read from `ec`) |
| ~2438–2452 | `leg-m-sltype`, `leg-m-slval`, `leg-m-atrperiod`, `leg-m-atrmult`, `leg-m-trailstep`, `leg-m-trailoff`, `leg-m-slwait` | Leg modal **Stoploss** tab fields |
| ~2458–2475 | `leg-m-slaction-group`, `leg-m-exectarget`, `leg-m-reentryprice`, `leg-m-maxreentries`, `leg-m-armed` | On-SL action + cross-leg / re-entry knobs |
| ~2483–2493 | `leg-m-tptype`, `leg-m-tpval`, `leg-m-tgtatrperiod`, `leg-m-tgtatrmult`, `leg-m-tpaction`, `leg-m-tpwait` | Leg modal **Target** tab fields |
| ~2497–2507 | `leg-m-tgttrail-enabled/reach/lock/every/by` | Leg-level trailing-target inputs |
| ~2620–2647 | save handler writing `ec.stop_loss_type … ec.on_target_action` | Reads all form fields back into `slot.exit_config` |
| ~2226–2235 | portfolio save | POSTs full portfolio (all `exit_config`s) to `/api/portfolios/save` |

> The leg SL/TGT fields carry **no `pf-ui-only` class** — they are backend-wired
> (see §4). `exit_price_format` (OHLCV / LTP / Bid-Ask / Mark) is also selectable here.

### 2.2 API — `server.py`

| Line(s) | Symbol | Role |
|---|---|---|
| ~1475–1480 | `/api/portfolios/list` → `api_list_portfolios()` | List saved portfolios |
| ~1483–1494 | `/api/portfolios/load` → `api_load_portfolio()` | Load a portfolio (SL/TGT persisted inside each slot's `exit_config`) |
| ~1497–1522 | `/api/portfolios/save` → `api_save_portfolio()` | Accept + persist portfolio JSON including all `exit_config`s |
| ~1346–1471 | `_validate_portfolio_sl()` | Pre-save validation of SL/Target config |
| ~1364–1366 | `validate_leg_actions(action, has_execute_target=…)` | Validates `on_sl_action` / `on_target_action` strings |
| ~1562–1640 | `/api/portfolios/backtest` → `api_portfolio_backtest()` | Runs the backtest; leg exit configs flow through to the engine |

### 2.3 Schema (the leg-level SL/TGT dataclass) — `core/models/exit_config/__init__.py`

This `ExitConfig` dataclass **is** the leg-level SL/TGT model. Verified field lines:

| Line | Field | Default | Meaning |
|---|---|---|---|
| 29 | `exit_price_format` | `"ohlcv"` | Trigger series: `ohlcv` (B) / `ltp` (C) / `bidask` (A) |
| **32** | `stop_loss_type` | `"none"` | `none` \| `percentage` \| `points` \| `trailing` \| `atr` |
| **33** | `stop_loss_value` | `0.0` | The SL value you type |
| 34 | `trailing_sl_step` | `0.0` | Trailing-SL ratchet step (profit units) |
| 35 | `trailing_sl_offset` | `0.0` | Amount SL tightens per ratchet step |
| 43 | `sl_atr_period` | `0` | ATR lookback for ATR-based SL |
| 44 | `sl_atr_multiplier` | `0.0` | `SL = entry ± k·ATR` |
| **47** | `target_type` | `"none"` | `none` \| `percentage` \| `points` \| `atr` |
| **48** | `target_value` | `0.0` | The TGT value you type |
| 58 | `tgt_atr_period` | `0` | ATR lookback for ATR-based TGT |
| 59 | `tgt_atr_multiplier` | `0.0` | `TP = entry ± k·ATR` |
| 62 | `target_lock_trigger` | `None` | Profit threshold that arms a one-shot SL upgrade |
| 63 | `target_lock_minimum` | `None` | New locked SL floor (profit %) |
| 72–76 | `tgt_trail_enabled`, `tgt_trail_when_profit_reach`, `tgt_trail_lock_min_profit`, `tgt_trail_every`, `tgt_trail_by` | — | Leg-level trailing-target / profit-lock |
| 81 | `sl_wait_sec` | `0` | SL confirmation, wall-clock seconds (preferred) |
| 82 | `sl_wait_bars` | `0` | SL confirmation, legacy bar count |
| 89 | `tgt_wait_sec` | `0` | Target confirmation, seconds (preferred) |
| 90 | `tgt_wait_bars` | `0` | Target confirmation, legacy bar count |
| 103 | `on_sl_action` | `"close"` | Action on SL hit |
| 104 | `on_target_action` | `"close"` | Action on TGT hit |
| 105 | `max_re_executions` | `0` | Re-execute cap (0 = unlimited) |
| 110 | `execute_target_leg_id` | `""` | Sibling slot armed by `execute` action |
| 113 | `reentry_price` | `0.0` | Price level for `re_entry` (0 = original entry) |
| 115 | `max_re_entries` | `0` | Per-day re-entry cap |
| 118 | `armed_at_start` | `True` | If False, leg dormant until a sibling `execute` arms it |
| 123–124 | `squareoff_time`, `squareoff_tz` | `None` | Leg-level force-close time |
| 126–132 | `has_exit_management()` | — | True if any SL/TGT/trailing/squareoff configured |

### 2.4 Serialization & action parsing

**`core/models/serialization/__init__.py`**

| Line(s) | Symbol | Role |
|---|---|---|
| 68–82 | `portfolio_from_dict()` | Pops the `exit_config` dict per slot and constructs `ExitConfig(**…)` |
| 23–33 | `_filter_known_fields()` | Keeps only known dataclass keys (schema-drift tolerant) |
| 36–65 | `_migrate_legacy_trade_size()` | Legacy `trade_size` → `lots` migration (does not touch SL/TGT directly) |

**`core/models/leg_actions/__init__.py`**

| Line(s) | Symbol | Role |
|---|---|---|
| 12–14 | `VALID_LEG_ACTIONS` | `close`, `re_execute`, `reverse`, `execute`, `re_entry`, `keep_leg_running` |
| 17–30 | `parse_leg_actions()` | Parses comma-separated / legacy single action |
| 33–57 | `validate_leg_actions()` | Enforces legal action combinations (spec §4.8) |

### 2.5 Engine — `core/managed_strategy.py` (where SL/TGT actually fire)

This is the heart of the leg-level exit logic. Verified lines:

| Line(s) | Symbol | Role in SL/TGT |
|---|---|---|
| 63–102 | `advance_trailing_target()` | Pure fn: leg trailing-target ratchet (`active, stop, anchor, hit`) |
| 105–132 | `_SL_TYPE_CANON` / `_TGT_TYPE_CANON` | Maps spec names → canonical (`Premium→percentage`, `AbsolutePremium→points`) |
| 1779–1965 | `config_from_exit()` | `ExitConfig → ManagedExitConfig`; SL/TGT field block at **1867–1894** |
| **1628–1712** | `on_order_filled()` | Converts SL/TGT *value* → absolute trigger price at entry |
| 1666–1686 | (within `on_order_filled`) | Initial **SL** price: `percentage`/`trailing` (1666), `points` (1668), `atr` (1673), else 0 (1685) |
| 1689–1712 | (within `on_order_filled`) | Initial **TP** price: `percentage` (1689), `points` (1694), `atr` (1699), else 0 (1711) |
| **1109–1371** | `_check_exits()` | Per-bar monitor (full breakdown below) |
| 1248–1256 | (within `_check_exits`) | Target-lock: one-shot SL upgrade once `highest_profit ≥ target_lock_trigger` |
| 1258–1291 | (within `_check_exits`) | Trailing SL (Trail-After-Move 1259; standard ratchet 1281) |
| 1293–1321 | (within `_check_exits`) | **SL hit check** + `sl_wait_sec` (1305) / `sl_wait_bars` (1312) confirm |
| 1323–1336 | (within `_check_exits`) | Trailing-target via `advance_trailing_target()` |
| 1338–1370 | (within `_check_exits`) | **TP hit check** + `tgt_wait_sec` (1354) / `tgt_wait_bars` (1360) confirm |
| **1372–1525** | `_handle_exit()` | Dispatch `on_sl_action` / `on_target_action`; `on_sl/target_action_on` filters |
| 1714–1718 | `_compute_sl_price()` | `entry × (1 ∓ pct/100)` — BUY below, SELL above; negative pct ⇒ profit side |
| 1720–1736 | `_snap_to_tick()` | Snaps trigger price to instrument `price_increment` |
| 1738–1750 | `_reset_exit_state()` | Zeros SL/TGT state on position close |

**SL hit detection (verified `_check_exits:1296–1300`):**
```python
if self.current_sl > 0:
    if is_long and bar_low <= self.current_sl:      # LONG SL fires on bar LOW
        sl_hit = True
    elif is_short and bar_high >= self.current_sl:  # SHORT SL fires on bar HIGH
        sl_hit = True
```

**TP hit detection (verified `_check_exits:1342–1346`):**
```python
if not tp_condition and self.current_tp > 0:
    if is_long and bar_high >= self.current_tp:     # LONG TP fires on bar HIGH
        tp_condition = True
    elif is_short and bar_low <= self.current_tp:   # SHORT TP fires on bar LOW
        tp_condition = True
```

### 2.6 Portfolio-level overlay — `core/backtest_runner.py`

This is a **different tier** from leg-level SL/TGT (portfolio / user level). Included so
you can see where the leg exits sit in the `leg → portfolio → user` hierarchy.

| Line(s) | Symbol | Role |
|---|---|---|
| 1009–1103 | `_resolve_pf_stoploss()` | Validate portfolio SL config → `_PfStoplossSettings` |
| 1106–1176 | `_resolve_pf_target()` | Validate portfolio Target config → `_PfTargetSettings` |
| 1179–1250 | `_resolve_move_sl_to_cost()` | Validate Move-SL-to-Cost + ReExecute gating |
| 1376–1567 | `_apply_portfolio_clip()` | Walk aggregated equity curve; eval order: trailing SL → fixed SL → trailing target → fixed target |
| 1699–1786 | `_underlying_sl_clip()` | Underlying-price-based portfolio SL |
| 1789–1838 | `_user_sl_clip()` | User/tag-level cumulative-PnL SL |
| 1841–1898 | `_underlying_tgt_clip()` | Underlying-price-based portfolio Target |

---

## 3. SL/TGT type & direction reference

### Supported SL types (`stop_loss_type`)

| Type | Resolution at entry (`on_order_filled`) |
|---|---|
| `none` | No SL (`current_sl = 0`) |
| `percentage` | `_compute_sl_price`: BUY `entry×(1−v/100)`, SELL `entry×(1+v/100)` |
| `points` | BUY `entry − v`, SELL `entry + v` |
| `trailing` | Same as `percentage` at entry; then ratchets in `_check_exits:1281` |
| `atr` | `dist = k·ATR`; BUY `entry − dist`, SELL `entry + dist` |

### Supported TGT types (`target_type`)

| Type | Resolution at entry (`on_order_filled`) |
|---|---|
| `none` | No TP (`current_tp = 0`) |
| `percentage` | BUY `entry×(1+v/100)`, SELL `entry×(1−v/100)` |
| `points` | BUY `entry + v`, SELL `entry − v` |
| `atr` | `dist = k·ATR`; BUY `entry + dist`, SELL `entry − dist` |

### BUY vs SELL — sign & hit-detection summary

| | LONG (BUY) | SHORT (SELL) |
|---|---|---|
| SL placed | **below** entry | **above** entry |
| TP placed | **above** entry | **below** entry |
| SL fires on | bar **low** ≤ SL | bar **high** ≥ SL |
| TP fires on | bar **high** ≥ TP | bar **low** ≤ TP |

> **Tick snapping (TBD-2 resolved):** all trigger prices pass through `_snap_to_tick()`
> using the instrument's `price_increment`. Exits fire **MARKET** orders, so the broker
> handles final tick alignment at fill.

---

## 4. Wiring status

Per `LOGICS_BACKEND_STATUS.md` (section on `sl_tgt.html` — slot SL/Target reference),
leg-level SL/TGT is **fully wired end-to-end** — none of these fields carry the
`pf-ui-only` (red) marker:

- ✅ `stop_loss_type` / `stop_loss_value`, `target_type` / `target_value`
- ✅ trailing SL (`trailing_sl_step` / `trailing_sl_offset`)
- ✅ ATR-based SL **and** ATR-based Target
- ✅ target-lock (`target_lock_trigger` / `target_lock_minimum`)
- ✅ leg-level trailing target (`tgt_trail_*`)
- ✅ SL/Target wait gates (`sl_wait_sec`/`bars`, `tgt_wait_sec`/`bars`)
- ✅ actions + combinations (`on_sl_action`, `on_target_action`, validated at save)
- ✅ `exit_price_format` (OHLCV / LTP / Bid-Ask / Mark) and intrabar high/low triggering

**Single exception:** leg-level **Underlying-based** SL/Target is *not* implemented at
the leg level — underlying-movement exits are handled at the **portfolio** level
(`_underlying_sl_clip` / `_underlying_tgt_clip` in `backtest_runner.py`).

---

## 5. Summary

When you enter a leg-level **SL** or **Target** value:

1. It is stored on the slot's `exit_config` JSON in the UI (`static/js/portfolio.js`)
   and saved via `/api/portfolios/save` (`server.py`, validated by
   `_validate_portfolio_sl`).
2. At run time it becomes an **`ExitConfig`** dataclass
   (`core/models/exit_config/__init__.py`) via `portfolio_from_dict`
   (`core/models/serialization/`), then a runtime `ManagedExitConfig` via
   `config_from_exit` (`core/managed_strategy.py:1779`).
3. On entry fill, **`on_order_filled()`** (`managed_strategy.py:1628`) turns your
   *value* into an absolute trigger **price** (`current_sl` / `current_tp`), applying
   BUY-below / SELL-above signs and tick-snapping.
4. Every bar, **`_check_exits()`** (`managed_strategy.py:1109`) tests whether the bar's
   high/low pierced the trigger (plus trailing, target-lock, wait-gates), and on a hit
   calls **`_handle_exit()`** (`managed_strategy.py:1372`), which runs your
   `on_sl_action` / `on_target_action`.
5. The **portfolio-level** SL/TGT is a separate tier applied after the run by
   `_apply_portfolio_clip()` (`core/backtest_runner.py:1376`) — it does not change how
   the leg-level value you entered is enforced.

In short: **leg SL/TGT value lives in `ExitConfig` → becomes a price in
`on_order_filled` → is monitored in `_check_exits` → acted on in `_handle_exit`.** All
core leg-level SL/TGT functionality is wired; only leg-level *underlying-based* exits
are deferred to the portfolio tier.

---

*Generated by reading the source on branch `Path_A_LowAPI`. Line numbers verified
against `core/managed_strategy.py`, `core/models/exit_config/__init__.py`,
`core/models/serialization/__init__.py`, `core/backtest_runner.py`, `server.py`, and
`static/js/portfolio.js`.*
