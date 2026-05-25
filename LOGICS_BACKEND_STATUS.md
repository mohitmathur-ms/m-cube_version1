# Logics Backend Implementation Status

Tracks which UI-configurable logics from `5. Logics/` are wired through to the backend and which are still UI-only.

**Visual markers on UI:** Two distinct CSS classes communicate state:

| Class | Color | Meaning |
|---|---|---|
| `.pf-ui-only` | Red (`#fff0f0` bg, `#c44` text) | Backend NOT wired but should be — work to do |
| `.pf-live-only` | Gray (`#f5f5f5` bg, `#888` text) | Field is real but only matters in live trading or options trading — backtest will never use it |
| (no class) | Normal | Backend wired and working |

CSS rules at [static/css/style.css:1156-1209](static/css/style.css#L1156-L1209). When a logic gets implemented, the `pf-ui-only` class is removed from its section. Fields marked `pf-live-only` stay marked permanently (they will not be implemented at backtest level).

## Legend
- ✅ **Wired** — UI field flows through to backend behavior, verified by differential test
- ⚠️ **Partial** — Some fields wired, some not; see notes
- ❌ **Missing** — UI accepts the field but backend silently ignores it
- 🔴 **UI-Red** — Section is marked `pf-ui-only` (should be wired)
- ⚫ **UI-Gray** — Section is marked `pf-live-only` (live/options only)

---

## Status by logic

### 1. `Exit_Settings_Logics.html` &mdash; Slot exit settings  &mdash;  ⚠️ Partial 🔴

| Field | Status | Location |
|---|---|---|
| `exit_order_type` (MARKET / Limit / SL_Limit) | ⚠️ Partial | MARKET wired (engine default). Limit / SL_Limit now **blocked at portfolio save** — `server.py:_validate_portfolio_sl` returns 400 (spec §8.1). |
| `exit_sell_first` | ❌ Missing | UI flag; backend doesn't sort exit batches. |
| VWAP enhancement on SL/Target fills (§4.2/§3) | ✅ **Wired (real session VWAP)** | `vwap_exit_fill` toggle / `_USE_VWAP_FILL=1` reprices SL/Target exit fills to the conservative VWAP price (`exit = max(vwap, hit_price)`, spec `sl_tgt.html`). The VWAP is the **spec session-cumulative volume-weighted** value `Σ((H+L+C)/3·volume)/Σ(volume)` over the trading session, computed in `_build_vwap_lookup` from each bar's `volume`, reset at the venue's `session_start_time` (UTC, via `_session_start_minute` / `_vwap_session_bucket` reading `adapter_admin/adapters_config`). Falls back to the bar typical only when a session has no volume yet (FX volume is broker quote-size per the spec volume caveat — spec-correct). Direction rule per §4.2: **SELL leg = MAX(vwap, hit)**, **BUY leg = MIN(vwap, hit)**. Both **Format A** (bid/ask) and **Format B** (single OHLCV/LAST/MID series, for crypto/OHLCV-only slots — `_build_vwap_lookup` "single" bucket) are handled; Format C / no-volume no-op. Runner-side post-run report surgery: `_build_vwap_lookup` + `_apply_vwap_fill`, applied in `_extract_results` and `_extract_slot_from_group_reports`. Result dict carries `vwap_fill_applied` / `vwap_fill_adjustments`. Path A only; Path B (`_USE_BACKTEST_NODE`) is a no-op. |

**Backend layer:** L4 (Execution layer — doesn't exist yet)
**UI tab:** `pf-tab-pf-exit` (line 1075 in `static/js/portfolio.js`) &mdash; **already marked `pf-ui-only`**

---

### 2. `Execution_logics.html` &mdash; Portfolio execution parameters  &mdash;  ⚠️ Partial 🔴⚫

| Field | Status | Notes |
|---|---|---|
| `product` (MIS/NRML) | ✅ **Wired (backtest UX shortcut)** | MIS supplies a default `squareoff_time` from `mis_squareoff_time` / `mis_squareoff_tz` on `PortfolioConfig` when no explicit value is set on portfolio/slot/leg (routed via `effective_portfolio_squareoff` in `models.py` and consumed by the runner at the 3 dispatch sites). Explicit `squareoff_time` always wins. NRML is a no-op. Leverage / margin are NOT modeled — `default_leverage` stays 1. |
| `strategy_tag` | ⚫ **Live-only** | Broker integration. UI marked `pf-live-only`. |
| `leg_fail_action` | ⚫ **Live-only** | Live order-failure handling. UI marked `pf-live-only`. |
| `leg_execution` (Parallel/Sequential) | ⚫ **Live-only** | Live order placement timing. UI marked `pf-live-only`. |
| `portfolio_execution_mode` | ⚫ **Live-only** | "Start time" mode is the implicit backtest default. Other modes (Manual / CombinedPremium / etc.) are options-trading concepts. UI marked `pf-live-only`. |
| `based_on` (DayOpen/StartTime) | ⚫ **Options-only** | Options reference pricing. UI marked `pf-live-only`. |
| `entry_price` | ⚫ **Options-only** | Same. RBO has its own entry-price model — separate concern. |
| `rounding_value` | ⚫ **Options-only** | Strike rounding. UI marked `pf-live-only`. |
| `adjust_price` | ⚫ **Options-only** | Strike offset. UI marked `pf-live-only`. |
| `start_time` (intra-day entry window) | ✅ **End-to-end wired this session** | Bars before this UTC time are dropped per day. See Phase 1 below. |
| `end_time` (intra-day entry window) | ✅ **End-to-end wired this session** | Bars after this UTC time are dropped per day. |
| `sqoff_time` (exec-tab) | ⚪ Wired via Timing tab | The exec-tab's SqOff Time is unwired (`pf-live-only`); the Timing tab's SqOff Time is the canonical wired one (`PortfolioConfig.squareoff_time`). |
| `run_on_days` | ✅ Wired (last session) | See logic 3 below for details. |
| `start_day` / `sqoff_day` / `holiday_handling` | ⚫ **Options-only** | Expiry-aware day handling. UI marked `pf-live-only`. |

**Backend layer:** L3 (Portfolio runner) for entry window + run_on_days; L4 (Execution layer) for the rest
**UI tab:** `pf-tab-pf-exec` (line 508) &mdash; **mixed marking now**: live/options fields gray, wired fields plain, no remaining red in this tab

---

### 3. `Other_Settings_Logic.html` &mdash; Other portfolio adjustments  &mdash;  ⚠️ Partial 🔴

| Field | Status | Notes |
|---|---|---|
| `straddle_width_multiplier` | ❌ Missing | Strike override formula not implemented |
| `delay_between_legs` | ❌ Missing | Re-execution delay not implemented |
| `on_target_action_on` | ❌ Missing | Action filter (Only / Trailing / Both) not implemented |
| `on_sl_action_on` | ❌ Missing | Same as above |
| `run_on_days` | ✅ **End-to-end wired this session** &mdash; see Phase 1 below |

**Backend layer:** L3 (Portfolio runner) for `run_on_days`; L2 (ManagedExitStrategy) for delay & action filters; new schema for straddle multiplier
**UI tab:** `pf-tab-pf-other` (line 826) &mdash; **already marked `pf-ui-only`**

---

### 4. `ReExecute_Logics.html` &mdash; Re-execution plugins  &mdash;  ❌ Missing 🔴

| Field | Status |
|---|---|
| `no_reexec_sl_cost` | ❌ Missing |
| `no_wait_trade_reexec` | ❌ Missing |
| `no_strike_change_reexec` | ❌ Missing |
| `no_reentry_after_end` | ❌ Missing |
| `no_reentry_sl_cost` | ❌ Missing |

**Backend layer:** L2 (ManagedExitStrategy) — needs `leg["sl_moved_to_cost"]` flag tracking
**UI tab:** `pf-tab-pf-reexecute` (line 789) &mdash; **already marked `pf-ui-only`**

---

### 5. `dynamic_Hedge_logics.html` &mdash; Dynamic Hedge  &mdash;  ❌ Missing 🔴

All fields missing — no hedge logic exists in backend.

**Backend layer:** L4 (Execution layer — needs hedge coordination + parent-leg pairing)
**UI tab:** `pf-tab-pf-dynhedge` (line 691) &mdash; **already marked `pf-ui-only`**
**Estimated effort:** 10-15 days (see migration plan)

---

### 6. `legs_logics.html` &mdash; Per-leg configuration  &mdash;  ⚠️ Partial 🔴

| Aspect | Status |
|---|---|
| Single-leg strategies (EMA, RSI, Bollinger) | ✅ Wired (Path A backtest) |
| Multi-leg pyramid sequencing for non-RBO strategies | ❌ Missing |
| Per-leg `Wait & Trade` delay | ❌ Missing |
| Per-leg `Hedge Required` flag | ❌ Missing (no hedge layer) |
| RBO leg fields (`leg1_lots` ... `leg10_lots`, `leg[N]_range_end_hhmm`) | ✅ Fully wired in `strategies/range_breakout.py` |

**Backend layer:** L1 (Strategy) — would require generalized `LegPyramidWrapper`
**UI tab:** mixed across tabs &mdash; **partially marked**

---

### 7. `portfolio_sl_tgt.html` &mdash; Portfolio SL/Target  &mdash;  ✅ Mostly Wired

| Field | Status | Location |
|---|---|---|
| `max_loss` (User-level cap) | ✅ Wired (real clip) | `_user_sl_clip` — first breaching bar force-clips every slot |
| `max_profit` (User-level cap) — **now a real clip** | ✅ Wired | `_user_tgt_clip` — was flag-only; now force-sqoffs (spec §6) |
| User-level Trailing SL (`trailing_sl_*`) | ✅ Wired | `get_user_trailing_sl` + `_user_sl_clip` ratchet (spec §6.1) |
| User-level Trailing Target / Profit-Lock (`trailing_tgt_*`) — **new** | ✅ Wired | `get_user_trailing_target` + `_user_tgt_clip` (spec §6.1 target doc) |
| **Hard halt mid-bar** | ❌ Missing | Slots run to completion; portfolio caps applied post-hoc by the clip |
| `stoploss.type` — Combined Loss | ✅ Wired | `core/backtest_runner.py:_apply_portfolio_clip` |
| `stoploss.type` — Underlying Movement | ✅ Wired | `_underlying_sl_clip` — fires on the primary slot's price crossing `pf_sl_value` (D5 underlying = self) |
| `stoploss.type` — Loss and Underlying Range | ✅ Wired | `_underlying_sl_clip` — hybrid PnL + `pf_sl_underlying_below/above` |
| `stoploss.type` — Combined / Absolute Premium | ❌ Missing | Options-only; downgraded to Combined Loss with a warning |
| `stoploss.trailing.*` | ✅ Wired | `_apply_portfolio_clip` PnL-step ratchet |
| `target.type` — Combined Profit | ✅ Wired | `_apply_portfolio_clip` step 4 |
| `target.type` — Underlying Movement — **new** | ✅ Wired | `_underlying_tgt_clip` — primary-instrument price crossing (was downgraded) |
| `target.trailing.*` (Profit-Lock) | ✅ Wired | `_apply_portfolio_clip` step 3 |
| `portfolio_action` — SqOff | ✅ Wired | Drops post-clip trades for the clipped slot set |
| `portfolio_action` — ReExecute (replay), incl. "at Entry Price" / "Same Contract" variants — **now config-driven** | ✅ Wired | Runs whenever a ReExecute-family action is configured on the portfolio SL/Target (no env flag): re-runs slots flat from the clip via `_filter_bars_after_ns`, splices via `_splice_merged_results`, recurses to the ReExecute count. **Entry-price variants now pin the re-entry**: the replay captures each slot's pre-clip entry price (`_entry_at_clip` — the position open at the clip) and injects `reexec_entry_price`/`reexec_entry_was_long` into the slot worker, which arms the §1.2(d) price-wait so the first re-entry waits until price returns to that original entry before the signal fires (`_is_entry_price_reexec` gates capture). "Same Contract" shares the pin (one FX instrument). `_USE_PF_REEXEC_REPLAY` kept as override |
| `portfolio_action` — cross-portfolio (SqOff/Execute/Start Other) | ✅ Wired | `publish_cross_portfolio_event` / `consume_cross_portfolio_events` — applied across sequential portfolio runs in a server process |
| `portfolio_delay_sl` / `portfolio_delay_target` | ✅ Wired | `pf_*_delay_sec` — deferred-confirm with oscillation guard |
| `move_sl_hit_on_leg_sl` / `move_sl_hit_on_leg_target` (cross-slot) | ✅ **Wired (config-driven)** | Previously `pf-live-only`/flag-gated. Now wired via the two-pass runner, auto-active when either checkbox is set: pass 1 records every leg's SL/target hit timestamps, the parent unions them into a cross-process event bus, pass 2 pre-seeds each worker's `_CROSS_SLOT_EVENT_BUSES`. `core/backtest_runner.py:_compute_agg_coordination`, `core/managed_strategy.py` `__init__` bus pre-seed + Hit-On-Leg branch in `_check_exits`. `_USE_PF_AGG_MOVE_SL` kept as override. |
| `move_sl_agg_pnl_enabled` / `move_sl_agg_pnl_threshold` / `move_sl_agg_pnl_direction` (§2.3 portfolio-aggregate trigger) | ✅ **Wired (config-driven)** | When the whole portfolio's combined P&L crosses the threshold, every open leg snaps its SL to entry. Auto-active when `move_sl_agg_pnl_enabled` is set (no env flag). Pass 1 builds the merged combined-P&L curve, the parent finds the first crossing timestamp, pass 2 injects it via `_MoveSLConfig.agg_trigger_ns` → `ManagedExitConfig.move_sl_agg_trigger_ns` → aggregate branch in `_check_exits`. `core/models.py` PortfolioConfig fields. `_USE_PF_AGG_MOVE_SL` kept as override. |

Two-pass note: when an aggregate / cross-slot Move-SL trigger is configured
(or `_USE_PF_AGG_MOVE_SL=1`), the portfolio runs **twice** (~2× runtime) — a
discovery pass then a deterministic replay pass. No fixpoint iteration: pass 2
is a pure function of pass 1 + the bar data. Path B (`_USE_BACKTEST_NODE`) is
not wired for the cross-slot leg-event surfacing; the aggregate-P&L trigger
still works there (it reads only equity curves).

ReExecute replay note: configuring any ReExecute-family action on the portfolio
SL/Target (or `_USE_PF_REEXEC_REPLAY=1`) turns the `ReExecute`
portfolio action into a genuine clip-then-replay. On a ReExecute clip the
slots are re-run **flat** from the clip timestamp (`_filter_bars_after_ns`
drops pre-cutoff bars), the segment is merged and spliced onto the pre-clip
trades (`_splice_merged_results`), and the loop recurses on the segment's own
first ReExecute clip up to the configured ReExecute count (0 = unlimited,
hard-capped at 50). Default off → the prior "ReExecute keeps trades"
behaviour, zero regression. Portfolio-level metrics are re-derived exactly;
per-slot `per_strategy` sub-metrics keep segment values (documented v1 limit).

**Backend layer:** L3 (Portfolio runner) for halts + simple types; new schema for advanced types
**UI tabs:** `pf-tab-pf-target` (line 875), `pf-tab-pf-stoploss` (line 941) &mdash; **partially marked**

---

### 8. `rbo_logics.html` &mdash; Range Breakout Order  &mdash;  ⚠️ Mixed

**Per-strategy RBO** (`strategies/range_breakout.py`): ✅ Fully implemented &mdash; daily ranges, breakout triggers, leg pyramiding, re-entry limits, opposite-side SL toggle.

**Portfolio-level RBO tab UI** (different feature — applies range-breakout gating to whole portfolio, not just one strategy): ❌ Missing &mdash; `rbo_enabled`, `range_monitoring_start/end`, `entry_at`, `range_buffer`, `cancel_other`, etc. all UI-only.

**Backend layer:** L3 (Portfolio runner for portfolio-level RBO gate)
**UI tab:** `pf-tab-pf-rangebrk` (line 631) &mdash; **already marked `pf-ui-only`**

---

### 9. `sl_tgt.html` &mdash; Slot SL/Target reference  &mdash;  ✅ Mostly Wired

| Field | Status | Location |
|---|---|---|
| `stop_loss_type`, `stop_loss_value` | ✅ Wired | `core/models.py:23-24`, `core/managed_strategy.py:200-256` |
| `trailing_sl_step`, `trailing_sl_offset` | ✅ Wired | Same |
| `target_type`, `target_value` (`none`/`percentage`/`points`/`atr`) | ✅ Wired | `core/models.py`, `core/managed_strategy.py:on_order_filled` |
| `target_lock_trigger`, `target_lock_minimum` (one-shot SL upgrade) | ✅ Wired | Same |
| `on_sl_action`, `on_target_action` (close/re_execute/reverse/execute/re_entry/keep_leg_running) | ✅ Wired | `core/managed_strategy.py:_handle_exit` |
| `on_sl_action` **combinations** (up to 3, comma-separated) | ✅ Wired | `parse_leg_actions` / `validate_leg_actions` (spec §4.8); validated at save |
| `stop_loss_type = "atr"` (ATR-based SL) | ✅ Wired | `sl_atr_period` / `sl_atr_multiplier` — `AverageTrueRange` indicator sized at entry |
| `target_type = "atr"` (ATR-based Target) — **new** | ✅ Wired | `tgt_atr_period` / `tgt_atr_multiplier` — dedicated `AverageTrueRange` (spec §1.1 fn.4) |
| **Target Wait / Delay** (`tgt_wait_sec` / `tgt_wait_bars`) — **new** | ✅ Wired | `_check_exits` TP branch — mirror of SL Wait (spec §4.3) |
| **Leg-level Trailing Target** (`tgt_trail_*` ratcheting profit-lock) — **new** | ✅ Wired | `advance_trailing_target` + `_check_exits` (spec §4.7); `_tp_was_trail` drives `on_target_action_on` |
| SL/Target type **spec-name aliases** (`Premium`/`AbsolutePremium`) — **new** | ✅ Wired | `_canon_exit_type` in `config_from_exit` (spec §4.4) |
| Intrabar high/low SL/TP triggering | ✅ Wired | `_check_exits` — engine default (spec §4.1) |
| **Three-format exit engine** (`exit_price_format` = `ohlcv`/`ltp`/`bidask`) — **new** | ✅ Wired | `resolve_trigger_hl` + Format-A bid/ask subscription & per-ts buffering in `ManagedExitStrategy` (spec §3) |
| `max_re_executions` | ✅ Wired | `core/managed_strategy.py` |
| `sl_wait_sec` / `sl_wait_bars` | ✅ Wired | `core/managed_strategy.py` |
| `squareoff_time`, `squareoff_tz` (with leg > slot > portfolio priority) | ✅ Wired | `core/models.py`, `core/managed_strategy.py` |
| Intraday entry window — End Time exit-monitoring (`entry_end_minute`) — **new** | ✅ Wired | Managed slots keep post-window bars; `_check_entries` gates fresh entries so SL/Target are monitored past End Time until squareoff (spec §9) |
| `no_reentry_after_end` (block ReExecute/ReEntry past End Time) — **new** | ✅ Wired | Real gate in `_check_entries`; threaded portfolio → `_MoveSLConfig` → `ManagedExitConfig` (spec §7 P3) |
| Underlying SL/Target (leg level) | ❌ Missing | Per spec, leg-level Underlying uses the portfolio level instead |
| Conservative VWAP exit fill (SL/Target hits) — **per-portfolio toggle** | ✅ Wired (real session VWAP) | `vwap_exit_fill` config + `_USE_VWAP_FILL` env flag → `default_vwap_fill` threaded to slot workers → `_build_vwap_lookup` / `_apply_vwap_fill` (spec §4.2/§8.1). VWAP is the session-cumulative volume-weighted `Σ(typical·vol)/Σ(vol)`, reset at the venue `session_start_time` (UTC). UI: Exit Settings tab |
| **Directional-close exit fill (§8.1)** — **per-portfolio toggle** | ✅ Wired | `directional_close_fill` config + `_USE_DIRECTIONAL_FILL` env flag → `default_directional_fill` threaded to slot workers → `_build_close_lookup` / `_apply_directional_close_fill`. Directional close is the exit base price (LONG→bid close, SHORT→ask close), modelling the exit half-spread; keeps the engine MID `close` when a quote side is missing (`last_mtm`/`entry` rungs degenerate in a bar backtest). **Composes** with VWAP fill per spec: VWAP owns leg SL/Target exits (§4.2, trigger-extreme anchor), directional close is the base for squareoff/EOD exits (§8.1) — disjoint sets via `vwap_active` skip. Path A only. UI: Exit Settings tab. Tests: `tests/test_vwap_and_agg_movesl.py` |
| **Winter Time Adjustment** (`winter_time_adjust`, +1h DST shift) — **new** | ✅ Wired | `_apply_winter_time` / `add_one_hour` shift entry-window, square-off & RBO times at run start (spec §9). UI: Timing tab |
| **Timing-ordering save validation** (Start ≤ End ≤ SqOff; date sanity) — **new** | ✅ Wired | `server._validate_portfolio_times` (400 on save) + client mirror `_validateTimingOrder` (spec §9) |
| Mark Price (crypto fair-value trigger/fill) | ✅ **Wired (prev-close proxy)** | New `exit_price_format = "mark"` (spec §3). Mark price proxied by the **previous bar's close** (approach B — rolling `_prev_close` updated every bar in `_on_primary_bar`), so SL/Target triggers consult the last settled price and intra-bar wicks (and the current bar's own close) don't fire; first-bar fallback is the current close. Crypto-only: `ManagedExitStrategy.on_start` downgrades to `ohlcv` with a log on non-crypto venues (`_CRYPTO_VENUES` gate); `config_from_exit` lets `"mark"` pass through. `resolve_trigger_hl` mark branch (`mark_price` arg). UI: slot Exit Price Format selector. Tests: `tests/test_three_format_engine.py`. A real exchange mark series is a one-line source swap when/if ingested |
| **Portfolio Tags** (per-tag SL/Target between Portfolio & User) — **new** | ✅ Wired | `core/tags.py` (config/tags.json registry + `_TAG_PNL_AGGREGATOR`); `_merge_portfolio_results` rolls each portfolio's PnL into its tag bucket and runs `_user_sl_clip`/`_user_tgt_clip` with `scope_label="TAG"`, combined via `_earliest_clip`. `portfolio_tag` on PortfolioConfig; UI selector on Timing tab; `/api/tags/*` endpoints. Clips the tag's group on cumulative-combined-PnL breach (spec §11) |
| **Hierarchical limit validation** (portfolio SL ≤ tag SL ≤ user SL; same on Target) — **new** | ✅ Wired | `server._validate_portfolio_hierarchy` (400 on save). Leg→portfolio summing is N/A here (leg SL is a %/points price trigger, not a currency budget) |

#### User-level SL/Target (spec §6) — ✅ Wired

| Field | Status | Location |
|---|---|---|
| User Max Loss + Trailing SL | ✅ Wired | `_user_sl_clip` — real equity-curve clip |
| **User Max Profit** (`max_profit`) — **new clip** | ✅ Wired | `_user_tgt_clip` — real force-sqoff (was flag-only) |
| **User Trailing Target / Profit-Lock** (`trailing_tgt_*`) — **new** | ✅ Wired | `get_user_trailing_target` + `_user_tgt_clip`; admin UI in `adapter_admin` users panel |

#### Portfolio-level Target (spec §5) — ✅ Wired

| Field | Status | Location |
|---|---|---|
| `pf_tgt_type = "Combined Profit"` | ✅ Wired | `_apply_portfolio_clip` |
| **`pf_tgt_type = "Underlying Movement"`** — **new** | ✅ Wired | `_underlying_tgt_clip` — primary-instrument price crossing (was downgraded) |
| Trailing Target / Profit-Lock (`pf_tgt_trail_*`) | ✅ Wired | `_apply_portfolio_clip` step 3 |

**Backend layer:** L2 (ManagedExitStrategy)
**UI tab:** N/A (per-leg config in slot editor) &mdash; **NOT marked** (since base SL/TGT is wired)

---

## Phase 1 — Implemented this session

### `run_on_days` day-of-week filter — ✅ END-TO-END WIRED

**1. Schema** &mdash; [core/models.py:113](core/models.py#L113)
```python
run_on_days: Optional[list[str]] = None
```
Accepts subset of `["MON","TUE","WED","THU","FRI","SAT","SUN"]` (case-insensitive, full names like `"Monday"` also accepted). `None` = no filter.

**2. Helper** &mdash; [core/backtest_runner.py](core/backtest_runner.py)
- `_DAY_NAME_TO_WEEKDAY` map (3-letter and full day name → int 0..6)
- `_allowed_weekdays(run_on_days)` resolves UI-shape into a set of weekday integers
- `_filter_bars_by_weekday(bars, allowed_weekdays)` drops bars whose UTC weekday isn't allowed; uses integer modulo on `ts_event` nanoseconds (no per-bar pandas conversion)

**3. Reader / behavior** &mdash; both engine sites:
- `_run_single_slot()` accepts `default_run_on_days` parameter, applies filter inside `_phase("run_on_days_filter", ...)` after bars are loaded
- `_run_slot_group()` accepts the same parameter and applies the same filter
- `run_portfolio_backtest()` passes `portfolio.run_on_days` down to both worker types

**4. Result surfacing**
- Per-slot result now includes `result["run_on_days"]` (echo of the configured list) and `result["bars_filtered_by_run_on_days"]` (count of bars dropped) when filter is active
- If filter drops every bar, a clear `ValueError` is raised mentioning the filter

**5. UI translation** &mdash; [static/js/portfolio.js:1297-1308](static/js/portfolio.js#L1297-L1308)
```javascript
if (pf._ui.run_on_days === "Custom") {
    pf.run_on_days = pf._ui.selected_days.length ? pf._ui.selected_days : null;
} else if (pf._ui.run_on_days === "Mon - Fri") {
    pf.run_on_days = ["Mon","Tue","Wed","Thu","Fri"];
} else if (pf._ui.run_on_days === "Mon - Thu") {
    pf.run_on_days = ["Mon","Tue","Wed","Thu"];
} else {
    pf.run_on_days = null;  // "All Days" or default
}
```

**6. UI marking change** &mdash; the Run On Days field row at [static/js/portfolio.js:572](static/js/portfolio.js#L572) is no longer marked `pf-ui-only`. The parent fieldset (which contains other UI-only fields like Start Time / End Time at execution-level) is still marked, so the row will inherit some red background visually until those other fields are wired too. To get a fully-non-red Run On Days row, the field should be moved to a separate fieldset in a future cleanup.

**Differential test recipe:**
1. Configure portfolio with default Run On Days = "All Days" → run backtest, save `total_trades` from result.
2. Configure same portfolio with Run On Days = "Mon - Thu" → run again. Expect `total_trades` ≤ previous count (Friday bars now dropped).
3. Configure with Custom selecting just `["Mon"]` → `total_trades` should be much smaller; result includes `bars_filtered_by_run_on_days > 0`.
4. Configure with Custom selecting nothing → `ValueError` raised at backtest start (no bars left after filter).

---

### `entry_start_time` / `entry_end_time` intra-day window — ✅ END-TO-END WIRED (this session)

**1. Schema** &mdash; [core/models.py:120-121](core/models.py#L120-L121)
```python
entry_start_time: Optional[str] = None  # "HH:MM" UTC
entry_end_time: Optional[str] = None    # "HH:MM" UTC
```

**2. Helpers** &mdash; [core/backtest_runner.py](core/backtest_runner.py)
- `_hhmm_to_minute(s)` parses "HH:MM" or "HH:MM:SS" into minute-of-day (0..1439). Returns None for empty/malformed input — never raises.
- `_filter_bars_by_time_of_day(bars, start_hhmm, end_hhmm)` drops bars whose UTC time-of-day falls outside the window. Both endpoints inclusive. Either endpoint may be None for unbounded. Supports wrap-around windows (e.g. 22:00..02:00 keeps overnight bars).

**3. Reader / behavior** &mdash; both engine sites:
- `_run_single_slot()` and `_run_slot_group()` accept `default_entry_start_time` / `default_entry_end_time` parameters. Filter applied inside `_phase("entry_window_filter", ...)` after the run_on_days filter.
- `run_portfolio_backtest()` passes `portfolio.entry_start_time` and `portfolio.entry_end_time` down to all 3 worker submit sites.

**4. Result surfacing**
- Per-slot result includes `entry_start_time`, `entry_end_time`, and `bars_filtered_by_entry_window` when window is configured
- If filter drops every bar, `ValueError` raised mentioning the filter

**5. UI translation** &mdash; [static/js/portfolio.js:1304-1310](static/js/portfolio.js#L1304-L1310)
```javascript
pf.entry_start_time = pf._ui.start_time && pf._ui.start_time !== "00:00:00"
    ? pf._ui.start_time : null;
pf.entry_end_time = pf._ui.end_time && pf._ui.end_time !== "23:59:59"
    ? pf._ui.end_time : null;
```

**6. UI marking change** &mdash; the Start Time and End Time field rows in the Execution Parameters > Timing fieldset are no longer marked `pf-ui-only`. The parent fieldset has had its `pf-ui-only` removed too — it now contains only wired fields (Start Time, End Time, Run On Days) plus three options-only fields marked `pf-live-only` (Start Day / SqOff Day / Holiday).

**Caveat — daily bars bypass the filter automatically:**

The entry window only applies to **intraday** bar types (`-MINUTE-`, `-HOUR-`, `-SECOND-`). Daily / weekly / monthly bars (`-DAY-`, `-WEEK-`, `-MONTH-`) have a single `ts_event` per period (typically 00:00 UTC), so an HH:MM window would unconditionally drop every bar. The filter detects non-intraday bar types via `_is_intraday_bar_type()` and skips itself, surfacing a `"warning"` and an `"entry_window_skipped"` field in the result dict.

This means: if you have a portfolio with daily bars and the UI defaults Start Time = 09:30 / End Time = 16:15, the backtest will run normally — those defaults are silently ignored for that bar type.

**Caveat — exit interaction (intraday bars only):**

Bars dropped at the tail of each day mean the strategy can't process exits past `entry_end_time`. **If your strategy needs to close positions after the window**, set `squareoff_time` (Timing tab) equal to or earlier than `entry_end_time`, so positions are force-closed before the bars stop arriving. Example:
- `entry_start_time = "09:30"` (no entries before 09:30)
- `entry_end_time = "16:00"` (no entries or bars seen after 16:00)
- `squareoff_time = "16:00"` (force-close at 16:00 — fires on the last visible bar)

For 24-hour markets (crypto), set both endpoints to keep all bars (e.g. `00:00..23:59`) or leave them None.

**Differential test recipe:**
1. Run portfolio with no entry window → save `total_trades`.
2. Run with `entry_start_time = "09:30"`, `entry_end_time = "16:00"` → expect fewer trades; result includes `bars_filtered_by_entry_window > 0`.
3. Run with `entry_start_time = "09:30"`, `entry_end_time = "09:30"` → expect 1 bar/day visible; trades drastically reduced.
4. Run with wrap-around `entry_start_time = "22:00"`, `entry_end_time = "02:00"` → expect overnight bars kept, daytime dropped.

---

---

## Dashboard fix this session: `flat_trades` + `decisive_win_rate`

**Why:** A user backtest showed `12,052 trades, 339 wins, 180 losses`. The wins+losses didn't sum to total trades, and the displayed 2.8% win rate was misleading. Cross-checking the position report CSV revealed **11,533 trades had P&L of exactly $0.00** — they're real round-trip trades but the price moved by less than the sub-cent precision could represent (driven by `trade_size: 1` on EURUSD, where each trade was for 1 EUR). The summary cards hid this entire bucket.

**Fix:**

1. Backend computes two new metrics for every result dict:
   - `flat_trades = total_trades - wins - losses` (closed positions where realized P&L rounded to 0)
   - `decisive_win_rate = wins / (wins + losses) * 100` — `None` when no decisive trades exist
2. Wired through all three result-emitting sites:
   - `_extract_results()` (single backtest) — [core/backtest_runner.py:2095-2108](core/backtest_runner.py#L2095-L2108)
   - `_extract_slot_from_group_reports()` (shared-engine path) — [core/backtest_runner.py:830-840](core/backtest_runner.py#L830-L840)
   - `_merge_portfolio_results()` — aggregates `total_flat` across slots, emits portfolio-level decisive rate
3. Dashboard cards updated:
   - New **Flat Trades** card (with hover explaining what it means)
   - New **Decisive Win Rate** card (with hover: "Wins / (Wins + Losses) — excludes flat trades")
   - Per-strategy table gains Decisive Win Rate and Flat columns
4. HTML report template (`docs/report_template.html`) gains the same two KPIs plus client-side derivation in `calculateMetrics()` so downloaded reports show the same numbers as the dashboard.
5. UI falls back to client-side derivation if a legacy backend returns the result dict without the new fields — safe rolling deploy.

**On the user's actual run:**

| Metric | Old display | New display |
|---|---|---|
| Total Trades | 12,052 | 12,052 |
| Wins | 339 | 339 |
| Losses | 180 | 180 |
| **Flat Trades** | (hidden) | **11,533** |
| Win Rate | 2.81% | 2.81% |
| **Decisive Win Rate** | — | **65.32%** |

The 2.81% was always going to be misleading at trade_size=1; the new 65.32% is the meaningful "of trades that produced P&L, what fraction were wins?" figure.

**Differential test recipe:**
1. Run a portfolio with `trade_size=1` on EURUSD → expect a large `flat_trades` count and `decisive_win_rate` very different from `win_rate`
2. Run the same portfolio with `trade_size=100000` → expect `flat_trades=0` and `decisive_win_rate ≈ win_rate`
3. Both should produce the same total P&L (× 100000 in the second case)

---

---

## Path B (BacktestNode) backend implementation — opt-in this session

**What landed:**

1. **Three new entry points** in [core/backtest_runner.py](core/backtest_runner.py), each a drop-in for its Path A counterpart (signatures match exactly, picklable for ProcessPoolExecutor):
   - `run_backtest_node()` — replaces `run_backtest()` body
   - `_run_single_slot_node()` — replaces `_run_single_slot()` body
   - `_run_slot_group_node()` — replaces `_run_slot_group()` body
2. **Shared `_build_run_config()` helper** that produces the three Nautilus configs (`BacktestVenueConfig` / `BacktestDataConfig` / `BacktestEngineConfig`) bundled into a `BacktestRunConfig`. Single source of truth for the venue/data/engine wiring used by all three Path B variants.
3. **Env-flag gates** at the top of each public/worker function:
   - `run_backtest()` routes to `run_backtest_node()` when `_USE_BACKTEST_NODE=1`
   - `_run_single_slot()` and `_run_slot_group()` route to their `_node` variants only when `_USE_BACKTEST_NODE=1` **and** no filters are configured
4. **Result tagging** — Path B results get `"path_b": True` so you can tell at a glance which engine path produced them.

### How to opt in

```bash
# PowerShell
$env:_USE_BACKTEST_NODE = "1"

# Then run a backtest as normal
python server.py
```

The flag is process-scoped — child workers inherit it from the parent. Unset to revert.

### ⚠️ Known limitation: `run_on_days` and entry-window filters auto-fall-back to Path A

Path B reads bars directly from the catalog via `BacktestDataConfig`. There's no place to inject our weekday or time-of-day filtering between the catalog and the engine — pre-loading + filtering would conflict with the node's own data load on `run()`.

When **either** `run_on_days` or `entry_start_time`/`entry_end_time` is configured, the gate auto-falls-back to Path A so the run remains correct. This is detected by `_path_b_supports_filters()` and is silent (no warning emitted). Result is identical to running Path A for those slots.

To use Path B:
- Set `_USE_BACKTEST_NODE=1`
- Use a portfolio without `run_on_days` set (or set to "All Days")
- Use a portfolio without intra-day entry window (or use 24-hour bars where window is irrelevant)

If you need both Path B and the filters together, that's future work (would require translating the filters into pyarrow expressions for `BacktestDataConfig.filter_expr`).

### Verification

```
python verify_session_changes.py
```

Section 19 covers Path B specifically — 28 checks confirming:
- All four config classes import correctly
- Helper functions exist and behave correctly (env flag, filter compatibility check)
- `_build_run_config()` produces a valid `BacktestRunConfig` with correct field types (`starting_balances` as `list[str]`, etc.)
- All three Path B entry points exist with signatures matching their Path A counterparts (so they're true drop-ins)
- Env-flag gate code is present in all three sites
- `path_b: True` tag is surfaced in result dicts

**121 of 121 verification checks pass.** Static + API correctness is confirmed; bit-exact parity testing requires a real run with `_USE_BACKTEST_NODE=1` against a Path A baseline.

### Recommended verification before flipping the default

1. Pick a portfolio with NO `run_on_days` and NO entry window (e.g. one of the `FX_9_Slot_*` ones with those fields cleared).
2. Run with `_USE_BACKTEST_NODE` unset → save `total_pnl`, `total_trades`, `wins`, `losses` from the result.
3. Set `$env:_USE_BACKTEST_NODE = "1"`, run again → expect bit-exact match.
4. If matched, look at the result dict — should now contain `"path_b": True`.
5. Compare timing — Path B might be slightly faster due to fewer Python-side bar manipulations, but the difference for `chunk_size=None` runs should be small.

Once parity is confirmed across a few portfolios, you can:
- Make it default by removing the gate (or flipping the default of the env flag)
- Try `chunk_size=100_000` (requires editing `_build_run_config` callers) for memory savings on long backtests

---

## What's still pending (not in this session)

| Logic | Effort | Priority |
|---|---|---|
| `on_target_action_on` / `on_sl_action_on` filters | 1 day | High |
| `delay_between_legs` between re-executions | 1 day | High |
| Hard portfolio halt mid-bar (`max_loss` / `max_profit`) | 2-3 days | High |
| ReExecute 5 plugins (`no_reexec_sl_cost`, etc.) | 3-4 days | Medium |
| `straddle_width_multiplier` strike override | 2 days | Medium |
| Portfolio-level SL/TGT advanced types (Combined Premium, Underlying Movement, etc.) | 5-7 days | Medium |
| Portfolio-level RBO tab | 3-5 days | Low |
| Generalized leg pyramid (non-RBO strategies) | 5-7 days | Low |
| Execution parameters layer (product, strategy_tag, leg_*, etc.) | 5-10 days | Low (live trading) |
| Dynamic Hedge | 10-15 days | Low (live trading) |

**Total remaining:** roughly 5-8 weeks of focused work.

---

## How to mark a section as &ldquo;not implemented&rdquo;

Apply the `pf-ui-only` class to the container, fieldset, and field rows. Example pattern from `static/js/portfolio.js`:

```html
<div class="pf-tab-content" id="pf-tab-pf-XXX">
    <fieldset class="pf-fieldset pf-ui-only">
        <legend>Section Name</legend>
        <div class="pf-field-row pf-ui-only">
            <span class="pf-field-label">Field Label</span>
            <input type="text" ... />
        </div>
    </fieldset>
</div>
```

CSS rules at [static/css/style.css:1157-1181](static/css/style.css#L1157-L1181) handle the visual styling automatically.

When implementing a logic, **remove** the `pf-ui-only` class from the matching section so users see it's now functional.
