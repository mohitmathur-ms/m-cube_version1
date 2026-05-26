# Code Review Report

| Field | Value |
|-------|-------|
| Date | 2026-05-26 16:11 |
| Reviewer | Claude (automated) |
| Feature/Task | Replace market-order exits with native NautilusTrader bracket orders (`_USE_NATIVE_BRACKET`) |
| Files Reviewed | 4 (core/managed_strategy.py, CLAUDE.md, how_to_use.txt, scripts/generate_native_bracket_report.py) + 1 test harness |
| Review Duration | ~12 min |

Scope note: `git diff` against HEAD also shows pre-existing uncommitted Phase‑3 (`exit_check_on_base_bar`) edits in `core/managed_strategy.py`, `core/models/exit_config/__init__.py`, `static/js/portfolio.js`, `LOGICS_BACKEND_STATUS.md`, `tests/test_multi_tf_exits.py` that were **not authored in this session**. This review covers only the native-bracket changes.

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 0 |
| Logic | 2 |
| Performance | 0 |
| Style | 2 |
| **Total** | **4** |

**Verdict: PASS_WITH_WARNINGS**

---

## Findings

### Critical
> None found.

### Logic

#### [L-1] Bracket-submission fallback leaves a native-bracket leg with NO exit management
- **File:** `core/managed_strategy.py:2021-2025` (`_submit_bracket`)
- **Issue:** When the computed SL/TP are unusable (`sl <= 0 or tp <= 0`), the code degrades to `self._submit_order(side)` (a plain market entry). But `self._exit_mode` remains `"native_bracket"`, so `_on_primary_bar` (`:1132`) still **skips** the Python `_check_exits`. The result is an open position with **no resting SL/TP and no Python monitoring** — it can only ever close via squareoff or engine-end.
- **Impact:** A silently unmanaged position (unbounded risk) if the fallback is ever reached. Currently **unreachable in practice**: the classifier requires `stop_loss_value`/`target_value > 0` for points/percentage, and `indicators_initialized()` gates entry until ATR is valid (so ATR `dist > 0`). So it is latent/defensive, not an active bug — but it is a trap for future changes.
- **Fix:** On fallback, also force this leg onto the Python path for monitoring — e.g. set `self._exit_mode = "python"` before `_submit_order`/`_set_exit_levels`, and `log.warning(...)`. Alternatively raise/skip the entry rather than entering unmanaged.

#### [L-2] Native-bracket exits lose the rich EXIT-REASON tag the Python path produces
- **File:** `core/managed_strategy.py:2039` (entry only) vs `_handle_exit` reason-building at `:1547-1573`
- **Issue:** The Python path tags the closing fill with a descriptive reason (e.g. `"Stop Loss: price=… ≤ SL=… (entry …, -1.23%)"`) consumed by the orderbook's EXIT REASON / EXIT DETAILED REASON columns. Native-bracket children carry only the `order_factory` default tags (`"STOP_LOSS"` / `"TAKE_PROFIT"`), so the orderbook EXIT REASON for native legs is coarser than for Python legs.
- **Impact:** Reporting inconsistency between the two exit modes — not a functional failure, but the tearsheet/orderbook EXIT REASON column will look different (less detailed) for native-bracket legs. Downstream code that string-matches the old reason prefixes (`"Stop Loss"`, `"Take Profit"`) may not match `"STOP_LOSS"`.
- **Fix:** Pass `sl_tags=[...]` / `tp_tags=[...]` to `order_factory.bracket(...)` with reason strings whose prefix matches the orderbook's split convention (`"Stop Loss: …"` / `"Take Profit: …"`), or normalize the factory tags in the orderbook builder.

### Performance
> None found. (No new loops, I/O, or allocations on the hot path; classification runs once per leg at config build.)

### Style

#### [S-1] `on_position_closed` parameter `event` is unused
- **File:** `core/managed_strategy.py:1885`
- **Issue:** `event` is unused (the handler only resets state). This is required by the NautilusTrader handler signature, so it is intentional, but it trips linters (IDE hint already flagged it).
- **Fix:** Optional — rename to `_event` or add a short `# noqa`/comment noting the signature requirement (a docstring already explains the handler).

#### [S-2] `_submit_bracket` sets `self.current_sl/current_tp` for "parity" but they are never read for native legs
- **File:** `core/managed_strategy.py:2043-2044`
- **Issue:** For native-bracket legs `_check_exits` is skipped, so `current_sl`/`current_tp` are written but not used by the exit logic (only potentially by reporting). Slightly misleading — implies monitoring that doesn't happen.
- **Fix:** Keep if the orderbook/reporting reads them; otherwise drop, or add a comment clarifying they are report-only for this mode.

---

## Strengths
- **Single guarded exit chokepoint:** `_check_exits` has exactly one call site (`core/managed_strategy.py:1135`), and the native-bracket guard wraps it (`:1126-1135`). This makes "native legs never run Python exit checks" robust across the per-bar, aggregated, and base-bar (Phase‑3) dispatch paths — verified by grep (one call site only).
- **Parity-safe rollout:** Gated behind `_USE_NATIVE_BRACKET` (`_classify_exit_mode`, `:165`), default off; 259/259 tests pass with the flag both off and on (Evidence).
- **DRY refactor:** `_compute_sl_tp` (`:1877`) is shared by the Python (`on_order_filled`) and native (`_submit_bracket`) paths; `on_order_filled` behavior is unchanged (still keyed off the fill price).
- **Correct idiomatic wiring (ntm3-confirmed):** `submit_order_list`, no `position_id` under NETTING, `OrderFilled.order_type` branch, `on_position_closed` flat reset, `cancel_all_orders` before close in squareoff (`:1276-1278`), `tp_post_only=False` to avoid taker rejection on L1 bars.
- **Conservative classifier:** `_classify_exit_mode` excludes trailing / target-lock / wait-bars / RBO / Move-SL / non-close actions and requires both SL and TP — verified by the e2e Part A matrix.

---

## Recommendations
- Address **L-1** before enabling `_USE_NATIVE_BRACKET=1` in any shared environment — even though unreachable today, an unmanaged position is the worst failure mode and the fix is one line.
- Consider promoting `scripts/sim_native_bracket_e2e.py` into `tests/` as a pytest (it already asserts + exits non-zero) so the native-bracket routing is covered by the regular suite, not only an ad-hoc script.
- The separate `verify_session_changes.py` harness currently crashes on a stale `core/backtest_runner.py` path (pre-existing, unrelated to this change) — worth a follow-up fix so the project's verification gate runs again.

---

## Evidence

```
$ venv/Scripts/python.exe -m pytest tests/ -q
259 passed in 2.91s        PYTEST_EXIT=0

$ _USE_NATIVE_BRACKET=1 venv/Scripts/python.exe -m pytest tests/ -q
259 passed in 2.76s        PYTEST_FLAGON_EXIT=0

$ venv/Scripts/python.exe scripts/sim_native_bracket_e2e.py
  Part A — config_from_exit classification
    [PASS] plain SL+TP, flag on  -> native_bracket
    [PASS] trailing SL           -> python
    [PASS] SL only (no TP)       -> python
    [PASS] on_sl_action=re_execute-> python
    [PASS] plain SL+TP, flag OFF  -> python
  Part B — engine run (exit_mode=native_bracket)
    bracket orders: ['1', '3', '2']            # MARKET + STOP_MARKET + LIMIT
    entry MARKET fill = 1.10011
    SL trigger price  = 1.09910   SL fill = 1.09909   slip-vs-trigger = 0.07 pips
    (a Python market-close would fill ~1.09850, 6.0 pips from the stop)
    [PASS] bracket = MARKET + STOP_MARKET + LIMIT (one order_list)
    [PASS] SL filled AT trigger price (<=0.5 pip)
    [PASS] bracket fill far closer to stop than market-close baseline
  RESULT: PASS   (exit 0)

$ for s in sim_phase1_signal sim_phase2_bracket sim_phase3_python_exit; do ...; done
  sim_phase1_signal     -> exit 0  PASS
  sim_phase2_bracket    -> exit 0  PASS
  sim_phase3_python_exit-> exit 0  PASS

$ venv/Scripts/python.exe scripts/generate_native_bracket_report.py
  Market  mean |slip| = 7.00 pips (worst 10.00)
  Bracket mean |slip| = 1.67 pips (worst 5.00)
```

Grep confirming the single guarded exit chokepoint:
```
$ grep -n "self._check_exits(" core/managed_strategy.py
1135:                self._check_exits(close, is_long, is_short, eff_high, eff_low)   # only call site
```
