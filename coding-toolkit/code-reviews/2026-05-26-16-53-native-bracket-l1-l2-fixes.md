# Code Review Report

| Field | Value |
|-------|-------|
| Date | 2026-05-26 16:53 |
| Reviewer | Claude (automated) |
| Feature/Task | Fix L-1 (unmanaged-position fallback) & L-2 (exit-reason tags) in the native-bracket path (`_USE_NATIVE_BRACKET`) |
| Files Reviewed | 1 (`core/managed_strategy.py` — `_submit_bracket` only) |
| Review Duration | ~1 min (subagent), follow-up to the 16-11 review |

Scope note: this review covers **only** the two fixes made this session inside
`_submit_bracket`. The same file carries other uncommitted edits (Phase‑3
`exit_check_on_base_bar`) that were **not** authored in this work and are out of scope.
This report follows up `2026-05-26-16-11-native-bracket-orders.md`, which raised L-1/L-2.

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 0 |
| Logic | 0 |
| Performance | 0 |
| Style | 1 |
| **Total** | **1** |

**Verdict: PASS**

(Zero Critical + zero Logic = PASS per the reporter verdict table. The two prior Logic
findings, L-1 and L-2, are now resolved and verified correct.)

---

## Findings

### Critical
> None found.

### Logic
> None found.

The two findings from the predecessor report are resolved:

- **L-1 (was: bracket-submission fallback leaves a leg with no exit management) — FIXED.**
  `core/managed_strategy.py:2022-2040` (`_submit_bracket`). When `_compute_sl_tp` cannot
  produce a usable two-sided bracket (`sl <= 0 or tp <= 0`), the fallback now sets
  `self._exit_mode = "python"`, logs a warning, and submits a plain market entry. With
  the mode flipped, the entry fill flows through the Python branch of `on_order_filled`
  (`:1854-1883`), which recomputes `current_sl`/`current_tp` from the **actual fill
  price** and re-seeds the exit state machine; `_on_primary_bar` (`:1125`) then runs
  `_check_exits` for the leg. The unmanaged-position (unbounded-risk) gap is closed.

- **L-2 (was: native exits lose the rich EXIT-REASON tag) — FIXED.**
  `core/managed_strategy.py:2052-2062`. The bracket is now submitted with
  `sl_tags=[sl_reason]` / `tp_tags=[tp_reason]` whose prefixes are `"Stop Loss: …"` /
  `"Take Profit: …"`. The orderbook builder derives the EXIT REASON column via
  `core/report_generator.py:354` (`exit_tags_raw.split(":", 1)[0].strip()`), so native
  legs now yield `Stop Loss` / `Take Profit` — consistent with the Python path
  (`_handle_exit`, `:1640`/`:1652`) and matching downstream prefix string-matches. The
  `≤`/`≥` operators are oriented correctly for buy vs sell.

### Performance
> None found. (Two f-string builds and two kwargs added to a path that runs once per
> entry; classification still happens once per leg at config-build time.)

### Style

#### [S-1] Native-bracket EXIT DETAILED REASON shows an approximate entry price
- **File:** `core/managed_strategy.py:2056-2057`
- **Issue:** The detail strings read `entry ~{ref_price:.4f}` using the signal-bar close,
  not the true entry, which fills at the next bar's open. The Python path's detail carries
  the real fill price and realized pct.
- **Impact:** Minor reporting asymmetry in the EXIT DETAILED REASON column for native-bracket
  rows only. The EXIT REASON column is unaffected (it consumes only the prefix before `:`),
  and the SL/TP **trigger** prices shown are exact. The actual fill price is genuinely
  unknown at submit time, so the `~` is honest.
- **Fix:** Accepted as-is. Only revisit if a downstream consumer parses the DETAILED column
  for native-bracket rows.

Resolved during this session (no longer open):
- Reachability + permanent-mode-switch rationale documented in the fallback comment
  (`_submit_bracket`).
- The vestigial `self._set_exit_levels(side)` call in the fallback is annotated as a no-op
  stub whose real seeding happens on the fill.

---

## Strengths
- **Single guarded exit chokepoint preserved:** `_check_exits` still has one call site
  (`core/managed_strategy.py:1139`) behind the `self._exit_mode != "native_bracket"` guard
  (`:1125`), so flipping a fallback leg to `"python"` cleanly re-enables monitoring across
  the per-bar / aggregated / base-bar dispatch paths.
- **Fill-driven re-seed on fallback:** the fix relies on the proven Python
  `on_order_filled` path to compute SL/TP from the real fill price rather than the stale
  signal-bar `ref_price` — strictly safer than an unmanaged position.
- **L-1 is load-bearing, not defensive:** `_classify_exit_mode` (`:177-186`) admits ATR
  legs on `sl_atr_multiplier > 0` alone, so a cold ATR at the first signal genuinely drives
  `_compute_sl_tp` to `0.0` and triggers the fallback (see correction below).
- **Idiomatic Nautilus wiring:** `sl_tags` / `tp_tags` (and the existing `entry_tags`,
  `tp_post_only`) are confirmed valid kwargs of `OrderFactory.bracket` in
  `nautilus_trader==1.224.0` via signature introspection.

---

## Recommendations
- **Correction to the predecessor report:** `2026-05-26-16-11-native-bracket-orders.md`
  described the L-1 fallback as "currently unreachable in practice." That is inaccurate —
  `_classify_exit_mode` (`core/managed_strategy.py:177-186`) classifies an ATR/ATR leg as
  `native_bracket` based on `sl_atr_multiplier > 0`, **not** on the ATR being initialized.
  On the first signal the ATR can still be cold, so `_compute_sl_tp` returns `0.0` and the
  fallback fires. L-1 fixes a reachable bug, not a latent one.
- Carry forward the prior report's still-open items (both out of scope here):
  promote `scripts/sim_native_bracket_e2e.py` into `tests/` as a pytest; fix the stale
  `core/backtest_runner.py` path that crashes `verify_session_changes.py`.

---

## Evidence

```
$ venv/Scripts/python.exe -m pytest tests/ -q
259 passed in 5.13s                                    # flag OFF

$ _USE_NATIVE_BRACKET=1 venv/Scripts/python.exe -m pytest tests/ -q
259 passed in 3.69s                                    # flag ON

$ venv/Scripts/python.exe scripts/sim_native_bracket_e2e.py
  Part A — config_from_exit classification
    [PASS] plain SL+TP, flag on  -> native_bracket
    [PASS] trailing SL           -> python
    [PASS] SL only (no TP)       -> python
    [PASS] on_sl_action=re_execute-> python
    [PASS] plain SL+TP, flag OFF  -> python
  Part B — engine run (exit_mode=native_bracket)
    entry MARKET fill = 1.10011
    SL trigger price  = 1.09910   SL fill = 1.09909   slip-vs-trigger = 0.07 pips
    [PASS] bracket = MARKET + STOP_MARKET + LIMIT (one order_list)
    [PASS] SL filled AT trigger price (<=0.5 pip)
    [PASS] bracket fill far closer to stop than market-close baseline
  RESULT: PASS   (exit 0)
```

L-2 manual verification — bracket child order tags after the e2e run
(`PYTHONIOENCODING=utf-8 _USE_NATIVE_BRACKET=1 … print(o.order_type, '->', o.tags)`):
```
1 -> ['EMA Cross BUY: fast(2)=1.1001 ≥ slow(3)=1.1001']        # entry (MARKET)
3 -> ['Stop Loss: native bracket SL=1.0991 (entry ~1.1001, trigger ≤)']   # STOP_MARKET
2 -> ['Take Profit: native bracket TP=1.1021 (entry ~1.1001, trigger ≥)'] # LIMIT
```
→ `split(":",1)[0]` yields `Stop Loss` / `Take Profit`, consistent with the Python path.

```
$ venv/Scripts/python.exe scripts/generate_native_bracket_report.py
  wrote D:\m-cube_version1\html_reports\native_bracket_orders_report.html
  Market  mean |slip| = 7.00 pips (worst 10.00)
  Bracket mean |slip| = 1.67 pips (worst 5.00)
```
