# Fix Report — Explicit Overnight Entry-Window Flag (`entry_window_overnight`)

**Date:** 2026-06-15
**Area:** Portfolio timing validation (entry window / square-off)
**Type:** Feature + bug fix (save-validation correctness)

---

## 1. Problem

Saving a portfolio whose entry window or square-off crossed midnight was rejected with:

> `Entry End Time 23:30:00 is after Square-off Time 01:30:00 (spec §9: End ≤ SqOff).`

The save-validator (`server.py::_validate_portfolio_times` and its JS mirror
`portfolio.js::_validateTimingOrder`) compared the times as **naive same-day
minutes-of-day**: `23:30 → 1410`, `01:30 → 90`, and `1410 > 90` ⇒ rejected. It
had no concept of an overnight session, so a legitimate "enter until 23:30,
square off 01:30 the next morning" window was impossible to save.

### Key finding: the engine already supported overnight; only the validator didn't

The runtime was *more capable than the validator allowed*:

- **Bar filter** — `core/backtest_runner/bar_filters/__init__.py::_filter_bars_by_time_of_day`
  already handles a wrap-around window via its `if lo > hi:` branch, keeping bars
  in `[lo, 24h) ∪ [0, hi]`.
- **Square-off** — `core/managed_strategy.py` (~L1322) is a **per-calendar-day**
  trigger: it tracks `local_date`, resets each new session, and force-closes
  when `local_min >= squareoff_min`. An overnight position is correctly closed
  the next morning when the clock reaches the square-off time.

So the runtime could already run a 23:30 → 01:30 window. The validator was the
sole blocker — and it blocked it with a same-day comparison that could never
express an overnight window.

---

## 2. Solution chosen

Rather than **silently inferring** "overnight" from `start > end` (surprising,
and a typo like `18:00`/`08:00` would become a silent 14-hour overnight window),
we added an **explicit opt-in boolean** `entry_window_overnight`.

- **OFF (default):** behavior unchanged — a window whose End/SqOff decreases
  past midnight is rejected as a typo (the desired safety).
- **ON:** the validator compares the window in **elapsed minutes** — each
  endpoint that lands at/before the previous one in the `Start → End → SqOff`
  chain is rolled `+1 day` before the ordering check.

The flag governs **save-validation and UI intent only** — no engine change was
needed, because the runtime already wraps the window and squares off per day.

`MIS` (intraday, must close same session) **forces the flag off** in the UI;
overnight is a `NRML` / 24h-session concept.

---

## 3. Changes made

| # | File | Change |
|---|------|--------|
| 1 | `core/models/portfolio_config/__init__.py` | New schema field `entry_window_overnight: bool = False` (with docstring) after `entry_end_time`. |
| 2 | `server.py` (`_validate_portfolio_times`) | Replaced the same-day ordering checks with elapsed-minute logic that rolls post-start endpoints `+1 day` when the flag is on. |
| 3 | `static/js/portfolio.js` (open/hydrate) | Seed `pf._ui.overnight` from the persisted field so the checkbox shows the saved value on re-edit. |
| 4 | `static/js/portfolio.js` (Timing tab render) | New checkbox **"Window spans to next day (overnight)"** (`#pf-m-overnight`), rendered `disabled` when product is MIS. |
| 5 | `static/js/portfolio.js` (serialize) | Write the checkbox state into `pf.entry_window_overnight` on save. |
| 6 | `static/js/portfolio.js` (`_onProductChange`) | MIS → uncheck + disable the box; NRML → re-enable. |
| 7 | `static/js/portfolio.js` (`_validateTimingOrder`) | Client mirror of the elapsed-minute logic so the user gets immediate feedback. |
| 8 | `config/portfolio_tabs/timing.json` | Tab metadata entry for the new field (`backend_enabled: true`). |
| 9 | `LOGICS_BACKEND_STATUS.md` | Updated the timing-ordering validation row to document overnight support. |

**Serialization** needed no change: `portfolio_to_dict` uses `asdict` and
`portfolio_from_dict` uses the schema-driven `_filter_known_fields`, so the new
field round-trips automatically. **The engine was not touched.**

---

## 4. Verification

**Schema round-trip** — `entry_window_overnight` serializes/deserializes (`True → True`).

**Validator matrix** (`server._validate_portfolio_times`):

| Case | Flag | Window | Expected | Result |
|------|------|--------|----------|--------|
| Original bug | OFF | End 23:30 / SqOff 01:30 | REJECT | ✅ REJECT |
| Same window opted-in | ON | End 23:30 / SqOff 01:30 | PASS | ✅ PASS |
| Full overnight | ON | 22:00–23:30 / SqOff 01:30 | PASS | ✅ PASS |
| 23h wrap | ON | 10:00–09:00 / SqOff 09:30 | PASS | ✅ PASS |
| Normal same-day | OFF | 09:30–16:00 / SqOff 20:00 | PASS | ✅ PASS |
| Genuine typo | OFF | Start 18:00 / End 09:00 | REJECT | ✅ REJECT |
| MIS overnight opt-in | ON | End 23:30 / mis-SqOff 01:30 | PASS | ✅ PASS |

**Parse checks:** `server.py`, `portfolio_config/__init__.py` parse clean (`PY OK`);
`timing.json` valid (`JSON OK`). All 7 JS edits confirmed present and the
`_validateTimingOrder` block re-read coherent (no dangling lines).

---

## 5. Advantages of this fix

1. **Unblocks a real, already-supported capability.** Overnight windows ran
   correctly at runtime all along; this removes the artificial save-time wall —
   no engine risk.
2. **Explicit beats implicit.** Opt-in intent means a mistyped same-day window
   (e.g. `18:00`/`08:00`) is still caught as a typo instead of silently becoming
   a 14-hour overnight window. Safety is preserved by default.
3. **Validator now matches engine capability.** The save-time rule and the
   runtime bar-filter/square-off behavior are finally consistent — the class of
   "the engine can do it but the UI won't let me save it" bug is closed for this
   field.
4. **Correct product semantics.** Overnight is tied to NRML/24h sessions and
   force-disabled for MIS (intraday must close same session) — the opposite of
   the initial instinct to gate it *behind* MIS.
5. **Zero migration / backward-compatible.** New field defaults to `False`;
   every existing portfolio validates and behaves exactly as before. Schema-
   driven serialization means no (de)serialization code changed.
6. **Server + client parity.** Both validators use identical elapsed-minute
   logic, so the user gets instant feedback that matches the server's verdict
   (no swallowed 400s).

---

## 6. Notes / scope boundaries

- The runtime bar filter still wraps a window purely on `lo > hi`, independent
  of the flag (unchanged, low-risk). The flag deliberately governs **save
  validation + UI intent**; it does not alter bar-filtering or square-off
  execution.
- An overnight window is most meaningful with an explicit entry **Start** time
  to anchor the roll; the validator still degrades gracefully (anchors on End,
  then Start) when an endpoint is unset.
