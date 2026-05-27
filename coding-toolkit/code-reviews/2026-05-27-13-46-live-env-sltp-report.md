# Code Review Report

| Field | Value |
|-------|-------|
| Date | 2026-05-27 13:46 |
| Reviewer | Claude (automated) |
| Feature/Task | Commit 38c8a21 — research report: who monitors SL/TP in LIVE trading (venue vs Python) |
| Files Reviewed | 2 |
| Review Duration | ~6 min |

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 0 |
| Logic | 1 |
| Performance | 0 |
| Style | 3 |
| **Total** | **4** |

**Verdict: PASS_WITH_WARNINGS**

This commit is 99% a documentation deliverable (`html_reports/Live_Env_Monitoring_sl_tp.html`, 211 lines) plus one stray whitespace line in `core/managed_strategy.py`. There is no executable logic change. The one Logic finding is a factual incompleteness in the report's reproduction of NautilusTrader source, not a runtime bug. Safe to keep, but worth correcting the report and dropping the stray line.

---

## Findings

### Critical
> None found.

### Logic

#### [L-1] Routing snippet omits the `exec_algorithm_id` branch — presents a 3-way route as 2-way
- **File:** `html_reports/Live_Env_Monitoring_sl_tp.html:104-108` (section 2 code block)
- **Issue:** The report quotes `trading/strategy.pyx` routing as a simple `if emulation_trigger != NO_TRIGGER … else send_risk_command`. The actual source (verified at `trading/strategy.pyx:891-897`) has **three** branches:
  ```
  if order.emulation_trigger != TriggerType.NO_TRIGGER:
      self._manager.send_emulator_command(command)
  elif order.exec_algorithm_id is not None:
      self._manager.send_algo_command(command, order.exec_algorithm_id)   # ← omitted in report
  else:
      self._manager.send_risk_command(command)
  ```
- **Impact:** A reader trusting the report would conclude the only fork is "emulate vs venue." For the SL/TP-ownership question the conclusion is unaffected (default brackets carry no exec algo), but presenting an edited snippet as a verbatim source quote (`src` citation) undermines the report's accuracy claim. The cited line range `891-897` is correct, so the omission is visible to anyone who opens the file.
- **Fix:** Either add the `elif exec_algorithm_id` branch to the snippet with a one-line note ("exec-algo route, not relevant to brackets"), or change the framing from a verbatim quote to "simplified — see source" so it is not read as exact.

### Performance
> None found.

### Style

#### [S-1] Stray blank line with trailing whitespace, unrelated to the commit's purpose
- **File:** `core/managed_strategy.py:2054`
- **Issue:** The only code change in this commit inserts a blank line containing 8 trailing spaces (`        $`, confirmed via `cat -A`) between the `tp_reason` assignment and `self.order_factory.bracket(`. It is unrelated to the research-report intent of the commit and introduces trailing whitespace.
- **Fix:** Remove the line (or strip it to a clean empty line). Mixing an incidental edit into a docs-only commit also muddies `git blame` on the native-bracket code path.

#### [S-2] `support_contingent_orders=True` presented as an m-cube setting, but it is never set in the codebase
- **File:** `html_reports/Live_Env_Monitoring_sl_tp.html:147`
- **Issue:** The OCO/OUO cell states the behaviour is "enforced by simulated venue (`support_contingent_orders=True`)". A repo-wide grep finds this token **only** in the report — m-cube never passes it to `add_venue`. It is a NautilusTrader `add_venue` default (True), so the behaviour is real, but the report implies m-cube configures it explicitly.
- **Fix:** Reword to "Nautilus `add_venue` default (`support_contingent_orders=True`)" so it is clear this is inherited default behaviour, not m-cube configuration.

#### [S-3] Missing newline at end of file
- **File:** `html_reports/Live_Env_Monitoring_sl_tp.html:211`
- **Issue:** `git show` reports `\ No newline at end of file`. Minor POSIX/tooling nit; some diff and concat tools complain.
- **Fix:** Add a trailing newline.

---

## Strengths
- **Citations are accurate and verifiable.** I checked the two NautilusTrader source references against the installed `nautilus_trader==1.224.0` package: `trading/strategy.pyx:891-897` (the routing fork) and `common/factories.pyx:1259-1263` (the `NO_TRIGGER` docstring "orders are sent directly to the venue") both match the quoted text and line ranges exactly.
- **m-cube symbol references all exist.** `_classify_exit_mode` (`managed_strategy.py:152`), `_run_exit_phase` (`:1074`), `resolve_trigger_hl` (`:212`), `self.current_sl`/`self.current_tp` (`:525-526`), and the `_USE_NATIVE_BRACKET` gate (`:166`) are all real — the report does not invent APIs.
- **Conclusion is correct and well-scoped.** The central claim — default `bracket()` → `NO_TRIGGER` → venue-managed; m-cube's default Python state machine is the hand-rolled equivalent of the emulated path; backtest uses `SimulatedExchange` — is consistent with both the package source and `managed_strategy.py`. It also correctly notes the CLAUDE.md fact that L4/live execution does not exist yet, so only the backtest column applies today.
- Clear three-column comparison table and a routing knowledge-graph that distinguishes venue / Python / engine paths with a legend.

---

## Recommendations
- The HTML pulls Mermaid from a public CDN (`cdn.jsdelivr.net/npm/mermaid@10`). For an internal/offline research artifact, consider vendoring the script or noting the network dependency, so the diagram still renders without internet.
- Keep documentation-only commits separate from code edits. Had the stray `managed_strategy.py:2054` line not been bundled here, this would have been a pure-docs commit with zero code-review surface.
- Consider cross-linking this report to `scripts/generate_native_bracket_report.py`, which already documents the OUO/OTO contingency model the report's section 3 references — they cover adjacent ground.

---

## Evidence

Commit contents:
```
$ git show 38c8a21 --stat
 core/managed_strategy.py                    |   1 +
 html_reports/Live_Env_Monitoring_sl_tp.html | 211 ++++++++++++++++++++++++++++
 2 files changed, 212 insertions(+)
```

Stray trailing-whitespace line (S-1):
```
$ git show 38c8a21:core/managed_strategy.py | sed -n '2051,2055p' | cat -A
        tp_op = "..." if is_buy else "..."$
        sl_reason = f"Stop Loss: native bracket SL=..."$
        tp_reason = f"Take Profit: native bracket TP=..."$
        $                         ← inserted blank line, 8 trailing spaces
        bracket = self.order_factory.bracket($
```

Citation verification — `trading/strategy.pyx:891-897` (L-1 / Strengths):
```
        # Route order
        if order.emulation_trigger != TriggerType.NO_TRIGGER:
            self._manager.send_emulator_command(command)
        elif order.exec_algorithm_id is not None:                  ← branch omitted from report
            self._manager.send_algo_command(command, order.exec_algorithm_id)
        else:
            self._manager.send_risk_command(command)
```

Citation verification — `common/factories.pyx:1259-1263`:
```
        emulation_trigger : TriggerType, default ``NO_TRIGGER``
            ...
            - ``NO_TRIGGER`` (default): Disables local emulation; orders are sent directly to the venue.
```

`support_contingent_orders` is only in the report, never set in m-cube (S-2):
```
$ grep -rn "support_contingent_orders" .
html_reports\Live_Env_Monitoring_sl_tp.html:147: ... (support_contingent_orders=True)
```

m-cube symbols referenced by the report all resolve (Strengths):
```
core/managed_strategy.py:152  def _classify_exit_mode(cfg: dict) -> str:
core/managed_strategy.py:166      if os.environ.get("_USE_NATIVE_BRACKET") != "1":
core/managed_strategy.py:212  def resolve_trigger_hl(
core/managed_strategy.py:525      self.current_sl = 0.0
core/managed_strategy.py:526      self.current_tp = 0.0
core/managed_strategy.py:1074 def _run_exit_phase(self, bar, bid_bar, ask_bar, ...):
```