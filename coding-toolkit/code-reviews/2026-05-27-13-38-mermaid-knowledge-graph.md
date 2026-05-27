# Code Review Report

| Field | Value |
|-------|-------|
| Date | 2026-05-27 13:38 |
| Reviewer | Claude (automated) |
| Feature/Task | Mermaid knowledge graph in push-report (commit 3d3fff2) |
| Files Reviewed | 3 |
| Review Duration | ~8 min |

---

## Summary

| Severity | Count |
|----------|-------|
| Critical | 0 |
| Logic | 0 |
| Performance | 1 |
| Style | 3 |
| **Total** | **4** |

**Verdict: PASS**

---

## Findings

### Critical
> None found.

### Logic
> None found.

### Performance

#### [P-1] Mermaid loaded from CDN with a floating major-version tag
- **File:** `.github/scripts/generate_report.py:613`
- **Issue:** `<script src="https://cdn.jsdelivr.net/npm/mermaid@10/dist/mermaid.min.js">` pins only the major version, so every report resolves to whatever latest `10.x` jsDelivr serves at *view time*. A future patch/minor that changes rendering (or a CDN outage) silently degrades or breaks the graph in already-committed historical reports.
- **Impact:** Non-reproducible rendering of archived reports; external availability dependency. Low blast radius (internal reports, render-only), so not Logic/Critical.
- **Fix:** Pin an exact version (e.g. `mermaid@10.9.1`) so committed reports render identically forever. Optional: add SRI `integrity`/`crossorigin` attributes for tamper-resistance.

### Style

#### [S-1] `securityLevel:"loose"` is broader than needed
- **File:** `.github/scripts/generate_report.py:614`
- **Issue:** Mermaid is initialized with `securityLevel:"loose"`, which permits click bindings and HTML inside labels. The graph carries no click handlers and all label text is already neutralized by `_mlabel` (strips `< > & " | [ ] { }` backtick), so the looser mode buys nothing.
- **Fix:** Drop to the default `securityLevel:"strict"`. The `classDef`/styling used here still applies under strict mode, and it removes a defense-in-depth gap on AI-authored label content.

#### [S-2] Inconsistent edge-label quoting between the two builders
- **File:** `.github/scripts/mermaid_graph.py:164` (vs `:115`)
- **Issue:** `from_concept` emits quoted edge labels (`-->|"rel"|`) while `from_depgraph` emits an unquoted literal (`-->|depends on|`). Both parse today, but the unquoted form is fragile if the label ever stops being a fixed string.
- **Fix:** Quote the depgraph label too (`-->|"depends on"|`) for consistency with `from_concept`.

#### [S-3] Edge-introduced nodes always colored as "concept"
- **File:** `.github/scripts/mermaid_graph.py:110-111`
- **Issue:** In `from_concept`, a node first seen via an edge (`_tok(e.get("source",""))`) is created with the default `kind="concept"`, so if the model lists an edge endpoint that it omitted from `nodes`, that node renders purple regardless of its true kind.
- **Fix:** Minor — acceptable as graceful fallback. If precise coloring matters, skip edges whose endpoints are not in the declared `nodes` set, or require the model to declare all endpoints (schema already implies this).

---

## Strengths
- `mermaid_graph.py:51-64` — `_mlabel` is a solid sanitizer: it strips exactly the characters that break Mermaid's `["..."]`/`|"..."|` parsers *and* the HTML-significant `< > &`, so the un-escaped injection at `generate_report.py:493` (`<div class='mermaid'>{src}</div>`) is safe by construction.
- Graceful degradation is well thought through: concept graph → deterministic depgraph → graceful note (`generate_report.py:459-497`), and `_normalize_report` (`:242-256`) coerces a missing/malformed `concept_graph` into an empty `{nodes,edges}` so the renderer never guesses.
- Node/edge caps (`_MAX_NODES=16`, `_MAX_EDGES=22`) prevent hairball graphs and mirror the existing `conceptgraph_svg.py` caps — consistent with repo convention.
- All three callers of the renamed `_graph_section(report, graph)` signature were updated consistently (verified via grep); no stale call sites.
- Pure-stdlib, never-raises contract is honored — both builders return `""` on malformed input.

---

## Recommendations
- Add a small pytest for `mermaid_graph.py` (the `__main__` self-check is good but unenforced): assert `from_concept`/`from_depgraph` return `""` on empty/malformed input, respect the node/edge caps, and that `_mlabel` strips `<`/`>`/`&`. This locks in the never-raises + sanitization guarantees that the security argument above depends on.
- Consider documenting the exact pinned Mermaid version in `push-report.yml`'s header comment once [P-1] is addressed.

---

## Evidence

`mermaid_graph.py` self-check runs clean and emits valid Mermaid for both paths:

```
$ python .github/scripts/mermaid_graph.py
graph LR
  n0["ManagedExitStrategy"]
  n1["ExitConfig"]
  n2["on_bar"]
  n0 -->|"reads"| n1
  n0 -->|"runs"| n2
  class n0 kclass;
  class n1 kconfig;
  class n2 kfunc;
  classDef kclass fill:#3a2a12,stroke:#f0883e,color:#ffd9b3;
  ...
---
graph LR
  n0["core/backtest_runner/runner.py"]
  n1["core/managed_strategy.py"]
  n0 -->|depends on| n1
  class n0 kdep;
  class n1 kchanged;
  ...
```

Caller consistency check (renamed signature has no stale call sites):

```
$ grep -rn "_graph_section\|_mermaid_section" .github/scripts/
generate_report.py:459:def _mermaid_section(report: dict | None, graph: dict | None) -> str:
generate_report.py:505:def _graph_section(report: dict | None, graph: dict | None) -> str:
generate_report.py:507:    mermaid_html = _mermaid_section(report, graph)
generate_report.py:602:    graph_html = _graph_section(report, graph)
```

HTML head declares `<meta charset="utf-8">` (`generate_report.py:609`), so the `…` ellipsis emitted by `_mlabel` truncation renders correctly.
