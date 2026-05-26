---
name: docs-library-empty
description: The ntm3 PDF knowledge base is unpopulated — only INDEX.md/README.md exist, so answers must come from package source
metadata:
  type: project
---

As of 2026-05-25, `.claude/agents/ntm3_docs/` contains ONLY `INDEX.md` and
`README.md` — the 28+ concept PDFs referenced by INDEX.md are NOT present.
INDEX.md is still the fill-in template ("Drop your 30 PDFs into this folder").

**Why:** The intended authoritative source (versioned NautilusTrader concept
PDFs) was never added to the repo.

**How to apply:** Until PDFs are added, fall back to the installed package
source under `venv/Lib/site-packages/nautilus_trader/` and explicitly flag the
answer as source-derived, not doc-derived. Cython source lives as `.pyx`/`.pxd`
(no `.pyi` stubs); the `.pyd` is compiled. Relevant for 1.224.0:
- `Strategy.on_bar` is inherited from `Actor` — defined in
  `common/actor.pyx` (`cpdef void on_bar(self, Bar bar)`), dispatched by
  `handle_bar` in the same file.
- `Bar` class is in `model/data.pyx` / `model/data.pxd`.
