---
name: docs-library-populated
description: The ntm3 docs library is fully populated — 28 concept docs exist as both .pdf and .txt; use the .txt files as primary source
metadata:
  type: project
---

As of 2026-06-01, `.claude/agents/ntm3_docs/` is FULLY POPULATED. Each of the
28 concepts exists as BOTH `<Concept>.pdf` AND a `<Concept>.txt` plain-text
extraction. The `.txt` files are the primary working source: Grep-able and
Read-able with no page limit.

**Why:** Earlier (2026-05-25) the folder held only INDEX.md/README.md; that is
no longer true. The PDFs and txt extractions were added.

**How to apply:** Grep across `**/ntm3_docs/*.txt` to locate a term, then Read
the matching `.txt` around the hit. Filenames have quirks: the Strategies doc is
`Startegies.txt` (sic), DST is `DST_timezone.txt`, Logging is `Logger.txt`,
Live Trading is `Live Trading.txt` (space in name). Cite the concept doc +
section heading. Only fall back to package source if a topic isn't covered.

**EMPTY .txt files (0 bytes, confirmed 2026-06-08):** `Architecture.txt`,
`Actors.txt`, `Startegies.txt` (Strategies) are ALL empty — their PDFs exist but
text extraction is missing. Questions about the `Actor` API surface, `Strategy`
lifecycle/handlers, or system architecture CANNOT be answered from .txt; must
read the .pdf (Read tool — but PDF rendering may be unavailable, see
[[tooling-pdf-no-poppler]]) or fall back to package source under
`venv/Lib/site-packages/nautilus_trader/`. Cache.txt and Portfolio.txt are rich
and cover most cross-strategy/portfolio-query needs that Actors/Strategies would
otherwise document.
