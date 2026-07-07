---
name: tooling-pdf-no-poppler
description: PDFs in ntm3_docs are unreadable (no poppler) BUT plain-text .txt siblings now exist and are the primary source — Grep + Read them
metadata:
  type: reference
---

**UPDATE 2026-05-25 — RESOLVED via .txt siblings.** Every doc now has a plain-text
`.txt` sibling next to its PDF in `.claude/agents/ntm3_docs/` (e.g. `Orders.txt`,
`Execution.txt`, `Positions.txt`). These are fully readable and grep-able. This is
now the **primary reading workflow**:
1. Read `INDEX.md` to map concept -> doc name.
2. `Grep` the `.txt` files for the term (note actual filenames use spaces +
   TitleCase, e.g. `Order Book.txt`, `Live Trading.txt`, `Value Types.txt`;
   `Startegies.txt` is misspelled; `Logger.txt`/`MessageBus.txt`/`DST_timezone.txt`
   differ from the INDEX slugs).
3. `Read` the matching `.txt` with offset/limit. The .txt is wrapped PDF text:
   page-furniture lines (URLs, "5/25/26", "NautilusTrader Documentation", "N/23")
   are interleaved — ignore them. Some long lines show as `[Omitted long matching line]`
   in Grep; Read the line range to get full content.
Confirmed working on the MARKET-vs-LIMIT mean-reversion question.

**Legacy (still true):** the PDFs themselves cannot be opened by the Read tool —
poppler (`pdftoppm`) is not on PATH on this Windows box. So always use the `.txt`
sibling, never the `.pdf`. If a `.txt` is ever missing, fall back to package source
under `venv/Lib/site-packages/nautilus_trader/` and flag that it came from source.

**RE-TESTED 2026-05-25 (Order Book validation run).** A task explicitly asked me to
open `Order Book.pdf` via the Read tool's `pages=` range to confirm a "newly-installed
poppler/pdftoppm" path. It STILL fails with the exact error:
`pdftoppm failed: Command 'pdftoppm' not found or is in an unsafe location (current directory)`.
Tried both `pages=1-10` and `pages=1-5`, absolute path, confirmed filename via Glob —
all fail identically. PDF rendering is NOT available; the `.txt` siblings and package
source remain the only usable sources.
