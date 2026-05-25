# ntm3 knowledge base

This folder is the document corpus for the **`ntm3`** sub-agent
(`.claude/agents/ntm3.md`). It holds the **28 NautilusTrader concept PDFs** that
ntm3 reads to answer questions.

## How to add the PDFs

1. Copy your `.pdf` files directly into this folder
   (`.claude/agents/ntm3_docs/`).
2. Open [`INDEX.md`](./INDEX.md) and make each row's **PDF filename** match a
   real file in this folder. Add/remove rows so the index reflects exactly what's
   here.
3. That's it — ntm3 reads `INDEX.md` first, then opens the matching PDF on demand
   via its `Read` tool. There is **no rebuild or import step**; files are read
   live at query time.

## Notes

- Only `.pdf` files are the knowledge base. The `.md` files here (`README.md`,
  `INDEX.md`) are documentation/index helpers.
- The parent `.claude/agents/` directory is scanned **recursively** for agent
  definitions, but `.pdf` files are ignored and `.md` files without agent
  frontmatter (like these) are skipped — so nothing here is mistaken for a second
  agent.
- Keep one concept per PDF and use lowercase `name.pdf` filenames (see the naming
  convention in `INDEX.md`).
- Large PDFs are fine: the `Read` tool pages through them (≤20 pages per read),
  so ntm3 reads long docs in chunks.