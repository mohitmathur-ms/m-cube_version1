# `ntm3` — NautilusTrader docs sub-agent

`ntm3` is a **project-scoped Claude Code sub-agent** that answers NautilusTrader
questions for m-cube using a library of 28 concept PDFs as its knowledge base.
Defined in [`ntm3.md`](./ntm3.md); its document corpus lives in
[`ntm3_docs/`](./ntm3_docs/).

## What it's for

m-cube runs on `nautilus_trader==1.224.0`. When you're wiring a strategy, exit
logic, or backtest path and need the exact NautilusTrader API/behavior, ntm3
looks it up in the PDFs and cites the source — so the main session doesn't have
to carry all that doc context.

## How to invoke it

| Method | How | When |
|---|---|---|
| **Natural language** | Mention it in a normal prompt: *"ask ntm3 how OrderBook deltas work"* | Quick delegation; Claude decides to hand off based on the agent's `description`. |
| **`@`-mention (guaranteed)** | Type `@`, pick **`ntm3 (agent)`** from the typeahead | When you want to be sure the request runs as ntm3. |
| **Whole session** | `claude --agent ntm3` | Run an entire CLI session as ntm3 (doc-research session). |

It auto-delegates when a task matches its `description` (NautilusTrader APIs/
concepts, or debugging code in `core/`, `strategies/`, `server.py`).

## How to give it the 28 PDFs

This is the only setup step:

1. Drop all 28 `.pdf` files into [`ntm3_docs/`](./ntm3_docs/).
2. Edit [`ntm3_docs/INDEX.md`](./ntm3_docs/INDEX.md) so every row's filename
   matches a real PDF (one concept per file, lowercase `name.pdf`).

There's no attach/import/rebuild step — a sub-agent can't have files bound in its
YAML. ntm3 gains knowledge purely by (a) its system prompt pointing at
`ntm3_docs/` + `INDEX.md`, and (b) holding the `Read` tool, which reads PDFs
natively (long PDFs are paged in ≤20-page chunks).

## Frontmatter reference (`ntm3.md`)

| Field | Value | Why |
|---|---|---|
| `name` | `ntm3` | Unique id; how you `@`-mention or `--agent` it. |
| `description` | doc-expert blurb | Drives **automatic delegation** — Claude reads this to decide when to route to ntm3. |
| `tools` | `Read, Grep, Glob` | Read-only researcher. `Read` opens PDFs/files; `Glob` lists the corpus; `Grep` searches text/code. No `Edit`/`Bash`, so it can't change the repo. |
| `memory` | `project` | Persistent, project-scoped notebook for durable NautilusTrader↔m-cube findings across sessions. |
| `color` | `cyan` | UI tag in the agent picker. |
| *(model omitted)* | inherits session model | Runs on whatever the main session uses (currently Opus 4.7). |

To let it fall back to the live nautilustrader.io site, add `WebFetch` (and
optionally `WebSearch`) to the `tools` line.

## Note on this file

This README ends in `.README.md` and has **no agent frontmatter**, so the
recursive agent loader in `.claude/agents/` ignores it — it won't be registered
as a second agent.
