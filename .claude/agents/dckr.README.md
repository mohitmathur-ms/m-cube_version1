# `dckr` — Docker / containerization sub-agent

`dckr` is a **project-scoped Claude Code sub-agent** that answers Docker and
containerization questions for m-cube using a knowledge base of documents you
provide. Defined in [`dckr.md`](./dckr.md); its document corpus lives in
[`dckr_docs/`](./dckr_docs/).

## What it's for

m-cube is a Flask app on `nautilus_trader==1.224.0` with a process-pool backtest
worker model, a `ParquetDataCatalog` on disk, large host CSV data paths, and a
second adapter-admin Flask app. When you're packaging, deploying, or debugging
m-cube (or an m-cube-like NautilusTrader system) **inside a container**, `dckr`
looks the answer up in its docs and cites the source — so the main session
doesn't have to carry all that container context.

It is the containerization counterpart to [`ntm3`](./ntm3.md) (which answers
NautilusTrader API/concept questions). Use `ntm3` for "how does NautilusTrader
work"; use `dckr` for "how do I run this in a container".

## How to invoke it

| Method | How | When |
|---|---|---|
| **Natural language** | Mention it in a normal prompt: *"ask dckr how to mount the catalog volume"* | Quick delegation; Claude decides to hand off based on the agent's `description`. |
| **`@`-mention (guaranteed)** | Type `@`, pick **`dckr (agent)`** from the typeahead | When you want to be sure the request runs as dckr. |
| **Whole session** | `claude --agent dckr` | Run an entire CLI session as dckr (container-research session). |

It auto-delegates when a task matches its `description` (Dockerfiles, compose,
volumes, networking, env, resource limits, running m-cube/NautilusTrader in
containers).

## How to give it the knowledge base

This is the only setup step:

1. Drop your documents into [`dckr_docs/`](./dckr_docs/). They can be `.md`,
   `.txt`, or `.pdf` (one topic per file, lowercase `name.ext`). For PDFs,
   optionally add a same-stem `.txt` extraction so the agent can grep it.
2. Edit [`dckr_docs/INDEX.md`](./dckr_docs/INDEX.md) so every row's filename
   matches a real file you dropped in.

There's no attach/import/rebuild step — a sub-agent can't have files bound in
its YAML. `dckr` gains knowledge purely by (a) its system prompt pointing at
`dckr_docs/` + `INDEX.md`, and (b) holding the `Read`/`Grep`/`Glob` tools.

## Frontmatter reference (`dckr.md`)

| Field | Value | Why |
|---|---|---|
| `name` | `dckr` | Unique id; how you `@`-mention or `--agent` it. |
| `description` | container-expert blurb | Drives **automatic delegation** — Claude reads this to decide when to route to dckr. |
| `tools` | `Read, Grep, Glob` | Read-only researcher. No `Edit`/`Bash`, so it can't change the repo or run containers. |
| `memory` | `project` | Persistent, project-scoped notebook for durable container↔m-cube findings across sessions. |
| `color` | `blue` | UI tag in the agent picker. |
| *(model omitted)* | inherits session model | Runs on whatever the main session uses. |

To let it fetch live Docker / NautilusTrader docs from the web, add `WebFetch`
(and optionally `WebSearch`) to the `tools` line.

## Note on this file

This README ends in `.README.md` and has **no agent frontmatter**, so the
recursive agent loader in `.claude/agents/` ignores it — it won't be registered
as a second agent.
