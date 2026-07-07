# `.github/` — CI & automation for m-cube

This folder holds everything GitHub runs **automatically on your behalf**: two
workflows and the Python scripts one of them uses. Nothing here ships in the
product — it's all build/test/reporting plumbing.

> **New to GitHub Actions but know Docker/Kubernetes?** Read the 60-second
> primer below; the rest of the doc assumes those mappings.

---

## GitHub Actions in container terms

| Actions concept | What it is | Closest container analogy |
|---|---|---|
| **Workflow** (a `.yml` in `workflows/`) | A spec saying *what to run* and *when* | A manifest (`Job`/`Deployment` spec) |
| **Trigger** (`on:`) | The events that start a run (a push, a manual click) | The controller watching events and creating a Job |
| **Runner** (`runs-on: ubuntu-latest`) | A clean throwaway VM created per run, destroyed after | An ephemeral pod/VM |
| **Job** (e.g. `build:`) | A unit that runs on one runner; its steps share a workspace | A pod (containers share a volume) |
| **Step** | One command, run in order; a failure stops the job | A layer / a command in a pod |
| **`uses:`** | Pull a prebuilt reusable action from the Marketplace | `image:` — pulling a published image |
| **`run:`** | Execute a raw shell command (bash on Linux runners) | `command:` / `RUN` |
| **`permissions:`** | The scopes of the auto-issued `GITHUB_TOKEN` | A scoped service-account token |
| **`secrets.*`** | Encrypted values injected as env vars | Mounted `Secret`s |

Key idea: every run starts on an **empty** machine. That's why the first step is
always "check out the repo" — like a fresh pod with no code until you clone it.

---

## Directory map

```
.github/
├── README.md                  ← this file
├── workflows/
│   ├── python-app.yml          CI: flake8 lint + pytest on every push
│   └── push-report.yml         Generates an AI HTML "change report" per push, commits it back
└── scripts/                    Helpers used only by push-report.yml
    ├── requirements.txt        Python deps for the scripts (just `anthropic`)
    ├── collect_meta.py         Gather commit metadata + a capped git diff  → meta.json
    ├── generate_report.py      meta.json → ask Claude → render self-contained HTML
    ├── depgraph.py             AST scan → first-party import/dependency graph
    ├── depgraph_svg.py         Render that graph as inline SVG + HTML tables
    └── _style.py               Shared GitHub-dark CSS palette for the HTML
```

---

## Workflow 1 — `python-app.yml` (Continuous Integration)

Standard "is the code healthy?" check: **lint + run tests** on a Linux runner.
This is essentially GitHub's stock *Python application* starter workflow.

### Triggers & permissions

| Setting | Value | Why |
|---|---|---|
| `on: push` | Every push to **any** branch | Catch breakage early |
| `paths-ignore` | `html_reports/**` | The other workflow commits there; ignoring it avoids burning a CI run on a report-only commit |
| `on: workflow_dispatch` | Manual "Run workflow" button | Re-run on demand from the Actions tab |
| `permissions` | `contents: read` | This job only reads the repo — least privilege, no write-back |

### Steps (job `build`, on `ubuntu-latest`)

1. **Checkout** — `actions/checkout@v4` clones the repo into the runner.
2. **Set up Python 3.12** — `actions/setup-python@v5` installs the interpreter and
   enables pip's **download cache**, keyed on `requirements.txt`. Unchanged deps
   restore from cache (like Docker layer caching) instead of re-downloading.
3. **Install dependencies** — upgrade `pip`, install `flake8` + `pytest`, then
   `pip install -r requirements.txt` *only if that file exists* (`if [ -f … ]`).
4. **Lint with flake8** — two passes:
   - **Pass 1** `--select=E9,F63,F7,F82` → real breakage only (syntax errors,
     undefined names). No `--exit-zero`, so **this pass can fail the build**.
   - **Pass 2** `--exit-zero --max-complexity=10 --max-line-length=127` →
     everything else, reported as **advisory warnings** (always exits 0).
5. **Test with pytest** — runs `pytest tests/`. Any failing test turns the run red.

### What makes this workflow fail
- A flake8 **pass-1** error (syntax / undefined name), or
- A failing test in `pytest tests/`.

### Packages / actions used
- Actions: `actions/checkout@v4`, `actions/setup-python@v5`.
- Tools: `flake8`, `pytest`, plus the project's own `requirements.txt`.

> ⚠️ **Good to know:** m-cube is Windows-first and needs Python ≥ 3.11 (see
> `CLAUDE.md` / `start.bat`), but this CI runs on **Ubuntu + Python 3.12**. A
> green check means "passes on Linux/3.12" — close to, but not identical to, your
> local Windows environment.

---

## Workflow 2 — `push-report.yml` (AI change-report generator)

On every push, this builds a colorful, self-contained **HTML report** explaining
what changed (a plain-English summary, a technical breakdown, a worked example, a
dependency graph, the changed-file list, and risks), then **commits that HTML
back into the repo** under:

```
html_reports/<github-username>/<IST-date>/<IST-time>/<short-sha>/index.html
                                                                 index.json   ← sidecar manifest
```

### Triggers, permissions & concurrency

| Setting | Value | Why |
|---|---|---|
| `on: push` + `paths-ignore: html_reports/**` | Every push, except report-only commits | **Loop prevention** — the workflow's own commit lands under `html_reports/`, which must not re-trigger it |
| `on: workflow_dispatch` | Manual runs | On-demand |
| `permissions` | `contents: write` | It **commits and pushes** the report back |
| `concurrency` | `group: push-report-${{ github.ref }}`, `cancel-in-progress: false` | Serialize runs **per branch** so two commit-backs can't race; different branches still run in parallel |

### Steps (job `report`, on `ubuntu-latest`)

1. **Checkout (full history)** — `actions/checkout@v4` with `fetch-depth: 0` so the
   scripts can `git diff` across the whole commit range. Default
   `persist-credentials: true` lets the job push later.
2. **Set up Python 3.12** — `actions/setup-python@v5`, pip cache keyed on
   `.github/scripts/requirements.txt`.
3. **Install report deps** — `pip install -r .github/scripts/requirements.txt`
   (installs `anthropic`).
4. **Set up Node 20** — `actions/setup-node@v4`.
5. **Install Claude Code CLI** — `npm install -g @anthropic-ai/claude-code`. Used
   only for the subscription-auth path; harmless if only the API key is set.
6. **Compute report identifiers** — builds the output dir from the **IST**
   (`Asia/Kolkata`) date & time + short SHA (the runner clock is UTC, so it
   converts; `:` is illegal in paths, so the time uses `HH-MM-SS`). Exposes
   `short_sha` and `report_dir` as step outputs.
7. **Collect commit metadata + diff** — runs `collect_meta.py` → `meta.json`.
8. **Generate report HTML** — runs `generate_report.py` → `…/index.html` (+ a
   `.json` manifest). Reads the AI secrets and `MODEL` (`claude-sonnet-4-6`).
9. **Commit report into the repo** — configures the `github-actions[bot]` identity,
   stages only the new report dir, commits with `[skip ci]` in the message
   (a second loop-prevention belt-and-braces), then **pushes with a retry-rebase
   loop** (up to 3 tries): fetch → rebase onto the latest branch tip → push.
   Because each run only *adds* files under a unique `<actor>/<date>/<time>/<sha>/`
   path, the rebase can't conflict.

### The AI-backend fallback chain (in `generate_report.py`)

The report is generated by trying backends **in order**, and it **never hard-fails**:

1. **`CLAUDE_CODE_OAUTH_TOKEN` set?** → shell out to the `claude` CLI (subscription auth).
2. else **`ANTHROPIC_API_KEY` set?** → use the `anthropic` Python SDK (forced tool-use JSON).
3. **neither / both fail?** → render a **metadata-only** report with an
   "AI explanation unavailable" banner, and still exit 0 so the workflow stays green.

### Packages / actions / secrets used
- Actions: `actions/checkout@v4`, `actions/setup-python@v5`, `actions/setup-node@v4`.
- Runtimes: Python 3.12, Node 20.
- Python deps: `anthropic==0.104.0` (see `scripts/requirements.txt`).
- npm deps: `@anthropic-ai/claude-code`.
- Secrets: `CLAUDE_CODE_OAUTH_TOKEN` (preferred), `ANTHROPIC_API_KEY` (fallback).
  Either, both, or neither may be configured.

---

## The `scripts/` generator pipeline

These run **inside** `push-report.yml` (steps 7–8). Data flows left to right:

```
 git history
     │
     ▼
collect_meta.py ───────────────▶ meta.json ──────────────▶ generate_report.py ───▶ index.html
 (commit msgs, diffstat,        (commit range +            │                        + index.json
  size-capped unified diff,      capped diff as JSON)       ├─ _call_claude()  → AI summary (CLI or SDK, with fallback)
  changed-file list)                                        ├─ depgraph.py     → first-party import graph
                                                            │   └─ depgraph_svg.py → inline SVG + tables
                                                            └─ _style.py       → shared GitHub-dark CSS
```

### `collect_meta.py`
Runs `git` itself (so the range/truncation logic is testable locally, no fragile
bash JSON escaping) and writes `meta.json`.
- Resolves the commit range: uses the push event's `before`/`after`; if `before`
  is empty, all-zeros, or a missing object, falls back to `after^`, or the **empty
  tree** for a brand-new branch.
- Caps the unified diff at `DIFF_CAP_BYTES` (**default 60 000**) and flags
  `diff_truncated`.
- **Env contract:** `AFTER_SHA`, `BEFORE_SHA`, `BRANCH`, `REPO`, `SERVER_URL`,
  `DIFF_CAP_BYTES`, `REPORT_META` (output path).
- Pure stdlib (`subprocess`, `json`, `os`, `pathlib`).

### `generate_report.py`
Reads `meta.json`, gets an AI summary, and renders one self-contained HTML page
(all CSS inline; no CDN/JS) plus a sidecar manifest JSON for a landing page.
- The AI is asked for a strict structure via a `submit_report` tool schema
  (`headline`, `plain_english`, `technical`, `example`, `changed_files`,
  `risks_or_followups`); responses are normalized so loose CLI prose and strict
  SDK tool-use both parse.
- Grounds the model with a one-paragraph m-cube domain context.
- **Env contract:** `REPORT_META` & `OUTPUT_PATH` (required), `MODEL`
  (default `claude-sonnet-4-6`), `CLAUDE_CODE_OAUTH_TOKEN`, `ANTHROPIC_API_KEY`,
  `CLI_TIMEOUT` (default 300s).
- Deps: `anthropic` SDK + stdlib (`subprocess`, `re`, `json`, `html`, `shutil`,
  `datetime`, `pathlib`).

### `depgraph.py`
Pure-stdlib (`ast`) import scanner. Walks all first-party `.py` files (skipping
`venv`, `.git`, `node_modules`, `html_reports`, `.github`, `catalog`, …), parses
their `import` / `from … import` statements, resolves each to an in-repo file, and
builds a directed graph where an edge **importer → imported** means *"importer
depends on imported."* It then slices that down to the push's changed files + their
**1-hop neighbours**. Runs even in metadata-only mode (no network/AI).

### `depgraph_svg.py`
Renders the sliced graph as a fixed **three-column** inline SVG —
*depended-on-by → changed files → depends-on* — plus dark adjacency tables. No JS,
no CDN, renders offline.

### `_style.py`
Shared GitHub-dark color palette + base CSS, mirroring the look of the repo's other
report generators so the output feels native.

---

## Packages & libraries — quick reference

| Category | Item | Used by |
|---|---|---|
| Marketplace action | `actions/checkout@v4` | both workflows |
| Marketplace action | `actions/setup-python@v5` | both workflows |
| Marketplace action | `actions/setup-node@v4` | push-report |
| Python tool | `flake8` | python-app (lint) |
| Python tool | `pytest` | python-app (tests) |
| Python lib | `anthropic==0.104.0` | push-report (`generate_report.py`) |
| npm package | `@anthropic-ai/claude-code` | push-report (CLI auth path) |
| Runtime | Python 3.12 | both workflows |
| Runtime | Node 20 | push-report |
| Stdlib | `ast`, `subprocess`, `json`, `re`, `html`, `pathlib`, `datetime`, `os`, `shutil` | the `scripts/` |
| Secret | `CLAUDE_CODE_OAUTH_TOKEN` | push-report (preferred AI auth) |
| Secret | `ANTHROPIC_API_KEY` | push-report (fallback AI auth) |

---

## Running / testing the scripts locally

The report scripts work outside CI, which is handy for tweaking the HTML.

```powershell
# 1. (optional) put your key in a local .env — generate_report.py auto-loads it.
#    Real env vars / CI secrets always win, so this is a no-op in Actions.
#    .env example:
#    ANTHROPIC_API_KEY=sk-ant-...

# 2. collect metadata for the last commit into meta.json
$env:REPORT_META = "meta.json"
python .github/scripts/collect_meta.py

# 3. render the HTML report from it
$env:OUTPUT_PATH = "report.html"
python .github/scripts/generate_report.py
# (with no key set, you'll get the graceful metadata-only report)

# Quick self-check of the dependency scanner (prints "<N> modules, <M> edges"):
python .github/scripts/depgraph.py .
```

> These two workflows are independent: `python-app.yml` (CI) tells you the code is
> healthy; `push-report.yml` documents *what changed*. Neither affects the running
> app.
