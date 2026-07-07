"""Generate a single colorful HTML change-report for one pushed commit range.

Reads commit metadata + a size-capped diff from a JSON file (path in env
REPORT_META), asks the Claude API to produce a plain-English explanation and a
technical/engineering breakdown (forced JSON via tool-use), then renders a
self-contained HTML page to OUTPUT_PATH.

Two AI backends are supported, tried in order:
  1. CLAUDE_CODE_OAUTH_TOKEN -> shell out to the `claude` CLI (subscription auth).
  2. ANTHROPIC_API_KEY       -> the `anthropic` Python SDK (pay-per-call API).
If neither is present (or both fail), it degrades to a metadata-only report
with an "AI explanation unavailable" banner. It never hard-fails: any backend
error is logged and the script exits 0 so the workflow stays green.

Env contract (all set by the workflow):
  CLAUDE_CODE_OAUTH_TOKEN  Subscription OAuth token from `claude setup-token`.
  ANTHROPIC_API_KEY        API key (optional fallback).
  MODEL                    Optional model id. Default: claude-sonnet-4-6
  REPORT_META              Path to the JSON metadata file (required).
  OUTPUT_PATH              Path to write the report HTML (required).
"""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import sys
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from _style import BASE_CSS
import depgraph
import depgraph_svg

MODEL = os.environ.get("MODEL", "claude-sonnet-4-6")
MAX_TOKENS = 2000

# One-paragraph domain context distilled from CLAUDE.md so explanations are grounded.
DOMAIN_CONTEXT = (
    "The repository is 'm-cube', a backtesting and research platform for FX, "
    "crypto and other asset classes built on NautilusTrader. It is a single Flask "
    "web app: a Python backend plus a vanilla HTML/CSS/JS single-page frontend, used "
    "for loading historical data, running single-strategy and multi-slot portfolio "
    "backtests with exit management (stop-loss / take-profit / trailing), and viewing "
    "tearsheets. Backend layers: L0 data ingest (core/csv_loader, aggregator, "
    "nautilus_loader) -> ParquetDataCatalog; L1 pure entry strategies (strategies/); "
    "L2 ManagedExitStrategy (core/managed_strategy.py) the SL/TP/trailing engine; "
    "L3 portfolio orchestration (core/backtest_runner.py); server.py exposes the Flask "
    "REST API. There is no live execution layer."
)

SYSTEM_STATIC = (
    "You are a senior engineer writing release-style change notes for a software "
    "repository. You are given the metadata and unified diff of a single git push. "
    "Produce two clearly separated explanations of WHAT CHANGED IN THIS PUSH:\n"
    "  1. plain_english: for a non-technical reader (a manager or stakeholder). No "
    "jargon, no file paths, no code. Explain what the change does and why it matters, "
    "in 2-5 short paragraphs.\n"
    "  2. technical: for an engineer. Cover the implementation approach, which "
    "modules/layers are affected, data-flow or behavioral impact, and any notable "
    "design choices. You may reference files, functions and concepts.\n"
    "  3. example: ONE concrete worked illustration of the single most significant "
    "change in this push. Make it tangible: a small before/after, a representative "
    "code path or call, or a sample input -> output. Reference real symbols from the "
    "diff. Keep it short (a few lines / one short paragraph).\n"
    "Base everything ONLY on the provided diff and metadata; do not invent changes "
    "you cannot see. If the diff is truncated, say so and reason about what is visible. "
    "Always call the submit_report tool with your answer.\n\n"
    "Repository context:\n" + DOMAIN_CONTEXT
)

REPORT_TOOL = {
    "name": "submit_report",
    "description": "Submit the structured change report for this push.",
    "input_schema": {
        "type": "object",
        "properties": {
            "headline": {
                "type": "string",
                "description": "A concise one-line title summarizing the push.",
            },
            "plain_english": {
                "type": "string",
                "description": "Non-technical explanation, 2-5 short paragraphs. "
                "Separate paragraphs with blank lines.",
            },
            "technical": {
                "type": "string",
                "description": "Engineer-facing implementation breakdown. "
                "Separate paragraphs with blank lines.",
            },
            "example": {
                "type": "string",
                "description": "One concrete worked example illustrating the most "
                "significant change (before/after, a representative code path, or a "
                "sample input -> output). Short.",
            },
            "changed_files": {
                "type": "array",
                "description": "One entry per notable changed file.",
                "items": {
                    "type": "object",
                    "properties": {
                        "path": {"type": "string"},
                        "role": {
                            "type": "string",
                            "description": "One short line: what this file is / what changed in it.",
                        },
                    },
                    "required": ["path", "role"],
                },
            },
            "risks_or_followups": {
                "type": "array",
                "description": "Short bullet strings of risks, gaps or follow-ups. May be empty.",
                "items": {"type": "string"},
            },
        },
        "required": ["headline", "plain_english", "technical", "example", "changed_files", "risks_or_followups"],
    },
}


REQUIRED_FIELDS = REPORT_TOOL["input_schema"]["required"]
# Fields a response MUST have to count as a valid report. `example`, `changed_files`
# and `risks_or_followups` are requested but optional at parse time so a model that
# omits one doesn't sink an otherwise-good report into degraded mode.
CORE_FIELDS = ("headline", "plain_english", "technical")
CLI_TIMEOUT = int(os.environ.get("CLI_TIMEOUT", "300"))  # seconds


def _user_content(meta: dict) -> str:
    diff = meta.get("diff", "")
    if meta.get("diff_truncated"):
        diff += "\n\n[... diff truncated for length ...]"
    return (
        f"Branch: {meta.get('branch')}\n"
        f"Commit: {meta.get('sha')}\n"
        f"Author: {meta.get('author')}\n"
        f"Commit message(s):\n{meta.get('commit_messages', '')}\n\n"
        f"Diff stat:\n{meta.get('diffstat', '')}\n\n"
        f"Unified diff:\n{diff if diff.strip() else '(no textual diff available)'}"
    )


def _coerce_report(obj: object) -> dict | None:
    """Validate a parsed object has at least the core report fields."""
    if isinstance(obj, dict) and all(k in obj for k in CORE_FIELDS):
        return obj
    return None


def _split_path_role(line: str) -> dict:
    """Parse a 'path — role' / 'path - role' / 'path: role' string into a dict."""
    line = line.strip().lstrip("-*• ").strip()
    for sep in (" — ", " – ", " - ", ": "):
        if sep in line:
            path, role = line.split(sep, 1)
            return {"path": path.strip(), "role": role.strip()}
    return {"path": line, "role": ""}


def _normalize_report(report: dict) -> dict:
    """Coerce fields into the shapes render_html expects, regardless of whether
    the model returned strict types (SDK tool-use) or looser ones (CLI prose).

    `changed_files` -> list[{path, role}]; `risks_or_followups` -> list[str].
    """
    cf = report.get("changed_files")
    if isinstance(cf, str):
        report["changed_files"] = [
            _split_path_role(line) for line in cf.splitlines() if line.strip()
        ]
    elif isinstance(cf, list):
        norm = []
        for item in cf:
            if isinstance(item, dict):
                norm.append({"path": str(item.get("path", "")), "role": str(item.get("role", ""))})
            elif isinstance(item, str):
                norm.append(_split_path_role(item))
        report["changed_files"] = norm
    else:
        report["changed_files"] = []

    risks = report.get("risks_or_followups")
    if isinstance(risks, str):
        report["risks_or_followups"] = [
            r.strip().lstrip("-*• ").strip()
            for r in risks.splitlines() if r.strip()
        ]
    elif not isinstance(risks, list):
        report["risks_or_followups"] = []

    return report


def _extract_json(text: str) -> dict | None:
    """Pull the report JSON out of a free-form model response (handles ```json
    fences and surrounding prose) and validate it."""
    if not text:
        return None
    # Prefer a fenced ```json ... ``` block, else the largest {...} span.
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    candidates = [fence.group(1)] if fence else []
    brace = re.search(r"\{.*\}", text, re.DOTALL)
    if brace:
        candidates.append(brace.group(0))
    for cand in candidates:
        try:
            return _coerce_report(json.loads(cand))
        except json.JSONDecodeError:
            continue
    return None


def _generate_via_cli(meta: dict) -> tuple[dict | None, str | None]:
    """Subscription path: drive the `claude` CLI (auth via CLAUDE_CODE_OAUTH_TOKEN)."""
    exe = shutil.which("claude")
    if not exe:
        return None, "claude CLI not found on PATH."

    schema_hint = json.dumps(
        {k: REPORT_TOOL["input_schema"]["properties"][k].get("description", "")
         for k in REQUIRED_FIELDS},
        indent=2,
    )
    prompt = (
        _user_content(meta)
        + "\n\nRespond with ONLY a single JSON object (no prose, no code fences) "
        "matching exactly these keys:\n" + schema_hint
    )

    # The prompt embeds the (up to ~60 KB) diff, which can exceed the OS
    # command-line length limit, so pass it on stdin rather than as an argv.
    cmd = [
        exe, "-p",
        "--output-format", "json",
        "--model", MODEL,
        "--append-system-prompt", SYSTEM_STATIC,
    ]
    # On Windows npm installs `claude.cmd`; batch files must run via cmd.exe.
    if os.name == "nt" and exe.lower().endswith((".cmd", ".bat")):
        cmd = ["cmd", "/c", *cmd]
    try:
        proc = subprocess.run(
            cmd, input=prompt, capture_output=True, text=True,
            timeout=CLI_TIMEOUT, env={**os.environ},
        )
    except subprocess.TimeoutExpired:
        return None, f"claude CLI timed out after {CLI_TIMEOUT}s."
    except Exception as exc:  # noqa: BLE001
        return None, f"claude CLI invocation failed: {exc}"

    if proc.returncode != 0:
        return None, f"claude CLI exited {proc.returncode}: {proc.stderr.strip()[:300]}"

    # Envelope is {"result": "...", ...}; the report JSON lives inside .result.
    result_text = proc.stdout
    try:
        envelope = json.loads(proc.stdout)
        if isinstance(envelope, dict):
            result_text = envelope.get("result", "") or envelope.get("structured_output", "")
            if isinstance(result_text, dict):
                report = _coerce_report(result_text)
                if report:
                    return report, None
                result_text = json.dumps(result_text)
    except json.JSONDecodeError:
        pass  # not the envelope we expected; try to parse stdout directly

    report = _extract_json(result_text if isinstance(result_text, str) else proc.stdout)
    if report:
        return report, None
    return None, "Could not parse a report JSON from the claude CLI output."


def _generate_via_sdk(meta: dict) -> tuple[dict | None, str | None]:
    """API path: the anthropic SDK with forced tool-use JSON."""
    api_key = os.environ.get("ANTHROPIC_API_KEY", "").strip()
    if not api_key:
        return None, "ANTHROPIC_API_KEY is not set."

    try:
        import anthropic
    except ImportError as exc:  # pragma: no cover
        return None, f"anthropic SDK not importable: {exc}"

    try:
        client = anthropic.Anthropic(api_key=api_key)
        resp = client.messages.create(
            model=MODEL,
            max_tokens=MAX_TOKENS,
            system=[
                {
                    "type": "text",
                    "text": SYSTEM_STATIC,
                    "cache_control": {"type": "ephemeral"},
                }
            ],
            tools=[REPORT_TOOL],
            tool_choice={"type": "tool", "name": "submit_report"},
            messages=[{"role": "user", "content": _user_content(meta)}],
        )
    except Exception as exc:  # noqa: BLE001 - degrade gracefully on any API error
        return None, f"Claude API call failed: {exc}"

    for block in resp.content:
        if getattr(block, "type", None) == "tool_use" and block.name == "submit_report":
            return block.input, None
    return None, "Model did not return a submit_report tool call."


def _call_claude(meta: dict) -> tuple[dict | None, str | None]:
    """Dispatch to the best available backend: subscription CLI > API SDK > none.

    Returns (report_dict, None) on success or (None, error_message) otherwise.
    """
    has_token = bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "").strip())
    has_key = bool(os.environ.get("ANTHROPIC_API_KEY", "").strip())

    errors = []
    if has_token:
        report, err = _generate_via_cli(meta)
        if report:
            return _normalize_report(report), None
        errors.append(f"CLI: {err}")
    if has_key:
        report, err = _generate_via_sdk(meta)
        if report:
            return _normalize_report(report), None
        errors.append(f"SDK: {err}")

    if not errors:
        return None, "No AI backend configured (set CLAUDE_CODE_OAUTH_TOKEN or ANTHROPIC_API_KEY)."
    return None, " | ".join(errors)


# --------------------------------------------------------------------------- #
# HTML rendering
# --------------------------------------------------------------------------- #

def _paragraphs(text: str) -> str:
    """Turn blank-line-separated text into <p> blocks (escaped)."""
    blocks = [b.strip() for b in (text or "").split("\n\n") if b.strip()]
    if not blocks:
        return "<p class='muted'>(none)</p>"
    return "".join(f"<p>{escape(b)}</p>" for b in blocks)


def _changed_files_table(meta: dict, report: dict | None) -> str:
    roles: dict[str, str] = {}
    if report:
        for item in report.get("changed_files", []) or []:
            if isinstance(item, dict) and item.get("path"):
                roles[item["path"]] = item.get("role", "")
    paths = meta.get("changed_files") or list(roles.keys())
    if not paths:
        return "<p class='muted'>(no files reported)</p>"
    rows = []
    for p in paths:
        role = roles.get(p, "")
        rows.append(
            f"<tr><td><code>{escape(p)}</code></td>"
            f"<td>{escape(role) if role else '<span class=muted>&mdash;</span>'}</td></tr>"
        )
    return (
        "<table class='files'><thead><tr><th>File</th><th>What changed</th></tr></thead>"
        "<tbody>" + "".join(rows) + "</tbody></table>"
    )


def _risks_list(report: dict | None) -> str:
    items = (report or {}).get("risks_or_followups") or []
    items = [str(x).strip() for x in items if str(x).strip()]
    if not items:
        return "<p class='muted'>None flagged.</p>"
    return "<ul class='risks'>" + "".join(f"<li>{escape(x)}</li>" for x in items) + "</ul>"


def _build_graph(meta: dict) -> dict | None:
    """Deterministic dependency graph for the push (AST scan of files on disk).

    Never raises: any failure returns None so the report still renders.
    """
    try:
        return depgraph.graph_for_push(".", meta.get("changed_files") or [])
    except Exception as exc:  # noqa: BLE001
        print(f"[generate_report] dependency graph unavailable: {exc}", file=sys.stderr)
        return None


def _graph_section(graph: dict | None) -> str:
    """Render the dependency-graph SVG + adjacency tables, or a graceful note."""
    if graph is None:
        return "<p class='muted'>Dependency graph could not be computed for this push.</p>"
    svg = depgraph_svg.render_svg(graph)
    tables = depgraph_svg.render_tables(graph)
    legend = (
        "<p class='sub'>Arrows point <strong>importer &rarr; imported</strong> "
        "(&ldquo;depends on&rdquo;). Middle column = files changed in this push; "
        "left = files that import them; right = files they import.</p>"
    )
    return f"{legend}<div class='depgraph'>{svg}</div>{tables}"


# Most components now live in _style.py (the guide-style kit). Only the few
# report-page-specific bits remain here.
PAGE_CSS = BASE_CSS + """
  .meta-row { display: flex; flex-wrap: wrap; gap: 8px 14px; align-items: center;
              color: var(--muted); font-size: 0.85rem; }
  .meta-row .sep { color: var(--border); }
  .panel { border: 1px solid var(--border); border-radius: 14px; background: var(--surface);
           padding: 20px 22px; margin: 18px 0; scroll-margin-top: 16px; }
  .panel h2 { margin: 0 0 6px; font-size: 1.1rem; display: flex; align-items: center; gap: 10px; }
  .panel .sub { color: var(--muted); font-size: 0.8rem; margin: 0 0 14px; }
  .panel.plain { border-left: 4px solid var(--blue); }
  .panel.tech  { border-left: 4px solid var(--purple); }
  .panel.example-p { border-left: 4px solid var(--green); }
  .panel.graph-p { border-left: 4px solid var(--blue); }
  .panel.files-p { border-left: 4px solid var(--green); }
  .panel.risks-p { border-left: 4px solid var(--orange); }
  .dot { width: 11px; height: 11px; border-radius: 50%; display: inline-block; }
  .dot.blue { background: var(--blue); } .dot.purple { background: var(--purple); }
  .dot.green { background: var(--green); } .dot.orange { background: var(--orange); }
  .muted { color: var(--muted); }
  table.files { width: 100%; border-collapse: collapse; font-size: 0.86rem; }
  table.files th, table.files td { text-align: left; padding: 7px 10px; border-bottom: 1px solid var(--border); vertical-align: top; }
  table.files th { color: var(--muted); font-weight: 600; }
  ul.risks { margin: 0; padding-left: 20px; } ul.risks li { margin: 4px 0; }
  .banner { border: 1px solid #4a3416; background: #2a1e0e; color: var(--orange);
            border-radius: 12px; padding: 12px 16px; margin: 14px 0; font-size: 0.88rem; }
  footer { margin-top: 34px; color: var(--muted); font-size: 0.78rem; text-align: center; }
"""


def render_html(meta: dict, report: dict | None, error: str | None,
                graph: dict | None = None) -> str:
    short_sha = meta.get("short_sha", "")
    sha = meta.get("sha", "")
    branch = meta.get("branch", "")
    author = meta.get("author", "")
    commit_url = meta.get("commit_url", "")
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    headline = (report or {}).get("headline") or (
        meta.get("commit_messages", "").splitlines() or ["Change report"]
    )[0]

    sha_link = (
        f"<a href='{escape(commit_url)}'><code>{escape(short_sha)}</code></a>"
        if commit_url
        else f"<code>{escape(short_sha)}</code>"
    )

    banner = ""
    ai_chip = "<span class='chip ai'>AI summary</span>"
    if report is None:
        ai_chip = "<span class='chip warn'>AI summary unavailable</span>"
        banner = (
            "<div class='banner'><strong>AI explanation unavailable.</strong> "
            f"{escape(error or 'Unknown error.')} Showing commit metadata and diff only.</div>"
        )

    plain_html = _paragraphs((report or {}).get("plain_english", "")) if report else "<p class='muted'>Not generated.</p>"
    tech_html = _paragraphs((report or {}).get("technical", "")) if report else "<p class='muted'>Not generated.</p>"
    example_html = (
        _paragraphs((report or {}).get("example", ""))
        if report and (report or {}).get("example")
        else "<p class='muted'>No worked example generated for this push.</p>"
    )
    graph_html = _graph_section(graph)

    diffstat = escape(meta.get("diffstat", "").strip() or "(no diff stat)")
    diff_note = " <span class='chip warn'>truncated</span>" if meta.get("diff_truncated") else ""

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{escape(headline)} — {escape(short_sha)}</title>
<style>{PAGE_CSS}</style>
</head><body>
<div class="wrap">
  <header class="cover">
    <div class="meta-row">
      <span class="chip branch">{escape(branch)}</span>
      {ai_chip}
      <span class="sep">|</span>
      <span>{sha_link}</span>
      <span class="sep">|</span>
      <span>{escape(author)}</span>
    </div>
    <h1>{escape(headline)}</h1>
    <p class="subtitle">Push change report &middot; generated {generated}</p>
  </header>

  {banner}

  <nav class="toc">
    <h3>Contents</h3>
    <ol>
      <li><a href="#sec-plain">Plain English</a></li>
      <li><a href="#sec-tech">Technical / Engineering</a></li>
      <li><a href="#sec-example">Worked Example</a></li>
      <li><a href="#sec-graph">Dependency / Knowledge Graph</a></li>
      <li><a href="#sec-files">Changed Files</a></li>
      <li><a href="#sec-risks">Risks &amp; Follow-ups</a></li>
      <li><a href="#sec-diff">Diff stat</a></li>
    </ol>
  </nav>

  <section id="sec-plain" class="panel plain">
    <h2><span class="dot blue"></span> Plain English</h2>
    <p class="sub">What this change does, for a non-technical reader.</p>
    <div class="simple">{plain_html}</div>
  </section>

  <section id="sec-tech" class="panel tech">
    <h2><span class="dot purple"></span> Technical / Engineering</h2>
    <p class="sub">Implementation detail and impact, for engineers.</p>
    <div class="callout-tech">{tech_html}</div>
  </section>

  <section id="sec-example" class="panel example-p">
    <h2><span class="dot green"></span> Worked Example</h2>
    <p class="sub">A concrete illustration of the most significant change.</p>
    <div class="callout-ok">{example_html}</div>
  </section>

  <section id="sec-graph" class="panel graph-p">
    <h2><span class="dot blue"></span> Dependency / Knowledge Graph</h2>
    <p class="sub">Which files depend on the files changed in this push (1-hop, first-party imports).</p>
    {graph_html}
  </section>

  <section id="sec-files" class="panel files-p">
    <h2><span class="dot green"></span> Changed Files</h2>
    {_changed_files_table(meta, report)}
  </section>

  <section id="sec-risks" class="panel risks-p">
    <h2><span class="dot orange"></span> Risks &amp; Follow-ups</h2>
    {_risks_list(report)}
  </section>

  <section id="sec-diff" class="panel">
    <h2>Diff stat{diff_note}</h2>
    <pre>{diffstat}</pre>
  </section>

  <footer>
    Commit {escape(sha)} on <strong>{escape(branch)}</strong> &middot; m-cube change report &middot; model: {escape(MODEL)}
  </footer>
</div>
</body></html>
"""


def _load_dotenv() -> None:
    """Load a local .env into os.environ for LOCAL runs (no python-dotenv dep).

    Real environment variables / CI secrets always win (uses setdefault), so this
    is a no-op in GitHub Actions — there is no .env on the runner anyway.
    """
    dotenv = Path(os.environ.get("DOTENV_PATH", ".env"))
    if not dotenv.is_file():
        return
    for raw in dotenv.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[len("export "):]
        key, val = line.split("=", 1)
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key:
            os.environ.setdefault(key, val)
    print("[generate_report] loaded .env (local)")


def main() -> int:
    _load_dotenv()
    meta_path = os.environ.get("REPORT_META")
    out_path = os.environ.get("OUTPUT_PATH")
    if not meta_path or not out_path:
        print("REPORT_META and OUTPUT_PATH must be set", file=sys.stderr)
        return 1

    meta = json.loads(Path(meta_path).read_text(encoding="utf-8"))

    report, error = _call_claude(meta)
    if error:
        print(f"[generate_report] degraded mode: {error}", file=sys.stderr)

    graph = _build_graph(meta)
    html = render_html(meta, report, error, graph)
    out = Path(out_path)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"[generate_report] wrote {out}")

    # Sidecar manifest consumed by render_index.py to build the landing page.
    headline = (report or {}).get("headline") or (
        meta.get("commit_messages", "").splitlines() or ["Change report"]
    )[0]
    manifest = {
        "headline": headline,
        "branch": meta.get("branch", ""),
        "sha": meta.get("sha", ""),
        "short_sha": meta.get("short_sha", ""),
        "author": meta.get("author", ""),
        "commit_url": meta.get("commit_url", ""),
        "html_file": out.name,
        "generated_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "ai": report is not None,
    }
    out.with_suffix(".json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"[generate_report] wrote {out.with_suffix('.json')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
