"""Rebuild the Pages landing page (index.html) from all report manifests.

Scans REPORTS_DIR (default: out/reports) for the per-report sidecar manifests
written by generate_report.py (<sha>.json), groups them by branch (newest
first), and writes a colorful index.html linking every report. Run after the
new report is generated and after the existing gh-pages content has been merged
into the output dir, so the index reflects the full accumulated history.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from html import escape
from pathlib import Path

from _style import BASE_CSS

REPORTS_DIR = Path(os.environ.get("REPORTS_DIR", "out/reports"))

INDEX_CSS = BASE_CSS + """
  header.idx { border-bottom: 1px solid var(--border); padding-bottom: 18px; margin-bottom: 24px; }
  header.idx h1 { font-size: 1.7rem; margin: 0 0 4px; }
  header.idx p { color: var(--muted); margin: 0; font-size: 0.9rem; }
  .branch-group { margin: 26px 0; }
  .branch-group h2 { font-size: 1.05rem; margin: 0 0 12px; display: flex; align-items: center; gap: 10px; }
  .count { color: var(--muted); font-weight: 400; font-size: 0.85rem; }
  .card { display: block; border: 1px solid var(--border); border-radius: 12px;
          background: var(--surface); padding: 14px 16px; margin: 10px 0;
          transition: border-color .15s, transform .05s; }
  .card:hover { border-color: var(--blue); text-decoration: none; transform: translateY(-1px); }
  .card .head { font-size: 0.98rem; color: var(--text); font-weight: 600; margin-bottom: 5px; }
  .card .sub { color: var(--muted); font-size: 0.8rem; display: flex; flex-wrap: wrap; gap: 6px 12px; }
  .card .sub .sep { color: var(--border); }
  .empty { color: var(--muted); padding: 40px 0; text-align: center; }
  footer { margin-top: 36px; color: var(--muted); font-size: 0.78rem; text-align: center; }
"""


def _load_manifests() -> list[dict]:
    out = []
    if not REPORTS_DIR.exists():
        return out
    for jf in REPORTS_DIR.glob("*/*.json"):
        try:
            data = json.loads(jf.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
        # Href is relative to index.html (written at REPORTS_DIR.parent), so it
        # must include the reports dir name: e.g. "reports/<branch>/<sha>.html".
        branch_dir = jf.parent.name
        html_file = data.get("html_file", jf.with_suffix(".html").name)
        data["_href"] = f"{REPORTS_DIR.name}/{branch_dir}/{html_file}"
        data["_branch_dir"] = branch_dir
        out.append(data)
    return out


def _card(m: dict) -> str:
    headline = escape(m.get("headline", "(no title)"))
    short_sha = escape(m.get("short_sha", ""))
    author = escape(m.get("author", ""))
    when = escape((m.get("generated_utc", "") or "").replace("T", " "))
    ai_chip = "" if m.get("ai", True) else "<span class='chip warn'>no AI</span>"
    return (
        f"<a class='card' href='{escape(m['_href'])}'>"
        f"<div class='head'>{headline}</div>"
        f"<div class='sub'><code>{short_sha}</code> {ai_chip}"
        f"<span class='sep'>|</span><span>{author}</span>"
        f"<span class='sep'>|</span><span>{when}</span></div>"
        f"</a>"
    )


def render_index(manifests: list[dict]) -> str:
    generated = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    total = len(manifests)

    # Group by branch (use the manifest branch name, fall back to dir name).
    groups: dict[str, list[dict]] = {}
    for m in manifests:
        branch = m.get("branch") or m.get("_branch_dir", "unknown")
        groups.setdefault(branch, []).append(m)

    # Newest first within each group; branches ordered by their newest report.
    for items in groups.values():
        items.sort(key=lambda x: x.get("generated_utc", ""), reverse=True)
    ordered_branches = sorted(
        groups.keys(),
        key=lambda b: groups[b][0].get("generated_utc", ""),
        reverse=True,
    )

    if total == 0:
        body = "<div class='empty'>No reports yet. Push a commit to generate one.</div>"
    else:
        sections = []
        for branch in ordered_branches:
            items = groups[branch]
            cards = "".join(_card(m) for m in items)
            sections.append(
                "<div class='branch-group'>"
                f"<h2><span class='chip branch'>{escape(branch)}</span>"
                f"<span class='count'>{len(items)} report{'s' if len(items) != 1 else ''}</span></h2>"
                f"{cards}</div>"
            )
        body = "".join(sections)

    return f"""<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>m-cube — Change Reports</title>
<style>{INDEX_CSS}</style>
</head><body>
<div class="wrap">
  <header class="idx">
    <h1>m-cube &mdash; Change Reports</h1>
    <p>Auto-generated plain-English &amp; technical summaries, one per push. {total} report{'s' if total != 1 else ''} across {len(groups)} branch{'es' if len(groups) != 1 else ''}.</p>
  </header>
  {body}
  <footer>Index rebuilt {generated}</footer>
</div>
</body></html>
"""


def main() -> int:
    manifests = _load_manifests()
    out = REPORTS_DIR.parent / "index.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(render_index(manifests), encoding="utf-8")
    print(f"[render_index] wrote {out} ({len(manifests)} reports)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
