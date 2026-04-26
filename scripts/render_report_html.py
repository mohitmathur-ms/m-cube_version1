"""Render scratch/profiles/REPORT.md as a standalone, self-contained HTML.

Usage:
    python scripts/render_report_html.py
    python scripts/render_report_html.py --in path/to/REPORT.md --out path/to/REPORT.html
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import markdown  # type: ignore


CSS = """
:root {
  --bg: #ffffff;
  --fg: #1f2328;
  --muted: #57606a;
  --border: #d0d7de;
  --code-bg: #f6f8fa;
  --accent: #0969da;
  --accent-strong: #0550ae;
  --table-head-bg: #f6f8fa;
  --table-stripe: #fafbfc;
}
@media (prefers-color-scheme: dark) {
  :root {
    --bg: #0d1117;
    --fg: #e6edf3;
    --muted: #9199a1;
    --border: #30363d;
    --code-bg: #161b22;
    --accent: #58a6ff;
    --accent-strong: #79c0ff;
    --table-head-bg: #161b22;
    --table-stripe: #111720;
  }
}
* { box-sizing: border-box; }
html, body { margin: 0; padding: 0; }
body {
  background: var(--bg);
  color: var(--fg);
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif,
               "Apple Color Emoji", "Segoe UI Emoji";
  font-size: 14px;
  line-height: 1.55;
}
.container {
  max-width: 1024px;
  margin: 0 auto;
  padding: 32px 40px 96px;
}
h1, h2, h3, h4 {
  margin-top: 2em;
  margin-bottom: 0.5em;
  font-weight: 600;
  line-height: 1.3;
}
h1 { font-size: 2em; border-bottom: 1px solid var(--border); padding-bottom: 0.3em; }
h2 { font-size: 1.5em; border-bottom: 1px solid var(--border); padding-bottom: 0.3em; }
h3 { font-size: 1.2em; }
h4 { font-size: 1.05em; }
p { margin: 0 0 1em; }
hr { border: none; border-top: 1px solid var(--border); margin: 2.5em 0; }
a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }

code {
  font-family: ui-monospace, SFMono-Regular, "SF Mono", Consolas, "Liberation Mono", Menlo, monospace;
  font-size: 0.88em;
  background: var(--code-bg);
  border: 1px solid var(--border);
  padding: 0.1em 0.4em;
  border-radius: 4px;
}
pre {
  background: var(--code-bg);
  border: 1px solid var(--border);
  padding: 12px 14px;
  border-radius: 6px;
  overflow-x: auto;
  font-size: 0.88em;
  line-height: 1.5;
}
pre code {
  background: transparent;
  border: none;
  padding: 0;
  font-size: 1em;
}

ul, ol { padding-left: 1.4em; margin: 0 0 1em; }
li { margin: 0.25em 0; }

blockquote {
  margin: 1em 0;
  padding: 0.25em 1em;
  border-left: 3px solid var(--border);
  color: var(--muted);
}

table {
  border-collapse: collapse;
  margin: 0.75em 0 1.2em;
  font-size: 0.92em;
  width: 100%;
  max-width: 100%;
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
}
th, td {
  border: 1px solid var(--border);
  padding: 6px 10px;
  text-align: left;
}
th {
  background: var(--table-head-bg);
  font-weight: 600;
  white-space: nowrap;
}
tbody tr:nth-child(even) { background: var(--table-stripe); }
td:has(strong), th:has(strong) { font-variant-numeric: tabular-nums; }

strong { font-weight: 600; }
em { font-style: italic; }

/* Collapsible TL;DR / hidden sections */
details {
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 0 14px;
  margin: 1em 0;
  background: var(--code-bg);
}
details > summary {
  cursor: pointer;
  font-weight: 600;
  padding: 10px 0;
}
details[open] > summary { border-bottom: 1px solid var(--border); margin-bottom: 10px; }

/* Metadata header */
.meta {
  color: var(--muted);
  font-size: 0.9em;
  margin-bottom: 2em;
}

/* Hover highlighting for headings — subtle link anchor */
.heading-link {
  color: var(--muted);
  margin-left: 6px;
  opacity: 0;
  text-decoration: none;
  font-weight: normal;
}
h1:hover .heading-link, h2:hover .heading-link, h3:hover .heading-link, h4:hover .heading-link {
  opacity: 1;
}
"""


PAGE_TEMPLATE = """<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{title}</title>
<style>
{css}
</style>
</head>
<body>
<div class="container">
<div class="meta">Generated from <code>{src_relpath}</code></div>
{body}
</div>
</body>
</html>
"""


def render(md_path: Path, html_path: Path) -> None:
    md_text = md_path.read_text(encoding="utf-8")
    body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "toc", "sane_lists"],
        output_format="html5",
    )
    # Pull the first H1 as the <title>
    title = "Profiling Report"
    for line in md_text.splitlines():
        if line.startswith("# "):
            title = line[2:].strip()
            break

    page = PAGE_TEMPLATE.format(
        title=title,
        css=CSS.strip(),
        body=body,
        src_relpath=md_path.name,
    )
    html_path.write_text(page, encoding="utf-8")
    print(f"Wrote {html_path}  ({len(page):,} bytes)")


def main():
    project = Path(__file__).resolve().parent.parent
    ap = argparse.ArgumentParser(description="Render REPORT.md as standalone HTML.")
    ap.add_argument("--in", dest="in_path",
                    default=str(project / "scratch" / "profiles" / "REPORT.md"))
    ap.add_argument("--out", dest="out_path",
                    default=str(project / "scratch" / "profiles" / "REPORT.html"))
    args = ap.parse_args()

    in_path = Path(args.in_path)
    out_path = Path(args.out_path)
    if not in_path.exists():
        print(f"Input not found: {in_path}", file=sys.stderr)
        sys.exit(1)
    render(in_path, out_path)


if __name__ == "__main__":
    main()
