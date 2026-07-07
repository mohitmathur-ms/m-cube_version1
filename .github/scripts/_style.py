"""Shared GitHub-dark palette + base CSS for the push-report HTML pages.

Mirrors the colors used by the repo's existing report scripts
(tests/smoke_tests/*_report.py, core/report_generator.py) so the generated
Pages site feels native to the project. Self-contained — no CDN, no deps.
"""

from __future__ import annotations

# Core palette (GitHub-dark).
BG = "#0d1117"
SURFACE = "#161b22"
SURFACE_2 = "#1c2230"
BORDER = "#30363d"
TEXT = "#e6edf3"
MUTED = "#8b949e"
BLUE = "#58a6ff"
GREEN = "#3fb950"
ORANGE = "#f0883e"
RED = "#f85149"
PURPLE = "#bc8cff"

BASE_CSS = f"""
  :root {{
    --bg: {BG}; --surface: {SURFACE}; --surface2: {SURFACE_2};
    --border: {BORDER}; --text: {TEXT}; --muted: {MUTED};
    --blue: {BLUE}; --green: {GREEN}; --orange: {ORANGE};
    --red: {RED}; --purple: {PURPLE};
  }}
  * {{ box-sizing: border-box; }}
  body {{
    margin: 0; padding: 0; background: var(--bg); color: var(--text);
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
    line-height: 1.55; -webkit-font-smoothing: antialiased;
  }}
  a {{ color: var(--blue); text-decoration: none; }}
  a:hover {{ text-decoration: underline; }}
  .wrap {{ max-width: 980px; margin: 0 auto; padding: 28px 20px 64px; }}
  code, pre {{ font-family: "SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace; }}
  code {{ background: var(--surface2); padding: 1px 6px; border-radius: 6px; font-size: 0.88em; }}
  pre {{
    background: #010409; border: 1px solid var(--border); border-radius: 10px;
    padding: 14px 16px; overflow-x: auto; font-size: 0.82rem; line-height: 1.5;
  }}
  .chip {{
    display: inline-block; padding: 2px 10px; border-radius: 999px;
    font-size: 0.72rem; font-weight: 600; letter-spacing: .02em;
    border: 1px solid var(--border); background: var(--surface2); color: var(--muted);
  }}
  .chip.branch {{ color: var(--purple); border-color: #3b2d52; background: #1d1530; }}
  .chip.ai {{ color: var(--green); border-color: #1f3d28; background: #11261a; }}
  .chip.warn {{ color: var(--orange); border-color: #4a3416; background: #2a1e0e; }}

  /* ---- Guide-style component kit, ported to the GitHub-dark palette ---- */

  /* Gradient cover header (mirrors the aggregation guide, dark accents). */
  header.cover {{
    background: linear-gradient(135deg, #16304d 0%, #2a1d4a 100%);
    border: 1px solid var(--border); border-radius: 14px;
    padding: 26px 28px; margin-bottom: 22px;
  }}
  header.cover h1 {{ margin: 10px 0 8px; font-size: 1.6rem; line-height: 1.25; }}
  header.cover .subtitle {{ margin: 0; color: var(--text); opacity: 0.9; font-size: 0.9rem; }}

  /* Table of contents. */
  nav.toc {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 14px 20px; margin: 18px 0;
  }}
  nav.toc h3 {{ margin: 0 0 8px; font-size: 0.72rem; text-transform: uppercase;
                letter-spacing: .06em; color: var(--muted); }}
  nav.toc ol {{ margin: 0; padding-left: 20px; columns: 2; column-gap: 32px; }}
  nav.toc ol li {{ padding: 3px 0; break-inside: avoid; }}

  /* Callouts: a left-bordered tinted box per intent. */
  .simple, .callout, .callout-tech, .callout-ok, .callout-warn {{
    border-radius: 0 10px 10px 0; padding: 12px 16px; margin: 12px 0; font-size: 0.92rem;
    border-left: 4px solid var(--muted); background: var(--surface2);
  }}
  .simple {{ border-left-color: var(--muted); }}
  .simple::before {{ content: "Plain English: "; font-weight: 700; color: var(--muted);
                     font-size: 0.7rem; letter-spacing: .05em; text-transform: uppercase; }}
  .callout {{ border-left-color: var(--blue); background: rgba(88,166,255,0.08); }}
  .callout-tech {{ border-left-color: var(--purple); background: rgba(188,140,255,0.08); }}
  .callout-tech strong {{ color: var(--purple); }}
  .callout-ok {{ border-left-color: var(--green); background: rgba(63,185,80,0.08); }}
  .callout-warn {{ border-left-color: var(--orange); background: rgba(240,136,62,0.10); }}

  /* Badges. */
  .badge {{ display: inline-block; padding: 2px 8px; font-size: 0.72rem; font-weight: 600;
            border-radius: 5px; line-height: 1.4; white-space: nowrap; }}
  .badge-ok {{ background: rgba(63,185,80,0.15); color: var(--green); }}
  .badge-warn {{ background: rgba(240,136,62,0.15); color: var(--orange); }}
  .badge-info {{ background: rgba(88,166,255,0.15); color: var(--blue); }}
  .badge-purple {{ background: rgba(188,140,255,0.15); color: var(--purple); }}

  /* Filename label that sits above a code block / table. */
  .filebox {{
    background: #010409; color: var(--muted); border: 1px solid var(--border);
    border-bottom: none; padding: 7px 14px; border-radius: 8px 8px 0 0;
    font-family: "SFMono-Regular", Consolas, monospace; font-size: 0.74rem; margin-top: 12px;
  }}
  .filebox + pre, .filebox + table {{ margin-top: 0; border-radius: 0 0 8px 8px; border-top: none; }}

  /* General tables (dependency adjacency + worked examples). */
  table.dep, table.example {{
    width: 100%; border-collapse: collapse; font-size: 0.84rem;
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 8px; overflow: hidden; margin: 8px 0;
  }}
  table.dep th, table.dep td, table.example th, table.example td {{
    text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--border); vertical-align: top;
  }}
  table.dep th, table.example th {{
    background: var(--surface2); color: var(--muted); font-weight: 600;
    font-size: 0.7rem; text-transform: uppercase; letter-spacing: .04em;
  }}
  table.dep tr:last-child td, table.example tr:last-child td {{ border-bottom: none; }}

  /* Dependency-graph SVG container. */
  .depgraph {{
    background: var(--surface); border: 1px solid var(--border);
    border-radius: 12px; padding: 12px; margin: 8px 0 16px; overflow-x: auto;
  }}
  .depgraph svg text {{ paint-order: stroke; }}
"""

# Diff-stat coloring fragments used by both pages.
ADD_COLOR = GREEN
DEL_COLOR = RED
