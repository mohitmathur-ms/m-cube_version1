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
"""

# Diff-stat coloring fragments used by both pages.
ADD_COLOR = GREEN
DEL_COLOR = RED
