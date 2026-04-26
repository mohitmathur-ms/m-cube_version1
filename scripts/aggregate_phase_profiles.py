"""Aggregate per-slot phase profiles into a scaling-curve table.

Walks scratch/profiles/phase_*.json and prints a Markdown table suitable
for inclusion in REPORT.md. Computes the ratio of each phase's time at a
given span to the same phase at 1y — anything >> (span_in_years) is a
super-linear scaling red flag.

Usage:
    python scripts/aggregate_phase_profiles.py
    python scripts/aggregate_phase_profiles.py --out scratch/profiles/SCALING.md
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path


SPAN_YEARS = {"30d": 30 / 365.25, "1y": 1.0, "5y": 5.0, "10y": 10.0}
SPAN_ORDER = ["30d", "1y", "5y", "10y"]
PHASE_ORDER = [
    "registry_load",
    "catalog_init",
    "instruments_scan",
    "bars_load",
    "engine_build",
    "strategy_build",
    "engine_run",
    "fx_resolver_build",
    "extract_results",
]


def _load_all(profiles_dir: Path) -> list[dict]:
    out = []
    for p in sorted(profiles_dir.glob("phase_*.json")):
        try:
            out.append(json.loads(p.read_text()))
        except Exception as e:
            print(f"skip {p}: {e}")
    return out


def _fmt_phase_table(rows: list[dict]) -> str:
    by_strat: dict[str, dict[str, dict]] = {}
    for r in rows:
        by_strat.setdefault(r["strategy"], {})[r["span"]] = r

    buf = []
    for strat, by_span in sorted(by_strat.items()):
        buf.append(f"\n## {strat.upper()} strategy\n")
        buf.append("Per-phase seconds, by span:\n")
        header = ["phase"] + [s for s in SPAN_ORDER if s in by_span]
        buf.append("| " + " | ".join(header) + " |")
        buf.append("|" + "|".join(["---"] * len(header)) + "|")
        for phase in PHASE_ORDER:
            row = [phase]
            for span in SPAN_ORDER:
                if span not in by_span:
                    continue
                secs = (by_span[span].get("phase_times") or {}).get(phase, 0.0)
                row.append(f"{secs:.3f}")
            buf.append("| " + " | ".join(row) + " |")
        # Totals row
        totals = ["**TOTAL**"]
        elapsed_row = ["elapsed_seconds"]
        for span in SPAN_ORDER:
            if span not in by_span:
                continue
            phase_sum = sum((by_span[span].get("phase_times") or {}).values())
            totals.append(f"**{phase_sum:.2f}**")
            elapsed_row.append(f"{by_span[span].get('elapsed_seconds', 0):.2f}")
        buf.append("| " + " | ".join(totals) + " |")
        buf.append("| " + " | ".join(elapsed_row) + " |")

        # Scaling table — each phase's ratio vs its 1y cost vs naive span-years
        if "1y" in by_span:
            buf.append(f"\nScaling check: ratio = `phase_time(span) / phase_time(1y)`. "
                       f"Linear baseline = `span_years / 1 year`. "
                       f"Anything much larger than the baseline is super-linear.\n")
            header2 = ["phase", "1y baseline (s)"] + [
                f"{s} ratio (baseline {SPAN_YEARS[s]:.2f})" for s in SPAN_ORDER if s in by_span and s != "1y"
            ]
            buf.append("| " + " | ".join(header2) + " |")
            buf.append("|" + "|".join(["---"] * len(header2)) + "|")
            for phase in PHASE_ORDER:
                base = (by_span["1y"].get("phase_times") or {}).get(phase, 0.0)
                # Skip phases with baseline < 10ms — ratios on sub-millisecond
                # values are noise, not signal.
                if base < 0.01:
                    continue
                row = [phase, f"{base:.3f}"]
                for span in SPAN_ORDER:
                    if span == "1y" or span not in by_span:
                        continue
                    secs = (by_span[span].get("phase_times") or {}).get(phase, 0.0)
                    ratio = secs / base if base else 0.0
                    ideal = SPAN_YEARS[span]
                    flag = ""
                    # Only flag on spans longer than baseline — for shorter
                    # spans the ratio is dominated by fixed setup overhead
                    # and naturally looks super-linear without being a bug.
                    if ideal > 1.0:
                        if ratio > ideal * 2:
                            flag = " [SUPER-LINEAR]"
                        elif ratio > ideal * 1.3:
                            flag = " [slow]"
                    row.append(f"{ratio:.2f}x{flag}")
                buf.append("| " + " | ".join(row) + " |")

        # RSS + cache
        buf.append(f"\nOther stats by span:\n")
        buf.append("| span | wall_s | worker_rss_mb | cache_misses | trades |")
        buf.append("|---|---|---|---|---|")
        for span in SPAN_ORDER:
            if span not in by_span:
                continue
            r = by_span[span]
            buf.append(f"| {span} | {r.get('elapsed_seconds', 0):.2f} | "
                       f"{r.get('worker_rss_mb', '-')} | "
                       f"{r.get('cache_misses', '-')} | "
                       f"{r.get('total_trades', 0)} |")
    return "\n".join(buf)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--profiles-dir", default="scratch/profiles")
    ap.add_argument("--out", default=None, help="Write markdown report to this path")
    args = ap.parse_args()

    profiles = Path(args.profiles_dir)
    rows = _load_all(profiles)
    if not rows:
        print(f"No phase_*.json under {profiles}")
        return
    md = "# Phase-timer scaling report\n" + _fmt_phase_table(rows) + "\n"
    if args.out:
        Path(args.out).write_text(md, encoding="utf-8")
        print(f"Wrote {args.out}")
    else:
        print(md)


if __name__ == "__main__":
    main()
