"""Cross-check catalog aggregated bars against 1-MINUTE source via dual-path resample.

For each (instrument, side, target timeframe) tuple in the Nautilus catalog at
``catalog/data/bar/``, re-aggregate the 1-MINUTE bars via two independent paths:

  Path A: ``core.aggregator.aggregate_ohlcv`` (the production code).
  Path B: a local independent pandas resample with the same bucketing rule.

Then compare both against the stored aggregated parquet. Three pairwise checks
per tuple localize any mismatch to either the writer or the aggregator logic.

Emits:
  - Stdout summary table + per-tuple last-bar example.
  - Markdown report at ``reports/catalog_aggregation_verification.md``.
  - Exit 0 if all 42 tuples PASS three-way; 1 otherwise.
"""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Windows console defaults to cp1252; force UTF-8 so report glyphs (Σ, →) print.
try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

from core.aggregator import OHLCV_AGG, _rule_for, aggregate_ohlcv
from core.csv_loader import QUANTITY_MAX
from verify_final import DATA_DIR, load_bars

INSTRUMENTS = ("EURUSD", "GBPUSD", "USDJPY")
SIDES = ("ASK", "BID")
TIMEFRAMES = (
    "5-MINUTE",
    "15-MINUTE",
    "30-MINUTE",
    "1-HOUR",
    "2-HOUR",
    "1-DAY",
    "1-WEEK",
    "1-MONTH",
)

PRICE_TOL = 1e-8
VOLUME_TOL = 1e-6

REPORT_PATH = PROJECT_ROOT / "reports" / "catalog_aggregation_verification.md"


def bar_type(instr: str, tf: str, side: str) -> str:
    return f"{instr}.FOREX_MS-{tf}-{side}-EXTERNAL"


def manual_resample(df_1min: pd.DataFrame, timeframe: str) -> pd.DataFrame:
    """Path B: independent pandas resample. Mirrors aggregate_ohlcv math without calling it."""
    if df_1min.empty:
        return df_1min.iloc[0:0].copy()
    rule = _rule_for(timeframe, week_anchor="SUN")
    cols = ["open", "high", "low", "close", "volume"]
    out = (
        df_1min[cols]
        .resample(rule, closed="right", label="right")
        .agg(OHLCV_AGG)
        .dropna(subset=["open", "high", "low", "close"])
    )
    # Trailing partial drop: keep last bucket only if a 1-min bar lands exactly on T.
    if not out.empty:
        last_t = out.index[-1]
        if not (df_1min.index == last_t).any():
            out = out.iloc[:-1]
    out["volume"] = out["volume"].clip(upper=QUANTITY_MAX)
    return out


def compare_frames(
    expected: pd.DataFrame,
    actual: pd.DataFrame,
    price_tol: float = PRICE_TOL,
    volume_tol: float = VOLUME_TOL,
) -> dict:
    """Vectorized bar-by-bar comparison on union of timestamps."""
    if expected.empty and actual.empty:
        return {"n_match": 0, "n_mismatch": 0, "first_mismatches": [], "pass": True}

    union = expected.index.union(actual.index).sort_values()
    exp = expected.reindex(union)
    act = actual.reindex(union)

    price_cols = ["open", "high", "low", "close"]

    # Missing-side mask: NaN in either frame at a given ts after reindex.
    miss_exp = exp["open"].isna().to_numpy()
    miss_act = act["open"].isna().to_numpy()
    missing = miss_exp | miss_act

    # Numeric diffs (NaN where one side is missing — handle separately).
    diff_arrs = {
        c: np.abs(exp[c].to_numpy(dtype=np.float64) - act[c].to_numpy(dtype=np.float64))
        for c in price_cols
    }
    diff_vol = np.abs(
        exp["volume"].to_numpy(dtype=np.float64) - act["volume"].to_numpy(dtype=np.float64)
    )

    price_bad = np.zeros(len(union), dtype=bool)
    for c in price_cols:
        price_bad |= np.nan_to_num(diff_arrs[c], nan=np.inf) > price_tol
    vol_bad = np.nan_to_num(diff_vol, nan=np.inf) > volume_tol

    # A row is a mismatch if (a) missing in either side, or (b) any column over tolerance.
    mismatch_mask = missing | price_bad | vol_bad
    n_mismatch = int(mismatch_mask.sum())
    n_match = len(union) - n_mismatch

    # Collect up to 5 example mismatches for the report.
    first_mismatches: list[dict] = []
    if n_mismatch > 0:
        bad_idx = np.where(mismatch_mask)[0][:5]
        for i in bad_idx:
            ts = union[i]
            if miss_exp[i] or miss_act[i]:
                reason = "missing in expected" if miss_exp[i] else "missing in actual"
                first_mismatches.append({"ts": ts, "reason": reason})
                continue
            diffs = {}
            for c in price_cols:
                if diff_arrs[c][i] > price_tol:
                    diffs[c] = (float(exp[c].iat[i]), float(act[c].iat[i]), float(diff_arrs[c][i]))
            if diff_vol[i] > volume_tol:
                diffs["volume"] = (
                    float(exp["volume"].iat[i]),
                    float(act["volume"].iat[i]),
                    float(diff_vol[i]),
                )
            first_mismatches.append({"ts": ts, "diffs": diffs})

    return {
        "n_match": n_match,
        "n_mismatch": n_mismatch,
        "first_mismatches": first_mismatches,
        "pass": n_mismatch == 0,
    }


def build_last_bar_example(
    df_1min: pd.DataFrame, df_catalog: pd.DataFrame, timeframe: str
) -> dict:
    """Reconstruct the last catalog bar from its underlying 1-MIN window."""
    if df_catalog.empty or df_1min.empty:
        return {}
    agg_ts = df_catalog.index[-1]
    agg_bar = df_catalog.iloc[-1]

    # Find the (T-N, T] window for this bucket from the source by walking back
    # to the previous catalog timestamp (or one step before if only one bar).
    if len(df_catalog) >= 2:
        window_start = df_catalog.index[-2]
    else:
        window_start = df_1min.index[0] - pd.Timedelta(seconds=1)

    mask = (df_1min.index > window_start) & (df_1min.index <= agg_ts)
    window = df_1min[mask]
    if window.empty:
        return {"agg_ts": agg_ts, "n_1min": 0}

    exp_o = float(window.iloc[0]["open"])
    exp_h = float(window["high"].max())
    exp_l = float(window["low"].min())
    exp_c = float(window.iloc[-1]["close"])
    exp_v = float(min(window["volume"].sum(), QUANTITY_MAX))

    act_o = float(agg_bar["open"])
    act_h = float(agg_bar["high"])
    act_l = float(agg_bar["low"])
    act_c = float(agg_bar["close"])
    act_v = float(agg_bar["volume"])

    match = (
        abs(exp_o - act_o) <= PRICE_TOL
        and abs(exp_h - act_h) <= PRICE_TOL
        and abs(exp_l - act_l) <= PRICE_TOL
        and abs(exp_c - act_c) <= PRICE_TOL
        and abs(exp_v - act_v) <= VOLUME_TOL
    )

    return {
        "agg_ts": agg_ts,
        "window_start": window_start,
        "n_1min": len(window),
        "first_1min_ts": window.index[0],
        "last_1min_ts": window.index[-1],
        "expected": {"O": exp_o, "H": exp_h, "L": exp_l, "C": exp_c, "V": exp_v},
        "actual": {"O": act_o, "H": act_h, "L": act_l, "C": act_c, "V": act_v},
        "match": match,
    }


def _pf(ok: bool) -> str:
    return "PASS" if ok else "FAIL"


def summarize_tuple(
    instr: str, side: str, tf: str, df_1min: pd.DataFrame, df_cat: pd.DataFrame
) -> tuple[dict, dict]:
    """Return (summary_row, last_bar_example) for one (instr, side, tf)."""
    if df_1min.empty or df_cat.empty:
        return (
            {
                "instrument": instr,
                "side": side,
                "timeframe": tf,
                "src_1m_bars": len(df_1min),
                "agg_bars": len(df_cat),
                "reduction": float("nan"),
                "matched": 0,
                "mismatched": 0,
                "A_vs_cat": "SKIP",
                "B_vs_cat": "SKIP",
                "A_vs_B": "SKIP",
                "status": "SKIP",
                "first_mismatches": [],
            },
            {},
        )

    exp_A = aggregate_ohlcv(df_1min, tf, drop_partial=True)
    exp_B = manual_resample(df_1min, tf)
    cmp_A = compare_frames(exp_A, df_cat)
    cmp_B = compare_frames(exp_B, df_cat)
    cmp_AB = compare_frames(exp_A, exp_B)

    status_ok = cmp_A["pass"] and cmp_B["pass"] and cmp_AB["pass"]
    row = {
        "instrument": instr,
        "side": side,
        "timeframe": tf,
        "src_1m_bars": len(df_1min),
        "agg_bars": len(df_cat),
        "reduction": len(df_1min) / max(len(df_cat), 1),
        "matched": cmp_A["n_match"],
        "mismatched": cmp_A["n_mismatch"],
        "A_vs_cat": _pf(cmp_A["pass"]),
        "B_vs_cat": _pf(cmp_B["pass"]),
        "A_vs_B": _pf(cmp_AB["pass"]),
        "status": "PASS" if status_ok else "FAIL",
        "first_mismatches": cmp_A["first_mismatches"],
    }
    example = build_last_bar_example(df_1min, df_cat, tf)
    return row, example


def _git_head() -> str:
    try:
        out = subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            cwd=str(PROJECT_ROOT),
            stderr=subprocess.DEVNULL,
        )
        return out.decode().strip()
    except Exception:
        return "unknown"


def render_markdown(rows: list[dict], examples: dict) -> str:
    lines: list[str] = []
    lines.append("# Catalog Aggregation Verification")
    lines.append("")
    lines.append(f"- Run: {datetime.now(timezone.utc).isoformat(timespec='seconds')}")
    lines.append(f"- Git HEAD: {_git_head()}")
    lines.append(f"- Catalog: `catalog/data/bar/`")
    lines.append(f"- Price tolerance: {PRICE_TOL} | Volume tolerance: {VOLUME_TOL}")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(
        "| Instrument | Side | Target TF | 1-Min Bars | Agg Bars | Reduction | Matched | Mismatched | A vs Cat | B vs Cat | A vs B | Status |"
    )
    lines.append(
        "|---|---|---|---:|---:|---:|---:|---:|---|---|---|---|"
    )
    for r in rows:
        red = "n/a" if pd.isna(r["reduction"]) else f"{r['reduction']:.2f}x"
        lines.append(
            f"| {r['instrument']} | {r['side']} | {r['timeframe']} | "
            f"{r['src_1m_bars']:,} | {r['agg_bars']:,} | {red} | "
            f"{r['matched']:,} | {r['mismatched']:,} | "
            f"{r['A_vs_cat']} | {r['B_vs_cat']} | {r['A_vs_B']} | {r['status']} |"
        )

    # Aggregate-by-timeframe overview (one row per 1-Min -> TF transition,
    # summed across instruments and sides).
    lines.append("")
    lines.append("## 1-Minute → Each Timeframe (rollup across all instruments & sides)")
    lines.append("")
    lines.append(
        "| Conversion | Σ 1-Min Bars | Σ Agg Bars | Avg Reduction | Σ Matched | Σ Mismatched | Tuples PASS / Total |"
    )
    lines.append("|---|---:|---:|---:|---:|---:|---|")
    for tf in TIMEFRAMES:
        subset = [r for r in rows if r["timeframe"] == tf and r["status"] != "SKIP"]
        if not subset:
            continue
        sum_src = sum(r["src_1m_bars"] for r in subset)
        sum_agg = sum(r["agg_bars"] for r in subset)
        avg_red = (
            sum(r["reduction"] for r in subset) / len(subset) if subset else float("nan")
        )
        sum_match = sum(r["matched"] for r in subset)
        sum_mis = sum(r["mismatched"] for r in subset)
        n_pass = sum(1 for r in subset if r["status"] == "PASS")
        lines.append(
            f"| 1-Minute → {tf} | {sum_src:,} | {sum_agg:,} | {avg_red:.2f}x | "
            f"{sum_match:,} | {sum_mis:,} | {n_pass} / {len(subset)} |"
        )

    lines.append("")
    lines.append("## Per-Tuple Last-Bar Example (catalog vs reconstructed)")
    lines.append("")
    for r in rows:
        key = (r["instrument"], r["side"], r["timeframe"])
        ex = examples.get(key) or {}
        lines.append(f"### {r['instrument']} {r['side']} {r['timeframe']}")
        if not ex:
            lines.append("- (no data)")
            lines.append("")
            continue
        e, a = ex.get("expected"), ex.get("actual")
        if not e or not a:
            lines.append(f"- agg_ts: `{ex.get('agg_ts')}`, no underlying 1-min window")
            lines.append("")
            continue
        lines.append(f"- Aggregated bar timestamp: `{ex['agg_ts']}`")
        lines.append(
            f"- Underlying 1-MIN window: `({ex['window_start']}, {ex['agg_ts']}]` — "
            f"{ex['n_1min']} bars from `{ex['first_1min_ts']}` to `{ex['last_1min_ts']}`"
        )
        lines.append("")
        lines.append("| Field | Reconstructed from 1-MIN | Catalog Stored | Δ |")
        lines.append("|---|---:|---:|---:|")
        for field in ("O", "H", "L", "C", "V"):
            diff = abs(e[field] - a[field])
            fmt = "{:.10f}" if field != "V" else "{:.4f}"
            lines.append(
                f"| {field} | {fmt.format(e[field])} | {fmt.format(a[field])} | {diff:.2e} |"
            )
        lines.append(f"- Match: **{_pf(ex['match'])}**")
        lines.append("")

    failures = [r for r in rows if r["status"] == "FAIL"]
    lines.append("## Failures")
    lines.append("")
    if not failures:
        lines.append("_None — all tuples passed three-way agreement._")
    else:
        for r in failures:
            lines.append(
                f"### {r['instrument']} {r['side']} {r['timeframe']} — "
                f"A_vs_Cat={r['A_vs_cat']}, B_vs_Cat={r['B_vs_cat']}, A_vs_B={r['A_vs_B']}"
            )
            for m in r["first_mismatches"][:5]:
                if "reason" in m:
                    lines.append(f"- `{m['ts']}` — {m['reason']}")
                else:
                    diffs_str = ", ".join(
                        f"{k}: exp={v[0]:.10f} got={v[1]:.10f} Δ={v[2]:.2e}"
                        for k, v in m["diffs"].items()
                    )
                    lines.append(f"- `{m['ts']}` — {diffs_str}")
            lines.append("")

    return "\n".join(lines) + "\n"


def _print_stdout_summary(rows: list[dict]) -> None:
    print("=" * 120)
    print("CATALOG AGGREGATION VERIFICATION -- dual-path (A=core.aggregator, B=independent resample)")
    print("=" * 120)
    header = (
        f"{'Instrument':<10} {'Side':<5} {'TF':<10} "
        f"{'1-Min Bars':>12} {'Agg Bars':>10} {'Red.':>7} "
        f"{'Matched':>10} {'Mism.':>7} "
        f"{'A:Cat':<6} {'B:Cat':<6} {'A:B':<6} {'Status':<7}"
    )
    print(header)
    print("-" * len(header))
    for r in rows:
        red = "n/a   " if pd.isna(r["reduction"]) else f"{r['reduction']:6.2f}x"
        print(
            f"{r['instrument']:<10} {r['side']:<5} {r['timeframe']:<10} "
            f"{r['src_1m_bars']:>12,} {r['agg_bars']:>10,} {red:>7} "
            f"{r['matched']:>10,} {r['mismatched']:>7,} "
            f"{r['A_vs_cat']:<6} {r['B_vs_cat']:<6} {r['A_vs_B']:<6} {r['status']:<7}"
        )

    print()
    print("ROLLUP: 1-Minute -> each Target Timeframe (summed across instruments x sides)")
    print("-" * 100)
    print(
        f"{'Conversion':<26} {'Σ 1-Min':>14} {'Σ Agg':>12} {'Avg Red.':>10} "
        f"{'Σ Matched':>12} {'Σ Mismatched':>14} {'PASS/Total':>12}"
    )
    for tf in TIMEFRAMES:
        subset = [r for r in rows if r["timeframe"] == tf and r["status"] != "SKIP"]
        if not subset:
            continue
        sum_src = sum(r["src_1m_bars"] for r in subset)
        sum_agg = sum(r["agg_bars"] for r in subset)
        avg_red = sum(r["reduction"] for r in subset) / len(subset)
        sum_match = sum(r["matched"] for r in subset)
        sum_mis = sum(r["mismatched"] for r in subset)
        n_pass = sum(1 for r in subset if r["status"] == "PASS")
        print(
            f"{'1-Minute -> ' + tf:<26} {sum_src:>14,} {sum_agg:>12,} "
            f"{avg_red:>9.2f}x {sum_match:>12,} {sum_mis:>14,} "
            f"{n_pass:>5} / {len(subset):<5}"
        )

    n_pass = sum(1 for r in rows if r["status"] == "PASS")
    n_fail = sum(1 for r in rows if r["status"] == "FAIL")
    n_skip = sum(1 for r in rows if r["status"] == "SKIP")
    print()
    print(f"RESULT: {n_pass} PASS, {n_fail} FAIL, {n_skip} SKIP (total {len(rows)})")


def main() -> int:
    if not DATA_DIR.exists():
        print(f"ERROR: catalog data dir not found at {DATA_DIR}", file=sys.stderr)
        return 2

    rows: list[dict] = []
    examples: dict = {}

    for instr in INSTRUMENTS:
        for side in SIDES:
            src_type = bar_type(instr, "1-MINUTE", side)
            print(f"[load] {src_type}", file=sys.stderr, flush=True)
            df_1m = load_bars(src_type)
            if df_1m.empty:
                print(f"  ! no 1-MIN data for {instr} {side} — skipping all timeframes", file=sys.stderr)
                for tf in TIMEFRAMES:
                    rows.append(
                        {
                            "instrument": instr,
                            "side": side,
                            "timeframe": tf,
                            "src_1m_bars": 0,
                            "agg_bars": 0,
                            "reduction": float("nan"),
                            "matched": 0,
                            "mismatched": 0,
                            "A_vs_cat": "SKIP",
                            "B_vs_cat": "SKIP",
                            "A_vs_B": "SKIP",
                            "status": "SKIP",
                            "first_mismatches": [],
                        }
                    )
                continue

            for tf in TIMEFRAMES:
                dst_type = bar_type(instr, tf, side)
                df_cat = load_bars(dst_type)
                print(
                    f"  [verify] {instr} {side} {tf}: 1m={len(df_1m):,} cat={len(df_cat):,}",
                    file=sys.stderr,
                    flush=True,
                )
                row, ex = summarize_tuple(instr, side, tf, df_1m, df_cat)
                rows.append(row)
                examples[(instr, side, tf)] = ex

    _print_stdout_summary(rows)

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(render_markdown(rows, examples), encoding="utf-8")
    print(f"\nReport written to: {REPORT_PATH}")

    n_fail = sum(1 for r in rows if r["status"] == "FAIL")
    return 0 if n_fail == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
