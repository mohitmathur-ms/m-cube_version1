"""Comparative correctness harness for the two newest features:

  1. Custom streaming aggregation (core.aggregator.BarAggregator, wired through
     ManagedExitStrategy via ExitConfig/strategy_bar_types -> aggregate_to_bar_type).
  2. MIS / NRML product type (PortfolioConfig.product + mis_squareoff_time,
     resolved by core.models.effective_portfolio_squareoff).

Unlike a smoke test, every case here makes a *behavioural* assertion by running
the SAME portfolio twice with one variable changed and comparing the results:

  AGG1  base 1-MINUTE  vs  5-MINUTE  vs  1-HOUR aggregation
        -> coarser timeframe must yield monotonically fewer-or-equal signal
           trades (fewer EMA crossings), and 5-MINUTE must differ from base.
  AGG2  Format-A (bid/ask) aggregation to 15-MINUTE
        -> the paired-aggregator dispatch path runs, finite PnL, differs from base.
  MIS1  product=MIS (mis_squareoff 15:15 IST)  vs  product=NRML
        -> MIS forces a daily 'Squareoff' exit; NRML produces none.
  MIS2  MIS default squareoff vs an explicit portfolio squareoff_time
        -> explicit time wins (different squareoff clock), both still square off.

Writes html_reports/tests_html_report/_agg_mis_nrml_results.json for the report
generator. Uses the reserved _default user only — never mutates users.json.
"""
from __future__ import annotations
import collections, json, math, sys, time, traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CATALOG = str(ROOT / "catalog")
OUT = ROOT / "html_reports" / "tests_html_report" / "_agg_mis_nrml_results.json"

EUR = "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL"
# Composite carrier strings exactly as the Multileg UI emits them; config_from_exit
# strips the INTERNAL@ carrier to the plain EXTERNAL aggregation target.
C5 = "EURUSD.FOREX_MS-5-MINUTE-MID-INTERNAL@1-MINUTE-EXTERNAL"
C15 = "EURUSD.FOREX_MS-15-MINUTE-MID-INTERNAL@1-MINUTE-EXTERNAL"
C1H = "EURUSD.FOREX_MS-1-HOUR-MID-INTERNAL@1-MINUTE-EXTERNAL"

# Short, liquid window so a multi-day run stays fast but still spans many
# daily squareoff boundaries (for MIS) and many EMA crossings (for AGG).
START, END = "2021-01-04", "2021-01-29"


def ec(**kw):
    base = dict(
        exit_price_format="ohlcv", stop_loss_type="none", stop_loss_value=0.0,
        trailing_sl_step=0.0, trailing_sl_offset=0.0, sl_atr_period=0,
        sl_atr_multiplier=0.0, target_type="none", target_value=0.0,
        tgt_atr_period=0, tgt_atr_multiplier=0.0, target_lock_trigger=0.0,
        target_lock_minimum=0.0, tgt_trail_enabled=False,
        tgt_trail_when_profit_reach=0.0, tgt_trail_lock_min_profit=0.0,
        tgt_trail_every=0.0, tgt_trail_by=0.0, sl_wait_sec=0, sl_wait_bars=0,
        tgt_wait_sec=0, tgt_wait_bars=0, on_sl_action="close",
        on_target_action="close", max_re_executions=0, execute_target_leg_id="",
        reentry_price=0.0, max_re_entries=0, armed_at_start=True)
    base.update(kw)
    return base


def slot(strategy, bt, lots, exit_cfg, **kw):
    s = {"strategy_name": strategy, "bar_type_str": bt, "lots": lots,
         "strategy_params": kw.pop("params", {}), "exit_config": exit_cfg,
         "enabled": True}
    s.update(kw)
    return s


def pf(name, slots, **kw):
    base = dict(name=name, starting_capital=100000.0, start_date=START,
                end_date=END, allocation_mode="equal", slots=slots)
    base.update(kw)
    return base


def emap(f, s):
    return {"fast_ema_period": f, "slow_ema_period": s}


def _nan(x):
    try:
        return x is None or math.isnan(float(x)) or math.isinf(float(x))
    except (TypeError, ValueError):
        return True


def reasons(r):
    """Count exit-reason tag prefixes (e.g. 'Squareoff', 'Stop Loss')."""
    fr = r.get("fills_report")
    c = collections.Counter()
    if fr is not None and not getattr(fr, "empty", True) and "tags" in fr.columns:
        for t in fr["tags"]:
            if isinstance(t, (list, tuple)) and t:
                c[str(t[0]).split(":")[0]] += 1
    return dict(c)


def _summ(r):
    return dict(total_trades=r.get("total_trades"), total_pnl=r.get("total_pnl"),
                final_balance=r.get("final_balance"), win_rate=r.get("win_rate"),
                per_strategy_slots=len(r.get("per_strategy") or {}),
                exit_reasons=reasons(r))


def run():
    from core.models import portfolio_from_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    from core.users import reset_user_pnl

    def runpf(spec):
        reset_user_pnl()
        clear_cross_portfolio_bus()
        return run_portfolio_backtest(CATALOG, portfolio_from_dict(spec), user_id="_default")

    results = []

    def record(cid, title, settings, runs, verdict_fn):
        t0 = time.time()
        rec = dict(id=cid, title=title, settings=settings, range=f"{START} .. {END}")
        try:
            outs = {k: runpf(v) for k, v in runs.items()}
            rec["runs"] = {k: _summ(o) for k, o in outs.items()}
            ok, note = verdict_fn(outs)
            rec["verdict"] = "PASS" if ok else "FAIL"
            rec["note"] = note
            rec["status"] = "COMPLETED"
        except Exception as e:
            rec["status"] = "ERROR"
            rec["verdict"] = "FAIL"
            rec["error"] = f"{type(e).__name__}: {e}"
            rec["trace"] = traceback.format_exc()[-1500:]
            rec["note"] = "raised an unexpected exception"
        rec["seconds"] = round(time.time() - t0, 1)
        print(f"  {cid}: {rec['verdict']} ({rec['seconds']}s) — {rec.get('note','')}", flush=True)
        results.append(rec)
        OUT.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")

    # ── AGG1 — coarseness monotonicity ──────────────────────────────────────
    def agg1_pf(name, sbts):
        s = slot("EMA Cross", EUR, 0.05, ec(), params=emap(10, 30))
        if sbts:
            s["strategy_bar_types"] = sbts
        return pf(name, [s])

    def agg1_verdict(o):
        t_base = o["base_1m"].get("total_trades") or 0
        t_5m = o["agg_5m"].get("total_trades") or 0
        t_1h = o["agg_1h"].get("total_trades") or 0
        finite = all(not _nan(o[k].get("total_pnl")) for k in o)
        monotonic = t_base >= t_5m >= t_1h
        differs = t_5m != t_base
        fired = t_5m > 0
        ok = finite and monotonic and differs and fired
        return ok, (f"trades base1m={t_base} >= 5m={t_5m} >= 1h={t_1h} "
                    f"(monotonic={monotonic}, 5m!=base={differs}, 5m fired={fired}, "
                    f"all PnL finite={finite})")

    record("AGG1", "Streaming aggregation coarseness: 1-MIN vs 5-MIN vs 1-HOUR (EMA Cross 10/30, no SL/TP)",
           {"strategy": "EMA Cross 10/30", "instrument": "EURUSD MID",
            "base": "1-MINUTE", "aggregated": ["5-MINUTE", "1-HOUR"], "exit": "signal-only"},
           {"base_1m": agg1_pf("AGG1_base", None),
            "agg_5m": agg1_pf("AGG1_5m", [C5]),
            "agg_1h": agg1_pf("AGG1_1h", [C1H])},
           agg1_verdict)

    # ── AGG2 — Format-A (bid/ask) aggregation to 15-MIN ─────────────────────
    def agg2_pf(name, sbts):
        s = slot("EMA Cross", EUR, 0.04,
                 ec(exit_price_format="bidask", stop_loss_type="percentage",
                    stop_loss_value=0.20, target_type="percentage", target_value=0.30),
                 params=emap(9, 21))
        if sbts:
            s["strategy_bar_types"] = sbts
        return pf(name, [s], squareoff_time="22:00")

    def agg2_verdict(o):
        base, agg = o["base_1m"], o["agg_15m"]
        finite = not _nan(base.get("total_pnl")) and not _nan(agg.get("total_pnl"))
        ran = (agg.get("total_trades") or 0) >= 0 and len(agg.get("per_strategy") or {}) == 1
        differs = (agg.get("total_trades") or 0) != (base.get("total_trades") or 0)
        ok = finite and ran and differs
        return ok, (f"Format-A bid/ask aggregated to 15m ran (trades={agg.get('total_trades')}, "
                    f"base={base.get('total_trades')}, differs={differs}, PnL finite={finite})")

    record("AGG2", "Format-A bid/ask aggregation to 15-MINUTE (paired-aggregator dispatch path)",
           {"strategy": "EMA Cross 9/21", "instrument": "EURUSD MID -> auto BID/ASK pair",
            "base": "1-MINUTE", "aggregated": "15-MINUTE", "exit": "Format-A, SL 20% / TP 30%"},
           {"base_1m": agg2_pf("AGG2_base", None),
            "agg_15m": agg2_pf("AGG2_15m", [C15])},
           agg2_verdict)

    # ── MIS1 — MIS forces daily squareoff, NRML does not ────────────────────
    def mis_pf(name, product):
        s = slot("EMA Cross", EUR, 0.05,
                 ec(stop_loss_type="percentage", stop_loss_value=0.50,
                    target_type="percentage", target_value=0.80),
                 params=emap(10, 30))
        return pf(name, [s], product=product,
                  mis_squareoff_time="15:15", mis_squareoff_tz="Asia/Kolkata")

    def mis1_verdict(o):
        mis_sq = o["mis"]["__reasons"].get("Squareoff", 0)
        nrml_sq = o["nrml"]["__reasons"].get("Squareoff", 0)
        finite = not _nan(o["mis"].get("total_pnl")) and not _nan(o["nrml"].get("total_pnl"))
        ok = finite and mis_sq > 0 and nrml_sq == 0
        return ok, (f"MIS forced Squareoff exits={mis_sq} (>0 expected); "
                    f"NRML Squareoff exits={nrml_sq} (0 expected); PnL finite={finite}")

    # MIS1 needs the reason breakdown surfaced — wrap to attach it.
    def mis1_runs_verdict(o):
        for k in o:
            o[k]["__reasons"] = reasons(o[k])
        return mis1_verdict(o)

    record("MIS1", "MIS vs NRML: MIS supplies a daily forced squareoff (15:15 IST), NRML carries forward",
           {"strategy": "EMA Cross 10/30", "instrument": "EURUSD MID",
            "mis_squareoff_time": "15:15 Asia/Kolkata", "explicit squareoff_time": "none",
            "exit": "wide SL 50% / TP 80% so squareoff dominates"},
           {"mis": mis_pf("MIS1_mis", "MIS"),
            "nrml": mis_pf("MIS1_nrml", "NRML")},
           mis1_runs_verdict)

    # ── MIS2 — explicit portfolio squareoff_time overrides the MIS default ──
    def mis2_verdict(o):
        for k in o:
            o[k]["__reasons"] = reasons(o[k])
        mis_default_sq = o["mis_default"]["__reasons"].get("Squareoff", 0)
        explicit_sq = o["explicit"]["__reasons"].get("Squareoff", 0)
        finite = not _nan(o["mis_default"].get("total_pnl")) and not _nan(o["explicit"].get("total_pnl"))
        # Both square off; an earlier explicit clock (09:30 IST) cuts the session
        # shorter than the MIS 15:15 default, so the trade profiles must differ.
        both_square = mis_default_sq > 0 and explicit_sq > 0
        differ = o["mis_default"].get("total_trades") != o["explicit"].get("total_trades")
        ok = finite and both_square and differ
        return ok, (f"MIS-default Squareoff={mis_default_sq}, explicit-09:30 Squareoff={explicit_sq}; "
                    f"both square off={both_square}; trade profiles differ={differ}; finite={finite}")

    def mis2_pf(name, explicit_time):
        s = slot("EMA Cross", EUR, 0.05,
                 ec(stop_loss_type="percentage", stop_loss_value=0.50,
                    target_type="percentage", target_value=0.80),
                 params=emap(10, 30))
        kw = dict(product="MIS", mis_squareoff_time="15:15", mis_squareoff_tz="Asia/Kolkata")
        if explicit_time:
            kw["squareoff_time"] = explicit_time
            kw["squareoff_tz"] = "Asia/Kolkata"
        return pf(name, [s], **kw)

    record("MIS2", "Explicit portfolio squareoff_time (09:30 IST) overrides the MIS default (15:15 IST)",
           {"strategy": "EMA Cross 10/30", "instrument": "EURUSD MID",
            "mis_default": "15:15 Asia/Kolkata", "explicit": "09:30 Asia/Kolkata",
            "exit": "wide SL 50% / TP 80% so squareoff dominates"},
           {"mis_default": mis2_pf("MIS2_default", None),
            "explicit": mis2_pf("MIS2_explicit", "09:30")},
           mis2_verdict)

    OUT.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    npass = sum(1 for r in results if r["verdict"] == "PASS")
    print(f"\nAGG/MIS-NRML SUITE DONE — {npass}/{len(results)} PASS — results at {OUT}", flush=True)
    return results


if __name__ == "__main__":
    run()
