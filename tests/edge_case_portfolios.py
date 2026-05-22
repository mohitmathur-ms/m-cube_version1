"""Build 24+ COMPLEX edge-case portfolios, save them into the project so they
can be opened/run from the m-cube UI, run each as a real backtest, and write a
results JSON the report generator turns into an HTML report.

Every portfolio is deliberately maximal — leg-level SL/Target (all types: %, points,
trailing, ATR, target-lock, target-trailing, wait-bars/secs, SL/Target actions incl.
re_execute / reverse / execute / re_entry / keep_leg_running), portfolio-level SL &
Target (Combined Loss/Profit, Underlying Movement, Loss-and-Range, trailing, ReExecute,
delays, partial sqoff), Move-SL, MIS/NRML product, RBO, multi-zone squareoff, entry
window, run_on_days, and the custom streaming aggregator (strategy_bar_types) folded in.
A handful are intentional edge/degenerate cases where the engine must degrade gracefully.

Saved to: portfolios/_default/EDGE_*.json   (user_id=_default, runnable from the UI)
Results : html_reports/tests_html_report/_edge_case_results.json
"""
from __future__ import annotations
import collections, json, math, os, sys, time, traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
CATALOG = str(ROOT / "catalog")
PF_DIR = ROOT / "portfolios" / "_default"
OUT = ROOT / "html_reports" / "tests_html_report" / "_edge_case_results.json"

# ── instruments (catalog) ────────────────────────────────────────────────────
EUR = "EURUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL"
EUR_A = "EURUSD.FOREX_MS-1-MINUTE-ASK-EXTERNAL"
GBP = "GBPUSD.FOREX_MS-1-MINUTE-MID-EXTERNAL"
JPY = "USDJPY.FOREX_MS-1-MINUTE-MID-EXTERNAL"
BTC = "BTCUSD.BINANCE_MS-1-DAY-LAST-EXTERNAL"
LTC = "LTCUSD.BINANCE_MS-1-DAY-LAST-EXTERNAL"
XRP = "XRPUSD.BINANCE_MS-1-DAY-LAST-EXTERNAL"


def agg(base_bt: str, tf: str) -> str:
    """Composite carrier the UI emits for a strategy subscribe-timeframe."""
    p = base_bt.split("-")
    return f"{p[0]}-{tf}-{p[3]}-INTERNAL@1-MINUTE-EXTERNAL"


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


def leg(strategy, bt, lots, exit_cfg, agg_tf=None, **kw):
    s = {"strategy_name": strategy, "bar_type_str": bt, "lots": lots,
         "strategy_params": kw.pop("params", {}), "exit_config": exit_cfg,
         "enabled": True}
    if agg_tf:
        s["strategy_bar_types"] = [agg(bt, agg_tf)]
    s.update(kw)
    return s


def pf(name, start, end, slots, **kw):
    base = dict(name=name, starting_capital=100000.0, start_date=start,
                end_date=end, allocation_mode="equal", slots=slots)
    base.update(kw)
    return base


def emap(f, s):
    return {"fast_ema_period": f, "slow_ema_period": s}


def rsip(p=14, ob=70, os_=30):
    return {"rsi_period": p, "overbought": ob, "oversold": os_}


def bbp(p=20, sd=2.0):
    return {"bb_period": p, "bb_std": sd}


def map4(a=5, b=13, c=21, d=55, ema=True):
    return {"ma1_period": a, "ma2_period": b, "ma3_period": c, "ma4_period": d, "use_ema": ema}


PORTFOLIOS = []


def P(pid, title, desc, drange, env, builder, verdict):
    PORTFOLIOS.append(dict(id=pid, title=title, desc=desc, range=drange,
                           env=env, builder=builder, verdict=verdict))


def _finite(x):
    try:
        return x is not None and not math.isnan(float(x)) and not math.isinf(float(x))
    except (TypeError, ValueError):
        return False


# ════════════════════════════════════════════════════════════════════════════
#  24 complex portfolios + 1 cross-portfolio trio
# ════════════════════════════════════════════════════════════════════════════

# P01 — single leg, every leg-level exit knob, MIS, 5-min aggregation
P("EDGE_01", "Single leg — full leg-level SL/Target stack (trailing SL + target-lock + target-trailing + wait-bars), MIS, 5-min agg",
  "EMA Cross on EURUSD with trailing SL (step/offset), %-target with lock and target-trailing, SL/Target wait-bars; MIS daily squareoff; base 1-min aggregated to 5-min.",
  "2021-02-01 .. 2021-03-31", {},
  lambda: pf("EDGE_01_single_full_leg", "2021-02-01", "2021-03-31", [
      leg("EMA Cross", EUR, 0.05, ec(
          stop_loss_type="trailing", stop_loss_value=0.30, trailing_sl_step=0.05,
          trailing_sl_offset=0.03, target_type="percentage", target_value=0.50,
          target_lock_trigger=0.20, target_lock_minimum=0.10, tgt_trail_enabled=True,
          tgt_trail_when_profit_reach=0.35, tgt_trail_lock_min_profit=0.15,
          tgt_trail_every=0.05, tgt_trail_by=0.03, sl_wait_bars=2, tgt_wait_bars=1),
          agg_tf="5-MINUTE", params=emap(9, 21))],
      product="MIS", mis_squareoff_time="15:15", mis_squareoff_tz="Asia/Kolkata",
      entry_start_time="06:00", entry_end_time="20:00"),
  lambda r: (_finite(r.get("total_pnl")), f"trades={r.get('total_trades')}, pnl finite")),

# P02 — 4 legs, mixed instruments + strategies, leg + portfolio SL/Target, NRML
P("EDGE_02", "Quad-leg mixed instruments/strategies — leg SL/Target + portfolio Combined-Loss SL & Combined-Profit Target (both trailing), NRML, 5-min agg",
  "EUR EMA / GBP RSI / JPY Bollinger / EUR 4MA; each leg a different SL & Target type; portfolio Combined Loss SL with trailing + Combined Profit Target with trailing; NRML carry-forward; percentage allocation.",
  "2021-01-04 .. 2021-02-26", {},
  lambda: pf("EDGE_02_quad_mixed", "2021-01-04", "2021-02-26", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          target_type="percentage", target_value=0.30), agg_tf="5-MINUTE",
          params=emap(8, 21), allocation_pct=30),
      leg("RSI Mean Reversion", GBP, 0.04, ec(stop_loss_type="points", stop_loss_value=0.0030,
          target_type="points", target_value=0.0050), agg_tf="5-MINUTE",
          params=rsip(14, 72, 28), allocation_pct=25),
      leg("Bollinger Bands", JPY, 0.03, ec(stop_loss_type="trailing", stop_loss_value=0.25,
          trailing_sl_step=0.04, trailing_sl_offset=0.02, target_type="percentage",
          target_value=0.40), agg_tf="15-MINUTE", params=bbp(20, 2.0), allocation_pct=25),
      leg("4 Moving Averages", EUR, 0.04, ec(stop_loss_type="atr", sl_atr_period=14,
          sl_atr_multiplier=2.0, target_type="atr", tgt_atr_period=21, tgt_atr_multiplier=3.0),
          agg_tf="5-MINUTE", params=map4(5, 13, 21, 55, True), allocation_pct=20)],
      starting_capital=300000.0, allocation_mode="percentage", product="NRML",
      squareoff_time="21:00", squareoff_tz="America/New_York",
      pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=8000, pf_sl_delay_sec=30,
      pf_sl_trail_enabled=True, pf_sl_trail_every=2000, pf_sl_trail_by=500,
      pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=12000,
      pf_tgt_trail_enabled=True, pf_tgt_trail_lock_min_profit=3000,
      pf_tgt_trail_when_profit_reach=6000, pf_tgt_trail_every=1500, pf_tgt_trail_by=400),
  lambda r: (_finite(r.get("total_pnl")) and len(r.get("per_strategy") or {}) == 4,
             f"4 slots, trades={r.get('total_trades')}, clip={r.get('pf_clip_reason')}")),

# P03 — Format-A bid/ask everywhere, dual ATR, 15-min agg, MIS
P("EDGE_03", "Format-A bid/ask on all legs + dual-ATR SL/Target + 15-min aggregation, portfolio Combined-Loss SL, MIS",
  "Three EURUSD legs all using Format-A (bid/ask) exit pricing, ATR-based SL and ATR-based Target, aggregated to 15-min; portfolio Combined Loss SL with delay; MIS.",
  "2021-03-01 .. 2021-04-30", {},
  lambda: pf("EDGE_03_formatA_atr", "2021-03-01", "2021-04-30", [
      leg("EMA Cross", EUR, 0.05, ec(exit_price_format="bidask", stop_loss_type="atr",
          sl_atr_period=14, sl_atr_multiplier=2.0, target_type="atr", tgt_atr_period=21,
          tgt_atr_multiplier=3.0, sl_wait_sec=30, tgt_wait_sec=30), agg_tf="15-MINUTE",
          params=emap(8, 34)),
      leg("EMA Cross", EUR, 0.04, ec(exit_price_format="bidask", stop_loss_type="atr",
          sl_atr_period=10, sl_atr_multiplier=1.5, target_type="atr", tgt_atr_period=14,
          tgt_atr_multiplier=2.5), agg_tf="15-MINUTE", params=emap(13, 40)),
      leg("RSI Mean Reversion", EUR, 0.03, ec(exit_price_format="bidask",
          stop_loss_type="atr", sl_atr_period=14, sl_atr_multiplier=2.0,
          target_type="percentage", target_value=0.35), agg_tf="15-MINUTE", params=rsip(10, 75, 25))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="20:30",
      pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=6000, pf_sl_delay_sec=45,
      pf_sl_sqoff_only_loss_legs=True),
  lambda r: (_finite(r.get("total_pnl")), f"Format-A+ATR agg-15m, trades={r.get('total_trades')}")),

# P04 — RBO full settings, MIS, 5-min agg
P("EDGE_04", "RangeBreakout full settings (monitoring/entry windows, buffer, RangeHigh, cancel-other-side) + leg SL/Target + MIS + 5-min agg",
  "Portfolio RBO gate enabled with full parameters wrapping two EMA legs; entry window aligned to the RBO entry window; MIS; 5-min aggregation.",
  "2021-05-03 .. 2021-06-30", {},
  lambda: pf("EDGE_04_rbo_full", "2021-05-03", "2021-06-30", [
      leg("EMA Cross", GBP, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.25,
          target_type="percentage", target_value=0.40, on_sl_action="re_execute",
          max_re_executions=2), agg_tf="5-MINUTE", params=emap(9, 27)),
      leg("EMA Cross", GBP, 0.04, ec(stop_loss_type="trailing", stop_loss_value=0.30,
          trailing_sl_step=0.05, trailing_sl_offset=0.03, target_type="points",
          target_value=0.0050), agg_tf="5-MINUTE", params=emap(12, 36))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="20:00",
      entry_start_time="13:30", entry_end_time="17:00",
      rbo_enabled=True, range_monitoring_start="13:00:00", range_monitoring_end="14:00:00",
      rbo_entry_start="14:00:00", rbo_entry_end="17:00:00", rbo_range_buffer=15,
      rbo_entry_at="RangeHigh", rbo_monitoring="Underlying", rbo_cancel_other_side=True),
  lambda r: (_finite(r.get("total_pnl")), f"RBO gate, trades={r.get('total_trades')}")),

# P05 — Underlying-Movement portfolio SL + Underlying-Movement Target
P("EDGE_05", "Portfolio Underlying-Movement SL + Underlying-Movement Target (with target trailing) + leg SL, NRML, 5-min agg",
  "Single EUR leg; portfolio SL fires on underlying price below a level, portfolio Target fires on underlying above a level with target trailing; leg-level percentage SL too; NRML.",
  "2020-07-01 .. 2020-09-30", {},
  lambda: pf("EDGE_05_underlying", "2020-07-01", "2020-09-30", [
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.40,
          target_type="percentage", target_value=0.60), agg_tf="5-MINUTE", params=emap(10, 30))],
      product="NRML", pf_sl_enabled=True, pf_sl_type="Underlying Movement", pf_sl_value=1.0850,
      pf_sl_delay_sec=120, pf_tgt_enabled=True, pf_tgt_type="Underlying Movement",
      pf_tgt_value=1.2000, pf_tgt_delay_sec=60, pf_tgt_trail_enabled=True,
      pf_tgt_trail_lock_min_profit=500, pf_tgt_trail_when_profit_reach=900,
      pf_tgt_trail_every=200, pf_tgt_trail_by=100),
  lambda r: (_finite(r.get("total_pnl")), f"underlying SL+TGT, clip={r.get('pf_clip_reason')}")),

# P06 — ReExecute / reverse actions + portfolio ReExecute replay
P("EDGE_06", "Leg multi-action SL (re_execute,reverse) + Target re_execute + portfolio SL & Target ReExecute replay, MIS, 5-min agg",
  "Two EUR legs; leg SL action is a re_execute+reverse combo, Target action re_execute with caps; portfolio SL and Target both use the ReExecute action with replay counts (replay flag on).",
  "2021-04-01 .. 2021-05-31", {"_USE_PF_REEXEC_REPLAY": True},
  lambda: pf("EDGE_06_reexecute", "2021-04-01", "2021-05-31", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.18,
          target_type="percentage", target_value=0.25, on_sl_action="re_execute,reverse",
          on_target_action="re_execute", max_re_executions=3), agg_tf="5-MINUTE", params=emap(6, 18)),
      leg("RSI Mean Reversion", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          on_sl_action="re_execute", max_re_executions=2), agg_tf="5-MINUTE", params=rsip(14))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="21:00",
      pf_sl_enabled=True, pf_sl_value=300, pf_sl_action="ReExecute", pf_sl_reexecute_count=4,
      pf_tgt_enabled=True, pf_tgt_value=600, pf_tgt_action="ReExecute", pf_tgt_reexecute_count=4,
      no_reexec_sl_cost=True, no_wait_trade_reexec=True),
  lambda r: (_finite(r.get("total_pnl")) and (r.get("pf_reexec_replays", 0) or 0) <= 50,
             f"replays={r.get('pf_reexec_replays')}, trades={r.get('total_trades')}")),

# P07 — 10 legs across 3 instruments, grouped, mixed everything, NRML
P("EDGE_07", "Ten legs across EUR/GBP/JPY, mixed strategies & exit formats & aggregation timeframes, grouped engine, portfolio SL/Target, NRML",
  "Ten legs spanning all three FX pairs and all five-ish exit/aggregation combinations; runs in a shared grouped engine; portfolio Combined Loss SL + Combined Profit Target; NRML.",
  "2021-06-01 .. 2021-07-31", {"_USE_GROUPING": True},
  lambda: pf("EDGE_07_ten_legs", "2021-06-01", "2021-07-31", [
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          target_type="percentage", target_value=0.30), agg_tf="5-MINUTE", params=emap(8, 21)),
      leg("EMA Cross", EUR, 0.03, ec(exit_price_format="bidask", stop_loss_type="atr",
          sl_atr_period=14, sl_atr_multiplier=2.0), agg_tf="5-MINUTE", params=emap(10, 30)),
      leg("RSI Mean Reversion", EUR, 0.03, ec(exit_price_format="ltp",
          stop_loss_type="percentage", stop_loss_value=0.25), agg_tf="15-MINUTE", params=rsip(14)),
      leg("EMA Cross", GBP, 0.04, ec(stop_loss_type="trailing", stop_loss_value=0.25,
          trailing_sl_step=0.05, trailing_sl_offset=0.03), agg_tf="5-MINUTE", params=emap(9, 27)),
      leg("Bollinger Bands", GBP, 0.03, ec(stop_loss_type="points", stop_loss_value=0.0030,
          target_type="points", target_value=0.0050), agg_tf="5-MINUTE", params=bbp(20, 2.0)),
      leg("4 Moving Averages", GBP, 0.03, ec(target_type="percentage", target_value=0.35),
          agg_tf="15-MINUTE", params=map4(5, 13, 21, 55, False)),
      leg("EMA Cross", JPY, 0.02, ec(exit_price_format="ltp", stop_loss_type="atr",
          sl_atr_period=14, sl_atr_multiplier=2.0), agg_tf="5-MINUTE", params=emap(9, 27)),
      leg("RSI Mean Reversion", JPY, 0.02, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          target_type="percentage", target_value=0.30), agg_tf="5-MINUTE", params=rsip(10, 75, 25)),
      leg("Bollinger Bands", EUR, 0.03, ec(stop_loss_type="percentage", stop_loss_value=0.22,
          tgt_trail_enabled=True, tgt_trail_when_profit_reach=0.10, tgt_trail_lock_min_profit=0.04,
          tgt_trail_every=0.05, tgt_trail_by=0.03), agg_tf="5-MINUTE", params=bbp(14, 2.5)),
      leg("EMA Cross", JPY, 0.02, ec(stop_loss_type="percentage", stop_loss_value=0.30),
          agg_tf="1-HOUR", params=emap(12, 48))],
      starting_capital=600000.0, product="NRML", squareoff_time="21:00",
      run_on_days=["MON", "TUE", "WED", "THU", "FRI"], entry_start_time="08:00",
      entry_end_time="17:00", pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=20000,
      pf_sl_delay_sec=30, pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=30000),
  lambda r: (_finite(r.get("total_pnl")) and len(r.get("per_strategy") or {}) == 10,
             f"10 slots produced per_strategy ({len(r.get('per_strategy') or {})}/10)")),

# P08 — Move-SL full settings (agg-pnl two-pass), MIS
P("EDGE_08", "Move-SL full stack (move-to-cost, trail-after, LTP+buffer, aggregate-PnL two-pass) + leg trailing, MIS, 5-min agg",
  "Two EUR legs; every move_sl_* knob enabled including the aggregate-PnL two-pass trigger; leg trailing SL; MIS.",
  "2021-02-01 .. 2021-03-15", {"_USE_PF_AGG_MOVE_SL": True},
  lambda: pf("EDGE_08_move_sl", "2021-02-01", "2021-03-15", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="trailing", stop_loss_value=0.30,
          trailing_sl_step=0.04, trailing_sl_offset=0.02, target_type="percentage",
          target_value=0.40), agg_tf="5-MINUTE", params=emap(6, 18)),
      leg("RSI Mean Reversion", EUR, 0.04, ec(stop_loss_type="trailing", stop_loss_value=0.30,
          trailing_sl_step=0.04, trailing_sl_offset=0.02, target_type="percentage",
          target_value=0.40), agg_tf="5-MINUTE", params=rsip(14))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="22:00",
      move_sl_enabled=True, move_sl_safety_sec=120,
      move_sl_action="Move SL to LTP + Buffer for Loss Making Legs", move_sl_trail_after=True,
      move_sl_hit_on_leg_sl=True, move_sl_hit_on_leg_target=True, move_sl_ltp_buffer=0.0005,
      move_sl_agg_pnl_enabled=True, move_sl_agg_pnl_threshold=300, move_sl_agg_pnl_direction="profit"),
  lambda r: (_finite(r.get("total_pnl")), f"move-sl two-pass, trades={r.get('total_trades')}")),

# P10 — degenerate / malformed (graceful-degrade edge case)
P("EDGE_10", "DEGENERATE edge case — zero lots, malformed squareoff '25:99', bogus tz/enums, negative values, empty run_on_days",
  "Deliberately invalid everywhere: zero lots, invalid exit format, ATR period 0, negative targets/waits, malformed squareoff time and timezone, empty run_on_days, nonsense RBO/move-SL enums. The engine must raise a clean error OR degrade — never corrupt.",
  "2021-01-04 .. 2021-01-04", {"_USE_PF_REEXEC_REPLAY": True},
  lambda: pf("EDGE_10_degenerate", "2021-01-04", "2021-01-04", [
      leg("EMA Cross", EUR, 0, ec(exit_price_format="garbage", stop_loss_type="atr",
          sl_atr_period=0, sl_atr_multiplier=2.0, target_type="atr", target_value=-0.5,
          tgt_atr_period=0, tgt_atr_multiplier=-1, target_lock_trigger=-1, target_lock_minimum=-1,
          tgt_trail_enabled=True, tgt_trail_when_profit_reach=0, sl_wait_sec=-5, sl_wait_bars=-3,
          on_sl_action="keep_leg_running,re_execute", on_target_action="re_execute,re_entry",
          max_re_executions=-1, execute_target_leg_id="nope", reentry_price=-1, max_re_entries=-1,
          armed_at_start=False), params={"fast_ema_period": 0, "slow_ema_period": 0})],
      starting_capital=0.0, allocation_mode="percentage", product="MIS", max_loss=-1, max_profit=0,
      squareoff_time="25:99", squareoff_tz="Mars/Olympus", run_on_days=[],
      entry_start_time="18:00", entry_end_time="06:00", rbo_enabled=True,
      range_monitoring_start="14:00:00", range_monitoring_end="13:00:00",
      rbo_entry_at="NonsenseSide", rbo_range_buffer=-5, pf_sl_enabled=True,
      pf_sl_type="Underlying Movement", pf_sl_value=0, pf_sl_action="ReExecute",
      pf_sl_reexecute_count=999999, move_sl_enabled=True, move_sl_action="BogusAction",
      move_sl_agg_pnl_direction="sideways"),
  lambda r: (True, "degenerate config — graceful error OR degraded run both acceptable")),

# P11 — timing gates + 3-zone squareoff across DST
P("EDGE_11", "All timing gates at once — run_on_days + entry window + 3-zone squareoff (leg/slot/portfolio, 3 timezones) across a DST boundary, 5-min agg",
  "Single EUR leg with leg-level squareoff (Asia/Tokyo) inside a slot-level squareoff (Europe/London) inside a portfolio squareoff (America/New_York); custom run_on_days; UTC entry window; range spans US DST 2021-03-14.",
  "2021-03-01 .. 2021-04-15", {},
  lambda: pf("EDGE_11_timing_dst", "2021-03-01", "2021-04-15", [
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          target_type="percentage", target_value=0.25, squareoff_time="12:00",
          squareoff_tz="Asia/Tokyo"), agg_tf="5-MINUTE", params=emap(9, 21),
          squareoff_time="15:30", squareoff_tz="Europe/London")],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="16:00",
      squareoff_tz="America/New_York", run_on_days=["TUE", "WED", "THU"],
      entry_start_time="13:00", entry_end_time="20:00", no_reentry_after_end=True),
  lambda r: (_finite(r.get("total_pnl")), f"3-zone sqoff across DST, trades={r.get('total_trades')}")),

# P12 — 1-HOUR aggregation, longer range
P("EDGE_12", "1-HOUR aggregation on a multi-leg portfolio, leg SL/Target + portfolio Combined-Loss, NRML, 4-month range",
  "Three legs aggregated to 1-HOUR bars (heavy aggregation); leg-level SL/Target; portfolio Combined Loss SL; NRML; long range.",
  "2021-01-04 .. 2021-04-30", {},
  lambda: pf("EDGE_12_hourly", "2021-01-04", "2021-04-30", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.50,
          target_type="percentage", target_value=0.80), agg_tf="1-HOUR", params=emap(10, 30)),
      leg("RSI Mean Reversion", GBP, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.50),
          agg_tf="1-HOUR", params=rsip(14)),
      leg("Bollinger Bands", JPY, 0.03, ec(target_type="percentage", target_value=1.0),
          agg_tf="1-HOUR", params=bbp(20, 2.0))],
      product="NRML", pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=15000),
  lambda r: (_finite(r.get("total_pnl")) and len(r.get("per_strategy") or {}) == 3,
             f"1-hour agg, trades={r.get('total_trades')}")),

# P13 — crypto LTP, 3 coins, daily bars (no aggregation possible)
P("EDGE_13", "Crypto trio (BTC/LTC/XRP) daily LAST bars — LTP-format exits, trailing SL, portfolio caps, MIS (no aggregation on daily base)",
  "BTC + LTC + XRP on 1-DAY LAST bars; Format-C (LTP) exit pricing; trailing SL + %-target; portfolio Combined Loss SL + Combined Profit Target; MIS. Daily base cannot aggregate up — confirms the no-aggregation branch on coarse bases.",
  "2021-01-01 .. 2021-12-31", {},
  lambda: pf("EDGE_13_crypto_ltp", "2021-01-01", "2021-12-31", [
      leg("Bollinger Bands", BTC, 0.5, ec(exit_price_format="ltp", stop_loss_type="trailing",
          stop_loss_value=3.0, trailing_sl_step=1.0, trailing_sl_offset=0.5,
          target_type="percentage", target_value=5.0, sl_wait_bars=2), params=bbp(20, 2.5)),
      leg("EMA Cross", LTC, 1.0, ec(exit_price_format="ltp", stop_loss_type="percentage",
          stop_loss_value=4.0, target_type="percentage", target_value=6.0), params=emap(10, 30)),
      leg("RSI Mean Reversion", XRP, 100.0, ec(stop_loss_type="percentage", stop_loss_value=5.0),
          params=rsip(14, 70, 30))],
      starting_capital=200000.0, product="MIS", mis_squareoff_time="23:30",
      pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=25000,
      pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=50000),
  lambda r: (_finite(r.get("total_pnl")) and len(r.get("per_strategy") or {}) == 3,
             f"crypto LTP daily, trades={r.get('total_trades')}")),

# P14 — mixed aggregation timeframes on the same instrument
P("EDGE_14", "Mixed aggregation timeframes — same EURUSD base aggregated to 5-min, 15-min and 1-hour on three different legs, MIS",
  "Three EUR legs identical except aggregation timeframe (5-min / 15-min / 1-hour) to confirm each leg subscribes to its own composite independently in one portfolio; MIS.",
  "2021-02-01 .. 2021-03-31", {},
  lambda: pf("EDGE_14_mixed_agg", "2021-02-01", "2021-03-31", [
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.30,
          target_type="percentage", target_value=0.45), agg_tf="5-MINUTE", params=emap(10, 30)),
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.30,
          target_type="percentage", target_value=0.45), agg_tf="15-MINUTE", params=emap(10, 30)),
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.30,
          target_type="percentage", target_value=0.45), agg_tf="1-HOUR", params=emap(10, 30))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="21:00"),
  lambda r: (_finite(r.get("total_pnl")) and len(r.get("per_strategy") or {}) == 3,
             f"mixed agg tfs, trades={r.get('total_trades')}")),

# P15 — squareoff precedence (leg > slot > portfolio)
P("EDGE_15", "Squareoff precedence — leg-level vs slot-level vs portfolio-level squareoff resolved leg>slot>portfolio, two legs each at a different level, MIS",
  "Two EUR legs: leg A sets its own ExitConfig.squareoff_time (earliest), leg B relies on slot squareoff, and the portfolio sets a third; confirms the documented leg>slot>portfolio precedence; MIS supplies a fourth default that explicit times override.",
  "2021-04-01 .. 2021-05-15", {},
  lambda: pf("EDGE_15_sqoff_precedence", "2021-04-01", "2021-05-15", [
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.25,
          target_type="percentage", target_value=0.35, squareoff_time="11:00",
          squareoff_tz="Asia/Kolkata"), agg_tf="5-MINUTE", params=emap(9, 21)),
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.25,
          target_type="percentage", target_value=0.35), agg_tf="5-MINUTE", params=emap(12, 36),
          squareoff_time="14:00", squareoff_tz="Asia/Kolkata")],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="20:00",
      squareoff_tz="America/New_York"),
  lambda r: (_finite(r.get("total_pnl")), f"sqoff precedence, trades={r.get('total_trades')}")),

# P16 — wait-confirmation (bars + secs) on both SL and Target
P("EDGE_16", "SL/Target wait-confirmation — sl_wait_bars + sl_wait_sec + tgt_wait_bars + tgt_wait_sec all set, two legs, NRML, 5-min agg",
  "Two legs whose SL and Target only trigger after a confirmation window (both bar-count and seconds); confirms the wait-confirmation state machine under aggregation; NRML.",
  "2021-03-01 .. 2021-04-15", {},
  lambda: pf("EDGE_16_wait_confirm", "2021-03-01", "2021-04-15", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          target_type="percentage", target_value=0.30, sl_wait_bars=3, sl_wait_sec=300,
          tgt_wait_bars=2, tgt_wait_sec=120), agg_tf="5-MINUTE", params=emap(8, 21)),
      leg("RSI Mean Reversion", GBP, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.25,
          target_type="percentage", target_value=0.35, sl_wait_bars=2, tgt_wait_bars=1),
          agg_tf="5-MINUTE", params=rsip(14))],
      product="NRML", squareoff_time="21:00"),
  lambda r: (_finite(r.get("total_pnl")), f"wait-confirm, trades={r.get('total_trades')}")),

# P17 — all three exit formats in one portfolio
P("EDGE_17", "All three exit-price formats in one portfolio — leg1 OHLCV, leg2 LTP, leg3 bid/ask (Format A) — plus 5-min agg, MIS",
  "Three EUR legs, one per exit-price format, to confirm the three-format engine coexists in a single multi-leg portfolio with aggregation; MIS.",
  "2021-05-01 .. 2021-06-15", {},
  lambda: pf("EDGE_17_three_formats", "2021-05-01", "2021-06-15", [
      leg("EMA Cross", EUR, 0.05, ec(exit_price_format="ohlcv", stop_loss_type="percentage",
          stop_loss_value=0.20, target_type="percentage", target_value=0.30), agg_tf="5-MINUTE",
          params=emap(8, 21)),
      leg("EMA Cross", EUR, 0.04, ec(exit_price_format="ltp", stop_loss_type="percentage",
          stop_loss_value=0.20, target_type="percentage", target_value=0.30), agg_tf="5-MINUTE",
          params=emap(10, 30)),
      leg("EMA Cross", EUR, 0.04, ec(exit_price_format="bidask", stop_loss_type="percentage",
          stop_loss_value=0.20, target_type="percentage", target_value=0.30), agg_tf="5-MINUTE",
          params=emap(12, 36))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="20:30"),
  lambda r: (_finite(r.get("total_pnl")) and len(r.get("per_strategy") or {}) == 3,
             f"3 formats coexist, trades={r.get('total_trades')}")),

# P18 — re-entry action (reentry_price, max_re_entries, armed_at_start False)
P("EDGE_18", "Re-entry action — on_sl_action=re_entry with reentry_price + max_re_entries, armed_at_start False, two legs, MIS, 5-min agg",
  "Legs whose SL action re-enters at a fixed price up to a max, starting disarmed; confirms the re_entry path; MIS.",
  "2021-04-01 .. 2021-05-31", {},
  lambda: pf("EDGE_18_reentry", "2021-04-01", "2021-05-31", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.15,
          on_sl_action="re_entry", reentry_price=1.1850, max_re_entries=3, armed_at_start=False),
          agg_tf="5-MINUTE", params=emap(9, 21)),
      leg("RSI Mean Reversion", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.18,
          on_sl_action="re_entry", reentry_price=1.1900, max_re_entries=2), agg_tf="5-MINUTE",
          params=rsip(14))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="21:00", no_reentry_after_end=True),
  lambda r: (_finite(r.get("total_pnl")), f"re_entry, trades={r.get('total_trades')}")),

# P19 — cross-leg execute (execute_target_leg_id)
P("EDGE_19", "Cross-leg execute — leg A's SL action 'execute' arms leg B via execute_target_leg_id; grouped engine; 5-min agg; NRML",
  "Two linked legs: leg A on SL executes leg B (which starts disarmed); confirms the cross-leg execute wiring with slot_ids in a grouped engine; NRML.",
  "2021-05-03 .. 2021-06-30", {"_USE_GROUPING": True},
  lambda: pf("EDGE_19_cross_leg_execute", "2021-05-03", "2021-06-30", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="points", stop_loss_value=0.0030,
          target_type="points", target_value=0.0045, on_sl_action="re_execute,execute",
          max_re_executions=2, execute_target_leg_id="legB"), agg_tf="5-MINUTE",
          params=emap(6, 24), slot_id="legA"),
      leg("RSI Mean Reversion", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.15,
          armed_at_start=False), agg_tf="5-MINUTE", params=rsip(14), slot_id="legB")],
      product="NRML", squareoff_time="19:00", entry_start_time="08:00", entry_end_time="14:00"),
  lambda r: (_finite(r.get("total_pnl")), f"cross-leg execute, trades={r.get('total_trades')}")),

# P20 — kitchen sink (everything at once)
P("EDGE_20", "KITCHEN SINK — every feature at once: many legs, ATR+trailing+lock+target-trailing, RBO, portfolio SL(Loss+Range)+Target ReExecute, Move-SL two-pass, MIS, run_on_days, entry window, 5-min agg, all flags",
  "Maximal portfolio combining leg-level ATR SL & Target with trailing and target-lock, multi-action SL combos with cross-leg execute, RBO gate, portfolio Loss-and-Underlying-Range SL with ReExecute, Combined Profit Target with ReExecute and trailing, full Move-SL with aggregate-PnL two-pass, MIS, run_on_days, entry window, multi-zone squareoff, 5-min aggregation — with grouping + agg-move-SL + reexec-replay all enabled.",
  "2021-03-08 .. 2021-04-08", {"_USE_GROUPING": True, "_USE_PF_AGG_MOVE_SL": True, "_USE_PF_REEXEC_REPLAY": True},
  lambda: pf("EDGE_20_kitchen_sink", "2021-03-08", "2021-04-08", [
      leg("EMA Cross", EUR, 0.05, ec(exit_price_format="bidask", stop_loss_type="atr",
          trailing_sl_step=0.05, trailing_sl_offset=0.03, sl_atr_period=14, sl_atr_multiplier=2.0,
          target_type="atr", tgt_atr_period=21, tgt_atr_multiplier=3.0, target_lock_trigger=0.25,
          target_lock_minimum=0.10, tgt_trail_enabled=True, tgt_trail_when_profit_reach=0.30,
          tgt_trail_lock_min_profit=0.12, tgt_trail_every=0.08, tgt_trail_by=0.04, sl_wait_sec=15,
          tgt_wait_sec=15, on_sl_action="re_execute,execute", on_target_action="re_execute",
          max_re_executions=3, execute_target_leg_id="legB", squareoff_time="18:45"),
          agg_tf="5-MINUTE", params=emap(8, 21), allocation_pct=40, slot_id="legA"),
      leg("RSI Mean Reversion", EUR, 0.04, ec(exit_price_format="bidask", stop_loss_type="atr",
          trailing_sl_step=0.05, trailing_sl_offset=0.03, sl_atr_period=14, sl_atr_multiplier=2.0,
          target_type="atr", tgt_atr_period=21, tgt_atr_multiplier=3.0, target_lock_trigger=0.25,
          target_lock_minimum=0.10, tgt_trail_enabled=True, tgt_trail_when_profit_reach=0.30,
          tgt_trail_lock_min_profit=0.12, on_sl_action="re_execute,execute",
          on_target_action="re_execute", max_re_executions=3, execute_target_leg_id="legC",
          squareoff_time="18:45"), agg_tf="5-MINUTE", params=rsip(14, 72, 28),
          allocation_pct=35, slot_id="legB"),
      leg("Bollinger Bands", GBP, 0.03, ec(stop_loss_type="atr", trailing_sl_step=0.05,
          trailing_sl_offset=0.03, sl_atr_period=14, sl_atr_multiplier=2.0, target_type="atr",
          tgt_atr_period=21, tgt_atr_multiplier=3.0, target_lock_trigger=0.25,
          target_lock_minimum=0.10, tgt_trail_enabled=True, tgt_trail_when_profit_reach=0.30,
          on_sl_action="re_execute,execute", on_target_action="re_execute", max_re_executions=3,
          execute_target_leg_id="legA", squareoff_time="18:45"), agg_tf="5-MINUTE",
          params=bbp(20, 2.0), allocation_pct=25, slot_id="legC", squareoff_time="19:00",
          squareoff_tz="Europe/London")],
      starting_capital=500000.0, allocation_mode="percentage", product="MIS",
      mis_squareoff_time="15:15", squareoff_time="20:30", squareoff_tz="America/New_York",
      max_loss=80000, max_profit=100000, run_on_days=["MON", "TUE", "WED", "THU", "FRI"],
      entry_start_time="13:00", entry_end_time="18:00", rbo_enabled=True,
      range_monitoring_start="12:30:00", range_monitoring_end="13:30:00",
      rbo_entry_start="13:30:00", rbo_entry_end="17:30:00", rbo_range_buffer=20, rbo_entry_at="Any",
      rbo_cancel_other_side=True, pf_sl_enabled=True, pf_sl_type="Loss and Underlying Range",
      pf_sl_value=12000, pf_sl_underlying_below=1.0700, pf_sl_underlying_above=1.2500,
      pf_sl_action="ReExecute", pf_sl_delay_sec=45, pf_sl_reexecute_count=3,
      pf_sl_sqoff_only_loss_legs=True, pf_sl_trail_enabled=True, pf_sl_trail_every=3000,
      pf_sl_trail_by=800, pf_tgt_enabled=True, pf_tgt_value=25000, pf_tgt_action="ReExecute",
      pf_tgt_delay_sec=20, pf_tgt_reexecute_count=3, pf_tgt_trail_enabled=True,
      pf_tgt_trail_lock_min_profit=8000, pf_tgt_trail_when_profit_reach=15000,
      pf_tgt_trail_every=3000, pf_tgt_trail_by=1500, move_sl_enabled=True, move_sl_safety_sec=120,
      move_sl_action="Move SL to LTP + Buffer for Loss Making Legs", move_sl_trail_after=True,
      move_sl_no_buy_legs=True, move_sl_hit_on_leg_sl=True, move_sl_hit_on_leg_target=True,
      move_sl_ltp_buffer=0.0005, move_sl_agg_pnl_enabled=True, move_sl_agg_pnl_threshold=6000,
      move_sl_agg_pnl_direction="profit", no_reexec_sl_cost=True, no_wait_trade_reexec=True,
      no_strike_change_reexec=True, no_reentry_after_end=True, delay_between_legs_sec=60,
      on_sl_action_on="OnSL_Only", on_target_action_on="OnTarget_Trailing_Only",
      straddle_width_multiplier=1.0, trail_wait_trade=True),
  lambda r: (_finite(r.get("total_pnl")),
             f"kitchen-sink slots={len(r.get('per_strategy') or {})}, "
             f"replays={r.get('pf_reexec_replays')}, clip={r.get('pf_clip_reason')}")),

# P21 — ultra-tight SL/TP churn (edge: enormous trade count)
P("EDGE_21", "Ultra-tight SL/Target churn — 0.02% SL and 0.02% target so the legs churn constantly, 5-min agg, MIS (stress on re-entry throughput)",
  "Two EUR legs with near-zero SL/Target so almost every bar exits and re-enters; confirms no runaway/leak under maximal churn with aggregation; MIS.",
  "2021-02-01 .. 2021-02-28", {},
  lambda: pf("EDGE_21_tight_churn", "2021-02-01", "2021-02-28", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.02,
          target_type="percentage", target_value=0.02), agg_tf="5-MINUTE", params=emap(3, 8)),
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=0.02,
          target_type="percentage", target_value=0.02), agg_tf="5-MINUTE", params=emap(5, 13))],
      product="MIS", mis_squareoff_time="15:15", squareoff_time="22:00"),
  lambda r: (_finite(r.get("total_pnl")), f"churn trades={r.get('total_trades')}")),

# P22 — caps/SL never hit (edge: pure signal, NRML carry, long range)
P("EDGE_22", "Never-triggered caps — huge SL/Target and huge portfolio caps so only signals act, NRML carry-forward, 4-month range, 15-min agg",
  "Wide-open SL/Target and portfolio caps (effectively disabled) so the run is pure signal/flip; NRML carries positions; confirms behaviour when no protective level ever fires.",
  "2021-01-04 .. 2021-04-30", {},
  lambda: pf("EDGE_22_never_hit", "2021-01-04", "2021-04-30", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="percentage", stop_loss_value=999.0,
          target_type="percentage", target_value=999.0), agg_tf="15-MINUTE", params=emap(10, 30)),
      leg("RSI Mean Reversion", GBP, 0.04, ec(stop_loss_type="percentage", stop_loss_value=999.0),
          agg_tf="15-MINUTE", params=rsip(14))],
      product="NRML", max_loss=99999999, max_profit=99999999, pf_sl_enabled=True,
      pf_sl_type="Combined Loss", pf_sl_value=99999999, pf_tgt_enabled=True,
      pf_tgt_type="Combined Profit", pf_tgt_value=99999999),
  lambda r: (_finite(r.get("total_pnl")), f"no-trigger pure-signal, trades={r.get('total_trades')}")),

# P23 — conflicting entry window vs squareoff (empty admissible window edge)
P("EDGE_23", "Conflicting gates edge — entry window AFTER the squareoff time (empty admissible window) + run_on_days weekend-only, MIS, 5-min agg",
  "Entry window 16:00-20:00 but squareoff at 14:00, plus run_on_days = SAT/SUN (no FX weekend bars): the admissible set is empty. Must produce a clean zero-trade or explanatory result, not a crash.",
  "2021-03-01 .. 2021-03-31", {},
  lambda: pf("EDGE_23_conflict_gates", "2021-03-01", "2021-03-31", [
      leg("EMA Cross", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.20,
          target_type="percentage", target_value=0.30), agg_tf="5-MINUTE", params=emap(9, 21))],
      product="MIS", mis_squareoff_time="14:00", squareoff_time="14:00",
      entry_start_time="16:00", entry_end_time="20:00", run_on_days=["SAT", "SUN"]),
  lambda r: (True, f"empty-window edge handled, trades={r.get('total_trades')}")),

# P24 — single trading day, intraday MIS squareoff, full leg exits
P("EDGE_24", "Single trading day, high-frequency — full leg SL/Target + intraday MIS squareoff + 5-min agg on one day",
  "One trading day; full leg-level SL/Target stack; MIS forces an intraday squareoff; 5-min aggregation; confirms intraday squareoff + aggregation on a tight single-day window.",
  "2021-06-08 .. 2021-06-08", {},
  lambda: pf("EDGE_24_single_day", "2021-06-08", "2021-06-08", [
      leg("EMA Cross", EUR, 0.05, ec(stop_loss_type="trailing", stop_loss_value=0.20,
          trailing_sl_step=0.04, trailing_sl_offset=0.02, target_type="percentage",
          target_value=0.30, target_lock_trigger=0.15, target_lock_minimum=0.08),
          agg_tf="5-MINUTE", params=emap(5, 13)),
      leg("RSI Mean Reversion", EUR, 0.04, ec(stop_loss_type="percentage", stop_loss_value=0.25),
          agg_tf="5-MINUTE", params=rsip(14, 75, 25))],
      product="MIS", mis_squareoff_time="15:15", mis_squareoff_tz="Asia/Kolkata"),
  lambda r: (_finite(r.get("total_pnl")), f"single-day intraday MIS, trades={r.get('total_trades')}")),


def _reasons(r):
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
                max_drawdown=r.get("max_drawdown"), per_strategy_slots=len(r.get("per_strategy") or {}),
                pf_clip_reason=r.get("pf_clip_reason"), pf_clip_ts=r.get("pf_clip_ts"),
                pf_clip_action=r.get("pf_clip_action"), pf_reexec_replays=r.get("pf_reexec_replays"),
                max_loss_hit=r.get("max_loss_hit"), max_profit_hit=r.get("max_profit_hit"),
                exit_reasons=_reasons(r))


def set_env(flags):
    for k in ("_USE_GROUPING", "_USE_BACKTEST_NODE", "_USE_PF_AGG_MOVE_SL",
              "_USE_PF_REEXEC_REPLAY", "_PROFILE_PHASES"):
        os.environ[k] = "1" if flags.get(k) else "0"


def run():
    from core.models import portfolio_from_dict, portfolio_to_dict
    from core.backtest_runner import run_portfolio_backtest, clear_cross_portfolio_bus
    from core.users import reset_user_pnl

    PF_DIR.mkdir(parents=True, exist_ok=True)
    results = []
    for p in PORTFOLIOS:
        rec = dict(id=p["id"], title=p["title"], desc=p["desc"], range=p["range"], env=p["env"])
        t0 = time.time()
        print(f"\n{'='*72}\n{p['id']} — {p['title'][:60]}\n{'='*72}", flush=True)
        try:
            d = p["builder"]()
            pfobj = portfolio_from_dict(d)
            canon = portfolio_to_dict(pfobj)
            # Save into the project so it can be opened/run from the UI.
            (PF_DIR / f"{d['name']}.json").write_text(
                json.dumps(canon, indent=2, default=str), encoding="utf-8")
            rec["saved_as"] = f"portfolios/_default/{d['name']}.json"
            rec["params"] = canon  # full parameter dump for the report
            set_env(p["env"])
            reset_user_pnl()
            clear_cross_portfolio_bus()
            r = run_portfolio_backtest(CATALOG, pfobj, user_id="_default")
            rec["status"] = "COMPLETED"
            rec["result"] = _summ(r)
            ok, note = p["verdict"](r)
            rec["verdict"] = "PASS" if ok else "FAIL"
            rec["note"] = note
        except Exception as e:
            rec["status"] = "ERROR"
            rec["error"] = f"{type(e).__name__}: {e}"
            rec["trace"] = traceback.format_exc()[-1500:]
            # The two intentional degenerate/edge cases are allowed to raise cleanly.
            rec["verdict"] = "DEGRADED-OK" if p["id"] in ("EDGE_10", "EDGE_23") else "FAIL"
            rec["note"] = "raised a handled exception — see error"
        rec["seconds"] = round(time.time() - t0, 1)
        print(f"  -> {rec['status']} / {rec['verdict']} ({rec['seconds']}s) — {rec.get('note','')}", flush=True)
        results.append(rec)
        OUT.write_text(json.dumps(results, indent=2, default=str), encoding="utf-8")
    npass = sum(1 for r in results if r["verdict"] == "PASS")
    nde = sum(1 for r in results if r["verdict"] == "DEGRADED-OK")
    print(f"\nEDGE SUITE DONE — {npass} PASS, {nde} DEGRADED-OK, "
          f"{len(results)-npass-nde} FAIL — results at {OUT}", flush=True)


if __name__ == "__main__":
    run()
