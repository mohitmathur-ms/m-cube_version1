"""Extended multi-portfolio settings matrix — the settings NOT covered by the BXT
suite. Each is INJECTED onto a NIFTY 5-min base (so all share the instrument and
stress the shared-instrument session path), run STANDALONE and in a SESSION with the
others; per-leg session result must equal standalone. Run under unified OR
_USE_BACKTEST_NODE=1.

Settings covered here: SL-wait, target-lock, re-entry, move-SL (aggregate-P&L),
move-SL (hit-on-leg, 2-leg), RBO, underlying portfolio-SL, portfolio-target ReExecute.
(Format-A bid/ask is FX-only → tested separately via BXT_11.)
"""
import os, sys, json, copy
from pathlib import Path
os.environ.setdefault("_USE_UNIFIED_ENGINE", "1")
from core.models import portfolio_from_dict
from core.backtest_runner.unified import _run_portfolio_unified
from multiportfolio_session_parity_test import _common_args, _summary

BASE = json.loads(Path("portfolios/_default/BXT_01_sl_tgt_5m.json").read_text(encoding="utf-8"))


def _mut_slwait(d):
    d["slots"][0]["exit_config"]["sl_wait_bars"] = 3

def _mut_tgtlock(d):
    ec = d["slots"][0]["exit_config"]
    ec["target_lock_trigger"] = 0.1
    ec["target_lock_minimum"] = 0.05

def _mut_reentry(d):
    ec = d["slots"][0]["exit_config"]
    ec["on_sl_action"] = "re_entry"
    ec["reentry_price"] = 0.0
    ec["max_re_entries"] = 3

def _mut_movesl_agg(d):
    d.update(move_sl_enabled=True, move_sl_safety_sec=0, move_sl_agg_pnl_enabled=True,
             move_sl_agg_pnl_threshold=300, move_sl_agg_pnl_direction="loss")

def _mut_movesl_hit(d):
    # Hit-on-leg needs siblings → duplicate the slot into a 2-leg portfolio.
    s2 = copy.deepcopy(d["slots"][0]); s2["slot_id"] = d["slots"][0]["slot_id"] + "_leg2"
    d["slots"].append(s2)
    d.update(move_sl_enabled=True, move_sl_safety_sec=0,
             move_sl_hit_on_leg_sl=True, move_sl_hit_on_leg_target=True)

def _mut_rbo(d):
    d.update(rbo_enabled=True, range_monitoring_start="04:00:00", range_monitoring_end="05:00:00",
             rbo_entry_start="05:00:00", rbo_entry_end="10:00:00", rbo_monitoring="Underlying",
             rbo_entry_at="RangeHigh", rbo_range_buffer=0, rbo_cancel_other_side=False)

def _mut_underlying_sl(d):
    d.update(pf_sl_enabled=True, pf_sl_type="Underlying Movement", pf_sl_value=24500.0,
             pf_sl_action="SqOff")

def _mut_pftgt_reexec(d):
    d.update(pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=300.0,
             pf_tgt_action="ReExecute")

# ── Gap features (OK in the compliance report; now also tested at multi-pf) ──
def _mut_atr_target(d):
    ec = d["slots"][0]["exit_config"]
    ec["target_type"] = "atr"; ec["tgt_atr_period"] = 14; ec["tgt_atr_multiplier"] = 2.0

def _mut_loss_range(d):
    d.update(pf_sl_enabled=True, pf_sl_type="Loss and Underlying Range", pf_sl_value=200.0,
             pf_sl_underlying_below=24400.0, pf_sl_underlying_above=24900.0, pf_sl_action="SqOff")

def _dup_leg(d):
    s2 = copy.deepcopy(d["slots"][0]); s2["slot_id"] = d["slots"][0]["slot_id"] + "_leg2"
    d["slots"].append(s2); return s2

def _mut_sel_sqoff_loss(d):
    _dup_leg(d)
    d.update(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=300.0,
             pf_sl_action="SqOff", pf_sl_sqoff_only_loss_legs=True)

def _mut_sel_sqoff_profit(d):
    _dup_leg(d)
    d.update(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=300.0,
             pf_sl_action="SqOff", pf_sl_sqoff_only_profit_legs=True)

def _mut_pf_trail_sl(d):
    d.update(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=500.0,
             pf_sl_action="SqOff", pf_sl_trail_enabled=True, pf_sl_trail_every=200.0, pf_sl_trail_by=100.0)

def _mut_pf_trail_tgt(d):
    d.update(pf_tgt_enabled=True, pf_tgt_type="Combined Profit", pf_tgt_value=500.0,
             pf_tgt_action="SqOff", pf_tgt_trail_enabled=True, pf_tgt_trail_lock_min_profit=100.0,
             pf_tgt_trail_when_profit_reach=300.0, pf_tgt_trail_every=100.0, pf_tgt_trail_by=50.0)

def _mut_leg_trail_after_move(d):
    d.update(move_sl_enabled=True, move_sl_safety_sec=0, move_sl_trail_after=True)

def _mut_pf_sl_reexec_count(d):
    d.update(pf_sl_enabled=True, pf_sl_type="Combined Loss", pf_sl_value=200.0,
             pf_sl_action="ReExecute", pf_sl_reexecute_count=2, pf_sl_delay_sec=60)

def _mut_leg_execute(d):
    s1 = d["slots"][0]
    s2 = _dup_leg(d)
    s2["exit_config"]["armed_at_start"] = False           # dormant until armed
    s1["exit_config"]["on_sl_action"] = "execute"          # leg1 SL → arm leg2
    s1["exit_config"]["execute_target_leg_id"] = s2["slot_id"]

MUTATORS = [
    ("slwait", _mut_slwait), ("tgtlock", _mut_tgtlock), ("reentry", _mut_reentry),
    ("movesl_agg", _mut_movesl_agg), ("movesl_hit", _mut_movesl_hit), ("rbo", _mut_rbo),
    ("underlying_sl", _mut_underlying_sl), ("pftgt_reexec", _mut_pftgt_reexec),
    ("atr_target", _mut_atr_target), ("loss_range", _mut_loss_range),
    ("sel_sqoff_loss", _mut_sel_sqoff_loss), ("sel_sqoff_profit", _mut_sel_sqoff_profit),
    ("pf_trail_sl", _mut_pf_trail_sl), ("pf_trail_tgt", _mut_pf_trail_tgt),
    ("leg_trail_after_move", _mut_leg_trail_after_move),
    ("pf_sl_reexec_count", _mut_pf_sl_reexec_count), ("leg_execute", _mut_leg_execute),
]


def _build(tag, mut):
    d = copy.deepcopy(BASE)
    d["name"] = f"{tag}_pf"
    for s in d["slots"]:
        s["slot_id"] = f"{tag}_{s['slot_id']}"
    mut(d)
    return portfolio_from_dict(d)


def main():
    standalone, specs, all_pairs, all_maps, base_args = {}, [], [], {}, None
    for tag, mut in MUTATORS:
        pf = _build(tag, mut)
        args, spec, smap = _common_args(pf, "catalog", "custom_strategies", "_default")
        standalone[tag] = _summary(_run_portfolio_unified(**args)[0])
        if base_args is None:
            base_args = args
        specs.append(spec); all_pairs += list(args["slot_capital_pairs"]); all_maps.update(smap)

    merged = dict(base_args); merged["slot_capital_pairs"] = all_pairs
    merged["portfolio_name"] = "MATRIX_SESSION"
    sess = _summary(_run_portfolio_unified(session_pf_specs=specs, slot_pf_ids=all_maps, **merged)[0])

    ok_all = True
    for tag, _ in MUTATORS:
        ok = all(sess.get(sid) == val for sid, val in standalone[tag].items())
        ok_all &= ok
        print(f"[{'PASS' if ok else 'FAIL'}] {tag:16s} standalone={standalone[tag]}  "
              f"session={{{', '.join(f'{s!r}: {sess.get(s)}' for s in standalone[tag])}}}")
        if not ok:
            for sid, val in standalone[tag].items():
                if sess.get(sid) != val:
                    print(f"      DIVERGE {sid}: standalone={val} session={sess.get(sid)}")
    print("MATRIX_PARITY_OK" if ok_all else "MATRIX_PARITY_FAIL")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
