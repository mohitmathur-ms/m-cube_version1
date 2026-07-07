"""Tag-tier & User-tier aggregate caps — live, same-session end-to-end test.

Drives the PRODUCTION entry point ``run_session_backtest`` (so it exercises the
config read, the live tag/user monitor in the unified engine, AND the
``live_session_caps`` double-clip suppression in ``_merge_portfolio_results``).

Setup: two portfolios (PFA, PFB) in ONE session engine, EACH with its OWN
portfolio SL/Target DISABLED — so the ONLY thing that can ever stop them is the
tier above (tag, then user). We then run the session twice and compare each
portfolio's (n_trades, pnl):

  • TIGHT tier cap  → the tier's combined-loss breaches early → the WHOLE group
                      is force-squared same-bar.
  • HUGE  tier cap  → never breaches → both portfolios run free.

If BOTH portfolios are cut short under the tight cap (and run free under the huge
one), the tier monitor demonstrably (a) summed combined PnL across the group and
(b) force-squared every portfolio in it — live, in one session. Because each
portfolio's own caps are OFF, the tier is the sole possible cause.

A temp ``config/tags.json`` / ``config/users.json`` is used (real config never
touched). Run: venv\\Scripts\\python.exe multiportfolio_tier_caps_test.py
"""
import os
import sys
import json
import copy
import tempfile
from pathlib import Path

os.environ.setdefault("_USE_UNIFIED_ENGINE", "1")

import core.tags as tags
import core.users as users
from core.models import portfolio_from_dict
from core.backtest_runner.session import run_session_backtest

BASE = "portfolios/_default/CRYPTO_REEXEC_ENTRY.json"
COIN = "eth"   # both portfolios trade the SAME losing instrument → combined loss
               # is large & guaranteed to breach a tight cap (also exercises the
               # shared-instrument per-scope P&L reconstruction in the tag/user monitor).


def _mk(base, name, tag):
    d = copy.deepcopy(base)
    d["name"] = name
    d["slots"] = [s for s in d["slots"] if COIN in s["slot_id"].lower()]
    for s in d["slots"]:
        s["slot_id"] = f"{s['slot_id']}_{name}"
    # Own portfolio SL/Target OFF — only the tier above can stop these legs.
    d.update(pf_sl_enabled=False, pf_tgt_enabled=False, portfolio_tag=tag)
    return portfolio_from_dict(d)


def _metrics(results):
    """{portfolio_name: (n_trades, round(total_pnl, 2))}."""
    return {nm: (int(r.get("total_trades", 0)), round(float(r.get("total_pnl", 0.0)), 2))
            for nm, r in results.items()}


def _run(portfolios, user_id):
    tags.reset_tag_pnl()
    users.reset_user_pnl()
    return _metrics(run_session_backtest("catalog", portfolios, "custom_strategies", user_id))


def _set_tag(tmp_tags, max_loss=0.0, max_profit=0.0):
    tags._TAGS_FILE = tmp_tags
    tmp_tags.write_text(json.dumps({"tags": []}), encoding="utf-8")
    tags.upsert_tag({"tag": "grp", "max_loss": float(max_loss), "max_profit": float(max_profit)})


def _set_user(tmp_users, max_loss=None, max_profit=None):
    users._USERS_FILE = tmp_users
    rec = {"user_id": "_default", "alias": "Default", "multiplier": 1.0,
           "allowed_instruments": None}
    if max_loss is not None:
        rec["max_loss"] = float(max_loss)
    if max_profit is not None:
        rec["max_profit"] = float(max_profit)
    tmp_users.write_text(json.dumps({"users": [rec]}), encoding="utf-8")


def _check(label, tight, huge):
    """Both portfolios must (a) run free under HUGE and (b) be cut/changed under TIGHT."""
    ok = True
    for nm in sorted(set(tight) | set(huge)):
        t, h = tight.get(nm), huge.get(nm)
        changed = t != h
        cut = t is not None and h is not None and t[0] <= h[0]
        flag = "OK" if (changed and cut) else "BAD"
        if not (changed and cut):
            ok = False
        print(f"    [{flag}] {nm}: tight(capped)={t}  huge(free)={h}")
    print(f"  [{'PASS' if ok else 'FAIL'}] {label}: whole group "
          f"{'force-squared live on the tier breach' if ok else 'NOT uniformly squared'}")
    return ok


def main():
    base = json.loads(Path(BASE).read_text(encoding="utf-8"))
    _orig_tags_file, _orig_users_file = tags._TAGS_FILE, users._USERS_FILE
    tmp = Path(tempfile.mkdtemp(prefix="tiercaps_"))
    ok_all = True
    try:
        # ── TAG tier ──────────────────────────────────────────────────────────
        A = _mk(base, "PFA", "grp")
        B = _mk(base, "PFB", "grp")
        tmp_tags = tmp / "tags.json"
        _set_tag(tmp_tags, max_loss=30.0)        # tight loss → breaches almost immediately
        tag_tight = _run([A, B], "_default")
        _set_tag(tmp_tags, max_loss=1e12, max_profit=1e12)   # huge → never breaches
        tag_huge = _run([A, B], "_default")
        print("TAG TIER -Combined-Loss cap shared by PFA+PFB:")
        ok_tag = _check("tag Combined-Loss", tag_tight, tag_huge)
        ok_all &= ok_tag
        # Profit side: a tight Max-Profit squares the group when combined profit hits it.
        _set_tag(tmp_tags, max_profit=30.0)
        tag_tgt_tight = _run([A, B], "_default")
        print("TAG TIER -Combined-Profit cap shared by PFA+PFB:")
        ok_tag_tgt = _check("tag Combined-Profit", tag_tgt_tight, tag_huge)
        ok_all &= ok_tag_tgt
        tags._TAGS_FILE = _orig_tags_file   # detach before the user phase

        # ── USER tier (no tag; the user cap spans ALL the user's portfolios) ───
        AU = _mk(base, "PFA", None)
        BU = _mk(base, "PFB", None)
        tmp_users = tmp / "users.json"
        _set_user(tmp_users, max_loss=30.0)          # tight loss
        usr_tight = _run([AU, BU], "_default")
        _set_user(tmp_users, max_loss=1e12, max_profit=1e12)   # huge
        usr_huge = _run([AU, BU], "_default")
        print("USER TIER -Max-Loss spanning PFA+PFB:")
        ok_usr = _check("user Max-Loss", usr_tight, usr_huge)
        ok_all &= ok_usr
        # Profit side: a tight Max-Profit squares all the user's portfolios.
        _set_user(tmp_users, max_profit=30.0)
        usr_tgt_tight = _run([AU, BU], "_default")
        print("USER TIER -Max-Profit spanning PFA+PFB:")
        ok_usr_tgt = _check("user Max-Profit", usr_tgt_tight, usr_huge)
        ok_all &= ok_usr_tgt

        # ── No-double-clip sanity: with the live tier on, the post-run tier clip is
        # suppressed (live_session_caps), so each capped portfolio must show a SINGLE
        # sane clip — kept >=1 trade and not truncated below its free-run count. A
        # double-clip (live square PLUS the legacy post-run clip on the prefix offset)
        # would over-truncate; over-truncation to 0 trades on a breaching cap is its
        # tell-tale. ──
        def _sane(capped, free):
            return all(c[0] >= 1 and c[0] <= free[k][0]
                       for k, c in capped.items() if k in free)
        nodbl = _sane(tag_tight, tag_huge) and _sane(usr_tight, usr_huge)
        print(f"  [{'PASS' if nodbl else 'FAIL'}] no double-clip: every capped "
              f"portfolio kept a single sane clip (1 <= capped <= free); "
              f"tag={tag_tight} user={usr_tight}")
        ok_all &= nodbl
    finally:
        tags._TAGS_FILE, users._USERS_FILE = _orig_tags_file, _orig_users_file

    print("TIER_CAPS_OK" if ok_all else "TIER_CAPS_FAIL")
    sys.exit(0 if ok_all else 1)


if __name__ == "__main__":
    main()
