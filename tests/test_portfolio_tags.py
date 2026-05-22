"""Unit tests for Portfolio Tags (spec execution_logic.html §11).

Covers:
  * core/tags.py — registry CRUD, limit accessors, cross-portfolio PnL aggregator
  * tag-scoped equity-curve clips (_user_sl_clip / _user_tgt_clip with scope_label="TAG")
  * server hierarchical limit validation (portfolio SL ≤ tag SL ≤ user SL)

The tag tier is a faithful clone of the user tier one level down, so the clip
maths reuse the user-level functions; these tests pin the TAG-scoped behaviour
and the registry/aggregator helpers.
"""

import core.tags as tags
from core.backtest_runner import _user_sl_clip, _user_tgt_clip
from core.models import PortfolioConfig


def _curve(*balances, seed=10_000.0):
    out = [{"timestamp": None, "balance": seed}]
    for i, b in enumerate(balances, start=1):
        out.append({"timestamp": f"2024-01-01T00:{i:02d}:00+00:00", "balance": b})
    return out


# ── tags.py registry + aggregator ───────────────────────────────────────────

def test_tag_aggregator_accumulates_and_resets():
    tags.reset_tag_pnl()
    assert tags.get_tag_cumulative_pnl("t1") == 0.0
    assert tags.add_tag_pnl("t1", -500.0) == -500.0
    assert tags.add_tag_pnl("t1", -250.0) == -750.0
    assert tags.get_tag_cumulative_pnl("t1") == -750.0
    # Other tags are independent.
    assert tags.get_tag_cumulative_pnl("t2") == 0.0
    tags.reset_tag_pnl("t1")
    assert tags.get_tag_cumulative_pnl("t1") == 0.0


def test_tag_aggregator_ignores_empty_tag():
    tags.reset_tag_pnl()
    assert tags.add_tag_pnl(None, 100.0) == 0.0
    assert tags.add_tag_pnl("", 100.0) == 0.0


def test_validate_tag_slug():
    assert tags.validate_tag("non-trending")
    assert tags.validate_tag("Tag_1")
    assert not tags.validate_tag("")
    assert not tags.validate_tag(None)
    assert not tags.validate_tag("bad name")   # space
    assert not tags.validate_tag("a" * 49)     # too long


def test_tag_limit_accessors_roundtrip(tmp_path, monkeypatch):
    # Point the registry at a temp file so the suite doesn't touch config/tags.json.
    monkeypatch.setattr(tags, "_TAGS_FILE", tmp_path / "tags.json")
    tags.upsert_tag({
        "tag": "scalp", "max_loss": 30000, "max_profit": 50000,
        "trailing_sl_enabled": True, "trailing_sl_every": 1000, "trailing_sl_by": 200,
        "trailing_tgt_enabled": True, "trailing_tgt_when_reach": 5000,
        "trailing_tgt_lock": 2000, "trailing_tgt_every": 1000, "trailing_tgt_by": 500,
    })
    assert tags.get_tag_max_loss("scalp") == 30000
    assert tags.get_tag_max_profit("scalp") == 50000
    assert tags.get_tag_trailing_sl("scalp") == {"every": 1000.0, "by": 200.0}
    tt = tags.get_tag_trailing_target("scalp")
    assert tt["when_reach"] == 5000 and tt["lock"] == 2000
    # Unknown tag → no caps.
    assert tags.get_tag_max_loss("nope") is None
    assert tags.get_tag_trailing_sl("nope") is None
    # Delete removes it.
    assert tags.delete_tag("scalp") is True
    assert tags.get_tag("scalp") is None
    assert tags.delete_tag("scalp") is False


# ── tag-scoped clips (scope_label="TAG") ────────────────────────────────────

def test_tag_sl_clip_fires_with_tag_reason():
    # PnL goes 0 → -200 → -600; tag Max Loss 500 → fires at tick 2 (-600).
    clip = _user_sl_clip(
        _curve(9_800, 9_400), 10_000.0, 0.0,
        eff_max_loss=500.0, user_trail_sl=None, all_slot_ids=["s1", "s2"],
        scope_label="TAG",
    )
    assert clip.clip_reason == "TAG_STOPLOSS"
    assert clip.clip_action == "SqOff"
    assert set(clip.clipped_slots) == {"s1", "s2"}


def test_tag_sl_clip_uses_cumulative_tag_pnl():
    # This portfolio's PnL only reaches -300, but the tag already carries -400
    # from earlier portfolios → combined -700 breaches a 500 cap.
    clip = _user_sl_clip(
        _curve(9_900, 9_700), 10_000.0, cum_user_pnl=-400.0,
        eff_max_loss=500.0, user_trail_sl=None, all_slot_ids=["s1"],
        scope_label="TAG",
    )
    assert clip.clip_reason == "TAG_STOPLOSS"


def test_tag_tgt_clip_fires_with_tag_reason():
    clip = _user_tgt_clip(
        _curve(10_200, 10_600, 11_200), 10_000.0, 0.0,
        eff_max_profit=1000.0, user_trail_tgt=None, all_slot_ids=["s1"],
        scope_label="TAG",
    )
    assert clip.clip_reason == "TAG_TARGET"


def test_user_scope_label_default_unchanged():
    # Omitting scope_label must keep the legacy USER reasons (back-compat).
    clip = _user_sl_clip(
        _curve(9_400), 10_000.0, 0.0,
        eff_max_loss=500.0, user_trail_sl=None, all_slot_ids=["s1"],
    )
    assert clip.clip_reason == "USER_STOPLOSS"


# ── server hierarchical validation (portfolio ≤ tag ≤ user) ──────────────────

def test_hierarchy_blocks_portfolio_exceeding_tag(tmp_path, monkeypatch):
    import server
    monkeypatch.setattr(tags, "_TAGS_FILE", tmp_path / "tags.json")
    tags.upsert_tag({"tag": "grp", "max_loss": 30000, "max_profit": 50000})
    # Portfolio SL 40000 > tag Max Loss 30000 → blocked.
    pf = PortfolioConfig(portfolio_tag="grp", pf_sl_enabled=True, pf_sl_value=40000)
    ok, err = server._validate_portfolio_hierarchy(pf, "_default")
    assert not ok and "tag" in err.lower()
    # Within the cap → ok.
    pf2 = PortfolioConfig(portfolio_tag="grp", pf_sl_enabled=True, pf_sl_value=20000)
    ok2, _ = server._validate_portfolio_hierarchy(pf2, "_default")
    assert ok2


def test_hierarchy_ok_when_no_tag(tmp_path, monkeypatch):
    import server
    monkeypatch.setattr(tags, "_TAGS_FILE", tmp_path / "tags.json")
    pf = PortfolioConfig(pf_sl_enabled=True, pf_sl_value=999999)
    ok, _ = server._validate_portfolio_hierarchy(pf, "_default")
    assert ok
