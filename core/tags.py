"""
Portfolio Tag registry + cross-portfolio PnL aggregation (spec §11).

A *tag* (a.k.a. "Strategy Tag") groups several portfolios so an SL/Target can
be assigned to a SUBSET of strategies — a tier that sits BETWEEN the portfolio
and user levels in the evaluation hierarchy (spec execution_logic.html §11):

    leg  →  portfolio  →  TAG  →  user

When a tag's SL/Target fires, every portfolio carrying that tag is squared off
(via the post-hoc equity-curve clip), while portfolios in other tags keep
running. The user-level cap remains the ultimate ceiling.

This module is the direct analogue of ``core.users`` one level down: tag limit
definitions live in ``config/tags.json`` and a module-level aggregator
accumulates each tag's combined PnL across the portfolios run in a session, so
the runner can clip the moment a tag's combined PnL breaches its limit.

Tag limits are absolute amounts in the reporting currency (same units as the
portfolio- and user-level caps), so they are directly comparable for the
hierarchical validation (portfolio SL ≤ tag SL ≤ user SL).
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_TAGS_FILE = _PROJECT_ROOT / "config" / "tags.json"

# Same conservative slug rule as user_id — a tag name may surface in logs and
# is matched verbatim; keep it free of shell metas / whitespace.
_TAG_RE = re.compile(r"^[A-Za-z0-9_-]{1,48}$")


def validate_tag(tag: Optional[str]) -> bool:
    """True iff ``tag`` is a safe slug we'll accept as a tag identifier."""
    return bool(tag) and bool(_TAG_RE.match(tag))


def _seed_if_missing() -> None:
    """Ensure ``config/tags.json`` exists (empty registry). Idempotent."""
    if _TAGS_FILE.exists():
        return
    _TAGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _TAGS_FILE.write_text(
        json.dumps(
            {
                "_meta": {
                    "description": (
                        "Portfolio Tag registry (spec §11). Each tag groups "
                        "portfolios that share a risk cap between the portfolio "
                        "and user levels. Limits are absolute amounts in the "
                        "reporting currency."
                    ),
                    "schema_version": 1,
                },
                "tags": [],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def load_tags() -> dict:
    """Read the full tag registry dict from disk (seeds an empty one if absent)."""
    _seed_if_missing()
    try:
        return json.loads(_TAGS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"tags": []}


def save_tags(registry: dict) -> None:
    """Replace the on-disk tag registry. Caller is responsible for validation."""
    _TAGS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _TAGS_FILE.write_text(json.dumps(registry, indent=2), encoding="utf-8")


def list_tags() -> list[dict]:
    """Return all tag definitions (validated names only)."""
    out = []
    for t in load_tags().get("tags") or []:
        name = t.get("tag")
        if name and validate_tag(name):
            out.append(t)
    return out


def get_tag(tag: Optional[str]) -> Optional[dict]:
    """Return the full tag definition dict, or None when unknown."""
    if not validate_tag(tag):
        return None
    for t in load_tags().get("tags") or []:
        if t.get("tag") == tag:
            return t
    return None


def upsert_tag(tag_def: dict) -> dict:
    """Insert or replace a tag definition by name. Returns the saved registry."""
    name = tag_def.get("tag")
    if not validate_tag(name):
        raise ValueError(f"invalid tag name: {name!r}")
    reg = load_tags()
    tags = [t for t in (reg.get("tags") or []) if t.get("tag") != name]
    tags.append(tag_def)
    reg["tags"] = tags
    save_tags(reg)
    return reg


def delete_tag(tag: str) -> bool:
    """Remove a tag definition. Returns True when something was removed."""
    reg = load_tags()
    before = reg.get("tags") or []
    after = [t for t in before if t.get("tag") != tag]
    if len(after) == len(before):
        return False
    reg["tags"] = after
    save_tags(reg)
    return True


def _abs_or_none(raw) -> Optional[float]:
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return abs(v) if v else None


def get_tag_max_loss(tag: Optional[str]) -> Optional[float]:
    """Tag-level Max Loss cap (absolute, reporting ccy). None = no cap."""
    t = get_tag(tag)
    return _abs_or_none(t.get("max_loss")) if t else None


def get_tag_max_profit(tag: Optional[str]) -> Optional[float]:
    """Tag-level Max Profit cap (absolute, reporting ccy). None = no cap."""
    t = get_tag(tag)
    return _abs_or_none(t.get("max_profit")) if t else None


def get_tag_trailing_sl(tag: Optional[str]) -> Optional[dict]:
    """Tag-level Trailing SL config, or None. Mirrors get_user_trailing_sl."""
    t = get_tag(tag)
    if not t or not t.get("trailing_sl_enabled"):
        return None
    try:
        every = float(t.get("trailing_sl_every") or 0.0)
        by = float(t.get("trailing_sl_by") or 0.0)
    except (TypeError, ValueError):
        return None
    if every <= 0 or by <= 0:
        return None
    return {"every": every, "by": by}


def get_tag_trailing_target(tag: Optional[str]) -> Optional[dict]:
    """Tag-level Trailing Target / Profit-Lock config, or None.

    Mirrors get_user_trailing_target — returns
    ``{"when_reach", "lock", "every", "by"}`` when enabled with a positive
    activation threshold.
    """
    t = get_tag(tag)
    if not t or not t.get("trailing_tgt_enabled"):
        return None
    try:
        when_reach = float(t.get("trailing_tgt_when_reach") or 0.0)
        lock = float(t.get("trailing_tgt_lock") or 0.0)
        every = float(t.get("trailing_tgt_every") or 0.0)
        by = float(t.get("trailing_tgt_by") or 0.0)
    except (TypeError, ValueError):
        return None
    if when_reach <= 0:
        return None
    return {"when_reach": when_reach, "lock": lock, "every": every, "by": by}


# Cumulative PnL aggregator per tag across the portfolios run in a session.
# Direct analogue of users._USER_PNL_AGGREGATOR: the runner rolls each
# portfolio's final PnL into its tag's bucket so a later portfolio sharing the
# tag sees the accrued combined PnL and the tag clip fires at the right point.
_TAG_PNL_AGGREGATOR: dict[str, float] = {}


def get_tag_cumulative_pnl(tag: Optional[str]) -> float:
    """Cumulative PnL across portfolios sharing this tag, 0.0 if unknown."""
    if not tag:
        return 0.0
    return _TAG_PNL_AGGREGATOR.get(tag, 0.0)


def add_tag_pnl(tag: Optional[str], pnl_delta: float) -> float:
    """Add a portfolio's final PnL to its tag's cumulative aggregate."""
    if not tag:
        return 0.0
    cur = _TAG_PNL_AGGREGATOR.get(tag, 0.0)
    new = cur + float(pnl_delta or 0.0)
    _TAG_PNL_AGGREGATOR[tag] = new
    return new


def reset_tag_pnl(tag: Optional[str] = None) -> None:
    """Reset the per-tag PnL aggregator. None clears all tags."""
    if tag is None:
        _TAG_PNL_AGGREGATOR.clear()
    else:
        _TAG_PNL_AGGREGATOR.pop(tag, None)
