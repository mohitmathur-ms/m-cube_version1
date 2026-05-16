"""
User registry: identity-only multi-user layer (no auth/security).

The X-User-Id header on every API call must match a ``user_id`` listed in
``config/users.json``. Any network caller can spoof headers — this layer
is only appropriate for trusted internal teams. The registry also carries
each user's ``multiplier`` (per-user trade-size scalar) and
``allowed_instruments`` (whitelist of bare symbols, or None for all).

The "_default" user_id is reserved for legacy portfolios migrated from the
flat ``portfolios/*.json`` layout — see ``core.migrate_users``.
"""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
_USERS_FILE = _PROJECT_ROOT / "config" / "users.json"

# URL-safe slug. Used as a path component (portfolios/<user_id>/), so we keep
# it conservative — no slashes, no spaces, no shell metas.
_USER_ID_RE = re.compile(r"^[a-z0-9_-]{1,32}$")

# Returned to API callers when the registry is missing or empty so the rest
# of the system stays predictable.
_FALLBACK_DEFAULT_USER = {
    "user_id": "_default",
    "alias": "Default User",
    "multiplier": 1.0,
    "allowed_instruments": None,
}


def _seed_if_missing() -> None:
    """Ensure ``users.json`` exists with at least the ``_default`` user.

    Idempotent — only writes when the file is absent. Lets server startup
    proceed even on a fresh checkout without manual setup.
    """
    if _USERS_FILE.exists():
        return
    _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _USERS_FILE.write_text(
        json.dumps(
            {
                "_meta": {
                    "description": (
                        "Identity-only user registry. NO AUTH/SECURITY — "
                        "anyone on the network can spoof any header. "
                        "Use only in trusted environments."
                    ),
                    "schema_version": 1,
                },
                "users": [_FALLBACK_DEFAULT_USER],
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def validate_user_id(user_id: str) -> bool:
    """True iff ``user_id`` is a URL-safe slug we'll accept as a path component."""
    return bool(user_id) and bool(_USER_ID_RE.match(user_id))


def load_users() -> dict:
    """Read the full registry dict from disk.

    Returns the parsed JSON. Missing/corrupt file falls back to a minimal
    registry with just the ``_default`` user — callers can rely on at least
    that user always existing.
    """
    _seed_if_missing()
    try:
        return json.loads(_USERS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"users": [dict(_FALLBACK_DEFAULT_USER)]}


def save_users(registry: dict) -> None:
    """Replace the on-disk registry. Caller is responsible for validation."""
    _USERS_FILE.parent.mkdir(parents=True, exist_ok=True)
    _USERS_FILE.write_text(json.dumps(registry, indent=2), encoding="utf-8")


def list_users() -> list[dict]:
    """Public-safe user list — only ``user_id`` and ``alias``.

    Used by the frontend picker. Multipliers and allowlists are admin-only
    info; not returned here.
    """
    users = load_users().get("users") or []
    out = []
    for u in users:
        uid = u.get("user_id")
        if uid and validate_user_id(uid):
            out.append({"user_id": uid, "alias": u.get("alias") or uid})
    return out


def get_user(user_id: Optional[str]) -> Optional[dict]:
    """Return the full user dict for ``user_id``, or None.

    Callers should treat None as "unknown user → 401". Returns the raw dict
    from the registry (``user_id``, ``alias``, ``multiplier``,
    ``allowed_instruments``) so callers can read multiplier/allowlist
    without a second lookup.
    """
    if not user_id or not validate_user_id(user_id):
        return None
    for u in load_users().get("users") or []:
        if u.get("user_id") == user_id:
            return u
    return None


def get_multiplier(user_id: Optional[str]) -> float:
    """Resolve a user's multiplier with safe defaults.

    Unknown user, missing field, non-positive value → 1.0 (no-op). Used by
    ``effective_slot_qty`` to scale order quantity per-user before the
    admin cap is applied.
    """
    user = get_user(user_id)
    if not user:
        return 1.0
    raw = user.get("multiplier")
    try:
        m = float(raw) if raw is not None else 1.0
    except (TypeError, ValueError):
        return 1.0
    return m if m > 0 else 1.0


def get_user_max_loss(user_id: Optional[str]) -> Optional[float]:
    """User-level Max Loss cap (spec §3 Level 3). Absolute amount in base ccy.

    None = no cap. Used by the portfolio runner to short-circuit further
    trading when cumulative PnL across all of this user's portfolios reaches
    the limit. Stored as ``max_loss`` on the user record.
    """
    user = get_user(user_id)
    if not user:
        return None
    raw = user.get("max_loss")
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return abs(v) if v else None


def get_user_max_profit(user_id: Optional[str]) -> Optional[float]:
    """User-level Max Profit cap (spec §3 Level 3). Mirrors Max Loss."""
    user = get_user(user_id)
    if not user:
        return None
    raw = user.get("max_profit")
    if raw is None:
        return None
    try:
        v = float(raw)
    except (TypeError, ValueError):
        return None
    return abs(v) if v else None


# Cumulative PnL aggregator across all portfolios for a given user, used by
# the user-level Max Loss / Max Profit caps (spec §3). The orchestrator
# updates this after each portfolio finishes; the runner reads it at the
# start of the next run to decide whether to skip / clip early.
_USER_PNL_AGGREGATOR: dict[str, float] = {}


def get_user_cumulative_pnl(user_id: Optional[str]) -> float:
    """Return the cumulative PnL across all portfolios for this user, 0.0 if unknown."""
    if not user_id:
        return 0.0
    return _USER_PNL_AGGREGATOR.get(user_id, 0.0)


def add_user_pnl(user_id: Optional[str], pnl_delta: float) -> float:
    """Add a portfolio's final PnL to the user's cumulative aggregate.

    Returns the new total. Idempotent on (user_id, None) — no-op if user_id is empty.
    """
    if not user_id:
        return 0.0
    cur = _USER_PNL_AGGREGATOR.get(user_id, 0.0)
    new = cur + float(pnl_delta or 0.0)
    _USER_PNL_AGGREGATOR[user_id] = new
    return new


def reset_user_pnl(user_id: Optional[str] = None) -> None:
    """Reset the per-user PnL aggregator. None clears all users."""
    if user_id is None:
        _USER_PNL_AGGREGATOR.clear()
    else:
        _USER_PNL_AGGREGATOR.pop(user_id, None)


def get_allowed_instruments(user_id: Optional[str]) -> Optional[list[str]]:
    """Return the user's allowed-instruments whitelist, uppercased.

    None (or unknown user) means no restriction — callers should treat that
    as "all instruments allowed". A list means whitelist; bare symbols only
    (e.g. ``["EURUSD", "BTCUSD"]``), not full bar_type strings.
    """
    user = get_user(user_id)
    if not user:
        return None
    allowed = user.get("allowed_instruments")
    if allowed is None:
        return None
    if not isinstance(allowed, list):
        return None
    return [str(s).upper() for s in allowed if isinstance(s, str)]


def is_instrument_allowed(user_id: Optional[str], symbol: str) -> bool:
    """Check a single bare symbol (e.g. "EURUSD") against a user's allowlist.

    Anonymous/unknown user → False (deny by default). Known user with no
    allowlist → True. Known user with allowlist → membership test.
    """
    user = get_user(user_id)
    if not user:
        return False
    allowed = user.get("allowed_instruments")
    if allowed is None:
        return True
    if not isinstance(allowed, list):
        return True
    return symbol.upper() in {str(s).upper() for s in allowed if isinstance(s, str)}


def validate_registry_payload(payload: dict) -> tuple[bool, str]:
    """Validate a full registry replacement before save.

    Returns ``(ok, error_message)``. Used by the admin POST endpoint so the
    on-disk registry always satisfies our invariants (slug-safe ids, positive
    multipliers, no duplicates, well-formed allowlists).
    """
    if not isinstance(payload, dict):
        return False, "payload must be an object"
    users = payload.get("users")
    if not isinstance(users, list) or not users:
        return False, "'users' must be a non-empty list"
    seen_ids = set()
    for i, u in enumerate(users):
        if not isinstance(u, dict):
            return False, f"users[{i}] must be an object"
        uid = u.get("user_id")
        if not isinstance(uid, str) or not validate_user_id(uid):
            return False, (
                f"users[{i}].user_id must match [a-z0-9_-]{{1,32}} "
                f"(got {uid!r})"
            )
        if uid in seen_ids:
            return False, f"duplicate user_id: {uid}"
        seen_ids.add(uid)
        alias = u.get("alias")
        if alias is not None and not isinstance(alias, str):
            return False, f"users[{i}].alias must be a string or omitted"
        m = u.get("multiplier", 1.0)
        try:
            mf = float(m)
        except (TypeError, ValueError):
            return False, f"users[{i}].multiplier must be numeric"
        if mf <= 0:
            return False, f"users[{i}].multiplier must be > 0 (got {mf})"
        ai = u.get("allowed_instruments")
        if ai is not None and not isinstance(ai, list):
            return False, f"users[{i}].allowed_instruments must be a list or null"
        if isinstance(ai, list) and not all(isinstance(s, str) for s in ai):
            return False, f"users[{i}].allowed_instruments must be a list of strings"
        # User-level Max Loss / Max Profit (spec §3 Level 3). Both optional;
        # null/missing → no cap. Numeric & finite when present.
        for cap_field in ("max_loss", "max_profit"):
            cap_val = u.get(cap_field)
            if cap_val is None:
                continue
            try:
                cf = float(cap_val)
            except (TypeError, ValueError):
                return False, f"users[{i}].{cap_field} must be numeric or null"
            if cf != cf:  # NaN check
                return False, f"users[{i}].{cap_field} must be a finite number"
    return True, ""
