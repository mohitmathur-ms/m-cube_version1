"""Persistent runtime history for LPT scheduling.

Stores per-(bar_type, strategy) average wall time per day of data so
subsequent scheduling passes can estimate more accurately than the
span-only heuristic. Falls back to span when no history exists.

Stored at <project_root>/.runtime_history.json — safe to delete anytime.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from threading import Lock

_PATH = Path(__file__).resolve().parent.parent / ".runtime_history.json"
_LOCK = Lock()
_MAX_SAMPLES = 20
_EMA_ALPHA = 0.3  # weight of new observation vs. running average


def _key(bar_type: str, strategy: str) -> str:
    return f"{bar_type}|{strategy}"


def load() -> dict:
    if not _PATH.exists():
        return {}
    try:
        return json.loads(_PATH.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save(data: dict) -> None:
    with _LOCK:
        _PATH.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")


def record(data: dict, bar_type: str, strategy: str, elapsed: float, span_days: int) -> None:
    """Update the in-memory history dict. Caller persists via save()."""
    if span_days <= 0:
        span_days = 1
    per_day = elapsed / span_days
    k = _key(bar_type, strategy)
    entry = data.get(k) or {"samples": [], "per_day": per_day}
    prev = entry.get("per_day", per_day)
    # Exponential moving average smooths one-off outliers (cold cache, swap, etc.)
    entry["per_day"] = round(_EMA_ALPHA * per_day + (1 - _EMA_ALPHA) * prev, 6)
    samples = entry.get("samples", [])
    samples.append({
        "elapsed": round(elapsed, 3),
        "span_days": int(span_days),
        "per_day": round(per_day, 6),
        "ts": int(time.time()),
    })
    entry["samples"] = samples[-_MAX_SAMPLES:]
    entry["last_updated"] = int(time.time())
    data[k] = entry


def estimate(data: dict, bar_type: str, strategy: str, span_days: int):
    """Return a duration estimate in seconds, or None if no history."""
    k = _key(bar_type, strategy)
    if k in data and data[k].get("per_day"):
        return float(data[k]["per_day"]) * max(1, span_days)
    return None
