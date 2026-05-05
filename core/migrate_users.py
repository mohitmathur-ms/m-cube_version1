"""
One-shot migration: move flat ``portfolios/*.json`` under ``portfolios/_default/``.

Run on server startup. Idempotent — safe to call repeatedly. The new
multi-user state layout partitions portfolios by ``user_id``; legacy
single-user installations are mapped onto the reserved ``_default``
user_id so existing files stay reachable without manual admin setup.

Mirrors the partitioning pattern: ``reports/`` follows the same scheme but
that one's handled inside server.py at write time (not migrated here —
historical reports are an artifact, not a config the user edits).
"""

from __future__ import annotations

import logging
import shutil
from pathlib import Path

_LOG = logging.getLogger(__name__)

DEFAULT_USER_ID = "_default"


def migrate_portfolios(portfolios_dir: Path | str) -> dict:
    """Move root-level ``*.json`` files into ``<portfolios_dir>/_default/``.

    Returns a small report dict (``{"moved": int, "already_partitioned": bool}``)
    so callers can log a single line on startup. No-op when the directory
    already looks partitioned (only subdirectories) or doesn't exist.
    """
    base = Path(portfolios_dir)
    if not base.exists():
        return {"moved": 0, "already_partitioned": True}

    flat_jsons = [p for p in base.glob("*.json") if p.is_file()]
    if not flat_jsons:
        # Already partitioned (or empty). Either way, nothing to do.
        return {"moved": 0, "already_partitioned": True}

    target = base / DEFAULT_USER_ID
    target.mkdir(parents=True, exist_ok=True)

    moved = 0
    for src in flat_jsons:
        dst = target / src.name
        if dst.exists():
            # A file with the same name already exists under _default —
            # don't clobber. Append a suffix so the legacy file is preserved
            # but isn't picked up automatically.
            dst = target / f"{src.stem}.legacy{src.suffix}"
        shutil.move(str(src), str(dst))
        moved += 1

    if moved:
        _LOG.info(
            "Migrated %d legacy portfolio file(s) to %s",
            moved, target,
        )

    return {"moved": moved, "already_partitioned": False}
