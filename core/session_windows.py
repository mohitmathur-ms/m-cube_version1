"""Recompute every venue's daily session window directly from the catalog.

The per-instrument ingest hook in :mod:`core.nautilus_loader` only updates a
venue's session window when data is loaded *through the app*. When parquet files
are dropped into the catalog out-of-band (e.g. copied from another system), that
hook never fires. This module closes that gap: it scans the catalog's bar
directories, derives each venue's UTC time-of-day window, and writes it back to
``adapter_admin/adapters_config/<venue>.json``. The server calls it on startup
and on the catalog-status request the dashboard issues on every page load, so
the configs always reflect whatever is currently in the catalog.

Window derivation matches :func:`core.csv_loader.session_window_from_df`: the
earliest and latest *time-of-day* (``ts_event mod 1 day``) across all bars of a
venue, unioned across every bar type and side on that venue.

Performance: bar timestamps for a 1-minute FX catalog run to tens of millions of
rows, so re-reading them on every page refresh would be wasteful. Each parquet
file's time-of-day extremes are cached keyed by ``(mtime, size)`` — a refresh
only reads files that are new or have changed, which is exactly the
drop-in-new-data case. Unchanged catalogs cost one ``stat`` per file.
"""

from __future__ import annotations

import logging
import threading
from pathlib import Path

import numpy as np
import pyarrow.dataset as pads

from core.venue_config import update_venue_session_window, venue_from_bar_type

logger = logging.getLogger(__name__)

_NS_PER_DAY = 86_400_000_000_000

# Per-file cache: str(path) -> (mtime, size, tod_min_ns, tod_max_ns).
# Parquet files in the catalog are immutable once written, so (mtime, size)
# is a sufficient validity key — a changed file (or a new one) misses and is
# re-read; everything else is a dict hit.
_FILE_TOD_CACHE: dict[str, tuple[float, int, int, int]] = {}
_CACHE_LOCK = threading.Lock()


def _fmt_tod(ns: int) -> str:
    """Nanoseconds-since-midnight -> zero-padded ``"HH:MM:SS"``."""
    secs = int(ns) // 1_000_000_000
    return f"{secs // 3600:02d}:{(secs % 3600) // 60:02d}:{secs % 60:02d}"


def _file_tod_extremes(path: Path) -> tuple[int, int] | None:
    """Return ``(tod_min_ns, tod_max_ns)`` for one parquet file, cached.

    Reads only the ``ts_event`` column. Returns ``None`` for an empty or
    unreadable file (logged and skipped by the caller).
    """
    key = str(path)
    try:
        st = path.stat()
    except OSError:
        return None
    cached = _FILE_TOD_CACHE.get(key)
    if cached is not None and cached[0] == st.st_mtime and cached[1] == st.st_size:
        return cached[2], cached[3]

    try:
        col = pads.dataset(key, format="parquet").to_table(columns=["ts_event"])["ts_event"].to_numpy()
    except Exception as e:
        logger.warning("session_window: could not read %s: %s", path.name, e)
        return None
    if col.size == 0:
        return None

    tod = np.mod(col, _NS_PER_DAY)
    lo, hi = int(tod.min()), int(tod.max())
    with _CACHE_LOCK:
        _FILE_TOD_CACHE[key] = (st.st_mtime, st.st_size, lo, hi)
    return lo, hi


def refresh_all_venue_session_windows(
    catalog_path: str | Path = "./catalog",
    configs_dir: Path | str | None = None,
) -> dict[str, tuple[str, str]]:
    """Recompute and persist every venue's session window from the catalog.

    Scans ``<catalog_path>/data/bar/<bar_type>/*.parquet``, groups bar types by
    venue (parsed from the bar-type directory name), unions each venue's
    time-of-day extremes across all its files, and writes the result to the
    venue's adapter config with ``mode="set"`` (the scan is authoritative).

    Returns ``{venue: (session_start_time, session_end_time)}`` for every venue
    that had catalog data. Best-effort and non-raising: a missing catalog
    returns ``{}``; unreadable files are skipped.
    """
    bar_root = Path(catalog_path) / "data" / "bar"
    if not bar_root.is_dir():
        return {}

    # venue -> [min_ns, max_ns]
    venue_extremes: dict[str, list[int]] = {}
    for bar_dir in bar_root.iterdir():
        if not bar_dir.is_dir():
            continue
        venue = venue_from_bar_type(bar_dir.name)
        if not venue:
            continue
        for parquet in bar_dir.glob("*.parquet"):
            extremes = _file_tod_extremes(parquet)
            if extremes is None:
                continue
            lo, hi = extremes
            cur = venue_extremes.get(venue)
            if cur is None:
                venue_extremes[venue] = [lo, hi]
            else:
                cur[0] = min(cur[0], lo)
                cur[1] = max(cur[1], hi)

    result: dict[str, tuple[str, str]] = {}
    for venue, (lo, hi) in venue_extremes.items():
        start, end = _fmt_tod(lo), _fmt_tod(hi)
        update_venue_session_window(venue, start, end, configs_dir, mode="set")
        result[venue] = (start, end)
    return result
