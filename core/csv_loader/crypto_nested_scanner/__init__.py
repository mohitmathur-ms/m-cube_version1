"""Scanner for the nested crypto layout (<root>/<BASE-QUOTE>/YYYY/MM/<daily>.csv;
the scanned root is the venue folder)."""

from __future__ import annotations

import time
from pathlib import Path

from core.csv_loader.constants import (
    _CRYPTO_MONTH_DIR_PATTERN,
    _CRYPTO_YEAR_DIR_PATTERN,
)
from core.csv_loader.scan_cache import (
    _CRYPTO_SCAN_CACHE,
    _CRYPTO_SCAN_CACHE_LOCK,
    _CRYPTO_SCAN_CACHE_TTL_SECONDS,
)


def _scan_crypto_nested_layout(root: Path) -> list[dict]:
    """Aggregate nested crypto CSVs into one entry per (base, quote) pair.

    Layout: the scanned root **is** the venue folder, and below it sit
    ``<BASE-QUOTE>/YYYY/MM/<daily>.csv`` — four path components below the root
    (pair, year-dir, month-dir, file). The venue is taken from the root folder
    name (e.g. ``D:\\crypto_yyy\\binance`` → venue ``BINANCE``), so the user
    selects a single venue by choosing which folder to scan.

    Each daily file is a single OHLCV stream (no ASK/BID split). Unlike the FX /
    commodity scanners the date is *not* in the filename, so the daily filename
    is left unconstrained; the structural gate (a ``BASE-QUOTE`` pair dir, a
    4-digit year dir, then a 1-2 digit month dir) is what identifies the layout.

    Files are collected into a ``files`` list on the emitted entry, which the
    downstream :func:`core.nautilus_loader.load_csv_and_store` handles via its
    ``files``-list branch (falling through to ``price_type="LAST"`` since no
    ``side`` is set). The base/quote currencies are parsed from the pair dir
    name and carried as ``symbol`` + ``quote_currency``; the venue (root folder
    name) is carried as ``venue`` so the load endpoint applies it per-entry.

    Result is cached under :data:`_CRYPTO_SCAN_CACHE` with a TTL — repeated UI
    refreshes hit the cache instead of re-walking the daily-file tree.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _CRYPTO_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _CRYPTO_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    # Venue is the scanned folder itself, e.g. ".../binance" -> "BINANCE".
    venue_uc = root.name.upper()

    # (base, quote) -> list of (year, month, filename, path) for sorting.
    aggregated: dict[tuple[str, str], list[tuple[int, int, str, str]]] = {}
    for csv_file in root.rglob("*.csv"):
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) != 4:
            continue
        pair_dir, year_dir, month_dir, filename = rel_parts
        if "-" not in pair_dir:
            continue
        if not _CRYPTO_YEAR_DIR_PATTERN.match(year_dir):
            continue
        if not _CRYPTO_MONTH_DIR_PATTERN.match(month_dir):
            continue
        base, _, quote = pair_dir.upper().partition("-")
        if not base or not quote:
            continue
        aggregated.setdefault((base, quote), []).append(
            (int(year_dir), int(month_dir), filename, str(csv_file))
        )

    entries: list[dict] = []
    auto_id = 0
    for (base, quote) in sorted(aggregated):
        dated = sorted(aggregated[(base, quote)])
        files = [path for _, _, _, path in dated]
        auto_id -= 1
        entries.append({
            "path": files[0],   # nominal — loader uses the 'files' list
            # Synthetic filename shaped so the cryptocurrency.json
            # filename_pattern (and the UI filter) accepts it. Actual files
            # are loaded via the 'files' list, not this path.
            "filename": f"{base}-{quote}_{venue_uc}_LAST_OHLCV.csv",
            "id": auto_id,
            "symbol": base,
            "quote_currency": quote,
            "venue": venue_uc,
            "name": f"{base}/{quote} ({venue_uc}, {len(files):,} daily files)",
            "files": files,
        })

    with _CRYPTO_SCAN_CACHE_LOCK:
        _CRYPTO_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries
