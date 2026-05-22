"""Scanner for the single-stream daily index layout
(<root>/YYYY/MM/DD.MM.YYYY_complete_df_OHLCV.csv)."""

from __future__ import annotations

import re
import time
from pathlib import Path

from core.csv_loader.constants import _DAILY_INDEX_PATTERN, _MONTH_ABBR
from core.csv_loader.scan_cache import (
    _INDEX_SCAN_CACHE,
    _INDEX_SCAN_CACHE_LOCK,
    _INDEX_SCAN_CACHE_TTL_SECONDS,
)


def _index_symbol_from_root(root: Path) -> str:
    """Derive a Nautilus-friendly symbol from the root folder name.

    Uppercase, keep only ``[A-Z0-9_]``, collapse runs of disallowed characters
    into a single underscore, and trim leading/trailing underscores. So
    ``D:\\complete_df_yyyy`` → ``COMPLETE_DF_YYYY``; ``D:\\NIFTY50`` → ``NIFTY50``.
    Falls back to ``"INDEX"`` if the result would otherwise be empty.
    """
    cleaned = re.sub(r"[^A-Z0-9]+", "_", root.name.upper()).strip("_")
    return cleaned or "INDEX"


def _scan_index_daily_layout(root: Path) -> list[dict]:
    """Aggregate single-stream daily index CSVs into one entry.

    Layout: ``<root>/YYYY/MM/DD.MM.YYYY_complete_df_OHLCV.csv`` — three-deep,
    no symbol directory, no ASK/BID split. Files are collected into a single
    ``files`` list on the emitted entry; the downstream
    :func:`core.nautilus_loader.load_csv_and_store` already handles this shape
    via its ``files``-list branch (it falls through to ``price_type="LAST"``
    when ``side`` is not ASK/BID/MID, matching the ``expose_sides: ["LAST"]``
    convention in ``adapter_admin/data_formats/index.json``).

    Symbol is derived from the root folder name via
    :func:`_index_symbol_from_root` — rename the data folder to get a cleaner
    symbol if needed.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _INDEX_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _INDEX_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    # (date_tuple, path_str) tuples so we can sort chronologically by the
    # date encoded in the filename — independent of filesystem walk order.
    dated: list[tuple[tuple[int, int, int], str]] = []
    for csv_file in root.rglob("*.csv"):
        match = _DAILY_INDEX_PATTERN.match(csv_file.name)
        if not match:
            continue
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        # Expect <root>/YYYY/MM/<file> — exactly two parent dirs above the file.
        if len(rel_parts) != 3:
            continue
        dd, mm, yyyy = match.group(1), match.group(2), match.group(3)
        try:
            date_key = (int(yyyy), int(mm), int(dd))
        except ValueError:
            continue
        dated.append((date_key, str(csv_file)))

    if not dated:
        with _INDEX_SCAN_CACHE_LOCK:
            _INDEX_SCAN_CACHE[key] = (now, root_mtime, [])
        return []

    dated.sort(key=lambda t: t[0])
    files = [path for _, path in dated]
    first_date, last_date = dated[0][0], dated[-1][0]

    symbol = _index_symbol_from_root(root)

    def _ddmonyyyy(d: tuple[int, int, int]) -> str:
        yyyy, mm, dd = d
        return f"{dd:02d}{_MONTH_ABBR[mm - 1]}{yyyy:04d}"

    # Shape the synthetic filename like the consolidated FX/commodity naming so
    # the index.json filename_pattern (and the FX-style UI filter) still accept
    # it. The actual files are loaded via the `files` list, not this path.
    synth_filename = (
        f"{symbol}_{_ddmonyyyy(first_date)}_{_ddmonyyyy(last_date)}_LAST_OHLCV.csv"
    )

    entry = {
        "path": files[0],            # nominal — loader uses 'files' list
        "filename": synth_filename,
        "id": -1,                    # negative, like other scanners
        "symbol": symbol,
        "name": f"{symbol} ({len(files):,} daily files)",
        "files": files,
    }
    entries = [entry]

    with _INDEX_SCAN_CACHE_LOCK:
        _INDEX_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries
