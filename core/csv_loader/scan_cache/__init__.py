"""Module-level scan caches for the daily-layout scanners.

Each daily-layout scanner (FX, commodity, index, crypto) caches its result here
keyed by ``str(root)``. Value shape: ``(cached_at, root_mtime, entries)``.

The fast-path key is the **root directory mtime**: any new pair dir or rename
under the root bumps it (Windows + Linux both expose this). When mtime is
unchanged the cache is reused regardless of how long ago the scan ran — that
eliminates the periodic 2-5s UI hangs the previous 60s TTL caused on big trees.
The longer TTL is only a safety net for new daily files dropped into existing
YYYY/MM/DD dirs, where the root mtime stays constant; users who need an
immediate refresh can call :func:`clear_fx_scan_cache`.

Scanners mutate these dict objects in place (never rebind), so importing the
same object reference from this module shares state across the package.
"""

from __future__ import annotations

import threading

# Cache for :func:`_scan_fx_daily_layout`. ``rglob`` over the FX tree
# (thousands of daily files under <PAIR>/YYYY/MM/DD/) takes 2–5s; the same scan
# is hit on every UI page load and view-data refresh.
_FX_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_FX_SCAN_CACHE_TTL_SECONDS = 600.0
_FX_SCAN_CACHE_LOCK = threading.Lock()

# Parallel cache for :func:`_scan_commodity_daily_layout`. Same mtime + TTL
# safety-net shape as the FX cache above; see that block for the rationale.
_COMMODITY_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_COMMODITY_SCAN_CACHE_TTL_SECONDS = 600.0
_COMMODITY_SCAN_CACHE_LOCK = threading.Lock()

# Parallel cache for :func:`_scan_index_daily_layout`. Same mtime + TTL
# safety-net shape as the FX cache above; see that block for the rationale.
_INDEX_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_INDEX_SCAN_CACHE_TTL_SECONDS = 600.0
_INDEX_SCAN_CACHE_LOCK = threading.Lock()

# Parallel cache for :func:`_scan_crypto_nested_layout`. Same mtime + TTL
# safety-net shape as the FX cache above; see that block for the rationale.
_CRYPTO_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_CRYPTO_SCAN_CACHE_TTL_SECONDS = 600.0
_CRYPTO_SCAN_CACHE_LOCK = threading.Lock()


def clear_fx_scan_cache() -> None:
    """Drop the daily-layout scan caches (FX + commodity + index + crypto). Call
    after manually adding new pair / commodity / index / crypto directories if
    you want the next ``scan_csv_folder`` to see them immediately rather than
    waiting for the TTL to expire."""
    with _FX_SCAN_CACHE_LOCK:
        _FX_SCAN_CACHE.clear()
    with _COMMODITY_SCAN_CACHE_LOCK:
        _COMMODITY_SCAN_CACHE.clear()
    with _INDEX_SCAN_CACHE_LOCK:
        _INDEX_SCAN_CACHE.clear()
    with _CRYPTO_SCAN_CACHE_LOCK:
        _CRYPTO_SCAN_CACHE.clear()
