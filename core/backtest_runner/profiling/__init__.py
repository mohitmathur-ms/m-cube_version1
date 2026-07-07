"""Phase wall-time profiling context manager and memoized strategy-config
introspection helpers (extra_bar_types / aggregate_to_bar_type support)."""

from __future__ import annotations

import contextlib
import time as _time_mod


@contextlib.contextmanager
def _phase(label: str, bag: dict | None):
    """Record phase wall-time into ``bag[label]`` when profiling is active.

    No-op when ``bag is None``; callers pass ``None`` in the hot path so
    non-profiling runs pay only the cost of a context-manager enter/exit.
    """
    if bag is None:
        yield
        return
    t0 = _time_mod.perf_counter()
    try:
        yield
    finally:
        bag[label] = bag.get(label, 0.0) + (_time_mod.perf_counter() - t0)


def _config_supports_extra_bar_types(config_class) -> bool:
    """Cheap memoized check for whether a strategy config accepts extra_bar_types.

    Walks the MRO once per class, stashes the result on the class itself so
    repeated slot runs with the same config class skip the MRO walk entirely.
    """
    cached = config_class.__dict__.get("_supports_extra_bar_types")
    if cached is not None:
        return cached
    for cls in reversed(config_class.__mro__):
        if "extra_bar_types" in getattr(cls, "__annotations__", {}):
            config_class._supports_extra_bar_types = True
            return True
    config_class._supports_extra_bar_types = False
    return False


def _config_supports_aggregate_to(config_class) -> bool:
    """Memoized check for whether a strategy config accepts aggregate_to_bar_type."""
    cached = config_class.__dict__.get("_supports_aggregate_to")
    if cached is not None:
        return cached
    for cls in reversed(config_class.__mro__):
        if "aggregate_to_bar_type" in getattr(cls, "__annotations__", {}):
            config_class._supports_aggregate_to = True
            return True
    config_class._supports_aggregate_to = False
    return False
