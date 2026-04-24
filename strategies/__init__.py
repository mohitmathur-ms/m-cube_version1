"""
Strategy registry - auto-discovers strategies from individual files in this package.
Each strategy file exports: STRATEGY_NAME, STRATEGY_CLASS, CONFIG_CLASS, DESCRIPTION, PARAMS.
"""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path


def _build_registry() -> dict:
    """Scan all modules in this package and build the STRATEGY_REGISTRY."""
    registry = {}
    package_dir = Path(__file__).parent

    for finder, module_name, is_pkg in pkgutil.iter_modules([str(package_dir)]):
        if module_name.startswith("_"):
            continue
        mod = importlib.import_module(f"strategies.{module_name}")

        name = getattr(mod, "STRATEGY_NAME", None)
        if name is None:
            continue

        registry[name] = {
            "strategy_class": mod.STRATEGY_CLASS,
            "config_class": mod.CONFIG_CLASS,
            "description": getattr(mod, "DESCRIPTION", ""),
            "params": getattr(mod, "PARAMS", {}),
        }

    return registry


STRATEGY_REGISTRY = _build_registry()
