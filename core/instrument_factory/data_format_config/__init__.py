"""Per-asset-class data-format config reading.

Resolves a bar type's venue → ``asset_class`` →
``adapter_admin/data_formats/<asset_class>.json`` and returns its ``instrument``
sub-block (``price_precision``, ``size_precision``, ``quote_currency``,
``base_currency_length``, ...).
"""

from __future__ import annotations

import json
from pathlib import Path

from core.venue_config import load_adapter_config_for_bar_type

# core/instrument_factory/data_format_config/__init__.py → repo root is 4 levels up.
_DATA_FORMATS_DIR = (
    Path(__file__).resolve().parent.parent.parent.parent
    / "adapter_admin"
    / "data_formats"
)


def data_format_instrument_cfg(bar_type_str: str) -> dict:
    """Return the ``instrument`` block from the asset class's data-format file.

    Resolves the venue's adapter config → ``asset_class`` →
    ``adapter_admin/data_formats/<asset_class>.json`` and returns its
    ``instrument`` sub-dict (``price_precision``, ``size_precision``,
    ``quote_currency``, ``base_currency_length``, ...). Returns ``{}`` when
    anything along the path is missing — callers then fall back to the
    built-in precision tables in ``create_instrument``.
    """
    adapter_cfg = load_adapter_config_for_bar_type(bar_type_str) or {}
    asset_class = (adapter_cfg.get("asset_class") or "").strip().lower().replace(" ", "_")
    if not asset_class:
        return {}
    fmt_path = _DATA_FORMATS_DIR / f"{asset_class}.json"
    if not fmt_path.exists():
        return {}
    try:
        fmt = json.loads(fmt_path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    inst = fmt.get("instrument")
    return inst if isinstance(inst, dict) else {}
