"""Bar-type → instrument resolution (the public validation entry point).

Prefers the actual catalog instrument (what the engine loads and calls
``make_qty()`` on); only when the symbol hasn't been ingested yet does it fall
back to building one from the asset class's ``data_formats`` precision.
"""

from __future__ import annotations

from functools import lru_cache

from core.instrument_factory.catalog_lookup import catalog_instrument
from core.instrument_factory.data_format_config import data_format_instrument_cfg
from core.instrument_factory.instrument_builder import create_instrument
from core.venue_config import symbol_from_bar_type, venue_from_bar_type


@lru_cache(maxsize=256)
def instrument_for_bar_type(bar_type_str: str):
    """Return the instrument used to validate a leg's lots/SL/TGT precision.

    Prefers the **actual catalog instrument** (what the engine loads and calls
    ``make_qty()`` on); only when the symbol hasn't been ingested yet does it
    fall back to building one from the asset class's ``data_formats`` precision,
    mirroring ``core/nautilus_loader.py``. Returns ``None`` when neither is
    available / the bar type can't be parsed.

    NOTE: because the catalog instrument is authoritative, a change to
    ``data_formats`` size/price precision only takes effect after the data is
    **re-ingested** (which rewrites the catalog instrument).
    """
    inst = catalog_instrument(bar_type_str)
    if inst is not None:
        return inst

    symbol = symbol_from_bar_type(bar_type_str)
    venue = venue_from_bar_type(bar_type_str)
    if not symbol or not venue:
        return None

    inst_cfg = data_format_instrument_cfg(bar_type_str)

    # Split base/quote the same way nautilus_loader does: FX uses an explicit
    # base_currency_length; otherwise strip the configured quote suffix so the
    # base-currency precision fallback (BASE_PRICE_PRECISION) still works for
    # variable-length crypto symbols like "ADAUSD" -> base "ADA", quote "USD".
    quote = (inst_cfg.get("quote_currency") or "USD").upper()
    base = symbol.upper()
    base_len = inst_cfg.get("base_currency_length")
    if base_len and len(base) > base_len:
        quote = base[base_len:]
        base = base[:base_len]
    elif quote and base.endswith(quote) and len(base) > len(quote):
        base = base[: -len(quote)]

    try:
        return create_instrument(
            base,
            quote,
            venue,
            price_precision=inst_cfg.get("price_precision"),
            size_precision=inst_cfg.get("size_precision"),
        )
    except Exception:
        return None
