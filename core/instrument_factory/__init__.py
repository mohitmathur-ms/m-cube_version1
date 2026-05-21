"""Dynamically create NautilusTrader CurrencyPair instruments for crypto/FX pairs.

This package decomposes the former monolithic ``core/instrument_factory.py`` into
single-responsibility components (see README.md). The public API is unchanged:

- ``create_instrument(base, quote, venue=..., price_precision=..., size_precision=...)``
- ``instrument_for_bar_type(bar_type_str)``

The precision tables and ``VENUE`` constant are re-exported here for backward
compatibility with any code that referenced them on the old module.
"""

from __future__ import annotations

from core.instrument_factory.bar_type_resolver import instrument_for_bar_type
from core.instrument_factory.currency import CURRENCY_PRECISION
from core.instrument_factory.instrument_builder import VENUE, create_instrument
from core.instrument_factory.price_size_precision import (
    BASE_PRICE_PRECISION,
    PRICE_PRECISION,
)

__all__ = [
    "create_instrument",
    "instrument_for_bar_type",
    "VENUE",
    "PRICE_PRECISION",
    "BASE_PRICE_PRECISION",
    "CURRENCY_PRECISION",
]
