"""Price- and size-precision resolution for instrument construction.

Holds the built-in precision tables (the safety net used when a per-asset-class
data-format config does not specify precision) and the resolution helpers that
turn an optional explicit override + base/quote currencies into concrete
precision integers.
"""

from __future__ import annotations

# Price precision defaults per QUOTE currency. This is a SAFETY NET used
# only when:
#   (a) the per-asset-class JSON config did not specify price_precision, AND
#   (b) the BASE currency has no explicit entry in BASE_PRICE_PRECISION below.
# Under-specifying precision here is silent and catastrophic: FX minute bars
# stored at precision=2 (1.08 → 1.08) make every bar a doji because open
# equals close. Always pick the precision that preserves the smallest real
# tick you expect in the source data. When adding a new quote currency,
# set it to match the data source, and prefer over-specifying (extra bytes
# in parquet) over under-specifying (lost price ticks).
PRICE_PRECISION = {
    "USD": 5,   # FX pairs /USD trade at 5 decimals (pip = 0.0001)
    "EUR": 5,   # FX pairs /EUR
    "GBP": 5,   # FX pairs /GBP
    "JPY": 3,   # FX /JPY: base is ~100-200, so pip scale = 0.01 → 3 decimals
    "USDT": 8,  # Crypto majors /USDT: price >> 1, 0.01 tick is fine
}

# Price precision overrides for specific base currencies (high-priced assets)
BASE_PRICE_PRECISION = {
    "BTC": 2,
    "ETH": 2,
    "SOL": 4,
    "XRP": 4,
    "DOGE": 6,
    "ADA": 4,
    "AVAX": 4,
    "LINK": 4,
    "DOT": 4,
    "MATIC": 6,
}


def resolve_price_precision(
    base: str,
    quote: str,
    override: int | None = None,
) -> int:
    """Resolve the price precision for a ``base``/``quote`` pair.

    Explicit ``override`` wins; otherwise the BASE-currency table is consulted
    first (high-priced assets like BTC), then the QUOTE-currency table, then a
    default of 2 for unknown pairs.
    """
    if override is not None:
        return override
    return BASE_PRICE_PRECISION.get(base, PRICE_PRECISION.get(quote, 2))


def resolve_size_precision(override: int | None = None) -> int:
    """Resolve the size precision.

    Size precision controls the number of decimal places an order/volume
    quantity may carry. It does NOT change the maximum representable value:
    Nautilus stores quantities as fixed-point with FIXED_PRECISION=9, so the
    cap is QUANTITY_MAX (~18.4B) regardless of size_precision. Bar volumes are
    already clipped to QUANTITY_MAX at CSV-load time (see core/csv_loader.py),
    so raising precision is safe. We default to 0 (whole units) only when no
    asset-class override is supplied; crypto sets a non-zero value so
    fractional lot sizes (e.g. 0.5, 0.001) are tradeable.
    """
    return override if override is not None else 0