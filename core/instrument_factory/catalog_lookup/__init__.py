"""Authoritative instrument lookup from the ParquetDataCatalog.

Returns the instrument the backtest engine would actually load for a bar type,
so precision validation can never disagree with what the backtest will do.
"""

from __future__ import annotations

from pathlib import Path

# core/instrument_factory/catalog_lookup/__init__.py → repo root is 4 levels up.
_CATALOG_PATH = Path(__file__).resolve().parent.parent.parent.parent / "catalog"


def catalog_instrument(bar_type_str: str):
    """Return the instrument the backtest engine would actually load for this
    bar type from the ParquetDataCatalog, or ``None`` if it isn't ingested.

    This is the authoritative source — ``core/backtest_runner.py`` resolves the
    instrument exactly this way (``catalog.instruments()`` matched on
    ``BarType.from_str(...).instrument_id``) and calls ``make_qty()`` on it.
    Reading it here means precision validation can never disagree with what the
    backtest will do (and it sidesteps venue-name mismatches between bar types
    and adapter configs, e.g. ``COINBASE`` vs ``COINBASE_MS``).
    """
    try:
        from nautilus_trader.model.data import BarType
        from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

        inst_id = BarType.from_str(bar_type_str).instrument_id
        catalog = ParquetDataCatalog(str(_CATALOG_PATH))
        for inst in catalog.instruments():
            if inst.id == inst_id:
                return inst
    except Exception:
        return None
    return None
