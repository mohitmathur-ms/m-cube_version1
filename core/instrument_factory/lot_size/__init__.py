"""Lot-size and trade-size-cap resolution for instrument construction.

Reads the per-symbol admin config (``lot_size`` + ``trade_size`` cap) from
``adapter_admin/adapters_config/<venue>.json`` under the ``instruments`` key,
via ``core.venue_config.load_instrument_config``.
"""

from __future__ import annotations

from nautilus_trader.model.objects import Quantity

from core.venue_config import load_instrument_config

# Global fallback cap applied when the admin config has no ``trade_size``.
_DEFAULT_MAX_QTY = 9_999_999_999


def resolve_lot_size(
    symbol_str: str,
    venue: str,
    size_prec: int,
) -> tuple[Quantity | None, int]:
    """Resolve ``(lot_size_quantity, max_qty_value)`` for a symbol/venue.

    Per-symbol admin config (lot_size + trade_size cap) lives in
    ``adapter_admin/adapters_config/<venue>.json`` under the ``instruments``
    key. Missing config → ``lot_size`` unset (``None``) and the default global
    cap below.
    """
    inst_cfg = load_instrument_config(symbol_str, venue) or {}
    lot_size_cfg = inst_cfg.get("lot_size")
    lot_size_quantity = (
        Quantity(int(lot_size_cfg), precision=size_prec) if lot_size_cfg else None
    )
    cap_cfg = inst_cfg.get("trade_size")
    max_qty_value = int(cap_cfg) if cap_cfg else _DEFAULT_MAX_QTY
    return lot_size_quantity, max_qty_value
