"""
FX rate resolver for converting PnL amounts between currencies.

Backtests can run across instruments whose quote currencies differ from the
account base currency (e.g. USDJPY's PnL is in JPY, but the account is USD).
Nautilus doesn't apply a conversion unless an FX rate source is registered,
so by default the JPY PnL is silently swallowed when rolling up into a USD
account balance.

This module provides a resolver driven by the venue's adapter config:

    fx_conversion = {
        "JPY": {
            "source": "catalog",                       # "catalog" | "static"
            "catalog_pair": "USDJPY.FOREX_MS",         # required when source="catalog"
            "fallback_rate": 0.0063                    # JPY -> USD; used on gaps
        },
        ...
    }

Usage in the backtest runner:

    resolver = FxRateResolver.from_adapter_config(adapter_cfg, catalog_path)
    usd_pnl = resolver.convert(amount=-12345.0, from_ccy="JPY",
                               at_timestamp=pd.Timestamp("2024-06-15", tz="UTC"))

The resolver is *timestamp-aware*: catalog-sourced rates are looked up at the
bar closest to (and not after) the requested timestamp, then forward-filled
across weekend/holiday gaps. A 2015 fill and a 2024 fill therefore convert at
their actual prevailing rates, not a single snapshot rate.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import pandas as pd


@dataclass(frozen=True)
class _RuleStatic:
    rate: float          # from_ccy -> base_ccy, e.g. 0.0063 for JPY -> USD


@dataclass(frozen=True)
class _RuleCatalog:
    catalog_pair: str    # e.g. "USDJPY.FOREX_MS"
    is_inverse: bool     # True when the pair is <base><from> (USDJPY for JPY->USD),
                         # False when it's <from><base> (EURUSD for EUR->USD).
    fallback_rate: Optional[float]  # used when no bar exists at/before timestamp


class FxRateResolver:
    """Resolve amounts in arbitrary currencies into the account base currency.

    Built once per backtest and reused across every PnL conversion. Catalog
    rate series are loaded lazily (first lookup for a given currency) so a
    backtest that never sees JPY positions doesn't pay the IO cost.
    """

    def __init__(
        self,
        base_currency: str,
        rules: dict[str, _RuleStatic | _RuleCatalog],
        catalog_path: str | None = None,
    ):
        self.base_currency = base_currency.upper()
        self._rules = rules
        self._catalog_path = catalog_path
        self._series_cache: dict[str, pd.Series] = {}  # catalog_pair -> close series

    # ─── Factory ──────────────────────────────────────────────────────────

    @classmethod
    def from_adapter_config(
        cls,
        adapter_cfg: dict | None,
        catalog_path: str | None,
    ) -> "FxRateResolver":
        """Build a resolver from an adapter config dict. Missing/invalid config
        yields a resolver that only supports same-currency no-ops (safe default).
        """
        if not adapter_cfg:
            return cls(base_currency="USD", rules={}, catalog_path=catalog_path)

        base = (adapter_cfg.get("account_base_currency") or "USD").upper()
        raw = adapter_cfg.get("fx_conversion") or {}

        rules: dict[str, _RuleStatic | _RuleCatalog] = {}
        for from_ccy, spec in raw.items():
            if not isinstance(spec, dict):
                continue
            from_ccy = from_ccy.upper()
            source = (spec.get("source") or "static").lower()
            if source == "catalog":
                pair = (spec.get("catalog_pair") or "").strip()
                if not pair:
                    continue
                # Pair orientation is inferred from the symbol part. For
                # JPY -> USD with pair "USDJPY", USD is base => is_inverse.
                # For EUR -> USD with pair "EURUSD", EUR is base => direct.
                symbol = pair.split(".")[0].upper()
                is_inverse = symbol.startswith(base) and symbol.endswith(from_ccy)
                fallback = spec.get("fallback_rate")
                rules[from_ccy] = _RuleCatalog(
                    catalog_pair=pair,
                    is_inverse=is_inverse,
                    fallback_rate=float(fallback) if fallback is not None else None,
                )
            else:
                rate = spec.get("rate")
                if rate is None:
                    continue
                rules[from_ccy] = _RuleStatic(rate=float(rate))

        return cls(base_currency=base, rules=rules, catalog_path=catalog_path)

    # ─── Public API ───────────────────────────────────────────────────────

    def convert(
        self,
        amount: float,
        from_ccy: str,
        at_timestamp: pd.Timestamp | None = None,
    ) -> float:
        """Convert `amount` in `from_ccy` into the base currency at `at_timestamp`.

        Falls back gracefully:
          * same-currency → pass-through
          * no rule configured → return amount unchanged (best-effort; the
            caller already saw native-currency numbers, so don't fabricate)
          * catalog lookup miss and no fallback → return amount unchanged
        """
        if amount == 0:
            return 0.0
        if not from_ccy:
            return float(amount)
        from_ccy = from_ccy.upper()
        if from_ccy == self.base_currency:
            return float(amount)

        rate = self.rate(from_ccy, at_timestamp)
        if rate is None:
            return float(amount)
        return float(amount) * rate

    def rate(
        self,
        from_ccy: str,
        at_timestamp: pd.Timestamp | None = None,
    ) -> float | None:
        """Return the conversion rate `from_ccy -> base_ccy` at `at_timestamp`.

        None means "no rule configured or no data available and no fallback".
        The caller is expected to treat that as an identity pass-through.
        """
        from_ccy = from_ccy.upper()
        if from_ccy == self.base_currency:
            return 1.0
        rule = self._rules.get(from_ccy)
        if rule is None:
            return None
        if isinstance(rule, _RuleStatic):
            return rule.rate

        # Catalog-sourced: look up the bar at or before at_timestamp.
        series = self._get_series(rule.catalog_pair)
        price = self._lookup_price(series, at_timestamp) if series is not None else None
        if price is None or price <= 0:
            return rule.fallback_rate
        return (1.0 / price) if rule.is_inverse else price

    # ─── Internals ────────────────────────────────────────────────────────

    def _get_series(self, catalog_pair: str) -> pd.Series | None:
        """Return a UTC-indexed Series of close prices for `catalog_pair`.

        Loads from the ParquetDataCatalog on first access and caches thereafter.
        The LAST-EXTERNAL bar type is preferred; if the pair only has BID/ASK
        series (common for FX), the BID is used (consistent with how Nautilus
        picks up quote-currency PnL on sells).
        """
        if self._catalog_path is None:
            return None
        if catalog_pair in self._series_cache:
            return self._series_cache[catalog_pair]

        bar_root = Path(self._catalog_path) / "data" / "bar"
        if not bar_root.exists():
            self._series_cache[catalog_pair] = None  # type: ignore[assignment]
            return None

        # Prefer daily LAST bars (cheap to read, enough granularity for PnL
        # conversion); fall back to any daily bar type, then to any bar type.
        candidates = sorted(bar_root.glob(f"{catalog_pair}-*"))
        if not candidates:
            self._series_cache[catalog_pair] = None  # type: ignore[assignment]
            return None

        def _priority(path: Path) -> tuple[int, str]:
            name = path.name
            # Daily LAST is cheapest; daily BID next; everything else last.
            if "-1-DAY-LAST-" in name:
                return (0, name)
            if "-1-DAY-BID-" in name:
                return (1, name)
            if "-LAST-" in name:
                return (2, name)
            if "-BID-" in name:
                return (3, name)
            return (4, name)

        candidates.sort(key=_priority)

        # Read parquet files directly (bypass NautilusTrader for speed and
        # to avoid importing heavy engine types here).
        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            tables = [
                pq.read_table(parquet_file, columns=["ts_event", "close"])
                for parquet_file in candidates[0].glob("*.parquet")
            ]
            if not tables:
                self._series_cache[catalog_pair] = None  # type: ignore[assignment]
                return None
            # Concatenate at the Arrow layer — one materialization to pandas
            # instead of N per-file ones followed by pd.concat.
            df = pa.concat_tables(tables).to_pandas()
            close = _decode_nautilus_price_column(df["close"])
            if close is None:
                self._series_cache[catalog_pair] = None  # type: ignore[assignment]
                return None
            ts = pd.to_datetime(df["ts_event"], unit="ns", utc=True)
            series = pd.Series(close, index=ts).sort_index()
            series = series[~series.index.duplicated(keep="last")]
            self._series_cache[catalog_pair] = series
            return series
        except Exception:
            self._series_cache[catalog_pair] = None  # type: ignore[assignment]
            return None

    @staticmethod
    def _lookup_price(series: pd.Series | None, at: pd.Timestamp | None) -> float | None:
        """asof-lookup into a sorted UTC-indexed price series.

        If `at` is None or the series has no bar at/before `at`, returns the
        *latest* known price (best-effort) so a trade that happens slightly
        before the series starts still gets a sensible rate. Callers wanting
        stricter behavior can check this via `series[:at]` themselves.
        """
        if series is None or series.empty:
            return None
        if at is None:
            return float(series.iloc[-1])
        if at.tzinfo is None:
            at = at.tz_localize("UTC")
        try:
            # asof handles weekend/holiday gaps by returning the last known
            # bar at or before `at`. For timestamps before the series start,
            # asof returns NaN — fall back to the earliest known price.
            val = series.asof(at)
            if pd.isna(val):
                return float(series.iloc[0])
            return float(val)
        except Exception:
            return None


def _decode_nautilus_price_column(col):
    """Decode a Nautilus ParquetDataCatalog `close` (or any price) column into
    a float64 numpy array.

    Nautilus stores prices as fixed-precision int64 scaled by 1e9 (so a
    USDJPY price of 119.817 is the int 119_817_000_000). Pyarrow materializes
    the column as one of:
      * float64      — already decoded by some paths
      * int64/uint64 — legacy/newer numeric writers
      * object dtype of 8-byte `bytes` blobs — raw fixed-precision int64 LE
        packed as FixedSizeBinary(8). This is what the FX ingest path emits.

    Returns a numpy float64 array (positional, not index-aligned), or None if
    the column shape isn't recognized.
    """
    import numpy as np

    scale = 1e9  # Nautilus FIXED_PRECISION = 9

    if col.dtype.kind == "f":
        return col.values.astype(np.float64)
    if col.dtype.kind in ("i", "u"):
        return col.values.astype(np.float64) / scale
    if col.dtype == object:
        nonnull = col.dropna()
        if nonnull.empty:
            return None
        sample = nonnull.iloc[0]
        if isinstance(sample, (bytes, bytearray)) and len(sample) == 8:
            # Reinterpret the whole column as a contiguous buffer of LE int64.
            # Much faster than a per-row int.from_bytes loop on multi-million
            # row FX series.
            buf = b"".join(col.tolist())
            arr = np.frombuffer(buf, dtype="<i8")
            return arr.astype(np.float64) / scale
    return None


def parse_money_string(money_str: str) -> tuple[float, str]:
    """Parse a Nautilus Money string like '-1234.56 JPY' into (amount, ccy).

    Positions_report stores PnL as formatted strings. This helper tolerates
    the common shapes: "123.45 USD", "-123.45 JPY", "0 JPY", and plain floats
    (returns ccy="" for the latter — caller treats that as unknown).
    """
    if money_str is None:
        return (0.0, "")
    s = str(money_str).strip()
    if not s:
        return (0.0, "")
    parts = s.split()
    try:
        amount = float(parts[0])
    except (ValueError, IndexError):
        return (0.0, "")
    ccy = parts[1].upper() if len(parts) > 1 else ""
    return (amount, ccy)
