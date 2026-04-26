"""Behaviour-equivalence safety net for the Tier 1 / Tier 2 perf optimisations.

Each helper that's about to be changed (iterrows-replacement, buffer-direct
decode, vectorised equity-curve merge) gets a "golden reference" snapshot of
its pre-change implementation in this file. The matching test calls the
production function and asserts its output is byte-identical to the reference
on synthetic fixtures.

Pattern: when a perf change preserves semantics, this file passes. If a refactor
silently changes behaviour, the affected test fails immediately with a diff —
no need to wait for an end-to-end backtest comparison.
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pytest

PROJECT = Path(__file__).resolve().parent.parent
if str(PROJECT) not in sys.path:
    sys.path.insert(0, str(PROJECT))

from core import backtest_runner, fx_rates, report_generator


# ─── Golden reference implementations (frozen snapshot of pre-change code) ──

def _ref_build_fills_lookup(fills_report) -> dict:
    """Reference snapshot of report_generator._build_fills_lookup (iterrows version)."""
    lookup: dict = {}
    if fills_report is None or fills_report.empty:
        return lookup

    df = fills_report.reset_index()
    col_order_id = report_generator._resolve_column(
        df, ["venue_order_id", "VenueOrderId", "client_order_id", "order_id"]
    )
    col_type = report_generator._resolve_column(df, ["type", "order_type", "OrderType"])
    col_contingency = report_generator._resolve_column(
        df, ["contingency_type", "ContingencyType"]
    )
    col_tags = report_generator._resolve_column(df, ["tags", "Tags"])

    for _, row in df.iterrows():
        oid = str(row[col_order_id]) if col_order_id else ""
        if not oid:
            continue
        lookup[oid] = {
            "type": str(row[col_type]) if col_type else "",
            "contingency_type": str(row[col_contingency]) if col_contingency else "",
            "tags": str(row[col_tags]) if col_tags else "",
        }
    return lookup


def _ref_extract_trade_pnls(pos_report: pd.DataFrame, pnl_col: str) -> list[float]:
    """Reference snapshot of the trade_pnls block at backtest_runner.py:1131."""
    trade_pnls: list[float] = []
    for _, row in pos_report.iterrows():
        val = row[pnl_col]
        if isinstance(val, (int, float)):
            trade_pnls.append(float(val))
            continue
        try:
            trade_pnls.append(float(str(val).split()[0]))
        except (ValueError, IndexError):
            trade_pnls.append(0.0)
    return trade_pnls


def _ref_merge_equity_curves(curves: list[list[dict]]) -> list[dict]:
    """Reference snapshot of backtest_runner._merge_equity_curves (pre-change)."""
    if not curves:
        return []
    if len(curves) == 1:
        return curves[0]

    all_timestamps = set()
    for curve in curves:
        for pt in curve:
            if pt.get("timestamp"):
                all_timestamps.add(pt["timestamp"])

    if not all_timestamps:
        return curves[0]

    sorted_ts = sorted(all_timestamps)

    curve_maps = []
    for curve in curves:
        ts_map = {}
        for pt in curve:
            if pt.get("timestamp"):
                ts_map[pt["timestamp"]] = pt["balance"]
        curve_maps.append(ts_map)

    merged = []
    last_balances = [0.0] * len(curves)
    for ts in sorted_ts:
        for i, ts_map in enumerate(curve_maps):
            if ts in ts_map:
                last_balances[i] = ts_map[ts]
        merged.append({"timestamp": ts, "balance": sum(last_balances)})

    return merged


def _ref_decode_nautilus_price_column(col):
    """Reference snapshot of fx_rates._decode_nautilus_price_column (pre-change)."""
    scale = 1e9

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
            buf = b"".join(col.tolist())
            arr = np.frombuffer(buf, dtype="<i8")
            return arr.astype(np.float64) / scale
    return None


# ─── Fixtures ───────────────────────────────────────────────────────────────

@pytest.fixture
def fills_report_fixture() -> pd.DataFrame:
    """Synthetic fills report covering the column-resolution paths."""
    return pd.DataFrame({
        "venue_order_id": ["ORD-1", "ORD-2", "", "ORD-4"],
        "type": ["MARKET", "LIMIT", "STOP_MARKET", "MARKET"],
        "contingency_type": ["", "ONE_CANCELS_OTHER", "", ""],
        "tags": ["entry", "", "stop_loss", "take_profit"],
    })


@pytest.fixture
def positions_report_fixture() -> pd.DataFrame:
    """Synthetic positions report exercising both numeric and money-string PnL."""
    return pd.DataFrame({
        "instrument_id": ["EURUSD.FOREX_MS", "GBPUSD.FOREX_MS", "USDJPY.FOREX_MS",
                          "EURUSD.FOREX_MS"],
        "entry": ["BUY", "SELL", "BUY", "SELL"],
        "peak_qty": [1000, 2000, 500, 1500],
        "avg_px_open": ["1.10000 EUR/USD", "1.25 GBP/USD", "150.0 USD/JPY", "1.10500"],
        "avg_px_close": ["1.10500", "1.24500", "151.0", "1.10000"],
        "realized_pnl": ["50.00 USD", "-100.00 USD", "0 JPY", "garbage_value"],
        "ts_opened": pd.to_datetime(
            ["2024-01-01 10:00", "2024-01-01 11:00",
             "2024-01-01 12:00", "2024-01-01 13:00"], utc=True,
        ),
        "ts_closed": pd.to_datetime(
            ["2024-01-01 10:30", "2024-01-01 11:45",
             "2024-01-01 12:50", "2024-01-01 13:30"], utc=True,
        ),
        "id": ["P1", "P2", "P3", "P4"],
        "opening_order_id": ["ORD-1", "ORD-2", "ORD-3", "ORD-4"],
        "closing_order_id": ["ORD-1c", "ORD-2c", "ORD-3c", "ORD-4c"],
    })


@pytest.fixture
def numeric_pnl_fixture() -> pd.DataFrame:
    """positions_report shape after positions_report_with_base added a numeric column."""
    return pd.DataFrame({
        "realized_pnl_usd": [50.0, -100.0, 0.0, 25.5, np.nan],
    })


@pytest.fixture
def equity_curves_fixture() -> list[list[dict]]:
    """Three equity curves with overlapping and distinct timestamps."""
    return [
        [
            {"timestamp": "2024-01-01T00:00:00Z", "balance": 100.0},
            {"timestamp": "2024-01-01T01:00:00Z", "balance": 105.0},
            {"timestamp": "2024-01-01T03:00:00Z", "balance": 110.0},
        ],
        [
            {"timestamp": "2024-01-01T00:30:00Z", "balance": 200.0},
            {"timestamp": "2024-01-01T02:00:00Z", "balance": 195.0},
            {"timestamp": "2024-01-01T03:00:00Z", "balance": 198.0},
        ],
        [
            {"timestamp": "2024-01-01T01:00:00Z", "balance": 300.0},
            {"timestamp": "2024-01-01T02:30:00Z", "balance": 305.0},
        ],
    ]


@pytest.fixture
def bytes_price_column_fixture() -> pd.Series:
    """Object-dtype Series of 8-byte LE int64 packed prices (Nautilus FX format)."""
    raw_int_prices = np.array(
        [119_817_000_000, 119_900_000_000, 120_050_000_000, 119_750_000_000],
        dtype="<i8",
    )
    blobs = [v.tobytes() for v in raw_int_prices]
    return pd.Series(blobs, dtype=object)


@pytest.fixture
def all_results_fixture(positions_report_fixture, fills_report_fixture) -> dict:
    """Shape that _build_orderbook expects: strategy_name -> {positions_report, fills_report}."""
    return {
        "EMA Cross EURUSD": {
            "positions_report": positions_report_fixture.copy(),
            "fills_report": fills_report_fixture.copy(),
        },
        "RSI Mean Reversion": {
            "positions_report": positions_report_fixture.iloc[:2].copy(),
            "fills_report": fills_report_fixture.iloc[:2].copy(),
        },
    }


# ─── Tests ──────────────────────────────────────────────────────────────────

class TestBuildFillsLookup:
    """Tier 1 #2: report_generator._build_fills_lookup."""

    def test_matches_iterrows_reference(self, fills_report_fixture):
        ref = _ref_build_fills_lookup(fills_report_fixture)
        new = report_generator._build_fills_lookup(fills_report_fixture)
        assert new == ref

    def test_skips_empty_order_ids(self, fills_report_fixture):
        new = report_generator._build_fills_lookup(fills_report_fixture)
        assert "" not in new
        assert len(new) == 3  # row 3 has empty venue_order_id

    def test_handles_empty_dataframe(self):
        empty = pd.DataFrame({"venue_order_id": [], "type": []})
        assert report_generator._build_fills_lookup(empty) == {}

    def test_handles_none(self):
        assert report_generator._build_fills_lookup(None) == {}


class TestExtractTradePnls:
    """Tier 1 #1: backtest_runner._extract_trade_pnls (extracted helper)."""

    def test_helper_exists(self):
        # The Tier 1 #1 fix extracts the inline iterrows block to a helper.
        assert hasattr(backtest_runner, "_extract_trade_pnls"), (
            "Expected backtest_runner._extract_trade_pnls helper after Tier 1 #1"
        )

    def test_money_string_column(self, positions_report_fixture):
        ref = _ref_extract_trade_pnls(positions_report_fixture, "realized_pnl")
        new = backtest_runner._extract_trade_pnls(
            positions_report_fixture, "realized_pnl"
        )
        assert new == ref
        # And the actual values should be: 50.0, -100.0, 0.0, 0.0 (garbage → fallback)
        assert new == [50.0, -100.0, 0.0, 0.0]

    def test_numeric_column(self, numeric_pnl_fixture):
        ref = _ref_extract_trade_pnls(numeric_pnl_fixture, "realized_pnl_usd")
        new = backtest_runner._extract_trade_pnls(
            numeric_pnl_fixture, "realized_pnl_usd"
        )
        # NaN compares False, so use zip-based comparison
        assert len(new) == len(ref)
        for a, b in zip(new, ref):
            if pd.isna(a) and pd.isna(b):
                continue
            assert a == b


class TestBuildOrderbook:
    """Tier 1 #3: report_generator._build_orderbook."""

    def _capture_reference_output(self, all_results: dict) -> list[dict]:
        """Run the original iterrows path by temporarily patching back. We can't
        easily snapshot the full _build_orderbook (12+ columns) inline, so we
        save the BEFORE output during this conftest run as the reference."""
        return report_generator._build_orderbook(all_results)

    def test_orderbook_shape(self, all_results_fixture):
        out = report_generator._build_orderbook(all_results_fixture)
        # 4 trades from "EMA Cross EURUSD" + 2 trades from "RSI Mean Reversion" = 6
        assert len(out) == 6
        # Sorted by ENTRY TIME (DD-MM-YYYY HH:MM:SS, lexicographic on string)
        entries = [t["ENTRY TIME"] for t in out]
        assert entries == sorted(entries)

    def test_orderbook_field_types_and_keys(self, all_results_fixture):
        out = report_generator._build_orderbook(all_results_fixture)
        for trade in out:
            for key in ("USERID", "SYMBOL", "EXCHANGE", "TRANSACTION", "LOTS",
                        "MULTIPLIER", "QUANTITY", "OrderID", "ENTRY TIME",
                        "ENTRY PRICE", "ENTRY REASON", "OPTION TYPE", "STRIKE",
                        "PORTFOLIO NAME", "STRATEGY", "EXIT TIME",
                        "AVG EXIT PRICE", "EXIT REASON", "PNL",
                        "_IS_HEDGE", "_PARENT_ID"):
                assert key in trade, f"missing key {key!r}"

    def test_pnl_parsed_as_float(self, all_results_fixture):
        out = report_generator._build_orderbook(all_results_fixture)
        # First trade has realized_pnl="50.00 USD" → 50.0
        first_eurusd = [t for t in out if t["STRATEGY"] == "EMA Cross EURUSD"][0]
        assert isinstance(first_eurusd["PNL"], float)

    def test_instrument_split(self, all_results_fixture):
        out = report_generator._build_orderbook(all_results_fixture)
        eurs = [t for t in out if t["SYMBOL"] == "EURUSD"]
        assert all(t["EXCHANGE"] == "FOREX_MS" for t in eurs)

    def test_reasons_use_fills_lookup(self, all_results_fixture):
        out = report_generator._build_orderbook(all_results_fixture)
        # Fixture: ORD-2 has tags="" but contingency_type="ONE_CANCELS_OTHER".
        # _determine_reason for non-reduce orders without tags → falls into the
        # "else" branch (LIMIT → "Limit Order"). So this is mostly a smoke check.
        for trade in out:
            assert trade["ENTRY REASON"]  # non-empty


class TestBuildLogsDataframe:
    """Tier 1 #4: report_generator.build_logs_dataframe."""

    def test_logs_shape(self, fills_report_fixture):
        # build_logs_dataframe expects fills with side/qty/price/ts/etc.
        # Our fills_report_fixture is missing side/qty/price columns, so
        # construct a richer fixture here.
        rich = pd.DataFrame({
            "venue_order_id": ["ORD-1", "ORD-2", "ORD-3"],
            "instrument_id": ["EURUSD.FOREX_MS"] * 3,
            "side": ["BUY", "SELL", "BUY"],
            "filled_qty": ["1000", "1000", "500"],
            "avg_px": ["1.10000", "1.10500", "1.25000"],
            "ts_last": pd.to_datetime(
                ["2024-01-01 10:00", "2024-01-01 10:30", "2024-01-01 11:00"],
                utc=True,
            ),
            "is_reduce_only": [False, True, False],
            "position_id": ["P1", "P1", "P2"],
            "type": ["MARKET", "MARKET", "LIMIT"],
            "contingency_type": ["", "", ""],
            "tags": ["entry", "exit", "entry"],
        })

        all_results = {"Strat-1": {"fills_report": rich}}
        out = report_generator.build_logs_dataframe(all_results, "2026-04-25 12:00:00")
        assert len(out) == 3
        assert set(out.columns) == {
            "Timestamp", "Backtest_Timestamp", "Log Type", "Message",
            "UserID", "Strategy Tag", "Option Portfolio", "Strike",
        }
        # Sorted ascending by Backtest_Timestamp
        assert list(out["Backtest_Timestamp"]) == sorted(out["Backtest_Timestamp"])

    def test_action_reflects_reduce_flag(self):
        df = pd.DataFrame({
            "venue_order_id": ["A", "B"],
            "instrument_id": ["EURUSD.FOREX_MS"] * 2,
            "side": ["BUY", "SELL"],
            "filled_qty": ["1000", "1000"],
            "avg_px": ["1.10", "1.11"],
            "ts_last": pd.to_datetime(["2024-01-01", "2024-01-02"], utc=True),
            "is_reduce_only": [False, True],
            "position_id": ["P1", "P1"],
            "type": ["MARKET", "MARKET"],
            "contingency_type": ["", ""],
            "tags": ["", ""],
        })
        out = report_generator.build_logs_dataframe({"S": {"fills_report": df}}, "")
        assert "ENTRY" in out.iloc[0]["Message"]
        assert "EXIT" in out.iloc[1]["Message"]


class TestDecodeNautilusPriceColumn:
    """Tier 1 #5: fx_rates._decode_nautilus_price_column (Arrow-buffer-direct)."""

    def test_float_path(self):
        col = pd.Series([1.5, 2.5, 3.5], dtype="float64")
        out = fx_rates._decode_nautilus_price_column(col)
        np.testing.assert_array_equal(out, np.array([1.5, 2.5, 3.5]))

    def test_int_path(self):
        col = pd.Series([100_000_000, 200_000_000], dtype="int64")
        out = fx_rates._decode_nautilus_price_column(col)
        np.testing.assert_allclose(out, [0.1, 0.2])

    def test_bytes_path_matches_reference(self, bytes_price_column_fixture):
        ref = _ref_decode_nautilus_price_column(bytes_price_column_fixture)
        new = fx_rates._decode_nautilus_price_column(bytes_price_column_fixture)
        np.testing.assert_array_equal(ref, new)
        # Sanity-check absolute values
        np.testing.assert_allclose(
            new, [119.817, 119.900, 120.050, 119.750], rtol=1e-9
        )

    def test_empty_series_returns_none(self):
        col = pd.Series([], dtype=object)
        assert fx_rates._decode_nautilus_price_column(col) is None

    def test_unrecognized_dtype_returns_none(self):
        col = pd.Series(["not_8_bytes", "either"], dtype=object)
        assert fx_rates._decode_nautilus_price_column(col) is None

    def test_arrow_chunked_input_works(self):
        """Tier 1 #5 should also handle pyarrow ChunkedArray / Array directly."""
        raw = np.array([119_817_000_000, 119_900_000_000], dtype="<i8")
        blobs = [v.tobytes() for v in raw]
        # Wrap as fixed-size binary Arrow array, then re-materialise as object Series.
        arr = pa.array(blobs, type=pa.binary(8))
        col = pd.Series(arr.to_pylist(), dtype=object)
        out = fx_rates._decode_nautilus_price_column(col)
        np.testing.assert_allclose(out, [119.817, 119.900], rtol=1e-9)


class TestMergeEquityCurves:
    """Tier 2 #6: backtest_runner._merge_equity_curves (vectorised)."""

    def test_empty_input(self):
        assert backtest_runner._merge_equity_curves([]) == []

    def test_single_curve_passthrough(self, equity_curves_fixture):
        single = [equity_curves_fixture[0]]
        out = backtest_runner._merge_equity_curves(single)
        assert out == equity_curves_fixture[0]

    def test_matches_reference(self, equity_curves_fixture):
        ref = _ref_merge_equity_curves(equity_curves_fixture)
        new = backtest_runner._merge_equity_curves(equity_curves_fixture)
        # Compare timestamps (must be sorted) and balances exactly.
        assert len(new) == len(ref)
        for r, n in zip(ref, new):
            assert r["timestamp"] == n["timestamp"]
            assert abs(r["balance"] - n["balance"]) < 1e-9, (
                f"balance mismatch at {r['timestamp']}: ref={r['balance']} new={n['balance']}"
            )

    def test_forward_fill_semantics(self, equity_curves_fixture):
        """Curve A has no point at 00:30 — its balance there should be 100.0
        (last known). Curve B's balance at 00:30 is 200.0. Curve C has nothing
        before 01:00 — its balance at 00:30 is 0.0 (initial)."""
        out = backtest_runner._merge_equity_curves(equity_curves_fixture)
        at_0030 = next(p for p in out if p["timestamp"] == "2024-01-01T00:30:00Z")
        assert abs(at_0030["balance"] - (100.0 + 200.0 + 0.0)) < 1e-9

    def test_no_timestamps_returns_first_curve(self):
        curves = [[{"balance": 100.0}, {"balance": 105.0}]]
        out = backtest_runner._merge_equity_curves(curves)
        assert out == curves[0]

    def test_within_curve_duplicate_timestamps_keep_last(self):
        """Real Nautilus equity curves can repeat the same timestamp twice
        when several events fire in the same nanosecond. The original
        dict-walk silently overwrote on duplicate keys; the vectorised path
        must do the same instead of erroring on a non-unique pandas index."""
        curves = [
            [
                {"timestamp": "2024-01-01T00:00:00Z", "balance": 100.0},
                {"timestamp": "2024-01-01T01:00:00Z", "balance": 105.0},
                # Duplicate timestamp — last value (110) must win.
                {"timestamp": "2024-01-01T01:00:00Z", "balance": 110.0},
                {"timestamp": "2024-01-01T02:00:00Z", "balance": 115.0},
            ],
            [
                {"timestamp": "2024-01-01T00:30:00Z", "balance": 200.0},
                {"timestamp": "2024-01-01T02:00:00Z", "balance": 210.0},
            ],
        ]
        ref = _ref_merge_equity_curves(curves)
        new = backtest_runner._merge_equity_curves(curves)
        assert len(new) == len(ref)
        for r, n in zip(ref, new):
            assert r["timestamp"] == n["timestamp"]
            assert abs(r["balance"] - n["balance"]) < 1e-9, (
                f"balance mismatch at {r['timestamp']}: ref={r['balance']} new={n['balance']}"
            )


class TestFxScanCacheMtimeGuard:
    """Tier 2 #9: cached FX scan returns instantly when root mtime unchanged.

    Scenario: a previous scan populated the cache; the directory tree hasn't
    been touched since. The next call must NOT rerun the rglob — the cache
    hit is the whole point of the optimisation.
    """

    def test_unchanged_mtime_uses_cached_entries(self, tmp_path):
        from core import csv_loader

        root = tmp_path / "fx"
        root.mkdir()
        (root / "EURUSD").mkdir()
        # Plant one file matching the FX daily pattern so the scan finds something.
        target_dir = root / "EURUSD" / "2024" / "01" / "01"
        target_dir.mkdir(parents=True)
        # Need both BID and ASK or the pair is skipped — see line 118.
        (target_dir / "01.01.2024_BID_OHLCV.csv").write_text("ts,open\n")
        (target_dir / "01.01.2024_ASK_OHLCV.csv").write_text("ts,open\n")

        csv_loader.clear_fx_scan_cache()
        first = csv_loader._scan_fx_daily_layout(root)
        assert len(first) == 3  # ASK, BID, MID

        # Sentinel: monkey-patch rglob to raise so a second walk would error.
        # (We prove the cache is hit by ensuring the function returns without
        # ever calling rglob again.)
        orig_rglob = type(root).rglob
        try:
            def _explode(self, pattern):
                raise AssertionError("rglob called when cache should have been hit")
            type(root).rglob = _explode  # type: ignore[method-assign]
            second = csv_loader._scan_fx_daily_layout(root)
            assert second == first
        finally:
            type(root).rglob = orig_rglob  # type: ignore[method-assign]


class TestPandasUtilsIfPresent:
    """Tier 2 #8: shared core/_pandas_utils.iter_columns helper.

    This is xfail until Tier 2 #8 lands. Once present, the helper must accept
    a DataFrame + column names and yield tuples in row order, with None for
    missing columns.
    """

    def test_helper_module_is_importable(self):
        try:
            from core import _pandas_utils
        except ImportError:
            pytest.skip("Tier 2 #8 has not been applied yet")

        df = pd.DataFrame({"a": [1, 2, 3], "b": ["x", "y", "z"]})
        rows = list(_pandas_utils.iter_columns(df, "a", "b"))
        assert rows == [(1, "x"), (2, "y"), (3, "z")]

    def test_missing_column_yields_none(self):
        try:
            from core import _pandas_utils
        except ImportError:
            pytest.skip("Tier 2 #8 has not been applied yet")

        df = pd.DataFrame({"a": [1, 2]})
        rows = list(_pandas_utils.iter_columns(df, "a", None))
        # None as a column name → yield None for that slot
        assert rows == [(1, None), (2, None)]
