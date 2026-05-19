"""
Load OHLCV data from local CSV files.

Scans a folder of CSV files and loads them into clean pandas DataFrames ready
for NautilusTrader wrangling. Two layouts are supported:

* Flat crypto layout: `{id}_{SYMBOL}_{Name}.csv` directly under the folder.
* FX daily layout: `<root>/<PAIR>/YYYY/MM/DD/DD.MM.YYYY_(BID|ASK)_OHLCV.csv`.
  For the FX layout the scanner emits **three entries per pair** — one each
  for ASK, BID and MID — so each side becomes its own selectable instrument
  in the UI and its own BarType key in the parquet catalog. MID is
  synthesized from ASK+BID at load time.
"""

from __future__ import annotations

import logging
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pacsv

logger = logging.getLogger(__name__)

# Worker pool size for parallel daily-file reads in :func:`concat_side`.
# 16 is a good default for SSDs/NVMe (pandas releases the GIL inside the
# C parser, so threads overlap cleanly). Lower it for HDDs; raise for
# arrays. Tunable per-call via the ``max_workers`` kwarg.
_CONCAT_SIDE_MAX_WORKERS = 16

# Module-level cache for :func:`_scan_fx_daily_layout`. ``rglob`` over the
# FX tree (thousands of daily files under <PAIR>/YYYY/MM/DD/) takes
# 2–5s; the same scan is hit on every UI page load and view-data refresh.
# Cache key: str(root). Value: (cached_at, root_mtime, entries).
#
# The fast-path key is the **root directory mtime**: any new pair dir or
# rename under the root bumps it (Windows + Linux both expose this). When
# mtime is unchanged the cache is reused regardless of how long ago the
# scan ran — that eliminates the periodic 2-5s UI hangs the previous 60s
# TTL caused on big trees. The longer TTL below is only a safety net for
# new daily files dropped into existing YYYY/MM/DD dirs, where the root
# mtime stays constant; users who need an immediate refresh can call
# :func:`clear_fx_scan_cache`.
_FX_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_FX_SCAN_CACHE_TTL_SECONDS = 600.0
_FX_SCAN_CACHE_LOCK = threading.Lock()

# Parallel cache for :func:`_scan_commodity_daily_layout`. Same mtime + TTL
# safety-net shape as the FX cache above; see that block for the rationale.
_COMMODITY_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_COMMODITY_SCAN_CACHE_TTL_SECONDS = 600.0
_COMMODITY_SCAN_CACHE_LOCK = threading.Lock()

# Parallel cache for :func:`_scan_index_daily_layout`. Same mtime + TTL
# safety-net shape as the FX cache above; see that block for the rationale.
_INDEX_SCAN_CACHE: dict[str, tuple[float, float, list[dict]]] = {}
_INDEX_SCAN_CACHE_TTL_SECONDS = 600.0
_INDEX_SCAN_CACHE_LOCK = threading.Lock()


# Default path to the user's crypto CSV data
DEFAULT_CSV_FOLDER = r"D:\Data_all\Fx"

# NautilusTrader QUANTITY_RAW_MAX is 18_446_744_072.999999488
# Use a safe value well below this to avoid Rust panics
QUANTITY_MAX = 16_000_000_000.0


# Common symbol-name misspellings in the source data we've observed.
# Normalize so downstream instrument creation uses ISO-like pair names.
_SYMBOL_NORMALIZE = {"EUROUSD": "EURUSD", "GPBUSD": "GBPUSD"}

# FX daily-file naming: "DD.MM.YYYY_{BID|ASK}_OHLCV.csv"
_DAILY_FX_PATTERN = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4})_(BID|ASK)_OHLCV\.csv$",
    re.IGNORECASE,
)

# Commodity daily-file naming: bare "ASK.csv" / "BID.csv". The date is encoded
# in the parent directory path (<COMMODITY>/YYYY/MM/DD/), not the filename, so
# the regex captures only the side.
_DAILY_COMMODITY_PATTERN = re.compile(r"^(ASK|BID)\.csv$", re.IGNORECASE)

# Index daily-file naming: "DD.MM.YYYY_complete_df_OHLCV.csv" — one consolidated
# file per trading day (no ASK/BID split — the index level itself is the bar
# stream). Layout is three-deep with no symbol directory:
# <root>/YYYY/MM/<DD.MM.YYYY>_complete_df_OHLCV.csv
_DAILY_INDEX_PATTERN = re.compile(
    r"^(\d{2})\.(\d{2})\.(\d{4})_complete_df_OHLCV\.csv$",
    re.IGNORECASE,
)

# Months → 3-letter abbreviation used in the synthetic display filename produced
# by :func:`_scan_index_daily_layout`. Matches the convention in the consolidated
# FX naming (e.g. "01JAN2020_31DEC2025") so the index display filename slots
# cleanly into the asset-class filter the UI applies.
_MONTH_ABBR = (
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
    "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
)

# Consolidated-FX naming: "{PAIR}_{PAIR}_DDMMMYYYY_DDMMMYYYY_{ASK|BID|MID}_OHLCV.csv"
# (matches the pattern documented in adapter_admin/data_formats/fx.json). One file
# per pair per side, replacing the thousands-of-daily-files layout. Captured groups
# are (pair, side).
_FLAT_FX_FILE_PATTERN = re.compile(
    r"^([A-Z]+)_\1_\d{2}[A-Z]{3}\d{4}_\d{2}[A-Z]{3}\d{4}_(ASK|BID|MID)_OHLCV\.csv$"
)


def _scan_fx_consolidated_files(root: Path) -> list[dict]:
    """Discover one-file-per-side consolidated FX CSVs.

    Each match produces a single entry that flows through the inline
    single-file path in :func:`core.nautilus_loader.load_csv_and_store`
    (no ``aggregated`` flag, no ``ask_files`` / ``bid_files`` lists), so
    ingest stays in-process and skips the background-job machinery
    entirely. The downstream loader picks ``price_type`` straight from
    ``entry["side"]``.

    Searched scope is intentionally shallow — the root and one level
    below — so we don't traverse a daily-layout tree (thousands of
    nested files) hunting for files that only ever live near the top.
    """
    seen: set[Path] = set()
    candidates: list[Path] = []
    for path in root.glob("*.csv"):
        if path.is_file():
            candidates.append(path)
            seen.add(path)
    for path in root.glob("*/*.csv"):
        if path.is_file() and path not in seen:
            candidates.append(path)

    entries: list[dict] = []
    auto_id = 0
    for path in sorted(candidates):
        match = _FLAT_FX_FILE_PATTERN.match(path.name)
        if not match:
            continue
        symbol_raw, side = match.group(1), match.group(2)
        symbol = _SYMBOL_NORMALIZE.get(symbol_raw, symbol_raw)
        auto_id -= 1
        entries.append({
            "path": str(path),
            "filename": path.name,
            "id": auto_id,
            "symbol": symbol,
            "side": side,
            "name": f"{symbol} {side} (consolidated)",
        })
    return entries


def _scan_fx_daily_layout(root: Path) -> list[dict]:
    """Aggregate daily FX CSVs (<root>/<PAIR>/YYYY/MM/DD/DD.MM.YYYY_(BID|ASK)_OHLCV.csv)
    into three entries per pair: ASK, BID and MID.

    Each entry carries both sides' file lists so the downstream loader can
    either concatenate one side directly (ASK or BID) or synthesize MID
    from ASK+BID via :func:`_merge_ask_bid_to_mid`. Pairs missing either
    side are skipped so the downstream merge always has something to join.

    Result is cached under :data:`_FX_SCAN_CACHE` with a TTL — repeated UI
    refreshes hit the cache instead of re-walking thousands of daily files.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _FX_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _FX_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    aggregated: dict[tuple[str, str], list[str]] = {}
    for csv_file in root.rglob("*.csv"):
        match = _DAILY_FX_PATTERN.match(csv_file.name)
        if not match:
            continue
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) < 2:
            # File directly in root — skip, handled by flat scan.
            continue
        pair_dir = rel_parts[0].upper()
        symbol = _SYMBOL_NORMALIZE.get(pair_dir, pair_dir)
        side = match.group(4).upper()
        aggregated.setdefault((symbol, side), []).append(str(csv_file))

    # Reshape into per-pair maps with both sides.
    pairs: dict[str, dict[str, list[str]]] = {}
    for (symbol, side), files in aggregated.items():
        pairs.setdefault(symbol, {})[side] = sorted(files)

    entries: list[dict] = []
    auto_id = 0
    for symbol in sorted(pairs):
        sides = pairs[symbol]
        ask_files = sides.get("ASK", [])
        bid_files = sides.get("BID", [])
        if not ask_files or not bid_files:
            missing = "ASK" if not ask_files else "BID"
            logger.warning("fx_scan skip %s: missing %s side", symbol, missing)
            continue
        total_files = len(ask_files) + len(bid_files)
        for entry_side in ("ASK", "BID", "MID"):
            auto_id -= 1
            # Synthetic filename shaped like the consolidated format so the
            # fx.json filename_pattern still accepts it for UI filtering.
            synth_filename = (
                f"{symbol}_{symbol}_01JAN2015_31DEC2025_{entry_side}_OHLCV.csv"
            )
            nominal_path = (bid_files if entry_side == "BID" else ask_files)[0]
            entries.append({
                "path": nominal_path,  # nominal — loader uses ask_files / bid_files
                "filename": synth_filename,
                "id": auto_id,
                "symbol": symbol,
                "side": entry_side,
                "name": f"{symbol} {entry_side} ({total_files:,} daily files)",
                "ask_files": ask_files,
                "bid_files": bid_files,
                "aggregated": True,
            })

    with _FX_SCAN_CACHE_LOCK:
        _FX_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries


def _scan_commodity_daily_layout(root: Path) -> list[dict]:
    """Aggregate daily commodity CSVs (<root>/<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv)
    into three entries per symbol: ASK, BID and MID.

    Mirrors :func:`_scan_fx_daily_layout` so the produced entry shape feeds
    cleanly into :func:`core.nautilus_loader.load_csv_and_store` (it dispatches
    on ``entry["side"]`` + ``ask_files`` / ``bid_files`` + ``aggregated``).
    The only structural differences from the FX scanner:

    * The filename pattern (:data:`_DAILY_COMMODITY_PATTERN`) captures only
      the side — the date lives in the parent dirs instead of the filename.
    * No symbol normalisation: commodities don't share the FX misspelling
      quirks that :data:`_SYMBOL_NORMALIZE` handles.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _COMMODITY_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _COMMODITY_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    aggregated: dict[tuple[str, str], list[str]] = {}
    for csv_file in root.rglob("*.csv"):
        match = _DAILY_COMMODITY_PATTERN.match(csv_file.name)
        if not match:
            continue
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        if len(rel_parts) < 2:
            # File directly in root — not a daily-layout file.
            continue
        symbol = rel_parts[0].upper()
        side = match.group(1).upper()
        aggregated.setdefault((symbol, side), []).append(str(csv_file))

    pairs: dict[str, dict[str, list[str]]] = {}
    for (symbol, side), files in aggregated.items():
        pairs.setdefault(symbol, {})[side] = sorted(files)

    entries: list[dict] = []
    auto_id = 0
    for symbol in sorted(pairs):
        sides = pairs[symbol]
        ask_files = sides.get("ASK", [])
        bid_files = sides.get("BID", [])
        if not ask_files or not bid_files:
            missing = "ASK" if not ask_files else "BID"
            logger.warning("commodity_scan skip %s: missing %s side", symbol, missing)
            continue
        total_files = len(ask_files) + len(bid_files)
        for entry_side in ("ASK", "BID", "MID"):
            auto_id -= 1
            # Shape the synthetic filename like the consolidated FX naming so the
            # commodity.json filename_pattern (and the FX-style UI filter) still
            # accept it. The date range is a display placeholder — actual files
            # are loaded via ask_files / bid_files.
            synth_filename = (
                f"{symbol}_{symbol}_01JAN2015_31DEC2025_{entry_side}_OHLCV.csv"
            )
            nominal_path = (bid_files if entry_side == "BID" else ask_files)[0]
            entries.append({
                "path": nominal_path,
                "filename": synth_filename,
                "id": auto_id,
                "symbol": symbol,
                "side": entry_side,
                "name": f"{symbol} {entry_side} ({total_files:,} daily files)",
                "ask_files": ask_files,
                "bid_files": bid_files,
                "aggregated": True,
            })

    with _COMMODITY_SCAN_CACHE_LOCK:
        _COMMODITY_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries


def _index_symbol_from_root(root: Path) -> str:
    """Derive a Nautilus-friendly symbol from the root folder name.

    Uppercase, keep only ``[A-Z0-9_]``, collapse runs of disallowed characters
    into a single underscore, and trim leading/trailing underscores. So
    ``D:\\complete_df_yyyy`` → ``COMPLETE_DF_YYYY``; ``D:\\NIFTY50`` → ``NIFTY50``.
    Falls back to ``"INDEX"`` if the result would otherwise be empty.
    """
    cleaned = re.sub(r"[^A-Z0-9]+", "_", root.name.upper()).strip("_")
    return cleaned or "INDEX"


def _scan_index_daily_layout(root: Path) -> list[dict]:
    """Aggregate single-stream daily index CSVs into one entry.

    Layout: ``<root>/YYYY/MM/DD.MM.YYYY_complete_df_OHLCV.csv`` — three-deep,
    no symbol directory, no ASK/BID split. Files are collected into a single
    ``files`` list on the emitted entry; the downstream
    :func:`core.nautilus_loader.load_csv_and_store` already handles this shape
    via its ``files``-list branch (it falls through to ``price_type="LAST"``
    when ``side`` is not ASK/BID/MID, matching the ``expose_sides: ["LAST"]``
    convention in ``adapter_admin/data_formats/index.json``).

    Symbol is derived from the root folder name via
    :func:`_index_symbol_from_root` — rename the data folder to get a cleaner
    symbol if needed.
    """
    key = str(root)
    try:
        root_mtime = root.stat().st_mtime
    except OSError:
        return []

    now = time.monotonic()
    cached = _INDEX_SCAN_CACHE.get(key)
    if (cached is not None
            and cached[1] == root_mtime
            and now - cached[0] < _INDEX_SCAN_CACHE_TTL_SECONDS):
        return list(cached[2])

    # (date_tuple, path_str) tuples so we can sort chronologically by the
    # date encoded in the filename — independent of filesystem walk order.
    dated: list[tuple[tuple[int, int, int], str]] = []
    for csv_file in root.rglob("*.csv"):
        match = _DAILY_INDEX_PATTERN.match(csv_file.name)
        if not match:
            continue
        try:
            rel_parts = csv_file.relative_to(root).parts
        except ValueError:
            continue
        # Expect <root>/YYYY/MM/<file> — exactly two parent dirs above the file.
        if len(rel_parts) != 3:
            continue
        dd, mm, yyyy = match.group(1), match.group(2), match.group(3)
        try:
            date_key = (int(yyyy), int(mm), int(dd))
        except ValueError:
            continue
        dated.append((date_key, str(csv_file)))

    if not dated:
        with _INDEX_SCAN_CACHE_LOCK:
            _INDEX_SCAN_CACHE[key] = (now, root_mtime, [])
        return []

    dated.sort(key=lambda t: t[0])
    files = [path for _, path in dated]
    first_date, last_date = dated[0][0], dated[-1][0]

    symbol = _index_symbol_from_root(root)

    def _ddmonyyyy(d: tuple[int, int, int]) -> str:
        yyyy, mm, dd = d
        return f"{dd:02d}{_MONTH_ABBR[mm - 1]}{yyyy:04d}"

    # Shape the synthetic filename like the consolidated FX/commodity naming so
    # the index.json filename_pattern (and the FX-style UI filter) still accept
    # it. The actual files are loaded via the `files` list, not this path.
    synth_filename = (
        f"{symbol}_{_ddmonyyyy(first_date)}_{_ddmonyyyy(last_date)}_LAST_OHLCV.csv"
    )

    entry = {
        "path": files[0],            # nominal — loader uses 'files' list
        "filename": synth_filename,
        "id": -1,                    # negative, like other scanners
        "symbol": symbol,
        "name": f"{symbol} ({len(files):,} daily files)",
        "files": files,
    }
    entries = [entry]

    with _INDEX_SCAN_CACHE_LOCK:
        _INDEX_SCAN_CACHE[key] = (now, root_mtime, list(entries))
    return entries


def clear_fx_scan_cache() -> None:
    """Drop the daily-layout scan caches (FX + commodity + index). Call after
    manually adding new pair / commodity / index directories if you want the
    next ``scan_csv_folder`` to see them immediately rather than waiting for
    the TTL to expire."""
    with _FX_SCAN_CACHE_LOCK:
        _FX_SCAN_CACHE.clear()
    with _COMMODITY_SCAN_CACHE_LOCK:
        _COMMODITY_SCAN_CACHE.clear()
    with _INDEX_SCAN_CACHE_LOCK:
        _INDEX_SCAN_CACHE.clear()


def scan_csv_folder(folder: str = DEFAULT_CSV_FOLDER) -> list[dict]:
    """
    Scan a folder for all CSV files and return metadata for each.

    Tries to parse the crypto naming pattern {id}_{SYMBOL}_{Name}.csv
    for backward compatibility. For files that don't match, basic
    metadata is still returned so the frontend can apply its own
    pattern matching based on the selected asset class.

    When the folder contains no direct CSV files but does contain the
    FX daily-file layout (<PAIR>/YYYY/MM/DD/*.csv), files are aggregated
    by pair and returned as **three entries per pair** (ASK, BID, MID)
    so each side is independently selectable in the UI. The same
    aggregation also applies to the commodity daily-file layout
    (<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv), which is tried as a final
    fallback after the FX scanners return empty.

    Returns
    -------
    list[dict]
        Each dict has keys: path, filename, id, symbol, name.
        Aggregated FX/commodity entries also carry:
        side ("ASK"|"BID"|"MID"), ask_files (list), bid_files (list),
        aggregated (True).
    """
    folder_path = Path(folder)
    if not folder_path.exists():
        return []

    # Try the consolidated FX layout first — one CSV per pair per side at
    # or near the root. Inline ingest, no background jobs. If anything
    # matches, return immediately so this layout never gets shadowed by
    # the legacy crypto-flat heuristic for filenames like
    # "EURUSD_EURUSD_..._ASK_OHLCV.csv".
    fx_consolidated = _scan_fx_consolidated_files(folder_path)
    if fx_consolidated:
        return fx_consolidated

    results = []
    crypto_pattern = re.compile(r"^(\d+)_([A-Z]+)_(.+)\.csv$")
    auto_id = 0

    for csv_file in sorted(folder_path.glob("*.csv")):
        match = crypto_pattern.match(csv_file.name)
        if match:
            results.append({
                "path": str(csv_file),
                "filename": csv_file.name,
                "id": int(match.group(1)),
                "symbol": match.group(2),
                "name": match.group(3),
            })
        else:
            auto_id -= 1  # negative IDs for non-crypto files to avoid collisions
            stem = csv_file.stem
            results.append({
                "path": str(csv_file),
                "filename": csv_file.name,
                "id": auto_id,
                "symbol": stem.split("_")[0] if "_" in stem else stem,
                "name": stem,
            })

    # Fall back to the daily-layout aggregator only when the flat scan
    # found nothing — otherwise a directory like Fx_single_file wouldn't
    # behave as before.
    if not results:
        results = _scan_fx_daily_layout(folder_path)

    # Final fallback: commodity daily layout (<COMMODITY>/YYYY/MM/DD/(ASK|BID).csv).
    # Runs only when neither flat nor FX-daily produced anything, so existing
    # roots stay on their current code path.
    if not results:
        results = _scan_commodity_daily_layout(folder_path)

    # Last-resort fallback: index daily layout
    # (<root>/YYYY/MM/DD.MM.YYYY_complete_df_OHLCV.csv). Three-deep, no symbol
    # directory, single consolidated stream.
    if not results:
        results = _scan_index_daily_layout(folder_path)

    return results


def get_display_label(entry: dict) -> str:
    """Create a display label like 'BTC - Bitcoin (1_BTC_Bitcoin.csv)'."""
    return f"{entry['symbol']} - {entry['name']} ({entry['filename']})"


def _parse_timestamps(series: pd.Series) -> pd.Series:
    """Parse a timestamp column to UTC datetime, handling various formats."""
    non_null = series.dropna()
    if non_null.empty:
        return pd.to_datetime(series, utc=True)
    sample = str(non_null.iloc[0])

    # FX format: "DD.MM.YYYY HH:MM:SS[.fff] GMT±HHMM". pd.to_datetime with
    # an explicit format is ~50s on 4M unique minute timestamps because the
    # parser cache never hits. Transform to ISO-8601 in vectorised PyArrow
    # string ops then let pa.cast parse — measured ~22x faster, bit-identical.
    if "GMT" in sample and "." in sample.split(" ")[0]:
        arr = pa.array(series, type=pa.string())
        day = pc.utf8_slice_codeunits(arr, 0, 2)
        mon = pc.utf8_slice_codeunits(arr, 3, 5)
        yr = pc.utf8_slice_codeunits(arr, 6, 10)
        hms = pc.utf8_slice_codeunits(arr, 11, -9)  # variable fractional ok
        off = pc.utf8_slice_codeunits(arr, -5, None)
        iso = pc.binary_join_element_wise(yr, mon, day, "-")
        iso = pc.binary_join_element_wise(iso, hms, "T")
        iso = pc.binary_join_element_wise(iso, off, "")
        return iso.cast(pa.timestamp("ns", "UTC")).to_pandas()

    # Default: let pandas auto-parse with UTC.
    return pd.to_datetime(series, utc=True)


_OHLCV_LOWER = ("open", "high", "low", "close", "volume")


def load_csv(csv_path: str, timestamp_column: str = "ts",
             required_columns: list[str] | None = None,
             optional_columns: list[str] | None = None,
             delimiter: str = ",") -> pd.DataFrame:
    """
    Load a CSV file and return a clean OHLCV DataFrame.

    Parameters
    ----------
    csv_path : str
        Path to the CSV file.
    timestamp_column : str
        Name of the timestamp column in the CSV (default: "ts").
    required_columns : list[str] | None
        Columns that must exist. Defaults to [ts_col, open, high, low, close, volume].
    optional_columns : list[str] | None
        Extra columns to include if present in the source file. Missing
        optional columns are silently skipped (no validation error). Used
        e.g. for commodity ASK/BID files where `ask_vwap` exists on ASK files
        and `bid_vwap` exists on BID files — declaring both as optional lets
        the same config load either side without per-side branching.
    delimiter : str
        CSV delimiter (default: ",").

    Returns
    -------
    pd.DataFrame
        Columns: open, high, low, close, volume (+ any optional_columns present).
        Index: timestamp (UTC datetime)
    """
    ts_col_lc = (timestamp_column or "ts").lower()

    # Read header only (cheap) to discover the source case-mixed names, then
    # re-read with usecols + dtype so the C parser only materializes the
    # columns we need with the right types — skips ~10 extra columns the
    # FX daily files ship with and removes the post-hoc to_numeric pass.
    header_df = pd.read_csv(csv_path, delimiter=delimiter, nrows=0)
    case_map = {c.lower(): c for c in header_df.columns}

    needed_lc = [c.lower() for c in (required_columns or [ts_col_lc, *_OHLCV_LOWER])]
    missing = [c for c in needed_lc if c not in case_map]
    if missing:
        raise ValueError(
            f"CSV missing required columns: {missing}. Found: {list(header_df.columns)}"
        )

    src_ts = case_map[ts_col_lc]
    src_ohlcv = [case_map[c] for c in _OHLCV_LOWER]

    # Resolve optional columns against the actual header. Anything not present
    # is silently dropped from the include list — that's the whole point of
    # the field. Preserves config order and de-dupes against OHLCV/timestamp.
    optional_lc = [c.lower() for c in (optional_columns or [])]
    extra_lc = [c for c in optional_lc
                if c in case_map and c != ts_col_lc and c not in _OHLCV_LOWER]
    src_extra = [case_map[c] for c in extra_lc]

    use_cols = [src_ts, *src_ohlcv, *src_extra]

    # Use PyArrow for the data read — ~35x faster than pandas on the
    # 4M-row OHLCV files (benchmarked). Header probe above stays in pandas
    # since it's a one-row read and gives us the case-insensitive column
    # mapping PyArrow's include_columns needs as exact case.
    table = pacsv.read_csv(
        csv_path,
        parse_options=pacsv.ParseOptions(delimiter=delimiter),
        convert_options=pacsv.ConvertOptions(
            include_columns=use_cols,
            column_types={c: pa.float64() for c in [*src_ohlcv, *src_extra]},
        ),
    )
    df = table.to_pandas()
    rename_map = {src_ts: "timestamp"}
    rename_map.update({src: lc for src, lc in zip(src_ohlcv, _OHLCV_LOWER)})
    rename_map.update({src: lc for src, lc in zip(src_extra, extra_lc)})
    df = df.rename(columns=rename_map)

    df["timestamp"] = _parse_timestamps(df["timestamp"])
    df = df.dropna(subset=["timestamp"]).set_index("timestamp")
    # Only OHLCV NaNs disqualify a row — an optional column like ask_vwap may
    # legitimately be NaN on individual bars and shouldn't drop the row.
    df = df.dropna(subset=list(_OHLCV_LOWER))

    # Each daily file is already monotonic — only sort if a concatenation
    # upstream broke that invariant.
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()

    # Cap volume to QUANTITY_MAX to avoid NautilusTrader overflow.
    df["volume"] = df["volume"].clip(upper=QUANTITY_MAX)

    return df


def concat_side(files: list[str], timestamp_column: str = "ts",
                required_columns: list[str] | None = None,
                optional_columns: list[str] | None = None,
                delimiter: str = ",",
                max_workers: int | None = None) -> pd.DataFrame:
    """Concatenate daily CSVs for one side (ASK or BID) in timestamp order,
    dropping overlapping-midnight duplicates.

    Files are loaded in parallel through a thread pool — pandas releases
    the GIL inside the C parser, so I/O on N files overlaps cleanly.
    ``ThreadPoolExecutor.map`` preserves input order, so the chronological
    ordering from the scanner flows through to the concat and the global
    sort is usually a no-op (only kicks in if concatenation broke
    monotonicity, e.g. overlapping ranges across files).

    Parameters
    ----------
    max_workers : int | None
        Override the pool size (default: capped at the module constant
        :data:`_CONCAT_SIDE_MAX_WORKERS`). Pass ``1`` to force serial
        loading — useful for benchmarks and disk-bound HDD setups.
    """
    if not files:
        raise ValueError("No loadable CSVs for side")

    def _safe_load(path: str) -> pd.DataFrame | None:
        try:
            return load_csv(path, timestamp_column=timestamp_column,
                            required_columns=required_columns,
                            optional_columns=optional_columns,
                            delimiter=delimiter)
        except Exception as e:
            logger.warning("csv_load skip %s: %s", Path(path).name, e)
            return None

    workers = (max_workers if max_workers is not None
               else min(_CONCAT_SIDE_MAX_WORKERS, len(files)))

    if workers <= 1 or len(files) == 1:
        loaded: list[pd.DataFrame | None] = [_safe_load(p) for p in files]
    else:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="csv_load") as ex:
            loaded = list(ex.map(_safe_load, files))

    parts = [df for df in loaded if df is not None]
    if not parts:
        raise ValueError("No loadable CSVs for side")
    df = pd.concat(parts, copy=False)
    if not df.index.is_monotonic_increasing:
        df = df.sort_index()
    df = df[~df.index.duplicated(keep="first")]
    return df


def _merge_ask_bid_to_mid(ask_df: pd.DataFrame, bid_df: pd.DataFrame) -> pd.DataFrame:
    """Row-wise MID from ASK + BID OHLCV frames.

    O/H/L/C = (ask + bid) / 2, volume = ask + bid. Inner-join on the
    timestamp index, so rows present on only one side are dropped.

    Any extra (non-OHLCV) columns present on either side are carried through
    to the MID frame with their original per-side values — e.g. for
    commodity data this preserves ``ask_vwap`` (from the ASK side) and
    ``bid_vwap`` (from the BID side) on the merged result.
    """
    common_idx = ask_df.index.intersection(bid_df.index)
    ask_aligned = ask_df.loc[common_idx]
    bid_aligned = bid_df.loc[common_idx]
    ohlc = ["open", "high", "low", "close"]
    mid = (ask_aligned[ohlc] + bid_aligned[ohlc]) * 0.5
    mid["volume"] = (ask_aligned["volume"] + bid_aligned["volume"]).clip(upper=QUANTITY_MAX)
    # Carry per-side extras through unchanged. ASK-side wins on name collisions
    # (none expected: ask_vwap vs bid_vwap are distinctly named in practice).
    base_cols = {*ohlc, "volume"}
    for col in ask_aligned.columns:
        if col not in base_cols:
            mid[col] = ask_aligned[col]
    for col in bid_aligned.columns:
        if col not in base_cols and col not in mid.columns:
            mid[col] = bid_aligned[col]
    return mid


def load_pair_mid(entry: dict, timestamp_column: str = "ts",
                  required_columns: list[str] | None = None,
                  optional_columns: list[str] | None = None,
                  delimiter: str = ",") -> pd.DataFrame:
    """Load one pair's ASK and BID file lists and return a merged MID OHLCV frame."""
    ask_files = entry.get("ask_files") or []
    bid_files = entry.get("bid_files") or []
    if not ask_files or not bid_files:
        raise ValueError(
            f"load_pair_mid requires both ask_files and bid_files in the entry "
            f"(got ask={len(ask_files)}, bid={len(bid_files)})"
        )
    ask_df = concat_side(ask_files, timestamp_column, required_columns,
                         optional_columns, delimiter)
    bid_df = concat_side(bid_files, timestamp_column, required_columns,
                         optional_columns, delimiter)
    return _merge_ask_bid_to_mid(ask_df, bid_df)
