"""
Flask backend for the M_Cube Crypto Dashboard.

Provides REST API endpoints that reuse the existing core/ modules
for data loading, visualization, backtesting, and report generation.

Run with: python server.py
"""

import json
import sys
import threading
import traceback
import uuid
from datetime import datetime
from pathlib import Path

import time as _time

from flask import Flask, jsonify, request, send_from_directory, Response, stream_with_context
from flask_cors import CORS
import pandas as pd

# Project root directory (server.py lives at the root)
PROJECT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_DIR))

from core.csv_loader import DEFAULT_CSV_FOLDER, scan_csv_folder, get_display_label
from core.nautilus_loader import load_csv_and_store, load_catalog
from core.backtest_runner import run_backtest, run_portfolio_backtest, _run_single_backtest_task
from core.report_generator import generate_report, build_orderbook_dataframe, build_logs_dataframe
from core.custom_strategy_loader import (
    get_merged_registry, sanitize_filename, validate_and_load_strategy,
    get_strategy_template, get_strategy_guidelines,
)

app = Flask(__name__, static_folder="static", static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB max upload
CORS(app)


@app.errorhandler(Exception)
def handle_exception(e):
    """Return JSON for any unhandled server error."""
    return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500

# Default paths
CATALOG_PATH = str(PROJECT_DIR / "catalog")
CUSTOM_STRATEGIES_DIR = PROJECT_DIR / "custom_strategies"
REPORTS_DIR = PROJECT_DIR / "reports"

# {absolute_filename: (mtime, strategy_name_or_None)} — avoids re-reading every
# custom strategy file on each upload. Invalidates by mtime, so external edits
# still get picked up.
_STRATEGY_NAME_CACHE: dict[str, tuple[float, str | None]] = {}


def _read_strategy_name(py_path: Path) -> str | None:
    """Return the STRATEGY_NAME declared in a custom strategy file, or None.

    Reads the file lazily and caches by mtime so repeated duplicate-name
    checks across uploads don't re-parse unchanged files.
    """
    key = str(py_path)
    try:
        mtime = py_path.stat().st_mtime
    except OSError:
        return None
    cached = _STRATEGY_NAME_CACHE.get(key)
    if cached is not None and cached[0] == mtime:
        return cached[1]
    name: str | None = None
    try:
        content = py_path.read_text(encoding="utf-8")
    except Exception:
        _STRATEGY_NAME_CACHE[key] = (mtime, None)
        return None
    for line in content.splitlines():
        line = line.strip()
        if line.startswith("STRATEGY_NAME") and "=" in line:
            name = line.split("=", 1)[1].strip().strip("\"'")
            break
    _STRATEGY_NAME_CACHE[key] = (mtime, name)
    return name


# ─── Static files ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/reports/<path:filename>")
def download_report(filename):
    """Serve a generated report file from the active user's reports dir."""
    safe_name = Path(filename).name  # prevent directory traversal
    user_dir = _user_reports_dir(_resolve_user_id())
    return send_from_directory(str(user_dir), safe_name, as_attachment=True)


# ─── Orderbook API ───────────────────────────────────────────────────────────

@app.route("/api/orderbook/list")
def api_orderbook_list():
    """List the active user's saved orderbook CSV files."""
    user_dir = _user_reports_dir(_resolve_user_id())
    files = sorted(
        [f.name for f in user_dir.glob("order_book_*.csv")],
        key=lambda n: (user_dir / n).stat().st_mtime,
        reverse=True,
    )
    return jsonify({"files": files})


@app.route("/api/orderbook/load")
def api_orderbook_load():
    """Load one of the active user's saved orderbook CSVs."""
    filename = request.args.get("file", "")
    safe_name = Path(filename).name  # prevent directory traversal
    if not safe_name.startswith("order_book_") or not safe_name.endswith(".csv"):
        return jsonify({"error": "Invalid file name"}), 400
    user_dir = _user_reports_dir(_resolve_user_id())
    filepath = user_dir / safe_name
    if not filepath.exists():
        return jsonify({"error": "File not found"}), 404
    df = pd.read_csv(filepath)
    records = df.fillna("").to_dict(orient="records")
    return jsonify({"data": records})


# ─── Catalog API ─────────────────────────────────────────────────────────────

@app.route("/api/catalog/status")
def catalog_status():
    """Return catalog status and available data types."""
    catalog_path = request.args.get("path", CATALOG_PATH)
    if not Path(catalog_path).exists():
        return jsonify({"exists": False, "data_types": []})
    try:
        catalog = load_catalog(catalog_path)
        data_types = catalog.list_data_types()
        return jsonify({"exists": True, "data_types": data_types})
    except Exception as e:
        return jsonify({"exists": False, "error": str(e)})


# ─── Data Formats, Asset Classes & Configured Adapters API ─────────────────

@app.route("/api/data-formats")
def get_data_formats():
    """Read data format configs from individual JSON files per asset class."""
    formats_dir = PROJECT_DIR / "adapter_admin" / "data_formats"
    formats = {}
    if formats_dir.exists():
        for f in sorted(formats_dir.glob("*.json")):
            try:
                formats[f.stem] = json.loads(f.read_text(encoding="utf-8"))
            except Exception:
                continue
    return jsonify({"formats": formats})



@app.route("/api/asset-classes")
def get_asset_classes():
    """Return asset classes from NautilusTrader."""
    try:
        from nautilus_trader.model.enums import AssetClass
        classes = [member.name.lower() for member in AssetClass]
    except ImportError:
        classes = []
    return jsonify({"asset_classes": classes})


@app.route("/api/configured-adapters")
def get_configured_adapters():
    """Read saved adapter configs and return venues grouped by asset class."""
    config_dir = PROJECT_DIR / "adapter_admin" / "adapters_config"
    adapters_by_class = {}
    if config_dir.exists():
        for f in sorted(config_dir.glob("*.json")):
            try:
                config = json.loads(f.read_text(encoding="utf-8"))
                asset_class = config.get("asset_class", "crypto")
                venue = config.get("venue", "")
                if venue:
                    adapters_by_class.setdefault(asset_class, []).append(venue)
            except Exception:
                continue
    return jsonify({"adapters": adapters_by_class})


# ─── CSV Scan / Load API ────────────────────────────────────────────────────

@app.route("/api/csv/scan")
def csv_scan():
    """Scan CSV folder and return available files."""
    folder = request.args.get("folder", DEFAULT_CSV_FOLDER)
    entries = scan_csv_folder(folder)
    for entry in entries:
        entry["label"] = get_display_label(entry)
    return jsonify({"entries": entries, "count": len(entries), "folder": folder})


# ─── CSV ingest background-job state ────────────────────────────────────────
#
# MID synthesis re-reads thousands of daily files; doing it inline blocks
# the HTTP response for tens of seconds per pair. Instead, ASK and BID
# entries are processed inline (fast — a single side concat) and any
# entry with side == "MID" is dispatched to a worker thread. The client
# polls /api/csv/jobs/<job_id> for completion. State is in-memory, single
# Flask process — fine for the current single-user setup; restart wipes
# pending jobs (the user just re-submits).

DOJI_WARN_THRESHOLD = 0.30  # above this, the precision is almost certainly wrong

_csv_jobs: dict[str, dict] = {}
_csv_jobs_lock = threading.Lock()


def _build_load_row(entry: dict, result: dict, asset_class: str) -> dict:
    """Shape the load_csv_and_store result into the row format the UI expects."""
    df = result["dataframe"]
    row = {
        "symbol": result["symbol"],
        "name": result["name"],
        "num_bars": result["num_bars"],
        "date_start": str(df.index[0].date()),
        "date_end": str(df.index[-1].date()),
        "latest_close": float(df["close"].iloc[-1]),
        "sample_data": df.tail(10).reset_index().to_dict(orient="records"),
        "doji_rate": round(result.get("doji_rate", 0.0), 4),
        "price_precision": result.get("price_precision"),
        "side": entry.get("side"),
    }
    if row["doji_rate"] > DOJI_WARN_THRESHOLD:
        row["warning"] = (
            f"{row['doji_rate']*100:.1f}% of ingested bars have open==close. "
            f"The configured price_precision ({row['price_precision']}) is likely "
            f"too low for this data source — stored bars lose tick-level detail. "
            f"Check adapter_admin/data_formats/{asset_class}.json and confirm "
            f"price_precision matches the decimals in the source CSV."
        )
    return row


def _run_csv_load_job(job_id: str, entry: dict, catalog_path: str,
                      venue: str, data_format: dict | None,
                      asset_class: str) -> None:
    """Worker target for background MID ingest. Always lands the job in a
    terminal state (success or error) so the polling client can stop."""
    with _csv_jobs_lock:
        _csv_jobs[job_id]["status"] = "running"
        _csv_jobs[job_id]["started_at"] = _time.time()
    try:
        result = load_csv_and_store(csv_entry=entry, catalog_path=catalog_path,
                                    venue=venue, data_format=data_format)
        row = _build_load_row(entry, result, asset_class)
        with _csv_jobs_lock:
            _csv_jobs[job_id]["status"] = "success"
            _csv_jobs[job_id]["result"] = row
            _csv_jobs[job_id]["finished_at"] = _time.time()
    except Exception as e:
        with _csv_jobs_lock:
            _csv_jobs[job_id]["status"] = "error"
            _csv_jobs[job_id]["error"] = str(e)
            _csv_jobs[job_id]["finished_at"] = _time.time()


@app.route("/api/csv/load", methods=["POST"])
def csv_load():
    """Load selected CSV entries into the catalog.

    Entries with ``side == "MID"`` are dispatched to background threads and
    returned as ``pending_jobs``; the rest are processed inline. Clients
    poll ``/api/csv/jobs/<job_id>`` for the pending entries.
    """
    data = request.json
    entries = data.get("entries", [])
    catalog_path = data.get("catalog_path", CATALOG_PATH)
    venue = (data.get("venue", "BINANCE") or "BINANCE").upper().strip()
    asset_class = data.get("asset_class", "")

    # Load data format config for this asset class
    data_format = None
    if asset_class:
        fmt_file = PROJECT_DIR / "adapter_admin" / "data_formats" / f"{asset_class}.json"
        if fmt_file.exists():
            data_format = json.loads(fmt_file.read_text(encoding="utf-8"))

    results: list[dict] = []
    errors: list[dict] = []
    pending_jobs: list[dict] = []

    # Any entry that triggers a bulk daily-file ingest (FX ASK/BID/MID, or any
    # other aggregated layout) goes to a background thread so the HTTP
    # response returns immediately. Single-file entries (legacy crypto)
    # stay inline because they finish in <100ms.
    def _is_bulk(e: dict) -> bool:
        return bool(e.get("aggregated") or e.get("ask_files")
                    or e.get("bid_files") or e.get("files"))

    inline_entries = [e for e in entries if not _is_bulk(e)]
    background_entries = [e for e in entries if _is_bulk(e)]

    for entry in inline_entries:
        try:
            result = load_csv_and_store(csv_entry=entry, catalog_path=catalog_path,
                                        venue=venue, data_format=data_format)
            results.append(_build_load_row(entry, result, asset_class))
        except Exception as e:
            errors.append({"symbol": entry.get("symbol", "?"),
                           "side": entry.get("side"), "error": str(e)})

    for entry in background_entries:
        job_id = uuid.uuid4().hex
        with _csv_jobs_lock:
            _csv_jobs[job_id] = {
                "status": "pending",
                "symbol": entry.get("symbol", "?"),
                "name": entry.get("name", ""),
                "side": entry.get("side"),
                "filename": entry.get("filename"),
                "queued_at": _time.time(),
            }
        thread = threading.Thread(
            target=_run_csv_load_job,
            args=(job_id, entry, catalog_path, venue, data_format, asset_class),
            daemon=True,
        )
        thread.start()
        pending_jobs.append({
            "job_id": job_id,
            "symbol": entry.get("symbol", "?"),
            "name": entry.get("name", ""),
            "side": entry.get("side"),
            "filename": entry.get("filename"),
        })

    return jsonify({"results": results, "errors": errors,
                    "pending_jobs": pending_jobs})


@app.route("/api/csv/jobs/<job_id>", methods=["GET"])
def csv_job_status(job_id: str):
    """Poll the status of a background CSV-ingest job."""
    with _csv_jobs_lock:
        job = _csv_jobs.get(job_id)
        if job is None:
            return jsonify({"error": "unknown job_id"}), 404
        # Snapshot — we don't hold the lock across the JSON serialization.
        snapshot = dict(job)
    return jsonify(snapshot)


# ─── Data View API ───────────────────────────────────────────────────────────

_PARQUET_NAME_RE = __import__("re").compile(
    r"^(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}-\d{9}Z_"
    r"(\d{4}-\d{2}-\d{2})T\d{2}-\d{2}-\d{2}-\d{9}Z\.parquet$"
)


def _bar_type_range_from_files(bar_dir) -> dict | None:
    """Return {'start_date','end_date'} parsed from parquet filenames.

    ParquetDataCatalog stores bars in files named
    ``<start_iso>_<end_iso>.parquet`` — so a directory listing alone tells us
    the date range without opening a single row. This is 100-1000x faster
    than `catalog.bars()` + in-memory min/max scan on multi-million-row FX
    datasets. Returns None if no parseable filenames are found (caller
    should fall back to the slow path).
    """
    min_start = None
    max_end = None
    for f in bar_dir.glob("*.parquet"):
        m = _PARQUET_NAME_RE.match(f.name)
        if not m:
            continue
        s, e = m.group(1), m.group(2)
        if min_start is None or s < min_start:
            min_start = s
        if max_end is None or e > max_end:
            max_end = e
    if min_start is None:
        return None
    return {"start_date": min_start, "end_date": max_end}


@app.route("/api/data/bar_types")
def get_bar_types():
    """Get available bar types from catalog, with date ranges.

    Fast-path: parse parquet filenames under catalog/data/bar/<bar_type>/
    instead of loading every bar. On the FX catalog (~24M bars) this is
    sub-second vs. ~3 minutes for the old scan path. Falls back to the
    bar-level scan only for directories with non-standard filenames.
    """
    catalog_path = request.args.get("path", CATALOG_PATH)
    try:
        bar_root = Path(catalog_path) / "data" / "bar"
        if not bar_root.exists():
            return jsonify({"bar_types": [], "bar_type_details": {}})

        bar_type_details = {}
        fallback_needed = []
        for bt_dir in sorted(bar_root.iterdir()):
            if not bt_dir.is_dir():
                continue
            info = _bar_type_range_from_files(bt_dir)
            if info:
                bar_type_details[bt_dir.name] = info
            else:
                fallback_needed.append(bt_dir.name)

        # Only fall back to bar-level scanning for the subset of bar types
        # whose filenames didn't parse — keeps the common case fast while
        # preserving correctness for any weird legacy layouts.
        if fallback_needed:
            catalog = load_catalog(catalog_path)
            for bt in fallback_needed:
                try:
                    bars = catalog.bars(bar_types=[bt])
                except Exception:
                    bars = []
                if not bars:
                    continue
                mn = min(b.ts_event for b in bars)
                mx = max(b.ts_event for b in bars)
                bar_type_details[bt] = {
                    "start_date": str(pd.Timestamp(mn, unit="ns", tz="UTC").date()),
                    "end_date":   str(pd.Timestamp(mx, unit="ns", tz="UTC").date()),
                }

        bar_types = sorted(bar_type_details.keys())
        return jsonify({"bar_types": bar_types, "bar_type_details": bar_type_details})
    except Exception as e:
        return jsonify({"bar_types": [], "bar_type_details": {}, "error": str(e)})


@app.route("/api/data/bars")
def get_bars():
    """Get bar data for a specific bar type.

    Query params
    ------------
    bar_type : required
    start, end : ISO dates (YYYY-MM-DD). Pushed down to the parquet query
        so only the matching rows are read — critical for multi-million-bar
        FX streams where returning the full range would send 500 MB of JSON.
    limit : int, default 5000. When the filtered range exceeds this, the
        server downsamples by time-bucket (open of bucket, max high, min
        low, close of bucket, sum volume) so chart rendering stays fast
        while preserving OHLC shape. Response carries `downsampled: true`
        and `raw_count` so the UI can display "X raw bars → Y points".
    """
    catalog_path = request.args.get("path", CATALOG_PATH)
    bar_type_str = request.args.get("bar_type", "")
    start_str = request.args.get("start", "")
    end_str = request.args.get("end", "")
    try:
        limit = int(request.args.get("limit", 5000))
    except ValueError:
        limit = 5000
    limit = max(100, min(limit, 100_000))

    if not bar_type_str:
        return jsonify({"error": "bar_type parameter required"}), 400

    try:
        catalog = load_catalog(catalog_path)
        start_arg = pd.Timestamp(start_str, tz="UTC") if start_str else None
        end_arg = (pd.Timestamp(end_str, tz="UTC") + pd.Timedelta(days=1) - pd.Timedelta(nanoseconds=1)) if end_str else None
        bars = catalog.bars(bar_types=[bar_type_str], start=start_arg, end=end_arg)

        if not bars:
            return jsonify({"data": [], "count": 0, "raw_count": 0, "downsampled": False})

        raw_count = len(bars)
        # Bars come sorted from the parquet reader; preserve order.
        if raw_count <= limit:
            data = [{
                "timestamp": pd.Timestamp(b.ts_event, unit="ns", tz="UTC").isoformat(),
                "open": float(b.open), "high": float(b.high),
                "low": float(b.low), "close": float(b.close),
                "volume": float(b.volume),
            } for b in bars]
            return jsonify({
                "data": data, "count": len(data),
                "raw_count": raw_count, "downsampled": False,
            })

        # Downsample: chunk into ceil(raw_count / limit)-sized buckets and
        # emit one OHLC per bucket. Preserves visible volatility far better
        # than plain stride-sampling which drops wicks.
        stride = (raw_count + limit - 1) // limit
        out = []
        for i in range(0, raw_count, stride):
            chunk = bars[i:i + stride]
            first, last = chunk[0], chunk[-1]
            hi = max(float(b.high) for b in chunk)
            lo = min(float(b.low)  for b in chunk)
            vol = sum(float(b.volume) for b in chunk)
            out.append({
                "timestamp": pd.Timestamp(first.ts_event, unit="ns", tz="UTC").isoformat(),
                "open":  float(first.open),
                "high":  hi,
                "low":   lo,
                "close": float(last.close),
                "volume": vol,
            })
        return jsonify({
            "data": out, "count": len(out),
            "raw_count": raw_count, "downsampled": True,
            "bucket_size": stride,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ─── Strategies API ─────────────────────────────────────────────────────────

@app.route("/api/strategies")
def get_strategies():
    """Get all available strategies (built-in + custom)."""
    CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)
    merged_registry, warnings = get_merged_registry(CUSTOM_STRATEGIES_DIR)

    strategies = {}
    for name, entry in merged_registry.items():
        strategies[name] = {
            "description": entry["description"],
            "params": entry["params"],
        }

    return jsonify({"strategies": strategies, "warnings": warnings})


# ─── Custom Strategy Management API ─────────────────────────────────────────

@app.route("/api/custom_strategies/list")
def list_custom_strategies():
    """List all uploaded custom strategy files."""
    CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)
    files = sorted(
        f.name for f in CUSTOM_STRATEGIES_DIR.glob("*.py")
        if not f.name.startswith("__")
    )
    return jsonify({"files": files, "count": len(files)})


@app.route("/api/custom_strategies/upload", methods=["POST"])
def upload_custom_strategy():
    """Upload and validate a custom strategy .py file.

    Validation runs in a subprocess to prevent NautilusTrader's Rust bindings
    from crashing the main Flask process.
    """
    import subprocess, json as _json

    CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename or not file.filename.endswith(".py"):
        return jsonify({"error": "File must be a .py file"}), 400

    safe_name = sanitize_filename(file.filename)
    if not safe_name.endswith(".py"):
        safe_name += ".py"
    dest_path = CUSTOM_STRATEGIES_DIR / safe_name
    file_existed = dest_path.exists()

    file.save(str(dest_path))

    # Run validation in a subprocess to avoid crashing the server
    parent_dir_str = str(PROJECT_DIR).replace("\\", "\\\\")
    dest_path_str = str(dest_path).replace("\\", "\\\\")
    validate_script = (
        'import sys, json\n'
        f'sys.path.insert(0, "{parent_dir_str}")\n'
        'try:\n'
        '    from core.custom_strategy_loader import validate_and_load_strategy\n'
        '    from pathlib import Path\n'
        f'    fp = Path("{dest_path_str}")\n'
        '    entry = validate_and_load_strategy(fp)\n'
        '    mod_name = "custom_strategy_" + fp.stem\n'
        '    mod = sys.modules.get(mod_name)\n'
        '    strat_name = getattr(mod, "STRATEGY_NAME", fp.stem) if mod else fp.stem\n'
        '    print(json.dumps({"ok": True, "strategy_name": strat_name}))\n'
        'except ValueError as e:\n'
        '    print(json.dumps({"ok": False, "error": str(e)}))\n'
        'except Exception as e:\n'
        '    print(json.dumps({"ok": False, "error": str(e)}))\n'
    )

    try:
        result = subprocess.run(
            [sys.executable, "-c", validate_script],
            capture_output=True, text=True, timeout=30,
        )
        output = result.stdout.strip()
        if not output:
            stderr = result.stderr.strip()
            dest_path.unlink(missing_ok=True)
            return jsonify({"error": f"Validation failed:\n{stderr[:1000]}"}), 400

        vresult = _json.loads(output)

        if not vresult.get("ok"):
            dest_path.unlink(missing_ok=True)
            return jsonify({"error": vresult.get("error", "Validation failed")}), 400

        strat_name = vresult["strategy_name"]

        # Check for duplicate names among other custom files. _read_strategy_name
        # caches per-file by mtime so this loop avoids re-reading unchanged files
        # on subsequent uploads.
        for other_file in CUSTOM_STRATEGIES_DIR.glob("*.py"):
            if other_file.name == safe_name or other_file.name.startswith("__"):
                continue
            if _read_strategy_name(other_file) == strat_name:
                dest_path.unlink()
                return jsonify({"error": f"A custom strategy named '{strat_name}' already exists."}), 400

        msg = f"Replaced existing file: {safe_name}" if file_existed else f"Strategy '{strat_name}' loaded successfully!"
        return jsonify({"success": True, "message": msg, "strategy_name": strat_name, "filename": safe_name})

    except subprocess.TimeoutExpired:
        dest_path.unlink(missing_ok=True)
        return jsonify({"error": "Validation timed out (30s)"}), 400
    except Exception as e:
        dest_path.unlink(missing_ok=True)
        return jsonify({"error": f"Unexpected error: {e}"}), 500


@app.route("/api/custom_strategies/delete", methods=["POST"])
def delete_custom_strategy():
    """Delete a custom strategy file."""
    data = request.json
    filename = data.get("filename", "")
    if not filename:
        return jsonify({"error": "filename required"}), 400

    file_path = CUSTOM_STRATEGIES_DIR / sanitize_filename(filename)
    if file_path.exists():
        file_path.unlink()
        return jsonify({"success": True, "message": f"Deleted {filename}"})
    return jsonify({"error": "File not found"}), 404


@app.route("/api/custom_strategies/template")
def download_template():
    """Return the strategy template file content."""
    content = get_strategy_template()
    return Response(content, mimetype="text/x-python",
                    headers={"Content-Disposition": "attachment; filename=custom_strategy_template.py"})


@app.route("/api/custom_strategies/guidelines")
def strategy_guidelines():
    """Return strategy guidelines as markdown."""
    return jsonify({"guidelines": get_strategy_guidelines()})


# ─── Backtest API ────────────────────────────────────────────────────────────

def _serialize_backtest_result(results: dict, strategy_name: str,
                                user_id: str | None = None) -> dict:
    """Convert a single backtest result (with DataFrames) to a JSON-serializable dict."""
    serializable = {
        "starting_capital": results["starting_capital"],
        "final_balance": results["final_balance"],
        "total_pnl": results["total_pnl"],
        "total_return_pct": results["total_return_pct"],
        "total_orders": results["total_orders"],
        "total_trades": results["total_trades"],
        "wins": results["wins"],
        "losses": results["losses"],
        "win_rate": results["win_rate"],
        "total_days": results.get("total_days", 0),
        "winning_days": results.get("winning_days", 0),
        "losing_days": results.get("losing_days", 0),
        "win_pct_days": results.get("win_pct_days", 0.0),
        "loss_pct_days": results.get("loss_pct_days", 0.0),
        "equity_curve_ts": results.get("equity_curve_ts", []),
        "warning": results.get("warning"),
    }

    # Convert reports to lists of dicts. Skip the full DataFrame copy + per-column
    # apply — instead, convert to records first and stringify non-primitive values
    # per cell (same net effect, one pass, no column-wise Series allocation).
    _primitive = (int, float, str, bool, type(None))
    for report_key in ["fills_report", "positions_report", "account_report"]:
        report = results.get(report_key)
        if report is not None and not report.empty:
            records = report.reset_index().fillna("").to_dict(orient="records")
            for rec in records:
                for k, v in rec.items():
                    if not isinstance(v, _primitive):
                        rec[k] = str(v)
            serializable[report_key] = records
        else:
            serializable[report_key] = []

    # Build order book and logs
    safe_name = sanitize_filename(strategy_name)
    ob_df = build_orderbook_dataframe({safe_name: results}, user_id=user_id)
    serializable["order_book"] = ob_df.fillna("").to_dict(orient="records") if not ob_df.empty else []

    logs_df = build_logs_dataframe({safe_name: results})
    serializable["logs"] = logs_df.fillna("").to_dict(orient="records") if not logs_df.empty else []

    return serializable


@app.route("/api/backtest/run-stream", methods=["POST"])
def run_backtest_stream():
    """Run backtests with real-time progress streaming (newline-delimited JSON)."""
    user_or_resp = _get_user_or_401()
    if not isinstance(user_or_resp, dict):
        return user_or_resp
    user_id_for_run = user_or_resp["user_id"]
    data = request.json
    starting_capital = data.get("starting_capital", 100000.0)
    strategies_config = data.get("strategies", {})
    catalog_path = data.get("catalog_path", CATALOG_PATH)

    instruments = data.get("instruments", [])
    if not instruments:
        bar_type_str = data.get("bar_type", "")
        if bar_type_str:
            instruments = [{"bar_type": bar_type_str}]

    if not instruments or not strategies_config:
        return jsonify({"error": "instruments and strategies required"}), 400

    # Allowlist enforcement (matches /api/portfolios/save & /backtest).
    # Reject up-front so we don't spin up worker processes for a request
    # that's going to fail per-symbol anyway.
    allowed = user_or_resp.get("allowed_instruments")
    if isinstance(allowed, list):
        allowed_set = {str(s).upper() for s in allowed if isinstance(s, str)}
        for inst in instruments:
            bt = inst.get("bar_types") or inst.get("bar_type", "")
            primary_bt = bt[0] if isinstance(bt, list) else bt
            sym = (_symbol_from_bar_type(primary_bt) or "").upper()
            if sym not in allowed_set:
                return jsonify({"error": (
                    f"Instrument '{sym}' is not in user '{user_id_for_run}'s "
                    f"allowed_instruments. Ask admin to update the allowlist."
                )}), 403

    def generate():
        import os
        from concurrent.futures import ProcessPoolExecutor, as_completed

        CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)
        REPORTS_DIR.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%d_%B_%Y").lower()

        total_runs = len(instruments) * len(strategies_config)
        completed_runs = 0

        yield json.dumps({"event": "start", "total": total_runs,
                          "instruments": len(instruments),
                          "strategies": len(strategies_config)}) + "\n"

        # Per-instrument state indexed by position in instruments list
        inst_meta = []
        for inst_cfg in instruments:
            bar_type_str = inst_cfg.get("bar_types") or inst_cfg.get("bar_type", "")
            primary_bt = bar_type_str[0] if isinstance(bar_type_str, list) else bar_type_str
            inst_meta.append({
                "bar_type_str": bar_type_str,
                "primary_bt": primary_bt,
                "start_date": inst_cfg.get("start_date") or None,
                "end_date": inst_cfg.get("end_date") or None,
                "strategies_serialized": {},
                "strategies_raw": {},
                "pending": len(strategies_config),
            })

        instrument_results = {}
        errors = []
        custom_dir_str = str(CUSTOM_STRATEGIES_DIR)

        # Match the portfolio path's worker cap (was 8; 16-core boxes only used
        # half their cores). CPU-bound per task but the OS schedules fine.
        max_workers = min(total_runs, (os.cpu_count() or 2), 32)

        # LPT scheduling: submit the longest-expected tasks first so shorter
        # ones tail-fill behind them. History-aware — if we've seen this
        # (bar_type, strategy) before, use the observed per-day runtime
        # instead of the span heuristic. Closes the same-span-different-work
        # gap that made USDJPY the tail on Forex runs.
        from core import runtime_history
        history = runtime_history.load()

        def _span_days(meta):
            if meta["start_date"] and meta["end_date"]:
                try:
                    return max(1, (pd.Timestamp(meta["end_date"]) - pd.Timestamp(meta["start_date"])).days)
                except Exception:
                    return 1
            return 1

        def _duration_estimate(inst_idx, strategy_name):
            meta = inst_meta[inst_idx]
            span = _span_days(meta)
            hist = runtime_history.estimate(history, meta["primary_bt"], strategy_name, span)
            if hist is not None:
                return hist
            mult = 1.2 if "bollinger" in strategy_name.lower() else 1.0
            return span * mult

        submit_order = [
            (inst_idx, strategy_name)
            for inst_idx in range(len(inst_meta))
            for strategy_name in strategies_config.keys()
        ]
        submit_order.sort(key=lambda x: _duration_estimate(*x), reverse=True)

        t_start = _time.time()
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for inst_idx, strategy_name in submit_order:
                meta = inst_meta[inst_idx]
                cfg = strategies_config[strategy_name]
                future = executor.submit(
                    _run_single_backtest_task,
                    catalog_path=catalog_path,
                    bar_type_str=meta["bar_type_str"],
                    strategy_name=strategy_name,
                    strategy_params=cfg.get("params", {}),
                    trade_size=cfg.get("trade_size", 1),
                    starting_capital=starting_capital,
                    start_date=meta["start_date"],
                    end_date=meta["end_date"],
                    custom_strategies_dir=custom_dir_str,
                    user_id=user_id_for_run,
                )
                futures[future] = (inst_idx, strategy_name)
                yield json.dumps({"event": "progress", "status": "running",
                                  "completed": completed_runs, "total": total_runs,
                                  "current_instrument": meta["primary_bt"],
                                  "current_strategy": strategy_name}) + "\n"

            print(f"[Backtest] Submitted {total_runs} backtests to {max_workers} workers.")

            for future in as_completed(futures):
                inst_idx, strategy_name = futures[future]
                meta = inst_meta[inst_idx]
                primary_bt = meta["primary_bt"]
                try:
                    results = future.result()
                    meta["strategies_raw"][strategy_name] = results
                    meta["strategies_serialized"][strategy_name] = _serialize_backtest_result(
                        results, strategy_name, user_id=user_id_for_run)
                    elapsed_sec = results.get("elapsed_seconds") if isinstance(results, dict) else None
                    if elapsed_sec is not None:
                        runtime_history.record(
                            history, primary_bt, strategy_name,
                            float(elapsed_sec), _span_days(meta),
                        )
                    completed_runs += 1
                    print(f"[Backtest] Completed {strategy_name} on {primary_bt} "
                          f"({completed_runs}/{total_runs}, {elapsed_sec}s)")
                    yield json.dumps({"event": "progress", "status": "complete",
                                      "completed": completed_runs, "total": total_runs,
                                      "current_instrument": primary_bt,
                                      "current_strategy": strategy_name,
                                      "elapsed_seconds": elapsed_sec}) + "\n"
                except Exception as e:
                    completed_runs += 1
                    print(f"[Backtest] ERROR {strategy_name} on {primary_bt}: {e}")
                    errors.append({"instrument": primary_bt, "strategy": strategy_name,
                                   "error": str(e), "traceback": traceback.format_exc()})
                    yield json.dumps({"event": "progress", "status": "error",
                                      "completed": completed_runs, "total": total_runs,
                                      "current_instrument": primary_bt,
                                      "current_strategy": strategy_name,
                                      "error": str(e)}) + "\n"

                meta["pending"] -= 1
                # When all strategies for an instrument have finished, write its reports
                if meta["pending"] == 0:
                    inst_strategies = meta["strategies_serialized"]
                    inst_strategies_raw = meta["strategies_raw"]
                    report_html = ""
                    inst_label = sanitize_filename(primary_bt.split("-")[0])

                    if inst_strategies:
                        user_dir = _user_reports_dir(user_id_for_run)
                        raw_name = f"{inst_label}_{'_'.join(inst_strategies.keys())}"
                        report_name = sanitize_filename(raw_name)
                        try:
                            report_html = generate_report(inst_strategies_raw, backtest_name=report_name,
                                                          user_id=user_id_for_run,
                                                          date_range={
                                                              "start": meta["start_date"] or "",
                                                              "end": meta["end_date"] or "",
                                                          })
                            report_path = user_dir / f"{report_name}_report.html"
                            report_path.write_text(report_html, encoding="utf-8")
                        except Exception as e:
                            errors.append({"instrument": primary_bt, "strategy": "HTML Report",
                                           "error": str(e), "traceback": traceback.format_exc()})

                        for strat_name, strat_results in inst_strategies.items():
                            safe_strat = sanitize_filename(strat_name)
                            prefix = f"{inst_label}_{safe_strat}_{timestamp}"
                            if strat_results.get("positions_report"):
                                pd.DataFrame(strat_results["positions_report"]).to_csv(
                                    user_dir / f"position_report_{prefix}.csv", index=False)
                            if strat_results.get("fills_report"):
                                pd.DataFrame(strat_results["fills_report"]).to_csv(
                                    user_dir / f"order_fill_report_{prefix}.csv", index=False)
                            if strat_results.get("account_report"):
                                pd.DataFrame(strat_results["account_report"]).to_csv(
                                    user_dir / f"account_report_{prefix}.csv", index=False)
                            if strat_results.get("order_book"):
                                pd.DataFrame(strat_results["order_book"]).to_csv(
                                    user_dir / f"order_book_{prefix}.csv", index=False)
                            if strat_results.get("logs"):
                                pd.DataFrame(strat_results["logs"]).to_csv(
                                    user_dir / f"backtest_{prefix}_logs.csv", index=False)

                    instrument_results[primary_bt] = {
                        "date_range": {"start": meta["start_date"] or "", "end": meta["end_date"] or ""},
                        "strategies": inst_strategies,
                        "report_html": report_html,
                        "report_name": inst_label,
                    }

        elapsed_total = _time.time() - t_start
        # Persist runtime history so the next LPT pass can use observed per-day
        # runtimes instead of the span heuristic. One JSON write per run.
        try:
            runtime_history.save(history)
        except Exception:
            pass
        # Final complete event (use default=str to handle Timestamp and other non-serializable types)
        yield json.dumps({"event": "complete",
                          "instrument_results": instrument_results,
                          "errors": errors}, default=str) + "\n"
        print(f"[Backtest] All {total_runs} backtests finished in {elapsed_total:.1f}s "
              f"(workers={max_workers}).")

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
        headers={"X-Content-Type-Options": "nosniff",
                 "Cache-Control": "no-cache, no-store, must-revalidate"},
    )


@app.route("/api/backtest/run", methods=["POST"])
def run_backtest_api():
    """Run backtests for selected instruments × strategies."""
    user_or_resp = _get_user_or_401()
    if not isinstance(user_or_resp, dict):
        return user_or_resp
    user_id_for_run = user_or_resp["user_id"]
    data = request.json
    starting_capital = data.get("starting_capital", 100000.0)
    strategies_config = data.get("strategies", {})
    catalog_path = data.get("catalog_path", CATALOG_PATH)

    # Support both old format (single bar_type) and new format (instruments array)
    instruments = data.get("instruments", [])
    if not instruments:
        bar_type_str = data.get("bar_type", "")
        if bar_type_str:
            instruments = [{"bar_type": bar_type_str}]

    if not instruments or not strategies_config:
        return jsonify({"error": "instruments and strategies required"}), 400

    # Allowlist enforcement before doing any catalog work.
    allowed = user_or_resp.get("allowed_instruments")
    if isinstance(allowed, list):
        allowed_set = {str(s).upper() for s in allowed if isinstance(s, str)}
        for inst in instruments:
            bt = inst.get("bar_types") or inst.get("bar_type", "")
            primary_bt = bt[0] if isinstance(bt, list) else bt
            sym = (_symbol_from_bar_type(primary_bt) or "").upper()
            if sym not in allowed_set:
                return jsonify({"error": (
                    f"Instrument '{sym}' is not in user '{user_id_for_run}'s "
                    f"allowed_instruments. Ask admin to update the allowlist."
                )}), 403

    CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)
    merged_registry, _ = get_merged_registry(CUSTOM_STRATEGIES_DIR)
    REPORTS_DIR.mkdir(exist_ok=True)
    timestamp = datetime.now().strftime("%d_%B_%Y").lower()

    instrument_results = {}
    errors = []

    for inst_cfg in instruments:
        # Support both single bar_type and multi bar_types
        bar_type_str = inst_cfg.get("bar_types") or inst_cfg.get("bar_type", "")
        start_date = inst_cfg.get("start_date") or None
        end_date = inst_cfg.get("end_date") or None
        primary_bt = bar_type_str[0] if isinstance(bar_type_str, list) else bar_type_str

        inst_strategies = {}
        inst_strategies_raw = {}

        for strategy_name, cfg in strategies_config.items():
            try:
                results = run_backtest(
                    catalog_path=catalog_path,
                    bar_type_str=bar_type_str,
                    strategy_name=strategy_name,
                    strategy_params=cfg.get("params", {}),
                    trade_size=cfg.get("trade_size", 1),
                    starting_capital=starting_capital,
                    registry=merged_registry,
                    start_date=start_date,
                    end_date=end_date,
                    user_id=user_id_for_run,
                )

                inst_strategies_raw[strategy_name] = results
                inst_strategies[strategy_name] = _serialize_backtest_result(
                    results, strategy_name, user_id=user_id_for_run)

            except Exception as e:
                errors.append({
                    "instrument": primary_bt,
                    "strategy": strategy_name,
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                })

        # Generate per-instrument HTML report and save CSVs
        report_html = ""
        inst_label = sanitize_filename(primary_bt.split("-")[0])  # e.g. BTCUSD.BINANCE

        if inst_strategies:
            user_dir = _user_reports_dir(user_id_for_run)
            # HTML report for this instrument
            raw_name = f"{inst_label}_{'_'.join(inst_strategies.keys())}"
            report_name = sanitize_filename(raw_name)
            try:
                report_html = generate_report(inst_strategies_raw, backtest_name=report_name,
                                              user_id=user_id_for_run,
                                              date_range={
                                                  "start": start_date or "",
                                                  "end": end_date or "",
                                              })
                report_path = user_dir / f"{report_name}_report.html"
                report_path.write_text(report_html, encoding="utf-8")
            except Exception as e:
                errors.append({"instrument": primary_bt, "strategy": "HTML Report",
                               "error": str(e), "traceback": traceback.format_exc()})

            # Save CSV reports per instrument×strategy
            for strat_name, strat_results in inst_strategies.items():
                safe_strat = sanitize_filename(strat_name)
                prefix = f"{inst_label}_{safe_strat}_{timestamp}"

                if strat_results.get("positions_report"):
                    pd.DataFrame(strat_results["positions_report"]).to_csv(
                        user_dir / f"position_report_{prefix}.csv", index=False)
                if strat_results.get("fills_report"):
                    pd.DataFrame(strat_results["fills_report"]).to_csv(
                        user_dir / f"order_fill_report_{prefix}.csv", index=False)
                if strat_results.get("account_report"):
                    pd.DataFrame(strat_results["account_report"]).to_csv(
                        user_dir / f"account_report_{prefix}.csv", index=False)
                if strat_results.get("order_book"):
                    pd.DataFrame(strat_results["order_book"]).to_csv(
                        user_dir / f"order_book_{prefix}.csv", index=False)
                if strat_results.get("logs"):
                    pd.DataFrame(strat_results["logs"]).to_csv(
                        user_dir / f"backtest_{prefix}_logs.csv", index=False)

        # Use primary bar type as key for results
        result_key = primary_bt
        instrument_results[result_key] = {
            "date_range": {"start": start_date or "", "end": end_date or ""},
            "strategies": inst_strategies,
            "report_html": report_html,
            "report_name": inst_label,
        }

    return jsonify({
        "instrument_results": instrument_results,
        "errors": errors,
    })


# ─── Portfolio API ──────────────────────────────────────────────────────────

from core.models import (
    PortfolioConfig, StrategySlotConfig, ExitConfig,
    portfolio_to_dict, portfolio_from_dict,
    save_portfolio, load_portfolio, list_portfolios, delete_portfolio,
)
from core.templates import get_templates, build_template

_PORTFOLIOS_ROOT = PROJECT_DIR / "portfolios"
PORTFOLIOS_DIR = str(_PORTFOLIOS_ROOT)  # Kept for back-compat with non-user-scoped callers

# One-shot migration: move legacy flat ``portfolios/*.json`` under
# ``portfolios/_default/``. Idempotent — no-op when already partitioned.
from core.migrate_users import migrate_portfolios as _migrate_portfolios, DEFAULT_USER_ID
_migrate_portfolios(_PORTFOLIOS_ROOT)

from core.users import (
    get_user as _get_user,
    list_users as _list_users,
    is_instrument_allowed as _is_instrument_allowed,
)
from core.venue_config import symbol_from_bar_type as _symbol_from_bar_type


def _resolve_user_id() -> str:
    """Read X-User-Id from the current request, falling back to _default.

    Identity-only: any caller can spoof the header. We accept missing
    headers for back-compat (legacy clients that don't know about users
    yet) and route them to the ``_default`` user, which inherits the
    pre-migration portfolios. To enforce a strict-401 policy later, swap
    this for ``_get_user_or_401``.
    """
    uid = (request.headers.get("X-User-Id") or "").strip() or DEFAULT_USER_ID
    user = _get_user(uid)
    return user["user_id"] if user else DEFAULT_USER_ID


def _get_user_or_401():
    """Return the user dict for X-User-Id, or a 401 ``Response``.

    Use this when an endpoint must reject unknown users outright (e.g.
    actions that mutate state). Returns either the user dict or a Flask
    response with status 401 — caller checks ``isinstance(..., dict)``.
    """
    uid = (request.headers.get("X-User-Id") or "").strip()
    if not uid:
        return jsonify({"error": "X-User-Id header required"}), 401
    user = _get_user(uid)
    if not user:
        return jsonify({"error": f"Unknown user: {uid}"}), 401
    return user


def _user_portfolios_dir(user_id: str) -> str:
    """Per-user portfolio directory; ensures it exists.

    All portfolio CRUD endpoints route through this so user-A's POST
    and user-B's POST land in different subdirectories of ``portfolios/``.
    Same shape as ``REPORTS_DIR / <user_id>`` for reports.
    """
    p = _PORTFOLIOS_ROOT / user_id
    p.mkdir(parents=True, exist_ok=True)
    return str(p)


def _user_reports_dir(user_id: str):
    """Per-user reports directory; ensures it exists.

    All CSV/HTML report writes route through this so two users running a
    portfolio with the same name don't overwrite each other. Read endpoints
    (orderbook list/load, /api/reports/<file>) scope through here too, so
    each user only sees their own historical reports.
    """
    p = REPORTS_DIR / user_id
    p.mkdir(parents=True, exist_ok=True)
    return p


def _check_portfolio_allowlist(portfolio_config, user) -> tuple[bool, str]:
    """Reject portfolios whose slots include symbols outside the user's allowlist.

    Returns ``(ok, error_message)``. ``user`` is the full registry dict
    (so we read ``allowed_instruments`` directly without a second lookup).
    Empty / null allowlist → always OK.
    """
    allowed = user.get("allowed_instruments")
    if not isinstance(allowed, list):
        return True, ""
    allowed_set = {str(s).upper() for s in allowed if isinstance(s, str)}
    for slot in portfolio_config.slots:
        sym = _symbol_from_bar_type(slot.bar_type_str) or ""
        if sym.upper() not in allowed_set:
            return False, (
                f"Instrument '{sym}' is not in user '{user.get('user_id')}'s "
                f"allowed_instruments. Ask admin to update the allowlist."
            )
    return True, ""


@app.route("/api/users/list")
def api_users_list():
    """Public-safe user list for the frontend picker.

    Only returns ``user_id`` and ``alias`` — no multipliers or allowlists
    leak through. Used by the first-load picker in static/js/app.js. The
    full registry (with multipliers and allowlists) is admin-only via
    ``adapter_admin/admin_server.py:/api/users`` on port 5001.
    """
    return jsonify({"users": _list_users()})


@app.route("/api/users/me")
def api_users_me():
    """Return the active user's own row (read-only).

    Surfaces what the server will actually use for this session —
    ``multiplier`` (applied to every order quantity) and
    ``allowed_instruments`` (whitelist enforced on save/backtest). Other
    users' rows are NOT exposed here; only the caller's own info.

    Falls back to ``_default`` when no/unknown header is sent, mirroring
    ``_resolve_user_id``. The 4 user-facing fields are returned bare —
    no ``_meta``, no other users.
    """
    uid = _resolve_user_id()
    user = _get_user(uid) or {}
    return jsonify({
        "user_id": user.get("user_id", uid),
        "alias": user.get("alias", uid),
        "multiplier": user.get("multiplier", 1.0),
        "allowed_instruments": user.get("allowed_instruments"),
    })


@app.route("/api/portfolios/list")
def api_list_portfolios():
    """List the current user's saved portfolios."""
    uid = _resolve_user_id()
    names = list_portfolios(_user_portfolios_dir(uid))
    return jsonify({"portfolios": names})


@app.route("/api/portfolios/load")
def api_load_portfolio():
    """Load one of the current user's portfolios by name."""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400
    uid = _resolve_user_id()
    try:
        config = load_portfolio(name, _user_portfolios_dir(uid))
        return jsonify({"portfolio": portfolio_to_dict(config)})
    except FileNotFoundError:
        return jsonify({"error": f"Portfolio '{name}' not found"}), 404


@app.route("/api/portfolios/save", methods=["POST"])
def api_save_portfolio():
    """Save a portfolio under the current user's directory.

    Rejects 401 for unknown users (mutating endpoint), 403 when any slot's
    symbol is outside the user's allowlist.
    """
    user_or_resp = _get_user_or_401()
    if not isinstance(user_or_resp, dict):
        return user_or_resp
    data = request.json
    try:
        config = portfolio_from_dict(data)
        ok, err = _check_portfolio_allowlist(config, user_or_resp)
        if not ok:
            return jsonify({"error": err}), 403
        path = save_portfolio(config, _user_portfolios_dir(user_or_resp["user_id"]))
        return jsonify({"success": True, "message": f"Portfolio '{config.name}' saved.", "path": str(path)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/portfolios/delete", methods=["POST"])
def api_delete_portfolio():
    """Delete one of the current user's portfolios."""
    user_or_resp = _get_user_or_401()
    if not isinstance(user_or_resp, dict):
        return user_or_resp
    name = request.json.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400
    if delete_portfolio(name, _user_portfolios_dir(user_or_resp["user_id"])):
        return jsonify({"success": True, "message": f"Portfolio '{name}' deleted."})
    return jsonify({"error": "Portfolio not found"}), 404


@app.route("/api/portfolios/templates")
def api_portfolio_templates():
    """List available portfolio templates."""
    templates = get_templates()
    result = {name: info["description"] for name, info in templates.items()}
    return jsonify({"templates": result})


@app.route("/api/portfolios/from-template", methods=["POST"])
def api_portfolio_from_template():
    """Build a portfolio from a template."""
    data = request.json
    template_name = data.get("template", "")
    bar_types = data.get("bar_types", [])
    if not template_name:
        return jsonify({"error": "template name required"}), 400
    try:
        config = build_template(template_name, bar_types)
        return jsonify({"portfolio": portfolio_to_dict(config)})
    except ValueError as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/portfolios/backtest", methods=["POST"])
def api_portfolio_backtest():
    """Run a portfolio backtest with streaming progress (parallel execution)."""
    user_or_resp = _get_user_or_401()
    if not isinstance(user_or_resp, dict):
        return user_or_resp
    user_id_for_run = user_or_resp["user_id"]
    data = request.json
    catalog_path = data.get("catalog_path", CATALOG_PATH)

    def generate():
        import time as _time
        import queue
        import threading

        try:
            config = portfolio_from_dict(data.get("portfolio", data))
            ok, err = _check_portfolio_allowlist(config, user_or_resp)
            if not ok:
                yield json.dumps({"event": "error", "error": err}) + "\n"
                return
            enabled_slots = config.enabled_slots

            if not enabled_slots:
                yield json.dumps({"event": "error", "error": "No enabled slots"}) + "\n"
                return

            CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)
            merged_registry, _ = get_merged_registry(CUSTOM_STRATEGIES_DIR)

            total_slots = len(enabled_slots)
            total_steps = total_slots + 2  # slots completing + reports + done

            # Send start event with slot details
            slot_info = []
            for slot in enabled_slots:
                inst = slot.bar_type_str.split("-")[0] if slot.bar_type_str else "N/A"
                capital_info = ""
                if config.allocation_mode == "percentage":
                    capital_info = f" ({slot.allocation_pct}%)"
                else:
                    capital_info = f" ({config.starting_capital / total_slots:,.0f})"
                slot_info.append({"slot_id": slot.slot_id,
                                  "display_name": slot.display_name + capital_info,
                                  "instrument": inst, "strategy": slot.strategy_name})

            yield json.dumps({"event": "start", "total": total_steps,
                              "slots": slot_info,
                              "allocation_mode": config.allocation_mode}) + "\n"

            # Run backtest in a background thread, use queue for progress
            progress_queue = queue.Queue()
            result_holder = [None]
            error_holder = [None]

            def on_slot_complete(slot_id):
                progress_queue.put({"type": "slot_done", "slot_id": slot_id})

            def run_backtest_thread():
                try:
                    results = run_portfolio_backtest(
                        catalog_path=catalog_path,
                        portfolio=config,
                        custom_strategies_dir=str(CUSTOM_STRATEGIES_DIR),
                        on_slot_complete=on_slot_complete,
                        user_id=user_id_for_run,
                    )
                    result_holder[0] = results
                except Exception as e:
                    error_holder[0] = e
                finally:
                    progress_queue.put({"type": "done"})

            t0 = _time.time()
            thread = threading.Thread(target=run_backtest_thread)
            thread.start()

            # Yield progress events as slots complete
            slots_completed = 0
            yield json.dumps({"event": "progress", "phase": "engine",
                              "completed": 0, "total": total_steps,
                              "message": f"Running {total_slots} strategies in parallel...",
                              "slots_completed": 0}) + "\n"

            while True:
                try:
                    msg = progress_queue.get(timeout=0.5)
                except queue.Empty:
                    continue

                if msg["type"] == "slot_done":
                    slots_completed += 1
                    # Find slot display name
                    slot_name = msg["slot_id"]
                    for si in slot_info:
                        if si["slot_id"] == msg["slot_id"]:
                            slot_name = si["display_name"]
                            break
                    pct = slots_completed
                    yield json.dumps({"event": "progress", "phase": "engine",
                                      "completed": pct, "total": total_steps,
                                      "message": f"{slot_name} completed ({slots_completed}/{total_slots})",
                                      "completed_slot_id": msg["slot_id"],
                                      "slots_completed": slots_completed}) + "\n"

                elif msg["type"] == "done":
                    break

            thread.join()
            elapsed = _time.time() - t0

            if error_holder[0]:
                raise error_holder[0]

            results = result_holder[0]

            # Progress: generating reports
            yield json.dumps({"event": "progress", "phase": "reports",
                              "completed": total_slots, "total": total_steps,
                              "message": f"All slots finished in {elapsed:.1f}s. Generating reports...",
                              "slots_completed": total_slots}) + "\n"

            # Save CSV reports under the user's reports directory.
            user_dir = _user_reports_dir(user_id_for_run)
            timestamp = datetime.now().strftime("%d_%B_%Y").lower()
            portfolio_label = sanitize_filename(config.name)
            prefix = f"portfolio_{portfolio_label}_{timestamp}"

            for report_key, report_name in [
                ("positions_report", "position_report"),
                ("fills_report", "order_fill_report"),
            ]:
                report = results.get(report_key)
                if report is not None and not report.empty:
                    report.to_csv(user_dir / f"{report_name}_{prefix}.csv", index=False)

            # Build per-strategy reports for order book and logs
            per_strat = results.get("per_strategy", {})
            pos_report = results.get("positions_report")
            fills_rep = results.get("fills_report")
            slot_to_tid = results.get("slot_to_trader_id", {})
            slot_to_sid = results.get("slot_to_strategy_id", {})

            all_results_for_reports = {}
            for slot_id, sr in per_strat.items():
                strat_label = sanitize_filename(sr["display_name"])
                # Prefer trader_id for filtering — it is unique per slot even
                # when multiple slots share the same strategy_id.
                actual_tid = slot_to_tid.get(slot_id, "")
                actual_sid = slot_to_sid.get(slot_id, "")
                slot_pos = pd.DataFrame()
                slot_fills = pd.DataFrame()
                if actual_tid:
                    if pos_report is not None and not pos_report.empty and "trader_id" in pos_report.columns:
                        slot_pos = pos_report[pos_report["trader_id"] == actual_tid]
                    if fills_rep is not None and not fills_rep.empty and "trader_id" in fills_rep.columns:
                        slot_fills = fills_rep[fills_rep["trader_id"] == actual_tid]
                elif actual_sid:
                    if pos_report is not None and not pos_report.empty and "strategy_id" in pos_report.columns:
                        slot_pos = pos_report[pos_report["strategy_id"] == actual_sid]
                    if fills_rep is not None and not fills_rep.empty and "strategy_id" in fills_rep.columns:
                        slot_fills = fills_rep[fills_rep["strategy_id"] == actual_sid]
                all_results_for_reports[strat_label] = {
                    "positions_report": slot_pos, "fills_report": slot_fills,
                    "starting_capital": results["starting_capital"],
                    "final_balance": results["final_balance"],
                    "total_pnl": sr["pnl"], "total_return_pct": results["total_return_pct"],
                    "total_orders": 0, "total_trades": sr["trades"],
                    "wins": sr["wins"], "losses": sr["losses"], "win_rate": sr["win_rate"],
                }

            ob_df = build_orderbook_dataframe(all_results_for_reports, user_id=user_id_for_run)
            if not ob_df.empty:
                ob_df.to_csv(user_dir / f"order_book_{prefix}.csv", index=False)
                results["order_book"] = ob_df.fillna("").to_dict(orient="records")
            else:
                results["order_book"] = []

            logs_df = build_logs_dataframe(all_results_for_reports)
            if not logs_df.empty:
                logs_df.to_csv(user_dir / f"backtest_{prefix}_logs.csv", index=False)

            report_html = ""
            try:
                report_html = generate_report(all_results_for_reports,
                                              backtest_name=f"Portfolio: {config.name}",
                                              user_id=user_id_for_run,
                                              date_range={
                                                  "start": config.start_date or "",
                                                  "end": config.end_date or "",
                                              })
                report_path = user_dir / f"{prefix}_report.html"
                report_path.write_text(report_html, encoding="utf-8")
            except Exception as e:
                print(f"[Portfolio] HTML report generation failed: {e}")

            results["report_file"] = f"{prefix}_report.html"
            results["report_name"] = prefix

            # Clean up non-serializable / internal-only data
            for key in ["fills_report", "positions_report", "account_report",
                         "slot_to_strategy_id", "slot_to_trader_id", "errors",
                         "daily_pnl"]:
                results.pop(key, None)
            # Also remove daily_pnl from per-strategy entries
            for sr in results.get("per_strategy", {}).values():
                sr.pop("daily_pnl", None)
            results["equity_curve"] = [float(v) for v in results["equity_curve"]]
            for pt in results.get("equity_curve_ts", []):
                pt["balance"] = float(pt["balance"])

            # Send final complete event with full results
            yield json.dumps({"event": "complete", "completed": total_steps,
                              "total": total_steps, "elapsed": elapsed,
                              "results": results}, default=str) + "\n"

        except Exception as e:
            yield json.dumps({"event": "error", "error": str(e),
                              "traceback": traceback.format_exc()}) + "\n"

    return Response(
        stream_with_context(generate()),
        mimetype="application/x-ndjson",
        headers={"X-Content-Type-Options": "nosniff",
                 "Cache-Control": "no-cache, no-store, must-revalidate"},
    )


if __name__ == "__main__":
    print("=" * 60)
    print("  M_Cube Crypto Dashboard (HTML/CSS/JS)")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    # threaded=True so concurrent tab switches don't queue behind each other on
    # the dev server's single Werkzeug worker. The catalog read paths are
    # I/O-bound and safe to run from threads.
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False, threaded=True)
