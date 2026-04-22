"""
Flask backend for the M_Cube Crypto Dashboard.

Provides REST API endpoints that reuse the existing core/ modules
for data loading, visualization, backtesting, and report generation.

Run with: python server.py
"""

import json
import sys
import traceback
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


# ─── Static files ───────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


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


@app.route("/api/csv/load", methods=["POST"])
def csv_load():
    """Load selected CSV entries into the catalog."""
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

    results = []
    errors = []

    for entry in entries:
        try:
            result = load_csv_and_store(csv_entry=entry, catalog_path=catalog_path,
                                        venue=venue, data_format=data_format)
            df = result["dataframe"]
            results.append({
                "symbol": result["symbol"],
                "name": result["name"],
                "num_bars": result["num_bars"],
                "date_start": str(df.index[0].date()),
                "date_end": str(df.index[-1].date()),
                "latest_close": float(df["close"].iloc[-1]),
                "sample_data": df.tail(10).reset_index().to_dict(orient="records"),
            })
        except Exception as e:
            errors.append({"symbol": entry.get("symbol", "?"), "error": str(e)})

    return jsonify({"results": results, "errors": errors})


# ─── Data View API ───────────────────────────────────────────────────────────

@app.route("/api/data/bar_types")
def get_bar_types():
    """Get available bar types from catalog, with date ranges."""
    catalog_path = request.args.get("path", CATALOG_PATH)
    try:
        catalog = load_catalog(catalog_path)
        all_bars = catalog.bars()
        if not all_bars:
            return jsonify({"bar_types": [], "bar_type_details": {}})

        # Group bars by bar_type to find min/max dates
        by_type = {}
        for bar in all_bars:
            bt = str(bar.bar_type)
            if bt not in by_type:
                by_type[bt] = {"min_ts": bar.ts_event, "max_ts": bar.ts_event}
            else:
                if bar.ts_event < by_type[bt]["min_ts"]:
                    by_type[bt]["min_ts"] = bar.ts_event
                if bar.ts_event > by_type[bt]["max_ts"]:
                    by_type[bt]["max_ts"] = bar.ts_event

        bar_types = sorted(by_type.keys())
        bar_type_details = {}
        for bt in bar_types:
            info = by_type[bt]
            bar_type_details[bt] = {
                "start_date": str(pd.Timestamp(info["min_ts"], unit="ns", tz="UTC").date()),
                "end_date": str(pd.Timestamp(info["max_ts"], unit="ns", tz="UTC").date()),
            }

        return jsonify({"bar_types": bar_types, "bar_type_details": bar_type_details})
    except Exception as e:
        return jsonify({"bar_types": [], "bar_type_details": {}, "error": str(e)})


@app.route("/api/data/bars")
def get_bars():
    """Get bar data for a specific bar type."""
    catalog_path = request.args.get("path", CATALOG_PATH)
    bar_type_str = request.args.get("bar_type", "")

    if not bar_type_str:
        return jsonify({"error": "bar_type parameter required"}), 400

    try:
        catalog = load_catalog(catalog_path)
        bars = catalog.bars(bar_types=[bar_type_str])

        if not bars:
            return jsonify({"data": [], "count": 0})

        data = []
        for bar in bars:
            data.append({
                "timestamp": pd.Timestamp(bar.ts_event, unit="ns", tz="UTC").isoformat(),
                "open": float(bar.open),
                "high": float(bar.high),
                "low": float(bar.low),
                "close": float(bar.close),
                "volume": float(bar.volume),
            })

        data.sort(key=lambda x: x["timestamp"])
        return jsonify({"data": data, "count": len(data)})
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

        # Check for duplicate names among other custom files
        for other_file in CUSTOM_STRATEGIES_DIR.glob("*.py"):
            if other_file.name == safe_name or other_file.name.startswith("__"):
                continue
            # Quick check: read file and look for STRATEGY_NAME
            try:
                content = other_file.read_text(encoding="utf-8")
                for line in content.splitlines():
                    line = line.strip()
                    if line.startswith("STRATEGY_NAME") and "=" in line:
                        # Extract string value
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val == strat_name:
                            dest_path.unlink()
                            return jsonify({"error": f"A custom strategy named '{strat_name}' already exists."}), 400
                        break
            except Exception:
                continue

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

def _serialize_backtest_result(results: dict, strategy_name: str) -> dict:
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
        "equity_curve_ts": results.get("equity_curve_ts", []),
    }

    # Convert reports to lists of dicts
    for report_key in ["fills_report", "positions_report", "account_report"]:
        report = results.get(report_key)
        if report is not None and not report.empty:
            report_copy = report.copy()
            for col in report_copy.columns:
                report_copy[col] = report_copy[col].apply(
                    lambda x: str(x) if not isinstance(x, (int, float, str, bool, type(None))) else x
                )
            report_copy = report_copy.fillna("").reset_index()
            serializable[report_key] = report_copy.to_dict(orient="records")
        else:
            serializable[report_key] = []

    # Build order book and logs
    safe_name = sanitize_filename(strategy_name)
    ob_df = build_orderbook_dataframe({safe_name: results})
    serializable["order_book"] = ob_df.fillna("").to_dict(orient="records") if not ob_df.empty else []

    logs_df = build_logs_dataframe({safe_name: results})
    serializable["logs"] = logs_df.fillna("").to_dict(orient="records") if not logs_df.empty else []

    return serializable


@app.route("/api/backtest/run-stream", methods=["POST"])
def run_backtest_stream():
    """Run backtests with real-time progress streaming (newline-delimited JSON)."""
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

        # Cap worker count — each backtest is CPU-bound; too many hurts memory
        max_workers = min(total_runs, (os.cpu_count() or 2), 8)

        t_start = _time.time()
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for inst_idx, meta in enumerate(inst_meta):
                for strategy_name, cfg in strategies_config.items():
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
                        results, strategy_name)
                    completed_runs += 1
                    print(f"[Backtest] Completed {strategy_name} on {primary_bt} "
                          f"({completed_runs}/{total_runs})")
                    yield json.dumps({"event": "progress", "status": "complete",
                                      "completed": completed_runs, "total": total_runs,
                                      "current_instrument": primary_bt,
                                      "current_strategy": strategy_name}) + "\n"
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
                        raw_name = f"{inst_label}_{'_'.join(inst_strategies.keys())}"
                        report_name = sanitize_filename(raw_name)
                        try:
                            report_html = generate_report(inst_strategies_raw, backtest_name=report_name)
                            report_path = REPORTS_DIR / f"{report_name}_report.html"
                            report_path.write_text(report_html, encoding="utf-8")
                        except Exception as e:
                            errors.append({"instrument": primary_bt, "strategy": "HTML Report",
                                           "error": str(e), "traceback": traceback.format_exc()})

                        for strat_name, strat_results in inst_strategies.items():
                            safe_strat = sanitize_filename(strat_name)
                            prefix = f"{inst_label}_{safe_strat}_{timestamp}"
                            if strat_results.get("positions_report"):
                                pd.DataFrame(strat_results["positions_report"]).to_csv(
                                    REPORTS_DIR / f"position_report_{prefix}.csv", index=False)
                            if strat_results.get("fills_report"):
                                pd.DataFrame(strat_results["fills_report"]).to_csv(
                                    REPORTS_DIR / f"order_fill_report_{prefix}.csv", index=False)
                            if strat_results.get("account_report"):
                                pd.DataFrame(strat_results["account_report"]).to_csv(
                                    REPORTS_DIR / f"account_report_{prefix}.csv", index=False)
                            if strat_results.get("order_book"):
                                pd.DataFrame(strat_results["order_book"]).to_csv(
                                    REPORTS_DIR / f"order_book_{prefix}.csv", index=False)
                            if strat_results.get("logs"):
                                pd.DataFrame(strat_results["logs"]).to_csv(
                                    REPORTS_DIR / f"backtest_{prefix}_logs.csv", index=False)

                    instrument_results[primary_bt] = {
                        "date_range": {"start": meta["start_date"] or "", "end": meta["end_date"] or ""},
                        "strategies": inst_strategies,
                        "report_html": report_html,
                        "report_name": inst_label,
                    }

        elapsed_total = _time.time() - t_start
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
                )

                inst_strategies_raw[strategy_name] = results
                inst_strategies[strategy_name] = _serialize_backtest_result(results, strategy_name)

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
            # HTML report for this instrument
            raw_name = f"{inst_label}_{'_'.join(inst_strategies.keys())}"
            report_name = sanitize_filename(raw_name)
            try:
                report_html = generate_report(inst_strategies_raw, backtest_name=report_name)
                report_path = REPORTS_DIR / f"{report_name}_report.html"
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
                        REPORTS_DIR / f"position_report_{prefix}.csv", index=False)
                if strat_results.get("fills_report"):
                    pd.DataFrame(strat_results["fills_report"]).to_csv(
                        REPORTS_DIR / f"order_fill_report_{prefix}.csv", index=False)
                if strat_results.get("account_report"):
                    pd.DataFrame(strat_results["account_report"]).to_csv(
                        REPORTS_DIR / f"account_report_{prefix}.csv", index=False)
                if strat_results.get("order_book"):
                    pd.DataFrame(strat_results["order_book"]).to_csv(
                        REPORTS_DIR / f"order_book_{prefix}.csv", index=False)
                if strat_results.get("logs"):
                    pd.DataFrame(strat_results["logs"]).to_csv(
                        REPORTS_DIR / f"backtest_{prefix}_logs.csv", index=False)

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

PORTFOLIOS_DIR = str(PROJECT_DIR / "portfolios")


@app.route("/api/portfolios/list")
def api_list_portfolios():
    """List all saved portfolios."""
    names = list_portfolios(PORTFOLIOS_DIR)
    return jsonify({"portfolios": names})


@app.route("/api/portfolios/load")
def api_load_portfolio():
    """Load a portfolio by name."""
    name = request.args.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400
    try:
        config = load_portfolio(name, PORTFOLIOS_DIR)
        return jsonify({"portfolio": portfolio_to_dict(config)})
    except FileNotFoundError:
        return jsonify({"error": f"Portfolio '{name}' not found"}), 404


@app.route("/api/portfolios/save", methods=["POST"])
def api_save_portfolio():
    """Save a portfolio configuration."""
    data = request.json
    try:
        config = portfolio_from_dict(data)
        path = save_portfolio(config, PORTFOLIOS_DIR)
        return jsonify({"success": True, "message": f"Portfolio '{config.name}' saved.", "path": str(path)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/portfolios/delete", methods=["POST"])
def api_delete_portfolio():
    """Delete a portfolio by name."""
    name = request.json.get("name", "")
    if not name:
        return jsonify({"error": "name required"}), 400
    if delete_portfolio(name, PORTFOLIOS_DIR):
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
    data = request.json
    catalog_path = data.get("catalog_path", CATALOG_PATH)

    def generate():
        import time as _time
        import queue
        import threading

        try:
            config = portfolio_from_dict(data.get("portfolio", data))
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

            # Save CSV reports
            REPORTS_DIR.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%d_%B_%Y").lower()
            portfolio_label = sanitize_filename(config.name)
            prefix = f"portfolio_{portfolio_label}_{timestamp}"

            for report_key, report_name in [
                ("positions_report", "position_report"),
                ("fills_report", "order_fill_report"),
            ]:
                report = results.get(report_key)
                if report is not None and not report.empty:
                    report.to_csv(REPORTS_DIR / f"{report_name}_{prefix}.csv", index=False)

            # Build per-strategy reports for order book and logs
            per_strat = results.get("per_strategy", {})
            pos_report = results.get("positions_report")
            fills_rep = results.get("fills_report")
            slot_to_sid = results.get("slot_to_strategy_id", {})

            all_results_for_reports = {}
            for slot_id, sr in per_strat.items():
                strat_label = sanitize_filename(sr["display_name"])
                actual_sid = slot_to_sid.get(slot_id, "")
                slot_pos = pd.DataFrame()
                slot_fills = pd.DataFrame()
                if actual_sid:
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

            ob_df = build_orderbook_dataframe(all_results_for_reports)
            if not ob_df.empty:
                ob_df.to_csv(REPORTS_DIR / f"order_book_{prefix}.csv", index=False)

            logs_df = build_logs_dataframe(all_results_for_reports)
            if not logs_df.empty:
                logs_df.to_csv(REPORTS_DIR / f"backtest_{prefix}_logs.csv", index=False)

            try:
                report_html = generate_report(all_results_for_reports,
                                              backtest_name=f"Portfolio: {config.name}")
                report_path = REPORTS_DIR / f"{prefix}_report.html"
                report_path.write_text(report_html, encoding="utf-8")
            except Exception as e:
                print(f"[Portfolio] HTML report generation failed: {e}")

            # Clean up non-serializable data
            for key in ["fills_report", "positions_report", "account_report",
                         "slot_to_strategy_id", "errors"]:
                results.pop(key, None)
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
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
