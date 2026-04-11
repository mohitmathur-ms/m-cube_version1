"""
Flask backend for the M_Cube Crypto Dashboard (HTML/CSS/JS version).

Provides REST API endpoints that reuse the existing core/ modules
for data loading, visualization, backtesting, and report generation.

Run with: python server.py
"""

import sys
import traceback
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS
import pandas as pd

# Add parent directory so core package imports work
PARENT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PARENT_DIR))

from core.csv_loader import DEFAULT_CSV_FOLDER, scan_csv_folder, get_display_label
from core.nautilus_loader import load_csv_and_store, load_catalog
from core.backtest_runner import run_backtest
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
CATALOG_PATH = str(PARENT_DIR / "catalog")
CUSTOM_STRATEGIES_DIR = PARENT_DIR / "custom_strategies"
REPORTS_DIR = PARENT_DIR / "reports"


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

    results = []
    errors = []

    for entry in entries:
        try:
            result = load_csv_and_store(csv_entry=entry, catalog_path=catalog_path)
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
    """Get available bar types from catalog."""
    catalog_path = request.args.get("path", CATALOG_PATH)
    try:
        catalog = load_catalog(catalog_path)
        all_bars = catalog.bars()
        if not all_bars:
            return jsonify({"bar_types": []})
        bar_types = sorted({str(bar.bar_type) for bar in all_bars})
        return jsonify({"bar_types": bar_types})
    except Exception as e:
        return jsonify({"bar_types": [], "error": str(e)})


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
    parent_dir_str = str(PARENT_DIR).replace("\\", "\\\\")
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

@app.route("/api/backtest/run", methods=["POST"])
def run_backtest_api():
    """Run backtests for selected strategies."""
    data = request.json
    bar_type_str = data.get("bar_type", "")
    starting_capital = data.get("starting_capital", 100000.0)
    strategies_config = data.get("strategies", {})
    catalog_path = data.get("catalog_path", CATALOG_PATH)

    if not bar_type_str or not strategies_config:
        return jsonify({"error": "bar_type and strategies required"}), 400

    CUSTOM_STRATEGIES_DIR.mkdir(exist_ok=True)
    merged_registry, _ = get_merged_registry(CUSTOM_STRATEGIES_DIR)

    all_results = {}
    all_results_raw = {}  # Keep raw results with DataFrames for report generation
    errors = []

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
            )

            all_results_raw[strategy_name] = results

            # Convert DataFrames to serializable dicts
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
            }

            # Convert reports to lists of dicts
            for report_key in ["fills_report", "positions_report", "account_report"]:
                report = results.get(report_key)
                if report is not None and not report.empty:
                    report_copy = report.copy()
                    for col in report_copy.columns:
                        report_copy[col] = report_copy[col].apply(lambda x: str(x) if not isinstance(x, (int, float, str, bool, type(None))) else x)
                    serializable[report_key] = report_copy.reset_index().to_dict(orient="records")
                else:
                    serializable[report_key] = []

            # Build order book from raw results
            safe_name = sanitize_filename(strategy_name)
            ob_df = build_orderbook_dataframe({safe_name: results})
            serializable["order_book"] = ob_df.to_dict(orient="records") if not ob_df.empty else []

            # Build logs from raw results
            logs_df = build_logs_dataframe({safe_name: results})
            serializable["logs"] = logs_df.to_dict(orient="records") if not logs_df.empty else []

            all_results[strategy_name] = serializable

        except Exception as e:
            errors.append({
                "strategy": strategy_name,
                "error": str(e),
                "traceback": traceback.format_exc(),
            })

    # Save reports & generate HTML
    backtest_name = ""
    report_html = ""
    if all_results:
        REPORTS_DIR.mkdir(exist_ok=True)

        raw_name = f"{bar_type_str.split('.')[0]}_{'_'.join(all_results.keys())}"
        backtest_name = sanitize_filename(raw_name)

        # Generate HTML report from raw results
        try:
            report_html = generate_report(all_results_raw, backtest_name=backtest_name)
            report_path = REPORTS_DIR / f"{backtest_name}_report.html"
            report_path.write_text(report_html, encoding="utf-8")
        except Exception as e:
            report_html = ""
            errors.append({"strategy": "HTML Report", "error": str(e), "traceback": traceback.format_exc()})

        # Save CSV reports
        timestamp = datetime.now().strftime("%d_%B_%Y").lower()
        for strat_name, strat_results in all_results.items():
            safe_name = sanitize_filename(strat_name)

            if strat_results.get("positions_report"):
                pd.DataFrame(strat_results["positions_report"]).to_csv(
                    REPORTS_DIR / f"position_report_{safe_name}_{timestamp}.csv", index=False
                )
            if strat_results.get("fills_report"):
                pd.DataFrame(strat_results["fills_report"]).to_csv(
                    REPORTS_DIR / f"order_fill_report_{safe_name}_{timestamp}.csv", index=False
                )
            if strat_results.get("account_report"):
                pd.DataFrame(strat_results["account_report"]).to_csv(
                    REPORTS_DIR / f"account_report_{safe_name}_{timestamp}.csv", index=False
                )
            if strat_results.get("order_book"):
                pd.DataFrame(strat_results["order_book"]).to_csv(
                    REPORTS_DIR / f"order_book_{safe_name}_{timestamp}.csv", index=False
                )
            if strat_results.get("logs"):
                pd.DataFrame(strat_results["logs"]).to_csv(
                    REPORTS_DIR / f"backtest_{safe_name}_{timestamp}_logs.csv", index=False
                )

    return jsonify({
        "results": all_results,
        "errors": errors,
        "backtest_name": backtest_name,
        "report_html": report_html,
    })


if __name__ == "__main__":
    print("=" * 60)
    print("  M_Cube Crypto Dashboard (HTML/CSS/JS)")
    print("  Open http://localhost:5000 in your browser")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
