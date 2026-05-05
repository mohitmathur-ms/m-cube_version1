"""
Flask backend for the Adapter Admin Panel.

Provides REST API endpoints for managing NautilusTrader adapter configurations
and custom adapter file uploads.

Run with: python admin_server.py
"""

import json
import subprocess
import sys
import traceback
from datetime import datetime
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS

from adapter_registry import (
    get_registry_for_frontend,
    mask_config,
    is_masked,
)
from adapter_discovery import invalidate_cache
from custom_adapter_loader import (
    sanitize_filename,
    get_validation_script,
    get_adapter_template,
    get_adapter_guidelines,
)

PROJECT_DIR = Path(__file__).resolve().parent
# core.users lives at the repo root, not under adapter_admin/. Add the repo
# root to sys.path so we can reuse the same registry helpers as server.py.
_REPO_ROOT = PROJECT_DIR.parent
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from core.users import (
    load_users as _load_users,
    save_users as _save_users,
    validate_registry_payload as _validate_registry_payload,
)

app = Flask(__name__, static_folder="static", static_url_path="")
app.config["MAX_CONTENT_LENGTH"] = 16 * 1024 * 1024  # 16 MB
CORS(app)

ADAPTERS_CONFIG_DIR = PROJECT_DIR / "adapters_config"
CUSTOM_ADAPTERS_DIR = PROJECT_DIR / "custom_adapters"


@app.errorhandler(Exception)
def handle_exception(e):
    return jsonify({"error": str(e), "traceback": traceback.format_exc()}), 500


# ─── Static files ──────────────────────────────────────────────────────────

@app.route("/")
def index():
    return send_from_directory("static", "index.html")


# ─── Registry API ──────────────────────────────────────────────────────────

@app.route("/api/registry")
def get_registry():
    """Return discovered adapter registry metadata for form generation."""
    return jsonify({"registry": get_registry_for_frontend()})


@app.route("/api/registry/refresh", methods=["POST"])
def refresh_registry():
    """Force re-discovery of adapters (e.g., after installing a new package)."""
    invalidate_cache()
    return jsonify({"success": True, "registry": get_registry_for_frontend()})


@app.route("/api/asset-classes")
def get_asset_classes():
    """Return asset classes from NautilusTrader."""
    try:
        from nautilus_trader.model.enums import AssetClass
        classes = [member.name.lower() for member in AssetClass]
    except ImportError:
        classes = []
    return jsonify({"asset_classes": classes})


# ─── Adapter Config CRUD ──────────────────────────────────────────────────

DATA_FORMATS_DIR = PROJECT_DIR / "data_formats"


def _read_data_formats() -> dict:
    """Read all data format configs from individual JSON files."""
    DATA_FORMATS_DIR.mkdir(exist_ok=True)
    formats = {}
    for f in sorted(DATA_FORMATS_DIR.glob("*.json")):
        try:
            config = json.loads(f.read_text(encoding="utf-8"))
            formats[f.stem] = config
        except Exception:
            continue
    return formats


def _write_data_format(asset_class: str, config: dict):
    """Write a single asset class config to its JSON file."""
    DATA_FORMATS_DIR.mkdir(exist_ok=True)
    file_path = DATA_FORMATS_DIR / f"{asset_class}.json"
    file_path.write_text(json.dumps(config, indent=2), encoding="utf-8")


# ─── Data Formats API ─────────────────────────────────────────────────────

@app.route("/api/data-formats")
def get_data_formats():
    """Return all data format configs."""
    return jsonify({"formats": _read_data_formats()})


@app.route("/api/data-formats/<asset_class>")
def get_data_format(asset_class):
    """Return data format config for a specific asset class."""
    formats = _read_data_formats()
    if asset_class not in formats:
        return jsonify({"error": f"No config for '{asset_class}'"}), 404
    return jsonify({"asset_class": asset_class, "config": formats[asset_class]})


@app.route("/api/data-formats/<asset_class>", methods=["PUT"])
def update_data_format(asset_class):
    """Update data format config for an asset class."""
    file_path = DATA_FORMATS_DIR / f"{asset_class}.json"
    if not file_path.exists():
        return jsonify({"error": f"Unknown asset class '{asset_class}'"}), 404

    existing = json.loads(file_path.read_text(encoding="utf-8"))
    data = request.json

    # Merge incoming data into existing config
    for section in ("csv", "instrument", "trading"):
        if section in data:
            if section not in existing:
                existing[section] = {}
            existing[section].update(data[section])

    if "label" in data:
        existing["label"] = data["label"]

    _write_data_format(asset_class, existing)
    return jsonify({"success": True, "message": f"Data format for '{asset_class}' updated", "config": existing})


@app.route("/api/data-formats/<asset_class>/reset", methods=["POST"])
def reset_data_format(asset_class):
    """Reset data format config for an asset class to all nulls."""
    file_path = DATA_FORMATS_DIR / f"{asset_class}.json"
    if not file_path.exists():
        return jsonify({"error": f"Unknown asset class '{asset_class}'"}), 404

    existing = json.loads(file_path.read_text(encoding="utf-8"))
    label = existing.get("label", asset_class.capitalize())

    reset_config = {
        "label": label,
        "csv": {
            "filename_pattern": None, "filename_description": None,
            "required_columns": None, "timestamp_column": None,
            "timestamp_format": None, "delimiter": ","
        },
        "instrument": {
            "type": None, "quote_currency": None, "price_precision": None,
            "size_precision": None, "currency_type": None, "timeframe": None
        },
        "trading": {
            "maker_fee": None, "taker_fee": None,
            "margin_init": None, "margin_maint": None
        }
    }
    _write_data_format(asset_class, reset_config)
    return jsonify({"success": True, "message": f"Data format for '{asset_class}' reset"})


# ─── Adapter Config CRUD ──────────────────────────────────────────────────

@app.route("/api/adapters")
def list_adapters():
    """List all saved adapter configurations."""
    ADAPTERS_CONFIG_DIR.mkdir(exist_ok=True)
    adapters = []
    for f in sorted(ADAPTERS_CONFIG_DIR.glob("*.json")):
        try:
            config = json.loads(f.read_text(encoding="utf-8"))
            exchange_type = config.get("exchange_type", "")
            masked = mask_config(config, exchange_type)
            adapters.append(masked)
        except Exception:
            continue
    return jsonify({"adapters": adapters, "count": len(adapters)})


@app.route("/api/adapters", methods=["POST"])
def create_adapter():
    """Create a new adapter configuration."""
    ADAPTERS_CONFIG_DIR.mkdir(exist_ok=True)
    data = request.json

    name = (data.get("name") or "").strip()
    venue = (data.get("venue") or "").strip().upper()
    exchange_type = (data.get("exchange_type") or "").strip()

    if not name:
        return jsonify({"error": "Adapter name is required"}), 400
    if not venue:
        return jsonify({"error": "Venue name is required"}), 400
    if not exchange_type:
        return jsonify({"error": "Exchange type is required"}), 400

    adapter_id = sanitize_filename(name.lower().replace(" ", "_"))
    file_path = ADAPTERS_CONFIG_DIR / f"{adapter_id}.json"

    if file_path.exists():
        return jsonify({"error": f"An adapter with name '{name}' already exists"}), 400

    # Check for duplicate venue
    for f in ADAPTERS_CONFIG_DIR.glob("*.json"):
        try:
            existing = json.loads(f.read_text(encoding="utf-8"))
            if existing.get("venue") == venue:
                return jsonify({"error": f"Venue '{venue}' is already configured in '{existing.get('name', f.stem)}'"}), 400
        except Exception:
            continue

    now = datetime.now().isoformat()
    config = {
        "id": adapter_id,
        "name": name,
        "venue": venue,
        "exchange_type": exchange_type,
        "asset_class": (data.get("asset_class") or "crypto").strip().lower(),
        "is_custom": data.get("is_custom", False),
        "custom_adapter_file": data.get("custom_adapter_file"),
        "testnet": data.get("testnet", True),
        "data_config": data.get("data_config", {}),
        "exec_config": data.get("exec_config", {}),
        "account_base_currency": (data.get("account_base_currency") or "USD").upper(),
        "fx_conversion": data.get("fx_conversion") or {},
        "created_at": now,
        "updated_at": now,
    }

    file_path.write_text(json.dumps(config, indent=2), encoding="utf-8")
    return jsonify({"success": True, "message": f"Adapter '{name}' created", "adapter": mask_config(config, exchange_type)})


@app.route("/api/adapters/<adapter_id>")
def get_adapter(adapter_id):
    """Get a specific adapter configuration."""
    safe_id = sanitize_filename(adapter_id)
    file_path = ADAPTERS_CONFIG_DIR / f"{safe_id}.json"

    if not file_path.exists():
        return jsonify({"error": "Adapter not found"}), 404

    config = json.loads(file_path.read_text(encoding="utf-8"))

    # Reveal full values for editing if requested
    if request.args.get("reveal") == "true":
        return jsonify({"adapter": config})

    return jsonify({"adapter": mask_config(config, config.get("exchange_type", ""))})


@app.route("/api/adapters/<adapter_id>", methods=["PUT"])
def update_adapter(adapter_id):
    """Update an adapter configuration."""
    safe_id = sanitize_filename(adapter_id)
    file_path = ADAPTERS_CONFIG_DIR / f"{safe_id}.json"

    if not file_path.exists():
        return jsonify({"error": "Adapter not found"}), 404

    existing = json.loads(file_path.read_text(encoding="utf-8"))
    data = request.json
    exchange_type = existing.get("exchange_type", "")

    # Update fields. `account_base_currency` and `fx_conversion` drive the
    # FX PnL conversion at backtest time (see core/fx_rates.py) — they must
    # round-trip through this endpoint or the admin panel edits silently
    # won't take effect.
    for key in (
        "name", "venue", "testnet", "is_custom", "custom_adapter_file",
        "asset_class", "account_base_currency", "fx_conversion",
    ):
        if key in data:
            existing[key] = data[key]

    # Update config sections, preserving masked values
    for section_key in ("data_config", "exec_config"):
        if section_key in data and data[section_key]:
            new_section = data[section_key]
            old_section = existing.get(section_key, {})
            merged = old_section.copy()
            for k, v in new_section.items():
                if is_masked(v):
                    # Keep the existing unmasked value
                    continue
                merged[k] = v
            existing[section_key] = merged

    if "venue" in data:
        existing["venue"] = (data["venue"] or "").strip().upper()

    existing["updated_at"] = datetime.now().isoformat()

    file_path.write_text(json.dumps(existing, indent=2), encoding="utf-8")
    return jsonify({"success": True, "message": f"Adapter '{existing['name']}' updated",
                     "adapter": mask_config(existing, exchange_type)})


@app.route("/api/adapters/<adapter_id>", methods=["DELETE"])
def delete_adapter(adapter_id):
    """Delete an adapter configuration."""
    safe_id = sanitize_filename(adapter_id)
    file_path = ADAPTERS_CONFIG_DIR / f"{safe_id}.json"

    if not file_path.exists():
        return jsonify({"error": "Adapter not found"}), 404

    config = json.loads(file_path.read_text(encoding="utf-8"))
    file_path.unlink()
    return jsonify({"success": True, "message": f"Deleted adapter '{config.get('name', adapter_id)}'"})


@app.route("/api/adapters/<adapter_id>/test", methods=["POST"])
def test_adapter(adapter_id):
    """Test connection to an exchange (basic connectivity check)."""
    safe_id = sanitize_filename(adapter_id)
    file_path = ADAPTERS_CONFIG_DIR / f"{safe_id}.json"

    if not file_path.exists():
        return jsonify({"error": "Adapter not found"}), 404

    config = json.loads(file_path.read_text(encoding="utf-8"))
    exchange_type = config.get("exchange_type", "")

    from adapter_discovery import get_full_registry
    registry = get_full_registry()
    if exchange_type not in registry:
        return jsonify({"success": False, "message": f"No built-in test available for '{exchange_type}'. Custom adapter connection testing is not yet supported."})

    # Build a simple connectivity test script
    test_script = _build_test_script(exchange_type, config)

    try:
        result = subprocess.run(
            [sys.executable, "-c", test_script],
            capture_output=True, text=True, timeout=15,
        )
        output = result.stdout.strip()
        if not output:
            stderr = result.stderr.strip()
            return jsonify({"success": False, "message": f"Test failed:\n{stderr[:500]}"})

        test_result = json.loads(output)
        return jsonify(test_result)

    except subprocess.TimeoutExpired:
        return jsonify({"success": False, "message": "Connection test timed out (15s)"})
    except Exception as e:
        return jsonify({"success": False, "message": f"Test error: {e}"})


def _build_test_script(exchange_type: str, config: dict) -> str:
    """Build a subprocess script to test exchange connectivity and credentials.

    Two-step test:
      1. Ping the public endpoint to check reachability.
      2. If API credentials are provided, call an authenticated endpoint
         to verify they actually work.
    """
    data_config = config.get("data_config", {})
    exec_config = config.get("exec_config", {})
    testnet = config.get("testnet", True)

    # Merge credentials from both configs
    api_key = data_config.get("api_key") or exec_config.get("api_key") or ""
    api_secret = data_config.get("api_secret") or exec_config.get("api_secret") or ""

    if exchange_type == "Binance":
        ping_url = "https://testnet.binance.vision/api/v3/ping" if testnet else "https://api.binance.com/api/v3/ping"
        auth_base = "https://testnet.binance.vision" if testnet else "https://api.binance.com"
        return _binance_test_script(ping_url, auth_base, api_key, api_secret)

    elif exchange_type == "Bybit":
        ping_url = "https://api-testnet.bybit.com/v5/market/time" if testnet else "https://api.bybit.com/v5/market/time"
        auth_base = "https://api-testnet.bybit.com" if testnet else "https://api.bybit.com"
        return _bybit_test_script(ping_url, auth_base, api_key, api_secret)

    else:
        return (
            'import json\n'
            f'print(json.dumps({{"success": False, "message": "Automated connection test not available for {exchange_type}. '
            f'Please verify your credentials manually."}}))\n'
        )


def _binance_test_script(ping_url: str, auth_base: str, api_key: str, api_secret: str) -> str:
    """Build Binance test script with reachability + credential validation."""
    return (
        'import json, urllib.request, hmac, hashlib, time\n'
        '\n'
        'result = {"success": False, "message": ""}\n'
        '\n'
        '# Step 1: Reachability\n'
        'try:\n'
        f'    req = urllib.request.urlopen("{ping_url}", timeout=10)\n'
        '    if req.status != 200:\n'
        '        result["message"] = f"Exchange unreachable (HTTP {req.status})"\n'
        '        print(json.dumps(result))\n'
        '        raise SystemExit\n'
        'except SystemExit:\n'
        '    raise\n'
        'except Exception as e:\n'
        '    result["message"] = f"Exchange unreachable: {e}"\n'
        '    print(json.dumps(result))\n'
        '    raise SystemExit\n'
        '\n'
        '# Step 2: Credential validation\n'
        f'api_key = "{api_key}"\n'
        f'api_secret = "{api_secret}"\n'
        '\n'
        'if not api_key or not api_secret:\n'
        '    result["success"] = True\n'
        '    result["message"] = "Exchange is reachable, but no API credentials were provided to verify authentication."\n'
        '    print(json.dumps(result))\n'
        '    raise SystemExit\n'
        '\n'
        'try:\n'
        '    ts = str(int(time.time() * 1000))\n'
        '    query = f"timestamp={ts}"\n'
        '    sig = hmac.new(api_secret.encode(), query.encode(), hashlib.sha256).hexdigest()\n'
        f'    url = f"{auth_base}/api/v3/account?{{query}}&signature={{sig}}"\n'
        '    req = urllib.request.Request(url)\n'
        '    req.add_header("X-MBX-APIKEY", api_key)\n'
        '    resp = urllib.request.urlopen(req, timeout=10)\n'
        '    if resp.status == 200:\n'
        '        result["success"] = True\n'
        '        result["message"] = "Connection successful! API credentials are valid."\n'
        '    else:\n'
        '        result["message"] = f"Authentication failed (HTTP {resp.status})"\n'
        'except urllib.error.HTTPError as e:\n'
        '    body = e.read().decode()\n'
        '    try:\n'
        '        err = json.loads(body)\n'
        '        msg = err.get("msg", body[:200])\n'
        '    except Exception:\n'
        '        msg = body[:200]\n'
        '    result["message"] = f"Exchange is reachable, but authentication failed: {msg}"\n'
        'except Exception as e:\n'
        '    result["message"] = f"Exchange is reachable, but credential test failed: {e}"\n'
        '\n'
        'print(json.dumps(result))\n'
    )


def _bybit_test_script(ping_url: str, auth_base: str, api_key: str, api_secret: str) -> str:
    """Build Bybit test script with reachability + credential validation."""
    return (
        'import json, urllib.request, hmac, hashlib, time\n'
        '\n'
        'result = {"success": False, "message": ""}\n'
        '\n'
        '# Step 1: Reachability\n'
        'try:\n'
        f'    req = urllib.request.urlopen("{ping_url}", timeout=10)\n'
        '    if req.status != 200:\n'
        '        result["message"] = f"Exchange unreachable (HTTP {req.status})"\n'
        '        print(json.dumps(result))\n'
        '        raise SystemExit\n'
        'except SystemExit:\n'
        '    raise\n'
        'except Exception as e:\n'
        '    result["message"] = f"Exchange unreachable: {e}"\n'
        '    print(json.dumps(result))\n'
        '    raise SystemExit\n'
        '\n'
        '# Step 2: Credential validation\n'
        f'api_key = "{api_key}"\n'
        f'api_secret = "{api_secret}"\n'
        '\n'
        'if not api_key or not api_secret:\n'
        '    result["success"] = True\n'
        '    result["message"] = "Exchange is reachable, but no API credentials were provided to verify authentication."\n'
        '    print(json.dumps(result))\n'
        '    raise SystemExit\n'
        '\n'
        'try:\n'
        '    ts = str(int(time.time() * 1000))\n'
        '    recv_window = "5000"\n'
        '    param_str = ts + api_key + recv_window\n'
        '    sig = hmac.new(api_secret.encode(), param_str.encode(), hashlib.sha256).hexdigest()\n'
        f'    url = "{auth_base}/v5/user/query-api"\n'
        '    req = urllib.request.Request(url)\n'
        '    req.add_header("X-BAPI-API-KEY", api_key)\n'
        '    req.add_header("X-BAPI-SIGN", sig)\n'
        '    req.add_header("X-BAPI-TIMESTAMP", ts)\n'
        '    req.add_header("X-BAPI-RECV-WINDOW", recv_window)\n'
        '    resp = urllib.request.urlopen(req, timeout=10)\n'
        '    body = json.loads(resp.read().decode())\n'
        '    if body.get("retCode") == 0:\n'
        '        result["success"] = True\n'
        '        result["message"] = "Connection successful! API credentials are valid."\n'
        '    else:\n'
        '        msg = body.get("retMsg", "Unknown error")\n'
        '        result["message"] = f"Exchange is reachable, but authentication failed: {msg}"\n'
        'except urllib.error.HTTPError as e:\n'
        '    body = e.read().decode()\n'
        '    try:\n'
        '        err = json.loads(body)\n'
        '        msg = err.get("retMsg", body[:200])\n'
        '    except Exception:\n'
        '        msg = body[:200]\n'
        '    result["message"] = f"Exchange is reachable, but authentication failed: {msg}"\n'
        'except Exception as e:\n'
        '    result["message"] = f"Exchange is reachable, but credential test failed: {e}"\n'
        '\n'
        'print(json.dumps(result))\n'
    )


# ─── Catalog Pair Discovery ───────────────────────────────────────────────

# The catalog lives at <project-root>/catalog, one directory level above
# adapter_admin. The FX conversion UI needs to know which pairs are ingested
# so the admin can bind a currency to the right catalog series.
_CATALOG_ROOT = PROJECT_DIR.parent / "catalog"


@app.route("/api/catalog/pairs")
def list_catalog_pairs():
    """List currency pair instruments available in the catalog, grouped by venue.

    Query params
    ------------
    venue : optional — filter to a single venue (case-insensitive)

    Response
    --------
    {
        "pairs": [
            {
                "instrument_id": "USDJPY.FOREX_MS",
                "symbol": "USDJPY",
                "venue": "FOREX_MS",
                "base": "USD",
                "quote": "JPY"
            },
            ...
        ]
    }
    """
    venue_filter = (request.args.get("venue") or "").strip().upper() or None

    bar_root = _CATALOG_ROOT / "data" / "bar"
    cp_root = _CATALOG_ROOT / "data" / "currency_pair"

    # Prefer currency_pair/<ID>/... dirs (one entry per instrument); fall back
    # to bar/<ID>-... which has one entry per bar type but many duplicates.
    instrument_ids: set[str] = set()
    if cp_root.exists():
        for d in cp_root.iterdir():
            if d.is_dir() and "." in d.name:
                instrument_ids.add(d.name)
    if not instrument_ids and bar_root.exists():
        for d in bar_root.iterdir():
            if d.is_dir() and "." in d.name:
                # "USDJPY.FOREX_MS-1-MINUTE-BID-EXTERNAL" → "USDJPY.FOREX_MS"
                instrument_ids.add(d.name.split("-", 1)[0])

    pairs = []
    for iid in sorted(instrument_ids):
        if "." not in iid:
            continue
        symbol, venue = iid.split(".", 1)
        symbol = symbol.strip().upper()
        venue = venue.strip().upper()
        if venue_filter and venue != venue_filter:
            continue
        # Heuristic: FX symbols are 6 characters with 3/3 base/quote split.
        # For other asset classes we just report symbol without splitting.
        base, quote = None, None
        if len(symbol) == 6:
            base, quote = symbol[:3], symbol[3:]
        pairs.append({
            "instrument_id": iid,
            "symbol": symbol,
            "venue": venue,
            "base": base,
            "quote": quote,
        })
    return jsonify({"pairs": pairs, "count": len(pairs)})


# ─── Custom Adapter File Management ───────────────────────────────────────

@app.route("/api/custom_adapters/list")
def list_custom_adapters():
    """List all uploaded custom adapter files."""
    CUSTOM_ADAPTERS_DIR.mkdir(exist_ok=True)
    files = sorted(
        f.name for f in CUSTOM_ADAPTERS_DIR.glob("*.py")
        if not f.name.startswith("__")
    )
    return jsonify({"files": files, "count": len(files)})


@app.route("/api/custom_adapters/upload", methods=["POST"])
def upload_custom_adapter():
    """Upload and validate a custom adapter .py file."""
    CUSTOM_ADAPTERS_DIR.mkdir(exist_ok=True)

    if "file" not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files["file"]
    if not file.filename or not file.filename.endswith(".py"):
        return jsonify({"error": "File must be a .py file"}), 400

    safe_name = sanitize_filename(file.filename)
    if not safe_name.endswith(".py"):
        safe_name += ".py"
    dest_path = CUSTOM_ADAPTERS_DIR / safe_name
    file_existed = dest_path.exists()

    file.save(str(dest_path))

    # Run validation in subprocess
    validate_script = get_validation_script(dest_path, PROJECT_DIR)

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

        vresult = json.loads(output)

        if not vresult.get("ok"):
            dest_path.unlink(missing_ok=True)
            return jsonify({"error": vresult.get("error", "Validation failed")}), 400

        adapter_name = vresult["adapter_name"]

        # Check for duplicate names
        for other_file in CUSTOM_ADAPTERS_DIR.glob("*.py"):
            if other_file.name == safe_name or other_file.name.startswith("__"):
                continue
            try:
                content = other_file.read_text(encoding="utf-8")
                for line in content.splitlines():
                    line = line.strip()
                    if line.startswith("ADAPTER_NAME") and "=" in line:
                        val = line.split("=", 1)[1].strip().strip("\"'")
                        if val == adapter_name:
                            dest_path.unlink()
                            return jsonify({"error": f"A custom adapter named '{adapter_name}' already exists in {other_file.name}"}), 400
                        break
            except Exception:
                continue

        invalidate_cache()
        msg = f"Replaced existing file: {safe_name}" if file_existed else f"Adapter '{adapter_name}' uploaded successfully!"
        return jsonify({
            "success": True,
            "message": msg,
            "adapter_name": adapter_name,
            "filename": safe_name,
            "supports_data": vresult.get("supports_data", False),
            "supports_exec": vresult.get("supports_exec", False),
            "params": vresult.get("params", {}),
        })

    except subprocess.TimeoutExpired:
        dest_path.unlink(missing_ok=True)
        return jsonify({"error": "Validation timed out (30s)"}), 400
    except Exception as e:
        dest_path.unlink(missing_ok=True)
        return jsonify({"error": f"Unexpected error: {e}"}), 500


@app.route("/api/custom_adapters/<filename>", methods=["DELETE"])
def delete_custom_adapter(filename):
    """Delete a custom adapter file."""
    safe_name = sanitize_filename(filename)
    file_path = CUSTOM_ADAPTERS_DIR / safe_name

    if not file_path.exists():
        return jsonify({"error": "File not found"}), 404

    file_path.unlink()
    invalidate_cache()
    return jsonify({"success": True, "message": f"Deleted {safe_name}"})


@app.route("/api/custom_adapters/template")
def download_template():
    """Return the adapter template file content."""
    content = get_adapter_template()
    return Response(
        content,
        mimetype="text/x-python",
        headers={"Content-Disposition": "attachment; filename=custom_adapter_template.py"},
    )


@app.route("/api/custom_adapters/guidelines")
def adapter_guidelines():
    """Return adapter guidelines as markdown."""
    return jsonify({"guidelines": get_adapter_guidelines()})


# ─── Users API (multi-user identity layer) ────────────────────────────────
# Reads/writes config/users.json. The full registry (user_id, alias,
# multiplier, allowed_instruments) is admin-only — the main server only
# exposes a public-safe slim list at /api/users/list.

@app.route("/api/users")
def get_users():
    """Return the full users registry — admin-only view."""
    return jsonify(_load_users())


@app.route("/api/users", methods=["POST"])
def post_users():
    """Replace the users registry. Validates before writing.

    Same shape as the GET response. Validation enforces slug-safe ids,
    positive multipliers, well-formed allowlists, and no duplicates so the
    on-disk file always satisfies our resolver invariants.
    """
    payload = request.json
    if not isinstance(payload, dict):
        return jsonify({"error": "JSON object required"}), 400
    # Preserve the existing _meta block if the client didn't send one.
    if "_meta" not in payload:
        existing = _load_users()
        if isinstance(existing, dict) and "_meta" in existing:
            payload["_meta"] = existing["_meta"]
    ok, err = _validate_registry_payload(payload)
    if not ok:
        return jsonify({"error": err}), 400
    _save_users(payload)
    return jsonify({"success": True, "users": payload.get("users", [])})


# ─── Main ──────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    ADAPTERS_CONFIG_DIR.mkdir(exist_ok=True)
    CUSTOM_ADAPTERS_DIR.mkdir(exist_ok=True)

    print("=" * 60)
    print("  M_Cube Adapter Admin Panel")
    print("  Open http://localhost:5001 in your browser")
    print("=" * 60)
    app.run(debug=True, host="0.0.0.0", port=5001, use_reloader=False)
