"""
Dynamic adapter discovery for NautilusTrader.

Scans nautilus_trader.adapters at runtime to find all available adapters,
introspects their config classes to extract configurable fields, and merges
with custom uploaded adapters into a unified registry.
"""

from __future__ import annotations

import enum
import importlib
import importlib.util
import logging
import pkgutil
import sys
import types
import typing
from pathlib import Path

logger = logging.getLogger(__name__)

# Adapters to skip during discovery
_EXCLUDED = {"_template", "__pycache__", "sandbox", "env"}

# Fields inherited from base config classes — internal plumbing, not user-facing
_BASE_FIELDS: set[str] | None = None

# Heuristic patterns for detecting sensitive fields
_SENSITIVE_PATTERNS = {"key", "secret", "mnemonic", "private_key", "passphrase", "password", "token"}

# Acronyms to uppercase in labels
_ACRONYMS = {"api", "http", "ws", "url", "id", "ibg", "ip", "gtd", "ms"}

# Display name overrides for adapters with non-obvious naming
_NAME_MAP = {
    "interactive_brokers": "InteractiveBrokers",
    "dydx": "dYdX",
    "architect_ax": "ArchitectAX",
    "okx": "OKX",
    "bitmex": "BitMEX",
}


def _get_base_fields() -> set[str]:
    """Get field names from base config classes to exclude from UI."""
    global _BASE_FIELDS
    if _BASE_FIELDS is not None:
        return _BASE_FIELDS

    try:
        import msgspec.structs
        from nautilus_trader.config import LiveDataClientConfig, LiveExecClientConfig

        base = set()
        for cls in (LiveDataClientConfig, LiveExecClientConfig):
            for f in msgspec.structs.fields(cls):
                base.add(f.name)
        _BASE_FIELDS = base
    except Exception:
        _BASE_FIELDS = {"handle_revised_bars", "instrument_provider", "routing"}

    return _BASE_FIELDS


def _is_sensitive(field_name: str) -> bool:
    """Check if a field name suggests sensitive data."""
    name_lower = field_name.lower()
    # Exact-match fields that are NOT sensitive despite containing a pattern word
    if name_lower in ("key_type", "api_passphrase"):
        return name_lower == "api_passphrase"  # passphrase is sensitive, key_type is not
    return any(pat in name_lower for pat in _SENSITIVE_PATTERNS)


def _field_to_label(field_name: str) -> str:
    """Convert snake_case field name to a human-readable label."""
    words = field_name.replace("_", " ").split()
    return " ".join(
        w.upper() if w.lower() in _ACRONYMS else w.capitalize()
        for w in words
    )


def _unwrap_type(tp):
    """Unwrap Optional/Union/Annotated to get the core type."""
    # Unwrap Annotated (e.g., Annotated[int, Meta(gt=0)])
    if typing.get_origin(tp) is typing.Annotated:
        tp = typing.get_args(tp)[0]

    # Unwrap Optional / Union with None — handles both typing.Union and types.UnionType
    origin = getattr(tp, "__origin__", None)
    args = getattr(tp, "__args__", ())

    if isinstance(tp, types.UnionType) or origin is typing.Union:
        non_none = [a for a in args if a is not type(None)]
        if non_none:
            tp = non_none[0]
            # Unwrap Annotated inside Optional
            if typing.get_origin(tp) is typing.Annotated:
                tp = typing.get_args(tp)[0]

    return tp


def _type_to_param(field_name: str, field_type, default) -> dict:
    """Convert a Python type + default into a UI param definition."""
    actual = _unwrap_type(field_type)
    param = {
        "label": _field_to_label(field_name),
        "type": "text",
        "required": False,
        "sensitive": _is_sensitive(field_name),
    }

    # Set default value
    if default is not msgspec.NODEFAULT:
        if isinstance(default, enum.Enum):
            param["default"] = default.value
        elif isinstance(default, (str, int, float, bool)) or default is None:
            param["default"] = default
        # else: complex default (Venue, InstrumentProviderConfig, etc.) — skip

    # Determine UI type
    if isinstance(actual, type) and issubclass(actual, enum.Enum):
        param["type"] = "select"
        param["options"] = [m.value for m in actual]
    elif actual is bool:
        param["type"] = "checkbox"
    elif actual in (int, float):
        param["type"] = "number"
    elif actual is str:
        param["type"] = "text"
    else:
        # Complex types (dict, tuple, frozenset, custom classes) — text input
        param["type"] = "text"

    # Override text → password for sensitive fields
    if param["sensitive"] and param["type"] == "text":
        param["type"] = "password"

    return param


def _extract_params(config_cls) -> dict:
    """Extract UI-friendly param definitions from a msgspec.Struct config class."""
    import msgspec.structs

    base_fields = _get_base_fields()
    params = {}

    for f in msgspec.structs.fields(config_cls):
        if f.name in base_fields or f.name.startswith("_"):
            continue

        param = _type_to_param(f.name, f.type, f.default)

        # Skip fields with complex types that aren't useful in a form
        actual = _unwrap_type(f.type)
        if isinstance(actual, type) and not issubclass(actual, (str, int, float, bool, enum.Enum)):
            # Skip complex object types (Venue, InstrumentProviderConfig, etc.)
            continue

        # Also skip collection types (dict, tuple, frozenset, list)
        origin = getattr(actual, "__origin__", None)
        if origin in (dict, tuple, frozenset, list, set):
            continue

        params[f.name] = param

    return params


def _introspect_adapter(modname: str) -> dict | None:
    """Import an adapter module and extract registry metadata."""
    mod = importlib.import_module(f"nautilus_trader.adapters.{modname}")
    all_exports = getattr(mod, "__all__", [])

    if not all_exports:
        all_exports = [x for x in dir(mod) if not x.startswith("_")]

    # Find config and factory classes by naming convention
    configs_data = [n for n in all_exports if n.endswith("DataClientConfig")]
    configs_exec = [n for n in all_exports if n.endswith("ExecClientConfig")]
    factories_data = [n for n in all_exports if n.endswith("LiveDataClientFactory")]
    factories_exec = [n for n in all_exports if n.endswith("LiveExecClientFactory")]

    # Fallback: some adapters (e.g. IB) don't re-export at the top level.
    # Try importing from .config and .factories submodules directly.
    _submod_classes = {}  # cls_name -> actual class object (for param extraction)
    if not configs_data and not configs_exec:
        for sub in ("config", "factories"):
            try:
                submod = importlib.import_module(f"nautilus_trader.adapters.{modname}.{sub}")
                for name in dir(submod):
                    if name.endswith("DataClientConfig"):
                        configs_data.append(name)
                        _submod_classes[name] = getattr(submod, name)
                    elif name.endswith("ExecClientConfig"):
                        configs_exec.append(name)
                        _submod_classes[name] = getattr(submod, name)
                    elif name.endswith("LiveDataClientFactory"):
                        factories_data.append(name)
                    elif name.endswith("LiveExecClientFactory"):
                        factories_exec.append(name)
            except Exception:
                pass

    if not configs_data and not configs_exec:
        return None

    # Display name
    display_name = _NAME_MAP.get(modname, modname.replace("_", " ").title().replace(" ", ""))

    # Extract params from config classes, merging data + exec (deduplicating)
    params = {}
    for cls_name in configs_data + configs_exec:
        try:
            cls = _submod_classes.get(cls_name) or getattr(mod, cls_name)
            cls_params = _extract_params(cls)
            for k, v in cls_params.items():
                if k not in params:
                    params[k] = v
        except Exception as e:
            logger.warning(f"Failed to extract params from {cls_name}: {e}")

    return {
        "module": f"nautilus_trader.adapters.{modname}",
        "display_name": display_name,
        "description": _generate_description(modname, bool(configs_data), bool(configs_exec)),
        "supports_data": bool(configs_data),
        "supports_exec": bool(configs_exec),
        "factory_data": factories_data[0] if factories_data else None,
        "factory_exec": factories_exec[0] if factories_exec else None,
        "config_data": configs_data[0] if configs_data else None,
        "config_exec": configs_exec[0] if configs_exec else None,
        "params": params,
        "source": "builtin",
    }


def _generate_description(modname: str, has_data: bool, has_exec: bool) -> str:
    """Generate a human-readable description for an adapter."""
    caps = []
    if has_data:
        caps.append("Data")
    if has_exec:
        caps.append("Execution")
    cap_str = " + ".join(caps)
    pretty = _NAME_MAP.get(modname, modname.replace("_", " ").title())
    return f"{pretty} adapter ({cap_str})"


def discover_builtin_adapters() -> dict:
    """Scan nautilus_trader.adapters and return a registry dict."""
    try:
        import nautilus_trader.adapters as adapters_pkg
    except ImportError:
        logger.warning("NautilusTrader not installed — no built-in adapters available")
        return {}

    # Need msgspec for field introspection
    try:
        import msgspec  # noqa: F401
    except ImportError:
        logger.warning("msgspec not installed — cannot introspect adapter configs")
        return {}

    registry = {}
    for _importer, modname, ispkg in pkgutil.iter_modules(adapters_pkg.__path__):
        if not ispkg or modname in _EXCLUDED:
            continue
        try:
            entry = _introspect_adapter(modname)
            if entry:
                registry[entry["display_name"]] = entry
        except Exception as e:
            logger.warning(f"Skipping adapter '{modname}': {e}")

    return registry


def discover_custom_adapters(custom_dir: Path) -> dict:
    """Scan the custom_adapters directory and return registry entries."""
    registry = {}
    if not custom_dir.exists():
        return registry

    for py_file in sorted(custom_dir.glob("*.py")):
        if py_file.name.startswith("__"):
            continue
        try:
            module_name = f"custom_adapter_{py_file.stem}"
            # Clear cached module to pick up changes
            if module_name in sys.modules:
                del sys.modules[module_name]

            spec = importlib.util.spec_from_file_location(module_name, str(py_file))
            mod = importlib.util.module_from_spec(spec)
            sys.modules[module_name] = mod
            spec.loader.exec_module(mod)

            name = getattr(mod, "ADAPTER_NAME", py_file.stem)
            raw_params = getattr(mod, "PARAMS", {})
            has_data = getattr(mod, "DATA_CLIENT_CLASS", None) is not None
            has_exec = getattr(mod, "EXEC_CLIENT_CLASS", None) is not None

            if not has_data and not has_exec:
                continue

            # Normalize params — custom adapters define PARAMS in a slightly
            # different format; ensure consistency with built-in param defs
            params = {}
            if isinstance(raw_params, dict):
                for k, v in raw_params.items():
                    param = {
                        "label": v.get("label", _field_to_label(k)),
                        "type": v.get("type", "text"),
                        "required": v.get("required", False),
                        "sensitive": v.get("sensitive", _is_sensitive(k)),
                    }
                    if "default" in v:
                        param["default"] = v["default"]
                    if "options" in v:
                        param["options"] = v["options"]
                    if "placeholder" in v:
                        param["placeholder"] = v["placeholder"]
                    if param["sensitive"] and param["type"] == "text":
                        param["type"] = "password"
                    params[k] = param

            registry[name] = {
                "module": str(py_file),
                "display_name": name,
                "description": f"Custom adapter: {name}",
                "supports_data": has_data,
                "supports_exec": has_exec,
                "factory_data": None,
                "factory_exec": None,
                "config_data": None,
                "config_exec": None,
                "params": params,
                "source": "custom",
                "custom_file": py_file.name,
            }
        except Exception as e:
            logger.warning(f"Skipping custom adapter '{py_file.name}': {e}")

    return registry


# ─── Cached registry ─────────────────────────────────────────────────────

_cache: dict | None = None
_custom_dir: Path = Path(__file__).resolve().parent / "custom_adapters"


def get_full_registry() -> dict:
    """Return the merged registry (built-in + custom), cached after first call."""
    global _cache
    if _cache is None:
        _cache = discover_builtin_adapters()
        custom = discover_custom_adapters(_custom_dir)
        _cache.update(custom)
    return _cache


def invalidate_cache():
    """Clear the cached registry. Call after custom adapter upload/delete."""
    global _cache
    _cache = None


# Make msgspec import lazy — only needed at introspection time
try:
    import msgspec
    msgspec.NODEFAULT  # verify the sentinel exists
except (ImportError, AttributeError):
    # Create a fallback sentinel
    class _NoDefault:
        pass
    class _msgspec_stub:
        NODEFAULT = _NoDefault()
    msgspec = _msgspec_stub  # type: ignore
