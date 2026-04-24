"""
Adapter registry for NautilusTrader exchanges.

Dynamically discovers built-in adapters from NautilusTrader at runtime
and merges with custom uploaded adapters. Provides masking utilities
for sensitive configuration values.
"""

from adapter_discovery import get_full_registry, _is_sensitive


def get_registry_for_frontend():
    """Return the registry safe for JSON serialization to the frontend."""
    registry = get_full_registry()
    result = {}
    for name, entry in registry.items():
        result[name] = {
            "description": entry["description"],
            "supports_data": entry["supports_data"],
            "supports_exec": entry["supports_exec"],
            "params": entry["params"],
            "source": entry.get("source", "builtin"),
        }
    return result


def mask_sensitive_value(value: str) -> str:
    """Mask a sensitive value, showing only the last 4 characters."""
    if not value or len(value) <= 4:
        return "****"
    return "****" + value[-4:]


def is_masked(value: str) -> bool:
    """Check if a value is a masked placeholder."""
    return isinstance(value, str) and value.startswith("****")


def mask_config(config: dict, exchange_type: str) -> dict:
    """Mask sensitive fields in an adapter config dict."""
    registry = get_full_registry()
    registry_entry = registry.get(exchange_type, {})
    params = registry_entry.get("params", {})

    masked = config.copy()
    for section_key in ("data_config", "exec_config"):
        section = masked.get(section_key)
        if not section:
            continue
        section = section.copy()
        for param_name in section:
            param_def = params.get(param_name, {})
            is_sens = param_def.get("sensitive", False) or _is_sensitive(param_name)
            if is_sens and isinstance(section[param_name], str) and section[param_name]:
                section[param_name] = mask_sensitive_value(section[param_name])
        masked[section_key] = section

    return masked
