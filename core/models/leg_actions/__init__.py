"""Leg-level exit-action parsing and validation (spec execution_logic.html §4.7/§4.8).

A leg's ``on_sl_action`` / ``on_target_action`` may be a single legacy value or
a comma-separated combination of up to three actions. This component owns the
canonical list of valid actions, the splitter, and the combination validator.
"""

from __future__ import annotations


# Valid leg-level exit actions (spec execution_logic.html §4.7).
VALID_LEG_ACTIONS = (
    "close", "re_execute", "reverse", "execute", "re_entry", "keep_leg_running",
)


def parse_leg_actions(action_str: str | None) -> list[str]:
    """Split a leg action config string into an ordered, de-duplicated list.

    Accepts the legacy bare single value ("close") and the new comma-separated
    combination form ("re_execute,execute"). Empty / None → ["close"].
    """
    if not action_str:
        return ["close"]
    seen: list[str] = []
    for tok in str(action_str).split(","):
        a = tok.strip()
        if a and a not in seen:
            seen.append(a)
    return seen or ["close"]


def validate_leg_actions(
    action_str: str | None, has_execute_target: bool = False
) -> tuple[bool, str]:
    """Validate a leg action combination per spec execution_logic.html §4.8.

    Returns ``(ok, error_message)``. Rules enforced:
      • every token is a known action
      • at most 3 actions
      • ``keep_leg_running`` must be the only action
      • ``re_execute`` + ``re_entry`` is contradictory
      • ``execute`` requires a configured ``execute_target_leg_id``
    """
    actions = parse_leg_actions(action_str)
    unknown = [a for a in actions if a not in VALID_LEG_ACTIONS]
    if unknown:
        return False, f"unknown leg action(s): {', '.join(unknown)}"
    if len(actions) > 3:
        return False, "at most 3 leg actions may be combined"
    if "keep_leg_running" in actions and len(actions) > 1:
        return False, "'keep_leg_running' cannot be combined with other actions"
    if "re_execute" in actions and "re_entry" in actions:
        return False, "'re_execute' and 're_entry' cannot be combined"
    if "execute" in actions and not has_execute_target:
        return False, "'execute' action requires an execute_target_leg_id"
    return True, ""
