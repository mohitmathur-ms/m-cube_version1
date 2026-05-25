"""The cross-portfolio event bus (publish / consume / clear) and portfolio
action normalisation (legacy aliases, ReExecute-family classifiers)."""

from __future__ import annotations


_VALID_PF_ACTIONS_FX = (
    "SqOff", "ReExecute",
    # ReExecute-family — all three drive the config-driven portfolio ReExecute
    # replay (spec §2.4): slots re-run flat from the clip timestamp and the
    # segment is spliced on. The "at Entry Price" variants replay as plain
    # ReExecute (the FX adaptation the spec marks ⚙️ — price-wait re-entry at a
    # specific level lives at the leg level, spec §1.2(d)).
    "ReExecute at Entry Price",
    "ReExecute Same Contract at EntryPrice",
    # Cross-portfolio actions (spec §2.1(h)/(i)/(j)). Each clip in this
    # portfolio also writes an event to the cross-portfolio bus; the named
    # target portfolio reads it at its next run start.
    "SqOff Other Portfolio",
    "Execute Other Portfolio",
    "Start Other Portfolio",
)
_OPTIONS_ONLY_PF_ACTIONS: tuple[str, ...] = ()
_CROSS_PORTFOLIO_ACTIONS = (
    "SqOff Other Portfolio", "Execute Other Portfolio", "Start Other Portfolio",
)

# Cross-portfolio event bus. Keyed by target_portfolio name; each entry is a
# list of pending events (action, source_portfolio, ts_iso). When portfolio B
# runs, `consume_cross_portfolio_events("B")` pops its queue and the runner
# applies the requested action(s) at run start.
_CROSS_PORTFOLIO_EVENT_BUS: dict[str, list[dict]] = {}


def publish_cross_portfolio_event(target_portfolio: str, action: str,
                                  source_portfolio: str, ts_iso: str) -> None:
    """Append a cross-portfolio event for later consumption by target_portfolio."""
    if not target_portfolio:
        return
    _CROSS_PORTFOLIO_EVENT_BUS.setdefault(target_portfolio, []).append(
        {"action": action, "source": source_portfolio, "ts": ts_iso}
    )


def consume_cross_portfolio_events(target_portfolio: str) -> list[dict]:
    """Pop all pending events targeting this portfolio. Returns [] if none."""
    if not target_portfolio:
        return []
    return _CROSS_PORTFOLIO_EVENT_BUS.pop(target_portfolio, []) or []


def clear_cross_portfolio_bus() -> None:
    """Reset the cross-portfolio bus (used between orchestrator sessions)."""
    _CROSS_PORTFOLIO_EVENT_BUS.clear()

# D2 rename (locked): "SameStrike" → "Same Contract". Futures use contract month, not strike.
# Translate legacy config strings on load so existing portfolios keep working.
_LEGACY_PF_ACTION_ALIASES = {
    "ReExecute SameStrike at EntryPrice": "ReExecute Same Contract at EntryPrice",
}


def _normalize_pf_action(action: str | None) -> str:
    """Translate legacy action names (D2) so old configs still resolve correctly."""
    if not action:
        return "SqOff"
    return _LEGACY_PF_ACTION_ALIASES.get(action, action)


# ReExecute-family actions all trigger clip-then-replay (spec §2.4 / §5.2).
_REEXECUTE_FAMILY_ACTIONS = (
    "ReExecute",
    "ReExecute at Entry Price",
    "ReExecute Same Contract at EntryPrice",
)
# The "at Entry Price" variants additionally pin each slot's first re-entry to
# its pre-clip entry price (price-wait re-entry, spec §1.2(d) / §5.2). "Same
# Contract" is automatic for FX (one instrument), so it shares the entry-price
# pin. Plain "ReExecute" re-enters at the next signal's market price.
_ENTRY_PRICE_REEXEC_ACTIONS = (
    "ReExecute at Entry Price",
    "ReExecute Same Contract at EntryPrice",
)


def _is_reexec_action(action: str) -> bool:
    return action in _REEXECUTE_FAMILY_ACTIONS


def _is_entry_price_reexec(action: str) -> bool:
    return action in _ENTRY_PRICE_REEXEC_ACTIONS
