"""Composite (internally-aggregated) bar-type construction and validation.

Two helpers built on ``timeframe_utils``:
- ``build_composite_bar_type`` assembles a NautilusTrader composite bar type
  (``...-INTERNAL@<base>-EXTERNAL``) from a base feed + a coarser timeframe.
- ``normalize_strategy_bar_types`` validates/repairs a slot's strategy-subscribe
  timeframes against its data feed (coarser → keep, same → collapse, finer → drop).
"""

from __future__ import annotations

from core.models.timeframe_utils import _bar_type_timeframe, _timeframe_seconds


def build_composite_bar_type(base_bar_type: str, sub_step_unit: str) -> str:
    """Build a NautilusTrader composite (internally-aggregated) bar type.

    Given a BASE EXTERNAL bar type and a (coarser-or-equal) strategy-subscribe
    timeframe, returns the bar type the strategy subscribes to:

        base "EURUSD.FOREX_MS-1-MINUTE-BID-EXTERNAL", sub "5-MINUTE"
          -> "EURUSD.FOREX_MS-5-MINUTE-BID-INTERNAL@1-MINUTE-EXTERNAL"

    The ``@<step>-<unit>-EXTERNAL`` suffix is Nautilus's composite-source spec:
    a 5-minute bar internally aggregated from the loaded 1-minute EXTERNAL
    data. When ``sub_step_unit`` equals the base step-unit the result is just
    the base bar type (no aggregation). Returns "" on malformed input.
    """
    parts = str(base_bar_type or "").split("-")
    if len(parts) < 5:
        return ""
    inst, b_step, b_unit, price = parts[0], parts[1], parts[2], parts[3]
    sub = str(sub_step_unit or "").strip().upper()
    if not sub:
        return ""
    base_tf = f"{b_step}-{b_unit}"
    if sub == base_tf:
        return f"{inst}-{base_tf}-{price}-EXTERNAL"
    return f"{inst}-{sub}-{price}-INTERNAL@{b_step}-{b_unit}-EXTERNAL"


def normalize_strategy_bar_types(
    base_bar_type: str,
    strategy_bar_types,
) -> tuple[list[str], list[str]]:
    """Validate/repair a slot's strategy-subscribe timeframes against its data feed.

    The strategy evaluates signals only on its ``strategy_bar_types`` (an
    INTERNAL composite that NautilusTrader aggregates from the EXTERNAL
    ``base_bar_type`` feed). Aggregation only works when the target timeframe is
    strictly **coarser** than the feed; a same-resolution or finer composite
    makes the aggregator emit ~zero bars, so the strategy silently trades
    nothing. Per-entry rule:

    * **coarser** (target > base): valid aggregation — keep the entry.
    * **same** (target == base): collapse — drop the degenerate composite so the
      strategy runs on the base feed directly ("ingest the data as-is").
    * **finer** (target < base): can't aggregate up — drop it (fall back to the
      base feed) and emit a warning.

    Non-INTERNAL or unparseable entries are kept untouched (defensive). Returns
    ``(cleaned_list, warnings)``; ``cleaned_list`` is de-duplicated, order-preserving.
    """
    cleaned: list[str] = []
    warnings: list[str] = []
    seen: set[str] = set()

    base_tf = _bar_type_timeframe(base_bar_type)
    base_sec = _timeframe_seconds(base_tf)

    def _keep(s: str) -> None:
        if s not in seen:
            seen.add(s)
            cleaned.append(s)

    for entry in (strategy_bar_types or []):
        s = str(entry or "").strip()
        if not s:
            continue
        is_internal = "INTERNAL" in s.upper()
        tgt_tf = _bar_type_timeframe(s)
        tgt_sec = _timeframe_seconds(tgt_tf)

        # Defensive: a plain EXTERNAL sub, or anything we can't parse, is left
        # untouched rather than risk dropping a valid (if unusual) bar type.
        if not is_internal or base_sec is None or tgt_sec is None:
            _keep(s)
            continue

        if tgt_sec > base_sec:
            _keep(s)              # coarser → aggregate (unchanged behaviour)
        elif tgt_sec == base_sec:
            continue              # same → run on the base feed as-is
        else:
            warnings.append(      # finer → cannot aggregate up
                f"Strategy timeframe {tgt_tf} is finer than the data timeframe "
                f"{base_tf}; cannot aggregate up — running on the {base_tf} feed instead."
            )

    return cleaned, warnings
