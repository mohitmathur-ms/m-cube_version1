"""Mixin that lets a plain entry-strategy aggregate its base bars in-process.

Strategies subscribe to the BASE timeframe (e.g. 1-MINUTE EXTERNAL) and route
every incoming bar through :meth:`_route_bar`. When an ``aggregate_to_bar_type``
is configured, bars are fed to a :class:`core.aggregator.BarAggregator` and the
strategy logic only runs on the emitted higher-timeframe bar — replacing
Nautilus' built-in ``TimeBarAggregator`` (no composite ``INTERNAL@`` bar types).

When no aggregation is configured the mixin is a no-op passthrough: indicators
are registered with the engine as before and ``_route_bar`` returns the bar
unchanged, so existing single-timeframe behaviour is byte-identical.
"""

from __future__ import annotations

from nautilus_trader.model.data import Bar, BarType

from core.aggregator import (
    BarAggregator,
    external_from_composite,
    timeframe_of_bar_type,
)


class AggregatingStrategyMixin:
    """Add streaming aggregation to a ``nautilus_trader`` ``Strategy`` subclass.

    Usage in the host strategy:

    - ``on_start``: build indicators, then call
      ``self._setup_aggregation(instrument, base_bt, aggregate_to_bt, [inds...])``.
      If it returns ``True`` (aggregating), subscribe ONLY to ``base_bt`` and do
      NOT ``register_indicator_for_bars`` (the mixin feeds them). If it returns
      ``False`` (passthrough), register/subscribe exactly as before.
    - ``on_bar``: ``bar = self._route_bar(bar); if bar is None: return`` then run
      the existing logic against ``bar``.
    - Gate entries on ``self._indicators_ready()`` instead of
      ``self.indicators_initialized()`` (works in both modes).
    - ``on_stop``: call ``self._flush_aggregator()``.
    """

    def _setup_aggregation(
        self,
        instrument,
        base_bar_type: BarType,
        aggregate_to_bar_type: str,
        indicators,
    ) -> bool:
        """Configure aggregation. Returns ``True`` when aggregating, else ``False``."""
        self._agg_indicators = [i for i in (indicators or []) if i is not None]
        self._aggregator: BarAggregator | None = None
        self._aggregating = False

        target = str(aggregate_to_bar_type or "").strip()
        if target:
            target = external_from_composite(target) or target
        if not target:
            return False

        base_bt = (
            base_bar_type
            if isinstance(base_bar_type, BarType)
            else BarType.from_str(str(base_bar_type))
        )
        try:
            target_bt = BarType.from_str(target)
        except Exception as exc:  # noqa: BLE001 — degrade to passthrough
            self.log.warning(f"aggregate_to_bar_type {target!r} invalid ({exc}); not aggregating")
            return False

        # Same timeframe as the base ⇒ nothing to aggregate.
        if target_bt == base_bt:
            return False

        try:
            self._aggregator = BarAggregator(
                instrument, target_bt, timeframe_of_bar_type(target)
            )
        except ValueError as exc:
            # Unsupported timeframe for streaming floor (e.g. 1-MONTH).
            self.log.warning(f"aggregation disabled for {target!r} ({exc}); not aggregating")
            return False

        self._aggregating = True
        return True

    def _route_bar(self, bar: Bar) -> Bar | None:
        """Passthrough the bar, or aggregate and return the closed window (or None)."""
        if not getattr(self, "_aggregating", False):
            return bar
        agg = self._aggregator.on_bar(bar)
        if agg is None:
            return None
        for ind in self._agg_indicators:
            ind.handle_bar(agg)
        return agg

    def _indicators_ready(self) -> bool:
        """True once every tracked indicator is initialized (both modes)."""
        return all(ind.initialized for ind in getattr(self, "_agg_indicators", []))

    def _flush_aggregator(self) -> None:
        """Drain the trailing partial window at stop (no order action)."""
        if getattr(self, "_aggregating", False) and self._aggregator is not None:
            self._aggregator.flush()
