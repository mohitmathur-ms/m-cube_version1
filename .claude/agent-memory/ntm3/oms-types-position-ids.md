---
name: oms-types-position-ids
description: How OmsType (NETTING/HEDGING/UNSPECIFIED) drives position ID assignment in nautilus_trader 1.224.0 — venue vs strategy config, resolution order
metadata:
  type: reference
---

OmsType enum (`nautilus_trader/model/enums.py` -> rust core): UNSPECIFIED=0, NETTING=1, HEDGING=2.

Resolution order at fill time (`execution/engine.pyx::_determine_oms_type`, ~L1413):
1. Strategy override (`self._oms_overrides[strategy_id]`, set by `register_oms_type` when Trader adds a strategy). Strategy's `oms_type` comes from `StrategyConfig.oms_type` (default `None` -> UNSPECIFIED).
2. If UNSPECIFIED -> fall through to the venue/exec client's native `oms_type`.
3. If no client at all -> default NETTING.

Position ID rules (`execution/engine.pyx`):
- NETTING: deterministic ID `f"{instrument_id}-{strategy_id}"` (`_determine_netting_position_id`, L1524). One netted position per instrument+strategy; closed position is snapshotted before ID reuse on flip/reopen.
- HEDGING: new generated ID per entry (`_pos_id_generator.generate`); venue can also generate venue position IDs. Reopening a closed position in HEDGING just logs a warning (not allowed in NETTING).

Backtest venue config: `BacktestVenueConfig.oms_type` (`backtest/config.py`), required field (no default). `BacktestEngine.add_venue(oms_type=...)` is also required (positional). Docstring: "If HEDGING will generate new position IDs." Matching engine `_get_position_id` (backtest/engine.pyx L7573) implements venue-side ID gen for HEDGING.

m-cube relevance: venue is added in `core/backtest_runner.py` / `core/venue_config.py`; check what oms_type it passes to add_venue. NETTING is the typical FX choice (one net position per pair per strategy), which also makes the grouped Path-A per-strategy_id PnL recovery clean.
