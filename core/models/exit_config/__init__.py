"""Per-leg exit-management settings (the innermost schema dataclass).

``ExitConfig`` is the leaf of the ``leg → slot → portfolio`` data-model
hierarchy: it captures everything about how a single strategy slot exits a
position — stop-loss / take-profit type and value, ATR sizing, trailing,
target-locking, leg-level trailing-target, SL/Target wait gates, on-SL/Target
actions (and their cross-leg / price-wait re-entry knobs), and leg-level
square-off.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass
class ExitConfig:
    """Exit management settings for a strategy slot."""

    # Exit-trigger data format (spec execution_logic.html §3 — three-format
    # engine). Selects which price series SL/Target triggers consult:
    #   "ohlcv"  — Format B: trigger on the slot's own bar high/low (default).
    #   "ltp"    — Format C: single last price; trigger collapses to close.
    #   "bidask" — Format A: SELL exits trigger on the BID series, BUY exits
    #              on the ASK series. Requires paired ASK/BID bars in the
    #              catalog (FX/synth-MID slots); falls back to "ohlcv" with a
    #              log when no bid/ask data is available.
    exit_price_format: str = "ohlcv"

    # Stop Loss
    stop_loss_type: str = "none"  # "none", "percentage", "points", "trailing", "atr"
    stop_loss_value: float = 0.0
    trailing_sl_step: float = 0.0
    trailing_sl_offset: float = 0.0

    # ATR-based SL (spec sl_features.html §1.1 footnote 4). When
    # stop_loss_type == "atr", the leg SL is sized from the bar series'
    # Average True Range at entry rather than a fixed distance:
    #   BUY leg : sl = entry_price − sl_atr_multiplier × ATR
    #   SELL leg: sl = entry_price + sl_atr_multiplier × ATR
    # sl_atr_period is the ATR lookback in bars. Both default 0 → feature off.
    sl_atr_period: int = 0
    sl_atr_multiplier: float = 0.0

    # Target / Take Profit
    target_type: str = "none"  # "none", "percentage", "points", "atr"
    target_value: float = 0.0

    # ATR-based Target (spec sl_features.html §1.1 footnote 4). When
    # target_type == "atr", the leg TP is sized from the bar series' Average
    # True Range at entry rather than a fixed distance:
    #   BUY leg : tp = entry_price + tgt_atr_multiplier × ATR
    #   SELL leg: tp = entry_price − tgt_atr_multiplier × ATR
    # tgt_atr_period is the ATR lookback in bars. Both default 0 → feature off.
    # Independent of the SL-side ATR knobs — SL and Target can each be ATR-sized
    # with their own period/multiplier (spec: "applies symmetrically").
    tgt_atr_period: int = 0
    tgt_atr_multiplier: float = 0.0

    # Target Locking (one-shot SL upgrade once a profit threshold is reached).
    target_lock_trigger: Optional[float] = None
    target_lock_minimum: Optional[float] = None

    # Leg-Level Trailing Target / Profit-Lock (spec execution_logic_target.html
    # §4.7). A ratcheting profit-lock at the leg level: once leg profit reaches
    # tgt_trail_when_profit_reach the lock activates at tgt_trail_lock_min_profit;
    # thereafter the locked floor ratchets up by tgt_trail_by for every
    # tgt_trail_every of further profit. If profit falls back to the floor the
    # leg exits (reason TARGET_TRAIL). All thresholds are in profit-% units
    # (consistent with target_lock_* and the leg trailing-SL ratchet).
    tgt_trail_enabled: bool = False
    tgt_trail_when_profit_reach: float = 0.0
    tgt_trail_lock_min_profit: float = 0.0
    tgt_trail_every: float = 0.0
    tgt_trail_by: float = 0.0

    # SL Wait — spec name "SL Wait (sec)". Prefer wall-clock seconds via
    # sl_wait_sec; sl_wait_bars retained for backward compat with existing
    # portfolio JSON. If both > 0, sl_wait_sec wins.
    sl_wait_sec: int = 0
    sl_wait_bars: int = 0

    # Target Wait — spec execution_logic_target.html §4.3. Mirror of SL Wait
    # on the target side: a target trigger must persist for the configured
    # duration before the exit fires; resets if price retreats inside the TP.
    # tgt_wait_sec is wall-clock seconds (preferred); tgt_wait_bars is the
    # legacy bar-count gate. If both > 0, tgt_wait_sec wins.
    tgt_wait_sec: int = 0
    tgt_wait_bars: int = 0

    # On Target/SL Actions. Valid values:
    #   "close"             — SqOff: flatten and stay flat.
    #   "re_execute"        — flatten + immediately re-arm signal entry (capped by max_re_executions).
    #   "reverse"           — close + open opposite side.
    #   "execute"           — flatten + arm execute_target_leg_id sibling slot (spec §1.2 1.2(c)).
    #   "re_entry"          — flatten + price-wait re-entry (spec §1.2 1.2(d)); see reentry_price.
    #   "keep_leg_running"  — ignore the trigger; position remains open with SL/TP disarmed for this trade.
    # Action combinations (spec execution_logic.html §4.8): up to 3 actions may
    # be combined as a comma-separated string (e.g. "re_execute,execute").
    # ``validate_leg_actions`` rejects invalid combos. A bare single value (no
    # comma) is the legacy form and still parses to a 1-element list.
    on_sl_action: str = "close"
    on_target_action: str = "close"
    max_re_executions: int = 0

    # ── Cross-leg & price-wait re-entry knobs (spec §1.2 1.2(c)/(d)) ──
    # Sibling slot to arm when this leg's on_sl_action / on_target_action
    # resolves to "execute". Empty string disables.
    execute_target_leg_id: str = ""
    # Price level the re_entry action waits for before re-arming. 0.0 falls
    # back to the original entry price recorded on the trade that exited.
    reentry_price: float = 0.0
    # Optional: limit how many price-wait re-entries can fire per day. 0 = unlimited.
    max_re_entries: int = 0
    # When False this leg starts UNARMED — it ignores its own signals until
    # a sibling slot's "execute" action arms it (spec §1.2 1.2(c)).
    armed_at_start: bool = True

    # Leg-level square-off (most specific). HH:MM, e.g. "17:00".
    # When set, every day at this local time the position is force-closed and
    # no new entries are allowed until the next session.
    squareoff_time: Optional[str] = None
    squareoff_tz: Optional[str] = None  # IANA name, e.g. "America/New_York"

    def has_exit_management(self) -> bool:
        return (
            self.stop_loss_type != "none"
            or self.target_type != "none"
            or self.tgt_trail_enabled
            or self.squareoff_time is not None
        )
