# BTSoftware → M_Cube FX Clone: Knowledge Resource & Design

> Source: BTSoftware beta documentation (Mohit Mathur / BT Analytics, ~160 pages).
> Goal: Distill BTSoftware's Indian options backtesting features into a reusable
> knowledge base, and adapt them to a **FX/forex-first** engine built on
> **NautilusTrader**. Prefer Nautilus built-ins over custom code wherever possible.

**Audience**: This doc is a reference for future implementation sessions. It is
*not* a user manual — it is the spec / mapping we will code against.

---

## 1. What BTSoftware Is, In One Page

BTSoftware is a local-PC Indian options backtester + simulator built around a
strict **4-level hierarchy** where every level owns its own P&L guardrails:

```
User                 — daily max loss/profit, SqOff time
  └─ Strategy Tag    — grouping mechanism, strategy-wide max loss/profit/trailing
        └─ Portfolio — one multi-leg options strategy, entry/exit rules, combined SL/TGT
              └─ Leg — one strike (up to 8), individual SL/TGT/Trailing, on-action hooks
```

The engine's value is not *any single feature* but the **composability** of:

- Dynamic leg selection (ATM / Relative / Premium / Greeks)
- Multi-mode portfolio entry (StartTime, CombinedPremium, UnderlyingLevel, Range Breakout, Rolling Straddle Breakout)
- Target/SL expressed in *multiple units* (points, %, absolute premium, rupees, underlying movement, delta, theta)
- `OnTarget` / `OnSL` action chains (SqOff, ReExecute, ReEntry, Pyramiding, Execute_Other_Portfolio, Opposite_Position, …)
- Wait & Trade, Range Breakout, Freak Protection, Order Slicing layered on top
- A visual **Conditional Builder** with data-series + indicators + AND/OR brackets

These primitives assemble into everything from a plain short straddle to a
delta-neutral self-adjusting Iron Condor with conditional re-entries.

### FX-relevance verdict

~70% of the feature surface maps cleanly to FX spot/CFD/futures — what you lose
is strikes/greeks/expiry, what you keep is the entire **execution-and-exit
orchestration layer**, which is where the IP lives. The plan below preserves the
orchestration and strips away the options-specific strike/greek/expiry plumbing.

---

## 2. Concept Mapping: Options → FX

| BTSoftware concept | FX equivalent | Nautilus primitive |
|---|---|---|
| Symbol (NIFTY, BANKNIFTY) | Currency pair (EUR/USD, GBP/JPY) | `InstrumentId`, `CurrencyPair` |
| Underlying Spot/Future | Same pair (there is only one) — or a correlated pair used as "underlying" | bar subscription |
| Expiry (Weekly/Monthly/Weekly_1) | **N/A** (spot FX has no expiry). For FX futures/NDFs keep contract-month resolver. | `FuturesContract` for forwards |
| Strike (Normal / Relative / Both) | Price level: absolute, pips from current mid, or ATR-multiple | n/a — just a `Price` |
| ATM / Relative (ATM+2) | Current mid; or `mid ± N × strike_step` for grid/ladder strategies | — |
| Strike Step | Grid spacing (pips) | — |
| CE / PE / FUT | BUY / SELL (FX is directional, no option type) | `OrderSide` |
| Lots | Position size in base-currency units or mini-lots | `Quantity` |
| Lot Size | Contract size (often 100k for FX) | `Instrument.multiplier` |
| Premium (per-lot / all-lots) | Entry price × size; or MTM in account ccy | `Position.unrealized_pnl` |
| Combined Premium | Net MTM of the multi-leg structure | `Portfolio.net_exposure` + helper |
| Value Per Lot / All Lots | P&L per 1 lot vs total (UI-only difference) | formatting |
| Delta / Gamma / Vega / Theta / IV | **Not applicable to spot FX.** For FX *options* adapters: use pricing libs (py_vollib/QuantLib). For spot: drop these leg types. | external lib, later phase |
| Nearest Premium / Delta leg selection | **Map to:** nearest price to target, or nearest-Z-score on a signal | custom scanner |
| Freak Protection | Tick/spike filter on incoming quotes/bars | custom `MessageBus` filter |
| Implied Futures ATM | **N/A** (no synthetic futures for spot). Drop. | — |
| Lot SqOff at expiry | Position roll / scheduled close | `Clock.set_time_alert` |
| Broker = Stoxxo | Live adapters: OANDA, FXCM, IC Markets, Interactive Brokers, cTrader | `LiveExecClient` subclasses |

**Rule of thumb**: if a BTSoftware concept requires `strike`, `option_type`, or
greeks, either drop it for spot FX or map it to "price level" / "signal value".

---

## 3. Feature Catalog — What We Keep, What We Drop, What We Map

Organized by BTSoftware section so you can cross-reference the PDF page numbers.

### 3.1 Hierarchy (PDF: Hierarchy of BTSoftware)

Keep the whole 4-level structure — it is already partially present in
`core/models.py` (`PortfolioConfig → StrategySlotConfig → ExitConfig`). We
extend it:

```
Account (≈ User)                 // account_id, daily risk budget, SqOff time
  └─ StrategyTag                 // group label, tag-wide risk limits, linked accounts
        └─ Portfolio             // multi-leg FX structure, entry rule, combined exits
              └─ Leg              // one FX position, per-leg exits & action hooks
```

**Decisions (locked in):**
- Existing `PortfolioConfig` → rename conceptually to "StrategyTag layer"? **No** — keep name, add an optional `AccountConfig` and `StrategyTagConfig` above it. Breaking churn not worth it.
- **Multi-leg = internal legs** (resolved 2026-04-24). Each `StrategySlotConfig` holds `legs: list[LegConfig]`; a single-leg strategy has exactly one entry in that list. No cross-slot linking via `group_id`. Matches BTSoftware's model and keeps action-hook targets (`SqOff_Leg3`, `Execute_Leg5`, …) resolvable by local index without cross-slot lookups.

### 3.2 Portfolio Execution Modes (PDF: Entry Settings)

Every mode becomes a strategy lifecycle trigger inside `ManagedStrategy.on_start` / `on_bar`:

| Mode | FX meaning | Nautilus primitive |
|---|---|---|
| `StartTime` | Fire entry at wall-clock time | `Clock.set_time_alert_ns()` |
| `Manual` | External trigger (UI/API/message) | `MessageBus.subscribe("cmd.execute")` |
| `CombinedPremium` | Enter when net MTM of the multi-leg reaches threshold | `on_bar` predicate; use `Portfolio.unrealized_pnl(instrument_id)` |
| `CombinedPremiumCrossover` | Same but only on crossover (prev → curr) | keep previous-bar value, test sign change |
| `UnderlyingLevel` | Enter when reference instrument (e.g. DXY, EUR/USD, correlated pair) crosses a price | subscribe to 2nd instrument; `on_quote_tick` handler |
| `RollingStraddleBreakout` | Enter when a rolling ATM straddle premium breaks day-open / start-time by N% | **FX replacement:** "Rolling ATR Channel breakout" — enter when price breaks `day_open ± k × ATR` |
| `UnderlyingRangeBreakout` | Price breaks high/low of first N minutes | **Keep as-is**: Opening Range Breakout (ORB) |
| `LegRangeBreakout` | Leg's *own* price breaks range | Same but per-leg |
| `StrategyRangeBreakout` | Predefined options strategy (e.g. long straddle) breaks range | FX replacement: reference indicator/spread breaks range |

**Implementation hint**: all modes collapse into the same predicate pattern:

```python
# pseudo-code inside ManagedStrategy
def _should_enter(self, bar) -> bool:
    if self.cfg.mode == "start_time":
        return self.clock.timestamp_ns() >= self._entry_time_ns
    if self.cfg.mode == "combined_premium":
        return self._net_mtm() >= self.cfg.entry_price  # with sign handling
    if self.cfg.mode == "orb":
        return bar.close > self._orb_high + self.cfg.buffer
    ...
```

Build a `EntryRule` abstract class and one subclass per mode — keeps the switch closed.

**Reference instrument is data-only** (resolved 2026-04-24). Modes that reference a non-traded instrument (e.g., `UnderlyingLevel` watching DXY while trading EUR/USD, or watching EUR/USD as a leading indicator while trading GBP/USD) subscribe via `self.subscribe_bars(bar_type)` / `self.subscribe_quote_ticks(instrument_id)` without needing the instrument present in the backtest's `venues` / tradable `instruments`. The orchestrator loads the reference catalog separately and registers no account for it.

### 3.3 Leg Settings / Exits (PDF: Portfolio Legs Settings + Target/SL)

This is the densest part of the PDF. Target & SL each have multiple **types**:

| SL/TGT Type | FX meaning | Notes |
|---|---|---|
| `Premium` (% or points on leg's own price) | % or pips on entry price | trivial |
| `AbsolutePremium` | Absolute entry-price target | trivial |
| `Underlying` | % / points / absolute on underlying (spot/fut) | `self.cache.bar(ref_bar_type)` |
| `Strike` | Target based on strike price anchor | **FX**: becomes "price at entry time" anchor, almost same as `AbsolutePremium`; *drop as redundant* |
| `Delta` / `Theta` | Based on greek value | **FX spot: drop.** FX options: later phase. |
| `Combined Profit` (rupees) | MTM in **USD baseline** (resolved 2026-04-24) | sum per-leg `Portfolio.unrealized_pnl`, convert each leg to USD via Nautilus FX rates at check-time; keep account-ccy only for display/reporting |
| `Combined Premium` (points/%) | Combined spread value move | keep |
| `AbsoluteCombinedPremium` | Absolute target on combined spread | keep |
| `Combined Multi Targets` | Layered T1/T2/T3 scaling | keep — use `Order` splits |
| `Underlying Movement` (combined SL) | Net SL based on reference instrument | keep |
| `LossAndUnderlyingRange` | Combined SL plus range box on underlying | keep (two conditions OR'd) |

**Trailing variants:**
- Target locking ("If profit reaches X, lock at Y"): stateful — track `high_water_mark`
- Profit trailing ("Every X, trail by Y"): stateful stepper
- SL trailing (same pattern, mirrored)
- `Trail only after Move SL to Cost` — trailing starts only after break-even move

**"On Hit" action vocabulary** — the most valuable part of the whole PDF:

```
None
SqOff_Current_Portfolio
SqOff_Leg1..8                              # close a specific sibling leg
Execute_Leg1..8                            # un-idle and fire a specific sibling leg
SqOff_Other_Portfolio  <name>              # kill a different portfolio
Execute_Other_Portfolio <name>             # fire a different portfolio
Start_Other_Portfolio  <name>              # mark-ready (respects its own StartTime etc)
ReExecute_Current_Portfolio                # whole portfolio restart
ReExecuteLeg                               # re-fire this leg with same settings (strike may update for ATM)
ReExecuteLeg_AfterFill_SqOff               # fire new, close old after fill (margin-safe for hedges)
ReEntry                                    # re-enter at the ORIGINAL avg entry price
Reverse_Position                           # (not impl in btsw) flip long↔short
Opposite_Position                          # re-enter but on the opposite side
Pyramiding                                 # keep original + add same strike
Keep_Leg_Running                           # take the action but DON'T close this leg
Sqoff_Linked_Legs                          # close all ReExecuted children of this leg
ReExecute_Portfolio_OnComplete             # restart portfolio when last leg closes
ReExecute_Portfolio_AtEntryPrice           # restart portfolio but only when price hits original entry
ReExecute_At_Opposite_Leg_LTP              # re-execute this leg using OTHER leg's current LTP
Duplicate_And_Execute                      # makes a copy (used w/ SqOff Copy Legs flag on ReEntry)
```

All 20+ actions reduce to about **6 primitives** once you generalize:

| Primitive | Behavior |
|---|---|
| `CLOSE(target=self \| leg_id \| portfolio \| other_portfolio)` | Close positions |
| `OPEN(target=self \| leg_id \| other_portfolio, side_policy=same\|opposite)` | Fire entries |
| `DUPLICATE(source=self \| leg_id, overrides={...})` | Clone a leg config and fire |
| `SET_PRICE_ANCHOR(kind=orig_entry \| opposite_ltp \| original_sl \| level)` | Schedule a re-entry trigger at a price |
| `CHAIN_FLAG(keep_leg=True \| sqoff_linked=True \| after_fill=True)` | Modifiers on the above |
| `SCHEDULE(delay=n_sec \| wait=n_sec \| safety=n_sec)` | Temporal guards (LegReExecutionSafetySeconds, etc) |

**Design recommendation**: implement as an `Action` sum-type (`dataclass` with a `kind` discriminator) + a single `ActionExecutor` in the strategy that dispatches on `kind`. Avoid duplicating BTSoftware's 20+ enum values one-for-one in code — collapse to the 6 primitives.

### 3.4 Target / SL Monitoring Modes (PDF: Monitoring tab)

Every monitor can be set to one of three modes:

| Monitor mode | Meaning | Nautilus hook |
|---|---|---|
| `Realtime` | Check on every tick/quote | `on_quote_tick` / `on_trade_tick` |
| `MinuteClose` | Check on every N-minute bar close | `on_bar` with aggregation `BarType`|
| `Interval` | Check every N seconds from entry | `Clock.set_timer_ns(interval_ns=...)` |

**Key quote from PDF** (applies to trailing too): *"For MinuteClose time, the bridge will always calculate minute close from 9:15 (Market Start Time)."* FX runs 24/5 so we need a chosen anchor.

**Decision (resolved 2026-04-24): global `SESSION_ANCHOR_UTC = 22:00`** — the whole engine treats Sunday 22:00 UTC as "start of week" and every subsequent 22:00 UTC as "start of day." MinuteClose grids, daily window boundaries, ORB ranges, and `day_open` all align to this anchor for every pair. Provisional MVP choice; per-pair liquid-hours can layer on later as a separate `active_hours` entry filter (Option C in the original design choice) without rewriting the MinuteClose math.

### 3.5 Wait & Trade (PDF: Portfolio Legs Settings #9 + Extra Conditions)

**Leg-level W&T**: on entry signal, note the current leg price `P`, apply adjustment `a` (points/%), wait for price to cross `P + a` (with sign handling), then fire.

**Combined W&T** (Extra Conditions tab): same but on the net combined value.

**Force-fill logic** (BTSoftware innovation, keep this): if one leg already filled but others still waiting on W&T, schedule a forced execution at either:
- A force price (`P - x` for positive W&T), or
- A force time (N seconds after first fill)

Prevents a straddle's second leg from drifting away forever after the first fills.

**Nautilus**: use `Clock.set_time_alert_ns` for the time gate + `on_quote_tick` for the price gate. Keep local state, not a broker stop order — otherwise you can't cancel cleanly if the force-fill fires first.

### 3.6 Range Breakouts (PDF: Range Breakouts tab)

Three orthogonal range types — **all three can coexist on one portfolio**:

1. **Underlying range** — high/low of reference instrument between `start_time` and `range_end_time`. Entry on break (with `EntryAt = High | Low | Any | CEOnHigh-PEOnLow | …`).
2. **Leg range** — each leg's *own* price range; per-leg trigger. BUY fires above range-high, SELL below range-low. `Opposite Side SL` flips high↔low into stop-loss on the opposite side.
3. **Strategy range** — apply the range calc to a predefined options strategy value (straddle premium etc).

**FX adaptation**:
- Underlying range = Opening Range Breakout (ORB) on any chosen instrument.
- Leg range = per-pair ORB when trading multiple pairs as legs of one structure.
- Strategy range = drop the options strategy wrapper; replace with "range on any derived series" (e.g., EUR/USD – GBP/USD spread; an indicator value; an FX-options straddle premium if options adapter present).

**Monitoring**: same three modes (Realtime / MinuteClose / Interval).
**Buffer**: same pattern (points or %). **Opposite Side SL**: keep.

`Breakout Not Req on Legs` flag — for mixing ORB legs with immediate-fill legs. Keep.

### 3.7 Range Breakout — Rolling Straddle → Rolling ATR Channel (FX)

PDF: "Portfolio Execution Mode = RollingStraddleBreakout". Entry when rolling
ATM straddle premium has moved by `±x%` vs day-open OR start-time.

FX replacement (**strongly recommended**, this is a great concept):

```
RollingATRChannelBreakout:
  base        = day_open | start_time_price
  channel_top = base + k * ATR(period)
  channel_bot = base - k * ATR(period)
  fire long   when close crosses above channel_top
  fire short  when close crosses below channel_bot
```

Gives you a volatility-adaptive breakout with the same "rolling vs static"
knob.

### 3.8 Freak Protection (PDF: How to Start #8)

Three modes: `Off / Smart / Strong`. On detection of a spike, PNL monitoring (and therefore SL) pauses for a few seconds.

**FX**: much more actionable here because retail FX feeds carry more spikes than Indian options. Implementation:

- Track rolling median tick-to-tick move; if incoming tick deviates > `k × median`, classify as freak.
- Pause SL/TGT checks for `cooldown_ms`.
- Implement as a **Nautilus `DataClient` middleware / `MessageBus` interceptor** so it applies to everything downstream.
- **Default mode: `Smart`** (resolved 2026-04-24) — fast classify, short cooldown. `Off` and `Strong` remain per-portfolio overrides.

Skip the `At Broker` exception — in FX broker-side SL logic is rarely usable for this anyway.

### 3.9 Order Slicing (PDF: Other Settings #9)

Three modes:

- `NotReqd` — single shot (auto-slices at broker freeze-qty limit)
- `TimeSliced` — N lots per order, fixed `wait_seconds` between slices
- `ExecutionMode` — slices re-check the *entry condition* between slices; next slice fires only if the condition still holds

**FX**: `ExecutionMode`-style slicing is how you run larger-than-market orders on illiquid crosses / during gaps. `TimeSliced` is the simpler VWAP-ish child.

**Nautilus**: use `OrderList` with a **custom `ExecAlgorithm`**. Don't roll your own order router. Nautilus ships TWAP-style algos — extend those rather than replacing.

### 3.10 ReExecute / ReEntry safety (PDF: ReExecute Settings)

Keep every one of these knobs, they are battle-scarred:

| Knob | Purpose | Default |
|---|---|---|
| `NoReEntryReExecuteAfterPortfolioEndTime` | No new re-fires after session end | True |
| `ExecuteReExecuteSqOffLegsDelayInSec` | Gap between chained leg actions | 0 (configurable) |
| `ExecuteReExecutePortfolioDelayInSec` | Gap between chained portfolio actions | 0 |
| `LegReExecutionSafetySeconds` | Minimum gap between two re-fires of same leg (anti-loop) | 3 |
| `PortfolioReExecutionSafetySeconds` | Same for whole portfolio | 5 |
| `MaxReExecuteCountPortfolio` | Absolute cap | 0 = unlimited |
| `OnSLReExecuteCountPortfolio` | Cap only for SL-triggered re-fires | 0 |
| `OnTargetReExecuteCountPortfolio` | Cap only for TGT-triggered re-fires | 0 |
| `NoReExecuteIfMovedSLToCost` | Once break-even stop set, don't re-enter | True |
| `NoWaitTradeForReExecute` | Skip W&T on re-execution | False |
| `NoStrikeChangeForReExecute` | Re-use original strike (ATM vs current ATM) | False — **FX: rename to `NoPriceRefreshForReExecute`** |
| `LegReExecuteMonitoringSettings (Realtime / MinuteClose)` | Check cadence | Realtime |
| `ReEntryMonitoringType (Realtime / MinuteClose / Interval)` | Check cadence for ReEntry trigger | Realtime |
| `MinDelayInReEntrySec` | Debounce SL-to-ReEntry path | 0 |
| `ReEntryAtOriginalLegAvgEntryPrice` | Target = orig entry price, not last | True |
| `ReEntryOrderType` (MARKET / LIMIT / SL_LIMIT) | Re-entry execution style | MARKET |
| `ReEntryTriggerOn (None / SLPrice / EntryPrice)` + buffer | Arm re-entry only after prev SL/entry reached | None |
| `NoReEntryIfMovedSLToCost` | | True |

These are the **most important implementation notes in the entire PDF** —
copy them verbatim into the `ReEntryConfig` dataclass docstring when we build
it.

### 3.11 Conditional Builder (PDF: Conditional Builder using Indicators, ~75 pages)

This is a whole mini-engine inside BTSoftware: **DataSeries → Indicator → Condition → Strategy trigger**.

Design shape:

```
DataSeries     = (symbol, timeframe, [option_details])   # e.g. NIFTY_SPOT[5M], NIFTY_CE(ATM,MONTHLY,FUT)[1M]
Indicator      = (type, DataSeries, params, data_points) # SMA(14) on NIFTY_SPOT[5M], MACD has 3 data_points
Condition      = (lhs: Value, op: ComparisonOp, rhs: Value)
                 where Value ∈ {FixedValue, DataSeries.data_point, Indicator.data_point}
ConditionGroup = (logical_op: AND|OR, children: [Condition | ConditionGroup])  # supports nesting
Strategy       = (EntryCondition, ExitCondition, target_portfolio_to_fire)
```

**Operators**: `>`, `<`, `>=`, `<=`, `=`, `≠`, `crosses above`, `crosses below`, `in range of N% / N points`.

**Offsets**: any `Value` can be lagged: `RSI(14)[5 candles ago]`. Needed for crossover-free crossover detection and divergence.

**Modifiers (Composite Values)**: `(value [+ | - | × | ÷ | +%] modifier)`. Lets you write `Close > (SMA(50) + 50)` or `Close > (EntryPrice + 5%)`.

**Points system** (optional): each condition has a `weight`; the group fires when `sum(weights_of_true_conditions) >= threshold`. Useful for "7 of 10 filters must pass". Off by default.

#### 3.11.1 Supported indicators (keep the whole list)

Trend: SMA, EMA, WMA, HMA, SuperTrend, Ichimoku (Tenkan/Kijun/Span-A/Span-B/Chikou/CloudTop/CloudBottom)

Momentum: RSI, RSIDivergence (returns {Type: 1|2, Price, RSIValue, Offset}), MACD (MACD/Signal/Histogram), Momentum, Fisher Transform, CCI, MFI, ADX (ADX/+DI/-DI)

Volatility: Bollinger Bands (Upper/Middle/Lower), ATR, Donchian Channel, Keltner Channel

Volume: VWAP (incl. rolling), OBV, Elder Force Index, MFI

Support/Resistance: Pivot Points (Standard P/R1..R3/S1..S3), Camarilla (L1..L4/H1..H4/Pivot), CPR (Pivot/TC/BC), **SRZones** (0/1/2/3/4 discrete zones), **HighLowChannel** (custom on any data point — Close, Volume, etc.)

Patterns: CandlePatterns (Doji, Hammer, Shooting Star, Engulfing ×2, and others — returns 0/1 per candle, no persistence)

#### 3.11.2 Nautilus mapping — **use built-ins wherever possible**

Nautilus ships most of these. Do **not** reimplement:

```python
from nautilus_trader.indicators.average.sma     import SimpleMovingAverage
from nautilus_trader.indicators.average.ema     import ExponentialMovingAverage
from nautilus_trader.indicators.average.wma     import WeightedMovingAverage
from nautilus_trader.indicators.average.hma     import HullMovingAverage
from nautilus_trader.indicators.rsi             import RelativeStrengthIndex
from nautilus_trader.indicators.macd            import MovingAverageConvergenceDivergence
from nautilus_trader.indicators.atr             import AverageTrueRange
from nautilus_trader.indicators.bollinger_bands import BollingerBands
from nautilus_trader.indicators.donchian_channel import DonchianChannel
from nautilus_trader.indicators.keltner_channel  import KeltnerChannel
from nautilus_trader.indicators.vwap             import VolumeWeightedAveragePrice
from nautilus_trader.indicators.obv              import OnBalanceVolume
from nautilus_trader.indicators.adx              import AverageDirectionalIndex
from nautilus_trader.indicators.cci              import CommodityChannelIndex
# + more under nautilus_trader.indicators.*
```

Every built-in Nautilus indicator implements the `Indicator` base, exposing
`handle_bar(bar)`, `value`, `initialized`. Wrap them once in our
`IndicatorAdapter` so the Condition Builder can read them uniformly.

For indicators Nautilus does **not** ship — implement only these:

- **SuperTrend** (simple, ATR + trend-flip state) — 50 LOC
- **Ichimoku** — 80 LOC
- **Fisher Transform** — 30 LOC
- **Elder Force Index** — 20 LOC
- **Pivot Points (Standard + Camarilla)** + **CPR** — session-aware, 100 LOC combined
- **SRZones** — zone detection, more complex (~200 LOC). Defer to phase 2.
- **HighLowChannel** over arbitrary data point — 40 LOC
- **RSIDivergence** as a pivot-based scanner — 100 LOC
- **CandlePatterns** — each pattern is 5–15 LOC; build as a single module with a registry.

Total custom-indicator LOC: ~700. Compare to BTSoftware's full set which would be thousands if written from scratch — this is why using Nautilus built-ins matters.

#### 3.11.3 Data series factory

```python
# core/data_series.py  (new)
@dataclass
class DataSeriesSpec:
    symbol: str | Literal["XXXXX"]    # XXXXX = inherit from portfolio at runtime
    timeframe_minutes: int             # 1..120, or 1440 for daily
    source: Literal["spot","future","option_ce","option_pe","option_pair"] = "spot"
    # option-specific fields, all Optional, ignored for spot FX:
    expiry: Optional[str] = None
    strike_selection: Optional[str] = None  # "ATM", "ATM+N", "NearestPremium", ...
    strike_step: Optional[float] = None
    leg2_spec: Optional["DataSeriesSpec"] = None   # for Options_Pair
    rolling: bool = False              # Rolling vs Static strike
```

Resolves to a Nautilus `BarType` at strategy start. For spot FX only the first
three fields are used.

#### 3.11.4 Expression representation

Pick **one** of two representations:

1. **Tree of dataclasses** (recommended): `Condition`, `ConditionGroup`, `Value` as nested dataclasses. Easy to serialize to JSON for storage. Easy to walk for evaluation.
2. **String DSL + parser**: BTSoftware's UI strings (e.g., `Close > (SMA(50) + 50)`) can round-trip to tree form. Good for the UI later, not needed for core.

Start with (1). The evaluator is a ~100 LOC recursive visitor.

### 3.12 Strike / price-level selection (PDF: Default Portfolio Settings #5)

Options strike modes: `Normal` (absolute), `Relative` (ATM ± N), `Both`. Plus the *Premium / Greek* leg toggle with `Value Type` (Premium, NearestPremium, Delta, IV, Theta, Gamma, Vega), `Between` range or `Nearest` target, `MaxDepth`, `Condition`, `Side`.

**FX mapping** (keep the framework, change the axes):

| BT option | FX equivalent |
|---|---|
| Strike `Normal` (absolute) | Absolute price level |
| Strike `Relative` (ATM ± N steps) | `current_mid ± N × pip_size` or `current_mid ± N × ATR` |
| Strike `Both` | Absolute or relative per-leg in same portfolio |
| Premium (value between X and Y) | Signal value between X and Y (e.g., RSI between 25 and 35) |
| Delta / Theta / IV / Vega / Gamma nearest | Drop for spot; keep for FX options phase |
| NearestPremium with `Condition AboveEqual / BelowEqual` | Nearest-price-with-constraint — useful in ladder/grid FX strategies |
| MaxDepth + Side (ITM/OTM/BOTH) | Depth of grid scan + direction filter |

Implementation: a `LegSelector` interface, one subclass per mode. Only the absolute/relative ones are needed for MVP.

### 3.13 Strategy / User risk guardrails (PDF: Target Settings on Strategy / User Level)

- `max_profit_abs` (rupees → **USD baseline**)
- `max_loss_abs` (USD baseline)
- `profit_locking`: `(trigger, floor)` pair — cumulative realized + unrealized, USD
- `profit_trailing`: `(step, trail_by)` pair, USD
- `sqoff_time`: daily cutoff
- On hit → fire SqOff of *all* children and block further entries for the session

All monetary thresholds are **USD** (resolved 2026-04-24); the monitor computes MTM by summing per-position P&L converted through Nautilus FX rates. Account-ccy values stay available for display.

Implement as a lightweight `RiskMonitor` component subscribed to `Portfolio.account_state` via Nautilus `MessageBus`. Runs every N seconds. **Do not** stuff this into individual strategies.

### 3.14 Positional / BTST (PDF: How to Perform Positional)

Flips the portfolio from intraday to multi-day holding. Key changes:

- `Positional` flag → no auto SqOff at session close
- `start_day` / `sqoff_day` anchored to expiry day number (0 = expiry, 28 = 28 days before)
- `Leg Rollover` on expiry: `None / SameStrike / ReValidate` — what happens to each leg when its expiry hits
- Start-day holiday handling (bump forward, or skip entry)

**FX**: no expiry, so `start_day`/`sqoff_day` collapse to calendar day offsets. `LegRollover` is mostly irrelevant for spot FX (keep concept only for FX futures/NDF). Positional monitoring windows (`PositionalPortfolioTargetMonitoringTimes`) map cleanly to active-hours filters for 24×5 FX.

### 3.15 CSV / Excel signal import (PDF: CSV Import)

"Bring your own entry signal" — columns `TradeNo, Type∈{EntryLong, ExitLong, EntryShort, ExitShort}, Date, Time`. Matches the same `TradeNo` for entry/exit.

**Very relevant for FX.** Nearly all external signal providers (TradingView, custom Python research, NinjaTrader exports) emit this shape.

Implementation: a `CsvSignalReplay` strategy that reads the file on `on_start`, schedules `Clock.set_time_alert_ns` per row, and fires `submit_order` at wall-clock time. No new primitives needed.

### 3.16 Freak Protect, Leg SL at Broker, Implied Futures, etc (minor)

Keep Freak Protect (see 3.8). Drop `LegSLAtBroker` for now (Nautilus already manages stop orders at broker; adding BTSoftware's 13-rule workflow is a rabbit hole). Drop `ImpliedFutures` entirely (spot FX has no synthetic futures).

---

## 4. Proposed Architecture (Nautilus-native)

```
┌───────────────────────────────────────────────────────────┐
│                        Web / API layer                    │
│      (existing server.py — becomes dumb gateway)          │
└───────────────┬───────────────────────────────┬───────────┘
                │                               │
        ┌───────▼─────────┐              ┌──────▼──────┐
        │  PortfolioSpec  │              │  Reports    │
        │   (dataclasses) │              │             │
        └───────┬─────────┘              └──────▲──────┘
                │                               │
        ┌───────▼───────────────────────────────┴──────┐
        │             BacktestOrchestrator             │
        │   - compiles Portfolio → Nautilus configs    │
        │   - resolves data series, instruments        │
        │   - binds risk monitors                      │
        └───────────────────────┬──────────────────────┘
                                │
                    ┌───────────▼───────────┐
                    │  BacktestEngine/Node  │   ← Nautilus
                    │  (batch or streaming) │
                    └───────────┬───────────┘
                                │
    ┌─────────────┬─────────────┼─────────────┬─────────────┐
    │             │             │             │             │
┌───▼───┐  ┌──────▼──────┐  ┌───▼────┐  ┌─────▼────┐  ┌────▼────┐
│ Data  │  │ Indicators  │  │ Strats │  │  Risk    │  │ Exec    │
│Client │  │ (Nautilus + │  │ (ours) │  │ Monitors │  │ Algos   │
│       │  │  ~700 LOC   │  │        │  │          │  │ (slice) │
│       │  │  custom)    │  │        │  │          │  │         │
└───────┘  └─────────────┘  └────────┘  └──────────┘  └─────────┘
```

All strategies inherit from one `ManagedFxPortfolioStrategy` that owns:

- an `EntryRule` (one per execution mode)
- a `LegManager` (per-leg config, W&T state, SL/TGT state, trailing)
- an `ActionExecutor` (for the 6-primitive action dispatch)
- an `ExitMonitor` (Realtime / MinuteClose / Interval)
- a `ConditionEvaluator` (walks the expression tree)

The existing `ManagedStrategy` in `core/managed_strategy.py` is the seed —
evolve it, don't replace it.

---

## 5. File / module plan (proposed; not yet implemented)

```
core/
  models.py                  — already has ExitConfig, PortfolioConfig. Extend, don't rewrite.
  data_series.py             — NEW. DataSeriesSpec + resolver → BarType.
  entry_rules.py             — NEW. EntryRule abstract + one subclass per mode.
  leg_manager.py             — NEW. Per-leg state machine (W&T, SL, TGT, trailing).
  action_executor.py         — NEW. Dispatches the 6-primitive actions.
  conditions.py              — NEW. Condition/ConditionGroup/Value + evaluator.
  indicators_custom.py       — NEW. SuperTrend, Fisher, Pivots, SRZones, CandlePatterns, ...
  freak_protect.py           — NEW. Tick/bar spike filter.
  risk_monitor.py            — NEW. User/StrategyTag level guardrails.
  managed_strategy.py        — EVOLVE. Composes the above.
  backtest_runner.py         — EVOLVE. Wires orchestrator.
```

---

## 6. Implementation phases (suggested order)

**Phase 0 — this doc** ✓

**Phase 1 — foundation (rest of current Q)**
- `DataSeriesSpec` + resolver
- `EntryRule` abstract + `StartTime`, `UnderlyingLevel`, `ORB (UnderlyingRangeBreakout)` subclasses
- Extend `ExitConfig` with `monitor_mode` (Realtime/MinuteClose/Interval), anchored to session open
- Wire Nautilus built-in indicators into a uniform `IndicatorAdapter`
- Port 3 custom indicators: SuperTrend, Pivots+CPR, CandlePatterns

**Phase 2 — multi-leg orchestration**
- `LegManager` with Wait&Trade, per-leg SL/TGT/Trailing, Idle legs
- `ActionExecutor` with the 6 primitives (CLOSE, OPEN, DUPLICATE, SET_PRICE_ANCHOR, CHAIN_FLAG, SCHEDULE)
- `CombinedPremium`, `CombinedPremiumCrossover`, `RollingATRChannelBreakout` entry modes
- ReExecute + ReEntry with all safety-second guards
- `LegRangeBreakout` + `StrategyRangeBreakout` per-leg variants

**Phase 3 — Condition Builder**
- Tree of `Condition` / `ConditionGroup` / `Value` + JSON round-trip
- Recursive evaluator with `Offset` and `Composite Values` support
- Crossover operator (prev-bar value cache)
- Points-weighted mode (optional flag)
- Wire as a generic `SignalDrivenStrategy` that consumes a compiled tree

**Phase 4 — production grade**
- Freak Protect as `MessageBus` middleware
- Risk Monitor (account + strategy tag levels)
- Order Slicing via custom `ExecAlgorithm` extending Nautilus TWAP
- Positional/BTST calendar logic
- CSV Signal Replay strategy

**Phase 5 — FX options (optional, much later)**
- Greeks via py_vollib / QuantLib
- `option_ce` / `option_pe` / `option_pair` data series types
- Nearest-delta / Nearest-premium leg selection
- Implied-future ATM resolver (if an FX vol surface is available)

---

## 7. Things to keep an eye on

- **Negative premium handling** (PDF repeats this warning ~10 times): for credit spreads / short straddles, combined premium is negative. Every target/SL/trailing/W&T config needs explicit sign semantics. Cover with unit tests before UI work.
- **Safety seconds** (`LegReExecutionSafetySeconds=3`, `PortfolioReExecutionSafetySeconds=5`): cheap loop guards that BTSoftware obviously earned in production. Ship with these defaults.
- **`KeepLegRunning` + `ReExecuteLeg`** → classic pyramiding. Cover with a test case.
- **MinuteClose alignment to session open, not candle-start-time**: miss this and every multi-timeframe condition is subtly wrong. For FX we anchor globally at **Sun 22:00 UTC** (see §3.4 and §9). Encode as a single module constant; any future per-pair override layers on top as an `active_hours` filter, not a re-anchor.
- **Run On Days** vs **Positional Start Day**: for positional portfolios `RunOnDays` applies *only* to start day; don't accidentally re-apply on holding days.
- **Idle legs** + relative strike: strike resolves at *execution* time, not *creation* time. Matches naturally for FX (price resolves at execute time anyway).
- **Parallel processing in BTSoftware can alter backtest accuracy** for certain orderings — Nautilus's deterministic single-thread event loop removes this class of bug entirely. This is a real win we should advertise.
- **Freeze-quantity auto-slicing**: BTSoftware auto-slices at broker freeze limits with a 0.1s gap. FX doesn't have freeze limits in the same way, but venues have max order sizes — make this a per-venue config, not a global.

---

## 8. Out of scope (explicit non-goals)

- Matching BTSoftware's exact UI grid / drag-drop behavior
- Supporting Indian F&O instruments directly (NIFTY/BANKNIFTY options) — different adapter, different problem
- Stoxxo broker compatibility / portfolio import-export
- Replicating BTSoftware's Cache Path / file-based persistence
- Multi-user role (`Admin / Editor / User`) permissions — punt to Phase 5
- `At Broker` SL orders (Nautilus handles broker-side stops its own way)

---

## 9. Design decisions

**All resolved 2026-04-24:**

| # | Question | Decision | Implications |
|---|---|---|---|
| 1 | Session anchor | **Global Sun 22:00 UTC** (MVP default; revisit if multi-session realism becomes a requirement) | One `SESSION_ANCHOR_UTC = time(22, 0)` constant. All MinuteClose grids, daily windows, ORB ranges, and "day open" references anchor here for every pair. Per-pair liquid-hours becomes a separate `active_hours` filter later if needed. See §3.4. |
| 2 | Multi-leg representation | **Internal legs** — each `StrategySlotConfig` owns `legs: list[LegConfig]` | Action hooks resolve by local index; no cross-slot `group_id` plumbing. See §3.1. |
| 3 | Reference instrument | **Data-only subscription** | Orchestrator loads reference catalog separately; no tradable account or venue for it. See §3.2. |
| 4 | Combined MTM currency | **USD baseline** | All combined-MTM targets / risk monitors compute in USD via Nautilus FX rates; account ccy used for display only. See §3.3 and §3.13. |
| 5 | Freak Protect default | **`Smart`** | `Off` / `Strong` remain selectable per portfolio. See §3.8. |

Nothing outstanding — Phase 1 is unblocked.

---

## 10. Appendix — hotspot pages in the source PDF

| Topic | PDF pages (approx) |
|---|---|
| Hierarchy | 4–5 |
| Portfolio default settings + Freak Protect | 13–17 |
| Leg settings + Wait&Trade + Target/SL types | 21–31 |
| On Target / On SL action vocabulary | 27–31 |
| Execution parameters (modes, timing) | 32–38 |
| Range breakouts (all three) | 39–44 |
| Extra Conditions + Combined W&T | 45–49 |
| Other Settings (Straddle-width multiplier, Premium comparison, Order slicing) | 52–58 |
| ReExecute Settings (all safety-seconds knobs) | 58–64 |
| Monitoring (Realtime / MinuteClose / Interval) | 65–67 |
| Dynamic Hedge | 67–70 |
| Combined Target + SL settings | 70–76 |
| Exit Settings | 77–78 |
| Strategy / User level risk | 83–86 |
| Positional / BTST | 87–92 |
| CSV / Excel import | 93–98 |
| Conditional Builder (Data Series + Indicators + Conditions) | 99–167 |

Use these as jump-points when cross-referencing during implementation.
