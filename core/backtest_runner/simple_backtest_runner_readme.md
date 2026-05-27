# core/backtest_runner — Simple Explanation

# Big Picture

This whole package is the main backtesting engine for the trading system.

It:
- runs strategies,
- manages portfolio-level logic,
- handles stoploss/target rules,
- processes bars/candles,
- merges results,
- converts PnL currencies,
- and creates final reports.

Earlier, everything was inside one huge Python file.

Now it has been split into many smaller folders, where each folder handles one specific responsibility.

---

# Folder-by-Folder Explanation

---

## profiling/

### What it does
Measures how much time different phases take.

### In simple words
Used for:
- performance checking,
- debugging slow code,
- checking strategy configs.

### Example
- “How long did bar loading take?”
- “How long did execution take?”

---

## bar_types/

### What it does
Handles candle/bar type logic.

### In simple words
Figures out:
- BID vs ASK vs MID bars,
- which bars should be grouped together,
- how slots should share engines.

### Example
If strategy uses:
- EURUSD-1min-BID

then this module may also find:
- EURUSD-1min-ASK

for realistic execution and fills.

---

## bar_filters/

### What it does
Filters bars before strategy execution.

### In simple words
Used for:
- running only on certain weekdays,
- only trading during certain times,
- removing bars after some cutoff.

### Example
Trade only:
- Monday–Friday
- between 9:15 and 15:30

---

## path_b/

### What it does
Handles the “Path B” backtesting flow.

### In simple words
Another execution style using `BacktestNode`.

It:
- prepares configs,
- chunks data,
- controls execution mode.

### Example
Think of it as:
> “Alternative execution backend.”

---

## rbo/

### What it does
Handles Range Breakout (RBO) settings.

### In simple words
Manages:
- breakout timing,
- winter time adjustment,
- RBO validation.

### Example
If market session timings shift during winter time,
this module adjusts timestamps properly.

---

## other_settings/

### What it does
Handles miscellaneous strategy settings.

### In simple words
Stores:
- delay between legs,
- what to do after stoploss,
- what to do after target hit.

### Example
A strategy may pause entries for some time after a stoploss.

---

## portfolio_exit_config/

### What it does
Handles portfolio-level exit settings.

### In simple words
Controls:
- portfolio stoploss,
- portfolio target,
- move-SL-to-cost rules.

### Example
If total portfolio loss reaches:
- -$500

then close all positions.

---

## cross_portfolio/

### What it does
Handles communication between portfolios.

### In simple words
Acts like a small message/event system.

### Example
One portfolio can publish:
> “Stop trading now”

and another portfolio can receive that event.

---

## portfolio_clip/

### What it does
Applies portfolio stoploss/target clipping.

### In simple words
This is where:
- portfolio exits,
- trailing logic,
- square-offs,
- move-SL coordination

actually happen.

### Example
If portfolio profit reaches a target,
this module may lock profits or exit trades.

---

## equity_curves/

### What it does
Builds and merges equity curves.

### In simple words
Creates account balance graphs over time.

### Example
Shows:
- account growth,
- drawdown,
- merged portfolio equity.

---

## data_cache/

### What it does
Caches loaded market data.

### In simple words
Prevents reloading the same parquet files repeatedly.

### Example
Makes repeated backtests much faster.

---

## report_utils/

### What it does
Contains helper utilities for reports.

### In simple words
Reusable helper functions used during result generation.

### Example
Formatting timestamps or selecting report columns.

---

## results/

### What it does
Extracts final backtest results.

### In simple words
Calculates:
- realized PnL,
- unrealized PnL,
- currency conversion,
- final reports.

### Example
Converts profits from another currency into USD.

---

## exit_fill/

### What it does
Models realistic trade exits.

### In simple words
Handles:
- VWAP fills,
- bid/ask fills,
- stoploss fills,
- target fills.

### Example
A stoploss may execute at BID instead of MID for realism.

---

## single_backtest/

### What it does
Runs one strategy backtest.

### In simple words
Main entry point for:
- single strategy execution.

### Example
Run one strategy on EURUSD data.

---

## slot_execution/

### What it does
Executes strategy slots.

### In simple words
Runs:
- single slots,
- grouped slots,
- multiprocessing workers.

### Example
If portfolio has 10 strategies,
this module helps run them efficiently in parallel.

---

## portfolio_results/

### What it does
Combines all strategy results together.

### In simple words
Merges:
- trade PnL,
- equity curves,
- strategy breakdowns,
- portfolio reports.

### Example
Creates one final combined portfolio result.

---

## orchestration/

### What it does
Top-level controller.

### In simple words
Main “manager” of the whole portfolio backtest process.

It coordinates everything together.

### Example
Starts all strategy executions and combines outputs.

---

# Dependency Graph (Simple Meaning)

The dependency graph shows:
> “Which folders depend on which other folders.”

Important point:
- the structure was designed carefully to avoid circular imports.

That is why helper modules like:
- `equity_curves/`
- `report_utils/`

exist separately.

---

# Hardcoded Values (Simple Meaning)

This section lists values that are fixed in code.

Examples:
- account currency = USD
- leverage = 1x
- max workers = 32
- OMS mode
- account type

Some values can only be changed by editing source code.

Others are “UI-backed”, meaning:
- there is a UI field,
- but code still contains fallback defaults.

---

# Most Important Folders To Learn First

If you want to understand the system faster, focus on:

1. `single_backtest/`
→ starts execution

2. `slot_execution/`
→ runs strategies

3. `portfolio_results/`
→ combines results

4. `portfolio_clip/`
→ risk management

5. `results/`
→ generates final metrics

6. `exit_fill/`
→ realistic fills

7. `bar_filters/`
→ time/day filtering

These contain most of the important trading/backtesting logic.
