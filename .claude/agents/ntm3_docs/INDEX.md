# ntm3 concept index

This file maps each **NautilusTrader concept** to the PDF that documents it.
`ntm3` reads this file first to choose which PDF(s) to open for a question.

> **This is a fill-in template.** Drop your 30 PDFs into this folder
> (`.claude/agents/ntm3_docs/`) and edit the table so each row's filename matches
> a real file. The table below mirrors the live `/concepts` route one-to-one;
> add/remove rows if the upstream docs change.

## Naming convention

- One PDF = one concept.
- Lowercase filename, words joined by `_`, `.pdf` extension.
- Name it after the `nautilustrader.io/docs/latest/concepts/<slug>` URL slug
  (e.g. the "Order Book" page at `/concepts/order_book/` → `order_book.pdf`).

## Concept → file

| # | Concept | PDF filename | Summary |
|---|---------|--------------|---------|
| 1 | Overview | `overview.pdf` | What NautilusTrader is, who it's for, top-level feature tour. |
| 2 | Architecture | `architecture.pdf` | System layers, the messaging core, and how components fit together. |
| 3 | Actors | `actors.pdf` | The `Actor` base component for custom, non-trading event-driven logic. |
| 4 | Strategies | `strategies.pdf` | The `Strategy` class: lifecycle, handlers, order/position management. |
| 5 | Instruments | `instruments.pdf` | Instrument definitions, precision, lot/tick sizes, instrument IDs. |
| 6 | Continuous Futures | `continuous_futures.pdf` | Rolling/continuous futures contract handling. |
| 7 | Synthetics | `synthetics.pdf` | Synthetic instruments derived from formulas over other instruments. |
| 8 | Value Types | `value_types.pdf` | Core value objects — `Price`, `Quantity`, `Money`, `Currency`. |
| 9 | Data | `data.pdf` | Built-in market data types (ticks, bars, quotes) and the data flow. |
| 10 | Events | `events.pdf` | The event model — order/position/account events on the message bus. |
| 11 | Options | `options.pdf` | Options instruments and options-specific modelling. |
| 12 | Greeks | `greeks.pdf` | Options analytics: greeks calculation and risk sensitivities. |
| 13 | Custom Data | `custom_data.pdf` | Defining and routing user-defined custom data types. |
| 14 | Order Book | `order_book.pdf` | L1/L2/L3 order book construction, deltas, and snapshots. |
| 15 | Execution | `execution.pdf` | Execution engine, order routing, fills, and the exec algorithm path. |
| 16 | Orders | `orders.pdf` | Order types, time-in-force, contingent/OCO/OTO order relationships. |
| 17 | Positions | `positions.pdf` | Position lifecycle, netting vs hedging, realized/unrealized PnL. |
| 18 | Cache | `cache.pdf` | The in-memory `Cache` for instruments, orders, positions, and data. |
| 19 | Message Bus | `message_bus.pdf` | Pub/sub + request/response messaging backbone connecting components. |
| 20 | Accounting | `accounting.pdf` | Account types, balances, margin calculation, base-currency handling. |
| 21 | Portfolio | `portfolio.pdf` | The `Portfolio` aggregate: net exposures, PnL, and balances across venues. |
| 22 | Reports | `reports.pdf` | Generating execution/position/account reports from results. |
| 23 | Logging | `logging.pdf` | The logging subsystem, log levels, and writing to file/stdout. |
| 24 | Backtesting | `backtesting.pdf` | Backtest concepts, `BacktestEngine`/`BacktestNode`, and run setup. |
| 25 | Visualization | `visualization.pdf` | Charting and visualizing backtest results. |
| 26 | Configuration | `configuration.pdf` | Config objects/patterns for engines, nodes, and components. |
| 27 | Live Trading | `live.pdf` | Live trading nodes, real-time data/exec clients, and going to production. |
| 28 | Adapters | `adapters.pdf` | Integration adapters for venues/data providers (data + exec clients). |
| 29 | Rust | `rust.pdf` | The Rust core, the Python/Rust boundary, and performance internals. |
| 30 | Deterministic Simulation Testing (DST) | `dst.pdf` | Deterministic simulation testing for reproducible reliability checks. |

_The table mirrors the live `nautilustrader.io/docs/latest/concepts/` route.
Save each concepts page as `<slug>.pdf` to match its row above._