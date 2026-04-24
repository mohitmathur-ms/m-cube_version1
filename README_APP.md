# M_Cube Crypto Dashboard

A web-based crypto data dashboard for loading data, viewing charts, running backtests,
and analyzing performance — built with Flask + HTML/CSS/JS.

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the App

```bash
python server.py
```

The app will open in your browser at `http://localhost:5000`.

## How to Use

### Step 1: Load Data
- Enter the path to your CSV folder
- Use preset buttons (BTC, ETH, SOL, etc.) or select symbols manually
- Click "Load Selected" to import into the NautilusTrader catalog

### Step 2: View Data
- Select a loaded symbol from the dropdown
- See candlestick charts and OHLCV data tables
- View daily return distribution and cumulative returns

### Step 3: Run Backtest
- Select a symbol and trading strategy:
  - **EMA Cross**: Fast/slow moving average crossover
  - **RSI Mean Reversion**: Buy oversold, sell overbought
  - **Bollinger Bands**: Trade at band boundaries
  - Or upload a **custom strategy** (.py file)
- Adjust strategy parameters
- Click "Run Backtest" to see results

### Step 4: Performance Tearsheet
- View equity curve and drawdown chart
- See win/loss distribution
- Analyze trade-by-trade P&L

## File Structure

```
mcube_html_11_April_2026/
├── server.py                  # Flask backend (REST API)
├── static/                    # Frontend (HTML/CSS/JS SPA)
│   ├── index.html
│   ├── css/style.css
│   └── js/
│       ├── app.js             # Router & utilities
│       ├── dashboard.js       # Home page
│       ├── load_data.js       # CSV loading UI
│       ├── view_data.js       # Chart viewing
│       ├── backtest.js        # Backtest runner UI
│       └── tearsheet.js       # Results tearsheet
├── core/                      # Business logic (shared)
│   ├── csv_loader.py
│   ├── nautilus_loader.py
│   ├── instrument_factory.py
│   ├── backtest_runner.py
│   ├── strategies.py
│   ├── custom_strategy_loader.py
│   └── report_generator.py
├── custom_strategies/         # User-uploaded strategies
├── catalog/                   # NautilusTrader data catalog
├── reports/                   # Auto-saved backtest reports
├── requirements.txt
└── README_APP.md
```

## Data Storage

Data is stored in the **catalog** folder using NautilusTrader's native ParquetDataCatalog format:
```
catalog/
├── data/
│   ├── bar/                   # OHLCV bar data (Parquet)
│   └── currency_pair/         # Instrument definitions
```

This catalog can be used directly by any NautilusTrader backtest script.
