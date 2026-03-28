# NautilusTrader Crypto Data Dashboard

A simple web UI for downloading crypto data, viewing charts, running backtests,
and analyzing performance — all without writing any code.

## Quick Start

### 1. Install Dependencies

```bash
# From the project root (d:\nautilus_trader)
pip install -r crypto_data_app/requirements.txt

# Make sure NautilusTrader is installed
pip install -e nautilus_trader/
```

### 2. Run the App

```bash
cd crypto_data_app
streamlit run app.py
```

The app will open in your browser at `http://localhost:8501`.

## How to Use

### Step 1: Download Data
- Click any preset button (BTC/USD, ETH/USD, etc.) or type a custom symbol
- Choose a date range
- Click "Download Data"
- Data is saved as Parquet files in the catalog folder

### Step 2: View Data
- Select a downloaded symbol from the dropdown
- See candlestick charts and OHLCV data tables
- View daily return distribution and cumulative returns

### Step 3: Run Backtest
- Select a symbol and trading strategy:
  - **EMA Cross**: Fast/slow moving average crossover
  - **RSI Mean Reversion**: Buy oversold, sell overbought
  - **Bollinger Bands**: Trade at band boundaries
- Adjust strategy parameters using sliders
- Click "Run Backtest" to see results

### Step 4: Performance Tearsheet
- View equity curve and drawdown chart
- See win/loss distribution
- Analyze trade-by-trade P&L

## How It Works (Under the Hood)

```
Yahoo Finance API
    |
    v
[yfinance download] --> pandas DataFrame (OHLCV)
    |
    v
[instrument_factory] --> CurrencyPair instrument (NautilusTrader type)
    |
    v
[BarDataWrangler] --> list[Bar] (NautilusTrader native Bar objects)
    |
    v
[ParquetDataCatalog] --> Parquet files on disk
    |
    v
[BacktestEngine] --> Runs strategy, generates trades
    |
    v
[Results] --> P&L, equity curve, trade stats
```

## File Structure

```
crypto_data_app/
├── app.py                     # Main entry point
├── pages/
│   ├── 1_download.py          # Download crypto data
│   ├── 2_view_data.py         # View charts & tables
│   ├── 3_backtest.py          # Run backtests
│   └── 4_tearsheet.py         # Performance analytics
├── core/
│   ├── data_fetcher.py        # Yahoo Finance downloader
│   ├── instrument_factory.py  # Create NautilusTrader instruments
│   ├── nautilus_loader.py     # Data wrangling & catalog storage
│   ├── backtest_runner.py     # BacktestEngine wrapper
│   └── strategies.py          # Trading strategy registry
├── requirements.txt
└── README_APP.md
```

## Supported Symbols

Any crypto pair available on Yahoo Finance, including:
- BTC/USD, ETH/USD, SOL/USD, XRP/USD
- DOGE/USD, ADA/USD, AVAX/USD, LINK/USD
- Or type any custom pair

## Data Storage

Downloaded data is stored in the **catalog** folder (configurable in the sidebar):
```
catalog/
├── CurrencyPair/...          # Instrument definitions
└── Bar/YAHOO/BTCUSD-1-DAY-LAST-EXTERNAL/data.parquet  # OHLCV bars
```

This is NautilusTrader's native ParquetDataCatalog format and can be
used directly by any NautilusTrader backtest script.
