"""
NautilusTrader Crypto Data Dashboard

A Streamlit app for non-coders to load local crypto CSV data, view charts,
run backtests, and analyze performance — all without writing Python.

Run with: streamlit run app.py
"""

import sys
from pathlib import Path

import streamlit as st

# Add this directory to path so 'core' package imports work
sys.path.insert(0, str(Path(__file__).resolve().parent))

st.set_page_config(
    page_title="M_Cube Crypto Dashboard",
    page_icon="⚓",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --- Sidebar: Global Settings ---
st.sidebar.title("M_Cube")
st.sidebar.caption("Crypto Data Dashboard")
st.sidebar.divider()

# Catalog path setting (persisted in session state)
if "catalog_path" not in st.session_state:
    st.session_state.catalog_path = str(Path(__file__).resolve().parent / "catalog")

st.sidebar.text_input(
    "Catalog Storage Path",
    key="catalog_path",
    help="Folder where loaded data is stored as Parquet files",
)

st.sidebar.divider()
st.sidebar.markdown(
    """
    **How it works:**
    1. **Load** - Import crypto CSV data into catalog
    2. **View** - Explore data tables & charts
    3. **Backtest** - Run trading strategies
    4. **Tearsheet** - Analyze performance

    Data source: local CSV files from
    `crypto_clean_data/` folder.

    Storage: NautilusTrader ParquetDataCatalog.
    """
)

# --- Main Page ---
st.title("Welcome to M_Cube Crypto Dashboard")
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.subheader("1. Load Data")
    st.write("Import daily OHLCV data from your local crypto CSV files.")
    st.page_link("pages/1_load_data.py", label="Go to Load Data", icon="📂")

with col2:
    st.subheader("2. View Data")
    st.write("Explore loaded data with interactive tables and candlestick charts.")
    st.page_link("pages/2_view_data.py", label="Go to View Data", icon="📊")

with col3:
    st.subheader("3. Run Backtest")
    st.write("Test trading strategies (EMA Cross, RSI, Bollinger) on your data.")
    st.page_link("pages/3_backtest.py", label="Go to Backtest", icon="🧪")

with col4:
    st.subheader("4. Tearsheet")
    st.write("View detailed performance analytics, equity curves, and drawdown charts.")
    st.page_link("pages/4_tearsheet.py", label="Go to Tearsheet", icon="📈")

st.markdown("---")

# Show catalog status
catalog_path = st.session_state.catalog_path
if Path(catalog_path).exists():
    from core.nautilus_loader import load_catalog

    try:
        catalog = load_catalog(catalog_path)
        data_types = catalog.list_data_types()
        st.success(f"Catalog loaded from: `{catalog_path}`")
        if data_types:
            st.json(data_types)
        else:
            st.info("Catalog is empty. Go to **Load Data** to get started.")
    except Exception as e:
        st.warning(f"Could not read catalog: {e}")
else:
    st.info(f"No catalog found at `{catalog_path}`. Go to **Load Data** to create one.")
