"""
Page 2: View Data

Display downloaded crypto data as interactive tables and candlestick charts.
"""

import sys
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


st.set_page_config(page_title="View Data", page_icon="📊", layout="wide")
st.title("📊 View Data")
st.markdown("Explore your downloaded crypto data with tables and interactive charts.")

# --- Catalog Path ---
if "catalog_path" not in st.session_state:
    st.session_state.catalog_path = str(Path(__file__).resolve().parent.parent / "catalog")

catalog_path = st.session_state.catalog_path

if not Path(catalog_path).exists():
    st.warning("No catalog found. Go to **Download Data** first.")
    st.stop()

# --- Load Catalog ---
try:
    from core.nautilus_loader import load_catalog

    catalog = load_catalog(catalog_path)
    data_types = catalog.list_data_types()
except Exception as e:
    st.error(f"Failed to load catalog: {e}")
    st.stop()

if not data_types:
    st.info("Catalog is empty. Go to **Download Data** to fetch some crypto data first.")
    st.stop()

# --- Get available bar types ---
try:
    all_bars = catalog.bars()
    if not all_bars:
        st.info("No bar data found in catalog. Download some data first.")
        st.stop()

    # Extract unique bar types
    bar_types = list({str(bar.bar_type) for bar in all_bars})
    bar_types.sort()
except Exception as e:
    st.error(f"Failed to read bars: {e}")
    st.stop()

# --- Symbol Selection ---
selected_bar_type = st.selectbox("Select Instrument / Bar Type", bar_types)

# --- Load bars for selected type ---
try:
    bars = catalog.bars(bar_types=[selected_bar_type])
except Exception as e:
    st.error(f"Failed to load bars for {selected_bar_type}: {e}")
    st.stop()

if not bars:
    st.warning(f"No bars found for {selected_bar_type}")
    st.stop()

# Convert bars to DataFrame
data = []
for bar in bars:
    data.append({
        "timestamp": pd.Timestamp(bar.ts_event, unit="ns", tz="UTC"),
        "open": float(bar.open),
        "high": float(bar.high),
        "low": float(bar.low),
        "close": float(bar.close),
        "volume": float(bar.volume),
    })

df = pd.DataFrame(data)
df = df.set_index("timestamp").sort_index()

# --- Date Filter ---
st.subheader("Date Filter")
col1, col2 = st.columns(2)
with col1:
    filter_start = st.date_input(
        "From",
        value=df.index[0].date(),
        min_value=df.index[0].date(),
        max_value=df.index[-1].date(),
    )
with col2:
    filter_end = st.date_input(
        "To",
        value=df.index[-1].date(),
        min_value=df.index[0].date(),
        max_value=df.index[-1].date(),
    )

# Apply filter
mask = (df.index.date >= filter_start) & (df.index.date <= filter_end)
df_filtered = df[mask]

# --- Summary Stats ---
st.subheader("Summary Statistics")
col_a, col_b, col_c, col_d, col_e = st.columns(5)
col_a.metric("Total Bars", len(df_filtered))
col_b.metric("Min Price", f"${df_filtered['low'].min():,.2f}")
col_c.metric("Max Price", f"${df_filtered['high'].max():,.2f}")
col_d.metric("Avg Close", f"${df_filtered['close'].mean():,.2f}")
col_e.metric("Total Volume", f"{df_filtered['volume'].sum():,.0f}")

# --- Candlestick Chart ---
st.subheader("Candlestick Chart")

fig = go.Figure()

fig.add_trace(
    go.Candlestick(
        x=df_filtered.index,
        open=df_filtered["open"],
        high=df_filtered["high"],
        low=df_filtered["low"],
        close=df_filtered["close"],
        name="OHLC",
    )
)

# Volume as bar chart on secondary y-axis
fig.add_trace(
    go.Bar(
        x=df_filtered.index,
        y=df_filtered["volume"],
        name="Volume",
        marker_color="rgba(100, 150, 255, 0.3)",
        yaxis="y2",
    )
)

fig.update_layout(
    title=selected_bar_type,
    yaxis=dict(title="Price (USD)", side="left"),
    yaxis2=dict(title="Volume", side="right", overlaying="y", showgrid=False),
    xaxis=dict(title="Date", rangeslider=dict(visible=False)),
    template="plotly_dark",
    height=600,
    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
)

st.plotly_chart(fig, use_container_width=True)

# --- Data Table ---
st.subheader("Data Table")
st.dataframe(
    df_filtered.style.format({
        "open": "${:,.2f}",
        "high": "${:,.2f}",
        "low": "${:,.2f}",
        "close": "${:,.2f}",
        "volume": "{:,.0f}",
    }),
    use_container_width=True,
    height=400,
)

# --- Price Change Analysis ---
st.subheader("Price Change Analysis")
df_filtered_copy = df_filtered.copy()
df_filtered_copy["daily_return"] = df_filtered_copy["close"].pct_change() * 100
df_filtered_copy["cumulative_return"] = ((1 + df_filtered_copy["close"].pct_change()).cumprod() - 1) * 100

col1, col2 = st.columns(2)

with col1:
    fig_returns = go.Figure()
    fig_returns.add_trace(
        go.Histogram(x=df_filtered_copy["daily_return"].dropna(), nbinsx=50, name="Daily Returns")
    )
    fig_returns.update_layout(
        title="Daily Return Distribution (%)",
        xaxis_title="Daily Return (%)",
        yaxis_title="Frequency",
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(fig_returns, use_container_width=True)

with col2:
    fig_cum = go.Figure()
    fig_cum.add_trace(
        go.Scatter(
            x=df_filtered_copy.index,
            y=df_filtered_copy["cumulative_return"],
            mode="lines",
            name="Cumulative Return",
            fill="tozeroy",
        )
    )
    fig_cum.update_layout(
        title="Cumulative Return (%)",
        xaxis_title="Date",
        yaxis_title="Return (%)",
        template="plotly_dark",
        height=400,
    )
    st.plotly_chart(fig_cum, use_container_width=True)
