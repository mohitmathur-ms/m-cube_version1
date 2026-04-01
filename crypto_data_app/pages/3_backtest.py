"""
Page 3: Run Backtest

Select a strategy, configure parameters, and run a backtest on downloaded data.
"""

import sys
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.strategies import STRATEGY_REGISTRY


st.set_page_config(page_title="Run Backtest", page_icon="🧪", layout="wide")
st.title("🧪 Run Backtest")
st.markdown("Test trading strategies on your downloaded crypto data.")

# --- Catalog Path ---
if "catalog_path" not in st.session_state:
    st.session_state.catalog_path = str(Path(__file__).resolve().parent.parent / "catalog")

catalog_path = st.session_state.catalog_path

if not Path(catalog_path).exists():
    st.warning("No catalog found. Go to **Download Data** first.")
    st.stop()

# --- Load available bar types ---
try:
    from core.nautilus_loader import load_catalog

    catalog = load_catalog(catalog_path)
    all_bars = catalog.bars()
    if not all_bars:
        st.info("No bar data in catalog. Download some data first.")
        st.stop()

    bar_types = sorted({str(bar.bar_type) for bar in all_bars})
except Exception as e:
    st.error(f"Failed to load catalog: {e}")
    st.stop()

# --- Configuration ---
st.subheader("Configuration")

col1, col2 = st.columns(2)

with col1:
    selected_bar_type = st.selectbox("Select Instrument / Bar Type", bar_types)
    strategy_name = st.selectbox("Select Strategy", list(STRATEGY_REGISTRY.keys()))

with col2:
    starting_capital = st.number_input("Starting Capital ($)", value=100_000.0, min_value=1000.0, step=10_000.0)
    trade_size = st.number_input("Trade Size (units)", value=1, min_value=1, step=1)

# --- Strategy Parameters ---
st.subheader("Strategy Parameters")
st.caption(STRATEGY_REGISTRY[strategy_name]["description"])

params = STRATEGY_REGISTRY[strategy_name]["params"]
strategy_params = {}

param_cols = st.columns(len(params))
for i, (param_key, param_info) in enumerate(params.items()):
    with param_cols[i]:
        if isinstance(param_info["default"], bool):
            strategy_params[param_key] = st.checkbox(
                param_info["label"],
                value=param_info["default"],
                key=f"param_{param_key}",
            )
        elif isinstance(param_info["default"], float):
            strategy_params[param_key] = st.number_input(
                param_info["label"],
                value=param_info["default"],
                min_value=param_info["min"],
                max_value=param_info["max"],
                step=0.5,
                key=f"param_{param_key}",
            )
        else:
            strategy_params[param_key] = st.number_input(
                param_info["label"],
                value=param_info["default"],
                min_value=param_info["min"],
                max_value=param_info["max"],
                step=1,
                key=f"param_{param_key}",
            )

# --- Run Backtest ---
st.markdown("---")
run_btn = st.button("🚀 Run Backtest", type="primary", use_container_width=True)

if run_btn:
    with st.spinner(f"Running {strategy_name} backtest on {selected_bar_type}..."):
        try:
            from core.backtest_runner import run_backtest

            results = run_backtest(
                catalog_path=catalog_path,
                bar_type_str=selected_bar_type,
                strategy_name=strategy_name,
                strategy_params=strategy_params,
                trade_size=trade_size,
                starting_capital=starting_capital,
            )

            # Store results in session
            st.session_state["backtest_results"] = results
            st.session_state["backtest_config"] = {
                "bar_type": selected_bar_type,
                "strategy": strategy_name,
                "params": strategy_params,
            }

        except Exception as e:
            st.error(f"Backtest failed: {e}")
            import traceback
            st.code(traceback.format_exc())

# --- Display Results ---
if "backtest_results" in st.session_state:
    results = st.session_state["backtest_results"]
    config = st.session_state.get("backtest_config", {})

    st.markdown("---")
    st.subheader("Backtest Results")
    st.caption(f"Strategy: **{config.get('strategy', 'N/A')}** | Instrument: **{config.get('bar_type', 'N/A')}**")

    # Summary metrics
    col_a, col_b, col_c, col_d, col_e, col_f = st.columns(6)

    pnl_color = "green" if results["total_pnl"] >= 0 else "red"

    col_a.metric("Starting Capital", f"${results['starting_capital']:,.2f}")
    col_b.metric("Final Balance", f"${results['final_balance']:,.2f}")
    col_c.metric(
        "Total P&L",
        f"${results['total_pnl']:,.2f}",
        delta=f"{results['total_return_pct']:+.2f}%",
    )
    col_d.metric("Total Trades", results["total_trades"])
    col_e.metric("Win Rate", f"{results['win_rate']:.1f}%")
    col_f.metric("Wins / Losses", f"{results['wins']} / {results['losses']}")

    # Positions report
    if results.get("positions_report") is not None and not results["positions_report"].empty:
        st.subheader("Positions Report")
        st.dataframe(results["positions_report"], use_container_width=True)

    # Fills report
    if results.get("fills_report") is not None and not results["fills_report"].empty:
        st.subheader("Order Fills Report")
        st.dataframe(results["fills_report"], use_container_width=True, height=400)

    # Account report
    if results.get("account_report") is not None and not results["account_report"].empty:
        st.subheader("Account Report")
        st.dataframe(results["account_report"], use_container_width=True)
