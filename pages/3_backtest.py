"""
Page 3: Run Backtest

Select multiple strategies, configure parameters per strategy, and run backtests on downloaded data.
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

# --- Shared Configuration ---
st.subheader("Configuration")

col1, col2 = st.columns(2)

with col1:
    selected_bar_type = st.selectbox("Select Instrument / Bar Type", bar_types)

with col2:
    starting_capital = st.number_input("Starting Capital ($)", value=100_000.0, min_value=1000.0, step=10_000.0)

# --- Strategy Selection ---
st.subheader("Select Strategies")

selected_strategies = st.multiselect(
    "Choose one or more strategies to backtest",
    list(STRATEGY_REGISTRY.keys()),
    default=[list(STRATEGY_REGISTRY.keys())[0]],
)

if not selected_strategies:
    st.info("Select at least one strategy to continue.")
    st.stop()

# --- Per-Strategy Configuration ---
st.subheader("Strategy Parameters")

strategy_configs = {}

for strategy_name in selected_strategies:
    with st.expander(f"**{strategy_name}**", expanded=True):
        st.caption(STRATEGY_REGISTRY[strategy_name]["description"])

        # Trade size for this strategy
        trade_size = st.number_input(
            "Trade Size (units)",
            value=1,
            min_value=1,
            step=1,
            key=f"trade_size_{strategy_name}",
        )

        # Strategy-specific parameters
        params = STRATEGY_REGISTRY[strategy_name]["params"]
        strategy_params = {}

        param_cols = st.columns(len(params))
        for i, (param_key, param_info) in enumerate(params.items()):
            with param_cols[i]:
                if isinstance(param_info["default"], bool):
                    strategy_params[param_key] = st.checkbox(
                        param_info["label"],
                        value=param_info["default"],
                        key=f"param_{strategy_name}_{param_key}",
                    )
                elif isinstance(param_info["default"], float):
                    strategy_params[param_key] = st.number_input(
                        param_info["label"],
                        value=param_info["default"],
                        min_value=param_info["min"],
                        max_value=param_info["max"],
                        step=0.5,
                        key=f"param_{strategy_name}_{param_key}",
                    )
                else:
                    strategy_params[param_key] = st.number_input(
                        param_info["label"],
                        value=param_info["default"],
                        min_value=param_info["min"],
                        max_value=param_info["max"],
                        step=1,
                        key=f"param_{strategy_name}_{param_key}",
                    )

        strategy_configs[strategy_name] = {
            "params": strategy_params,
            "trade_size": trade_size,
        }

# --- Run Backtest ---
st.markdown("---")
run_btn = st.button("🚀 Run Backtest", type="primary", use_container_width=True)

if run_btn:
    from core.backtest_runner import run_backtest

    all_results = {}
    progress = st.progress(0, text="Starting backtests...")

    for idx, strategy_name in enumerate(selected_strategies):
        cfg = strategy_configs[strategy_name]
        progress.progress(
            idx / len(selected_strategies),
            text=f"Running {strategy_name} ({idx + 1}/{len(selected_strategies)})...",
        )

        try:
            results = run_backtest(
                catalog_path=catalog_path,
                bar_type_str=selected_bar_type,
                strategy_name=strategy_name,
                strategy_params=cfg["params"],
                trade_size=cfg["trade_size"],
                starting_capital=starting_capital,
            )
            all_results[strategy_name] = results

        except Exception as e:
            st.error(f"**{strategy_name}** backtest failed: {e}")
            import traceback
            st.code(traceback.format_exc())

    progress.progress(1.0, text="All backtests complete!")

    if all_results:
        st.session_state["backtest_results"] = all_results
        st.session_state["backtest_config"] = {
            "bar_type": selected_bar_type,
            "strategies": strategy_configs,
        }

# --- Display Results ---
if "backtest_results" in st.session_state:
    all_results = st.session_state["backtest_results"]
    config = st.session_state.get("backtest_config", {})

    st.markdown("---")
    st.subheader("Backtest Results")
    st.caption(f"Instrument: **{config.get('bar_type', 'N/A')}** | Strategies: **{', '.join(all_results.keys())}**")

    # --- Comparison Table ---
    comparison_data = []
    for name, results in all_results.items():
        comparison_data.append({
            "Strategy": name,
            "Starting Capital": f"${results['starting_capital']:,.2f}",
            "Final Balance": f"${results['final_balance']:,.2f}",
            "Total P&L": f"${results['total_pnl']:,.2f}",
            "Return %": f"{results['total_return_pct']:+.2f}%",
            "Trades": results["total_trades"],
            "Win Rate": f"{results['win_rate']:.1f}%",
            "Wins": results["wins"],
            "Losses": results["losses"],
        })

    st.dataframe(pd.DataFrame(comparison_data), use_container_width=True, hide_index=True)

    # --- Per-Strategy Detail ---
    for name, results in all_results.items():
        with st.expander(f"**{name}** — Detailed Reports"):
            # Summary metrics
            col_a, col_b, col_c, col_d, col_e, col_f = st.columns(6)
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
                st.markdown("**Positions Report**")
                st.dataframe(results["positions_report"], use_container_width=True)

            # Fills report
            if results.get("fills_report") is not None and not results["fills_report"].empty:
                st.markdown("**Order Fills Report**")
                st.dataframe(results["fills_report"], use_container_width=True, height=400)

            # Account report
            if results.get("account_report") is not None and not results["account_report"].empty:
                st.markdown("**Account Report**")
                st.dataframe(results["account_report"], use_container_width=True)
