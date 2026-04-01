"""
Page 4: Performance Tearsheet

Display detailed performance analytics from backtest runs,
including equity curve comparison, drawdown, trade distribution, and stats.
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


st.set_page_config(page_title="Tearsheet", page_icon="📈", layout="wide")
st.title("📈 Performance Tearsheet")
st.markdown("Detailed performance analytics from your backtest results.")

# --- Check for results ---
if "backtest_results" not in st.session_state:
    st.info("No backtest results found. Go to **Run Backtest** and run a strategy first.")
    st.stop()

all_results = st.session_state["backtest_results"]
config = st.session_state.get("backtest_config", {})

strategy_names = list(all_results.keys())

# --- Overlay Equity Curve (all strategies) ---
st.subheader(f"Equity Comparison | {config.get('bar_type', 'N/A')}")

COLORS = ["#00d4aa", "#ff6b6b", "#4ecdc4", "#ffe66d", "#a29bfe", "#fd79a8"]

try:
    fig_compare = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],
        subplot_titles=("Equity Curves", "Drawdown"),
    )

    has_equity_data = False

    for i, name in enumerate(strategy_names):
        results = all_results[name]
        positions_report = results.get("positions_report")
        if positions_report is None or positions_report.empty:
            continue

        pos_df = positions_report.copy()
        pnl_col = None
        for col_name in pos_df.columns:
            if "pnl" in str(col_name).lower() or "realized" in str(col_name).lower():
                pnl_col = col_name
                break

        if pnl_col is None:
            continue

        pnl_series = pd.to_numeric(pos_df[pnl_col], errors="coerce").fillna(0)
        cumulative_pnl = pnl_series.cumsum()
        equity = results["starting_capital"] + cumulative_pnl
        color = COLORS[i % len(COLORS)]

        fig_compare.add_trace(
            go.Scatter(
                y=equity.values,
                mode="lines",
                name=name,
                line=dict(color=color, width=2),
            ),
            row=1, col=1,
        )

        peak = equity.cummax()
        drawdown = (equity - peak) / peak * 100
        fig_compare.add_trace(
            go.Scatter(
                y=drawdown.values,
                mode="lines",
                name=f"{name} DD",
                line=dict(color=color, width=1.5, dash="dot"),
                showlegend=False,
            ),
            row=2, col=1,
        )
        has_equity_data = True

    if has_equity_data:
        fig_compare.update_layout(
            template="plotly_dark",
            height=600,
            showlegend=True,
            legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        )
        fig_compare.update_yaxes(title_text="Balance ($)", row=1, col=1)
        fig_compare.update_yaxes(title_text="Drawdown (%)", row=2, col=1)
        st.plotly_chart(fig_compare, use_container_width=True)
    else:
        st.info("No position data available for equity comparison.")

except Exception as e:
    st.warning(f"Could not build equity comparison: {e}")

# --- Per-Strategy Tearsheet ---
st.markdown("---")

selected_strategy = st.selectbox("Select strategy for detailed tearsheet", strategy_names)
results = all_results[selected_strategy]

st.subheader(f"{selected_strategy} — Detailed Tearsheet")

# --- Summary Cards ---
st.markdown("### Performance Summary")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Return", f"{results['total_return_pct']:+.2f}%")
col2.metric("Total P&L", f"${results['total_pnl']:+,.2f}")
col3.metric("Win Rate", f"{results['win_rate']:.1f}%")
col4.metric("Total Trades", results["total_trades"])

st.markdown("---")

# --- Equity & Drawdown for selected strategy ---
positions_report = results.get("positions_report")

if positions_report is not None and not positions_report.empty:
    st.markdown("### Equity & Drawdown Analysis")

    try:
        pos_df = positions_report.copy()

        pnl_col = None
        for col_name in pos_df.columns:
            if "pnl" in str(col_name).lower() or "realized" in str(col_name).lower():
                pnl_col = col_name
                break

        if pnl_col is not None:
            pnl_series = pd.to_numeric(pos_df[pnl_col], errors="coerce").fillna(0)
            cumulative_pnl = pnl_series.cumsum()
            equity = results["starting_capital"] + cumulative_pnl

            fig_equity = make_subplots(
                rows=2, cols=1,
                shared_xaxes=True,
                vertical_spacing=0.05,
                row_heights=[0.7, 0.3],
                subplot_titles=("Equity Curve", "Drawdown"),
            )

            fig_equity.add_trace(
                go.Scatter(
                    y=equity.values,
                    mode="lines",
                    name="Equity",
                    line=dict(color="#00d4aa", width=2),
                    fill="tozeroy",
                    fillcolor="rgba(0, 212, 170, 0.1)",
                ),
                row=1, col=1,
            )

            peak = equity.cummax()
            drawdown = (equity - peak) / peak * 100
            fig_equity.add_trace(
                go.Scatter(
                    y=drawdown.values,
                    mode="lines",
                    name="Drawdown",
                    line=dict(color="#ff4444", width=1.5),
                    fill="tozeroy",
                    fillcolor="rgba(255, 68, 68, 0.2)",
                ),
                row=2, col=1,
            )

            fig_equity.update_layout(
                template="plotly_dark",
                height=600,
                showlegend=True,
                legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
            )
            fig_equity.update_yaxes(title_text="Balance ($)", row=1, col=1)
            fig_equity.update_yaxes(title_text="Drawdown (%)", row=2, col=1)

            st.plotly_chart(fig_equity, use_container_width=True)

            max_dd = drawdown.min()
            st.metric("Max Drawdown", f"{max_dd:.2f}%")

    except Exception as e:
        st.warning(f"Could not build equity curve: {e}")

    # --- Trade Distribution ---
    st.markdown("### Trade Distribution")

    try:
        pos_df = positions_report.copy()

        pnl_col = None
        for col_name in pos_df.columns:
            if "pnl" in str(col_name).lower() or "realized" in str(col_name).lower():
                pnl_col = col_name
                break

        if pnl_col is not None:
            pnl_values = pd.to_numeric(pos_df[pnl_col], errors="coerce").dropna()

            col1, col2 = st.columns(2)

            with col1:
                fig_dist = go.Figure()
                colors = ["#00d4aa" if v >= 0 else "#ff4444" for v in pnl_values]
                fig_dist.add_trace(
                    go.Bar(
                        x=list(range(len(pnl_values))),
                        y=pnl_values.values,
                        marker_color=colors,
                        name="Trade P&L",
                    )
                )
                fig_dist.update_layout(
                    title="P&L per Trade",
                    xaxis_title="Trade #",
                    yaxis_title="P&L ($)",
                    template="plotly_dark",
                    height=400,
                )
                st.plotly_chart(fig_dist, use_container_width=True)

            with col2:
                fig_pie = go.Figure()
                fig_pie.add_trace(
                    go.Pie(
                        labels=["Wins", "Losses"],
                        values=[results["wins"], results["losses"]],
                        marker_colors=["#00d4aa", "#ff4444"],
                        hole=0.4,
                    )
                )
                fig_pie.update_layout(
                    title="Win/Loss Ratio",
                    template="plotly_dark",
                    height=400,
                )
                st.plotly_chart(fig_pie, use_container_width=True)

            if len(pnl_values) > 0:
                st.markdown("### Trade Statistics")
                stats_col1, stats_col2, stats_col3, stats_col4 = st.columns(4)
                stats_col1.metric("Avg Win", f"${pnl_values[pnl_values > 0].mean():,.2f}" if (pnl_values > 0).any() else "$0.00")
                stats_col2.metric("Avg Loss", f"${pnl_values[pnl_values < 0].mean():,.2f}" if (pnl_values < 0).any() else "$0.00")
                stats_col3.metric("Best Trade", f"${pnl_values.max():,.2f}")
                stats_col4.metric("Worst Trade", f"${pnl_values.min():,.2f}")

    except Exception as e:
        st.warning(f"Could not render trade distribution: {e}")

else:
    st.info("No position data available for detailed analysis. The strategy may not have generated any trades.")

# --- Raw Data ---
st.markdown("---")
with st.expander("View Raw Reports"):
    if results.get("positions_report") is not None and not results["positions_report"].empty:
        st.markdown("**Positions Report**")
        st.dataframe(results["positions_report"], use_container_width=True)

    if results.get("fills_report") is not None and not results["fills_report"].empty:
        st.markdown("**Order Fills Report**")
        st.dataframe(results["fills_report"], use_container_width=True)

    if results.get("account_report") is not None and not results["account_report"].empty:
        st.markdown("**Account Report**")
        st.dataframe(results["account_report"], use_container_width=True)
