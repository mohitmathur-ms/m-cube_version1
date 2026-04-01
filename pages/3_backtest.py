"""
Page 3: Run Backtest

Select multiple strategies, configure parameters per strategy, and run backtests on downloaded data.
"""

import sys
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.custom_strategy_loader import (
    validate_and_load_strategy,
    get_strategy_template,
    get_strategy_guidelines,
    get_merged_registry,
    sanitize_filename,
)


st.set_page_config(page_title="Run Backtest", page_icon="🧪", layout="wide")
st.title("🧪 Run Backtest")
st.markdown("Test trading strategies on your downloaded crypto data.")

# --- Catalog Path ---
if "catalog_path" not in st.session_state:
    st.session_state.catalog_path = str(Path(__file__).resolve().parent.parent / "catalog")

catalog_path = st.session_state.catalog_path
custom_strategies_dir = Path(__file__).resolve().parent.parent / "custom_strategies"
custom_strategies_dir.mkdir(exist_ok=True)

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

# --- Custom Strategies ---
st.subheader("Custom Strategies")

upload_col, template_col = st.columns(2)

with upload_col:
    uploaded_file = st.file_uploader("Upload a custom strategy (.py)", type=["py"], key="strategy_uploader")

with template_col:
    st.download_button(
        label="Download Strategy Template",
        data=get_strategy_template(),
        file_name="custom_strategy_template.py",
        mime="text/x-python",
    )

# Process uploaded file
if uploaded_file is not None:
    safe_name = sanitize_filename(uploaded_file.name)
    if not safe_name.endswith(".py"):
        safe_name += ".py"
    dest_path = custom_strategies_dir / safe_name

    file_existed = dest_path.exists()
    dest_path.write_bytes(uploaded_file.getvalue())

    try:
        entry = validate_and_load_strategy(dest_path)
        # Check for duplicate STRATEGY_NAME among other custom files
        import sys as _sys
        mod_name = f"custom_strategy_{dest_path.stem}"
        mod = _sys.modules.get(mod_name)
        strat_name = getattr(mod, "STRATEGY_NAME", dest_path.stem) if mod else dest_path.stem

        # Check other custom files for same STRATEGY_NAME
        duplicate = False
        for other_file in custom_strategies_dir.glob("*.py"):
            if other_file.name == safe_name or other_file.name.startswith("__"):
                continue
            try:
                other_entry = validate_and_load_strategy(other_file)
                other_mod_name = f"custom_strategy_{other_file.stem}"
                other_mod = _sys.modules.get(other_mod_name)
                other_name = getattr(other_mod, "STRATEGY_NAME", other_file.stem) if other_mod else other_file.stem
                if other_name == strat_name:
                    duplicate = True
                    break
            except Exception:
                continue

        if duplicate:
            dest_path.unlink()
            st.error(f"A custom strategy named '{strat_name}' already exists. Use a different STRATEGY_NAME.")
        else:
            if file_existed:
                st.warning(f"Replaced existing file: {safe_name}")
            st.success(f"Strategy '{strat_name}' loaded successfully!")
    except ValueError as e:
        dest_path.unlink(missing_ok=True)
        st.error(f"Invalid strategy file:\n\n{e}")
    except Exception as e:
        dest_path.unlink(missing_ok=True)
        st.error(f"Unexpected error: {e}")

# Guidelines expander
with st.expander("Strategy Guidelines & Requirements"):
    st.markdown(get_strategy_guidelines())

# Manage existing custom strategies
custom_files = sorted(f for f in custom_strategies_dir.glob("*.py") if not f.name.startswith("__"))
if custom_files:
    with st.expander(f"Manage Custom Strategies ({len(custom_files)} loaded)"):
        for cf in custom_files:
            col_name, col_btn = st.columns([4, 1])
            with col_name:
                st.text(cf.name)
            with col_btn:
                if st.button("Delete", key=f"delete_{cf.name}", type="secondary"):
                    cf.unlink()
                    st.rerun()

# --- Build merged registry ---
merged_registry, load_warnings = get_merged_registry(custom_strategies_dir)
for warn in load_warnings:
    st.warning(warn)

# --- Strategy Selection ---
st.subheader("Select Strategies")

registry_keys = list(merged_registry.keys())
selected_strategies = st.multiselect(
    "Choose one or more strategies to backtest",
    registry_keys,
    default=[registry_keys[0]] if registry_keys else [],
)

if not selected_strategies:
    st.info("Select at least one strategy to continue.")
    st.stop()

# --- Per-Strategy Configuration ---
st.subheader("Strategy Parameters")

strategy_configs = {}

for strategy_name in selected_strategies:
    with st.expander(f"**{strategy_name}**", expanded=True):
        st.caption(merged_registry[strategy_name]["description"])

        # Trade size for this strategy
        trade_size = st.number_input(
            "Trade Size (units)",
            value=1,
            min_value=1,
            step=1,
            key=f"trade_size_{strategy_name}",
        )

        # Strategy-specific parameters
        params = merged_registry[strategy_name]["params"]
        strategy_params = {}

        param_cols = st.columns(max(len(params), 1))
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
                registry=merged_registry,
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

        # Generate HTML report
        from core.report_generator import generate_report

        raw_name = f"{selected_bar_type.split('.')[0]}_{'_'.join(selected_strategies)}"
        backtest_name = sanitize_filename(raw_name)
        report_html = generate_report(all_results, backtest_name=backtest_name)
        st.session_state["backtest_report_html"] = report_html
        st.session_state["backtest_report_name"] = backtest_name

        # Auto-save to reports directory
        reports_dir = Path(__file__).resolve().parent.parent / "reports"
        reports_dir.mkdir(exist_ok=True)
        report_path = reports_dir / f"{backtest_name}_report.html"
        report_path.write_text(report_html, encoding="utf-8")

        # Auto-save CSV reports per strategy
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        for strat_name, strat_results in all_results.items():
            safe_name = strat_name.replace(" ", "_")

            # Position report
            if strat_results.get("positions_report") is not None and not strat_results["positions_report"].empty:
                strat_results["positions_report"].to_csv(
                    reports_dir / f"position_report_{safe_name}_{timestamp}.csv", index=False
                )

            # Order fills report
            if strat_results.get("fills_report") is not None and not strat_results["fills_report"].empty:
                strat_results["fills_report"].to_csv(
                    reports_dir / f"order_fill_report_{safe_name}_{timestamp}.csv", index=False
                )

            # Account report
            if strat_results.get("account_report") is not None and not strat_results["account_report"].empty:
                acct_csv = strat_results["account_report"].copy()
                for col in acct_csv.columns:
                    if acct_csv[col].apply(lambda x: isinstance(x, (dict, list))).any():
                        acct_csv[col] = acct_csv[col].astype(str)
                acct_csv.to_csv(
                    reports_dir / f"account_report_{safe_name}_{timestamp}.csv", index=False
                )

        st.success(f"CSV reports saved to `reports/` folder (timestamp: {timestamp})")

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

    # --- HTML Report Download ---
    if "backtest_report_html" in st.session_state:
        report_name = st.session_state.get("backtest_report_name", "backtest")
        st.download_button(
            label="📄 Download HTML Report",
            data=st.session_state["backtest_report_html"],
            file_name=f"{report_name}_report.html",
            mime="text/html",
        )

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
                acct_df = results["account_report"].copy()
                # Convert nested object columns (e.g. margins, info) to readable strings
                for col in acct_df.columns:
                    if acct_df[col].apply(lambda x: isinstance(x, (dict, list))).any():
                        acct_df[col] = acct_df[col].astype(str)
                st.dataframe(acct_df, use_container_width=True)
