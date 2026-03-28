"""
Page 1: Load Crypto Data from Local CSVs

Scans the local CSV folder, lets users select symbols,
and loads them into the NautilusTrader ParquetDataCatalog.
"""

import sys
from pathlib import Path

import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from core.csv_loader import DEFAULT_CSV_FOLDER, scan_csv_folder, get_display_label


st.set_page_config(page_title="Load Data", page_icon="📂", layout="wide")
st.title("📂 Load Crypto Data")
st.markdown("Load daily OHLCV data from your local CSV files into the NautilusTrader catalog.")

# --- Settings ---
if "catalog_path" not in st.session_state:
    st.session_state.catalog_path = str(Path(__file__).resolve().parent.parent / "catalog")
if "csv_folder" not in st.session_state:
    st.session_state.csv_folder = DEFAULT_CSV_FOLDER

catalog_path = st.session_state.catalog_path

st.sidebar.markdown(f"**Catalog:** `{catalog_path}`")

# --- CSV Folder ---
csv_folder = st.text_input(
    "CSV Data Folder",
    value=st.session_state.csv_folder,
    help="Path to the folder containing your crypto CSV files",
)
st.session_state.csv_folder = csv_folder

# --- Scan for available CSVs ---
csv_entries = scan_csv_folder(csv_folder)

if not csv_entries:
    st.warning(f"No CSV files found in `{csv_folder}`. Check the path.")
    st.stop()

st.success(f"Found **{len(csv_entries)}** CSV files in the folder.")

# --- Build selection list ---
# Group by symbol for easier browsing
symbols_seen = {}
for entry in csv_entries:
    sym = entry["symbol"]
    if sym not in symbols_seen:
        symbols_seen[sym] = []
    symbols_seen[sym].append(entry)

st.subheader("Select Symbols to Load")

# Quick preset buttons for major coins
st.caption("Quick select major coins:")
major_ids = {1: "BTC", 1027: "ETH", 5426: "SOL", 52: "XRP", 74: "DOGE", 2010: "ADA",
             5994: "SHIB", 2: "LTC", 3794: "ATOM", 4642: "HBAR", 512: "XLM"}

preset_cols = st.columns(6)
selected_entries = []

# Preset buttons
for i, (cmc_id, sym) in enumerate(list(major_ids.items())[:6]):
    col = preset_cols[i]
    # Find the entry matching this CMC id
    matching = [e for e in csv_entries if e["id"] == cmc_id]
    if matching:
        entry = matching[0]
        if col.button(f"{sym} - {entry['name']}", key=f"preset_{cmc_id}", use_container_width=True):
            if "selected_to_load" not in st.session_state:
                st.session_state.selected_to_load = []
            st.session_state.selected_to_load = [entry]
            st.session_state.trigger_load = True

preset_cols2 = st.columns(6)
for i, (cmc_id, sym) in enumerate(list(major_ids.items())[6:]):
    col = preset_cols2[i]
    matching = [e for e in csv_entries if e["id"] == cmc_id]
    if matching:
        entry = matching[0]
        if col.button(f"{sym} - {entry['name']}", key=f"preset_{cmc_id}", use_container_width=True):
            if "selected_to_load" not in st.session_state:
                st.session_state.selected_to_load = []
            st.session_state.selected_to_load = [entry]
            st.session_state.trigger_load = True

st.markdown("---")

# Full list with multiselect
all_labels = [get_display_label(e) for e in csv_entries]
label_to_entry = {get_display_label(e): e for e in csv_entries}

selected_labels = st.multiselect(
    "Or select from all available files:",
    all_labels,
    help="Pick one or more CSV files to load into the catalog",
)

load_btn = st.button("📥 Load Selected into Catalog", type="primary", use_container_width=True)

# --- Handle Loading ---
should_load = load_btn or st.session_state.get("trigger_load", False)

entries_to_load = []
if st.session_state.get("trigger_load"):
    entries_to_load = st.session_state.get("selected_to_load", [])
    st.session_state.trigger_load = False
elif load_btn and selected_labels:
    entries_to_load = [label_to_entry[lbl] for lbl in selected_labels]

if should_load and entries_to_load:
    from core.nautilus_loader import load_csv_and_store

    progress = st.progress(0)
    results = []

    for i, entry in enumerate(entries_to_load):
        with st.spinner(f"Loading {entry['symbol']} - {entry['name']}..."):
            try:
                result = load_csv_and_store(
                    csv_entry=entry,
                    catalog_path=catalog_path,
                )
                results.append(result)
                st.success(
                    f"Loaded **{result['num_bars']}** daily bars for "
                    f"**{result['symbol']}** ({result['name']})"
                )
            except Exception as e:
                st.error(f"Failed to load {entry['symbol']}: {e}")

        progress.progress((i + 1) / len(entries_to_load))

    if results:
        st.markdown("---")
        st.subheader("Load Summary")

        for result in results:
            with st.expander(f"{result['symbol']} - {result['name']} ({result['num_bars']} bars)"):
                df = result["dataframe"]
                col_a, col_b, col_c = st.columns(3)
                col_a.metric("Bars", result["num_bars"])
                col_b.metric("Date Range", f"{df.index[0].date()} → {df.index[-1].date()}")
                col_c.metric("Latest Close", f"${df['close'].iloc[-1]:,.2f}")

                st.dataframe(df.tail(10), use_container_width=True)

        st.session_state["last_load_results"] = results

elif should_load and not entries_to_load:
    st.warning("No symbols selected. Pick from the presets or use the multiselect above.")

st.markdown("---")

# --- Show Current Catalog Contents ---
st.subheader("Current Catalog Contents")

if Path(catalog_path).exists():
    try:
        from core.nautilus_loader import load_catalog

        catalog = load_catalog(catalog_path)
        data_types = catalog.list_data_types()
        if data_types:
            st.json(data_types)
        else:
            st.info("Catalog is empty. Load some data above to get started!")
    except Exception as e:
        st.warning(f"Could not read catalog: {e}")
else:
    st.info("No catalog found yet. Load data to create one.")
