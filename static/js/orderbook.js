/**
 * Orderbook Page - Consolidated view of all trades from the last backtest or portfolio run.
 * Shows a numbered table matching the standard orderbook CSV schema with
 * select-backtest dropdown, refresh, and CSV export.
 */

const Orderbook = {
    _currentSource: "live",   // "live" or a filename from reports/
    _savedFiles: [],

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">Orderbook</h1>
            <p class="page-subtitle">Consolidated trade records from your last backtest or portfolio run.</p>

            <div class="card" style="padding: 16px; margin-bottom: 16px;">
                <div style="display: flex; align-items: center; gap: 12px; flex-wrap: wrap;">
                    <label style="font-weight: 600; font-size: 0.85rem;">Select Backtest:</label>
                    <select id="ob-source-select" class="form-select" style="min-width: 260px;"
                            onchange="Orderbook.onSourceChange()">
                        <option value="live">Current Results (Live)</option>
                    </select>
                    <button class="btn btn-sm" onclick="Orderbook.refresh()">Refresh</button>
                    <button class="btn btn-sm btn-primary" onclick="Orderbook.exportCSV()">Export as CSV</button>
                </div>
            </div>

            <div id="ob-table-area">
                <div class="alert alert-info">Run a backtest or portfolio to see the orderbook here.</div>
            </div>
        `;

        this._loadSavedFiles();
        this._renderTable();
    },

    onShow() {
        this._loadSavedFiles();
        if (this._currentSource === "live") {
            this._renderTable();
        }
    },

    /* ─── Data extraction ──────────────────────────────────────────────── */

    /** Collect all orderbook records from the current backtest results. */
    _collectFromBacktest() {
        const res = App.state.backtestResults;
        if (!res) return [];
        const rows = [];
        for (const instData of Object.values(res)) {
            const strategies = instData.strategies || {};
            for (const r of Object.values(strategies)) {
                if (r.order_book && r.order_book.length > 0) {
                    rows.push(...r.order_book);
                }
            }
        }
        return rows;
    },

    /** Collect orderbook from portfolio results (if present). */
    _collectFromPortfolio() {
        const r = App.state.portfolioResults;
        if (!r || !r.order_book) return [];
        return r.order_book;
    },

    /** Get the live orderbook rows from whichever result set is available. */
    _getLiveData() {
        // Prefer portfolio results if they exist and have orderbook data
        const pf = this._collectFromPortfolio();
        if (pf.length > 0) return pf;
        return this._collectFromBacktest();
    },

    /* ─── Rendering ────────────────────────────────────────────────────── */

    /** Visible columns in display order (exclude internal fields). */
    _displayColumns: [
        "USERID", "SYMBOL", "EXCHANGE", "TRANSACTION", "QUANTITY", "LOTS",
        "MULTIPLIER", "OrderID", "ENTRY TIME", "ENTRY PRICE", "ENTRY REASON",
        "OPTION TYPE", "PORTFOLIO NAME", "STRATEGY",
        "EXIT TIME", "AVG EXIT PRICE", "EXIT REASON", "PNL",
    ],

    _renderTable() {
        const area = document.getElementById("ob-table-area");
        if (!area) return;

        const data = this._currentSource === "live"
            ? this._getLiveData()
            : null;  // loaded async for saved files

        if (this._currentSource !== "live") return; // async load handles it

        if (!data || data.length === 0) {
            area.innerHTML = '<div class="alert alert-info">No orderbook data available. Run a backtest or portfolio first.</div>';
            return;
        }

        area.innerHTML = this._buildTableHTML(data);
    },

    _buildTableHTML(data) {
        const cols = this._displayColumns;
        let html = '<div class="table-container"><table>';
        html += '<thead><tr><th style="width: 36px;">#</th>';
        html += cols.map(c => `<th>${c}</th>`).join("");
        html += '</tr></thead><tbody>';

        for (let i = 0; i < data.length; i++) {
            const row = data[i];
            const pnl = parseFloat(row["PNL"]) || 0;
            const rowClass = pnl > 0 ? "ob-row-positive" : pnl < 0 ? "ob-row-negative" : "";
            html += `<tr class="${rowClass}">`;
            html += `<td style="color: var(--text-muted); font-size: 0.8rem;">${i + 1}</td>`;
            for (const c of cols) {
                let val = row[c] ?? "";
                if (c === "PNL" || c === "ENTRY PRICE" || c === "AVG EXIT PRICE") {
                    const num = parseFloat(val);
                    if (!isNaN(num)) {
                        const cls = c === "PNL" ? (num >= 0 ? "positive" : "negative") : "";
                        val = `<span class="${cls}">${num.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 6 })}</span>`;
                    }
                }
                html += `<td>${val}</td>`;
            }
            html += '</tr>';
        }

        html += '</tbody></table></div>';
        html += `<p class="section-caption" style="margin-top: 8px;">${data.length} trade(s)</p>`;
        return html;
    },

    /* ─── Saved files dropdown ─────────────────────────────────────────── */

    async _loadSavedFiles() {
        try {
            const resp = await App.api("/api/orderbook/list");
            this._savedFiles = resp.files || [];
        } catch {
            this._savedFiles = [];
        }
        this._populateDropdown();
    },

    _populateDropdown() {
        const sel = document.getElementById("ob-source-select");
        if (!sel) return;
        const prev = sel.value;
        let html = '<option value="live">Current Results (Live)</option>';
        for (const f of this._savedFiles) {
            const label = f.replace("order_book_", "").replace(".csv", "");
            html += `<option value="${f}">${label}</option>`;
        }
        sel.innerHTML = html;
        // Restore previous selection if still valid
        if ([...sel.options].some(o => o.value === prev)) {
            sel.value = prev;
        }
    },

    async onSourceChange() {
        const sel = document.getElementById("ob-source-select");
        this._currentSource = sel ? sel.value : "live";

        if (this._currentSource === "live") {
            this._renderTable();
            return;
        }

        // Load from saved CSV file
        const area = document.getElementById("ob-table-area");
        if (area) area.innerHTML = '<div class="alert alert-info">Loading orderbook...</div>';

        try {
            const resp = await App.api(`/api/orderbook/load?file=${encodeURIComponent(this._currentSource)}`);
            const data = resp.data || [];
            if (data.length === 0) {
                area.innerHTML = '<div class="alert alert-info">Orderbook file is empty.</div>';
                return;
            }
            area.innerHTML = this._buildTableHTML(data);
        } catch (e) {
            area.innerHTML = `<div class="alert alert-danger">Failed to load orderbook: ${e.message}</div>`;
        }
    },

    /* ─── Actions ──────────────────────────────────────────────────────── */

    refresh() {
        this._loadSavedFiles();
        if (this._currentSource === "live") {
            this._renderTable();
        } else {
            this.onSourceChange();
        }
        App.toast("Orderbook refreshed", "info");
    },

    exportCSV() {
        // If viewing a saved file, just trigger download from the server
        if (this._currentSource !== "live") {
            const link = document.createElement("a");
            link.href = `/api/reports/${encodeURIComponent(this._currentSource)}`;
            link.download = this._currentSource;
            link.click();
            return;
        }

        const data = this._getLiveData();
        if (!data || data.length === 0) {
            App.toast("No orderbook data to export", "warning");
            return;
        }

        const cols = this._displayColumns;
        const header = cols.join(",");
        const rows = data.map(row =>
            cols.map(c => {
                let v = String(row[c] ?? "");
                if (v.includes(",") || v.includes('"') || v.includes("\n")) {
                    v = '"' + v.replace(/"/g, '""') + '"';
                }
                return v;
            }).join(",")
        );
        const csv = header + "\n" + rows.join("\n");

        const blob = new Blob([csv], { type: "text/csv" });
        const url = URL.createObjectURL(blob);
        const link = document.createElement("a");
        link.href = url;
        link.download = "orderbook_export.csv";
        link.click();
        URL.revokeObjectURL(url);
    },
};
