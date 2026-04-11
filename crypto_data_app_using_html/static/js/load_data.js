/**
 * Load Data Page - Scan CSV folder, select symbols, load into catalog.
 */

const LoadData = {
    entries: [],
    selectedEntries: [],

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#128194; Load Crypto Data</h1>
            <p class="page-subtitle">Load daily OHLCV data from your local CSV files into the NautilusTrader catalog.</p>

            <div class="form-group">
                <label class="form-label">CSV Data Folder</label>
                <div class="form-row">
                    <div class="form-group" style="flex: 3;">
                        <input type="text" id="csv-folder" class="form-control" value="${this.getDefaultFolder()}" placeholder="Path to CSV folder">
                    </div>
                    <div class="form-group" style="flex: 0;">
                        <button class="btn btn-primary" onclick="LoadData.scanFolder()">Scan Folder</button>
                    </div>
                </div>
            </div>

            <div id="scan-results"></div>
            <div class="page-divider"></div>
            <h2 class="section-title">Current Catalog Contents</h2>
            <div id="catalog-contents">
                <div class="alert alert-info">Loading catalog...</div>
            </div>
        `;

        this.scanFolder();
        this.loadCatalogContents();
    },

    getDefaultFolder() {
        return "C:\\\\Users\\\\ADMIN\\\\Desktop\\\\id_name_all_symbols\\\\id_name_all_symbols";
    },

    async scanFolder() {
        const folder = document.getElementById("csv-folder").value;
        const resultsDiv = document.getElementById("scan-results");
        resultsDiv.innerHTML = '<div class="alert alert-info">Scanning folder...</div>';

        try {
            const data = await App.api(`/api/csv/scan?folder=${encodeURIComponent(folder)}`);
            this.entries = data.entries;

            if (data.count === 0) {
                resultsDiv.innerHTML = `<div class="alert alert-warning">No CSV files found in the folder. Check the path.</div>`;
                return;
            }

            // Group by symbol
            const majorIds = {1: "BTC", 1027: "ETH", 5426: "SOL", 52: "XRP", 74: "DOGE", 2010: "ADA",
                             5994: "SHIB", 2: "LTC", 3794: "ATOM", 4642: "HBAR", 512: "XLM"};

            let quickButtons = "";
            for (const [id, sym] of Object.entries(majorIds)) {
                const entry = this.entries.find(e => e.id === parseInt(id));
                if (entry) {
                    quickButtons += `<button class="quick-select-btn" onclick="LoadData.quickLoad(${id})">${sym} - ${entry.name}</button>`;
                }
            }

            // Build options for multiselect
            const options = this.entries.map(e =>
                `<option value="${e.id}">${e.filename}</option>`
            ).join("");

            resultsDiv.innerHTML = `
                <div class="alert alert-success">Found <strong>${data.count}</strong> CSV files.</div>

                <h2 class="section-title">Select Symbols to Load</h2>
                <p class="section-caption">Quick select major coins:</p>
                <div class="quick-select-grid">${quickButtons}</div>

                <div class="page-divider"></div>

                <div class="form-group">
                    <label class="form-label">Search & select from all available files:</label>
                    <input type="text" id="csv-search" class="form-control" placeholder="Type to search... (e.g. BTC, Ethereum, DOGE)"
                           oninput="LoadData.filterList()" style="margin-bottom: 8px;">
                    <select id="csv-multiselect" class="form-control" multiple>
                        ${options}
                    </select>
                    <p class="section-caption" id="csv-search-count" style="margin-top: 4px;">Showing ${data.count} files</p>
                </div>

                <button class="btn btn-primary btn-block" onclick="LoadData.loadSelected()">
                    &#128229; Load Selected into Catalog
                </button>

                <div id="load-progress" style="margin-top: 16px;"></div>
                <div id="load-results" style="margin-top: 16px;"></div>
            `;
        } catch (e) {
            resultsDiv.innerHTML = `<div class="alert alert-danger">Scan failed: ${e.message}</div>`;
        }
    },

    filterList() {
        const query = (document.getElementById("csv-search").value || "").toLowerCase();
        const select = document.getElementById("csv-multiselect");
        const countEl = document.getElementById("csv-search-count");
        let visible = 0;

        // Rebuild options filtered by query
        let html = "";
        for (const e of this.entries) {
            const label = e.filename.toLowerCase();
            if (!query || label.includes(query)) {
                html += `<option value="${e.id}">${e.filename}</option>`;
                visible++;
            }
        }
        select.innerHTML = html;
        if (countEl) countEl.textContent = `Showing ${visible} of ${this.entries.length} files`;
    },

    quickLoad(id) {
        const entry = this.entries.find(e => e.id === id);
        if (entry) {
            this.doLoad([entry]);
        }
    },

    loadSelected() {
        const select = document.getElementById("csv-multiselect");
        const selectedIds = Array.from(select.selectedOptions).map(o => parseInt(o.value));
        const entries = this.entries.filter(e => selectedIds.includes(e.id));
        if (entries.length === 0) {
            App.toast("No symbols selected.", "error");
            return;
        }
        this.doLoad(entries);
    },

    async doLoad(entries) {
        const progressDiv = document.getElementById("load-progress");
        const resultsDiv = document.getElementById("load-results");

        progressDiv.innerHTML = `
            <div class="progress-text">Loading ${entries.length} symbol(s)...</div>
            <div class="progress-bar-container">
                <div class="progress-bar-fill" id="load-progress-bar" style="width: 0%"></div>
            </div>
        `;
        resultsDiv.innerHTML = "";

        try {
            const data = await App.api("/api/csv/load", {
                method: "POST",
                body: JSON.stringify({ entries }),
            });

            // Update progress to 100%
            const bar = document.getElementById("load-progress-bar");
            if (bar) bar.style.width = "100%";

            // Show results
            let html = "";
            for (const result of data.results) {
                html += `
                    <div class="alert alert-success">
                        Loaded <strong>${result.num_bars}</strong> daily bars for
                        <strong>${result.symbol}</strong> (${result.name})
                    </div>`;

                html += App.accordionHTML(
                    `load-${result.symbol}`,
                    `${result.symbol} - ${result.name} (${result.num_bars} bars)`,
                    `
                    <div class="grid-3">
                        ${App.metricHTML("Bars", result.num_bars)}
                        ${App.metricHTML("Date Range", `${result.date_start} &rarr; ${result.date_end}`)}
                        ${App.metricHTML("Latest Close", App.currency(result.latest_close))}
                    </div>
                    <div style="margin-top: 16px;">
                        ${App.tableHTML(result.sample_data)}
                    </div>`
                );
            }

            for (const error of data.errors) {
                html += `<div class="alert alert-danger">Failed to load ${error.symbol}: ${error.error}</div>`;
            }

            resultsDiv.innerHTML = html;
            progressDiv.innerHTML = '<div class="progress-text" style="color: var(--success);">Loading complete!</div>';

            App.toast(`Loaded ${data.results.length} symbol(s) successfully.`, "success");
            this.loadCatalogContents();

        } catch (e) {
            progressDiv.innerHTML = "";
            resultsDiv.innerHTML = `<div class="alert alert-danger">Load failed: ${e.message}</div>`;
        }
    },

    async loadCatalogContents() {
        const div = document.getElementById("catalog-contents");
        if (!div) return;

        try {
            const data = await App.api("/api/catalog/status");
            if (data.exists && data.data_types && data.data_types.length > 0) {
                div.innerHTML = `
                    <div class="card">
                        <pre style="color: var(--text-secondary); font-size: 0.85rem; white-space: pre-wrap;">${JSON.stringify(data.data_types, null, 2)}</pre>
                    </div>`;
            } else {
                div.innerHTML = '<div class="alert alert-info">Catalog is empty. Load some data above to get started!</div>';
            }
        } catch (e) {
            div.innerHTML = `<div class="alert alert-danger">Could not read catalog: ${e.message}</div>`;
        }
    },
};
