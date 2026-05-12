/**
 * Load Data Page - Scan CSV folder, select symbols, load into catalog.
 *
 * Venues and asset classes are fetched from configured adapters
 * (saved in adapter_admin/adapters_config/*.json), not hardcoded.
 */

const LoadData = {
    entries: [],
    selectedEntries: [],
    allAssetClasses: [],
    assetVenues: {},  // populated from API
    dataFormats: {},  // per-asset-class data format configs

    async render(container) {
        // Fetch asset classes, configured adapters, and data formats
        try {
            const [acData, adData, dfData] = await Promise.all([
                App.api("/api/asset-classes"),
                App.api("/api/configured-adapters"),
                App.api("/api/data-formats"),
            ]);
            this.allAssetClasses = acData.asset_classes || [];
            this.assetVenues = adData.adapters || {};
            this.dataFormats = dfData.formats || {};
        } catch (e) {
            this.allAssetClasses = [];
            this.assetVenues = {};
            this.dataFormats = {};
        }

        const defaultAsset = this.allAssetClasses[0];
        const defaultVenues = this.assetVenues[defaultAsset] || [];

        const assetOptions = this.allAssetClasses.map(a =>
            `<option value="${a}">${a.charAt(0).toUpperCase() + a.slice(1)}</option>`
        ).join("");

        const venueOptions = defaultVenues.map(v =>
            `<option value="${v}">${v}</option>`
        ).join("");

        container.innerHTML = `
            <h1 class="page-title">&#128194; Load Market Data</h1>
            <p class="page-subtitle">Load daily OHLCV data from your local CSV files into the NautilusTrader catalog.</p>

            <div class="form-row" style="gap: 16px; align-items: flex-start;">
                <div class="form-group" style="flex: 1;">
                    <label class="form-label">Asset Class</label>
                    <select id="asset-class-select" class="form-control" onchange="LoadData.onAssetClassChange()">
                        ${assetOptions}
                    </select>
                </div>
                <div class="form-group" style="flex: 1;">
                    <label class="form-label">Venue / Exchange</label>
                    <select id="venue-select" class="form-control" onchange="LoadData.updateInstrumentPreview()"
                            ${defaultVenues.length === 0 ? 'style="display:none"' : ''}>
                        ${venueOptions}
                    </select>
                    <div id="no-venue-msg" class="alert alert-warning" style="margin-top:4px; font-size:0.85rem; ${defaultVenues.length > 0 ? 'display:none' : ''}">
                        No adapters configured for this asset class. Create one in the Adapter Admin Panel.
                    </div>
                    <p class="section-caption" id="instrument-preview">${defaultVenues.length > 0 ? `Instrument ID preview: BTCUSDT.${defaultVenues[0]}` : ''}</p>
                </div>
            </div>

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

            <div class="status-buttons-row">
                <button class="btn" onclick="LoadData.showLoadingStatusPopup()">
                    Loading Status <span class="btn-badge" id="loading-status-badge" style="display:none;">0</span>
                </button>
                <button class="btn" onclick="LoadData.showCatalogPopup()">
                    Catalog Contents <span class="btn-badge" id="catalog-count-badge" style="display:none;">0</span>
                </button>
            </div>

            <!-- Hidden containers that feed into the popups -->
            <div id="load-progress" style="display:none;"></div>
            <div id="load-results" style="display:none;"></div>
            <div id="catalog-contents" style="display:none;"></div>
        `;

        App.state.selectedAsset = defaultAsset;
        App.state.selectedVenue = defaultVenues[0] || "";
        this.scanFolder();
        this.loadCatalogContents();

        // Set action bar
        App.setActionBar(`
            <button class="btn btn-sm btn-primary" onclick="LoadData.scanFolder()">Scan Folder</button>
            <button class="btn btn-sm" onclick="LoadData.showLoadingStatusPopup()">Loading Status</button>
            <button class="btn btn-sm" onclick="LoadData.showCatalogPopup()">Catalog Contents</button>
        `);
    },

    onAssetClassChange() {
        const assetClass = document.getElementById("asset-class-select").value;
        const venueSelect = document.getElementById("venue-select");
        const noVenueMsg = document.getElementById("no-venue-msg");
        const venues = this.assetVenues[assetClass] || [];

        if (venues.length > 0) {
            venueSelect.innerHTML = venues.map(v =>
                `<option value="${v}">${v}</option>`
            ).join("");
            venueSelect.style.display = "";
            noVenueMsg.style.display = "none";
        } else {
            venueSelect.innerHTML = "";
            venueSelect.style.display = "none";
            noVenueMsg.style.display = "";
        }

        App.state.selectedAsset = assetClass;
        App.state.selectedVenue = venues[0] || "";
        this.updateInstrumentPreview();

        // Re-scan folder with new asset class's format config
        const folder = document.getElementById("csv-folder")?.value;
        if (folder) this.scanFolder();
    },

    updateInstrumentPreview() {
        const venueSelect = document.getElementById("venue-select");
        const preview = document.getElementById("instrument-preview");
        if (venueSelect && venueSelect.value) {
            App.state.selectedVenue = venueSelect.value;
            if (preview) {
                preview.textContent = `Instrument ID preview: BTCUSDT.${venueSelect.value}`;
            }
        } else {
            App.state.selectedVenue = "";
            if (preview) preview.textContent = "";
        }
    },

    getDefaultFolder() {
        return "C:\\\\Users\\\\HP\\\\Desktop\\\\MS\\\\Dataset\\\\id_name_all_symbols";
    },

    async scanFolder() {
        const folder = document.getElementById("csv-folder").value;
        const resultsDiv = document.getElementById("scan-results");
        resultsDiv.innerHTML = '<div class="alert alert-info">Scanning folder...</div>';

        // Get current asset class and its data format config
        const assetClass = document.getElementById("asset-class-select")?.value || "";
        const formatConfig = this.dataFormats[assetClass];
        const csvConfig = formatConfig?.csv || {};
        const isConfigured = csvConfig.filename_pattern !== null && csvConfig.filename_pattern !== undefined;

        if (!isConfigured) {
            resultsDiv.innerHTML = `
                <div class="alert alert-warning">
                    No data format configured for <strong>${formatConfig?.label || assetClass}</strong>.
                    Configure it in the Adapter Admin Panel (Data Formats page).
                </div>`;
            return;
        }

        try {
            const data = await App.api(`/api/csv/scan?folder=${encodeURIComponent(folder)}`, { timeoutMs: 0 });
            this.entries = data.entries;

            if (data.count === 0) {
                resultsDiv.innerHTML = `<div class="alert alert-warning">No CSV files found in the folder. Check the path.</div>`;
                return;
            }

            // Split files into matched and unmatched based on filename pattern
            let matched = [];
            let unmatched = [];
            try {
                const regex = new RegExp(csvConfig.filename_pattern);
                for (const entry of this.entries) {
                    if (regex.test(entry.filename)) {
                        matched.push(entry);
                    } else {
                        unmatched.push(entry);
                    }
                }
            } catch (regexErr) {
                // Invalid regex — treat all as matched
                matched = this.entries;
                unmatched = [];
            }

            // Quick select buttons for matched files
            const majorIds = {
                1: "BTC", 1027: "ETH", 5426: "SOL", 52: "XRP", 74: "DOGE", 2010: "ADA",
                5994: "SHIB", 2: "LTC", 3794: "ATOM", 4642: "HBAR", 512: "XLM"
            };

            let quickButtons = "";
            for (const [id, sym] of Object.entries(majorIds)) {
                const entry = matched.find(e => e.id === parseInt(id));
                if (entry) {
                    quickButtons += `<button class="quick-select-btn" onclick="LoadData.quickLoad(${id})">${sym} - ${entry.name}</button>`;
                }
            }

            // Build options for matched files multiselect
            const options = matched.map(e =>
                `<option value="${e.id}">${e.filename}</option>`
            ).join("");

            // Build unmatched files section
            let unmatchedHTML = "";
            if (unmatched.length > 0) {
                const unmatchedList = unmatched.map(e => `<li>${e.filename}</li>`).join("");
                unmatchedHTML = `
                    <div class="page-divider"></div>
                    ${App.accordionHTML("unmatched-files",
                        "Unmatched Files (" + unmatched.length + " files don't match the expected format)",
                        '<div class="alert alert-warning" style="margin-bottom:8px;">Expected format: <strong>' + (csvConfig.filename_description || csvConfig.filename_pattern) + '</strong></div>' +
                        '<ul style="font-size:0.85rem; color:var(--text-muted); max-height:200px; overflow-y:auto;">' + unmatchedList + '</ul>'
                    )}`;
            }

            resultsDiv.innerHTML = `
                <div class="alert alert-success">
                    Found <strong>${data.count}</strong> CSV files.
                    ${matched.length < data.count ? ` <strong>${matched.length}</strong> match the expected format, <strong>${unmatched.length}</strong> do not.` : ''}
                </div>

                ${matched.length > 0 ? `
                    <h2 class="section-title">Select Symbols to Load</h2>
                    ${quickButtons ? `<p class="section-caption">Quick select:</p><div class="quick-select-grid">${quickButtons}</div><div class="page-divider"></div>` : ''}

                    <div class="form-group">
                        <label class="form-label">Search & select from matched files:</label>
                        <input type="text" id="csv-search" class="form-control" placeholder="Type to search..."
                               oninput="LoadData.filterList()" style="margin-bottom: 8px;">
                        <select id="csv-multiselect" class="form-control" multiple>
                            ${options}
                        </select>
                        <p class="section-caption" id="csv-search-count" style="margin-top: 4px;">Showing ${matched.length} matched files</p>
                    </div>

                    <button class="btn btn-primary btn-block" onclick="LoadData.loadSelected()">
                        &#128229; Load Selected into Catalog
                    </button>
                ` : `
                    <div class="alert alert-warning">No files match the expected format: <strong>${csvConfig.filename_description || csvConfig.filename_pattern}</strong></div>
                `}

                ${unmatchedHTML}

                <div id="load-progress" style="display:none;"></div>
                <div id="load-results" style="display:none;"></div>
            `;

            // Store only matched entries for loading
            this.entries = matched;

        } catch (e) {
            resultsDiv.innerHTML = `<div class="alert alert-danger">Scan failed: ${e.message}</div>`;
        }
    },

    /** Filter the CSV multiselect. Called via inline oninput on every keystroke —
     *  the body is debounced + uses DocumentFragment to avoid per-keystroke reflow
     *  of a potentially very large <select multiple>. */
    _filterListImpl() {
        const searchEl = document.getElementById("csv-search");
        const select = document.getElementById("csv-multiselect");
        const countEl = document.getElementById("csv-search-count");
        if (!searchEl || !select) return;

        const query = (searchEl.value || "").toLowerCase();

        // Preserve the user's current selection across the rebuild.
        const selectedIds = new Set(
            Array.from(select.selectedOptions, o => o.value)
        );

        // Build into a DocumentFragment, then swap in one reflow.
        const frag = document.createDocumentFragment();
        let visible = 0;
        for (const e of this.entries) {
            if (!query || e.filename.toLowerCase().includes(query)) {
                const opt = document.createElement("option");
                opt.value = String(e.id);
                opt.textContent = e.filename;
                if (selectedIds.has(String(e.id))) opt.selected = true;
                frag.appendChild(opt);
                visible++;
            }
        }
        select.replaceChildren(frag);
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
        this._updateBadges();
        App.log(`Loading ${entries.length} symbol(s) into catalog...`, "MESSAGE", "LoadData");
        // Auto-open the loading status popup so user sees progress immediately
        this.showLoadingStatusPopup();

        try {
            const venueSelect = document.getElementById("venue-select");
            const venue = venueSelect ? venueSelect.value : "BINANCE";
            const assetClass = document.getElementById("asset-class-select")?.value || "";
            const data = await App.api("/api/csv/load", {
                method: "POST",
                body: JSON.stringify({ entries, venue, asset_class: assetClass }),
                // FX 1-minute ingest of multi-year ASK/BID merges can take many
                // minutes; the default 60s client timeout was aborting the
                // fetch ("signal is aborted without reason") before the server
                // finished. Disable client-side timeout for catalog ingest.
                timeoutMs: 0,
            });

            // Render inline (ASK / BID) results and errors immediately.
            let html = "";
            let warnCount = 0;
            for (const result of data.results) {
                html += this._renderLoadRow(result);
                if (result.warning) warnCount++;
            }
            for (const error of data.errors) {
                const sideTag = error.side ? ` [${error.side}]` : "";
                html += `<div class="alert alert-danger">Failed to load ${error.symbol}${sideTag}: ${error.error}</div>`;
            }
            // Append a placeholder card for each MID job still running on the
            // server; _pollPendingJob swaps it out when the worker reports
            // success or error.
            const pendingJobs = data.pending_jobs || [];
            for (const job of pendingJobs) {
                html += this._renderPendingJob(job);
            }
            resultsDiv.innerHTML = html;
            this._updateBadges();
            this._refreshOpenModal();

            const inlineDone = data.results.length + data.errors.length;
            // Track totals on the instance so _onPendingJobFinished can keep
            // the progress bar honest as background jobs complete.
            this._totalLoadJobs = entries.length;
            this._inlineDoneCount = inlineDone;
            this._updateLoadProgress();

            if (data.results.length > 0) {
                const tail = pendingJobs.length > 0
                    ? ` ${pendingJobs.length} ingest job(s) running in background.`
                    : "";
                App.toast(
                    `Loaded ${data.results.length} symbol(s) successfully.${tail}`,
                    "success"
                );
                App.log(`Loaded ${data.results.length} symbol(s) successfully.${tail}`, "SUCCESS", "LoadData");
            } else if (pendingJobs.length > 0) {
                App.toast(`${pendingJobs.length} ingest job(s) running in background — cards will fill in as each finishes.`, "success");
            }
            if (warnCount > 0) {
                App.toast(`${warnCount} symbol(s) have precision warnings — check the results panel.`, "error", 8000);
            }

            if (pendingJobs.length === 0) {
                this.loadCatalogContents();
            } else {
                for (const job of pendingJobs) {
                    this._pollPendingJob(job);
                }
            }

        } catch (e) {
            progressDiv.innerHTML = "";
            resultsDiv.innerHTML = `<div class="alert alert-danger">Load failed: ${e.message}</div>`;
        }
    },

    _renderLoadRow(result) {
        const sideTag = result.side ? ` [${result.side}]` : "";
        let html = `
            <div class="alert alert-success">
                Loaded <strong>${result.num_bars}</strong> daily bars for
                <strong>${result.symbol}${sideTag}</strong> (${result.name})
            </div>`;
        // Precision sanity warning from server. Surface inline so the user
        // sees it alongside the success banner for the same symbol.
        if (result.warning) {
            html += `
                <div class="alert alert-warning" style="margin-top: -8px;">
                    <strong>&#9888; Ingest sanity warning for ${result.symbol}${sideTag}:</strong>
                    <div style="margin-top: 4px; font-size: 0.9em;">${result.warning}</div>
                </div>`;
        }
        html += App.accordionHTML(
            `load-${result.symbol}-${result.side || "default"}`,
            `${result.symbol}${sideTag} - ${result.name} (${result.num_bars} bars)`,
            `
            <div class="grid-3">
                ${App.metricHTML("Bars", result.num_bars)}
                ${App.metricHTML("Date Range", `${result.date_start} &rarr; ${result.date_end}`)}
                ${App.metricHTML("Latest Close", App.currency(result.latest_close))}
            </div>
            <div style="margin-top: 8px; font-size: 0.85em; color: var(--text-secondary);">
                Stored precision: ${result.price_precision ?? "?"} decimals &middot;
                Doji rate (open==close): ${(result.doji_rate * 100 || 0).toFixed(1)}%
            </div>
            <div style="margin-top: 16px;">
                ${App.tableHTML(result.sample_data)}
            </div>`
        );
        return html;
    },

    _renderPendingJob(job) {
        // Each background ingest job lives in a placeholder card;
        // _pollPendingJob replaces it via outerHTML when the worker thread
        // finishes. Verb depends on side: MID is synthesized from ASK+BID
        // in-process, ASK/BID are read directly from the daily files.
        const verb = job.side === "MID" ? "Synthesizing" : "Loading";
        const sideTag = job.side ? ` ${job.side}` : "";
        return `
            <div class="alert alert-info" id="pending-job-${job.job_id}">
                ${verb} <strong>${job.symbol}${sideTag}</strong> in background...
                Catalog write will appear here when complete.
            </div>`;
    },

    _pollPendingJob(job) {
        const intervalMs = 3000;
        const handle = setInterval(async () => {
            let status;
            try {
                status = await App.api(`/api/csv/jobs/${encodeURIComponent(job.job_id)}`);
            } catch (e) {
                clearInterval(handle);
                this._replacePendingCard(job,
                    `<div class="alert alert-danger">Lost contact with MID job for ${job.symbol}: ${e.message}</div>`);
                this._onPendingJobFinished();
                return;
            }
            if (status.status === "success") {
                clearInterval(handle);
                this._replacePendingCard(job, this._renderLoadRow(status.result));
                this._onPendingJobFinished();
            } else if (status.status === "error") {
                clearInterval(handle);
                this._replacePendingCard(job,
                    `<div class="alert alert-danger">Failed to load ${job.symbol} [${job.side}]: ${status.error || "unknown error"}</div>`);
                this._onPendingJobFinished();
            }
            // status === "pending" | "running": keep polling.
        }, intervalMs);
    },

    _replacePendingCard(job, html) {
        const placeholder = document.getElementById(`pending-job-${job.job_id}`);
        if (placeholder) placeholder.outerHTML = html;
    },

    _onPendingJobFinished() {
        // Update the progress bar incrementally each time a background job
        // resolves, then refresh the catalog list once they're all done.
        this._updateLoadProgress();
        this._updateBadges();
        this._refreshOpenModal();
        const remaining = document.querySelectorAll(
            '#load-results [id^="pending-job-"]'
        ).length;
        if (remaining === 0) {
            this.loadCatalogContents();
        }
    },

    _updateLoadProgress() {
        const bar = document.getElementById("load-progress-bar");
        const text = document.querySelector("#load-progress .progress-text");
        const total = this._totalLoadJobs || 0;
        const inlineDone = this._inlineDoneCount || 0;
        const pendingRemaining = document.querySelectorAll(
            '#load-results [id^="pending-job-"]'
        ).length;
        const done = total - pendingRemaining;

        if (total === 0 || pendingRemaining === 0) {
            if (bar) bar.style.width = "100%";
            if (text) {
                text.textContent = "Loading complete!";
                text.style.color = "var(--success)";
            }
            return;
        }

        const pct = Math.round((done / total) * 100);
        if (bar) bar.style.width = `${pct}%`;
        if (text) {
            const inlineNote = inlineDone > 0
                ? `${inlineDone} done inline; `
                : "";
            text.textContent =
                `${inlineNote}${pendingRemaining} of ${total} still running in background...`;
        }
    },

    /** Show a modal popup with loading status (progress + results) */
    showLoadingStatusPopup() {
        this._activeModal = "loading-status";
        this._refreshLoadingStatusModal();
    },

    _refreshLoadingStatusModal() {
        const progressDiv = document.getElementById("load-progress");
        const resultsDiv = document.getElementById("load-results");
        const progressHTML = progressDiv ? progressDiv.innerHTML : "";
        const resultsHTML = resultsDiv ? resultsDiv.innerHTML : "";

        const content = progressHTML || resultsHTML
            ? `<div>${progressHTML}</div><div style="margin-top:12px;">${resultsHTML}</div>`
            : '<div class="alert alert-info">No loading activity yet. Select symbols above and click Load.</div>';

        this._openModal("Loading Status", content);
    },

    /** Show a modal popup with current catalog contents */
    showCatalogPopup() {
        this._activeModal = "catalog";
        const catalogDiv = document.getElementById("catalog-contents");
        const content = catalogDiv ? catalogDiv.innerHTML : '<div class="alert alert-info">Loading catalog...</div>';
        this._openModal("Current Catalog Contents", content);
    },

    /** Refresh the currently open modal if it matches the given type */
    _refreshOpenModal() {
        if (!document.getElementById("ld-modal-backdrop")) return;
        if (this._activeModal === "loading-status") {
            this._refreshLoadingStatusModal();
        } else if (this._activeModal === "catalog") {
            this.showCatalogPopup();
        }
    },

    /** Generic modal open helper */
    _openModal(title, bodyHTML) {
        const existing = document.getElementById("ld-modal-backdrop");
        // If modal already open, just update the body content
        if (existing) {
            const body = existing.querySelector(".modal-body");
            if (body) body.innerHTML = bodyHTML;
            const titleEl = existing.querySelector(".modal-header-title");
            if (titleEl) titleEl.textContent = title;
            return;
        }

        const backdrop = document.createElement("div");
        backdrop.className = "modal-backdrop";
        backdrop.id = "ld-modal-backdrop";
        backdrop.addEventListener("click", (e) => {
            if (e.target === backdrop) this._closeModal();
        });

        backdrop.innerHTML = `
            <div class="modal-dialog">
                <div class="modal-header">
                    <span class="modal-header-title">${title}</span>
                    <button class="modal-close-btn" onclick="LoadData._closeModal()">&times;</button>
                </div>
                <div class="modal-body">${bodyHTML}</div>
            </div>`;

        document.body.appendChild(backdrop);
    },

    _closeModal() {
        const existing = document.getElementById("ld-modal-backdrop");
        if (existing) existing.remove();
        this._activeModal = null;
    },

    /** Update the badge counts on the status buttons */
    _updateBadges() {
        // Loading status badge — show count of pending jobs
        const pendingCount = document.querySelectorAll('#load-results [id^="pending-job-"]').length;
        const statusBadge = document.getElementById("loading-status-badge");
        if (statusBadge) {
            if (pendingCount > 0) {
                statusBadge.textContent = pendingCount;
                statusBadge.className = "btn-badge running";
                statusBadge.style.display = "";
            } else {
                // Show total results count if any exist
                const totalResults = document.getElementById("load-results");
                const hasResults = totalResults && totalResults.innerHTML.trim().length > 0;
                if (hasResults) {
                    statusBadge.textContent = "Done";
                    statusBadge.className = "btn-badge";
                    statusBadge.style.display = "";
                } else {
                    statusBadge.style.display = "none";
                }
            }
        }
    },

    async loadCatalogContents() {
        const div = document.getElementById("catalog-contents");
        if (!div) return;

        try {
            const data = await App.api("/api/data/bar_types");
            const types = data.bar_types || [];
            const details = data.bar_type_details || {};

            // Update catalog badge
            const catalogBadge = document.getElementById("catalog-count-badge");
            if (catalogBadge) {
                catalogBadge.textContent = types.length;
                catalogBadge.style.display = types.length > 0 ? "" : "none";
            }

            if (types.length === 0) {
                div.innerHTML = '<div class="alert alert-info">Catalog is empty. Load some data above to get started!</div>';
                this._refreshOpenModal();
                return;
            }

            let rows = "";
            for (const bt of types) {
                // Parse: BTCUSD.BINANCE_MS-1-DAY-LAST-EXTERNAL
                const parts = bt.split("-");
                const instrument = parts[0] || bt;           // BTCUSD.BINANCE_MS
                const timeframe = parts.length >= 3 ? `${parts[1]}-${parts[2]}` : "—"; // 1-DAY
                const info = details[bt] || {};
                const dateRange = info.start_date && info.end_date
                    ? `${info.start_date} → ${info.end_date}` : "—";

                rows += `<tr>
                    <td>${instrument}</td>
                    <td>${timeframe}</td>
                    <td>${dateRange}</td>
                </tr>`;
            }

            div.innerHTML = `
                <div class="card">
                    <table class="table" style="margin:0;">
                        <thead><tr>
                            <th>Instrument</th>
                            <th>Timeframe</th>
                            <th>Date Range</th>
                        </tr></thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>`;
            this._refreshOpenModal();
        } catch (e) {
            div.innerHTML = `<div class="alert alert-danger">Could not read catalog: ${e.message}</div>`;
        }
    },
};

// Install the debounced public filterList once the module is parsed.
// Inline oninput="LoadData.filterList()" will hit this debounced wrapper,
// which after 120ms of quiet calls the real _filterListImpl.
LoadData.filterList = App.debounce(function () {
    LoadData._filterListImpl();
}, 120);
