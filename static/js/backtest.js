/**
 * Backtest Page - Configure and run trading strategies.
 * Supports multiple instruments with per-instrument date ranges.
 * Includes: custom strategy upload/manage, HTML report download,
 * order book, logs, and all CSV report types.
 */

const Backtest = {
    strategies: {},
    barTypes: [],
    selectedStrategies: [],
    lastInstrumentResults: {},  // keyed by bar_type

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#129514; Run Backtest</h1>
            <p class="page-subtitle">Test trading strategies on your downloaded market data.</p>

            <div id="backtest-setup">
                <div class="alert alert-info">Loading configuration...</div>
            </div>

            <div id="backtest-results-section"></div>
        `;

        await this.loadConfig();
        this.showPreviousResults();
    },

    async loadConfig() {
        try {
            const [barData, stratData, customData] = await Promise.all([
                App.api("/api/data/bar_types"),
                App.api("/api/strategies"),
                App.api("/api/custom_strategies/list"),
            ]);

            this.barTypes = barData.bar_types;
            this.barTypeDetails = barData.bar_type_details || {};
            this.strategies = stratData.strategies;
            this.customFiles = customData.files;

            if (this.barTypes.length === 0) {
                document.getElementById("backtest-setup").innerHTML =
                    '<div class="alert alert-warning">No data in catalog. Go to <strong>Load Data</strong> first.</div>';
                return;
            }

            this.renderSetup();
        } catch (e) {
            document.getElementById("backtest-setup").innerHTML =
                `<div class="alert alert-danger">Failed to load config: ${e.message}</div>`;
        }
    },

    renderSetup() {
        const stratNames = Object.keys(this.strategies);
        const stratCheckboxes = stratNames.map(name => `
            <div class="checkbox-row">
                <input type="checkbox" id="strat-${this.safeId(name)}" value="${name}"
                       onchange="Backtest.updateStrategySelection()" checked>
                <label for="strat-${this.safeId(name)}">${name}</label>
            </div>
        `).join("");

        // Custom strategies file list
        let customListHTML = "";
        if (this.customFiles && this.customFiles.length > 0) {
            const fileRows = this.customFiles.map(f => `
                <div style="display: flex; align-items: center; justify-content: space-between; padding: 6px 0; border-bottom: 1px solid var(--border-color);">
                    <span style="font-size: 0.85rem;">${f}</span>
                    <button class="btn btn-danger btn-sm" onclick="Backtest.deleteCustomStrategy('${f}')">Delete</button>
                </div>
            `).join("");
            customListHTML = App.accordionHTML("manage-custom", `Manage Custom Strategies (${this.customFiles.length} loaded)`, fileRows);
        }

        // Instrument checkboxes with per-instrument date range
        // Filter instruments by selected venue from Load Data page
        const venue = App.state.selectedVenue || "";
        this.filteredBarTypes = venue
            ? this.barTypes.filter(bt => bt.toUpperCase().includes(`.${venue.toUpperCase()}`))
            : this.barTypes;

        // Fallback: if no instruments match the venue, show all
        if (this.filteredBarTypes.length === 0) {
            this.filteredBarTypes = this.barTypes;
        }

        const instrumentList = this.filteredBarTypes.map(bt => {
            const sid = this.safeId(bt);
            const details = this.barTypeDetails[bt] || {};
            const startDate = details.start_date || "";
            const endDate = details.end_date || "";
            const rangeLabel = startDate && endDate
                ? `<span style="color: var(--text-secondary); font-size: 0.78rem; margin-left: 6px;">(Data: ${startDate} &rarr; ${endDate})</span>`
                : "";
            return `
                <div class="instrument-item" style="margin-bottom: 10px; padding: 6px 8px; border-radius: 6px; border: 1px solid var(--border-color);">
                    <div class="checkbox-row">
                        <input type="checkbox" id="inst-${sid}" value="${bt}"
                               onchange="Backtest.toggleInstrumentDates('${sid}')">
                        <label for="inst-${sid}">${bt} ${rangeLabel}</label>
                    </div>
                    <div id="inst-dates-${sid}" style="display: none; margin-left: 28px; margin-top: 6px;">
                        <div class="form-row" style="gap: 8px;">
                            <div class="form-group" style="flex: 1;">
                                <label class="form-label" style="font-size: 0.75rem;">From (default: ${startDate || "N/A"})</label>
                                <input type="date" id="inst-from-${sid}" class="form-control" style="font-size: 0.85rem;"
                                       value="${startDate}" min="${startDate}" max="${endDate}">
                            </div>
                            <div class="form-group" style="flex: 1;">
                                <label class="form-label" style="font-size: 0.75rem;">To (default: ${endDate || "N/A"})</label>
                                <input type="date" id="inst-to-${sid}" class="form-control" style="font-size: 0.85rem;"
                                       value="${endDate}" min="${startDate}" max="${endDate}">
                            </div>
                        </div>
                    </div>
                </div>`;
        }).join("");

        document.getElementById("backtest-setup").innerHTML = `
            <h2 class="section-title">Configuration</h2>

            <div class="form-group">
                <label class="form-label">Select Instruments</label>
                <p class="section-caption">Showing instruments for venue: <strong>${venue || "ALL"}</strong> (change on Load Data page).</p>
                <input type="text" id="inst-search" class="form-control" placeholder="Search instruments..."
                       oninput="Backtest.filterInstruments()" style="margin-bottom: 8px;">
                <div style="margin-bottom: 8px;">
                    <button class="btn btn-sm" onclick="Backtest.selectAllInstruments(true)">Select All</button>
                    <button class="btn btn-sm" onclick="Backtest.selectAllInstruments(false)">Deselect All</button>
                </div>
                <div id="instrument-list" style="max-height: 300px; overflow-y: auto; border: 1px solid var(--border-color); border-radius: 6px; padding: 8px;">
                    ${instrumentList}
                </div>
            </div>

            <div class="form-group">
                <label class="form-label">Starting Capital ($)</label>
                <input type="number" id="bt-capital" class="form-control" value="100000" min="1000" step="10000" style="max-width: 250px;">
            </div>

            <div class="page-divider"></div>

            <!-- Custom Strategies Section -->
            <h2 class="section-title">Custom Strategies</h2>
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Upload a custom strategy (.py)</label>
                    <input type="file" id="custom-strategy-file" accept=".py" class="form-control"
                           onchange="Backtest.uploadCustomStrategy()">
                </div>
                <div class="form-group" style="flex: 0;">
                    <label class="form-label">&nbsp;</label>
                    <a href="/api/custom_strategies/template" download="custom_strategy_template.py"
                       class="btn btn-sm">Download Strategy Template</a>
                </div>
            </div>
            <div id="upload-status"></div>

            ${App.accordionHTML("strategy-guidelines", "Strategy Guidelines & Requirements",
                '<div id="guidelines-content" style="font-size: 0.85rem; color: var(--text-secondary);">Loading...</div>'
            )}

            ${customListHTML}

            <div class="page-divider"></div>

            <!-- Strategy Selection -->
            <h2 class="section-title">Select Strategies</h2>
            ${stratCheckboxes}

            <div class="page-divider"></div>
            <h2 class="section-title">Strategy Parameters</h2>
            <div id="strategy-params"></div>

            <div class="page-divider"></div>
            <button class="btn btn-primary btn-block" onclick="Backtest.runBacktest()" id="run-backtest-btn">
                &#128640; Run Backtest
            </button>

            <div id="backtest-progress" style="margin-top: 16px;"></div>
        `;

        this.updateStrategySelection();
        this.loadGuidelines();
    },

    toggleInstrumentDates(sid) {
        const cb = document.getElementById(`inst-${sid}`);
        const dates = document.getElementById(`inst-dates-${sid}`);
        if (dates) {
            dates.style.display = cb && cb.checked ? "block" : "none";
        }
    },

    /** Inline-bound on every oninput — we install the debounced wrapper at the bottom of the file. */
    _filterInstrumentsImpl() {
        const searchEl = document.getElementById("inst-search");
        if (!searchEl) return;
        const query = (searchEl.value || "").toLowerCase();
        const items = document.querySelectorAll(".instrument-item");
        items.forEach(item => {
            const label = item.textContent.toLowerCase();
            item.style.display = (!query || label.includes(query)) ? "" : "none";
        });
    },

    selectAllInstruments(select) {
        const checkboxes = document.querySelectorAll('[id^="inst-"]');
        checkboxes.forEach(cb => {
            if (cb.type === "checkbox" && cb.id.startsWith("inst-") && !cb.id.includes("from") && !cb.id.includes("to") && !cb.id.includes("dates") && !cb.id.includes("search")) {
                cb.checked = select;
                const sid = cb.id.replace("inst-", "");
                this.toggleInstrumentDates(sid);
            }
        });
    },

    getSelectedInstruments() {
        const instruments = [];
        const checked = [];
        for (const bt of (this.filteredBarTypes || this.barTypes)) {
            const sid = this.safeId(bt);
            const cb = document.getElementById(`inst-${sid}`);
            if (cb && cb.checked) {
                const fromEl = document.getElementById(`inst-from-${sid}`);
                const toEl = document.getElementById(`inst-to-${sid}`);
                checked.push({
                    bar_type: bt,
                    instrument_id: bt.split("-")[0],
                    start_date: fromEl ? fromEl.value : "",
                    end_date: toEl ? toEl.value : "",
                });
            }
        }

        // Group by instrument_id: if multiple bar types share the same instrument,
        // send them as bar_types array (e.g., BID + ASK for same forex pair)
        const groups = {};
        for (const item of checked) {
            const key = item.instrument_id;
            if (!groups[key]) groups[key] = [];
            groups[key].push(item);
        }

        for (const items of Object.values(groups)) {
            if (items.length === 1) {
                // Single bar type — backward compatible
                instruments.push({
                    bar_type: items[0].bar_type,
                    start_date: items[0].start_date,
                    end_date: items[0].end_date,
                });
            } else {
                // Multiple bar types for same instrument — send as list
                instruments.push({
                    bar_types: items.map(i => i.bar_type),
                    start_date: items[0].start_date,
                    end_date: items[0].end_date,
                });
            }
        }
        return instruments;
    },

    async loadGuidelines() {
        try {
            const data = await App.api("/api/custom_strategies/guidelines");
            const el = document.getElementById("guidelines-content");
            if (el) {
                let md = data.guidelines;
                md = md.replace(/^### (.+)$/gm, '<h4 style="margin: 14px 0 6px; color: var(--text-primary);">$1</h4>');
                md = md.replace(/^## (.+)$/gm, '<h3 style="margin: 18px 0 8px; color: var(--text-primary);">$1</h3>');
                md = md.replace(/```python\n([\s\S]*?)```/g, '<pre style="background: var(--bg-input); padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.82rem;">$1</pre>');
                md = md.replace(/```\n?([\s\S]*?)```/g, '<pre style="background: var(--bg-input); padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.82rem;">$1</pre>');
                md = md.replace(/`([^`]+)`/g, '<code style="background: var(--bg-input); padding: 2px 4px; border-radius: 3px;">$1</code>');
                md = md.replace(/\*\*([^*]+)\*\*/g, '<strong>$1</strong>');
                md = md.replace(/^---$/gm, '<hr style="border-color: var(--border-color); margin: 12px 0;">');
                md = md.replace(/^\| (.+)/gm, (match) => {
                    const cells = match.split('|').filter(c => c.trim()).map(c => `<td style="padding: 4px 8px; border: 1px solid var(--border-color);">${c.trim()}</td>`);
                    return `<tr>${cells.join('')}</tr>`;
                });
                md = md.replace(/\n/g, '<br>');
                el.innerHTML = md;
            }
        } catch (e) {
            const el = document.getElementById("guidelines-content");
            if (el) el.textContent = "Failed to load guidelines.";
        }
    },

    async uploadCustomStrategy() {
        const input = document.getElementById("custom-strategy-file");
        const statusDiv = document.getElementById("upload-status");

        if (!input.files || input.files.length === 0) return;

        const formData = new FormData();
        formData.append("file", input.files[0]);

        statusDiv.innerHTML = '<div class="alert alert-info">Uploading and validating...</div>';

        try {
            const response = await fetch("/api/custom_strategies/upload", {
                method: "POST",
                body: formData,
            });

            let data;
            const text = await response.text();
            try {
                data = JSON.parse(text);
            } catch {
                statusDiv.innerHTML = `<div class="alert alert-danger">Server error: ${text.substring(0, 500)}</div>`;
                return;
            }

            if (response.ok && data.success) {
                statusDiv.innerHTML = `<div class="alert alert-success">${data.message}</div>`;
                App.toast(data.message, "success");
                input.value = "";
                const saved = this.saveFormState();
                await this.loadConfig();
                this.restoreFormState(saved);
                this.showPreviousResults();
            } else {
                statusDiv.innerHTML = `<div class="alert alert-danger">${data.error || "Upload failed"}</div>`;
            }
        } catch (e) {
            statusDiv.innerHTML = `<div class="alert alert-danger">Upload failed: ${e.message}</div>`;
        }
    },

    async deleteCustomStrategy(filename) {
        if (!confirm(`Delete custom strategy file "${filename}"?`)) return;

        try {
            const data = await App.api("/api/custom_strategies/delete", {
                method: "POST",
                body: JSON.stringify({ filename }),
            });
            App.toast(data.message, "success");
            const saved = this.saveFormState();
            await this.loadConfig();
            this.restoreFormState(saved);
            this.showPreviousResults();
        } catch (e) {
            App.toast("Delete failed: " + e.message, "error");
        }
    },

    /** Save current form values so they survive a full re-render. */
    saveFormState() {
        const state = {};
        const capEl = document.getElementById("bt-capital");
        if (capEl) state.capital = capEl.value;

        // Which instruments are checked + their date ranges
        state.instruments = [];
        for (const bt of (this.filteredBarTypes || this.barTypes)) {
            const sid = this.safeId(bt);
            const cb = document.getElementById(`inst-${sid}`);
            if (cb && cb.checked) {
                const fromEl = document.getElementById(`inst-from-${sid}`);
                const toEl = document.getElementById(`inst-to-${sid}`);
                state.instruments.push({
                    bar_type: bt,
                    start_date: fromEl ? fromEl.value : "",
                    end_date: toEl ? toEl.value : "",
                });
            }
        }

        // Which strategies are checked
        state.checkedStrategies = [];
        document.querySelectorAll('[id^="strat-"]').forEach(cb => {
            if (cb.checked) state.checkedStrategies.push(cb.value);
        });
        // Strategy param values
        state.params = {};
        document.querySelectorAll('[id^="param-"]').forEach(el => {
            state.params[el.id] = el.type === "checkbox" ? el.checked : el.value;
        });
        document.querySelectorAll('[id^="ts-"]').forEach(el => {
            state.params[el.id] = el.value;
        });
        return state;
    },

    /** Restore form values after a re-render. */
    restoreFormState(state) {
        if (!state) return;
        const capEl = document.getElementById("bt-capital");
        if (capEl && state.capital) capEl.value = state.capital;

        // Restore instrument selections + date ranges
        if (state.instruments) {
            for (const inst of state.instruments) {
                const sid = this.safeId(inst.bar_type);
                const cb = document.getElementById(`inst-${sid}`);
                if (cb) {
                    cb.checked = true;
                    this.toggleInstrumentDates(sid);
                    const fromEl = document.getElementById(`inst-from-${sid}`);
                    const toEl = document.getElementById(`inst-to-${sid}`);
                    if (fromEl && inst.start_date) fromEl.value = inst.start_date;
                    if (toEl && inst.end_date) toEl.value = inst.end_date;
                }
            }
        }

        // Restore strategy checkboxes
        if (state.checkedStrategies) {
            document.querySelectorAll('[id^="strat-"]').forEach(cb => {
                cb.checked = state.checkedStrategies.includes(cb.value);
            });
            this.updateStrategySelection();
        }
        // Restore param and trade-size values
        if (state.params) {
            for (const [id, val] of Object.entries(state.params)) {
                const el = document.getElementById(id);
                if (!el) continue;
                if (el.type === "checkbox") el.checked = val;
                else el.value = val;
            }
        }
    },

    safeId(name) {
        return name.replace(/[^a-zA-Z0-9]/g, "_");
    },

    updateStrategySelection() {
        const checkboxes = document.querySelectorAll('[id^="strat-"]');
        this.selectedStrategies = [];
        checkboxes.forEach(cb => {
            if (cb.checked) this.selectedStrategies.push(cb.value);
        });

        this.renderParams();
    },

    renderParams() {
        const paramsDiv = document.getElementById("strategy-params");
        if (!paramsDiv) return;

        if (this.selectedStrategies.length === 0) {
            paramsDiv.innerHTML = '<div class="alert alert-info">Select at least one strategy.</div>';
            return;
        }

        let html = "";
        for (const stratName of this.selectedStrategies) {
            const strat = this.strategies[stratName];
            const params = strat.params;

            let paramsHTML = `
                <div class="form-group" style="min-width: 140px;">
                    <label class="form-label">Trade Size (units)</label>
                    <input type="number" class="form-control" id="ts-${this.safeId(stratName)}" value="1" min="1" step="1">
                </div>
            `;

            for (const [key, info] of Object.entries(params)) {
                const id = `param-${this.safeId(stratName)}-${key}`;
                if (typeof info.default === "boolean") {
                    paramsHTML += `
                        <div class="form-group" style="min-width: 140px;">
                            <label class="form-label">${info.label}</label>
                            <div class="checkbox-row">
                                <input type="checkbox" id="${id}" ${info.default ? "checked" : ""}>
                                <label for="${id}">Enabled</label>
                            </div>
                        </div>`;
                } else {
                    paramsHTML += `
                        <div class="form-group" style="min-width: 140px;">
                            <label class="form-label">${info.label}</label>
                            <input type="number" class="form-control" id="${id}"
                                   value="${info.default}"
                                   ${info.min !== undefined ? `min="${info.min}"` : ""}
                                   ${info.max !== undefined ? `max="${info.max}"` : ""}
                                   step="${typeof info.default === 'number' && !Number.isInteger(info.default) ? '0.5' : '1'}">
                        </div>`;
                }
            }

            html += `
                <div class="strategy-config">
                    <div class="strategy-config-title">${stratName}</div>
                    <div class="strategy-config-desc">${strat.description}</div>
                    <div class="params-grid">${paramsHTML}</div>
                </div>`;
        }

        paramsDiv.innerHTML = html;
    },

    collectStrategiesConfig() {
        const strategies = {};
        for (const stratName of this.selectedStrategies) {
            const strat = this.strategies[stratName];
            const params = {};

            for (const [key, info] of Object.entries(strat.params)) {
                const id = `param-${this.safeId(stratName)}-${key}`;
                const el = document.getElementById(id);
                if (typeof info.default === "boolean") {
                    params[key] = el.checked;
                } else if (typeof info.default === "number" && !Number.isInteger(info.default)) {
                    params[key] = parseFloat(el.value);
                } else {
                    params[key] = parseInt(el.value);
                }
            }

            const tradeSize = parseInt(document.getElementById(`ts-${this.safeId(stratName)}`).value) || 1;

            strategies[stratName] = { params, trade_size: tradeSize };
        }
        return strategies;
    },

    async runBacktest() {
        const instruments = this.getSelectedInstruments();
        if (instruments.length === 0) {
            App.toast("Select at least one instrument.", "error");
            return;
        }
        if (this.selectedStrategies.length === 0) {
            App.toast("Select at least one strategy.", "error");
            return;
        }

        const capital = parseFloat(document.getElementById("bt-capital").value);
        const strategies = this.collectStrategiesConfig();

        const btn = document.getElementById("run-backtest-btn");
        btn.disabled = true;
        btn.textContent = "Running...";

        const progressDiv = document.getElementById("backtest-progress");
        progressDiv.innerHTML = `
            <div class="progress-text" id="progress-text">Initializing backtests...</div>
            <div class="progress-bar-container">
                <div class="progress-bar-fill" id="progress-bar" style="width: 0%; transition: width 0.3s ease;"></div>
            </div>
            <div id="progress-details" style="margin-top: 8px; font-size: 0.85rem; color: var(--text-secondary);"></div>
        `;

        try {
            const response = await fetch("/api/backtest/run-stream", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({
                    instruments,
                    starting_capital: capital,
                    strategies,
                }),
            });

            if (!response.ok) {
                throw new Error(`HTTP ${response.status}: ${response.statusText}`);
            }

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "";
            let finalResults = null;
            let finalErrors = [];

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n");
                buffer = lines.pop();

                for (const line of lines) {
                    if (!line.trim()) continue;
                    try {
                        const evt = JSON.parse(line);

                        if (evt.event === "start") {
                            const el = document.getElementById("progress-text");
                            if (el) el.textContent = `Running ${evt.total} backtest(s) (${evt.instruments} instrument(s) \u00d7 ${evt.strategies} strategies)...`;

                        } else if (evt.event === "progress") {
                            const pct = evt.total > 0 ? Math.round((evt.completed / evt.total) * 100) : 0;
                            const bar = document.getElementById("progress-bar");
                            const details = document.getElementById("progress-details");

                            if (evt.status === "running") {
                                if (bar) bar.style.width = pct + "%";
                                if (details) details.textContent = `Running: ${evt.current_strategy} on ${evt.current_instrument} (${evt.completed}/${evt.total})`;
                            } else if (evt.status === "complete") {
                                if (bar) bar.style.width = pct + "%";
                                if (details) details.textContent = `Completed: ${evt.current_strategy} on ${evt.current_instrument} in ${evt.elapsed}s (${evt.completed}/${evt.total})`;
                            } else if (evt.status === "error") {
                                if (bar) bar.style.width = pct + "%";
                                if (details) details.innerHTML += `<div style="color: var(--error);">Error: ${evt.current_strategy} on ${evt.current_instrument}: ${evt.error}</div>`;
                                finalErrors.push({ instrument: evt.current_instrument, strategy: evt.current_strategy, error: evt.error });
                            }

                        } else if (evt.event === "complete") {
                            finalResults = evt.instrument_results;
                            finalErrors = finalErrors.concat(evt.errors || []);
                        }
                    } catch (parseErr) {
                        console.error("Failed to parse stream event:", line, parseErr);
                    }
                }
            }

            // Finalize
            btn.disabled = false;
            btn.textContent = "\u{1F680} Run Backtest";

            if (finalResults) {
                progressDiv.innerHTML = '<div class="progress-text" style="color: var(--success);">All backtests complete!</div>';

                App.state.backtestResults = finalResults;
                App.state.backtestConfig = {
                    instruments: instruments.map(i => i.bar_type || (i.bar_types && i.bar_types[0]) || ""),
                    strategies: this.selectedStrategies,
                };
                this.lastInstrumentResults = finalResults;

                for (const err of finalErrors) {
                    const label = err.instrument ? `${err.instrument} / ${err.strategy}` : err.strategy;
                    App.toast(`${label} failed: ${err.error}`, "error", 6000);
                }

                const hasResults = Object.values(finalResults).some(
                    ir => Object.keys(ir.strategies).length > 0
                );
                if (hasResults) {
                    App.toast("Backtests completed! CSV reports saved to reports/ folder.", "success");
                    this.renderResults(finalResults);
                }
            } else {
                progressDiv.innerHTML = '<div class="alert alert-danger">No results received from server.</div>';
            }

        } catch (e) {
            progressDiv.innerHTML = `<div class="alert alert-danger">Backtest failed: ${e.message || e}</div>`;
            btn.disabled = false;
            btn.textContent = "\u{1F680} Run Backtest";
        }
    },

    showPreviousResults() {
        if (App.state.backtestResults) {
            this.lastInstrumentResults = App.state.backtestResults;
            this.renderResults(App.state.backtestResults);
        }
    },

    /** Trigger download of a string as a file */
    downloadFile(content, filename, mimeType) {
        const blob = new Blob([content], { type: mimeType });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url;
        a.download = filename;
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    },

    /** Convert array of objects to CSV string */
    arrayToCSV(data) {
        if (!data || data.length === 0) return "";
        const headers = Object.keys(data[0]);
        const rows = data.map(row => headers.map(h => {
            let val = row[h] ?? "";
            val = String(val);
            if (val.includes(",") || val.includes('"') || val.includes("\n")) {
                val = '"' + val.replace(/"/g, '""') + '"';
            }
            return val;
        }).join(","));
        return [headers.join(","), ...rows].join("\n");
    },

    /** Get short instrument label from bar type string */
    instLabel(barType) {
        return barType.split("-")[0];  // e.g. "BTCUSD.BINANCE" from "BTCUSD.BINANCE-1-DAY-LAST-EXTERNAL"
    },

    /**
     * Flag trade-count outliers that suggest a strategy param is
     * price-magnitude sensitive across instruments (e.g. Decoy firing
     * 22k times on USDJPY minute data but 0 on EURUSD with identical
     * params). Two checks:
     *   1. Cross-instrument ratio: max/min trade count per strategy > 10x
     *   2. Absolute volume: any single run > 10,000 trades
     */
    _computeSanityWarnings(instrumentResults) {
        const warnings = [];
        const HIGH_TRADE_THRESHOLD = 10000;
        const RATIO_THRESHOLD = 10;

        // Build strategy -> [(instrument, trades), ...]
        const perStrategy = {};
        for (const [bt, ir] of Object.entries(instrumentResults)) {
            const inst = this.instLabel(bt);
            for (const [name, r] of Object.entries(ir.strategies || {})) {
                if (!perStrategy[name]) perStrategy[name] = [];
                perStrategy[name].push({ inst, trades: r.total_trades || 0 });
            }
        }

        // Check 1: cross-instrument ratio per strategy
        for (const [name, rows] of Object.entries(perStrategy)) {
            if (rows.length < 2) continue;
            const nonZero = rows.filter(r => r.trades > 0);
            if (nonZero.length < 2) continue;
            const sorted = [...nonZero].sort((a, b) => a.trades - b.trades);
            const lo = sorted[0], hi = sorted[sorted.length - 1];
            if (hi.trades / lo.trades >= RATIO_THRESHOLD) {
                warnings.push(
                    `<strong>${name}</strong>: trade count varies ${Math.round(hi.trades / lo.trades)}&times; across instruments ` +
                    `(${hi.inst}: ${hi.trades.toLocaleString()} vs ${lo.inst}: ${lo.trades.toLocaleString()}) ` +
                    `&mdash; likely a price-magnitude-sensitive threshold. Consider percent-of-price or ATR-based params.`
                );
            }
        }

        // Check 2: absolute high-volume flag
        for (const [bt, ir] of Object.entries(instrumentResults)) {
            const inst = this.instLabel(bt);
            for (const [name, r] of Object.entries(ir.strategies || {})) {
                if ((r.total_trades || 0) > HIGH_TRADE_THRESHOLD) {
                    warnings.push(
                        `<strong>${name}</strong> on <strong>${inst}</strong>: ` +
                        `${r.total_trades.toLocaleString()} trades exceeds the ${HIGH_TRADE_THRESHOLD.toLocaleString()} sanity threshold ` +
                        `&mdash; transaction cost impact will dominate PnL; review params before trusting results.`
                    );
                }
            }
        }

        return warnings;
    },

    renderResults(instrumentResults) {
        const section = document.getElementById("backtest-results-section");
        let html = '<div class="page-divider"></div><h2 class="section-title">Backtest Results</h2>';

        // Trade-count sanity warnings surface at the top of the results so
        // they're impossible to miss when scanning the comparison tables.
        const sanityWarnings = this._computeSanityWarnings(instrumentResults);
        if (sanityWarnings.length > 0) {
            html += `
                <div class="alert alert-warning" style="margin-bottom: 20px;">
                    <strong>&#9888; Trade-Count Sanity Warnings (${sanityWarnings.length})</strong>
                    <ul style="margin: 8px 0 0 22px; padding: 0;">
                        ${sanityWarnings.map(w => `<li style="margin-bottom: 4px;">${w}</li>`).join("")}
                    </ul>
                </div>`;
        }

        for (const [barType, instData] of Object.entries(instrumentResults)) {
            const strategies = instData.strategies;
            const stratNames = Object.keys(strategies);
            if (stratNames.length === 0) continue;

            const label = this.instLabel(barType);
            const dateRange = instData.date_range;
            const dateStr = (dateRange.start || dateRange.end)
                ? `${dateRange.start || "start"} &rarr; ${dateRange.end || "end"}`
                : "all data";

            // Comparison table for this instrument
            const rows = stratNames.map(name => {
                const r = strategies[name];
                const pnlClass = r.total_pnl >= 0 ? "positive" : "negative";
                return `<tr>
                    <td><strong>${name}</strong></td>
                    <td>${App.currency(r.starting_capital)}</td>
                    <td>${App.currency(r.final_balance)}</td>
                    <td class="${pnlClass}">${App.currency(r.total_pnl)}</td>
                    <td class="${pnlClass}">${r.total_return_pct >= 0 ? "+" : ""}${r.total_return_pct.toFixed(2)}%</td>
                    <td>${r.total_trades}</td>
                    <td>${r.win_rate.toFixed(1)}%</td>
                    <td>${r.wins}</td>
                    <td>${r.losses}</td>
                </tr>`;
            }).join("");

            // HTML Report download button
            const safeBarType = this.safeId(barType);
            let reportBtnHTML = "";
            if (instData.report_html) {
                reportBtnHTML = `
                    <button class="btn btn-primary" onclick="Backtest.downloadFile(Backtest.lastInstrumentResults['${barType}'].report_html, '${instData.report_name || label}_report.html', 'text/html')">
                        &#128196; Download HTML Report for ${label}
                    </button>`;
            }

            // Per-strategy detail accordions
            let detailsHTML = "";
            for (const name of stratNames) {
                const r = strategies[name];
                const pnlDelta = r.total_return_pct;
                const safeName = this.safeId(name);
                const comboId = `${safeBarType}_${safeName}`;

                let innerHTML = `
                    <div class="grid-6">
                        ${App.metricHTML("Starting Capital", App.currency(r.starting_capital))}
                        ${App.metricHTML("Final Balance", App.currency(r.final_balance))}
                        ${App.metricHTML("Total P&L", App.currency(r.total_pnl), pnlDelta)}
                        ${App.metricHTML("Total Trades", r.total_trades)}
                        ${App.metricHTML("Win Rate", r.win_rate.toFixed(1) + "%")}
                        ${App.metricHTML("Wins / Losses", `${r.wins} / ${r.losses}`)}
                    </div>
                `;

                // CSV Download buttons
                let downloadBtns = '<div style="margin-top: 16px; display: flex; flex-wrap: wrap; gap: 8px;">';
                if (r.positions_report && r.positions_report.length > 0) {
                    downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(Backtest.lastInstrumentResults['${barType}'].strategies['${name}'].positions_report), 'position_report_${comboId}.csv', 'text/csv')">&#128190; Positions CSV</button>`;
                }
                if (r.fills_report && r.fills_report.length > 0) {
                    downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(Backtest.lastInstrumentResults['${barType}'].strategies['${name}'].fills_report), 'order_fill_report_${comboId}.csv', 'text/csv')">&#128190; Order Fills CSV</button>`;
                }
                if (r.account_report && r.account_report.length > 0) {
                    downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(Backtest.lastInstrumentResults['${barType}'].strategies['${name}'].account_report), 'account_report_${comboId}.csv', 'text/csv')">&#128190; Account CSV</button>`;
                }
                if (r.order_book && r.order_book.length > 0) {
                    downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(Backtest.lastInstrumentResults['${barType}'].strategies['${name}'].order_book), 'order_book_${comboId}.csv', 'text/csv')">&#128190; Order Book CSV</button>`;
                }
                if (r.logs && r.logs.length > 0) {
                    downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(Backtest.lastInstrumentResults['${barType}'].strategies['${name}'].logs), 'backtest_${comboId}_logs.csv', 'text/csv')">&#128190; Logs CSV</button>`;
                }
                downloadBtns += '</div>';
                innerHTML += downloadBtns;

                // Reports tables — rendered lazily. Large logs/fills can run to
                // tens of thousands of rows; building them all into innerHTML on
                // every results render locks the UI for seconds. App.lazyTableHTML
                // emits a placeholder with a "Click to render" link.
                const lazyKey = (kind) => `bt-${comboId}-${kind}`;
                if (r.positions_report && r.positions_report.length > 0) {
                    innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Positions Report</h3>`
                              + App.lazyTableHTML(lazyKey("positions"), r.positions_report);
                }
                if (r.fills_report && r.fills_report.length > 0) {
                    innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Order Fills Report</h3>`
                              + App.lazyTableHTML(lazyKey("fills"), r.fills_report);
                }
                if (r.account_report && r.account_report.length > 0) {
                    innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Account Report</h3>`
                              + App.lazyTableHTML(lazyKey("account"), r.account_report);
                }
                if (r.order_book && r.order_book.length > 0) {
                    innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Order Book</h3>`
                              + App.lazyTableHTML(lazyKey("orderbook"), r.order_book);
                }
                if (r.logs && r.logs.length > 0) {
                    innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Trading Logs</h3>`
                              + App.lazyTableHTML(lazyKey("logs"), r.logs);
                }

                detailsHTML += App.accordionHTML(`detail-${comboId}`, `${name} -- Detailed Reports`, innerHTML);
            }

            html += `
                <div class="card" style="margin-bottom: 24px; padding: 20px;">
                    <h3 style="margin: 0 0 4px; color: var(--accent);">${label}</h3>
                    <p class="section-caption" style="margin: 0 0 16px;">Date range: ${dateStr} | Strategies: ${stratNames.join(", ")}</p>

                    <div class="table-container comparison-table">
                        <table>
                            <thead>
                                <tr>
                                    <th>Strategy</th><th>Starting Capital</th><th>Final Balance</th>
                                    <th>Total P&L</th><th>Return %</th><th>Trades</th>
                                    <th>Win Rate</th><th>Wins</th><th>Losses</th>
                                </tr>
                            </thead>
                            <tbody>${rows}</tbody>
                        </table>
                    </div>

                    ${reportBtnHTML ? `<div style="margin-top: 16px;">${reportBtnHTML}</div>` : ""}

                    <div style="margin-top: 20px;">${detailsHTML}</div>
                </div>
            `;
        }

        section.innerHTML = html;
    },
};

// Debounce the instrument search — inline oninput calls Backtest.filterInstruments().
Backtest.filterInstruments = App.debounce(function () {
    Backtest._filterInstrumentsImpl();
}, 120);
