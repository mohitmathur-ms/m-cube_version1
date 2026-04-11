/**
 * Backtest Page - Configure and run trading strategies.
 * Includes: custom strategy upload/manage, HTML report download,
 * order book, logs, and all CSV report types.
 */

const Backtest = {
    strategies: {},
    barTypes: [],
    selectedStrategies: [],
    lastReportHTML: "",
    lastReportName: "",

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#129514; Run Backtest</h1>
            <p class="page-subtitle">Test trading strategies on your downloaded crypto data.</p>

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
        const barOptions = this.barTypes.map(bt => `<option value="${bt}">${bt}</option>`).join("");
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

        document.getElementById("backtest-setup").innerHTML = `
            <h2 class="section-title">Configuration</h2>
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Select Instrument / Bar Type</label>
                    <select id="bt-bar-type" class="form-control">${barOptions}</select>
                </div>
                <div class="form-group">
                    <label class="form-label">Starting Capital ($)</label>
                    <input type="number" id="bt-capital" class="form-control" value="100000" min="1000" step="10000">
                </div>
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

    async loadGuidelines() {
        try {
            const data = await App.api("/api/custom_strategies/guidelines");
            const el = document.getElementById("guidelines-content");
            if (el) {
                // Simple markdown-to-HTML: headings, bold, code blocks, tables
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
                // Reload config to refresh strategy list
                input.value = "";
                await this.loadConfig();
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
            await this.loadConfig();
            this.showPreviousResults();
        } catch (e) {
            App.toast("Delete failed: " + e.message, "error");
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

    collectConfig() {
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
        if (this.selectedStrategies.length === 0) {
            App.toast("Select at least one strategy.", "error");
            return;
        }

        const barType = document.getElementById("bt-bar-type").value;
        const capital = parseFloat(document.getElementById("bt-capital").value);
        const strategies = this.collectConfig();

        const btn = document.getElementById("run-backtest-btn");
        btn.disabled = true;
        btn.textContent = "Running...";

        const progressDiv = document.getElementById("backtest-progress");
        progressDiv.innerHTML = `
            <div class="progress-text">Running ${this.selectedStrategies.length} strategy(ies)...</div>
            <div class="progress-bar-container">
                <div class="progress-bar-fill" style="width: 30%"></div>
            </div>
        `;

        try {
            const data = await App.api("/api/backtest/run", {
                method: "POST",
                body: JSON.stringify({
                    bar_type: barType,
                    starting_capital: capital,
                    strategies,
                }),
            });

            progressDiv.innerHTML = '<div class="progress-text" style="color: var(--success);">All backtests complete!</div>';
            btn.disabled = false;
            btn.textContent = "\u{1F680} Run Backtest";

            // Store results + report
            App.state.backtestResults = data.results;
            App.state.backtestConfig = {
                bar_type: barType,
                strategies: Object.keys(data.results),
            };
            this.lastReportHTML = data.report_html || "";
            this.lastReportName = data.backtest_name || "backtest";

            // Show errors
            for (const err of data.errors) {
                App.toast(`${err.strategy} failed: ${err.error}`, "error", 6000);
            }

            if (Object.keys(data.results).length > 0) {
                App.toast("Backtests completed successfully! CSV reports saved to reports/ folder.", "success");
                this.renderResults(data.results, barType);
            }

        } catch (e) {
            progressDiv.innerHTML = `<div class="alert alert-danger">Backtest failed: ${e.message}</div>`;
            btn.disabled = false;
            btn.textContent = "\u{1F680} Run Backtest";
        }
    },

    showPreviousResults() {
        if (App.state.backtestResults && App.state.backtestConfig) {
            this.renderResults(App.state.backtestResults, App.state.backtestConfig.bar_type);
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

    renderResults(results, barType) {
        const section = document.getElementById("backtest-results-section");
        const names = Object.keys(results);

        // Comparison table
        const rows = names.map(name => {
            const r = results[name];
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
        let reportBtnHTML = "";
        if (this.lastReportHTML) {
            reportBtnHTML = `
                <button class="btn btn-primary" onclick="Backtest.downloadFile(Backtest.lastReportHTML, '${this.lastReportName}_report.html', 'text/html')">
                    &#128196; Download HTML Report
                </button>`;
        }

        // Per-strategy details
        let detailsHTML = "";
        for (const name of names) {
            const r = results[name];
            const pnlDelta = r.total_return_pct;
            const safeName = this.safeId(name);

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
                downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(App.state.backtestResults['${name}'].positions_report), 'position_report_${safeName}.csv', 'text/csv')">&#128190; Positions CSV</button>`;
            }
            if (r.fills_report && r.fills_report.length > 0) {
                downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(App.state.backtestResults['${name}'].fills_report), 'order_fill_report_${safeName}.csv', 'text/csv')">&#128190; Order Fills CSV</button>`;
            }
            if (r.account_report && r.account_report.length > 0) {
                downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(App.state.backtestResults['${name}'].account_report), 'account_report_${safeName}.csv', 'text/csv')">&#128190; Account CSV</button>`;
            }
            if (r.order_book && r.order_book.length > 0) {
                downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(App.state.backtestResults['${name}'].order_book), 'order_book_${safeName}.csv', 'text/csv')">&#128190; Order Book CSV</button>`;
            }
            if (r.logs && r.logs.length > 0) {
                downloadBtns += `<button class="btn btn-sm" onclick="Backtest.downloadFile(Backtest.arrayToCSV(App.state.backtestResults['${name}'].logs), 'backtest_${safeName}_logs.csv', 'text/csv')">&#128190; Logs CSV</button>`;
            }
            downloadBtns += '</div>';
            innerHTML += downloadBtns;

            // Reports tables
            if (r.positions_report && r.positions_report.length > 0) {
                innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Positions Report</h3>` + App.tableHTML(r.positions_report);
            }
            if (r.fills_report && r.fills_report.length > 0) {
                innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Order Fills Report</h3>` + App.tableHTML(r.fills_report);
            }
            if (r.account_report && r.account_report.length > 0) {
                innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Account Report</h3>` + App.tableHTML(r.account_report);
            }
            if (r.order_book && r.order_book.length > 0) {
                innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Order Book</h3>` + App.tableHTML(r.order_book);
            }
            if (r.logs && r.logs.length > 0) {
                innerHTML += `<h3 style="margin-top: 20px; font-size: 0.95rem;">Trading Logs</h3>` + App.tableHTML(r.logs);
            }

            detailsHTML += App.accordionHTML(`detail-${safeName}`, `${name} — Detailed Reports`, innerHTML);
        }

        section.innerHTML = `
            <div class="page-divider"></div>
            <h2 class="section-title">Backtest Results</h2>
            <p class="section-caption">Instrument: <strong>${barType}</strong> | Strategies: <strong>${names.join(", ")}</strong></p>

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
        `;
    },
};
