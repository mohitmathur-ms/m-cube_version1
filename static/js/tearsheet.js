/**
 * Tearsheet Page - Detailed performance analytics from backtest results.
 * Supports multi-instrument results with instrument + strategy selectors.
 */

const Tearsheet = {
    COLORS: ["#00d4aa", "#ff6b6b", "#4ecdc4", "#ffe66d", "#a29bfe", "#fd79a8"],

    /** Re-render only when the underlying backtest results object reference
     *  has changed (i.e. a new backtest finished). Same reference → keep the
     *  existing DOM so the user's instrument + strategy selection survives a
     *  trip to another tab and back. */
    onShow(container, { firstVisit }) {
        const cur = App.state.backtestResults;
        if (firstVisit) {
            this._lastRenderedResults = cur;
            return;
        }
        if (cur !== this._lastRenderedResults) {
            this._lastRenderedResults = cur;
            this.render(container);
        }
    },

    render(container) {
        this._lastRenderedResults = App.state.backtestResults;
        if (!App.state.backtestResults) {
            container.innerHTML = `
                <h1 class="page-title">&#128200; Performance Tearsheet</h1>
                <p class="page-subtitle">Detailed performance analytics from your backtest results.</p>
                <div class="empty-state">
                    <div class="empty-state-icon">&#128200;</div>
                    <div class="empty-state-text">No backtest results found. Go to <strong>Run Backtest</strong> and run a strategy first.</div>
                </div>
            `;
            return;
        }

        const instrumentResults = App.state.backtestResults;
        const instruments = Object.keys(instrumentResults);

        // Build instrument options
        const instOptions = instruments.map(bt => {
            const label = App.barTypeLabel(bt);
            return `<option value="${bt}">${label}</option>`;
        }).join("");

        container.innerHTML = `
            <h1 class="page-title">&#128200; Performance Tearsheet</h1>
            <p class="page-subtitle">Detailed performance analytics from your backtest results.</p>

            <div class="form-row" style="gap: 16px;">
                <div class="form-group" style="flex: 1;">
                    <label class="form-label">Select Instrument</label>
                    <select id="ts-instrument-select" class="form-control" onchange="Tearsheet.onInstrumentChange()">
                        ${instOptions}
                    </select>
                </div>
                <div class="form-group" style="flex: 1;">
                    <label class="form-label">Select Strategy for Detail</label>
                    <select id="ts-strategy-select" class="form-control" onchange="Tearsheet.renderDetail()">
                    </select>
                </div>
            </div>

            <h2 class="section-title" id="ts-equity-title">Equity Comparison</h2>
            <div id="equity-compare-chart" class="chart-container"></div>

            <div class="page-divider"></div>

            <div id="tearsheet-detail"></div>
        `;

        this.onInstrumentChange();
    },

    /** When instrument selection changes, update strategy dropdown and charts */
    onInstrumentChange() {
        const instrumentResults = App.state.backtestResults;
        const barType = document.getElementById("ts-instrument-select").value;
        const instData = instrumentResults[barType];

        if (!instData || !instData.strategies) return;

        const stratNames = Object.keys(instData.strategies);
        const stratSelect = document.getElementById("ts-strategy-select");
        stratSelect.innerHTML = stratNames.map(n => `<option value="${n}">${n}</option>`).join("");

        const label = App.barTypeLabel(barType);
        const titleEl = document.getElementById("ts-equity-title");
        if (titleEl) titleEl.textContent = `Equity Comparison | ${label}`;

        this.renderEquityComparison(instData.strategies, stratNames);
        this.renderDetail();
    },

    /** Extract PnL series from positions report */
    extractPnl(positionsReport) {
        if (!positionsReport || positionsReport.length === 0) return null;

        const cols = Object.keys(positionsReport[0]);
        let pnlCol = cols.find(c => c.toLowerCase().includes("pnl") || c.toLowerCase().includes("realized"));
        if (!pnlCol) return null;

        const values = positionsReport.map(row => {
            const val = row[pnlCol];
            if (typeof val === "number") return val;
            const str = String(val).split(" ")[0];
            return parseFloat(str) || 0;
        });

        return values;
    },

    renderEquityComparison(strategies, names) {
        const traces = [];
        let hasData = false;
        let hasTimestamps = false;

        names.forEach((name, i) => {
            const r = strategies[name];
            const color = this.COLORS[i % this.COLORS.length];

            // Prefer timestamped equity curve
            const curvePts = r.equity_curve_ts || [];
            if (curvePts.length > 0 && curvePts[0].timestamp) {
                hasData = true;
                hasTimestamps = true;
                traces.push({
                    x: curvePts.map(pt => pt.timestamp),
                    y: curvePts.map(pt => pt.balance),
                    mode: "lines",
                    name: name,
                    line: { color, width: 2 },
                });
                return;
            }

            // Fallback to PnL-based equity
            const pnlValues = this.extractPnl(r.positions_report);
            if (!pnlValues) return;

            hasData = true;
            let cum = 0;
            const equity = pnlValues.map(v => { cum += v; return r.starting_capital + cum; });

            traces.push({
                y: equity,
                mode: "lines",
                name: name,
                line: { color, width: 2 },
            });
        });

        if (!hasData) {
            document.getElementById("equity-compare-chart").innerHTML =
                '<div class="alert alert-info" style="margin: 20px;">No position data available for equity comparison.</div>';
            return;
        }

        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#ffffff",
            plot_bgcolor: "#ffffff",
            height: 500,
            font: { color: "#1a1a1a" },
            margin: { l: 60, r: 30, t: 30, b: 50 },
            yaxis: { title: "Balance ($)" },
            xaxis: { title: hasTimestamps ? "Date" : "Trade #", type: hasTimestamps ? "date" : "linear" },
            legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 },
        };

        App.plot("equity-compare-chart", traces, darkLayout);
    },

    renderDetail() {
        const instrumentResults = App.state.backtestResults;
        const barType = document.getElementById("ts-instrument-select").value;
        const stratName = document.getElementById("ts-strategy-select").value;
        const detail = document.getElementById("tearsheet-detail");

        if (!barType || !stratName) {
            detail.innerHTML = '<div class="alert alert-warning">Select an instrument and strategy.</div>';
            return;
        }

        const instData = instrumentResults[barType];
        const r = instData && instData.strategies ? instData.strategies[stratName] : null;

        if (!r) {
            detail.innerHTML = '<div class="alert alert-warning">No results for selected combination.</div>';
            return;
        }

        const label = App.barTypeLabel(barType);

        // Summary
        let html = `
            <h2 class="section-title">${label} / ${stratName} -- Detailed Tearsheet</h2>
            <h3 style="margin: 16px 0 12px;">Performance Summary</h3>
            <div class="grid-4">
                ${App.metricHTML("Total Return", `${r.total_return_pct >= 0 ? "+" : ""}${r.total_return_pct.toFixed(2)}%`)}
                ${App.metricHTML("Total P&L", App.currency(r.total_pnl))}
                ${App.metricHTML("Win Rate", `${r.win_rate.toFixed(1)}%`)}
                ${App.metricHTML("Total Trades", r.total_trades)}
            </div>
            <div class="page-divider"></div>
        `;

        const pnlValues = this.extractPnl(r.positions_report);

        // Build equity from timestamped curve or fallback to PnL
        const curvePts = r.equity_curve_ts || [];
        const hasTimestamps = curvePts.length > 0 && curvePts[0].timestamp;
        const hasEquityData = hasTimestamps || (pnlValues && pnlValues.length > 0);

        if (hasEquityData) {
            html += `
                <h3 style="margin-bottom: 16px;">Equity & Drawdown Analysis</h3>
                <div id="ts-equity-chart" class="chart-container"></div>

                <h3 style="margin: 24px 0 16px;">Trade Distribution</h3>
                <div class="grid-2">
                    <div id="ts-pnl-chart" class="chart-container"></div>
                    <div id="ts-pie-chart" class="chart-container"></div>
                </div>
            `;

            let equity, timestamps, drawdown;
            if (hasTimestamps) {
                timestamps = curvePts.map(pt => pt.timestamp);
                equity = curvePts.map(pt => pt.balance);
            } else {
                timestamps = null;
                let cum = 0;
                equity = pnlValues.map(v => { cum += v; return r.starting_capital + cum; });
            }

            // Drawdown — single pass, no allocation of a spread copy.
            let peak = equity[0];
            let maxDD = 0;
            drawdown = new Array(equity.length);
            for (let i = 0; i < equity.length; i++) {
                const v = equity[i];
                if (v > peak) peak = v;
                const dd = peak > 0 ? ((v - peak) / peak) * 100 : 0;
                drawdown[i] = dd;
                if (dd < maxDD) maxDD = dd;
            }

            // Trade statistics (still from pnlValues). Single-pass min/max/avg
            // — Math.max(...arr) blows the stack past ~100k trades.
            const tradeVals = pnlValues || [];
            let winSum = 0, winCount = 0, lossSum = 0, lossCount = 0;
            let bestTrade = 0, worstTrade = 0;
            if (tradeVals.length > 0) {
                bestTrade = -Infinity;
                worstTrade = Infinity;
                for (let i = 0; i < tradeVals.length; i++) {
                    const v = tradeVals[i];
                    if (v > 0) { winSum += v; winCount++; }
                    else if (v < 0) { lossSum += v; lossCount++; }
                    if (v > bestTrade) bestTrade = v;
                    if (v < worstTrade) worstTrade = v;
                }
                if (!Number.isFinite(bestTrade)) bestTrade = 0;
                if (!Number.isFinite(worstTrade)) worstTrade = 0;
            }
            const avgWin = winCount > 0 ? winSum / winCount : 0;
            const avgLoss = lossCount > 0 ? lossSum / lossCount : 0;

            html += `
                <h3 style="margin: 24px 0 12px;">Trade Statistics</h3>
                <div class="grid-5">
                    ${App.metricHTML("Avg Win", App.currency(avgWin))}
                    ${App.metricHTML("Avg Loss", App.currency(avgLoss))}
                    ${App.metricHTML("Best Trade", App.currency(bestTrade))}
                    ${App.metricHTML("Worst Trade", App.currency(worstTrade))}
                    ${App.metricHTML("Max Drawdown", `${maxDD.toFixed(2)}%`)}
                </div>
            `;

            html += `<div class="page-divider"></div>`;

            // Raw data — lazy-rendered. Tearsheet detail accordion would
            // otherwise build thousands of <tr> nodes before the user asks.
            const safeBT = barType.replace(/[^a-zA-Z0-9]/g, "_");
            const safeName = stratName.replace(/[^a-zA-Z0-9]/g, "_");
            const lazyKey = (kind) => `ts-${safeBT}-${safeName}-${kind}`;
            let rawContent = "";
            if (r.positions_report && r.positions_report.length > 0) {
                rawContent += `<h3 style="margin-bottom: 12px;">Positions Report</h3>`
                           + App.lazyTableHTML(lazyKey("positions"), r.positions_report);
            }
            if (r.fills_report && r.fills_report.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Order Fills Report</h3>`
                           + App.lazyTableHTML(lazyKey("fills"), r.fills_report);
            }
            if (r.account_report && r.account_report.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Account Report</h3>`
                           + App.lazyTableHTML(lazyKey("account"), r.account_report);
            }
            if (r.order_book && r.order_book.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Order Book</h3>`
                           + App.lazyTableHTML(lazyKey("orderbook"), r.order_book);
            }
            if (r.logs && r.logs.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Trading Logs</h3>`
                           + App.lazyTableHTML(lazyKey("logs"), r.logs);
            }

            if (rawContent) {
                html += App.accordionHTML("raw-reports", "View Raw Reports", rawContent);
            }

            detail.innerHTML = html;

            // Render charts after DOM update
            this.renderEquityDrawdown(equity, drawdown, timestamps);
            this.renderTradeDistribution(tradeVals, r.wins, r.losses);

        } else {
            html += '<div class="alert alert-info">No position data available for detailed analysis. The strategy may not have generated any trades.</div>';
            detail.innerHTML = html;
        }
    },

    renderEquityDrawdown(equity, drawdown, timestamps) {
        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#ffffff",
            plot_bgcolor: "#ffffff",
            font: { color: "#1a1a1a" },
            margin: { l: 60, r: 30, t: 30, b: 30 },
        };

        const xData = timestamps || equity.map((_, i) => i);
        const xTitle = timestamps ? "Date" : "Trade #";
        const xType = timestamps ? "date" : "linear";

        const traces = [
            {
                x: xData,
                y: equity,
                mode: "lines",
                name: "Equity",
                line: { color: "#00d4aa", width: 2 },
                fill: "tozeroy",
                fillcolor: "rgba(0, 212, 170, 0.1)",
                xaxis: "x",
                yaxis: "y",
            },
            {
                x: xData,
                y: drawdown,
                mode: "lines",
                name: "Drawdown",
                line: { color: "#ff4444", width: 1.5 },
                fill: "tozeroy",
                fillcolor: "rgba(255, 68, 68, 0.2)",
                xaxis: "x2",
                yaxis: "y2",
            },
        ];

        const layout = {
            ...darkLayout,
            height: 550,
            grid: { rows: 2, columns: 1, pattern: "independent", roworder: "top to bottom" },
            xaxis: { title: "", domain: [0, 1], anchor: "y", type: xType },
            yaxis: { title: "Balance ($)", domain: [0.35, 1], anchor: "x" },
            xaxis2: { title: xTitle, domain: [0, 1], anchor: "y2", type: xType },
            yaxis2: { title: "Drawdown (%)", domain: [0, 0.28], anchor: "x2" },
            showlegend: true,
            legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 },
        };

        App.plot("ts-equity-chart", traces, layout);
    },

    renderTradeDistribution(pnlValues, wins, losses) {
        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#ffffff",
            plot_bgcolor: "#ffffff",
            height: 400,
            font: { color: "#1a1a1a" },
            margin: { l: 50, r: 30, t: 50, b: 50 },
        };

        // P&L per trade bar chart
        const colors = pnlValues.map(v => v >= 0 ? "#00d4aa" : "#ff4444");
        App.plot("ts-pnl-chart", [{
            x: pnlValues.map((_, i) => i + 1),
            y: pnlValues,
            type: "bar",
            marker: { color: colors },
            name: "Trade P&L",
        }], {
            ...darkLayout,
            title: "P&L per Trade",
            xaxis: { title: "Trade #" },
            yaxis: { title: "P&L ($)" },
        });

        // Win/Loss pie
        App.plot("ts-pie-chart", [{
            labels: ["Wins", "Losses"],
            values: [wins, losses],
            type: "pie",
            marker: { colors: ["#00d4aa", "#ff4444"] },
            hole: 0.4,
        }], {
            ...darkLayout,
            title: "Win/Loss Ratio",
        });
    },
};
