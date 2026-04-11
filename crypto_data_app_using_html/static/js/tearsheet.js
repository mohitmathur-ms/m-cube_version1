/**
 * Tearsheet Page - Detailed performance analytics from backtest results.
 */

const Tearsheet = {
    COLORS: ["#00d4aa", "#ff6b6b", "#4ecdc4", "#ffe66d", "#a29bfe", "#fd79a8"],

    render(container) {
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

        const results = App.state.backtestResults;
        const config = App.state.backtestConfig;
        const names = Object.keys(results);

        container.innerHTML = `
            <h1 class="page-title">&#128200; Performance Tearsheet</h1>
            <p class="page-subtitle">Detailed performance analytics from your backtest results.</p>

            <h2 class="section-title">Equity Comparison | ${config.bar_type || "N/A"}</h2>
            <div id="equity-compare-chart" class="chart-container"></div>

            <div class="page-divider"></div>

            <div class="form-group">
                <label class="form-label">Select strategy for detailed tearsheet</label>
                <select id="ts-strategy-select" class="form-control" onchange="Tearsheet.renderDetail()">
                    ${names.map(n => `<option value="${n}">${n}</option>`).join("")}
                </select>
            </div>

            <div id="tearsheet-detail"></div>
        `;

        this.renderEquityComparison(results, names);
        this.renderDetail();
    },

    /** Extract PnL series from positions report */
    extractPnl(positionsReport) {
        if (!positionsReport || positionsReport.length === 0) return null;

        // Find pnl column
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

    renderEquityComparison(results, names) {
        const traces = [];
        let hasData = false;

        names.forEach((name, i) => {
            const r = results[name];
            const pnlValues = this.extractPnl(r.positions_report);
            if (!pnlValues) return;

            hasData = true;
            const color = this.COLORS[i % this.COLORS.length];

            // Cumulative equity
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
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            height: 500,
            font: { color: "#e6e6e6" },
            margin: { l: 60, r: 30, t: 30, b: 50 },
            yaxis: { title: "Balance ($)" },
            xaxis: { title: "Trade #" },
            legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 },
        };

        Plotly.newPlot("equity-compare-chart", traces, darkLayout, { responsive: true });
    },

    renderDetail() {
        const stratName = document.getElementById("ts-strategy-select").value;
        const results = App.state.backtestResults;
        const r = results[stratName];
        const detail = document.getElementById("tearsheet-detail");

        if (!r) {
            detail.innerHTML = '<div class="alert alert-warning">No results for selected strategy.</div>';
            return;
        }

        // Summary
        let html = `
            <h2 class="section-title">${stratName} — Detailed Tearsheet</h2>
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

        if (pnlValues && pnlValues.length > 0) {
            html += `
                <h3 style="margin-bottom: 16px;">Equity & Drawdown Analysis</h3>
                <div id="ts-equity-chart" class="chart-container"></div>

                <h3 style="margin: 24px 0 16px;">Trade Distribution</h3>
                <div class="grid-2">
                    <div id="ts-pnl-chart" class="chart-container"></div>
                    <div id="ts-pie-chart" class="chart-container"></div>
                </div>
            `;

            // Trade statistics
            const wins = pnlValues.filter(v => v > 0);
            const losses = pnlValues.filter(v => v < 0);
            const avgWin = wins.length > 0 ? wins.reduce((a, b) => a + b, 0) / wins.length : 0;
            const avgLoss = losses.length > 0 ? losses.reduce((a, b) => a + b, 0) / losses.length : 0;
            const bestTrade = Math.max(...pnlValues);
            const worstTrade = Math.min(...pnlValues);

            // Drawdown
            let cum = 0;
            const equity = pnlValues.map(v => { cum += v; return r.starting_capital + cum; });
            let peak = equity[0];
            const drawdown = equity.map(v => {
                if (v > peak) peak = v;
                return ((v - peak) / peak) * 100;
            });
            const maxDD = Math.min(...drawdown);

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

            // Raw data
            let rawContent = "";
            if (r.positions_report && r.positions_report.length > 0) {
                rawContent += `<h3 style="margin-bottom: 12px;">Positions Report</h3>` + App.tableHTML(r.positions_report);
            }
            if (r.fills_report && r.fills_report.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Order Fills Report</h3>` + App.tableHTML(r.fills_report);
            }
            if (r.account_report && r.account_report.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Account Report</h3>` + App.tableHTML(r.account_report);
            }
            if (r.order_book && r.order_book.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Order Book</h3>` + App.tableHTML(r.order_book);
            }
            if (r.logs && r.logs.length > 0) {
                rawContent += `<h3 style="margin: 20px 0 12px;">Trading Logs</h3>` + App.tableHTML(r.logs);
            }

            if (rawContent) {
                html += App.accordionHTML("raw-reports", "View Raw Reports", rawContent);
            }

            detail.innerHTML = html;

            // Render charts after DOM update
            this.renderEquityDrawdown(equity, drawdown);
            this.renderTradeDistribution(pnlValues, r.wins, r.losses);

        } else {
            html += '<div class="alert alert-info">No position data available for detailed analysis. The strategy may not have generated any trades.</div>';
            detail.innerHTML = html;
        }
    },

    renderEquityDrawdown(equity, drawdown) {
        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            font: { color: "#e6e6e6" },
            margin: { l: 60, r: 30, t: 30, b: 30 },
        };

        const traces = [
            {
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
            xaxis: { title: "", domain: [0, 1], anchor: "y" },
            yaxis: { title: "Balance ($)", domain: [0.35, 1], anchor: "x" },
            xaxis2: { title: "Trade #", domain: [0, 1], anchor: "y2" },
            yaxis2: { title: "Drawdown (%)", domain: [0, 0.28], anchor: "x2" },
            showlegend: true,
            legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 },
        };

        Plotly.newPlot("ts-equity-chart", traces, layout, { responsive: true });
    },

    renderTradeDistribution(pnlValues, wins, losses) {
        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            height: 400,
            font: { color: "#e6e6e6" },
            margin: { l: 50, r: 30, t: 50, b: 50 },
        };

        // P&L per trade bar chart
        const colors = pnlValues.map(v => v >= 0 ? "#00d4aa" : "#ff4444");
        Plotly.newPlot("ts-pnl-chart", [{
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
        }, { responsive: true });

        // Win/Loss pie
        Plotly.newPlot("ts-pie-chart", [{
            labels: ["Wins", "Losses"],
            values: [wins, losses],
            type: "pie",
            marker: { colors: ["#00d4aa", "#ff4444"] },
            hole: 0.4,
        }], {
            ...darkLayout,
            title: "Win/Loss Ratio",
        }, { responsive: true });
    },
};
