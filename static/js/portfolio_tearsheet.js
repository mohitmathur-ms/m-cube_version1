/**
 * Portfolio Tearsheet Page - Combined analytics for portfolio backtest results.
 * Sections: Summary, Equity Curve + Drawdown, Per-Strategy Equity, P&L Contribution,
 * Per-Strategy Metrics Table, Trade Distribution, Correlation Heatmap.
 */

const PortfolioTearsheet = {
    COLORS: ["#00d4aa", "#ff6b6b", "#4ecdc4", "#ffe66d", "#a29bfe", "#fd79a8", "#74b9ff", "#ffeaa7"],

    /** Re-render only when the underlying portfolio results object reference
     *  has changed. Same reference → keep the existing DOM so the active
     *  sub-tab (summary / equity / strategies / trades) survives nav. */
    onShow(container, { firstVisit }) {
        const cur = App.state.portfolioResults;
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
        this._lastRenderedResults = App.state.portfolioResults;
        const r = App.state.portfolioResults;

        if (!r) {
            container.innerHTML = `
                <h1 class="page-title">Portfolio Tearsheet</h1>
                <p class="page-subtitle">Combined analytics for your portfolio backtest.</p>
                <div class="empty-state">
                    <div class="empty-state-icon">&#128201;</div>
                    <div class="empty-state-text">No portfolio results. Go to <strong>Portfolio</strong> and run a backtest first.</div>
                </div>
            `;
            return;
        }

        const perStrat = r.per_strategy || {};
        const stratEntries = Object.entries(perStrat);

        container.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 6px;">
                <div>
                    <h1 class="page-title" style="margin-bottom: 2px;">Portfolio Tearsheet</h1>
                    <p class="page-subtitle" style="margin-bottom: 0;">${r.portfolio_name || "Portfolio"} | ${App.currency(r.starting_capital)} starting capital</p>
                </div>
            </div>

            <!-- Tab bar -->
            <div class="tab-bar">
                <button class="tab-btn active" onclick="PortfolioTearsheet.showTab('summary', this)">Summary</button>
                <button class="tab-btn" onclick="PortfolioTearsheet.showTab('equity', this)">Equity & Drawdown</button>
                <button class="tab-btn" onclick="PortfolioTearsheet.showTab('strategies', this)">Strategies</button>
                <button class="tab-btn" onclick="PortfolioTearsheet.showTab('trades', this)">Trades</button>
            </div>

            <div id="pt-tab-content"></div>
        `;

        this.showTab("summary");
    },

    showTab(tab, btn) {
        // Update active tab button
        if (btn) {
            document.querySelectorAll(".tab-btn").forEach(b => b.classList.remove("active"));
            btn.classList.add("active");
        }

        const r = App.state.portfolioResults;
        const content = document.getElementById("pt-tab-content");

        switch (tab) {
            case "summary": this._renderSummary(content, r); break;
            case "equity": this._renderEquity(content, r); break;
            case "strategies": this._renderStrategies(content, r); break;
            case "trades": this._renderTrades(content, r); break;
        }
    },

    _renderSummary(container, r) {
        const pnlClass = r.total_pnl >= 0 ? "positive" : "negative";
        let flagsHTML = "";
        if (r.max_loss_hit) flagsHTML += '<div class="alert alert-danger" style="margin-top: 12px;">Max Loss limit was hit during this backtest.</div>';
        if (r.max_profit_hit) flagsHTML += '<div class="alert alert-success" style="margin-top: 12px;">Max Profit target was reached during this backtest.</div>';

        // Per-strategy contribution bar
        const perStrat = r.per_strategy || {};
        const stratEntries = Object.entries(perStrat);
        let contribRows = stratEntries.map(([sid, sr]) => {
            const cls = sr.pnl >= 0 ? "positive" : "negative";
            const pct = r.total_pnl !== 0 ? ((sr.pnl / Math.abs(r.total_pnl)) * 100).toFixed(1) : "0.0";
            return `<tr>
                <td>${sr.display_name}</td>
                <td class="${cls}">${App.currency(sr.pnl)}</td>
                <td>${pct}%</td>
                <td>${sr.trades}</td>
                <td>${sr.win_rate.toFixed(1)}%</td>
                <td>${(sr.win_pct_days || 0).toFixed(1)}%</td>
            </tr>`;
        }).join("");

        container.innerHTML = `
            <div class="grid-6">
                ${App.metricHTML("Starting Capital", App.currency(r.starting_capital))}
                ${App.metricHTML("Final Balance", App.currency(r.final_balance))}
                ${App.metricHTML("Total P&L", App.currency(r.total_pnl), r.total_return_pct)}
                ${App.metricHTML("Total Trades", r.total_trades)}
                ${App.metricHTML("Win Rate (Trades)", r.win_rate.toFixed(1) + "%")}
                ${App.metricHTML("Win% (Days)", (r.win_pct_days || 0).toFixed(1) + "%")}
                ${App.metricHTML("Max Drawdown", r.max_drawdown.toFixed(2) + "%")}
            </div>

            ${flagsHTML}

            <h3 style="margin: 20px 0 10px;">Strategy Contribution</h3>
            <div class="table-container">
                <table>
                    <thead><tr><th>Strategy</th><th>P&L</th><th>Contribution</th><th>Trades</th><th>Win Rate (Trades)</th><th>Win% (Days)</th></tr></thead>
                    <tbody>${contribRows}</tbody>
                </table>
            </div>

            <div id="pt-contrib-chart" class="chart-container" style="margin-top: 16px;"></div>
        `;

        // P&L contribution bar chart
        if (stratEntries.length > 0) {
            const names = stratEntries.map(([, sr]) => sr.display_name);
            const pnls = stratEntries.map(([, sr]) => sr.pnl);
            const colors = pnls.map(v => v >= 0 ? "#00d4aa" : "#ff4444");

            App.plot("pt-contrib-chart", [{
                x: names,
                y: pnls,
                type: "bar",
                marker: { color: colors },
            }], {
                template: "plotly_dark",
                paper_bgcolor: "#ffffff",
                plot_bgcolor: "#ffffff",
                height: 350,
                font: { color: "#1a1a1a" },
                margin: { l: 60, r: 30, t: 30, b: 80 },
                yaxis: { title: "P&L ($)" },
            });
        }
    },

    _renderEquity(container, r) {
        container.innerHTML = `
            <div id="pt-equity-chart" class="chart-container"></div>
            <div id="pt-per-strat-equity" class="chart-container" style="margin-top: 16px;"></div>
        `;

        // Use timestamped equity curve if available, fall back to trade-indexed
        const curvePts = r.equity_curve_ts || [];
        const hasTimestamps = curvePts.length > 0 && curvePts[0].timestamp;

        let timestamps, equity;
        if (hasTimestamps) {
            timestamps = curvePts.map(pt => pt.timestamp);
            equity = curvePts.map(pt => pt.balance);
        } else {
            equity = r.equity_curve || [];
            timestamps = null;
        }

        if (equity.length === 0) {
            container.innerHTML = '<div class="alert alert-info">No equity data available.</div>';
            return;
        }

        // Drawdown — single pass.
        let peak = equity[0];
        const drawdown = new Array(equity.length);
        for (let i = 0; i < equity.length; i++) {
            const v = equity[i];
            if (v > peak) peak = v;
            drawdown[i] = peak > 0 ? ((v - peak) / peak) * 100 : 0;
        }

        const xData = timestamps || equity.map((_, i) => i);
        const xTitle = timestamps ? "Date" : "Trade #";

        // Combined equity + drawdown
        App.plot("pt-equity-chart", [
            {
                x: xData, y: equity, mode: "lines", name: "Portfolio Equity",
                line: { color: "#00d4aa", width: 2 },
                fill: "tozeroy", fillcolor: "rgba(0,212,170,0.1)",
                yaxis: "y",
            },
            {
                x: xData, y: drawdown, mode: "lines", name: "Drawdown",
                line: { color: "#ff4444", width: 1.5 },
                fill: "tozeroy", fillcolor: "rgba(255,68,68,0.15)",
                yaxis: "y2",
            },
        ], {
            template: "plotly_dark",
            paper_bgcolor: "#ffffff",
            plot_bgcolor: "#ffffff",
            height: 500,
            font: { color: "#1a1a1a" },
            margin: { l: 60, r: 60, t: 30, b: 50 },
            yaxis: { title: "Balance ($)", domain: [0.3, 1] },
            yaxis2: { title: "Drawdown (%)", domain: [0, 0.25], side: "right" },
            xaxis: { title: xTitle, type: timestamps ? "date" : "linear" },
            legend: { orientation: "h", y: 1.05, x: 0.5, xanchor: "center" },
        });

        // Per-strategy P&L bar chart (one bar per strategy)
        const perStrat = r.per_strategy || {};
        const stratEntries = Object.entries(perStrat);
        if (stratEntries.length > 0) {
            const names = stratEntries.map(([, sr]) => sr.display_name);
            const pnls = stratEntries.map(([, sr]) => sr.pnl);
            const colors = stratEntries.map(([, sr], i) => this.COLORS[i % this.COLORS.length]);

            App.plot("pt-per-strat-equity", [{
                x: names,
                y: pnls,
                type: "bar",
                marker: { color: colors },
                text: pnls.map(v => "$" + v.toFixed(2)),
                textposition: "auto",
                textfont: { color: "#1a1a1a" },
            }], {
                template: "plotly_dark",
                paper_bgcolor: "#ffffff",
                plot_bgcolor: "#ffffff",
                height: 400,
                font: { color: "#1a1a1a" },
                margin: { l: 60, r: 30, t: 30, b: 100 },
                yaxis: { title: "P&L ($)" },
            });
        }
    },

    _renderStrategies(container, r) {
        const perStrat = r.per_strategy || {};
        const stratEntries = Object.entries(perStrat);

        if (stratEntries.length === 0) {
            container.innerHTML = '<div class="alert alert-info">No per-strategy data.</div>';
            return;
        }

        // Detailed metrics table — single pass per strategy, no array spreads.
        let rows = stratEntries.map(([sid, sr]) => {
            const pnls = sr.trade_pnls || [];
            let winSum = 0, winCount = 0, lossSum = 0, lossCount = 0;
            let best = 0, worst = 0;
            if (pnls.length > 0) {
                best = -Infinity; worst = Infinity;
                for (let i = 0; i < pnls.length; i++) {
                    const v = pnls[i];
                    if (v > 0) { winSum += v; winCount++; }
                    else if (v < 0) { lossSum += v; lossCount++; }
                    if (v > best) best = v;
                    if (v < worst) worst = v;
                }
                if (!Number.isFinite(best)) best = 0;
                if (!Number.isFinite(worst)) worst = 0;
            }
            const avgWin = winCount > 0 ? winSum / winCount : 0;
            const avgLoss = lossCount > 0 ? lossSum / lossCount : 0;
            const pnlCls = sr.pnl >= 0 ? "positive" : "negative";

            return `<tr>
                <td>${sr.display_name}</td>
                <td>${App.barTypeLabel(sr.bar_type) || "N/A"}</td>
                <td class="${pnlCls}">${App.currency(sr.pnl)}</td>
                <td>${sr.trades}</td>
                <td>${sr.win_rate.toFixed(1)}%</td>
                <td>${(sr.win_pct_days || 0).toFixed(1)}%</td>
                <td>${App.currency(avgWin)}</td>
                <td>${App.currency(avgLoss)}</td>
                <td>${App.currency(best)}</td>
                <td>${App.currency(worst)}</td>
            </tr>`;
        }).join("");

        container.innerHTML = `
            <h3 style="margin-bottom: 10px;">Per-Strategy Metrics</h3>
            <div class="table-container">
                <table>
                    <thead><tr>
                        <th>Strategy</th><th>Instrument</th><th>P&L</th><th>Trades</th>
                        <th>Win Rate (Trades)</th><th>Win% (Days)</th><th>Avg Win</th><th>Avg Loss</th><th>Best</th><th>Worst</th>
                    </tr></thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>

            <h3 style="margin: 20px 0 10px;">Strategy Correlation</h3>
            <div id="pt-heatmap" class="heatmap-container"></div>
        `;

        // Correlation heatmap
        if (stratEntries.length >= 2) {
            this._renderCorrelation(stratEntries);
        } else {
            document.getElementById("pt-heatmap").innerHTML =
                '<div class="alert alert-info" style="margin: 12px;">Need at least 2 strategies for correlation.</div>';
        }
    },

    _renderCorrelation(stratEntries) {
        const names = stratEntries.map(([, sr]) => sr.display_name);
        const pnlArrays = stratEntries.map(([, sr]) => sr.trade_pnls || []);

        // Pad arrays to same length
        const maxLen = Math.max(...pnlArrays.map(a => a.length));
        const padded = pnlArrays.map(arr => {
            const p = [...arr];
            while (p.length < maxLen) p.push(0);
            return p;
        });

        // Calculate correlation matrix
        const n = padded.length;
        const matrix = [];
        for (let i = 0; i < n; i++) {
            const row = [];
            for (let j = 0; j < n; j++) {
                row.push(this._correlation(padded[i], padded[j]));
            }
            matrix.push(row);
        }

        App.plot("pt-heatmap", [{
            z: matrix,
            x: names,
            y: names,
            type: "heatmap",
            colorscale: [
                [0, "#ff4444"],
                [0.5, "#f0f0f0"],
                [1, "#00d4aa"],
            ],
            zmin: -1, zmax: 1,
            text: matrix.map(row => row.map(v => v.toFixed(2))),
            texttemplate: "%{text}",
            textfont: { color: "#1a1a1a", size: 12 },
        }], {
            template: "plotly_dark",
            paper_bgcolor: "#ffffff",
            plot_bgcolor: "#ffffff",
            height: 400,
            font: { color: "#1a1a1a" },
            margin: { l: 120, r: 30, t: 30, b: 120 },
        });
    },

    _correlation(a, b) {
        const n = a.length;
        if (n === 0) return 0;
        const meanA = a.reduce((s, v) => s + v, 0) / n;
        const meanB = b.reduce((s, v) => s + v, 0) / n;
        let num = 0, denA = 0, denB = 0;
        for (let i = 0; i < n; i++) {
            const da = a[i] - meanA;
            const db = b[i] - meanB;
            num += da * db;
            denA += da * da;
            denB += db * db;
        }
        const den = Math.sqrt(denA * denB);
        return den === 0 ? 0 : num / den;
    },

    _renderTrades(container, r) {
        const perStrat = r.per_strategy || {};
        const stratEntries = Object.entries(perStrat);

        if (stratEntries.length === 0) {
            container.innerHTML = '<div class="alert alert-info">No trade data.</div>';
            return;
        }

        let chartsHTML = stratEntries.map(([sid, sr], i) =>
            `<div>
                <h4 style="margin-bottom: 8px; color: ${this.COLORS[i % this.COLORS.length]};">${sr.display_name}</h4>
                <div id="pt-trades-${i}" class="chart-container"></div>
            </div>`
        ).join("");

        // Win/Loss donut
        container.innerHTML = `
            <div class="grid-2">
                <div>
                    <h3 style="margin-bottom: 10px;">Overall Win/Loss</h3>
                    <div id="pt-winloss-pie" class="chart-container"></div>
                </div>
                <div>
                    <h3 style="margin-bottom: 10px;">Combined Trade P&L</h3>
                    <div id="pt-all-trades" class="chart-container"></div>
                </div>
            </div>
            <h3 style="margin: 20px 0 10px;">Per-Strategy Trade Distribution</h3>
            <div class="grid-2">${chartsHTML}</div>
        `;

        // Win/Loss pie
        App.plot("pt-winloss-pie", [{
            labels: ["Wins", "Losses"],
            values: [r.wins, r.losses],
            type: "pie",
            marker: { colors: ["#00d4aa", "#ff4444"] },
            hole: 0.4,
        }], {
            template: "plotly_dark",
            paper_bgcolor: "#ffffff",
            plot_bgcolor: "#ffffff",
            height: 350,
            font: { color: "#1a1a1a" },
            margin: { l: 30, r: 30, t: 30, b: 30 },
        });

        // Combined trade P&L
        const allPnls = [];
        for (const [, sr] of stratEntries) {
            allPnls.push(...(sr.trade_pnls || []));
        }
        if (allPnls.length > 0) {
            const colors = allPnls.map(v => v >= 0 ? "#00d4aa" : "#ff4444");
            App.plot("pt-all-trades", [{
                x: allPnls.map((_, i) => i + 1),
                y: allPnls,
                type: "bar",
                marker: { color: colors },
            }], {
                template: "plotly_dark",
                paper_bgcolor: "#ffffff",
                plot_bgcolor: "#ffffff",
                height: 350,
                font: { color: "#1a1a1a" },
                margin: { l: 60, r: 30, t: 30, b: 50 },
                xaxis: { title: "Trade #" },
                yaxis: { title: "P&L ($)" },
            });
        }

        // Per-strategy trade P&L
        stratEntries.forEach(([sid, sr], i) => {
            const pnls = sr.trade_pnls || [];
            if (pnls.length === 0) return;
            const colors = pnls.map(v => v >= 0 ? "#00d4aa" : "#ff4444");
            App.plot(`pt-trades-${i}`, [{
                x: pnls.map((_, j) => j + 1),
                y: pnls,
                type: "bar",
                marker: { color: colors },
            }], {
                template: "plotly_dark",
                paper_bgcolor: "#ffffff",
                plot_bgcolor: "#ffffff",
                height: 280,
                font: { color: "#1a1a1a" },
                margin: { l: 50, r: 20, t: 10, b: 40 },
                xaxis: { title: "Trade #" },
                yaxis: { title: "P&L ($)" },
            });
        });
    },
};
