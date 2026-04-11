/**
 * View Data Page - Explore loaded crypto data with charts and tables.
 */

const ViewData = {
    barData: [],
    filteredData: [],

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#128202; View Data</h1>
            <p class="page-subtitle">Explore your downloaded crypto data with tables and interactive charts.</p>

            <div class="form-group">
                <label class="form-label">Select Instrument / Bar Type</label>
                <select id="view-bar-type" class="form-control" onchange="ViewData.loadBars()">
                    <option value="">Loading...</option>
                </select>
            </div>

            <div id="view-content"></div>
        `;

        await this.loadBarTypes();
    },

    async loadBarTypes() {
        try {
            const data = await App.api("/api/data/bar_types");
            const select = document.getElementById("view-bar-type");

            if (data.bar_types.length === 0) {
                select.innerHTML = '<option value="">No data available</option>';
                document.getElementById("view-content").innerHTML =
                    '<div class="alert alert-info">No bar data in catalog. Go to <strong>Load Data</strong> first.</div>';
                return;
            }

            select.innerHTML = data.bar_types.map(bt =>
                `<option value="${bt}">${bt}</option>`
            ).join("");

            this.loadBars();
        } catch (e) {
            App.toast("Failed to load bar types: " + e.message, "error");
        }
    },

    async loadBars() {
        const barType = document.getElementById("view-bar-type").value;
        if (!barType) return;

        const content = document.getElementById("view-content");
        content.innerHTML = '<div class="alert alert-info">Loading data...</div>';

        try {
            const data = await App.api(`/api/data/bars?bar_type=${encodeURIComponent(barType)}`);
            this.barData = data.data;

            if (this.barData.length === 0) {
                content.innerHTML = '<div class="alert alert-warning">No bars found.</div>';
                return;
            }

            // Parse dates
            const dates = this.barData.map(d => d.timestamp.split("T")[0]);
            const minDate = dates[0];
            const maxDate = dates[dates.length - 1];

            content.innerHTML = `
                <h2 class="section-title">Date Filter</h2>
                <div class="form-row">
                    <div class="form-group">
                        <label class="form-label">From</label>
                        <input type="date" id="view-date-from" class="form-control" value="${minDate}" min="${minDate}" max="${maxDate}" onchange="ViewData.applyFilter()">
                    </div>
                    <div class="form-group">
                        <label class="form-label">To</label>
                        <input type="date" id="view-date-to" class="form-control" value="${maxDate}" min="${minDate}" max="${maxDate}" onchange="ViewData.applyFilter()">
                    </div>
                </div>

                <div id="view-stats"></div>
                <h2 class="section-title">Candlestick Chart</h2>
                <div id="candlestick-chart" class="chart-container"></div>
                <h2 class="section-title">Data Table</h2>
                <div id="data-table"></div>
                <h2 class="section-title">Price Change Analysis</h2>
                <div class="grid-2">
                    <div id="returns-chart" class="chart-container"></div>
                    <div id="cumulative-chart" class="chart-container"></div>
                </div>
            `;

            this.applyFilter();
        } catch (e) {
            content.innerHTML = `<div class="alert alert-danger">Failed to load bars: ${e.message}</div>`;
        }
    },

    applyFilter() {
        const from = document.getElementById("view-date-from").value;
        const to = document.getElementById("view-date-to").value;

        this.filteredData = this.barData.filter(d => {
            const date = d.timestamp.split("T")[0];
            return date >= from && date <= to;
        });

        this.renderStats();
        this.renderCandlestick();
        this.renderTable();
        this.renderAnalysis();
    },

    renderStats() {
        const data = this.filteredData;
        if (data.length === 0) return;

        const lows = data.map(d => d.low);
        const highs = data.map(d => d.high);
        const closes = data.map(d => d.close);
        const volumes = data.map(d => d.volume);

        const avgClose = closes.reduce((a, b) => a + b, 0) / closes.length;
        const totalVol = volumes.reduce((a, b) => a + b, 0);

        document.getElementById("view-stats").innerHTML = `
            <h2 class="section-title">Summary Statistics</h2>
            <div class="grid-5">
                ${App.metricHTML("Total Bars", data.length)}
                ${App.metricHTML("Min Price", App.currency(Math.min(...lows)))}
                ${App.metricHTML("Max Price", App.currency(Math.max(...highs)))}
                ${App.metricHTML("Avg Close", App.currency(avgClose))}
                ${App.metricHTML("Total Volume", App.formatNumber(Math.round(totalVol)))}
            </div>
        `;
    },

    renderCandlestick() {
        const data = this.filteredData;
        const barType = document.getElementById("view-bar-type").value;

        const trace1 = {
            x: data.map(d => d.timestamp),
            open: data.map(d => d.open),
            high: data.map(d => d.high),
            low: data.map(d => d.low),
            close: data.map(d => d.close),
            type: "candlestick",
            name: "OHLC",
        };

        const trace2 = {
            x: data.map(d => d.timestamp),
            y: data.map(d => d.volume),
            type: "bar",
            name: "Volume",
            marker: { color: "rgba(100, 150, 255, 0.3)" },
            yaxis: "y2",
        };

        const layout = {
            title: barType,
            yaxis: { title: "Price (USD)", side: "left" },
            yaxis2: { title: "Volume", side: "right", overlaying: "y", showgrid: false },
            xaxis: { title: "Date", rangeslider: { visible: false } },
            template: "plotly_dark",
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            height: 600,
            legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 },
            font: { color: "#e6e6e6" },
            margin: { l: 60, r: 60, t: 50, b: 50 },
        };

        Plotly.newPlot("candlestick-chart", [trace1, trace2], layout, { responsive: true });
    },

    renderTable() {
        const data = this.filteredData.map(d => ({
            Timestamp: d.timestamp.replace("T", " ").slice(0, 19),
            Open: App.currency(d.open),
            High: App.currency(d.high),
            Low: App.currency(d.low),
            Close: App.currency(d.close),
            Volume: App.formatNumber(Math.round(d.volume)),
        }));

        document.getElementById("data-table").innerHTML = App.tableHTML(data);
    },

    renderAnalysis() {
        const data = this.filteredData;
        if (data.length < 2) return;

        // Daily returns
        const returns = [];
        for (let i = 1; i < data.length; i++) {
            returns.push(((data[i].close - data[i - 1].close) / data[i - 1].close) * 100);
        }

        // Cumulative returns
        const cumReturns = [];
        let cumProd = 1;
        for (let i = 1; i < data.length; i++) {
            cumProd *= (1 + (data[i].close - data[i - 1].close) / data[i - 1].close);
            cumReturns.push((cumProd - 1) * 100);
        }

        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            height: 400,
            font: { color: "#e6e6e6" },
            margin: { l: 50, r: 30, t: 50, b: 50 },
        };

        // Returns histogram
        Plotly.newPlot("returns-chart", [{
            x: returns,
            type: "histogram",
            nbinsx: 50,
            name: "Daily Returns",
            marker: { color: "rgba(0, 212, 170, 0.6)" },
        }], {
            ...darkLayout,
            title: "Daily Return Distribution (%)",
            xaxis: { title: "Daily Return (%)" },
            yaxis: { title: "Frequency" },
        }, { responsive: true });

        // Cumulative returns
        Plotly.newPlot("cumulative-chart", [{
            x: data.slice(1).map(d => d.timestamp),
            y: cumReturns,
            type: "scatter",
            mode: "lines",
            name: "Cumulative Return",
            fill: "tozeroy",
            line: { color: "#00d4aa" },
            fillcolor: "rgba(0, 212, 170, 0.1)",
        }], {
            ...darkLayout,
            title: "Cumulative Return (%)",
            xaxis: { title: "Date" },
            yaxis: { title: "Return (%)" },
        }, { responsive: true });
    },
};
