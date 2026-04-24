/**
 * View Data Page - Explore loaded market data with charts and tables.
 *
 * Performance note: for multi-million-bar FX streams we never fetch the
 * full range. The initial load picks a sensible window from the catalog
 * metadata (last ~5000 bars' worth of time) and refetches when the user
 * changes the date filter. Server-side downsampling keeps Plotly fast.
 */

const ViewData = {
    barData: [],
    barTypeDetails: {},
    currentBarType: "",
    lastMeta: {},  // raw_count / downsampled / bucket_size from last fetch
    MAX_TABLE_ROWS: 500,
    FETCH_LIMIT: 5000,

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#128202; View Data</h1>
            <p class="page-subtitle">Explore your loaded market data with tables and interactive charts.</p>

            <div class="form-row" style="align-items: flex-end;">
                <div class="form-group" style="flex: 2;">
                    <label class="form-label">Select Instrument / Bar Type</label>
                    <select id="view-bar-type" class="form-control" onchange="ViewData.onBarTypeChange()">
                        <option value="">Loading...</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">From</label>
                    <input type="date" id="view-date-from" class="form-control" onchange="ViewData.fetchBars()">
                </div>
                <div class="form-group">
                    <label class="form-label">To</label>
                    <input type="date" id="view-date-to" class="form-control" onchange="ViewData.fetchBars()">
                </div>
                <div class="form-group" style="flex: 0;">
                    <label class="form-label">&nbsp;</label>
                    <button class="btn" onclick="ViewData.fetchBars()">Refresh</button>
                </div>
            </div>

            <div id="view-content"></div>
        `;

        await this.loadBarTypes();
    },

    async loadBarTypes() {
        try {
            const data = await App.api("/api/data/bar_types");
            const select = document.getElementById("view-bar-type");

            if (!data.bar_types || data.bar_types.length === 0) {
                select.innerHTML = '<option value="">No data available</option>';
                document.getElementById("view-content").innerHTML =
                    '<div class="alert alert-info">No bar data in catalog. Go to <strong>Load Data</strong> first.</div>';
                return;
            }

            this.barTypeDetails = data.bar_type_details || {};
            select.innerHTML = data.bar_types.map(bt =>
                `<option value="${bt}">${bt}</option>`
            ).join("");

            this.onBarTypeChange();
        } catch (e) {
            App.toast("Failed to load bar types: " + e.message, "error");
        }
    },

    /** Pick a reasonable default window for a newly-selected bar type.
     *  We aim for ~FETCH_LIMIT worth of bars so nothing gets downsampled
     *  on first render; user can widen the window anytime. */
    pickDefaultWindow(barType) {
        const d = this.barTypeDetails[barType] || {};
        const start = d.start_date;
        const end = d.end_date;
        if (!start || !end) return { from: "", to: "" };
        const bpd = barType.includes("1-MINUTE") ? 1440
                  : barType.includes("1-HOUR")   ? 24
                  :                                 1;  // default daily
        const daysNeeded = Math.max(1, Math.ceil(this.FETCH_LIMIT / bpd));
        const endD = new Date(end + "T00:00:00Z");
        const fromD = new Date(endD);
        fromD.setUTCDate(fromD.getUTCDate() - daysNeeded + 1);
        const startD = new Date(start + "T00:00:00Z");
        const effFrom = fromD < startD ? startD : fromD;
        return { from: effFrom.toISOString().slice(0, 10), to: end };
    },

    onBarTypeChange() {
        this.currentBarType = document.getElementById("view-bar-type").value;
        if (!this.currentBarType) return;

        const { from, to } = this.pickDefaultWindow(this.currentBarType);
        const d = this.barTypeDetails[this.currentBarType] || {};
        const fromEl = document.getElementById("view-date-from");
        const toEl = document.getElementById("view-date-to");
        fromEl.value = from;
        fromEl.min = d.start_date || "";
        fromEl.max = d.end_date || "";
        toEl.value = to;
        toEl.min = d.start_date || "";
        toEl.max = d.end_date || "";

        this.fetchBars();
    },

    async fetchBars() {
        const barType = this.currentBarType || document.getElementById("view-bar-type").value;
        if (!barType) return;

        const from = document.getElementById("view-date-from").value;
        const to = document.getElementById("view-date-to").value;

        const content = document.getElementById("view-content");
        content.innerHTML = '<div class="alert alert-info">Loading data...</div>';

        const qs = new URLSearchParams({
            bar_type: barType,
            start: from || "",
            end: to || "",
            limit: String(this.FETCH_LIMIT),
        });
        const t0 = performance.now();

        try {
            const data = await App.api(`/api/data/bars?${qs.toString()}`);
            const elapsed = ((performance.now() - t0) / 1000).toFixed(2);
            this.barData = data.data || [];
            this.lastMeta = {
                raw_count: data.raw_count ?? this.barData.length,
                downsampled: !!data.downsampled,
                bucket_size: data.bucket_size || 1,
                elapsed,
            };

            if (this.barData.length === 0) {
                content.innerHTML = '<div class="alert alert-warning">No bars in this range.</div>';
                return;
            }

            content.innerHTML = `
                <div id="view-meta" style="margin: 8px 0 16px; font-size: 0.88em; color: var(--text-secondary);"></div>
                <div id="view-stats"></div>
                <h2 class="section-title">Candlestick Chart</h2>
                <div id="candlestick-chart" class="chart-container"></div>
                <h2 class="section-title">Data Table <span style="font-weight: normal; font-size: 0.8em; color: var(--text-secondary);">(first ${this.MAX_TABLE_ROWS} rows)</span></h2>
                <div id="data-table"></div>
                <h2 class="section-title">Price Change Analysis</h2>
                <div class="grid-2">
                    <div id="returns-chart" class="chart-container"></div>
                    <div id="cumulative-chart" class="chart-container"></div>
                </div>
            `;

            this.renderMeta();
            this.renderStats();
            this.renderCandlestick();
            this.renderTable();
            this.renderAnalysis();
        } catch (e) {
            content.innerHTML = `<div class="alert alert-danger">Failed to load bars: ${e.message || e}</div>`;
        }
    },

    renderMeta() {
        const m = this.lastMeta;
        const el = document.getElementById("view-meta");
        if (!el) return;
        const parts = [`Showing <strong>${this.barData.length.toLocaleString()}</strong> points`];
        if (m.downsampled) {
            parts.push(`downsampled from <strong>${m.raw_count.toLocaleString()}</strong> raw bars (bucket = ${m.bucket_size})`);
        }
        parts.push(`fetched in ${m.elapsed}s`);
        el.innerHTML = parts.join(" &middot; ");
    },

    renderStats() {
        const data = this.barData;
        if (data.length === 0) return;

        let lowMin = Infinity, highMax = -Infinity, closeSum = 0, volSum = 0;
        for (const d of data) {
            if (d.low < lowMin) lowMin = d.low;
            if (d.high > highMax) highMax = d.high;
            closeSum += d.close;
            volSum += d.volume;
        }

        document.getElementById("view-stats").innerHTML = `
            <h2 class="section-title">Summary Statistics</h2>
            <div class="grid-5">
                ${App.metricHTML("Total Bars", data.length)}
                ${App.metricHTML("Min Price", App.currency(lowMin))}
                ${App.metricHTML("Max Price", App.currency(highMax))}
                ${App.metricHTML("Avg Close", App.currency(closeSum / data.length))}
                ${App.metricHTML("Total Volume", App.formatNumber(Math.round(volSum)))}
            </div>
        `;
    },

    renderCandlestick() {
        const data = this.barData;
        const barType = this.currentBarType;

        // Single-pass column extraction — cheaper than five .map()s
        const x = new Array(data.length);
        const open = new Array(data.length);
        const high = new Array(data.length);
        const low = new Array(data.length);
        const close = new Array(data.length);
        const vol = new Array(data.length);
        for (let i = 0; i < data.length; i++) {
            const d = data[i];
            x[i] = d.timestamp;
            open[i] = d.open; high[i] = d.high; low[i] = d.low; close[i] = d.close;
            vol[i] = d.volume;
        }

        const trace1 = { x, open, high, low, close, type: "candlestick", name: "OHLC" };
        const trace2 = {
            x, y: vol, type: "bar", name: "Volume",
            marker: { color: "rgba(100, 150, 255, 0.3)" }, yaxis: "y2",
        };

        App.plot("candlestick-chart", [trace1, trace2], {
            title: barType,
            yaxis:  { title: "Price", side: "left" },
            yaxis2: { title: "Volume", side: "right", overlaying: "y", showgrid: false },
            xaxis:  { title: "Date", rangeslider: { visible: false } },
            template: "plotly_dark",
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            height: 600,
            legend: { orientation: "h", yanchor: "bottom", y: 1.02, xanchor: "right", x: 1 },
            font: { color: "#e6e6e6" },
            margin: { l: 60, r: 60, t: 50, b: 50 },
        });
    },

    renderTable() {
        const max = this.MAX_TABLE_ROWS;
        const slice = this.barData.slice(0, max);
        const rows = slice.map(d => ({
            Timestamp: d.timestamp.replace("T", " ").slice(0, 19),
            Open: App.currency(d.open),
            High: App.currency(d.high),
            Low: App.currency(d.low),
            Close: App.currency(d.close),
            Volume: App.formatNumber(Math.round(d.volume)),
        }));
        document.getElementById("data-table").innerHTML = App.tableHTML(rows);
    },

    renderAnalysis() {
        const data = this.barData;
        if (data.length < 2) return;

        const returns = [];
        const cumReturns = [];
        const cumDates = [];
        let cumProd = 1;
        for (let i = 1; i < data.length; i++) {
            const prev = data[i - 1].close;
            if (prev > 0) {
                const r = (data[i].close - prev) / prev;
                returns.push(r * 100);
                cumProd *= (1 + r);
                cumReturns.push((cumProd - 1) * 100);
                cumDates.push(data[i].timestamp);
            }
        }

        if (returns.length === 0) {
            document.getElementById("returns-chart").innerHTML =
                '<div class="alert alert-warning">No valid price data for return analysis.</div>';
            document.getElementById("cumulative-chart").innerHTML = "";
            return;
        }

        const darkLayout = {
            template: "plotly_dark",
            paper_bgcolor: "#21252b",
            plot_bgcolor: "#21252b",
            height: 400,
            font: { color: "#e6e6e6" },
            margin: { l: 50, r: 30, t: 50, b: 50 },
        };

        App.plot("returns-chart", [{
            x: returns, type: "histogram", nbinsx: 50, name: "Returns",
            marker: { color: "rgba(0, 212, 170, 0.6)" },
        }], {
            ...darkLayout,
            title: "Return Distribution (%)",
            xaxis: { title: "Return (%)" },
            yaxis: { title: "Frequency" },
        });

        App.plot("cumulative-chart", [{
            x: cumDates, y: cumReturns, type: "scatter", mode: "lines",
            name: "Cumulative Return", fill: "tozeroy",
            line: { color: "#00d4aa" }, fillcolor: "rgba(0, 212, 170, 0.1)",
        }], {
            ...darkLayout,
            title: "Cumulative Return (%)",
            xaxis: { title: "Date" }, yaxis: { title: "Return (%)" },
        });
    },
};
