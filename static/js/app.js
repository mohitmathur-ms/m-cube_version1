/**
 * Main Application - Router, utilities, and shared state.
 */

const App = {
    currentPage: "dashboard",
    state: {
        backtestResults: null,
        backtestConfig: null,
        selectedAsset: "crypto",
        selectedVenue: "BINANCE",
    },

    /** Initialize the app */
    init() {
        this.bindNavigation();
        this.navigate("dashboard");
    },

    /** Bind sidebar navigation clicks */
    bindNavigation() {
        document.querySelectorAll(".nav-item").forEach(item => {
            item.addEventListener("click", (e) => {
                e.preventDefault();
                const page = item.dataset.page;
                this.navigate(page);
            });
        });
    },

    /** Navigate to a page */
    navigate(page) {
        // Cancel any in-flight requests from the previous page — prevents
        // stale fetches from clobbering the new page's DOM.
        for (const ac of this._pendingAborters) ac.abort();
        this._pendingAborters.clear();

        this.currentPage = page;

        // Update active nav item
        document.querySelectorAll(".nav-item").forEach(item => {
            item.classList.toggle("active", item.dataset.page === page);
        });

        // Render page
        const main = document.getElementById("main-content");
        switch (page) {
            case "dashboard":   Dashboard.render(main); break;
            case "load_data":   LoadData.render(main); break;
            case "view_data":   ViewData.render(main); break;
            case "backtest":    Backtest.render(main); break;
            case "tearsheet":   Tearsheet.render(main); break;
            case "portfolio":   Portfolio.render(main); break;
            case "portfolio_tearsheet": PortfolioTearsheet.render(main); break;
        }
    },

    /** Show loading overlay */
    showLoading(text = "Loading...") {
        document.getElementById("loading-text").textContent = text;
        document.getElementById("loading-overlay").style.display = "flex";
    },

    /** Hide loading overlay */
    hideLoading() {
        document.getElementById("loading-overlay").style.display = "none";
    },

    /** Show toast notification */
    toast(message, type = "info", duration = 4000) {
        const container = document.getElementById("toast-container");
        const toast = document.createElement("div");
        toast.className = `toast toast-${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.opacity = "0";
            toast.style.transition = "opacity 0.3s";
            setTimeout(() => toast.remove(), 300);
        }, duration);
    },

    /** Make API call. Auto-aborts on navigate unless options.keepalive=true. */
    async api(endpoint, options = {}) {
        const { keepalive, timeoutMs = 60000, ...rest } = options;
        const ac = new AbortController();
        const timer = setTimeout(() => ac.abort(), timeoutMs);
        if (!keepalive) this._pendingAborters.add(ac);
        try {
            const response = await fetch(endpoint, {
                headers: { "Content-Type": "application/json" },
                signal: ac.signal,
                ...rest,
            });
            if (!response.ok) {
                const error = await response.json().catch(() => ({ error: response.statusText }));
                throw new Error(error.error || error.traceback || response.statusText || `HTTP ${response.status}`);
            }
            return response.json();
        } finally {
            clearTimeout(timer);
            this._pendingAborters.delete(ac);
        }
    },

    /** Pending request aborters, cancelled on navigation. */
    _pendingAborters: new Set(),

    /** Debounce helper — returns a function that fires after `wait` ms of quiet. */
    debounce(fn, wait = 150) {
        let t;
        return function (...args) {
            clearTimeout(t);
            t = setTimeout(() => fn.apply(this, args), wait);
        };
    },

    /** Plotly render helper: uses react() after first plot for ~5x faster updates.
     *  Safe to call before Plotly finishes loading (deferred <script>) — it waits. */
    async plot(divId, traces, layout, config = { responsive: true }) {
        const el = typeof divId === "string" ? document.getElementById(divId) : divId;
        if (!el) return;
        if (typeof Plotly === "undefined") {
            await this._plotlyReady();
            if (!el.isConnected) return; // user navigated away
        }
        // If Plotly has already rendered into this node it will have _fullData.
        if (el._fullData) {
            Plotly.react(el, traces, layout, config);
        } else {
            Plotly.newPlot(el, traces, layout, config);
        }
    },

    _plotlyReady() {
        return new Promise((resolve) => {
            const check = () => {
                if (typeof Plotly !== "undefined") resolve();
                else setTimeout(check, 50);
            };
            check();
        });
    },

    /** Single-pass min/max/sum/count over numeric values of a key or accessor. */
    stats(arr, accessor) {
        const get = typeof accessor === "function" ? accessor : (d) => d[accessor];
        let min = Infinity, max = -Infinity, sum = 0, n = 0;
        for (let i = 0; i < arr.length; i++) {
            const v = +get(arr[i]);
            if (Number.isFinite(v)) {
                if (v < min) min = v;
                if (v > max) max = v;
                sum += v;
                n++;
            }
        }
        return { min: n ? min : 0, max: n ? max : 0, sum, mean: n ? sum / n : 0, count: n };
    },

    /** Render a (potentially huge) table lazily — only builds the DOM when visible. */
    lazyTableHTML(id, data, columns = null) {
        if (!data || data.length === 0) return '<p class="section-caption">No data available.</p>';
        // Store the data globally keyed by id so the accordion onclick can find it.
        this._lazyTables = this._lazyTables || {};
        this._lazyTables[id] = { data, columns };
        return `<div class="lazy-table" data-lazy-id="${id}">
            <p class="section-caption">${data.length} rows — <a href="#" onclick="return App._renderLazy('${id}', this)">Click to render</a></p>
        </div>`;
    },

    _renderLazy(id, anchor) {
        const entry = this._lazyTables && this._lazyTables[id];
        if (!entry) return false;
        const wrap = anchor.closest(".lazy-table");
        if (wrap) wrap.innerHTML = this.tableHTML(entry.data, entry.columns);
        return false;
    },

    /** Format number as currency */
    currency(value) {
        return "$" + Number(value).toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
    },

    /** Format large number */
    formatNumber(value) {
        return Number(value).toLocaleString("en-US");
    },

    /** Create a metric card HTML */
    metricHTML(label, value, delta = null) {
        let deltaHTML = "";
        if (delta !== null) {
            const cls = delta >= 0 ? "positive" : "negative";
            deltaHTML = `<div class="metric-delta ${cls}">${delta >= 0 ? "+" : ""}${delta.toFixed(2)}%</div>`;
        }
        return `
            <div class="metric">
                <div class="metric-label">${label}</div>
                <div class="metric-value">${value}</div>
                ${deltaHTML}
            </div>`;
    },

    /** Create table HTML from array of objects */
    tableHTML(data, columns = null) {
        if (!data || data.length === 0) return '<p class="section-caption">No data available.</p>';

        const cols = columns || Object.keys(data[0]);
        let html = '<div class="table-container"><table>';
        html += "<thead><tr>" + cols.map(c => `<th>${c}</th>`).join("") + "</tr></thead>";
        html += "<tbody>";
        for (const row of data) {
            html += "<tr>" + cols.map(c => `<td>${row[c] ?? ""}</td>`).join("") + "</tr>";
        }
        html += "</tbody></table></div>";
        return html;
    },

    /** Create accordion HTML */
    accordionHTML(id, title, content, open = false) {
        return `
            <div class="accordion ${open ? "open" : ""}" id="acc-${id}">
                <div class="accordion-header" onclick="document.getElementById('acc-${id}').classList.toggle('open')">
                    <span>${title}</span>
                    <span class="accordion-arrow">&#9660;</span>
                </div>
                <div class="accordion-body">${content}</div>
            </div>`;
    },
};

// Boot the app when DOM is ready
document.addEventListener("DOMContentLoaded", () => App.init());
