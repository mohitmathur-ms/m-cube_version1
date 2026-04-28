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

    /** Per-page <div> containers that we keep alive across navigations.
     *  Hiding (display:none) instead of destroying preserves form values,
     *  scroll position, Plotly chart state and any module-local DOM refs.
     *  Cleared only on full page reload. */
    _pages: {},
    _pageInitialized: {},

    /** Map of page id → module. Single source of truth for the router. */
    _pageModules() {
        return {
            dashboard:           Dashboard,
            load_data:           LoadData,
            view_data:           ViewData,
            backtest:            Backtest,
            tearsheet:           Tearsheet,
            portfolio:           Portfolio,
            portfolio_tearsheet: PortfolioTearsheet,
        };
    },

    _logCount: 0,
    _logCollapsed: false,

    /** Initialize the app */
    init() {
        this.bindNavigation();
        this.log("System initialized", "MESSAGE", "SYSTEM");
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

    /** Navigate to a page.
     *  First visit: build a dedicated <div> container and call Module.render().
     *  Subsequent visits: just toggle visibility — DOM (and therefore every
     *  form selection, dropdown value, date input, scroll position, chart
     *  zoom) survives. If the page exposes onShow(container), it's invoked
     *  on every nav-in (including the first) so result-dependent tabs can
     *  refresh themselves when underlying state has changed. */
    navigate(page) {
        // Cancel any in-flight requests from the previous page — prevents
        // stale fetches from clobbering the new page's DOM.
        for (const ac of this._pendingAborters) ac.abort();
        this._pendingAborters.clear();

        const modules = this._pageModules();
        const mod = modules[page];
        if (!mod) return;

        this.currentPage = page;

        // Update active nav item
        document.querySelectorAll(".nav-item").forEach(item => {
            item.classList.toggle("active", item.dataset.page === page);
        });

        // Clear action bar for new page
        this.setActionBar("");

        // Log navigation
        const pageNames = {
            dashboard: "Dashboard", load_data: "Load Data", view_data: "View Data",
            backtest: "Run Backtest", tearsheet: "Tearsheet", portfolio: "Multileg",
            portfolio_tearsheet: "Portfolio Tearsheet",
        };
        this.log(`${pageNames[page] || page} tab loaded`, "MESSAGE", "SYSTEM", pageNames[page] || "");
        const main = document.getElementById("main-content");

        // Hide every other page's container.
        for (const [name, el] of Object.entries(this._pages)) {
            if (name !== page && el) el.style.display = "none";
        }

        // Get or create this page's container.
        let container = this._pages[page];
        const isFirstVisit = !container;
        if (isFirstVisit) {
            container = document.createElement("div");
            container.id = `page-${page}`;
            container.className = "page-container";
            main.appendChild(container);
            this._pages[page] = container;
        }
        container.style.display = "";

        // First visit: build the page's DOM. Subsequent visits: skip — the
        // existing DOM is reused so user selections persist.
        if (isFirstVisit) {
            try {
                mod.render(container);
            } catch (e) {
                container.innerHTML = `<div class="alert alert-danger">Failed to load page: ${e && e.message ? e.message : e}</div>`;
                console.error(`[App.navigate] render(${page}) threw:`, e);
            }
            this._pageInitialized[page] = true;
        }

        // Optional per-page hook fired on every show (including the first).
        // Pages that depend on App.state (e.g. Tearsheet on backtestResults)
        // implement this to re-render when the underlying data changes.
        if (typeof mod.onShow === "function") {
            try {
                mod.onShow(container, { firstVisit: isFirstVisit });
            } catch (e) {
                console.error(`[App.navigate] onShow(${page}) threw:`, e);
            }
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

    /** Make API call. Auto-aborts on navigate unless options.keepalive=true.
     *  Pass timeoutMs: 0 to disable the client-side timeout for long,
     *  user-initiated jobs (e.g. catalog ingest of multi-million-row datasets). */
    async api(endpoint, options = {}) {
        const { keepalive, timeoutMs = 60000, ...rest } = options;
        const ac = new AbortController();
        const timer = timeoutMs > 0 ? setTimeout(() => ac.abort(), timeoutMs) : null;
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
            if (timer) clearTimeout(timer);
            this._pendingAborters.delete(ac);
        }
    },

    /** Pending request aborters, cancelled on navigation. */
    _pendingAborters: new Set(),

    /** In-memory GET cache keyed by endpoint. Cleared on page reload. */
    _apiCache: new Map(),

    /** Cached GET. Returns the cached response if present and fresh, else
     *  fetches once and shares the in-flight promise across concurrent callers
     *  so duplicate requests collapse. Use for *config* endpoints whose values
     *  rarely change within a session (asset-classes, strategies, bar_types,
     *  templates, …). Pass `{force: true}` to bust. Default TTL: 5 minutes. */
    async cachedApi(endpoint, options = {}) {
        const { ttlMs = 5 * 60 * 1000, force = false, ...apiOpts } = options;
        const now = Date.now();
        const entry = this._apiCache.get(endpoint);
        if (!force && entry && entry.expires > now) {
            // Either a resolved value or an in-flight promise — both work.
            return entry.value instanceof Promise ? entry.value : entry.value;
        }
        const promise = this.api(endpoint, apiOpts).then(
            (val) => {
                this._apiCache.set(endpoint, { value: val, expires: Date.now() + ttlMs });
                return val;
            },
            (err) => {
                this._apiCache.delete(endpoint);
                throw err;
            },
        );
        this._apiCache.set(endpoint, { value: promise, expires: now + ttlMs });
        return promise;
    },

    /** Drop one or all entries from the API cache (e.g. after an ingest
     *  invalidates bar_types). */
    invalidateCache(endpoint) {
        if (endpoint == null) this._apiCache.clear();
        else this._apiCache.delete(endpoint);
    },

    /** Repopulate a <select>'s options without dropping the current value if
     *  it still appears among the new options. Pass an array of either
     *  `"value"` strings or `{value, label}` / `{value, text}` objects. */
    repopulateSelect(sel, options, { keepSelection = true } = {}) {
        if (!sel) return;
        const prev = keepSelection ? sel.value : null;
        const html = options.map(o => {
            if (typeof o === "string") return `<option value="${o}">${o}</option>`;
            const v = o.value;
            const t = o.label ?? o.text ?? v;
            return `<option value="${v}">${t}</option>`;
        }).join("");
        sel.innerHTML = html;
        if (keepSelection && prev != null) {
            const match = [...sel.options].some(o => o.value === prev);
            if (match) sel.value = prev;
        }
    },

    barTypeLabel(bt) {
        if (!bt) return "";
        const parts = String(bt).split("-");
        const inst = parts[0] || "";
        const pt = parts[3] || "";
        if (!pt || !inst.includes(".")) return inst;
        return inst.replace(".", `(${pt}).`);
    },

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

    /* ─── Activity Log System ────────────────────────────────────────────── */

    /** Add an entry to the persistent activity log panel.
     *  @param {string} message  - Log message
     *  @param {string} type     - MESSAGE | SUCCESS | ERROR | WARNING
     *  @param {string} source   - SYSTEM, or page/module name
     *  @param {string} stratTag - Optional strategy tag */
    log(message, type = "MESSAGE", source = "SYSTEM", stratTag = "") {
        const tbody = document.getElementById("log-tbody");
        if (!tbody) return;

        const now = new Date();
        const ts = now.getFullYear() + "-" +
            String(now.getMonth() + 1).padStart(2, "0") + "-" +
            String(now.getDate()).padStart(2, "0") + " " +
            String(now.getHours()).padStart(2, "0") + ":" +
            String(now.getMinutes()).padStart(2, "0") + ":" +
            String(now.getSeconds()).padStart(2, "0");

        const typeClass = {
            MESSAGE: "log-type-message",
            SUCCESS: "log-type-success",
            ERROR: "log-type-error",
            WARNING: "log-type-warning",
        }[type] || "log-type-message";

        const row = document.createElement("tr");
        row.innerHTML = `
            <td>${ts}</td>
            <td class="${typeClass}">${type}</td>
            <td>${message}</td>
            <td>${source}</td>
            <td>${stratTag}</td>
        `;

        // Insert at top (newest first)
        tbody.insertBefore(row, tbody.firstChild);

        this._logCount++;
        const badge = document.getElementById("log-count-badge");
        if (badge) {
            badge.textContent = this._logCount;
            badge.style.display = "";
        }

        // Auto-scroll to top
        const panel = document.getElementById("log-panel");
        if (panel) panel.scrollTop = 0;
    },

    /** Toggle log panel visibility */
    toggleLog() {
        this._logCollapsed = !this._logCollapsed;
        document.body.classList.toggle("log-collapsed", this._logCollapsed);
        const btn = document.getElementById("log-toggle-btn");
        if (btn) {
            const badge = document.getElementById("log-count-badge");
            const badgeHTML = badge ? badge.outerHTML : "";
            btn.innerHTML = this._logCollapsed ? `Show Log ${badgeHTML}` : `Log ${badgeHTML}`;
        }
    },

    /** Clear all log entries */
    clearLog() {
        const tbody = document.getElementById("log-tbody");
        if (tbody) tbody.innerHTML = "";
        this._logCount = 0;
        const badge = document.getElementById("log-count-badge");
        if (badge) badge.style.display = "none";
    },

    /** Set page-specific buttons in the action bar.
     *  @param {string} html - Button HTML to inject */
    setActionBar(html) {
        const el = document.getElementById("action-bar-buttons");
        if (el) el.innerHTML = html || "";
    },
};

// Boot the app when DOM is ready
document.addEventListener("DOMContentLoaded", () => App.init());
