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
            orderbook:           Orderbook,
            portfolio:           Portfolio,
            portfolio_tearsheet: PortfolioTearsheet,
        };
    },

    _logCount: 0,
    _logCollapsed: false,

    /** Initialize the app.
     *
     *  First step is the user-picker gate: every API call carries an
     *  X-User-Id header (see this.api), so we MUST resolve the active
     *  user before any module renders. If localStorage already has one,
     *  we trust it and continue; otherwise we show a blocking picker.
     */
    async init() {
        this.bindNavigation();
        this.initTheme();
        await this._ensureUserSelected();
        await this._refreshCurrentUser();
        this._renderUserChip();
        this.log(`Signed in as ${this.getUserAlias() || this.getUserId()}`, "MESSAGE", "SYSTEM");
        this.navigate("dashboard");
    },

    // ─── Theme (dark / light) ─────────────────────────────────────────────
    //
    // The chosen theme is applied early by an inline script in index.html
    // (so the first paint is correct). This block syncs in-page state with
    // that choice, lets the user toggle it, and exposes a Plotly layout
    // partial so charts re-render with theme-appropriate colors.

    /** Read the current theme from the DOM ("light" or "dark"). */
    getTheme() {
        return document.documentElement.getAttribute("data-theme") === "dark" ? "dark" : "light";
    },

    /** Persist + apply a theme. Repaints any already-rendered Plotly charts
     *  in place so their backgrounds and font colors match. */
    setTheme(theme) {
        const t = theme === "dark" ? "dark" : "light";
        document.documentElement.setAttribute("data-theme", t);
        try { localStorage.setItem("theme", t); } catch {}
        this._repaintPlotlyCharts();
    },

    /** Flip the theme. Wired to the header button via onclick. */
    toggleTheme() {
        this.setTheme(this.getTheme() === "dark" ? "light" : "dark");
    },

    /** Read whatever was set by the inline early-paint script (or apply
     *  the OS preference if nothing is stored). Idempotent — safe to call
     *  on every init. */
    initTheme() {
        const stored = (() => {
            try { return localStorage.getItem("theme"); } catch { return null; }
        })();
        if (stored === "light" || stored === "dark") {
            document.documentElement.setAttribute("data-theme", stored);
        } else if (!document.documentElement.getAttribute("data-theme")) {
            const prefersDark = window.matchMedia && window.matchMedia("(prefers-color-scheme: dark)").matches;
            document.documentElement.setAttribute("data-theme", prefersDark ? "dark" : "light");
        }
    },

    /** Plotly layout partial that matches the current theme. Spread this
     *  into chart layouts (`{ ...App.plotlyTheme(), title: ... }`) — the
     *  plot() helper also auto-merges it as a fallback. */
    plotlyTheme() {
        const dark = this.getTheme() === "dark";
        const css = getComputedStyle(document.documentElement);
        const pick = (name, fallback) => (css.getPropertyValue(name).trim() || fallback);
        const bg = pick("--bg-card", dark ? "#1e242c" : "#ffffff");
        const text = pick("--text-primary", dark ? "#e6edf3" : "#1a1a1a");
        const grid = pick("--border-light", dark ? "#2a3340" : "#e5e7eb");
        return {
            paper_bgcolor: bg,
            plot_bgcolor: bg,
            font: { color: text },
            xaxis: { gridcolor: grid, zerolinecolor: grid, linecolor: grid, tickcolor: grid },
            yaxis: { gridcolor: grid, zerolinecolor: grid, linecolor: grid, tickcolor: grid },
            legend: { bgcolor: "rgba(0,0,0,0)", font: { color: text } },
        };
    },

    /** Re-apply the current Plotly theme to every chart already on the page.
     *  Only touches layout (colors / fonts) — trace data is untouched. */
    _repaintPlotlyCharts() {
        if (typeof Plotly === "undefined") return;
        const theme = this.plotlyTheme();
        // Flatten the nested xaxis/yaxis/legend/font objects into Plotly's
        // dotted-path relayout syntax so we don't blow away existing settings.
        const update = {
            "paper_bgcolor": theme.paper_bgcolor,
            "plot_bgcolor": theme.plot_bgcolor,
            "font.color": theme.font.color,
            "xaxis.gridcolor": theme.xaxis.gridcolor,
            "xaxis.zerolinecolor": theme.xaxis.zerolinecolor,
            "xaxis.linecolor": theme.xaxis.linecolor,
            "xaxis.tickcolor": theme.xaxis.tickcolor,
            "yaxis.gridcolor": theme.yaxis.gridcolor,
            "yaxis.zerolinecolor": theme.yaxis.zerolinecolor,
            "yaxis.linecolor": theme.yaxis.linecolor,
            "yaxis.tickcolor": theme.yaxis.tickcolor,
            "legend.bgcolor": theme.legend.bgcolor,
            "legend.font.color": theme.legend.font.color,
        };
        document.querySelectorAll(".js-plotly-plot").forEach((el) => {
            try { Plotly.relayout(el, update); } catch (_) { /* non-fatal */ }
        });
    },

    // ─── User identity (X-User-Id, no auth) ───────────────────────────────

    /** Cached user list from /api/users/list (refreshed by the picker). */
    _userCache: null,

    /** Cached active-user row from /api/users/me. Includes multiplier and
     *  allowed_instruments — surfaced read-only in the header chip so the
     *  user can see what the server will apply to their orders. */
    _currentUser: null,

    /** Refresh ``_currentUser`` from /api/users/me. Direct fetch (NOT
     *  this.api) so we can call it during init before the rest of the
     *  router boots. Failures degrade gracefully — the chip falls back
     *  to alias-only. */
    async _refreshCurrentUser() {
        try {
            const res = await fetch("/api/users/me", { headers: this.userHeaders() });
            const data = await res.json().catch(() => ({}));
            if (data && data.user_id) {
                this._currentUser = data;
            }
        } catch (_e) { /* non-fatal */ }
    },

    /** Get the active user_id from localStorage, or null. */
    getUserId() {
        try { return localStorage.getItem("user_id") || null; } catch { return null; }
    },

    /** Get the active user's display alias, falling back to user_id. */
    getUserAlias() {
        const uid = this.getUserId();
        if (!uid) return null;
        const entry = (this._userCache || []).find(u => u.user_id === uid);
        return entry ? (entry.alias || uid) : uid;
    },

    /** Block until a user_id is in localStorage, prompting if needed.
     *  Called once on init; the "Switch user" link calls _showUserPicker
     *  directly to re-prompt without going through the rest of init.
     */
    async _ensureUserSelected() {
        // Fetch the user list once so the picker can render. Direct fetch
        // (NOT this.api) because the api wrapper would loop on missing
        // header — we're explicitly bootstrapping that header.
        try {
            const res = await fetch("/api/users/list");
            const data = await res.json().catch(() => ({}));
            this._userCache = Array.isArray(data.users) ? data.users : [];
        } catch (_e) {
            this._userCache = [];
        }
        const stored = this.getUserId();
        const known = stored && this._userCache.some(u => u.user_id === stored);
        if (known) return;
        // Stored id no longer exists in the registry → clear and re-prompt.
        if (stored && !known) {
            try { localStorage.removeItem("user_id"); } catch {}
        }
        await this._showUserPicker(/*allowCancel*/ false);
    },

    /** Render the user chip inside the header bar (next to Catalog Path).
     *  Shows alias + multiplier + allowed_instruments summary read-only.
     *  Click opens the picker for switching; editing multiplier or allowlist
     *  is admin-only (see adapter_admin Users page on port 5001).
     */
    _renderUserChip() {
        const host = document.querySelector(".header-settings") || document.body;
        let chip = document.getElementById("user-chip");
        if (!chip) {
            chip = document.createElement("button");
            chip.id = "user-chip";
            chip.type = "button";
            // Match .header-settings input styling so the chip reads as part
            // of the header rather than a floater. Larger gap accommodates
            // the multiplier/allowlist meta strip.
            chip.style.cssText = "margin-left:12px; padding:4px 10px; font-size:0.78rem; border:1px solid var(--border-color); border-radius:var(--radius-sm); background:var(--bg-input); color:var(--text-primary); cursor:pointer; display:inline-flex; align-items:center; gap:8px; max-width:520px;";
            chip.onclick = () => this._showUserPicker(true);
            host.appendChild(chip);
        }

        const u = this._currentUser || {};
        const alias = (u.alias || this.getUserAlias() || "—");
        const multiplier = (u.multiplier !== undefined ? Number(u.multiplier) : null);
        const multStr = multiplier !== null ? `×${multiplier}` : "";
        const allowed = u.allowed_instruments;

        // Allowlist summary: null/undefined → "All", short list → joined,
        // long list → first two + count. Keeps the header compact while the
        // tooltip still carries the full list.
        let allowedStr;
        if (!Array.isArray(allowed)) {
            allowedStr = "All";
        } else if (allowed.length === 0) {
            allowedStr = "(none)";
        } else if (allowed.length <= 3) {
            allowedStr = allowed.join(", ");
        } else {
            allowedStr = `${allowed.slice(0, 2).join(", ")} +${allowed.length - 2}`;
        }

        // Tooltip with the full picture for users who want detail.
        const fullAllowed = Array.isArray(allowed)
            ? (allowed.length ? allowed.join(", ") : "(none — cannot save any portfolio)")
            : "All instruments";
        chip.title = `User: ${alias}\nMultiplier: ${multStr || "(unknown)"}\nAllowed instruments: ${fullAllowed}\n(read-only — admin edits via :5001)\n\nClick to switch user`;

        const sep = `<span style="color:var(--border-color);">|</span>`;
        chip.innerHTML = `
            <span aria-hidden="true">&#128100;</span>
            <strong>${this._htmlEscape(alias)}</strong>
            ${multStr ? `${sep}<span style="color:var(--text-secondary);" title="Order-quantity multiplier">${multStr}</span>` : ""}
            ${sep}<span style="color:var(--text-secondary);" title="Allowed instruments">${this._htmlEscape(allowedStr)}</span>
            ${sep}<span style="color:var(--text-secondary); font-weight:400;">(switch)</span>
        `;
    },

    /** Show a blocking modal listing known users. Resolves once a user
     *  is picked. ``allowCancel`` decides whether the dialog can be
     *  dismissed without picking — false on first-load, true when
     *  switching (where the existing identity is still valid). */
    _showUserPicker(allowCancel) {
        return new Promise((resolve) => {
            const overlay = document.createElement("div");
            overlay.style.cssText = "position:fixed; inset:0; background:rgba(0,0,0,0.4); z-index:2000; display:flex; align-items:center; justify-content:center;";
            const modal = document.createElement("div");
            // Use the project's light-theme tokens so the modal matches
            // the rest of the UI (was hard-coded dark before — clashed).
            modal.style.cssText = "background:var(--bg-primary, #fff); color:var(--text-primary, #1f2937); padding:24px; border-radius:var(--radius-md, 8px); min-width:360px; max-width:520px; box-shadow:var(--shadow-lg, 0 10px 30px rgba(0,0,0,0.15)); border:1px solid var(--border-color, #e5e7eb);";

            const users = this._userCache || [];
            const optsHtml = users.map(u =>
                `<option value="${this._htmlEscape(u.user_id)}">${this._htmlEscape(u.alias || u.user_id)} (${this._htmlEscape(u.user_id)})</option>`
            ).join("");

            const current = this.getUserId() || (users[0] && users[0].user_id) || "";
            modal.innerHTML = `
                <h3 style="margin-top:0; color:var(--text-primary);">Choose user</h3>
                <p style="color:var(--text-secondary, #6b7280); font-size:0.85rem;">
                    Identity-only — no password. The picked id goes into every
                    API call as <code>X-User-Id</code>. Use only in trusted environments.
                </p>
                <select id="user-picker-select" class="form-control" style="width:100%; margin-bottom:14px;">
                    ${optsHtml || '<option value="">(no users — admin must seed config/users.json)</option>'}
                </select>
                <div style="display:flex; gap:8px; justify-content:flex-end;">
                    ${allowCancel ? '<button class="btn btn-secondary" id="user-picker-cancel">Cancel</button>' : ""}
                    <button class="btn btn-primary" id="user-picker-ok" ${users.length ? "" : "disabled"}>Continue</button>
                </div>`;

            overlay.appendChild(modal);
            document.body.appendChild(overlay);

            const sel = modal.querySelector("#user-picker-select");
            if (current) sel.value = current;
            modal.querySelector("#user-picker-ok").onclick = async () => {
                const picked = sel.value;
                if (!picked) return;
                try { localStorage.setItem("user_id", picked); } catch {}
                document.body.removeChild(overlay);
                // Refresh the active-user row so the chip reflects the new
                // multiplier / allowed_instruments before the next render.
                await this._refreshCurrentUser();
                this._renderUserChip();
                // Drop cached responses since they were keyed implicitly
                // to the previous user.
                this.invalidateCache && this.invalidateCache();
                resolve(picked);
            };
            const cancelBtn = modal.querySelector("#user-picker-cancel");
            if (cancelBtn) cancelBtn.onclick = () => {
                document.body.removeChild(overlay);
                resolve(this.getUserId());
            };
        });
    },

    _htmlEscape(s) {
        return String(s).replace(/[&<>"']/g, c => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
        }[c]));
    },

    /** Build headers for an API call with the X-User-Id injected. Exposed
     *  so direct fetch() callers (streaming endpoints, file uploads —
     *  see backtest.js, portfolio.js) can use the same identity rule. */
    userHeaders(extra = {}) {
        const uid = this.getUserId();
        const h = { ...extra };
        if (uid) h["X-User-Id"] = uid;
        return h;
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
            backtest: "Run Backtest", tearsheet: "Tearsheet", orderbook: "Orderbook",
            portfolio: "Multileg", portfolio_tearsheet: "Portfolio Tearsheet",
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
            // Identity header is injected on every API call. Callers that
            // pass a `headers` option in `rest` can override it (rare —
            // mostly for tests).
            const baseHeaders = { "Content-Type": "application/json", ...this.userHeaders() };
            const mergedHeaders = rest.headers ? { ...baseHeaders, ...rest.headers } : baseHeaders;
            const { headers: _hdr, ...restNoHeaders } = rest;
            const response = await fetch(endpoint, {
                headers: mergedHeaders,
                signal: ac.signal,
                ...restNoHeaders,
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
     *  Safe to call before Plotly finishes loading (deferred <script>) — it waits.
     *
     *  Auto-merges the current theme into `layout` so every chart picks up
     *  dark/light backgrounds without each page having to remember. The
     *  caller's layout wins — explicit colors are preserved. */
    async plot(divId, traces, layout, config = { responsive: true }) {
        const el = typeof divId === "string" ? document.getElementById(divId) : divId;
        if (!el) return;
        if (typeof Plotly === "undefined") {
            await this._plotlyReady();
            if (!el.isConnected) return; // user navigated away
        }
        const themed = this._mergeTheme(layout || {});
        // If Plotly has already rendered into this node it will have _fullData.
        if (el._fullData) {
            Plotly.react(el, traces, themed, config);
        } else {
            Plotly.newPlot(el, traces, themed, config);
        }
    },

    /** Merge the active Plotly theme with a caller-supplied layout. Caller's
     *  fields win; theme defaults fill in the gaps. */
    _mergeTheme(layout) {
        const theme = this.plotlyTheme();
        const merged = { ...theme, ...layout };
        // Nested axis/legend/font objects need a shallow merge of their own
        // so per-chart settings (title, range, etc.) survive.
        merged.xaxis = { ...theme.xaxis, ...(layout.xaxis || {}) };
        merged.yaxis = { ...theme.yaxis, ...(layout.yaxis || {}) };
        merged.font  = { ...theme.font,  ...(layout.font  || {}) };
        merged.legend = { ...theme.legend, ...(layout.legend || {}) };
        return merged;
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
