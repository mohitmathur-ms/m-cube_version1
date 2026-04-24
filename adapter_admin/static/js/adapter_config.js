/**
 * Adapter Config Page - Add or edit adapter configurations.
 */

const AdapterConfig = {
    registry: {},
    selectedExchange: null,
    isEditMode: false,
    editData: null,

    _assetClasses: null,
    _selectedAssetFilter: "all",

    // Catalog pairs for the currently-selected venue (populated on renderForm).
    // Used to populate the FX Conversion dropdowns so admins can bind a
    // currency to an actual ingested bar series instead of typing pair names
    // from memory.
    _catalogPairs: [],
    // In-memory FX conversion state while editing the form — committed to the
    // config payload on save(). Keyed by currency code, e.g. {"JPY": {...}}.
    _fxConversion: {},
    _accountBaseCurrency: "USD",

    // Which NautilusTrader AssetClass each adapter supports
    adapterAssets: {
        "Binance":              ["CRYPTOCURRENCY"],
        "Bybit":                ["CRYPTOCURRENCY"],
        "OKX":                  ["CRYPTOCURRENCY"],
        "Kraken":               ["CRYPTOCURRENCY"],
        "Deribit":              ["CRYPTOCURRENCY"],
        "dYdX":                 ["CRYPTOCURRENCY"],
        "Hyperliquid":          ["CRYPTOCURRENCY"],
        "BitMEX":               ["CRYPTOCURRENCY"],
        "ArchitectAX":          ["CRYPTOCURRENCY"],
        "InteractiveBrokers":   ["CRYPTOCURRENCY", "EQUITY", "FX", "COMMODITY", "DEBT", "INDEX"],
        "Databento":            ["EQUITY", "COMMODITY", "INDEX"],
        "Betfair":              ["ALTERNATIVE"],
        "Polymarket":           ["ALTERNATIVE"],
        "Tardis":               ["CRYPTOCURRENCY"],
    },

    async render(container) {
        this.isEditMode = !!App.state.editingAdapter;
        this.editData = App.state.editingAdapter;

        // Load registry and asset classes
        try {
            const [regData, acData] = await Promise.all([
                App.api("/api/registry"),
                App.api("/api/asset-classes"),
            ]);
            this.registry = regData.registry;
            window._registryCache = this.registry;
            if (acData.asset_classes && acData.asset_classes.length > 0) {
                this._assetClasses = acData.asset_classes;
            }
        } catch (e) {
            container.innerHTML = `<div class="alert alert-danger">Failed to load adapter registry: ${e.message}</div>`;
            return;
        }

        if (this.isEditMode) {
            // Edit mode — load full config with revealed secrets
            try {
                const data = await App.api(`/api/adapters/${this.editData.id}?reveal=true`);
                this.editData = data.adapter;
            } catch (e) {
                container.innerHTML = `<div class="alert alert-danger">Failed to load adapter: ${e.message}</div>`;
                return;
            }
            this.selectedExchange = this.editData.exchange_type;
            this.renderForm(container);
        } else {
            // Add mode — show exchange selection
            App.state.editingAdapter = null;
            this.selectedExchange = null;
            this.renderExchangeSelection(container);
        }
    },

    renderExchangeSelection(container) {
        // Collect all unique asset classes from the mapping
        const allAssets = new Set();
        for (const assets of Object.values(this.adapterAssets)) {
            assets.forEach(a => allAssets.add(a));
        }
        const assetFilterOptions = ["all", ...Array.from(allAssets).sort()].map(a => {
            const label = a === "all" ? "All Asset Classes" : a;
            const selected = a === this._selectedAssetFilter ? "selected" : "";
            return `<option value="${a}" ${selected}>${label}</option>`;
        }).join("");

        let cardsHTML = "";
        for (const [name, entry] of Object.entries(this.registry)) {
            const caps = [];
            if (entry.supports_data) caps.push('<span class="badge badge-info" style="font-size:0.7rem;">Data</span>');
            if (entry.supports_exec) caps.push('<span class="badge badge-info" style="font-size:0.7rem;">Exec</span>');

            const sourceBadge = entry.source === "custom"
                ? '<span class="badge badge-warning" style="font-size:0.7rem;">Custom</span>'
                : '<span class="badge badge-success" style="font-size:0.7rem;">Built-in</span>';

            const paramCount = Object.keys(entry.params || {}).length;
            const adapterAssets = this.adapterAssets[name] || [];
            const assetTags = adapterAssets.map(a =>
                `<span class="badge" style="font-size:0.65rem; background:var(--bg-input); color:var(--text-secondary); margin-right:3px;">${a}</span>`
            ).join("");

            cardsHTML += `
                <div class="card exchange-card" data-name="${name.toLowerCase()}" data-desc="${(entry.description || '').toLowerCase()}" data-assets="${adapterAssets.join(',')}" onclick="AdapterConfig.selectExchange('${name}')">
                    <div class="exchange-name">${name}</div>
                    <div class="exchange-desc">${entry.description}</div>
                    <div style="margin-top:4px;">${caps.join(" ")} ${sourceBadge}</div>
                    <div style="margin-top:4px;">${assetTags}</div>
                    <div style="font-size:0.72rem; color:var(--text-muted); margin-top:4px;">${paramCount} configurable fields</div>
                </div>`;
        }

        container.innerHTML = `
            <h1 class="page-title">&#10133; Add Adapter</h1>
            <p class="page-subtitle">Choose an exchange to configure. Showing all discovered NautilusTrader adapters.</p>

            <div class="stepper">
                <div class="step active">
                    <div class="step-number">1</div>
                    <span>Select Exchange</span>
                </div>
                <div class="step-connector"></div>
                <div class="step">
                    <div class="step-number">2</div>
                    <span>Configure</span>
                </div>
                <div class="step-connector"></div>
                <div class="step">
                    <div class="step-number">3</div>
                    <span>Save</span>
                </div>
            </div>

            <div style="display:flex; gap:12px; margin-bottom:16px; align-items:center; flex-wrap:wrap;">
                <select id="asset-filter" class="form-control" style="max-width:200px;" onchange="AdapterConfig.filterByAsset()">
                    ${assetFilterOptions}
                </select>
                <input type="text" id="exchange-search" class="form-control" style="max-width:350px;"
                       placeholder="Search adapters..." oninput="AdapterConfig.filterExchanges()">
                <span id="exchange-count" style="font-size:0.85rem; color:var(--text-muted);">
                    ${Object.keys(this.registry).length} adapters available
                </span>
                <button class="btn btn-sm" style="margin-left:auto;" onclick="AdapterConfig.refreshRegistry()">Refresh</button>
            </div>

            <div class="grid-3" id="exchange-grid">${cardsHTML}</div>
        `;
    },

    filterByAsset() {
        const select = document.getElementById("asset-filter");
        this._selectedAssetFilter = select ? select.value : "all";
        this.filterExchanges();
    },

    filterExchanges() {
        const searchEl = document.getElementById("exchange-search");
        const q = (searchEl ? searchEl.value : "").toLowerCase().trim();
        const assetFilter = this._selectedAssetFilter || "all";
        const cards = document.querySelectorAll("#exchange-grid .exchange-card");
        let visible = 0;
        cards.forEach(card => {
            const name = card.getAttribute("data-name") || "";
            const desc = card.getAttribute("data-desc") || "";
            const assets = card.getAttribute("data-assets") || "";

            const matchesSearch = !q || name.includes(q) || desc.includes(q);
            const matchesAsset = assetFilter === "all" || assets.split(",").includes(assetFilter);

            card.style.display = (matchesSearch && matchesAsset) ? "" : "none";
            if (matchesSearch && matchesAsset) visible++;
        });
        const countEl = document.getElementById("exchange-count");
        if (countEl) countEl.textContent = `${visible} of ${cards.length} adapters`;
    },

    async refreshRegistry() {
        try {
            const data = await App.api("/api/registry/refresh", { method: "POST" });
            this.registry = data.registry;
            window._registryCache = this.registry;
            App.toast(`Registry refreshed — ${Object.keys(this.registry).length} adapters found`, "success");
            this.renderExchangeSelection(document.getElementById("main-content"));
        } catch (e) {
            App.toast("Failed to refresh registry: " + e.message, "error");
        }
    },

    selectExchange(name) {
        this.selectedExchange = name;
        this.renderForm(document.getElementById("main-content"));
    },

    async renderForm(container) {
        const regEntry = this.registry[this.selectedExchange];
        const params = regEntry ? regEntry.params : {};
        const isCustom = regEntry && regEntry.source === "custom";

        const title = this.isEditMode ? `Edit: ${this.editData.name}` : `Configure ${this.selectedExchange} Adapter`;

        // Seed FX conversion editor state from the edited config (or blank for new).
        this._accountBaseCurrency = this.isEditMode
            ? ((this.editData.account_base_currency || "USD").toUpperCase())
            : "USD";
        this._fxConversion = this.isEditMode
            ? JSON.parse(JSON.stringify(this.editData.fx_conversion || {}))
            : {};

        // Kick off catalog-pair discovery in the background; we re-render the
        // FX section once results arrive. This keeps the form paint fast on
        // venues with no catalog data yet.
        this._catalogPairs = [];
        const venueForCatalog = (this.isEditMode ? this.editData.venue : null)
            || this.selectedExchange.toUpperCase().replace(/[^A-Z0-9]/g, '');
        this._loadCatalogPairs(venueForCatalog);

        let formHTML = "";

        // Name, Venue, and Asset Class — only show asset classes this adapter supports
        const supportedAssets = (this.adapterAssets[this.selectedExchange] || [])
            .map(a => a.toLowerCase());
        const allAssetClasses = this._assetClasses || ["cryptocurrency", "equity", "fx", "commodity", "debt", "index", "alternative"];
        const filteredAssetClasses = supportedAssets.length > 0
            ? allAssetClasses.filter(a => supportedAssets.includes(a))
            : allAssetClasses;
        const currentAssetClass = this.isEditMode
            ? (this.editData.asset_class || filteredAssetClasses[0] || "cryptocurrency")
            : (filteredAssetClasses[0] || "cryptocurrency");
        const assetClassOptions = filteredAssetClasses.map(a =>
            `<option value="${a}" ${a === currentAssetClass ? "selected" : ""}>${a.toUpperCase()}</option>`
        ).join("");

        formHTML += `
            <div class="form-row">
                <div class="form-group">
                    <label class="form-label">Adapter Name</label>
                    <input type="text" id="adapter-name" class="form-control"
                           value="${this.isEditMode ? this.editData.name : ''}"
                           placeholder="e.g. ${this.selectedExchange} Production">
                </div>
                <div class="form-group">
                    <label class="form-label">Venue Name</label>
                    <input type="text" id="adapter-venue" class="form-control"
                           value="${this.isEditMode ? this.editData.venue : this.selectedExchange.toUpperCase().replace(/[^A-Z0-9]/g, '')}"
                           placeholder="e.g. BINANCE">
                </div>
                <div class="form-group">
                    <label class="form-label">Asset Class</label>
                    <select id="adapter-asset-class" class="form-control">
                        ${assetClassOptions}
                    </select>
                </div>
            </div>`;

        // Dynamic params from registry
        if (Object.keys(params).length > 0) {
            formHTML += '<h2 class="section-title" style="margin-top:24px;">Connection Parameters</h2>';
            formHTML += '<div class="grid-2">';

            for (const [paramName, paramDef] of Object.entries(params)) {
                const existingVal = this.isEditMode
                    ? (this.editData.data_config?.[paramName] ?? this.editData.exec_config?.[paramName] ?? paramDef.default ?? "")
                    : (paramDef.default ?? "");

                formHTML += `<div class="form-group">`;
                formHTML += `<label class="form-label">${paramDef.label}</label>`;

                if (paramDef.type === "select") {
                    const opts = (paramDef.options || []).map(o =>
                        `<option value="${o}" ${String(o) === String(existingVal) ? 'selected' : ''}>${o}</option>`
                    ).join("");
                    // Add empty option for optional selects
                    const emptyOpt = !paramDef.required && existingVal === null
                        ? '<option value="" selected>-- None --</option>'
                        : (existingVal === null ? '<option value="" selected>-- None --</option>' : '');
                    formHTML += `<select id="param-${paramName}" class="form-control">${emptyOpt}${opts}</select>`;
                } else if (paramDef.type === "checkbox") {
                    const checked = existingVal === true || existingVal === "true" ? "checked" : "";
                    formHTML += `
                        <div class="checkbox-row">
                            <input type="checkbox" id="param-${paramName}" ${checked}>
                            <label for="param-${paramName}">${paramDef.label}</label>
                        </div>`;
                } else if (paramDef.type === "password") {
                    formHTML += `
                        <div class="password-wrapper">
                            <input type="password" id="param-${paramName}" class="form-control"
                                   value="${existingVal || ''}" placeholder="${paramDef.placeholder || ''}">
                            <button type="button" class="password-toggle" onclick="AdapterConfig.togglePassword('param-${paramName}')">Show</button>
                        </div>`;
                } else if (paramDef.type === "number") {
                    formHTML += `<input type="number" id="param-${paramName}" class="form-control"
                                       value="${existingVal !== null && existingVal !== undefined ? existingVal : ''}"
                                       placeholder="${paramDef.placeholder || ''}">`;
                } else {
                    formHTML += `<input type="text" id="param-${paramName}" class="form-control"
                                       value="${existingVal || ''}" placeholder="${paramDef.placeholder || ''}">`;
                }

                formHTML += `</div>`;
            }
            formHTML += '</div>';
        }

        // FX Conversion card (render only for FX-capable asset classes).
        // For crypto/equity/etc. single-currency accounts, this section would
        // just clutter the form — skip it unless the adapter deals in FX.
        if (currentAssetClass === "fx" || currentAssetClass === "forex") {
            formHTML += `
                <h2 class="section-title" style="margin-top:24px;">FX Conversion</h2>
                <p class="page-subtitle" style="margin-top:-8px;">
                    How to convert PnL from each non-base currency into your account's base
                    currency at backtest time. Use <b>Catalog</b> to look up the rate from an
                    ingested bar series (accurate across time); use <b>Fixed</b> for a single
                    static rate (fine for short backtests).
                </p>
                <div id="fx-conversion-card"></div>`;
        }

        // Action buttons
        formHTML += `
            <div class="page-divider"></div>
            <div style="display: flex; gap: 12px;">
                <button class="btn btn-primary" onclick="AdapterConfig.save()">
                    ${this.isEditMode ? 'Update Adapter' : 'Save Adapter'}
                </button>
                <button class="btn" onclick="App.navigate('dashboard')">Cancel</button>
            </div>`;

        container.innerHTML = `
            <h1 class="page-title">${this.isEditMode ? '&#9999;' : '&#10133;'} ${title}</h1>
            <p class="page-subtitle">${this.isEditMode ? 'Update the adapter configuration below.' : 'Fill in the connection parameters below.'}</p>

            ${!this.isEditMode ? `
            <div class="stepper">
                <div class="step completed">
                    <div class="step-number">1</div>
                    <span>Select Exchange</span>
                </div>
                <div class="step-connector"></div>
                <div class="step active">
                    <div class="step-number">2</div>
                    <span>Configure</span>
                </div>
                <div class="step-connector"></div>
                <div class="step">
                    <div class="step-number">3</div>
                    <span>Save</span>
                </div>
            </div>` : ''}

            ${formHTML}
        `;

        // Paint the FX Conversion card once the outer form is in the DOM.
        this._renderFxConversionCard();
    },

    async _loadCatalogPairs(venue) {
        // Fetch catalog pairs (optionally filtered by venue) so the FX
        // conversion dropdowns can bind each non-base currency to the right
        // ingested bar series. Silent-fail on error: the UI still lets the
        // admin type pair names manually.
        try {
            const url = venue
                ? `/api/catalog/pairs?venue=${encodeURIComponent(venue)}`
                : "/api/catalog/pairs";
            const data = await App.api(url);
            this._catalogPairs = data.pairs || [];
            // Prepopulate the base + any quote currencies as rows if editing
            // a blank adapter — makes the card useful without the admin
            // having to click "Add currency" for every pair in the catalog.
            this._seedFxRowsFromCatalog();
            this._renderFxConversionCard();
        } catch (e) {
            this._catalogPairs = [];
            this._renderFxConversionCard();
        }
    },

    _seedFxRowsFromCatalog() {
        // Collect every non-base currency that appears as base OR quote in
        // a catalog pair, so the card shows a row per currency the admin
        // will care about. Existing fx_conversion entries are preserved.
        const base = (this._accountBaseCurrency || "USD").toUpperCase();
        const seen = new Set(Object.keys(this._fxConversion).map(c => c.toUpperCase()));
        for (const p of this._catalogPairs) {
            for (const ccy of [p.base, p.quote]) {
                if (!ccy) continue;
                const c = ccy.toUpperCase();
                if (c === base) continue;
                if (seen.has(c)) continue;
                // Prefer catalog mode when a direct pair exists (e.g. JPY+USDJPY),
                // else default to static at 1.0 as a safe placeholder.
                const pair = this._findCatalogPair(c, base);
                this._fxConversion[c] = pair
                    ? { source: "catalog", catalog_pair: pair, fallback_rate: null }
                    : { source: "static", rate: 1.0 };
                seen.add(c);
            }
        }
    },

    _findCatalogPair(fromCcy, baseCcy) {
        // Return the instrument_id of a catalog pair that relates fromCcy to
        // baseCcy (in either orientation). e.g. ("JPY", "USD") → "USDJPY.FOREX_MS".
        const from = fromCcy.toUpperCase();
        const base = baseCcy.toUpperCase();
        for (const p of this._catalogPairs) {
            const s = (p.symbol || "").toUpperCase();
            if (s === from + base || s === base + from) return p.instrument_id;
        }
        return null;
    },

    _renderFxConversionCard() {
        const host = document.getElementById("fx-conversion-card");
        if (!host) return;  // not an FX asset class — nothing to render

        const base = (this._accountBaseCurrency || "USD").toUpperCase();
        const currencies = Object.keys(this._fxConversion).sort();

        // Header row: account base currency picker.
        let html = `
            <div class="form-row" style="margin-bottom:12px;">
                <div class="form-group" style="max-width:280px;">
                    <label class="form-label">Account base currency</label>
                    <input type="text" id="fx-base-ccy" class="form-control"
                           value="${base}" maxlength="6"
                           oninput="AdapterConfig._onBaseCurrencyChanged(this.value)">
                    <div style="font-size:0.75rem; color:var(--text-muted); margin-top:4px;">
                        All PnL is rolled up into this currency. Must match your starting-capital currency.
                    </div>
                </div>
            </div>`;

        // Pairs table.
        if (currencies.length === 0) {
            html += `
                <div class="alert alert-info" style="font-size:0.85rem;">
                    No non-base currencies detected. Use <b>Add currency</b> below, or ingest a
                    pair involving a non-base currency and reopen this page.
                </div>`;
        } else {
            html += `
                <table class="table" style="width:100%; margin-top:8px;">
                    <thead>
                        <tr>
                            <th style="width:80px;">Currency</th>
                            <th style="width:120px;">Source</th>
                            <th>Catalog pair</th>
                            <th style="width:120px;">Fixed rate (per 1 ${base})</th>
                            <th style="width:130px;">Fallback rate</th>
                            <th style="width:40px;"></th>
                        </tr>
                    </thead>
                    <tbody>`;
            for (const ccy of currencies) {
                html += this._renderFxRow(ccy, base);
            }
            html += `</tbody></table>`;
        }

        // Add-currency control.
        html += `
            <div style="margin-top:12px; display:flex; gap:8px; align-items:center;">
                <input type="text" id="fx-add-ccy" class="form-control"
                       style="max-width:120px;" placeholder="e.g. EUR" maxlength="6">
                <button class="btn btn-sm" onclick="AdapterConfig._addFxCurrency()">+ Add currency</button>
            </div>`;

        host.innerHTML = html;
    },

    _renderFxRow(ccy, base) {
        const rule = this._fxConversion[ccy] || {};
        const source = rule.source === "static" ? "static" : "catalog";
        const catalogPair = rule.catalog_pair || "";
        const rate = rule.rate != null ? rule.rate : "";
        const fallback = rule.fallback_rate != null ? rule.fallback_rate : "";

        // Build the catalog-pair <select> — only pairs that actually relate
        // this currency to the base are relevant, but we also include an
        // "other" path so advanced admins can bind to any ingested pair.
        const relevantPairs = this._catalogPairs.filter(p => {
            const s = (p.symbol || "").toUpperCase();
            return s.includes(ccy) || s.includes(base);
        });
        const pairOptions = ['<option value="">-- select pair --</option>']
            .concat(relevantPairs.map(p => {
                const sel = p.instrument_id === catalogPair ? "selected" : "";
                return `<option value="${p.instrument_id}" ${sel}>${p.instrument_id}</option>`;
            }));
        // If the configured pair isn't in the filtered list (e.g. user picked
        // an unusual triangulation), include it so we don't silently drop it.
        if (catalogPair && !relevantPairs.some(p => p.instrument_id === catalogPair)) {
            pairOptions.push(`<option value="${catalogPair}" selected>${catalogPair}</option>`);
        }

        const catalogDisabled = source !== "catalog" ? "disabled" : "";
        const staticDisabled = source !== "static" ? "disabled" : "";

        return `
            <tr data-ccy="${ccy}">
                <td><b>${ccy}</b></td>
                <td>
                    <select class="form-control" style="padding:4px 8px;"
                            onchange="AdapterConfig._onFxSourceChanged('${ccy}', this.value)">
                        <option value="catalog" ${source === 'catalog' ? 'selected' : ''}>Catalog</option>
                        <option value="static" ${source === 'static' ? 'selected' : ''}>Fixed</option>
                    </select>
                </td>
                <td>
                    <select class="form-control" style="padding:4px 8px;" ${catalogDisabled}
                            onchange="AdapterConfig._onFxCatalogPairChanged('${ccy}', this.value)">
                        ${pairOptions.join("")}
                    </select>
                </td>
                <td>
                    <input type="number" step="any" class="form-control" style="padding:4px 8px;"
                           value="${rate}" ${staticDisabled}
                           oninput="AdapterConfig._onFxRateChanged('${ccy}', this.value)">
                </td>
                <td>
                    <input type="number" step="any" class="form-control" style="padding:4px 8px;"
                           value="${fallback}" ${catalogDisabled} placeholder="optional"
                           oninput="AdapterConfig._onFxFallbackChanged('${ccy}', this.value)">
                </td>
                <td>
                    <button class="btn btn-sm" style="color:var(--danger);"
                            onclick="AdapterConfig._removeFxCurrency('${ccy}')"
                            title="Remove">&times;</button>
                </td>
            </tr>`;
    },

    _onBaseCurrencyChanged(value) {
        this._accountBaseCurrency = (value || "USD").toUpperCase();
        // Don't re-render here — the input would lose focus mid-type.
    },

    _onFxSourceChanged(ccy, value) {
        const rule = this._fxConversion[ccy] || {};
        if (value === "catalog") {
            this._fxConversion[ccy] = {
                source: "catalog",
                catalog_pair: rule.catalog_pair
                    || this._findCatalogPair(ccy, this._accountBaseCurrency)
                    || "",
                fallback_rate: rule.fallback_rate ?? null,
            };
        } else {
            this._fxConversion[ccy] = {
                source: "static",
                rate: rule.rate ?? 1.0,
            };
        }
        this._renderFxConversionCard();
    },

    _onFxCatalogPairChanged(ccy, value) {
        const rule = this._fxConversion[ccy] || { source: "catalog" };
        rule.catalog_pair = value || "";
        this._fxConversion[ccy] = rule;
    },

    _onFxRateChanged(ccy, value) {
        const rule = this._fxConversion[ccy] || { source: "static" };
        rule.rate = value === "" ? null : parseFloat(value);
        this._fxConversion[ccy] = rule;
    },

    _onFxFallbackChanged(ccy, value) {
        const rule = this._fxConversion[ccy] || { source: "catalog" };
        rule.fallback_rate = value === "" ? null : parseFloat(value);
        this._fxConversion[ccy] = rule;
    },

    _addFxCurrency() {
        const input = document.getElementById("fx-add-ccy");
        if (!input) return;
        const ccy = (input.value || "").trim().toUpperCase();
        if (!ccy) return;
        if (ccy === (this._accountBaseCurrency || "USD").toUpperCase()) {
            App.toast("Base currency doesn't need a conversion rule.", "warning");
            return;
        }
        if (this._fxConversion[ccy]) {
            App.toast(`${ccy} is already configured.`, "info");
            return;
        }
        const pair = this._findCatalogPair(ccy, this._accountBaseCurrency);
        this._fxConversion[ccy] = pair
            ? { source: "catalog", catalog_pair: pair, fallback_rate: null }
            : { source: "static", rate: 1.0 };
        input.value = "";
        this._renderFxConversionCard();
    },

    _removeFxCurrency(ccy) {
        delete this._fxConversion[ccy];
        this._renderFxConversionCard();
    },

    togglePassword(inputId) {
        const input = document.getElementById(inputId);
        const btn = input.nextElementSibling;
        if (input.type === "password") {
            input.type = "text";
            btn.textContent = "Hide";
        } else {
            input.type = "password";
            btn.textContent = "Show";
        }
    },

    async save() {
        const name = document.getElementById("adapter-name")?.value?.trim();
        const venue = document.getElementById("adapter-venue")?.value?.trim().toUpperCase();

        if (!name) { App.toast("Adapter name is required", "error"); return; }
        if (!venue) { App.toast("Venue name is required", "error"); return; }

        const regEntry = this.registry[this.selectedExchange];
        const isCustom = regEntry && regEntry.source === "custom";
        const exchangeType = this.selectedExchange;

        // Collect params
        const configValues = {};
        const params = regEntry?.params || {};
        for (const [paramName, paramDef] of Object.entries(params)) {
            const el = document.getElementById(`param-${paramName}`);
            if (!el) continue;
            if (paramDef.type === "checkbox") {
                configValues[paramName] = el.checked;
            } else if (paramDef.type === "number") {
                const val = el.value.trim();
                configValues[paramName] = val !== "" ? parseFloat(val) : (paramDef.default ?? null);
            } else {
                configValues[paramName] = el.value;
            }
        }

        const assetClass = document.getElementById("adapter-asset-class")?.value || "crypto";

        // Read the current base-currency input (it's a controlled input that
        // updates _accountBaseCurrency on the fly, but grab the live value in
        // case the user typed into it and hit Save without blurring).
        const baseEl = document.getElementById("fx-base-ccy");
        const accountBase = ((baseEl && baseEl.value) || this._accountBaseCurrency
            || "USD").toUpperCase();

        const payload = {
            name,
            venue,
            exchange_type: exchangeType,
            asset_class: assetClass,
            is_custom: isCustom || false,
            custom_adapter_file: isCustom ? (regEntry.custom_file || null) : null,
            testnet: configValues.testnet ?? configValues.is_testnet ?? false,
            data_config: { ...configValues },
            exec_config: { ...configValues },
            account_base_currency: accountBase,
            fx_conversion: this._fxConversion || {},
        };

        try {
            if (this.isEditMode) {
                await App.api(`/api/adapters/${this.editData.id}`, {
                    method: "PUT",
                    body: JSON.stringify(payload),
                });
                App.toast(`Adapter "${name}" updated!`, "success");
            } else {
                await App.api("/api/adapters", {
                    method: "POST",
                    body: JSON.stringify(payload),
                });
                App.toast(`Adapter "${name}" created!`, "success");
            }

            App.state.editingAdapter = null;
            App.navigate("dashboard");
        } catch (e) {
            App.toast(e.message, "error");
        }
    },
};
