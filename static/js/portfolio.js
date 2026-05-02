/**
 * Multileg Page - Manage multiple portfolios. Each portfolio row in the table
 * represents a complete portfolio (like the reference Multileg tab).
 * Click "Add Portfolio" to create, "Edit" to modify in a popup.
 */

const Portfolio = {
    strategies: {},
    barTypes: [],
    barTypeDetails: {},
    portfolios: [],       // array of all portfolio objects shown in the table
    activeIndex: null,     // index of portfolio selected for backtest
    results: null,
    templates: {},
    slotCounter: 0,
    _editingSlotIndex: null,

    async render(container) {
        container.innerHTML = `
            <div id="portfolio-loading" class="alert alert-info">Loading configuration...</div>
            <div id="portfolio-app" style="display: none;"></div>
        `;
        await this.loadConfig();
        App.setActionBar(`
            <button class="btn btn-sm" onclick="App.navigate('portfolio_tearsheet')">View Tearsheet</button>
        `);
    },

    async loadConfig() {
        try {
            const [barData, stratData, tmplData] = await Promise.all([
                App.api("/api/data/bar_types"),
                App.api("/api/strategies"),
                App.api("/api/portfolios/templates"),
            ]);
            this.barTypes = barData.bar_types || [];
            this.barTypeDetails = barData.bar_type_details || {};
            this.strategies = stratData.strategies || {};
            this.templates = tmplData.templates || {};

            if (this.barTypes.length === 0) {
                document.getElementById("portfolio-loading").innerHTML =
                    '<div class="alert alert-warning">No data in catalog. Go to <strong>Load Data</strong> first.</div>';
                return;
            }

            // Load all saved portfolios into the table on first visit
            if (this.portfolios.length === 0) {
                await this._loadAllSaved();
            }

            document.getElementById("portfolio-loading").style.display = "none";
            document.getElementById("portfolio-app").style.display = "";
            this.renderApp();
        } catch (e) {
            document.getElementById("portfolio-loading").innerHTML =
                `<div class="alert alert-danger">Failed to load: ${e.message}</div>`;
        }
    },

    async _loadAllSaved() {
        try {
            const data = await App.api("/api/portfolios/list");
            const names = data.portfolios || [];
            const loaded = [];
            for (const name of names) {
                try {
                    const d = await App.api(`/api/portfolios/load?name=${encodeURIComponent(name)}`);
                    if (d.portfolio) {
                        this._liftUniformSlotDates(d.portfolio);
                        loaded.push(d.portfolio);
                    }
                } catch {}
            }
            this.portfolios = loaded;
        } catch {
            this.portfolios = [];
        }
    },

    _newPortfolio(name) {
        return {
            name: name || "New Portfolio",
            description: "",
            starting_capital: 100000,
            max_loss: null,
            max_profit: null,
            start_date: null,
            end_date: null,
            squareoff_time: null,
            squareoff_tz: null,
            slots: [],
        };
    },

    _liftUniformSlotDates(portfolio) {
        if (!portfolio || !portfolio.slots || portfolio.slots.length === 0) return;
        if (portfolio.start_date || portfolio.end_date) return;
        const first = portfolio.slots[0];
        const s = first.start_date;
        const e = first.end_date;
        if (!s && !e) return;
        const allSame = portfolio.slots.every(sl => sl.start_date === s && sl.end_date === e);
        if (!allSame) return;
        portfolio.start_date = s || null;
        portfolio.end_date = e || null;
        for (const sl of portfolio.slots) { sl.start_date = null; sl.end_date = null; }
    },

    _SQUAREOFF_TZS: [
        "UTC", "America/New_York", "America/Chicago", "America/Los_Angeles",
        "Europe/London", "Europe/Berlin", "Asia/Tokyo", "Asia/Singapore", "Asia/Kolkata",
    ],

    _renderTzSelect(currentTz, id, inheritLabel) {
        const opts = [];
        if (inheritLabel) opts.push(`<option value="" ${!currentTz ? "selected" : ""}>${inheritLabel}</option>`);
        for (const tz of this._SQUAREOFF_TZS) {
            opts.push(`<option value="${tz}" ${currentTz === tz ? "selected" : ""}>${tz}</option>`);
        }
        return `<select class="form-control" id="${id}">${opts.join("")}</select>`;
    },

    /* ═══════════════════════════════════════════════════════════════════════
       MAIN TABLE — one row per portfolio (like reference Multileg tab)
       ═══════════════════════════════════════════════════════════════════════ */

    renderApp() {
        const colCount = 13;
        let rows = "";
        if (this.portfolios.length === 0) {
            rows = `<tr><td colspan="${colCount}" style="text-align:center; padding: 40px; color: var(--text-muted);">
                No portfolios yet. Click "+ Add Portfolio" to create one.
            </td></tr>`;
        } else {
            rows = this.portfolios.map((pf, i) => {
                const enabled = pf._enabled !== false;
                const statusColor = enabled ? "var(--success)" : "var(--text-muted)";
                const status = enabled ? "Enabled" : "Disabled";
                const instruments = (pf.slots || []).map(s => App.barTypeLabel(s.bar_type_str)).filter(Boolean);
                const instSummary = instruments.length > 0
                    ? [...new Set(instruments)].slice(0, 3).join(", ") + (instruments.length > 3 ? "..." : "")
                    : "—";
                const stratTags = [...new Set((pf.slots || []).map(s => s.strategy_name))];
                const stratSummary = stratTags.length > 0
                    ? stratTags.slice(0, 2).join(", ") + (stratTags.length > 2 ? "..." : "")
                    : "—";
                const sqOff = pf.squareoff_time || "—";
                const slotCount = (pf.slots || []).length;
                const isActive = this.activeIndex === i;
                const activeCls = isActive ? ' style="background: var(--accent-light);"' : '';

                return `<tr${activeCls}>
                    <td style="text-align:center;">
                        ${i + 1}
                        <input type="checkbox" ${enabled ? "checked" : ""}
                               onchange="Portfolio.togglePortfolio(${i})" style="margin-left:3px;">
                    </td>
                    <td style="color:${statusColor}; font-size:0.78rem;">${status}</td>
                    <td><strong style="cursor:pointer; color:var(--accent);" onclick="Portfolio.selectPortfolio(${i})">${pf.name}</strong></td>
                    <td style="font-size:0.8rem;">${instSummary}</td>
                    <td><button class="btn btn-xs" onclick="Portfolio.openEditPortfolio(${i})">Edit</button></td>
                    <td><button class="btn btn-xs" onclick="Portfolio.duplicatePortfolio(${i})">Copy</button></td>
                    <td><button class="btn btn-xs" style="color:var(--danger);" onclick="Portfolio.deletePortfolio(${i})">X</button></td>
                    <td>${stratSummary}</td>
                    <td style="text-align:center;">${slotCount}</td>
                    <td>$${App.formatNumber(pf.starting_capital || 100000)}</td>
                    <td>${sqOff}</td>
                    <td style="text-align:center;">
                        <button class="btn btn-xs btn-primary" onclick="Portfolio.runBacktestFor(${i})">Run</button>
                    </td>
                    <td style="font-size:0.75rem; color:var(--text-muted);">${pf.description || ""}</td>
                </tr>`;
            }).join("");
        }

        let resultsHTML = "";
        if (this.results) resultsHTML = this._renderResults();

        document.getElementById("portfolio-app").innerHTML = `
            <div style="border: 1px solid var(--border-color); border-radius: 4px; overflow: hidden;">
                <div style="overflow-x: auto;">
                    <table style="margin:0;">
                        <thead><tr>
                            <th style="width:50px; text-align:center;">Enabled</th>
                            <th>Status</th>
                            <th>Portfolio Name</th>
                            <th>Instruments</th>
                            <th>Edit</th>
                            <th>Copy</th>
                            <th>Delete</th>
                            <th>Strategy Tag</th>
                            <th style="text-align:center;">Slots</th>
                            <th>Capital</th>
                            <th>Sq-off Time</th>
                            <th>Backtest</th>
                            <th>Remarks</th>
                        </tr></thead>
                        <tbody>${rows}</tbody>
                    </table>
                </div>
                <div style="min-height: 100px; background: var(--bg-secondary); border-top: 1px solid var(--border-light);"></div>
            </div>

            <div style="border-top: 2px solid var(--border-color);"></div>

            <div style="display: flex; align-items: center; justify-content: center; gap: 16px; padding: 10px 0;">
                <button class="btn" onclick="Portfolio.openAddPortfolio()" style="padding: 6px 20px;">+ Add Portfolio</button>
                <button class="btn" onclick="Portfolio.openOptionsMenu(event)" style="padding: 6px 20px;">Options &#9660;</button>
            </div>

            <div style="display: flex; align-items: center; justify-content: center; gap: 10px; padding: 0 0 10px 0;">
                <span style="font-size: 0.84rem; color: var(--text-secondary);">From:</span>
                <input type="date" class="form-control" style="width:135px; font-size:0.82rem; padding:4px 8px;" id="pf-global-start" value="">
                <span style="font-size: 0.84rem; color: var(--text-secondary);">To:</span>
                <input type="date" class="form-control" style="width:135px; font-size:0.82rem; padding:4px 8px;" id="pf-global-end" value="">
                <button class="btn btn-primary btn-sm" onclick="Portfolio.runSelectedBacktest()" style="padding: 5px 16px;">
                    &#9654; Start Testing
                </button>
                <button class="btn btn-sm" onclick="Portfolio.openGlobalSettings()" style="padding: 5px 14px;">Settings</button>
            </div>

            <div id="pf-progress"></div>
            <div id="pf-results">${resultsHTML}</div>
        `;
    },

    /* ═══════════════════════════════════════════════════════════════════════
       PORTFOLIO-LEVEL OPERATIONS (table rows)
       ═══════════════════════════════════════════════════════════════════════ */

    selectPortfolio(index) {
        this.activeIndex = index;
        this.results = null;
        this.renderApp();
        App.log(`Selected portfolio: ${this.portfolios[index].name}`, "MESSAGE", "Multileg");
    },

    togglePortfolio(index) {
        this.portfolios[index]._enabled = !this.portfolios[index]._enabled;
        if (this.portfolios[index]._enabled === undefined) this.portfolios[index]._enabled = false;
        this.renderApp();
    },

    duplicatePortfolio(index) {
        const copy = JSON.parse(JSON.stringify(this.portfolios[index]));
        copy.name = copy.name + " (Copy)";
        this.portfolios.splice(index + 1, 0, copy);
        this.renderApp();
        App.log(`Portfolio "${this.portfolios[index].name}" copied`, "MESSAGE", "Multileg");
    },

    deletePortfolio(index) {
        const name = this.portfolios[index].name;
        if (!confirm(`Delete portfolio "${name}"?`)) return;
        this.portfolios.splice(index, 1);
        if (this.activeIndex === index) this.activeIndex = null;
        else if (this.activeIndex > index) this.activeIndex--;
        this.renderApp();
        // Also delete from server
        App.api("/api/portfolios/delete", {
            method: "POST", body: JSON.stringify({ name }),
        }).catch(() => {});
        App.log(`Portfolio "${name}" deleted`, "MESSAGE", "Multileg");
    },

    runBacktestFor(index) {
        this.activeIndex = index;
        this._currentPortfolio = this.portfolios[index];
        this.runBacktest();
    },

    runSelectedBacktest() {
        if (this.activeIndex === null || !this.portfolios[this.activeIndex]) {
            App.toast("Click a portfolio name to select it first, or click 'Run' on a row.", "error");
            return;
        }
        this._currentPortfolio = this.portfolios[this.activeIndex];
        // Apply global date overrides if set
        const gStart = document.getElementById("pf-global-start")?.value;
        const gEnd = document.getElementById("pf-global-end")?.value;
        if (gStart) this._currentPortfolio.start_date = gStart;
        if (gEnd) this._currentPortfolio.end_date = gEnd;
        this.runBacktest();
    },

    openGlobalSettings() {
        // Just a shortcut — if a portfolio is selected, open its settings
        if (this.activeIndex !== null && this.portfolios[this.activeIndex]) {
            this.openEditPortfolio(this.activeIndex);
        } else {
            App.toast("Select a portfolio first.", "info");
        }
    },

    openOptionsMenu(event) {
        const existing = document.getElementById("pf-options-menu");
        if (existing) { existing.remove(); return; }

        const menu = document.createElement("div");
        menu.id = "pf-options-menu";
        menu.style.cssText = "position:fixed; background:#fff; border:1px solid var(--border-color); border-radius:4px; box-shadow:0 4px 16px rgba(0,0,0,0.15); z-index:500; padding:4px 0; min-width:180px;";
        const rect = event.target.getBoundingClientRect();
        menu.style.top = (rect.bottom + 4) + "px";
        menu.style.left = rect.left + "px";

        const items = [
            { label: "Save All to Server", action: "Portfolio.saveAllPortfolios()" },
            { label: "Reload from Server", action: "Portfolio.reloadAll()" },
            { label: "Import Portfolio JSON", action: "document.getElementById('pf-import-input').click()" },
            { label: "Export Selected JSON", action: "Portfolio.exportSelectedJSON()" },
        ];
        menu.innerHTML = items.map(it =>
            `<div style="padding:7px 14px; font-size:0.84rem; cursor:pointer;"
                 onmouseover="this.style.background='var(--bg-hover)'" onmouseout="this.style.background=''"
                 onclick="document.getElementById('pf-options-menu').remove(); ${it.action}">${it.label}</div>`
        ).join("") + '<input type="file" id="pf-import-input" accept=".json" style="display:none;" onchange="Portfolio.importJSON(event)">';
        document.body.appendChild(menu);
        const close = (e) => { if (!menu.contains(e.target) && e.target !== event.target) { menu.remove(); document.removeEventListener("click", close); } };
        setTimeout(() => document.addEventListener("click", close), 0);
    },

    async saveAllPortfolios() {
        let saved = 0;
        for (const pf of this.portfolios) {
            try {
                const clean = JSON.parse(JSON.stringify(pf));
                delete clean._enabled;
                delete clean._ui;
                delete clean.on_leg_fail;
                delete clean.execution_mode;
                delete clean.strategy_tag;
                delete clean.max_legs;
                delete clean.tgt_sl_per_lot;
                await App.api("/api/portfolios/save", { method: "POST", body: JSON.stringify(clean) });
                saved++;
            } catch {}
        }
        App.toast(`Saved ${saved} portfolio(s) to server.`, "success");
        App.log(`Saved ${saved} portfolio(s)`, "SUCCESS", "Multileg");
    },

    async reloadAll() {
        this.portfolios = [];
        this.activeIndex = null;
        this.results = null;
        await this._loadAllSaved();
        this.renderApp();
        App.toast(`Loaded ${this.portfolios.length} portfolio(s) from server.`, "success");
    },

    exportSelectedJSON() {
        if (this.activeIndex === null) { App.toast("Select a portfolio first.", "info"); return; }
        const pf = this.portfolios[this.activeIndex];
        const json = JSON.stringify(pf, null, 2);
        const blob = new Blob([json], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a"); a.href = url; a.download = `${pf.name}.json`;
        document.body.appendChild(a); a.click(); document.body.removeChild(a); URL.revokeObjectURL(url);
    },

    importJSON(event) {
        const file = event.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                const pf = JSON.parse(e.target.result);
                this._liftUniformSlotDates(pf);
                this.portfolios.push(pf);
                this.renderApp();
                App.toast(`Imported "${pf.name}"`, "success");
                App.log(`Portfolio "${pf.name}" imported`, "MESSAGE", "Multileg");
            } catch (err) { App.toast("Invalid JSON: " + err.message, "error"); }
        };
        reader.readAsText(file);
    },

    /* ═══════════════════════════════════════════════════════════════════════
       ADD / EDIT PORTFOLIO MODAL (like reference "Create / Edit Portfolio")
       ═══════════════════════════════════════════════════════════════════════ */

    openAddPortfolio() {
        const pf = this._newPortfolio("Portfolio " + (this.portfolios.length + 1));
        this.portfolios.push(pf);
        const idx = this.portfolios.length - 1;
        this._openPortfolioModal(idx, true);
    },

    openEditPortfolio(index) {
        this._openPortfolioModal(index, false);
    },

    _openPortfolioModal(pfIndex, isNew) {
        this._editingPfIndex = pfIndex;
        this._editingIsNew = isNew;
        const pf = this.portfolios[pfIndex];
        const title = "Create / Edit Portfolio";
        const ui = pf._ui || {};

        // Strategy tags summary
        const stratTags = [...new Set((pf.slots || []).map(s => s.strategy_name))];
        const stratTagSummary = stratTags.length > 0 ? stratTags.join(", ") : "Default";

        // Build inline-editable legs table rows
        const stratOpts = Object.keys(this.strategies).map(n => `<option value="${n}">${n}</option>`).join("");
        const barOpts = this.barTypes.map(bt => `<option value="${bt}">${App.barTypeLabel(bt)}</option>`).join("");
        let legsRows = "";
        if ((pf.slots || []).length === 0) {
            legsRows = `<tr><td colspan="12" style="text-align:center; padding:20px; color:var(--text-muted);">No legs. Click "+ Add Leg" to add.</td></tr>`;
        } else {
            legsRows = pf.slots.map((slot, i) => {
                const ec = slot.exit_config || {};
                const slTypes = ["none", "percentage", "points", "trailing"];
                const tpTypes = ["none", "percentage", "points"];
                const slTypeOpts = slTypes.map(t => `<option value="${t}" ${(ec.stop_loss_type||"none")===t?"selected":""}>${t === "none" ? "None" : t.charAt(0).toUpperCase()+t.slice(1)}</option>`).join("");
                const tpTypeOpts = tpTypes.map(t => `<option value="${t}" ${(ec.target_type||"none")===t?"selected":""}>${t === "none" ? "None" : t.charAt(0).toUpperCase()+t.slice(1)}</option>`).join("");
                const sOpts = stratOpts.replace(`value="${slot.strategy_name}"`, `value="${slot.strategy_name}" selected`);
                const bOpts = barOpts.replace(`value="${slot.bar_type_str}"`, `value="${slot.bar_type_str}" selected`);

                return `<tr>
                    <td style="text-align:center;"><button class="leg-del-btn" onclick="Portfolio._deleteLeg(${i})" title="Delete">X</button></td>
                    <td style="text-align:center;"><button class="leg-copy-btn" onclick="Portfolio._copyLeg(${i})" title="Copy">&#128203;</button></td>
                    <td style="text-align:center; font-weight:600;">${i + 1}</td>
                    <td style="text-align:center;"><input type="checkbox" id="leg-il-enabled-${i}" ${slot.enabled !== false ? "checked" : ""}></td>
                    <td><select class="form-control" id="leg-il-strat-${i}" onchange="Portfolio._onInlineStratChange(${i})">${sOpts}</select></td>
                    <td><select class="form-control" id="leg-il-inst-${i}">${bOpts}</select></td>
                    <td><input type="number" class="form-control" id="leg-il-size-${i}" value="${slot.trade_size || 1}" min="1"></td>
                    <td><select class="form-control" id="leg-il-sltype-${i}" style="min-width:72px;">${slTypeOpts}</select></td>
                    <td><input type="number" class="form-control" id="leg-il-slval-${i}" value="${ec.stop_loss_value || 0}" step="0.5" min="0" style="width:52px;"></td>
                    <td><select class="form-control" id="leg-il-tptype-${i}" style="min-width:72px;">${tpTypeOpts}</select></td>
                    <td><input type="number" class="form-control" id="leg-il-tpval-${i}" value="${ec.target_value || 0}" step="0.5" min="0" style="width:52px;"></td>
                    <td style="text-align:center;"><button class="btn btn-xs" onclick="Portfolio._editLeg(${i})" style="font-size:0.72rem;">&#9881; Edit</button></td>
                </tr>`;
            }).join("");
        }

        const body = `
            <!-- Top settings bar -->
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; padding:8px 10px; background:#f8f9fa; border:1px solid var(--border-color); border-radius:4px;">
                <div style="flex:2; min-width:140px;">
                    <div style="font-size:0.68rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">PORTFOLIO NAME</div>
                    <input type="text" class="form-control" id="pf-m-name" value="${pf.name}" style="font-size:0.82rem; padding:4px 8px;">
                </div>
                <div style="flex:1; min-width:95px;">
                    <div style="font-size:0.68rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">CAPITAL</div>
                    <input type="number" class="form-control" id="pf-m-capital" value="${pf.starting_capital || 100000}" min="1000" style="font-size:0.82rem; padding:4px 8px;">
                </div>
                <div style="flex:0.8; min-width:90px;">
                    <div style="font-size:0.68rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">ALLOCATION</div>
                    <select class="form-control" id="pf-m-alloc" style="font-size:0.82rem; padding:4px 8px;">
                        <option value="equal" ${(pf.allocation_mode || 'equal') === 'equal' ? 'selected' : ''}>Equal</option>
                        <option value="percentage" ${pf.allocation_mode === 'percentage' ? 'selected' : ''}>Percentage</option>
                    </select>
                </div>
                <div class="pf-top-field pf-ui-only" style="flex:0.6; min-width:70px;">
                    <div style="font-size:0.68rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">MAX LEGS</div>
                    <input type="number" class="form-control" id="pf-m-maxlegs" value="${ui.max_legs || 24}" min="1" max="99" style="font-size:0.82rem; padding:4px 8px;">
                </div>
                <div class="pf-top-field pf-ui-only" style="flex:0.8; min-width:90px; display:flex; align-items:flex-end; gap:12px; padding-bottom:2px;">
                    <label style="font-size:0.75rem; color:var(--text-secondary); display:flex; align-items:center; gap:4px; cursor:pointer;">
                        <input type="checkbox" id="pf-m-tgtsl-perlot" ${ui.tgt_sl_per_lot ? "checked" : ""}> TGT/SL per Lot
                    </label>
                </div>
            </div>

            <!-- + Add Leg button -->
            <div style="margin-bottom:6px;">
                <button class="btn btn-sm btn-primary" onclick="Portfolio._addLeg()" style="padding:4px 16px;">+ Add Leg</button>
            </div>

            <!-- Inline legs table -->
            <div style="border:1px solid var(--border-color); border-radius:4px; overflow:auto; max-height:200px; margin-bottom:10px;">
                <table class="leg-inline-table">
                    <thead><tr>
                        <th style="width:32px;">Delete</th>
                        <th style="width:36px;">Copy</th>
                        <th style="width:28px;">ID</th>
                        <th style="width:36px;">Idle</th>
                        <th>Strategy</th>
                        <th>Instrument</th>
                        <th style="width:62px;">Size</th>
                        <th>Stop Loss</th>
                        <th style="width:56px;">SL Val</th>
                        <th>Target</th>
                        <th style="width:56px;">TGT Val</th>
                        <th style="width:52px;">Actions</th>
                    </tr></thead>
                    <tbody id="pf-m-legs-body">${legsRows}</tbody>
                </table>
            </div>

            <!-- Sub-tabs -->
            <div style="display:flex; gap:0; border-bottom:2px solid var(--border-color); margin-bottom:10px; flex-wrap:wrap;">
                <button class="slot-tab-btn active" data-tab="pf-exec" onclick="Portfolio._switchPfTab('pf-exec')">Execution Parameters</button>
                <button class="slot-tab-btn" data-tab="pf-rangebrk" onclick="Portfolio._switchPfTab('pf-rangebrk')">Range BreakOut</button>
                <button class="slot-tab-btn" data-tab="pf-other" onclick="Portfolio._switchPfTab('pf-other')">Other Settings</button>
                <button class="slot-tab-btn" data-tab="pf-dynhedge" onclick="Portfolio._switchPfTab('pf-dynhedge')">Dynamic Hedge</button>
                <button class="slot-tab-btn" data-tab="pf-target" onclick="Portfolio._switchPfTab('pf-target')">Target</button>
                <button class="slot-tab-btn" data-tab="pf-stoploss" onclick="Portfolio._switchPfTab('pf-stoploss')">Stoploss</button>
                <button class="slot-tab-btn" data-tab="pf-monitoring" onclick="Portfolio._switchPfTab('pf-monitoring')">Monitoring</button>
                <button class="slot-tab-btn" data-tab="pf-reexecute" onclick="Portfolio._switchPfTab('pf-reexecute')">ReExecute</button>
                <button class="slot-tab-btn" data-tab="pf-exit" onclick="Portfolio._switchPfTab('pf-exit')">Exit Settings</button>
                <button class="slot-tab-btn" data-tab="pf-timing" onclick="Portfolio._switchPfTab('pf-timing')">Timing</button>
            </div>

            <!-- Execution Parameters Tab -->
            <div class="pf-tab-content" id="pf-tab-pf-exec">
                <div style="display:flex; gap:14px; flex-wrap:wrap; margin-bottom:14px;">
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:260px;">
                        <legend>Execution Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Product</span>
                            <select class="form-control" id="pf-m-product" style="flex:1;">
                                <option value="MIS" ${(ui.product||'MIS')==='MIS'?'selected':''}>MIS</option>
                                <option value="NRML" ${ui.product==='NRML'?'selected':''}>NRML</option>
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Strategy Tag</span>
                            <select class="form-control" id="pf-m-strattag" style="flex:1;">
                                <option value="Default" ${(ui.strategy_tag||'Default')==='Default'?'selected':''}>Default</option>
                                ${stratTags.map(t => `<option value="${t}" ${ui.strategy_tag===t?'selected':''}>${t}</option>`).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">If One or More Leg Fail</span>
                            <select class="form-control" id="pf-m-legfail" style="flex:1;">
                                <option value="KeepPlacedLegs" ${(ui.on_leg_fail||'KeepPlacedLegs')==='KeepPlacedLegs'?'selected':''}>KeepPlacedLegs</option>
                                <option value="CancelPlacedLegs" ${ui.on_leg_fail==='CancelPlacedLegs'?'selected':''}>CancelPlacedLegs</option>
                            </select>
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:260px;">
                        <legend>Execution Mode</legend>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label wide">Portfolio Execution Mode</span>
                            <select class="form-control" id="pf-m-execmode" style="flex:1;" onchange="Portfolio._onExecModeChange()">
                                <option value="Start time" ${(ui.execution_mode||'Start time')==='Start time'?'selected':''}>Start time</option>
                                <option value="Manual" ${ui.execution_mode==='Manual'?'selected':''}>Manual</option>
                                <option value="CombinedPremium" ${ui.execution_mode==='CombinedPremium'?'selected':''}>CombinedPremium</option>
                                <option value="UnderlyingLevel" ${ui.execution_mode==='UnderlyingLevel'?'selected':''}>UnderlyingLevel</option>
                                <option value="CombinedPremiumCrossOver" ${ui.execution_mode==='CombinedPremiumCrossOver'?'selected':''}>CombinedPremiumCrossOver</option>
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only" id="pf-row-basedon" style="display:${['Start time','Manual'].includes(ui.execution_mode||'Start time')?'none':'flex'};">
                            <span class="pf-field-label wide">Based On</span>
                            <select class="form-control" id="pf-m-basedon" style="flex:1;" onchange="Portfolio._onBasedOnChange()">
                                <option value="None" ${(ui.based_on||'None')==='None'?'selected':''}>None</option>
                                <option value="DayOpen" ${ui.based_on==='DayOpen'?'selected':''}>DayOpen</option>
                                <option value="StartTime" ${ui.based_on==='StartTime'?'selected':''}>StartTime</option>
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only" id="pf-row-entryprice" style="display:${(ui.based_on||'None')==='None'&&!['Start time','Manual'].includes(ui.execution_mode||'Start time')?'flex':'none'};">
                            <span class="pf-field-label wide">Entry Price</span>
                            <input type="number" class="form-control" id="pf-m-entryprice" value="${ui.entry_price||0}" step="0.01" min="0" style="flex:1;">
                        </div>
                        <div class="pf-field-row pf-ui-only" id="pf-row-rounding" style="display:${['DayOpen','StartTime'].includes(ui.based_on||'None')?'flex':'none'};">
                            <span class="pf-field-label wide">Rounding Value</span>
                            <input type="number" class="form-control" id="pf-m-rounding" value="${ui.rounding_value||0}" step="0.5" min="0" style="flex:1;">
                        </div>
                        <div class="pf-field-row pf-ui-only" id="pf-row-adjustprice" style="display:${['DayOpen','StartTime'].includes(ui.based_on||'None')?'flex':'none'};">
                            <span class="pf-field-label wide">Adjust Price</span>
                            <input type="text" class="form-control" id="pf-m-adjustprice" value="${ui.adjust_price||''}" placeholder="e.g. +10, -5%" style="flex:1;">
                        </div>
                    </fieldset>
                </div>
                <fieldset class="pf-fieldset pf-ui-only">
                    <legend>Timing</legend>
                    <div style="display:flex; gap:14px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:200px;">
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Run On Days</span>
                                <select class="form-control" id="pf-m-runondays" style="flex:1;" onchange="Portfolio._onRunOnDaysChange()">
                                    <option value="All Days" ${(ui.run_on_days||'All Days')==='All Days'?'selected':''}>All Days</option>
                                    <option value="Mon - Fri" ${ui.run_on_days==='Mon - Fri'?'selected':''}>Mon - Fri</option>
                                    <option value="Mon - Thu" ${ui.run_on_days==='Mon - Thu'?'selected':''}>Mon - Thu</option>
                                    <option value="Custom" ${ui.run_on_days==='Custom'?'selected':''}>Custom</option>
                                </select>
                            </div>
                            <div id="pf-custom-days" style="display:${ui.run_on_days==='Custom'?'flex':'none'}; flex-wrap:wrap; gap:6px; margin-top:6px;">
                                ${["Mon","Tue","Wed","Thu","Fri","Sat","Sun"].map(d =>
                                    `<label style="font-size:0.78rem; display:flex; align-items:center; gap:3px; cursor:pointer;">
                                        <input type="checkbox" class="pf-custom-day" value="${d}" ${(ui.selected_days||["Mon","Tue","Wed","Thu","Fri"]).includes(d)?'checked':''}> ${d}
                                    </label>`
                                ).join("")}
                            </div>
                        </div>
                        <div style="flex:1; min-width:200px;">
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Start Time</span>
                                <input type="time" class="form-control" id="pf-m-starttime" value="${ui.start_time||'09:30:00'}" step="1" style="flex:1;">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">End Time</span>
                                <input type="time" class="form-control" id="pf-m-endtime" value="${ui.end_time||'16:15:00'}" step="1" style="flex:1;">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">SqOff Time</span>
                                <input type="time" class="form-control" id="pf-m-sqofftime-exec" value="${ui.sqoff_time_exec||'16:15:00'}" step="1" style="flex:1;">
                            </div>
                        </div>
                        <div style="flex:1; min-width:200px;">
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Start Day</span>
                                <select class="form-control" id="pf-m-startday" style="flex:1;">
                                    <option value="Before Expiry" ${(ui.start_day||'Before Expiry')==='Before Expiry'?'selected':''}>Before Expiry</option>
                                    <option value="On Expiry" ${ui.start_day==='On Expiry'?'selected':''}>On Expiry</option>
                                    <option value="Fixed" ${ui.start_day==='Fixed'?'selected':''}>Fixed</option>
                                </select>
                                <input type="number" class="form-control" id="pf-m-startdayoff" value="${ui.start_day_offset||1}" min="0" step="1" style="width:50px;">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">SqOff Day</span>
                                <input type="number" class="form-control" id="pf-m-sqoffday" value="${ui.sqoff_day||0}" min="0" step="1" style="width:60px;">
                                <span style="font-size:0.72rem; color:var(--text-muted); margin-left:4px;">(~before expiry)</span>
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <label style="font-size:0.75rem; color:var(--text-secondary); display:flex; align-items:center; gap:4px; cursor:pointer;">
                                    <input type="checkbox" id="pf-m-holiday" ${ui.holiday_handling?'checked':''}>
                                    If Holiday, Use Previous Day for Start &nbsp;Next Day for SqOff, Else no Entry
                                </label>
                            </div>
                        </div>
                    </div>
                    <p style="font-size:0.7rem; color:var(--text-muted); margin:6px 0 0; font-style:italic;">~ In Calendar days with holidays. Example 1 for Wednesday &amp; 6 for Friday etc</p>
                </fieldset>
            </div>

            <!-- Range BreakOut Tab (UI-only) -->
            <div class="pf-tab-content" id="pf-tab-pf-rangebrk" style="display:none;">
                <div style="margin-bottom:10px;">
                    <label class="pf-ui-only" style="font-size:0.84rem; display:flex; align-items:center; gap:6px; cursor:pointer;">
                        <input type="checkbox" id="pf-m-rbo-enabled" ${ui.rbo_enabled?'checked':''}>
                        Enable RangeBreakOut
                    </label>
                </div>
                <fieldset class="pf-fieldset pf-ui-only">
                    <legend>Range BreakOut Settings</legend>
                    <div style="display:flex; gap:14px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:220px;">
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Range Monitoring Start Time</span>
                                <input type="time" class="form-control" id="pf-m-rbo-monstart" value="${ui.range_monitoring_start||'09:30:00'}" step="1" style="width:110px;">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Range Monitoring End Time</span>
                                <input type="time" class="form-control" id="pf-m-rbo-monend" value="${ui.range_monitoring_end||'10:30:00'}" step="1" style="width:110px;"
                                    onchange="document.getElementById('pf-m-rbo-entrystart').value=this.value">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Entry Start Time</span>
                                <input type="time" class="form-control" id="pf-m-rbo-entrystart" value="${ui.entry_start||ui.range_monitoring_end||'10:30:00'}" step="1" style="width:110px;">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Entry End Time</span>
                                <input type="time" class="form-control" id="pf-m-rbo-entryend" value="${ui.entry_end||'16:15:00'}" step="1" style="width:110px;">
                            </div>
                        </div>
                        <div style="flex:1; min-width:220px;">
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Entry At</span>
                                <select class="form-control" id="pf-m-rbo-entryat" style="flex:1;">
                                    ${["Any","RangeHigh","RangeLow","C_OnHigh_P_OnLow","P_OnHigh_C_OnLow"].map(o =>
                                        `<option value="${o}" ${(ui.entry_at||'Any')===o?'selected':''}>${o}</option>`
                                    ).join("")}
                                </select>
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Range Buffer (mins)</span>
                                <input type="number" class="form-control" id="pf-m-rbo-buffer" value="${ui.range_buffer||0}" min="0" step="1" placeholder="e.g. 5" style="width:90px;">
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Monitoring</span>
                                <select class="form-control" id="pf-m-rbo-monitoring" style="flex:1;">
                                    <option value="Underlying" selected>Underlying</option>
                                </select>
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <label style="font-size:0.8rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                    <input type="checkbox" id="pf-m-rbo-cancelother" ${ui.cancel_other?'checked':''}>
                                    Cancel Other Side if One Executes
                                </label>
                            </div>
                        </div>
                    </div>
                </fieldset>
            </div>

            <!-- Dynamic Hedge Tab (UI-only) -->
            <div class="pf-tab-content" id="pf-tab-pf-dynhedge" style="display:none;">
                <fieldset class="pf-fieldset pf-ui-only">
                    <legend>Dynamic Hedge Settings</legend>
                    <div style="display:flex; gap:14px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:200px;">
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">Dynamic Hedge Type</span>
                                <select class="form-control" id="pf-m-dh-type" style="flex:1;" onchange="Portfolio._onDhTypeChange()">
                                    <option value="PremiumBased" ${(ui.hedge_type||'PremiumBased')==='PremiumBased'?'selected':''}>PremiumBased</option>
                                    <option value="DistanceBased" ${ui.hedge_type==='DistanceBased'?'selected':''}>DistanceBased</option>
                                </select>
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <span class="pf-field-label">SqOff Leg On</span>
                                <select class="form-control" id="pf-m-dh-sqoff" style="flex:1;">
                                    <option value="LegSqOff" ${(ui.sqoff_leg_on||'LegSqOff')==='LegSqOff'?'selected':''}>LegSqOff</option>
                                    <option value="PortfolioSqOff" ${ui.sqoff_leg_on==='PortfolioSqOff'?'selected':''}>PortfolioSqOff</option>
                                </select>
                            </div>
                        </div>
                        <div style="flex:1; min-width:220px;">
                            <div class="pf-field-row pf-ui-only pf-dh-premium" style="display:${(ui.hedge_type||'PremiumBased')==='PremiumBased'?'flex':'none'};">
                                <span class="pf-field-label">Hedge Distance from Strike (Min)</span>
                                <input type="number" class="form-control" id="pf-m-dh-distmin" value="${ui.hedge_distance_strike_min||10}" min="0" max="100" step="1" style="width:70px;">
                            </div>
                            <div class="pf-field-row pf-ui-only pf-dh-premium" style="display:${(ui.hedge_type||'PremiumBased')==='PremiumBased'?'flex':'none'};">
                                <span class="pf-field-label">Hedge Distance from Strike (Max)</span>
                                <input type="number" class="form-control" id="pf-m-dh-distmax" value="${ui.hedge_distance_strike_max||20}" min="0" max="100" step="1" style="width:70px;">
                            </div>
                            <div class="pf-field-row pf-ui-only pf-dh-premium" style="display:${(ui.hedge_type||'PremiumBased')==='PremiumBased'?'flex':'none'};">
                                <span class="pf-field-label">Min Premium</span>
                                <input type="number" class="form-control" id="pf-m-dh-premmin" value="${ui.premium_min||0.05}" min="0" step="0.01" style="width:90px;">
                            </div>
                            <div class="pf-field-row pf-ui-only pf-dh-premium" style="display:${(ui.hedge_type||'PremiumBased')==='PremiumBased'?'flex':'none'};">
                                <span class="pf-field-label">Max Premium</span>
                                <input type="number" class="form-control" id="pf-m-dh-premmax" value="${ui.premium_max||40.00}" min="0" step="0.01" style="width:90px;">
                            </div>
                            <div class="pf-field-row pf-ui-only pf-dh-distance" style="display:${ui.hedge_type==='DistanceBased'?'flex':'none'};">
                                <span class="pf-field-label">Hedge Fixed Min Distance</span>
                                <input type="number" class="form-control" id="pf-m-dh-fixeddist" value="${ui.hedge_fixed_min_distance||50}" min="0" max="10000" step="1" style="width:90px;">
                            </div>
                        </div>
                    </div>
                </fieldset>
            </div>

            <!-- Monitoring Tab (UI-only) -->
            <div class="pf-tab-content" id="pf-tab-pf-monitoring" style="display:none;">
                <div style="display:flex; gap:14px; flex-wrap:wrap;">
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:180px;">
                        <legend>Leg Target Monitoring</legend>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Target</span>
                            <select class="form-control" id="pf-m-mon-legtgt" style="flex:1;">
                                ${["Realtime","MinuteClose","Interval"].map(o => `<option value="${o}" ${(ui.leg_target_monitoring||'Realtime')===o?'selected':''}>${o}</option>`).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Trailing</span>
                            <select class="form-control" id="pf-m-mon-legtrail" style="flex:1;">
                                ${["Realtime","MinuteClose","Interval"].map(o => `<option value="${o}" ${(ui.leg_trailing_monitoring||'Realtime')===o?'selected':''}>${o}</option>`).join("")}
                            </select>
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:180px;">
                        <legend>Leg Stoploss Monitoring</legend>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">SL</span>
                            <select class="form-control" id="pf-m-mon-legsl" style="flex:1;">
                                ${["Realtime","MinuteClose","Interval"].map(o => `<option value="${o}" ${(ui.leg_sl_monitoring||'Realtime')===o?'selected':''}>${o}</option>`).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Trailing</span>
                            <select class="form-control" id="pf-m-mon-legsltrail" style="flex:1;">
                                ${["Realtime","MinuteClose","Interval"].map(o => `<option value="${o}" ${(ui.leg_sl_trailing_monitoring||'Realtime')===o?'selected':''}>${o}</option>`).join("")}
                            </select>
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:200px;">
                        <legend>Combined Target SL Monitoring</legend>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Target Monitoring</span>
                            <select class="form-control" id="pf-m-mon-combtgt" style="flex:1;">
                                ${["Realtime","MinuteClose","Interval"].map(o => `<option value="${o}" ${(ui.combined_target_monitoring||'Realtime')===o?'selected':''}>${o}</option>`).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">SL Monitoring</span>
                            <select class="form-control" id="pf-m-mon-combsl" style="flex:1;">
                                ${["Realtime","MinuteClose","Interval"].map(o => `<option value="${o}" ${(ui.combined_sl_monitoring||'Realtime')===o?'selected':''}>${o}</option>`).join("")}
                            </select>
                        </div>
                    </fieldset>
                </div>
            </div>

            <!-- ReExecute Tab (UI-only) -->
            <div class="pf-tab-content" id="pf-tab-pf-reexecute" style="display:none;">
                <div style="display:flex; gap:14px; flex-wrap:wrap;">
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:280px;">
                        <legend>ReExecute Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-reex-noslcost" ${ui.no_reexec_sl_cost?'checked':''}> No ReExecute If Moved SL to Cost
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-reex-nowait" ${ui.no_wait_trade_reexec?'checked':''}> No Wait &amp; Trade for ReExecute
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-reex-nostrike" ${ui.no_strike_change_reexec?'checked':''}> No Strike Change for ReExecute
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-reex-noend" ${ui.no_reentry_after_end?'checked':''}> No ReEntry/ReExecute after Portfolio End Time
                            </label>
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:280px;">
                        <legend>ReEntry Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-reex-noreentry" ${(ui.no_reentry_sl_cost!==false)?'checked':''}> No ReEntry If Moved SL to Cost
                            </label>
                        </div>
                    </fieldset>
                </div>
            </div>

            <!-- Other Settings Tab -->
            <div class="pf-tab-content" id="pf-tab-pf-other" style="display:none;">
                <fieldset class="pf-fieldset">
                    <legend>Other Settings</legend>
                    <div class="pf-field-row pf-ui-only">
                        <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                            <input type="checkbox" id="pf-m-other-trailwait" ${ui.trail_wait_trade?'checked':''}> Trail Wait Trade
                        </label>
                    </div>
                    <div class="pf-field-row pf-ui-only">
                        <span class="pf-field-label">Straddle Width Multiplier</span>
                        <input type="number" class="form-control" id="pf-m-other-swm" value="${ui.straddle_width_multiplier||0}" step="0.01" style="flex:1;">
                    </div>
                    <div class="pf-field-row pf-ui-only">
                        <span class="pf-field-label">Delay Between Legs in Sec.</span>
                        <input type="number" class="form-control" id="pf-m-other-legdelay" value="${ui.delay_between_legs||0}" min="0" step="1" style="flex:1;">
                    </div>
                    <div class="pf-field-row pf-ui-only">
                        <span class="pf-field-label">On Target Action On</span>
                        <select class="form-control" id="pf-m-other-tgtaction" style="flex:1;">
                            ${["OnTarget_N_Trailing_Both","OnTarget_Only","OnTarget_Trailing_Only"].map(o =>
                                `<option value="${o}" ${(ui.on_target_action_on||'OnTarget_N_Trailing_Both')===o?'selected':''}>${o}</option>`
                            ).join("")}
                        </select>
                    </div>
                    <div class="pf-field-row pf-ui-only">
                        <span class="pf-field-label">On SL Action On</span>
                        <select class="form-control" id="pf-m-other-slaction" style="flex:1;">
                            ${["OnSL_N_Trailing_Both","OnSL_Only","OnSL_Trailing_Only"].map(o =>
                                `<option value="${o}" ${(ui.on_sl_action_on||'OnSL_N_Trailing_Both')===o?'selected':''}>${o}</option>`
                            ).join("")}
                        </select>
                    </div>
                    <hr style="border:none; border-top:1px solid var(--border-light); margin:12px 0;">
                    <div class="pf-field-row">
                        <span class="pf-field-label">Description / Remarks</span>
                        <input type="text" class="form-control" id="pf-m-desc" value="${pf.description || ''}" placeholder="Optional remarks" style="flex:1;">
                    </div>
                    <div class="pf-field-row">
                        <span class="pf-field-label">Max Loss ($)</span>
                        <input type="number" class="form-control" id="pf-m-maxloss" value="${pf.max_loss || ''}" min="0" style="width:140px;">
                    </div>
                    <div class="pf-field-row">
                        <span class="pf-field-label">Max Profit ($)</span>
                        <input type="number" class="form-control" id="pf-m-maxprofit" value="${pf.max_profit || ''}" min="0" style="width:140px;">
                    </div>
                </fieldset>
            </div>

            <!-- Target Tab -->
            <div class="pf-tab-content" id="pf-tab-pf-target" style="display:none;">
                <div style="display:flex; gap:14px; flex-wrap:wrap;">
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:260px;">
                        <legend>Target Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-tgt-enabled" ${ui.target_enabled?'checked':''}> Enable Target
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Target Type</span>
                            <select class="form-control" id="pf-m-tgt-type" style="flex:1;">
                                ${["Combined Profit","Combined Premium","Absolute Combined Premium","Underlying Movement"].map(o =>
                                    `<option value="${o}" ${(ui.target_type||'Combined Profit')===o?'selected':''}>${o}</option>`
                                ).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Target Value</span>
                            <input type="number" class="form-control" id="pf-m-tgt-value" value="${ui.target_value||0}" step="0.01" min="0" style="width:110px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">On Target</span>
                            <select class="form-control" id="pf-m-tgt-action" style="flex:1;">
                                ${["SqOff","SqOff Other Portfolio","Execute Other Portfolio","Start Other Portfolio","ReExecute","ReExecute at Entry Price","ReExecute SameStrike at EntryPrice"].map(o =>
                                    `<option value="${o}" ${(ui.on_target||'SqOff')===o?'selected':''}>${o}</option>`
                                ).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Delay (sec)</span>
                            <input type="number" class="form-control" id="pf-m-tgt-delay" value="${ui.target_delay||0}" min="0" step="1" style="width:70px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">ReExecute Count (0 = Unlimited)</span>
                            <input type="number" class="form-control" id="pf-m-tgt-reexcount" value="${ui.target_reexecute_count||0}" min="0" step="1" style="width:70px;">
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:260px;">
                        <legend>Trailing Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-tgt-trail-enabled" ${ui.trail_target_enabled?'checked':''}> Enable Trailing
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Lock Minimum Profit</span>
                            <input type="number" class="form-control" id="pf-m-tgt-trail-lock" value="${ui.trail_lock_min_profit||0}" step="0.01" min="0" style="width:90px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">When Profit reach</span>
                            <input type="number" class="form-control" id="pf-m-tgt-trail-reach" value="${ui.trail_when_profit_reach||0}" step="0.01" min="0" style="width:90px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">For Every Increase By</span>
                            <input type="number" class="form-control" id="pf-m-tgt-trail-every" value="${ui.trail_every||0}" step="0.01" min="0" style="width:90px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Trail Profit By</span>
                            <input type="number" class="form-control" id="pf-m-tgt-trail-by" value="${ui.trail_by||0}" step="0.01" min="0" style="width:90px;">
                        </div>
                    </fieldset>
                </div>
            </div>

            <!-- Stoploss Tab -->
            <div class="pf-tab-content" id="pf-tab-pf-stoploss" style="display:none;">
                <div style="display:flex; gap:14px; flex-wrap:wrap;">
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:240px;">
                        <legend>Stoploss Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-enabled" ${ui.sl_enabled?'checked':''}> Enable Stoploss
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Type</span>
                            <select class="form-control" id="pf-m-sl-type" style="flex:1;">
                                ${["Combined Loss","Combined Premium","Absolute Combined Premium","Underlying Movement","Loss and Underlying Range"].map(o =>
                                    `<option value="${o}" ${(ui.sl_type||'Combined Loss')===o?'selected':''}>${o}</option>`
                                ).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Value</span>
                            <input type="number" class="form-control" id="pf-m-sl-value" value="${ui.sl_value||0}" step="0.01" min="0" style="width:110px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">On SL Action</span>
                            <select class="form-control" id="pf-m-sl-action" style="flex:1;">
                                ${["SqOff","SqOff Other Portfolio","Execute Other Portfolio","Start Other Portfolio","ReExecute","ReExecute at Entry Price","ReExecute SameStrike at EntryPrice"].map(o =>
                                    `<option value="${o}" ${(ui.on_sl_action||'SqOff')===o?'selected':''}>${o}</option>`
                                ).join("")}
                            </select>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Delay (sec)</span>
                            <input type="number" class="form-control" id="pf-m-sl-delay" value="${ui.sl_delay||0}" min="0" step="1" style="width:70px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">ReExecute Count</span>
                            <input type="number" class="form-control" id="pf-m-sl-reexcount" value="${ui.sl_reexecute_count||0}" min="0" step="1" style="width:70px;">
                        </div>
                        <div style="margin-top:10px; padding-top:8px; border-top:1px solid var(--border-light);">
                            <div style="font-size:0.78rem; font-weight:600; color:#c44; margin-bottom:6px;">On SL Hit — Selective SqOff</div>
                            <div class="pf-field-row pf-ui-only">
                                <label style="font-size:0.78rem; display:flex; align-items:center; gap:4px; cursor:pointer;">
                                    <input type="checkbox" id="pf-m-sl-sqoff-loss" ${ui.sqoff_loss_legs?'checked':''}> SqOff Only Loss Making Legs
                                </label>
                            </div>
                            <div class="pf-field-row pf-ui-only">
                                <label style="font-size:0.78rem; display:flex; align-items:center; gap:4px; cursor:pointer;">
                                    <input type="checkbox" id="pf-m-sl-sqoff-profit" ${ui.sqoff_profit_legs?'checked':''}> SqOff Only Profit Making Legs
                                </label>
                            </div>
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:200px;">
                        <legend>Trailing SL Settings</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-trail-enabled" ${ui.trail_sl_enabled?'checked':''}> Enable Trailing SL
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">For Every Profit of</span>
                            <input type="number" class="form-control" id="pf-m-sl-trail-every" value="${ui.trail_sl_every||0}" step="0.01" min="0" style="width:90px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Tighten SL By</span>
                            <input type="number" class="form-control" id="pf-m-sl-trail-by" value="${ui.trail_sl_by||0}" step="0.01" min="0" style="width:90px;">
                        </div>
                    </fieldset>
                    <fieldset class="pf-fieldset pf-ui-only" style="flex:1; min-width:240px;">
                        <legend>Move SL to Cost</legend>
                        <div class="pf-field-row pf-ui-only">
                            <label style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-move-enabled" ${ui.move_sl_enabled?'checked':''}> Enable Move SL to Cost
                            </label>
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Safety Seconds</span>
                            <input type="number" class="form-control" id="pf-m-sl-move-safety" value="${ui.move_sl_safety_seconds||0}" min="0" step="1" style="width:70px;">
                        </div>
                        <div class="pf-field-row pf-ui-only">
                            <span class="pf-field-label">Move SL Action</span>
                            <select class="form-control" id="pf-m-sl-move-action" style="flex:1;">
                                ${["Move Only for Profitable Legs","Move SL for All Legs Despite Loss / Profit","Move SL to LTP + Buffer for Loss Making Legs"].map(o =>
                                    `<option value="${o}" ${(ui.move_sl_action||'Move Only for Profitable Legs')===o?'selected':''}>${o}</option>`
                                ).join("")}
                            </select>
                        </div>
                        <div style="margin-top:8px;">
                            <label class="pf-field-row pf-ui-only" style="font-size:0.78rem; display:flex; align-items:center; gap:4px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-move-trail" ${ui.trail_after_move_sl?'checked':''}> Trail SL only after Move SL to Cost
                            </label>
                            <label class="pf-field-row pf-ui-only" style="font-size:0.78rem; display:flex; align-items:center; gap:4px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-move-nobuy" ${ui.no_move_buy_legs?'checked':''}> No Move SL for BUY Legs
                            </label>
                            <label class="pf-field-row pf-ui-only" style="font-size:0.78rem; display:flex; align-items:center; gap:4px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-move-hitsl" ${ui.hit_on_leg_sl?'checked':''}> Re-apply on every Leg SL hit
                            </label>
                            <label class="pf-field-row pf-ui-only" style="font-size:0.78rem; display:flex; align-items:center; gap:4px; cursor:pointer;">
                                <input type="checkbox" id="pf-m-sl-move-hittgt" ${ui.hit_on_leg_target?'checked':''}> Re-apply on every Leg Target hit *
                            </label>
                        </div>
                    </fieldset>
                </div>
            </div>

            <!-- Timing Tab -->
            <div class="pf-tab-content" id="pf-tab-pf-timing" style="display:none;">
                <fieldset class="pf-fieldset">
                    <legend>Timing</legend>
                    <div style="display:flex; gap:14px; flex-wrap:wrap;">
                        <div style="flex:1; min-width:200px;">
                            <div class="pf-field-row">
                                <span class="pf-field-label">Start Date</span>
                                <input type="date" class="form-control" id="pf-m-start" value="${pf.start_date || ''}" style="flex:1;">
                            </div>
                            <div class="pf-field-row">
                                <span class="pf-field-label">End Date</span>
                                <input type="date" class="form-control" id="pf-m-end" value="${pf.end_date || ''}" style="flex:1;">
                            </div>
                            <div class="pf-field-row">
                                <span class="pf-field-label">SqOff Time</span>
                                <input type="time" class="form-control" id="pf-m-sqoff" value="${pf.squareoff_time || ''}" style="flex:1;">
                            </div>
                        </div>
                        <div style="flex:1; min-width:200px;">
                            <div class="pf-field-row">
                                <span class="pf-field-label">Squareoff TZ</span>
                                ${this._renderTzSelect(pf.squareoff_tz, "pf-m-sqofftz", "(UTC)")}
                            </div>
                        </div>
                    </div>
                </fieldset>
            </div>

            <!-- Exit Settings Tab -->
            <div class="pf-tab-content" id="pf-tab-pf-exit" style="display:none;">
                <div style="display:flex; gap:14px; flex-wrap:wrap; align-items:flex-start;">
                    <div style="flex:1; min-width:260px;">
                        <div style="font-size:0.8rem; font-weight:600; color:var(--text-secondary); margin-bottom:6px;">Exit Order Type</div>
                        <select class="form-control pf-ui-only" id="pf-m-exit-ordertype" style="width:100%; margin-bottom:12px;">
                            ${["MARKET","Limit","SL_Limit"].map(o =>
                                `<option value="${o}" ${(ui.exit_order_type||'MARKET')===o?'selected':''}>${o}</option>`
                            ).join("")}
                        </select>
                        <label class="pf-ui-only" style="font-size:0.82rem; display:flex; align-items:center; gap:5px; cursor:pointer;">
                            <input type="checkbox" id="pf-m-exit-sellfirst" ${(ui.exit_sell_first!==false)?'checked':''}> Exit Sell Legs First
                        </label>
                    </div>
                    <div style="flex:1; min-width:300px;">
                        <div style="font-size:0.8rem; font-weight:600; color:var(--text-secondary); margin-bottom:6px;">On Portfolio Complete (Not Applicable on Manual SqOff)</div>
                        <select class="form-control pf-ui-only" id="pf-m-exit-oncomplete" style="width:100%;">
                            ${["None","SqOff Other Portfolio","Execute Other Portfolio","Start Other Portfolio"].map(o =>
                                `<option value="${o}" ${(ui.on_portfolio_complete||'None')===o?'selected':''}>${o}</option>`
                            ).join("")}
                        </select>
                    </div>
                </div>
            </div>

            <p style="font-size:0.72rem; color:var(--text-muted); margin:10px 0 0; font-style:italic;">
                * Fields marked in red are not yet enabled for backtest
            </p>
        `;

        // Footer goes outside modal-body, in the modal-dialog directly
        const footer = `
            <div class="pf-modal-footer">
                <span class="footer-label">Portfolio Name</span>
                <input type="text" class="form-control" id="pf-m-name-footer" value="${pf.name}" style="width:190px;">
                <span class="footer-label">Remarks</span>
                <input type="text" class="form-control" id="pf-m-remarks-footer" value="${pf.description || ''}" style="flex:1;">
                <button class="btn btn-sm" onclick="Portfolio._cancelPortfolioModal(${isNew ? 1 : 0})">Cancel</button>
                <button class="btn btn-sm btn-primary" style="min-width:140px; font-weight:600;" onclick="Portfolio._savePortfolioModal()">SAVE PORTFOLIO</button>
            </div>
        `;
        this._openModal(title, body, 1200, footer);
    },

    _switchPfTab(tabName) {
        document.querySelectorAll(".pf-tab-content").forEach(el => el.style.display = "none");
        // Only toggle pf-level tab buttons, not leg tab buttons
        document.querySelectorAll(`.slot-tab-btn[data-tab^="pf-"]`).forEach(el => el.classList.remove("active"));
        const content = document.getElementById(`pf-tab-${tabName}`);
        if (content) content.style.display = "";
        const btn = document.querySelector(`.slot-tab-btn[data-tab="${tabName}"]`);
        if (btn) btn.classList.add("active");
    },

    _onExecModeChange() {
        const mode = document.getElementById("pf-m-execmode")?.value || "Start time";
        const isDynamic = !["Start time", "Manual"].includes(mode);
        document.getElementById("pf-row-basedon").style.display = isDynamic ? "flex" : "none";
        if (!isDynamic) {
            document.getElementById("pf-row-entryprice").style.display = "none";
            document.getElementById("pf-row-rounding").style.display = "none";
            document.getElementById("pf-row-adjustprice").style.display = "none";
        } else {
            this._onBasedOnChange();
        }
    },

    _onBasedOnChange() {
        const basedOn = document.getElementById("pf-m-basedon")?.value || "None";
        document.getElementById("pf-row-entryprice").style.display = basedOn === "None" ? "flex" : "none";
        document.getElementById("pf-row-rounding").style.display = ["DayOpen", "StartTime"].includes(basedOn) ? "flex" : "none";
        document.getElementById("pf-row-adjustprice").style.display = ["DayOpen", "StartTime"].includes(basedOn) ? "flex" : "none";
    },

    _onDhTypeChange() {
        const isPremium = (document.getElementById("pf-m-dh-type")?.value || "PremiumBased") === "PremiumBased";
        document.querySelectorAll(".pf-dh-premium").forEach(el => el.style.display = isPremium ? "flex" : "none");
        document.querySelectorAll(".pf-dh-distance").forEach(el => el.style.display = isPremium ? "none" : "flex");
    },

    _onRunOnDaysChange() {
        const val = document.getElementById("pf-m-runondays")?.value;
        const el = document.getElementById("pf-custom-days");
        if (el) el.style.display = val === "Custom" ? "flex" : "none";
    },

    _syncInlineLegs() {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        const pf = this.portfolios[idx];
        (pf.slots || []).forEach((slot, i) => {
            const strat = document.getElementById(`leg-il-strat-${i}`);
            const inst = document.getElementById(`leg-il-inst-${i}`);
            const size = document.getElementById(`leg-il-size-${i}`);
            const enabled = document.getElementById(`leg-il-enabled-${i}`);
            const sltype = document.getElementById(`leg-il-sltype-${i}`);
            const slval = document.getElementById(`leg-il-slval-${i}`);
            const tptype = document.getElementById(`leg-il-tptype-${i}`);
            const tpval = document.getElementById(`leg-il-tpval-${i}`);
            if (strat) {
                const newStrat = strat.value;
                if (newStrat !== slot.strategy_name) {
                    slot.strategy_name = newStrat;
                    const stratDef = this.strategies[newStrat] || {};
                    slot.strategy_params = {};
                    if (stratDef.params) { for (const [k, info] of Object.entries(stratDef.params)) slot.strategy_params[k] = info.default; }
                }
            }
            if (inst) slot.bar_type_str = inst.value;
            if (size) slot.trade_size = parseFloat(size.value) || 1;
            if (enabled) slot.enabled = enabled.checked;
            if (!slot.exit_config) slot.exit_config = {};
            if (sltype) slot.exit_config.stop_loss_type = sltype.value;
            if (slval) slot.exit_config.stop_loss_value = parseFloat(slval.value) || 0;
            if (tptype) slot.exit_config.target_type = tptype.value;
            if (tpval) slot.exit_config.target_value = parseFloat(tpval.value) || 0;
        });
    },

    _onInlineStratChange(legIndex) {
        this._syncInlineLegs();
    },

    /** Build HTML for a single inline leg row */
    _buildInlineLegRow(slot, i) {
        const stratOpts = Object.keys(this.strategies).map(n => `<option value="${n}">${n}</option>`).join("");
        const barOpts = this.barTypes.map(bt => `<option value="${bt}">${App.barTypeLabel(bt)}</option>`).join("");
        const ec = slot.exit_config || {};
        const slTypes = ["none", "percentage", "points", "trailing"];
        const tpTypes = ["none", "percentage", "points"];
        const slTypeOpts = slTypes.map(t => `<option value="${t}" ${(ec.stop_loss_type||"none")===t?"selected":""}>${t === "none" ? "None" : t.charAt(0).toUpperCase()+t.slice(1)}</option>`).join("");
        const tpTypeOpts = tpTypes.map(t => `<option value="${t}" ${(ec.target_type||"none")===t?"selected":""}>${t === "none" ? "None" : t.charAt(0).toUpperCase()+t.slice(1)}</option>`).join("");
        const sOpts = stratOpts.replace(`value="${slot.strategy_name}"`, `value="${slot.strategy_name}" selected`);
        const bOpts = barOpts.replace(`value="${slot.bar_type_str}"`, `value="${slot.bar_type_str}" selected`);

        return `<tr>
            <td style="text-align:center;"><button class="leg-del-btn" onclick="Portfolio._deleteLeg(${i})" title="Delete">X</button></td>
            <td style="text-align:center;"><button class="leg-copy-btn" onclick="Portfolio._copyLeg(${i})" title="Copy">&#128203;</button></td>
            <td style="text-align:center; font-weight:600;">${i + 1}</td>
            <td style="text-align:center;"><input type="checkbox" id="leg-il-enabled-${i}" ${slot.enabled !== false ? "checked" : ""}></td>
            <td><select class="form-control" id="leg-il-strat-${i}" onchange="Portfolio._onInlineStratChange(${i})">${sOpts}</select></td>
            <td><select class="form-control" id="leg-il-inst-${i}">${bOpts}</select></td>
            <td><input type="number" class="form-control" id="leg-il-size-${i}" value="${slot.trade_size || 1}" min="1"></td>
            <td><select class="form-control" id="leg-il-sltype-${i}" style="min-width:72px;">${slTypeOpts}</select></td>
            <td><input type="number" class="form-control" id="leg-il-slval-${i}" value="${ec.stop_loss_value || 0}" step="0.5" min="0" style="width:52px;"></td>
            <td><select class="form-control" id="leg-il-tptype-${i}" style="min-width:72px;">${tpTypeOpts}</select></td>
            <td><input type="number" class="form-control" id="leg-il-tpval-${i}" value="${ec.target_value || 0}" step="0.5" min="0" style="width:52px;"></td>
            <td style="text-align:center;"><button class="btn btn-xs" onclick="Portfolio._editLeg(${i})" style="font-size:0.72rem;">&#9881; Edit</button></td>
        </tr>`;
    },

    /** Rebuild the entire legs tbody from current slot data (no modal reopen) */
    _refreshLegsTable() {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        const pf = this.portfolios[idx];
        const tbody = document.getElementById("pf-m-legs-body");
        if (!tbody) return;
        if ((pf.slots || []).length === 0) {
            tbody.innerHTML = `<tr><td colspan="12" style="text-align:center; padding:20px; color:var(--text-muted);">No legs. Click "+ Add Leg" to add.</td></tr>`;
        } else {
            tbody.innerHTML = pf.slots.map((slot, i) => this._buildInlineLegRow(slot, i)).join("");
        }
    },

    _copyLeg(legIndex) {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        this._syncInlineLegs();
        const pf = this.portfolios[idx];
        const copy = JSON.parse(JSON.stringify(pf.slots[legIndex]));
        this.slotCounter++;
        copy.slot_id = "s" + Date.now().toString(36) + this.slotCounter;
        pf.slots.splice(legIndex + 1, 0, copy);
        this._refreshLegsTable();
    },

    _toggleLeg(legIndex, checked) {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        this.portfolios[idx].slots[legIndex].enabled = checked;
    },

    _savePortfolioModal() {
        const idx = this._editingPfIndex;
        if (idx === null || idx === undefined) return;
        const pf = this.portfolios[idx];

        // Sync inline leg values first
        this._syncInlineLegs();

        // Read portfolio-level values from modal
        pf.name = document.getElementById("pf-m-name-footer")?.value || document.getElementById("pf-m-name")?.value || "Unnamed";
        pf.starting_capital = parseFloat(document.getElementById("pf-m-capital")?.value) || 100000;
        pf.allocation_mode = document.getElementById("pf-m-alloc")?.value || "equal";
        pf.description = document.getElementById("pf-m-remarks-footer")?.value || document.getElementById("pf-m-desc")?.value || "";
        pf.start_date = document.getElementById("pf-m-start")?.value || null;
        pf.end_date = document.getElementById("pf-m-end")?.value || null;
        pf.squareoff_time = document.getElementById("pf-m-sqoff")?.value || null;
        pf.squareoff_tz = document.getElementById("pf-m-sqofftz")?.value || null;
        pf.max_loss = parseFloat(document.getElementById("pf-m-maxloss")?.value) || null;
        pf.max_profit = parseFloat(document.getElementById("pf-m-maxprofit")?.value) || null;

        // UI-only fields — kept on client object for display, never sent to server
        pf._ui = pf._ui || {};
        // Top bar
        pf._ui.max_legs = parseInt(document.getElementById("pf-m-maxlegs")?.value) || 24;
        pf._ui.tgt_sl_per_lot = document.getElementById("pf-m-tgtsl-perlot")?.checked || false;
        // Execution Settings
        pf._ui.product = document.getElementById("pf-m-product")?.value || "MIS";
        pf._ui.strategy_tag = document.getElementById("pf-m-strattag")?.value || "Default";
        pf._ui.on_leg_fail = document.getElementById("pf-m-legfail")?.value || "KeepPlacedLegs";
        // Execution Mode
        pf._ui.execution_mode = document.getElementById("pf-m-execmode")?.value || "Start time";
        pf._ui.based_on = document.getElementById("pf-m-basedon")?.value || "None";
        pf._ui.entry_price = parseFloat(document.getElementById("pf-m-entryprice")?.value) || 0;
        pf._ui.rounding_value = parseFloat(document.getElementById("pf-m-rounding")?.value) || 0;
        pf._ui.adjust_price = document.getElementById("pf-m-adjustprice")?.value || "";
        // Timing (execution-level)
        pf._ui.run_on_days = document.getElementById("pf-m-runondays")?.value || "All Days";
        pf._ui.selected_days = [...document.querySelectorAll(".pf-custom-day:checked")].map(el => el.value);
        pf._ui.start_time = document.getElementById("pf-m-starttime")?.value || "09:30:00";
        pf._ui.end_time = document.getElementById("pf-m-endtime")?.value || "16:15:00";
        pf._ui.sqoff_time_exec = document.getElementById("pf-m-sqofftime-exec")?.value || "16:15:00";
        pf._ui.start_day = document.getElementById("pf-m-startday")?.value || "Before Expiry";
        pf._ui.start_day_offset = parseInt(document.getElementById("pf-m-startdayoff")?.value) || 1;
        pf._ui.sqoff_day = parseInt(document.getElementById("pf-m-sqoffday")?.value) || 0;
        pf._ui.holiday_handling = document.getElementById("pf-m-holiday")?.checked || false;
        // Range BreakOut
        pf._ui.rbo_enabled = document.getElementById("pf-m-rbo-enabled")?.checked || false;
        pf._ui.range_monitoring_start = document.getElementById("pf-m-rbo-monstart")?.value || "09:30:00";
        pf._ui.range_monitoring_end = document.getElementById("pf-m-rbo-monend")?.value || "10:30:00";
        pf._ui.entry_start = document.getElementById("pf-m-rbo-entrystart")?.value || "10:30:00";
        pf._ui.entry_end = document.getElementById("pf-m-rbo-entryend")?.value || "16:15:00";
        pf._ui.entry_at = document.getElementById("pf-m-rbo-entryat")?.value || "Any";
        pf._ui.range_buffer = parseInt(document.getElementById("pf-m-rbo-buffer")?.value) || 0;
        pf._ui.monitoring = document.getElementById("pf-m-rbo-monitoring")?.value || "Underlying";
        pf._ui.cancel_other = document.getElementById("pf-m-rbo-cancelother")?.checked || false;
        // Dynamic Hedge
        pf._ui.hedge_type = document.getElementById("pf-m-dh-type")?.value || "PremiumBased";
        pf._ui.sqoff_leg_on = document.getElementById("pf-m-dh-sqoff")?.value || "LegSqOff";
        pf._ui.hedge_distance_strike_min = parseInt(document.getElementById("pf-m-dh-distmin")?.value) || 10;
        pf._ui.hedge_distance_strike_max = parseInt(document.getElementById("pf-m-dh-distmax")?.value) || 20;
        pf._ui.premium_min = parseFloat(document.getElementById("pf-m-dh-premmin")?.value) || 0.05;
        pf._ui.premium_max = parseFloat(document.getElementById("pf-m-dh-premmax")?.value) || 40.00;
        pf._ui.hedge_fixed_min_distance = parseInt(document.getElementById("pf-m-dh-fixeddist")?.value) || 50;
        // Target
        pf._ui.target_enabled = document.getElementById("pf-m-tgt-enabled")?.checked || false;
        pf._ui.target_type = document.getElementById("pf-m-tgt-type")?.value || "Combined Profit";
        pf._ui.target_value = parseFloat(document.getElementById("pf-m-tgt-value")?.value) || 0;
        pf._ui.on_target = document.getElementById("pf-m-tgt-action")?.value || "SqOff";
        pf._ui.target_delay = parseInt(document.getElementById("pf-m-tgt-delay")?.value) || 0;
        pf._ui.target_reexecute_count = parseInt(document.getElementById("pf-m-tgt-reexcount")?.value) || 0;
        pf._ui.trail_target_enabled = document.getElementById("pf-m-tgt-trail-enabled")?.checked || false;
        pf._ui.trail_lock_min_profit = parseFloat(document.getElementById("pf-m-tgt-trail-lock")?.value) || 0;
        pf._ui.trail_when_profit_reach = parseFloat(document.getElementById("pf-m-tgt-trail-reach")?.value) || 0;
        pf._ui.trail_every = parseFloat(document.getElementById("pf-m-tgt-trail-every")?.value) || 0;
        pf._ui.trail_by = parseFloat(document.getElementById("pf-m-tgt-trail-by")?.value) || 0;
        // Stoploss
        pf._ui.sl_enabled = document.getElementById("pf-m-sl-enabled")?.checked || false;
        pf._ui.sl_type = document.getElementById("pf-m-sl-type")?.value || "Combined Loss";
        pf._ui.sl_value = parseFloat(document.getElementById("pf-m-sl-value")?.value) || 0;
        pf._ui.on_sl_action = document.getElementById("pf-m-sl-action")?.value || "SqOff";
        pf._ui.sl_delay = parseInt(document.getElementById("pf-m-sl-delay")?.value) || 0;
        pf._ui.sl_reexecute_count = parseInt(document.getElementById("pf-m-sl-reexcount")?.value) || 0;
        pf._ui.trail_sl_enabled = document.getElementById("pf-m-sl-trail-enabled")?.checked || false;
        pf._ui.trail_sl_every = parseFloat(document.getElementById("pf-m-sl-trail-every")?.value) || 0;
        pf._ui.trail_sl_by = parseFloat(document.getElementById("pf-m-sl-trail-by")?.value) || 0;
        pf._ui.move_sl_enabled = document.getElementById("pf-m-sl-move-enabled")?.checked || false;
        pf._ui.move_sl_safety_seconds = parseInt(document.getElementById("pf-m-sl-move-safety")?.value) || 0;
        pf._ui.move_sl_action = document.getElementById("pf-m-sl-move-action")?.value || "Move Only for Profitable Legs";
        pf._ui.trail_after_move_sl = document.getElementById("pf-m-sl-move-trail")?.checked || false;
        pf._ui.no_move_buy_legs = document.getElementById("pf-m-sl-move-nobuy")?.checked || false;
        pf._ui.hit_on_leg_sl = document.getElementById("pf-m-sl-move-hitsl")?.checked || false;
        pf._ui.hit_on_leg_target = document.getElementById("pf-m-sl-move-hittgt")?.checked || false;
        pf._ui.sqoff_loss_legs = document.getElementById("pf-m-sl-sqoff-loss")?.checked || false;
        pf._ui.sqoff_profit_legs = document.getElementById("pf-m-sl-sqoff-profit")?.checked || false;
        // Monitoring
        pf._ui.leg_target_monitoring = document.getElementById("pf-m-mon-legtgt")?.value || "Realtime";
        pf._ui.leg_trailing_monitoring = document.getElementById("pf-m-mon-legtrail")?.value || "Realtime";
        pf._ui.leg_sl_monitoring = document.getElementById("pf-m-mon-legsl")?.value || "Realtime";
        pf._ui.leg_sl_trailing_monitoring = document.getElementById("pf-m-mon-legsltrail")?.value || "Realtime";
        pf._ui.combined_target_monitoring = document.getElementById("pf-m-mon-combtgt")?.value || "Realtime";
        pf._ui.combined_sl_monitoring = document.getElementById("pf-m-mon-combsl")?.value || "Realtime";
        // ReExecute
        pf._ui.no_reexec_sl_cost = document.getElementById("pf-m-reex-noslcost")?.checked || false;
        pf._ui.no_wait_trade_reexec = document.getElementById("pf-m-reex-nowait")?.checked || false;
        pf._ui.no_strike_change_reexec = document.getElementById("pf-m-reex-nostrike")?.checked || false;
        pf._ui.no_reentry_after_end = document.getElementById("pf-m-reex-noend")?.checked || false;
        pf._ui.no_reentry_sl_cost = document.getElementById("pf-m-reex-noreentry")?.checked ?? true;
        // Exit Settings
        pf._ui.exit_order_type = document.getElementById("pf-m-exit-ordertype")?.value || "MARKET";
        pf._ui.exit_sell_first = document.getElementById("pf-m-exit-sellfirst")?.checked ?? true;
        pf._ui.on_portfolio_complete = document.getElementById("pf-m-exit-oncomplete")?.value || "None";
        // Other Settings
        pf._ui.trail_wait_trade = document.getElementById("pf-m-other-trailwait")?.checked || false;
        pf._ui.straddle_width_multiplier = parseFloat(document.getElementById("pf-m-other-swm")?.value) || 0;
        pf._ui.delay_between_legs = parseInt(document.getElementById("pf-m-other-legdelay")?.value) || 0;
        pf._ui.on_target_action_on = document.getElementById("pf-m-other-tgtaction")?.value || "OnTarget_N_Trailing_Both";
        pf._ui.on_sl_action_on = document.getElementById("pf-m-other-slaction")?.value || "OnSL_N_Trailing_Both";

        // Strip UI-only fields before sending to server
        const cleanPf = JSON.parse(JSON.stringify(pf));
        delete cleanPf._enabled;
        delete cleanPf._ui;
        delete cleanPf.on_leg_fail;
        delete cleanPf.execution_mode;
        delete cleanPf.strategy_tag;
        delete cleanPf.max_legs;
        delete cleanPf.tgt_sl_per_lot;
        App.api("/api/portfolios/save", { method: "POST", body: JSON.stringify(cleanPf) })
            .then(() => App.log(`Portfolio "${pf.name}" saved`, "SUCCESS", "Multileg", pf.name))
            .catch(() => {});

        this._closeModal();
        this.renderApp();
        App.toast(`Portfolio "${pf.name}" saved.`, "success");
    },

    _cancelPortfolioModal(isNew) {
        if (isNew) this.portfolios.pop();
        this._closeModal();
        this.renderApp();
    },

    /* ─── Leg (slot) operations inside the portfolio modal ────────────── */

    _addLeg() {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        this._syncInlineLegs();
        const pf = this.portfolios[idx];
        const firstStrat = Object.keys(this.strategies)[0] || "EMA Cross";
        const strat = this.strategies[firstStrat] || {};
        const defaultParams = {};
        if (strat.params) { for (const [k, info] of Object.entries(strat.params)) defaultParams[k] = info.default; }
        this.slotCounter++;
        pf.slots = pf.slots || [];
        pf.slots.push({
            slot_id: "s" + Date.now().toString(36) + this.slotCounter,
            strategy_name: firstStrat, strategy_params: defaultParams,
            bar_type_str: this.barTypes[0] || "", trade_size: 1, allocation_pct: 0,
            exit_config: { stop_loss_type: "none", stop_loss_value: 0, trailing_sl_step: 0, trailing_sl_offset: 0,
                target_type: "none", target_value: 0, sl_wait_bars: 0, on_sl_action: "close", on_target_action: "close",
                max_re_executions: 0, squareoff_time: null, squareoff_tz: null },
            enabled: true, squareoff_time: null, squareoff_tz: null,
        });
        // Just refresh the table rows in-place — no modal reopen
        this._refreshLegsTable();
    },

    _editLeg(legIndex) {
        this._syncInlineLegs();
        this._openLegModal(legIndex, false);
    },

    _deleteLeg(legIndex) {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        this._syncInlineLegs();
        this.portfolios[idx].slots.splice(legIndex, 1);
        // Just refresh the table rows in-place — no modal reopen
        this._refreshLegsTable();
    },

    _openLegModal(legIndex, isNew) {
        this._editingSlotIndex = legIndex;
        this._editingLegIsNew = isNew;
        const pf = this.portfolios[this._editingPfIndex];
        const slot = pf.slots[legIndex];
        const title = isNew ? `Add Leg #${legIndex + 1}` : `Edit Leg #${legIndex + 1} — ${slot.strategy_name}`;

        const stratOpts = Object.keys(this.strategies).map(n =>
            `<option value="${n}" ${n === slot.strategy_name ? "selected" : ""}>${n}</option>`).join("");
        const barOpts = this.barTypes.map(bt =>
            `<option value="${bt}" ${bt === slot.bar_type_str ? "selected" : ""}>${App.barTypeLabel(bt)}</option>`).join("");
        const paramsHTML = this._buildParamsHTML(pf, legIndex);
        const ec = slot.exit_config || {};
        const slTypes = ["none", "percentage", "points", "trailing"];
        const tpTypes = ["none", "percentage", "points"];
        const actions = ["close", "re_execute", "reverse"];

        const body = `
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; padding:8px; background:#f8f9fa; border:1px solid var(--border-color); border-radius:4px;">
                <div style="flex:2; min-width:140px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">STRATEGY</div>
                    <select class="form-control" id="leg-m-strategy" onchange="Portfolio._onLegStratChange()" style="font-size:0.82rem; padding:5px 8px;">${stratOpts}</select>
                </div>
                <div style="flex:2; min-width:140px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">INSTRUMENT</div>
                    <select class="form-control" id="leg-m-instrument" style="font-size:0.82rem; padding:5px 8px;">${barOpts}</select>
                </div>
                <div style="flex:0.7; min-width:70px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">SIZE</div>
                    <input type="number" class="form-control" id="leg-m-size" value="${slot.trade_size || 1}" min="1" style="font-size:0.82rem; padding:5px 8px;">
                </div>
                <div style="flex:0.7; min-width:70px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">ALLOC %</div>
                    <input type="number" class="form-control" id="leg-m-alloc" value="${slot.allocation_pct || 0}" min="0" max="100" step="0.5" style="font-size:0.82rem; padding:5px 8px;">
                </div>
            </div>

            <div style="display:flex; gap:0; border-bottom:2px solid var(--border-color); margin-bottom:10px;">
                <button class="slot-tab-btn active" data-tab="leg-params" onclick="Portfolio._switchLegTab('leg-params')">Parameters</button>
                <button class="slot-tab-btn" data-tab="leg-stoploss" onclick="Portfolio._switchLegTab('leg-stoploss')">Stoploss</button>
                <button class="slot-tab-btn" data-tab="leg-target" onclick="Portfolio._switchLegTab('leg-target')">Target</button>
                <button class="slot-tab-btn" data-tab="leg-timing" onclick="Portfolio._switchLegTab('leg-timing')">Timing</button>
            </div>

            <div class="leg-tab-content" id="leg-tab-leg-params">
                <div id="leg-m-params" style="display:flex; flex-wrap:wrap; gap:10px;">${paramsHTML}</div>
            </div>
            <div class="leg-tab-content" id="leg-tab-leg-stoploss" style="display:none;">
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <div class="form-group" style="flex:1; min-width:110px;"><label class="form-label">SL Type</label>
                        <select class="form-control" id="leg-m-sltype">${slTypes.map(t => `<option value="${t}" ${(ec.stop_loss_type||"none")===t?"selected":""}>${t}</option>`).join("")}</select></div>
                    <div class="form-group" style="flex:1; min-width:80px;"><label class="form-label">SL Value</label>
                        <input type="number" class="form-control" id="leg-m-slval" value="${ec.stop_loss_value||0}" step="0.5" min="0"></div>
                    <div class="form-group" style="flex:1; min-width:80px;"><label class="form-label">Trail Step</label>
                        <input type="number" class="form-control" id="leg-m-trailstep" value="${ec.trailing_sl_step||0}" step="0.5" min="0"></div>
                    <div class="form-group" style="flex:1; min-width:80px;"><label class="form-label">Trail Offset</label>
                        <input type="number" class="form-control" id="leg-m-trailoff" value="${ec.trailing_sl_offset||0}" step="0.5" min="0"></div>
                    <div class="form-group" style="flex:1; min-width:80px;"><label class="form-label">SL Wait</label>
                        <input type="number" class="form-control" id="leg-m-slwait" value="${ec.sl_wait_bars||0}" step="1" min="0"></div>
                    <div class="form-group" style="flex:1; min-width:100px;"><label class="form-label">On SL</label>
                        <select class="form-control" id="leg-m-slaction">${actions.map(a => `<option value="${a}" ${(ec.on_sl_action||"close")===a?"selected":""}>${a}</option>`).join("")}</select></div>
                    <div class="form-group" style="flex:1; min-width:80px;"><label class="form-label">Max Re-ex</label>
                        <input type="number" class="form-control" id="leg-m-maxreex" value="${ec.max_re_executions||0}" step="1" min="0"></div>
                </div>
            </div>
            <div class="leg-tab-content" id="leg-tab-leg-target" style="display:none;">
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <div class="form-group" style="flex:1; min-width:110px;"><label class="form-label">TP Type</label>
                        <select class="form-control" id="leg-m-tptype">${tpTypes.map(t => `<option value="${t}" ${(ec.target_type||"none")===t?"selected":""}>${t}</option>`).join("")}</select></div>
                    <div class="form-group" style="flex:1; min-width:80px;"><label class="form-label">TP Value</label>
                        <input type="number" class="form-control" id="leg-m-tpval" value="${ec.target_value||0}" step="0.5" min="0"></div>
                    <div class="form-group" style="flex:1; min-width:100px;"><label class="form-label">On TP</label>
                        <select class="form-control" id="leg-m-tpaction">${actions.map(a => `<option value="${a}" ${(ec.on_target_action||"close")===a?"selected":""}>${a}</option>`).join("")}</select></div>
                </div>
            </div>
            <div class="leg-tab-content" id="leg-tab-leg-timing" style="display:none;">
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <div class="form-group" style="flex:1; min-width:100px;"><label class="form-label">Slot SqOff</label>
                        <input type="time" class="form-control" id="leg-m-sqoff" value="${slot.squareoff_time||''}"></div>
                    <div class="form-group" style="flex:1; min-width:130px;"><label class="form-label">Slot SqOff TZ</label>
                        ${this._renderTzSelect(slot.squareoff_tz, "leg-m-sqofftz", "(inherit)")}</div>
                    <div class="form-group" style="flex:1; min-width:100px;"><label class="form-label">Leg SqOff</label>
                        <input type="time" class="form-control" id="leg-m-legsqoff" value="${ec.squareoff_time||''}"></div>
                    <div class="form-group" style="flex:1; min-width:130px;"><label class="form-label">Leg SqOff TZ</label>
                        ${this._renderTzSelect(ec.squareoff_tz, "leg-m-legsqofftz", "(inherit)")}</div>
                </div>
            </div>

            <div style="display:flex; justify-content:flex-end; gap:8px; margin-top:14px; padding-top:10px; border-top:1px solid var(--border-color);">
                <button class="btn btn-sm" onclick="Portfolio._cancelLegModal()">Cancel</button>
                <button class="btn btn-sm btn-primary" onclick="Portfolio._saveLegModal()">Save Leg</button>
            </div>
        `;
        // Close the portfolio modal and open leg modal
        this._closeModal();
        this._openModal(title, body, 700);
    },

    _switchLegTab(tabName) {
        document.querySelectorAll(".leg-tab-content").forEach(el => el.style.display = "none");
        // Only toggle leg tab buttons (not pf tab buttons)
        document.querySelectorAll(`.slot-tab-btn[data-tab^="leg-"]`).forEach(el => el.classList.remove("active"));
        const content = document.getElementById(`leg-tab-${tabName}`);
        if (content) content.style.display = "";
        const btn = document.querySelector(`.slot-tab-btn[data-tab="${tabName}"]`);
        if (btn) btn.classList.add("active");
    },

    _onLegStratChange() {
        const pfIdx = this._editingPfIndex;
        const legIdx = this._editingSlotIndex;
        if (pfIdx === null || legIdx === null) return;
        const stratName = document.getElementById("leg-m-strategy").value;
        const pf = this.portfolios[pfIdx];
        const slot = pf.slots[legIdx];
        slot.strategy_name = stratName;
        const strat = this.strategies[stratName] || {};
        slot.strategy_params = {};
        if (strat.params) { for (const [k, info] of Object.entries(strat.params)) slot.strategy_params[k] = info.default; }
        document.getElementById("leg-m-params").innerHTML = this._buildParamsHTML(pf, legIdx);
    },

    _saveLegModal() {
        const pfIdx = this._editingPfIndex;
        const legIdx = this._editingSlotIndex;
        if (pfIdx === null || legIdx === null) return;
        const slot = this.portfolios[pfIdx].slots[legIdx];

        slot.strategy_name = document.getElementById("leg-m-strategy").value;
        slot.bar_type_str = document.getElementById("leg-m-instrument").value;
        slot.trade_size = parseFloat(document.getElementById("leg-m-size").value) || 1;
        slot.allocation_pct = parseFloat(document.getElementById("leg-m-alloc").value) || 0;

        // Read params
        slot.strategy_params = {};
        document.querySelectorAll("#leg-m-params [data-param-key]").forEach(el => {
            const key = el.getAttribute("data-param-key");
            const ptype = el.getAttribute("data-param-type");
            if (ptype === "bool") slot.strategy_params[key] = el.checked;
            else if (ptype === "time") {
                const iz = el.getAttribute("data-inherit-zero") === "1";
                slot.strategy_params[key] = (iz && el.value === "") ? 0 : parseInt((el.value||"00:00").replace(":",""),10);
            } else if (ptype === "float") slot.strategy_params[key] = parseFloat(el.value);
            else slot.strategy_params[key] = parseInt(el.value);
        });

        // Exit config
        const ec = slot.exit_config = slot.exit_config || {};
        ec.stop_loss_type = document.getElementById("leg-m-sltype").value;
        ec.stop_loss_value = parseFloat(document.getElementById("leg-m-slval").value) || 0;
        ec.trailing_sl_step = parseFloat(document.getElementById("leg-m-trailstep").value) || 0;
        ec.trailing_sl_offset = parseFloat(document.getElementById("leg-m-trailoff").value) || 0;
        ec.sl_wait_bars = parseInt(document.getElementById("leg-m-slwait").value) || 0;
        ec.on_sl_action = document.getElementById("leg-m-slaction").value;
        ec.max_re_executions = parseInt(document.getElementById("leg-m-maxreex").value) || 0;
        ec.target_type = document.getElementById("leg-m-tptype").value;
        ec.target_value = parseFloat(document.getElementById("leg-m-tpval").value) || 0;
        ec.on_target_action = document.getElementById("leg-m-tpaction").value;
        ec.squareoff_time = document.getElementById("leg-m-legsqoff").value || null;
        ec.squareoff_tz = document.getElementById("leg-m-legsqofftz").value || null;
        slot.squareoff_time = document.getElementById("leg-m-sqoff").value || null;
        slot.squareoff_tz = document.getElementById("leg-m-sqofftz").value || null;

        // Go back to portfolio modal
        this._closeModal();
        this._openPortfolioModal(pfIdx, false);
    },

    _cancelLegModal() {
        if (this._editingLegIsNew) {
            const pfIdx = this._editingPfIndex;
            if (pfIdx !== null) this.portfolios[pfIdx].slots.pop();
        }
        this._closeModal();
        this._openPortfolioModal(this._editingPfIndex, false);
    },

    _buildParamsHTML(pf, legIndex) {
        const slot = pf.slots[legIndex];
        const strat = this.strategies[slot.strategy_name] || {};
        const params = strat.params || {};
        let html = "";
        for (const [key, info] of Object.entries(params)) {
            const val = slot.strategy_params[key] !== undefined ? slot.strategy_params[key] : info.default;
            if (info.type === "time") {
                const vNum = parseInt(val??0,10)||0;
                const iz = info.inherit_zero === true;
                const hhmm = (iz && vNum === 0) ? "" : `${String(Math.floor(vNum/100)).padStart(2,"0")}:${String(vNum%100).padStart(2,"0")}`;
                html += `<div class="form-group" style="min-width:110px;"><label class="form-label">${info.label}</label>
                    <input type="time" class="form-control" data-param-key="${key}" data-param-type="time" value="${hhmm}" ${iz?'data-inherit-zero="1"':''}></div>`;
            } else if (typeof info.default === "boolean") {
                html += `<div class="form-group" style="min-width:110px;"><label class="form-label">${info.label}</label>
                    <input type="checkbox" data-param-key="${key}" data-param-type="bool" ${val?"checked":""}></div>`;
            } else {
                const step = typeof info.default === "number" && !Number.isInteger(info.default) ? "0.5" : "1";
                html += `<div class="form-group" style="min-width:110px;"><label class="form-label">${info.label}</label>
                    <input type="number" class="form-control" data-param-key="${key}" data-param-type="${Number.isInteger(info.default)?'int':'float'}" value="${val}"
                        ${info.min!==undefined?`min="${info.min}"`:""} ${info.max!==undefined?`max="${info.max}"`:""} step="${step}"></div>`;
            }
        }
        return html || '<p class="section-caption">No configurable parameters.</p>';
    },

    /* ═══════════════════════════════════════════════════════════════════════
       MODAL HELPERS
       ═══════════════════════════════════════════════════════════════════════ */

    _openModal(title, bodyHTML, width, footerHTML) {
        this._closeModal();
        const backdrop = document.createElement("div");
        backdrop.className = "modal-backdrop";
        backdrop.id = "pf-modal-backdrop";
        const wideClass = width > 900 ? " modal-wide" : "";
        backdrop.innerHTML = `
            <div class="modal-dialog${wideClass}" style="width:${width||640}px;">
                <div class="modal-header">
                    <span class="modal-header-title">${title}</span>
                    <button class="modal-close-btn" onclick="Portfolio._closeModal()">&times;</button>
                </div>
                <div class="modal-body">${bodyHTML}</div>
                ${footerHTML || ""}
            </div>`;
        document.body.appendChild(backdrop);
    },
    _closeModal() { const el = document.getElementById("pf-modal-backdrop"); if (el) el.remove(); },

    /* ═══════════════════════════════════════════════════════════════════════
       BACKTEST (unchanged logic — runs one portfolio)
       ═══════════════════════════════════════════════════════════════════════ */

    async runBacktest() {
        const pf = this._currentPortfolio;
        if (!pf) { App.toast("No portfolio selected.", "error"); return; }
        const enabledSlots = (pf.slots||[]).filter(s => s.enabled !== false);
        if (enabledSlots.length === 0) { App.toast("Portfolio has no enabled slots.", "error"); return; }

        pf.allocation_mode = pf.allocation_mode || "equal";
        if (pf.allocation_mode === "percentage") {
            const totalPct = enabledSlots.reduce((sum, s) => sum + (s.allocation_pct || 0), 0);
            if (totalPct <= 0 || totalPct > 100) { App.toast(`Alloc % must sum to 1-100%. Current: ${totalPct.toFixed(1)}%`, "error"); return; }
        }

        // Strip UI-only fields before sending to server
        const cleanPf = JSON.parse(JSON.stringify(pf));
        delete cleanPf._enabled;
        delete cleanPf._ui;
        delete cleanPf.on_leg_fail;
        delete cleanPf.execution_mode;
        delete cleanPf.strategy_tag;
        delete cleanPf.max_legs;
        delete cleanPf.tgt_sl_per_lot;

        App.log(`Backtest started: "${pf.name}" with ${enabledSlots.length} slot(s)`, "MESSAGE", "Multileg", pf.name);
        const progressDiv = document.getElementById("pf-progress");
        progressDiv.innerHTML = `<div class="card" style="padding:16px;">
            <div class="progress-text" id="pf-progress-text">Initializing backtest for "${pf.name}"...</div>
            <div class="progress-bar-container"><div class="progress-bar-fill" id="pf-progress-bar" style="width:0%;"></div></div>
            <div id="pf-progress-details" style="margin-top:8px; font-size:0.85rem; color:var(--text-secondary);"></div>
            <div id="pf-progress-slots" style="margin-top:12px;"></div>
        </div>`;

        try {
            const response = await fetch("/api/portfolios/backtest", {
                method: "POST", headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ portfolio: cleanPf }),
            });
            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "", slotInfo = [];
            while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n"); buffer = lines.pop();
                for (const line of lines) {
                    if (!line.trim()) continue;
                    let evt; try { evt = JSON.parse(line); } catch { continue; }
                    const bar = document.getElementById("pf-progress-bar");
                    const text = document.getElementById("pf-progress-text");
                    const details = document.getElementById("pf-progress-details");
                    const slotsDiv = document.getElementById("pf-progress-slots");
                    if (evt.event === "start") {
                        slotInfo = evt.slots || [];
                        if (slotsDiv) slotsDiv.innerHTML = slotInfo.map(s => `<div id="pf-slot-${s.slot_id}" style="padding:4px 0; font-size:0.85rem; color:var(--text-secondary);"><span class="slot-icon" style="margin-right:6px;">&#9723;</span>${s.display_name}</div>`).join("");
                    } else if (evt.event === "progress") {
                        const pct = evt.total > 0 ? Math.round((evt.completed / evt.total) * 100) : 0;
                        if (bar) bar.style.width = pct + "%";
                        if (text) text.textContent = evt.message || "Processing...";
                        if (evt.phase === "engine") {
                            for (const s of slotInfo) { const el = document.getElementById(`pf-slot-${s.slot_id}`); if (el && !el.dataset.done) { const ic = el.querySelector(".slot-icon"); if (ic) ic.innerHTML = "&#9881;"; el.style.color = "var(--accent)"; } }
                            if (evt.completed_slot_id) { const el = document.getElementById(`pf-slot-${evt.completed_slot_id}`); if (el) { const ic = el.querySelector(".slot-icon"); if (ic) ic.innerHTML = "&#9989;"; el.style.color = "var(--text-primary)"; el.dataset.done = "1"; } }
                            if (details) details.textContent = `${evt.slots_completed||0}/${slotInfo.length} strategies completed`;
                        } else if (evt.phase === "reports") { for (const s of slotInfo) { const el = document.getElementById(`pf-slot-${s.slot_id}`); if (el) { const ic = el.querySelector(".slot-icon"); if (ic) ic.innerHTML = "&#9989;"; el.style.color = "var(--text-primary)"; } } if (details) details.textContent = evt.message; }
                    } else if (evt.event === "complete") {
                        if (bar) bar.style.width = "100%";
                        if (text) text.textContent = `Backtest complete in ${evt.elapsed?.toFixed(1)||"?"}s`;
                        if (details) details.textContent = "";
                        for (const s of slotInfo) { const el = document.getElementById(`pf-slot-${s.slot_id}`); if (el) { const ic = el.querySelector(".slot-icon"); if (ic) ic.innerHTML = "&#9989;"; el.style.color = "var(--text-primary)"; } }
                        const results = evt.results;
                        if (results && !results.error) {
                            this.results = results;
                            App.state.portfolioResults = results;
                            document.getElementById("pf-results").innerHTML = this._renderResults();
                            App.toast("Backtest finished!", "success");
                            App.log(`Backtest completed for "${pf.name}" in ${evt.elapsed?.toFixed(1)||"?"}s`, "SUCCESS", "Multileg", pf.name);
                        } else { throw new Error(results?.error || "Unknown error"); }
                    } else if (evt.event === "error") { throw new Error(evt.error || "Backtest failed"); }
                }
            }
        } catch (e) {
            document.getElementById("pf-progress").innerHTML = `<div class="alert alert-danger">Backtest failed: ${e.message}</div>`;
            App.log(`Backtest failed: ${e.message}`, "ERROR", "Multileg", pf.name);
        }
    },

    _renderResults() {
        const r = this.results;
        if (!r) return "";
        let flagsHTML = "";
        if (r.max_loss_hit) flagsHTML += '<span class="badge badge-danger">Max Loss Hit</span> ';
        if (r.max_profit_hit) flagsHTML += '<span class="badge badge-success">Max Profit Hit</span> ';
        let reportBtnHTML = "";
        if (r.report_file) {
            reportBtnHTML = `<a class="btn btn-sm btn-primary" href="/api/reports/${encodeURIComponent(r.report_file)}" download style="margin-right:6px;">&#128196; Download Report</a>`;
        }
        let warningsHTML = "";
        if (r.warnings && r.warnings.length) {
            const items = r.warnings.map(w => `<li><strong>${w.display_name}:</strong> ${w.warning}</li>`).join("");
            warningsHTML = `<div class="alert alert-warning" style="margin-bottom:12px;padding:8px 12px;font-size:0.85rem;"><strong>&#9888; Warnings:</strong><ul style="margin:4px 0 0 16px;padding:0;">${items}</ul></div>`;
        }
        let perStratRows = "";
        if (r.per_strategy) {
            for (const [, sr] of Object.entries(r.per_strategy)) {
                const cls = sr.pnl >= 0 ? "positive" : "negative";
                const warnIcon = sr.warning ? ' <span title="' + sr.warning.replace(/"/g, '&quot;') + '" style="cursor:help;color:#e6a817;">&#9888;</span>' : "";
                perStratRows += `<tr><td>${sr.display_name}${warnIcon}</td><td class="${cls}">${App.currency(sr.pnl)}</td><td>${sr.trades}</td><td>${sr.win_rate.toFixed(1)}%</td><td>${(sr.win_pct_days || 0).toFixed(1)}%</td><td>${sr.wins}</td><td>${sr.losses}</td></tr>`;
            }
        }
        return `<div class="portfolio-results">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                <span style="font-weight:600; font-size:0.95rem;">Results: ${r.portfolio_name||""}</span>
                <div>${flagsHTML}${reportBtnHTML}<button class="btn btn-sm btn-primary" onclick="App.navigate('portfolio_tearsheet')">Full Tearsheet</button></div>
            </div>
            ${warningsHTML}
            <div class="grid-6">
                ${App.metricHTML("Starting Capital", App.currency(r.starting_capital))}
                ${App.metricHTML("Final Balance", App.currency(r.final_balance))}
                ${App.metricHTML("Total P&L", App.currency(r.total_pnl), r.total_return_pct)}
                ${App.metricHTML("Total Trades", r.total_trades)}
                ${App.metricHTML("Win Rate (Trades)", r.win_rate.toFixed(1) + "%")}
                ${App.metricHTML("Win% (Days)", (r.win_pct_days || 0).toFixed(1) + "%")}
                ${App.metricHTML("Max Drawdown", r.max_drawdown.toFixed(2) + "%")}
            </div>
            ${perStratRows ? `<div class="table-container" style="margin-top:12px;"><table><thead><tr><th>Strategy</th><th>P&L</th><th>Trades</th><th>Win Rate (Trades)</th><th>Win% (Days)</th><th>Wins</th><th>Losses</th></tr></thead><tbody>${perStratRows}</tbody></table></div>` : ""}
        </div>`;
    },
};
