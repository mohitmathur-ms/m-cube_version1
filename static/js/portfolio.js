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
        const title = isNew ? "Create Portfolio" : `Edit Portfolio — ${pf.name}`;

        // Build slots table for the legs inside this portfolio
        let slotsRows = "";
        if ((pf.slots || []).length === 0) {
            slotsRows = `<tr><td colspan="8" style="text-align:center; padding:16px; color:var(--text-muted);">No slots. Click "+ Add Leg" to add.</td></tr>`;
        } else {
            slotsRows = pf.slots.map((slot, i) => {
                const ec = slot.exit_config || {};
                const sl = (ec.stop_loss_type || "none") !== "none" ? `${ec.stop_loss_type} ${ec.stop_loss_value || 0}` : "—";
                const tp = (ec.target_type || "none") !== "none" ? `${ec.target_type} ${ec.target_value || 0}` : "—";
                return `<tr>
                    <td style="text-align:center;">${i + 1}</td>
                    <td>${slot.strategy_name}</td>
                    <td>${App.barTypeLabel(slot.bar_type_str) || "—"}</td>
                    <td>${slot.trade_size || 1}</td>
                    <td>${sl}</td>
                    <td>${tp}</td>
                    <td>
                        <button class="btn btn-xs" onclick="Portfolio._editLeg(${i})">Edit</button>
                        <button class="btn btn-xs" style="color:var(--danger);" onclick="Portfolio._deleteLeg(${i})">X</button>
                    </td>
                </tr>`;
            }).join("");
        }

        const body = `
            <!-- Top: Portfolio-level settings (like reference EXCHG/SYMBOL row) -->
            <div style="display:flex; gap:8px; flex-wrap:wrap; margin-bottom:10px; padding:8px; background:#f8f9fa; border:1px solid var(--border-color); border-radius:4px;">
                <div style="flex:2; min-width:140px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">PORTFOLIO NAME</div>
                    <input type="text" class="form-control" id="pf-m-name" value="${pf.name}" style="font-size:0.82rem; padding:5px 8px;">
                </div>
                <div style="flex:1; min-width:100px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">CAPITAL</div>
                    <input type="number" class="form-control" id="pf-m-capital" value="${pf.starting_capital || 100000}" min="1000" style="font-size:0.82rem; padding:5px 8px;">
                </div>
                <div style="flex:1; min-width:100px;">
                    <div style="font-size:0.7rem; color:var(--text-muted); text-transform:uppercase; margin-bottom:2px;">ALLOCATION</div>
                    <select class="form-control" id="pf-m-alloc" style="font-size:0.82rem; padding:5px 8px;">
                        <option value="equal" ${(pf.allocation_mode || 'equal') === 'equal' ? 'selected' : ''}>Equal</option>
                        <option value="percentage" ${pf.allocation_mode === 'percentage' ? 'selected' : ''}>Percentage</option>
                    </select>
                </div>
            </div>

            <!-- + Add Leg button -->
            <div style="margin-bottom:8px;">
                <button class="btn btn-sm btn-primary" onclick="Portfolio._addLeg()">+ Add Leg</button>
            </div>

            <!-- Legs/Slots table (like reference legs table at top of modal) -->
            <div style="border:1px solid var(--border-color); border-radius:4px; overflow:auto; max-height:180px; margin-bottom:10px;">
                <table style="margin:0; font-size:0.82rem;">
                    <thead><tr>
                        <th style="width:30px;">#</th>
                        <th>Strategy</th>
                        <th>Instrument</th>
                        <th>Size</th>
                        <th>Stop Loss</th>
                        <th>Target</th>
                        <th>Actions</th>
                    </tr></thead>
                    <tbody id="pf-m-legs-body">${slotsRows}</tbody>
                </table>
            </div>

            <!-- Sub-tabs (like reference: Execution Parameters, Target, Stoploss, etc.) -->
            <div style="display:flex; gap:0; border-bottom:2px solid var(--border-color); margin-bottom:10px;">
                <button class="slot-tab-btn active" data-tab="pf-settings" onclick="Portfolio._switchPfTab('pf-settings')">Settings</button>
                <button class="slot-tab-btn" data-tab="pf-timing" onclick="Portfolio._switchPfTab('pf-timing')">Timing</button>
                <button class="slot-tab-btn" data-tab="pf-limits" onclick="Portfolio._switchPfTab('pf-limits')">Limits</button>
            </div>

            <div class="pf-tab-content" id="pf-tab-pf-settings">
                <div style="display:flex; gap:10px; flex-wrap:wrap;">
                    <div class="form-group" style="flex:1; min-width:120px;">
                        <label class="form-label">Description / Remarks</label>
                        <input type="text" class="form-control" id="pf-m-desc" value="${pf.description || ''}" placeholder="Optional remarks">
                    </div>
                </div>
            </div>

            <div class="pf-tab-content" id="pf-tab-pf-timing" style="display:none;">
                <fieldset style="border:1px solid var(--border-color); border-radius:4px; padding:12px;">
                    <legend style="font-weight:600; font-size:0.85rem; padding:0 6px;">Timing</legend>
                    <div style="display:flex; gap:8px; flex-wrap:wrap;">
                        <div class="form-group" style="flex:1; min-width:110px;">
                            <label class="form-label">Start Date</label>
                            <input type="date" class="form-control" id="pf-m-start" value="${pf.start_date || ''}">
                        </div>
                        <div class="form-group" style="flex:1; min-width:110px;">
                            <label class="form-label">End Date</label>
                            <input type="date" class="form-control" id="pf-m-end" value="${pf.end_date || ''}">
                        </div>
                        <div class="form-group" style="flex:1; min-width:100px;">
                            <label class="form-label">Squareoff Time</label>
                            <input type="time" class="form-control" id="pf-m-sqoff" value="${pf.squareoff_time || ''}">
                        </div>
                        <div class="form-group" style="flex:1; min-width:120px;">
                            <label class="form-label">Squareoff TZ</label>
                            ${this._renderTzSelect(pf.squareoff_tz, "pf-m-sqofftz", "(UTC)")}
                        </div>
                    </div>
                </fieldset>
            </div>

            <div class="pf-tab-content" id="pf-tab-pf-limits" style="display:none;">
                <fieldset style="border:1px solid var(--border-color); border-radius:4px; padding:12px;">
                    <legend style="font-weight:600; font-size:0.85rem; padding:0 6px;">Risk Limits</legend>
                    <div style="display:flex; gap:10px; flex-wrap:wrap;">
                        <div class="form-group" style="flex:1; min-width:120px;">
                            <label class="form-label">Max Loss ($)</label>
                            <input type="number" class="form-control" id="pf-m-maxloss" value="${pf.max_loss || ''}" min="0">
                        </div>
                        <div class="form-group" style="flex:1; min-width:120px;">
                            <label class="form-label">Max Profit ($)</label>
                            <input type="number" class="form-control" id="pf-m-maxprofit" value="${pf.max_profit || ''}" min="0">
                        </div>
                    </div>
                </fieldset>
            </div>

            <!-- Footer: Portfolio Name + SAVE PORTFOLIO button (like reference) -->
            <div style="display:flex; align-items:center; gap:12px; margin-top:14px; padding-top:10px; border-top:1px solid var(--border-color);">
                <span style="font-size:0.82rem; color:var(--text-secondary);">Portfolio Name</span>
                <input type="text" class="form-control" id="pf-m-name-footer" value="${pf.name}" style="width:200px; font-size:0.82rem; padding:4px 8px;">
                <span style="font-size:0.82rem; color:var(--text-secondary);">Remarks</span>
                <input type="text" class="form-control" id="pf-m-remarks-footer" value="${pf.description || ''}" style="flex:1; font-size:0.82rem; padding:4px 8px;">
                <button class="btn btn-sm" onclick="Portfolio._cancelPortfolioModal(${isNew ? 1 : 0})">Cancel</button>
                <button class="btn btn-sm btn-primary" style="min-width:140px; font-weight:600;" onclick="Portfolio._savePortfolioModal()">SAVE PORTFOLIO</button>
            </div>
        `;
        this._openModal(title, body, 780);
    },

    _switchPfTab(tabName) {
        document.querySelectorAll(".pf-tab-content").forEach(el => el.style.display = "none");
        document.querySelectorAll(".slot-tab-btn").forEach(el => el.classList.remove("active"));
        const content = document.getElementById(`pf-tab-${tabName}`);
        if (content) content.style.display = "";
        const btn = document.querySelector(`.slot-tab-btn[data-tab="${tabName}"]`);
        if (btn) btn.classList.add("active");
    },

    _savePortfolioModal() {
        const idx = this._editingPfIndex;
        if (idx === null || idx === undefined) return;
        const pf = this.portfolios[idx];

        // Read values from modal
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

        // Auto-save to server (strip UI-only fields)
        const cleanPf = JSON.parse(JSON.stringify(pf));
        delete cleanPf._enabled;
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
        const legIdx = pf.slots.length - 1;
        // Open leg editor immediately
        this._openLegModal(legIdx, true);
    },

    _editLeg(legIndex) {
        this._openLegModal(legIndex, false);
    },

    _deleteLeg(legIndex) {
        const idx = this._editingPfIndex;
        if (idx === null) return;
        this.portfolios[idx].slots.splice(legIndex, 1);
        // Refresh the portfolio modal
        this._closeModal();
        this._openPortfolioModal(idx, false);
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

    _openModal(title, bodyHTML, width) {
        this._closeModal();
        const backdrop = document.createElement("div");
        backdrop.className = "modal-backdrop";
        backdrop.id = "pf-modal-backdrop";
        backdrop.innerHTML = `
            <div class="modal-dialog" style="width:${width||640}px;">
                <div class="modal-header">
                    <span class="modal-header-title">${title}</span>
                    <button class="modal-close-btn" onclick="Portfolio._closeModal()">&times;</button>
                </div>
                <div class="modal-body">${bodyHTML}</div>
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

        // Strip UI-only fields (like _enabled) before sending to server
        const cleanPf = JSON.parse(JSON.stringify(pf));
        delete cleanPf._enabled;

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
        let perStratRows = "";
        if (r.per_strategy) {
            for (const [, sr] of Object.entries(r.per_strategy)) {
                const cls = sr.pnl >= 0 ? "positive" : "negative";
                perStratRows += `<tr><td>${sr.display_name}</td><td class="${cls}">${App.currency(sr.pnl)}</td><td>${sr.trades}</td><td>${sr.win_rate.toFixed(1)}%</td><td>${sr.wins}</td><td>${sr.losses}</td></tr>`;
            }
        }
        return `<div class="portfolio-results">
            <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:12px;">
                <span style="font-weight:600; font-size:0.95rem;">Results: ${r.portfolio_name||""}</span>
                <div>${flagsHTML}${reportBtnHTML}<button class="btn btn-sm btn-primary" onclick="App.navigate('portfolio_tearsheet')">Full Tearsheet</button></div>
            </div>
            <div class="grid-6">
                ${App.metricHTML("Starting Capital", App.currency(r.starting_capital))}
                ${App.metricHTML("Final Balance", App.currency(r.final_balance))}
                ${App.metricHTML("Total P&L", App.currency(r.total_pnl), r.total_return_pct)}
                ${App.metricHTML("Total Trades", r.total_trades)}
                ${App.metricHTML("Win Rate", r.win_rate.toFixed(1) + "%")}
                ${App.metricHTML("Max Drawdown", r.max_drawdown.toFixed(2) + "%")}
            </div>
            ${perStratRows ? `<div class="table-container" style="margin-top:12px;"><table><thead><tr><th>Strategy</th><th>P&L</th><th>Trades</th><th>Win Rate</th><th>Wins</th><th>Losses</th></tr></thead><tbody>${perStratRows}</tbody></table></div>` : ""}
        </div>`;
    },
};
