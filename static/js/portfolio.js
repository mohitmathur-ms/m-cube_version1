/**
 * Portfolio Page - Multi-strategy portfolio management with exit management.
 * Single-page layout: left sidebar for portfolio settings, right panel for slots + results.
 */

const Portfolio = {
    strategies: {},
    barTypes: [],
    barTypeDetails: {},
    portfolio: null,  // current PortfolioConfig dict
    results: null,
    templates: {},
    slotCounter: 0,

    async render(container) {
        container.innerHTML = `
            <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
                <div>
                    <h1 class="page-title" style="margin-bottom: 2px;">Portfolio Manager</h1>
                    <p class="page-subtitle" style="margin-bottom: 0;">Create multi-strategy portfolios with exit management.</p>
                </div>
            </div>
            <div id="portfolio-loading" class="alert alert-info">Loading configuration...</div>
            <div id="portfolio-app" style="display: none;"></div>
        `;

        await this.loadConfig();
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

            if (!this.portfolio) {
                this.portfolio = this._newPortfolio();
            }

            document.getElementById("portfolio-loading").style.display = "none";
            document.getElementById("portfolio-app").style.display = "";
            this.renderApp();
        } catch (e) {
            document.getElementById("portfolio-loading").innerHTML =
                `<div class="alert alert-danger">Failed to load: ${e.message}</div>`;
        }
    },

    _newPortfolio() {
        return {
            name: "New Portfolio",
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

    /** If portfolio-level dates are empty but every enabled slot shares the same
     *  non-empty range, lift them to the portfolio level and drop the per-slot
     *  copies. Makes imported JSONs (where every slot repeats the same range)
     *  editable from one pair of date inputs. */
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
        for (const sl of portfolio.slots) {
            sl.start_date = null;
            sl.end_date = null;
        }
    },

    /** Curated timezone list. Empty value = "inherit from outer level"
     *  (or UTC at the portfolio level). The runner falls back to UTC when
     *  null reaches it, but the inherit semantics let users set the tz once
     *  at the portfolio and override only when needed. */
    _SQUAREOFF_TZS: [
        "UTC",
        "America/New_York",
        "America/Chicago",
        "America/Los_Angeles",
        "Europe/London",
        "Europe/Berlin",
        "Asia/Tokyo",
        "Asia/Singapore",
        "Asia/Kolkata",
    ],

    /** Render a tz <select>. ``inheritLabel`` when truthy adds a leading
     *  "(inherit)" option that maps to null — used for slot/leg overrides. */
    _renderTzSelect(currentTz, onchangeJs, inheritLabel) {
        const opts = [];
        if (inheritLabel) {
            opts.push(`<option value="" ${!currentTz ? "selected" : ""}>${inheritLabel}</option>`);
        }
        for (const tz of this._SQUAREOFF_TZS) {
            opts.push(`<option value="${tz}" ${currentTz === tz ? "selected" : ""}>${tz}</option>`);
        }
        return `<select class="form-control" onchange="${onchangeJs}">${opts.join("")}</select>`;
    },

    renderApp() {
        const p = this.portfolio;

        // Template buttons
        const tmplBtns = Object.entries(this.templates).map(([name, desc]) =>
            `<button class="btn btn-sm" style="font-size: 0.75rem;" onclick="Portfolio.loadTemplate('${name}')" title="${desc}">${name}</button>`
        ).join(" ");

        // Saved portfolios
        let savedSection = `<button class="btn btn-sm btn-block" onclick="Portfolio.loadSavedList()">Load Saved</button>
            <div id="saved-list" style="margin-top: 6px;"></div>`;

        // Slots HTML
        let slotsHTML = "";
        if (p.slots.length === 0) {
            slotsHTML = '<div class="alert alert-info" style="margin: 0;">No strategy slots. Click "Add Slot" to add one.</div>';
        } else {
            slotsHTML = p.slots.map((slot, i) => this._renderSlot(slot, i)).join("");
        }

        // Results preview
        let resultsHTML = "";
        if (this.results) {
            resultsHTML = this._renderResults();
        }

        document.getElementById("portfolio-app").innerHTML = `
            <div class="portfolio-layout">
                <!-- Left sidebar -->
                <div class="portfolio-sidebar">
                    <div style="font-weight: 600; font-size: 0.95rem; color: var(--accent);">Portfolio Settings</div>

                    <div class="form-group" style="margin-bottom: 8px;">
                        <label class="form-label">Name</label>
                        <input type="text" class="form-control" id="pf-name" value="${p.name}" onchange="Portfolio.updateField('name', this.value)">
                    </div>
                    <div class="form-group" style="margin-bottom: 8px;">
                        <label class="form-label">Starting Capital ($)</label>
                        <input type="number" class="form-control" id="pf-capital" value="${p.starting_capital}" min="1000"
                               onchange="Portfolio.updateField('starting_capital', parseFloat(this.value))">
                    </div>
                    <div class="form-group" style="margin-bottom: 8px;">
                        <label class="form-label">Max Loss ($, optional)</label>
                        <input type="number" class="form-control" id="pf-maxloss" value="${p.max_loss || ''}" min="0"
                               onchange="Portfolio.updateField('max_loss', this.value ? parseFloat(this.value) : null)">
                    </div>
                    <div class="form-group" style="margin-bottom: 8px;">
                        <label class="form-label">Max Profit ($, optional)</label>
                        <input type="number" class="form-control" id="pf-maxprofit" value="${p.max_profit || ''}" min="0"
                               onchange="Portfolio.updateField('max_profit', this.value ? parseFloat(this.value) : null)">
                    </div>
                    <div style="display: flex; gap: 6px; margin-bottom: 8px;">
                        <div class="form-group" style="flex: 1; margin-bottom: 0;">
                            <label class="form-label">Start Date</label>
                            <input type="date" class="form-control" id="pf-start-date" value="${p.start_date || ''}"
                                   onchange="Portfolio.updateField('start_date', this.value || null)">
                        </div>
                        <div class="form-group" style="flex: 1; margin-bottom: 0;">
                            <label class="form-label">End Date</label>
                            <input type="date" class="form-control" id="pf-end-date" value="${p.end_date || ''}"
                                   onchange="Portfolio.updateField('end_date', this.value || null)">
                        </div>
                    </div>
                    <div style="display: flex; gap: 6px; margin-bottom: 8px;" title="Force-close all open slot positions every day at this local time. Leave blank to disable.">
                        <div class="form-group" style="flex: 1; margin-bottom: 0;">
                            <label class="form-label">Squareoff Time</label>
                            <input type="time" class="form-control" value="${p.squareoff_time || ''}"
                                   onchange="Portfolio.updateField('squareoff_time', this.value || null)">
                        </div>
                        <div class="form-group" style="flex: 1.3; margin-bottom: 0;">
                            <label class="form-label">Squareoff TZ</label>
                            ${this._renderTzSelect(p.squareoff_tz, "Portfolio.updateField('squareoff_tz', this.value || null)", "(UTC)")}
                        </div>
                    </div>
                    <div class="form-group" style="margin-bottom: 8px;">
                        <label class="form-label">Capital Allocation</label>
                        <select class="form-control" id="pf-allocation-mode"
                                onchange="Portfolio.updateField('allocation_mode', this.value)">
                            <option value="equal" ${(p.allocation_mode || 'equal') === 'equal' ? 'selected' : ''}>Divide Equally</option>
                            <option value="percentage" ${p.allocation_mode === 'percentage' ? 'selected' : ''}>Percentage per Slot</option>
                        </select>
                    </div>

                    <div style="font-weight: 500; font-size: 0.82rem; color: var(--text-secondary); margin-top: 4px;">Templates</div>
                    <div style="display: flex; flex-wrap: wrap; gap: 4px;">${tmplBtns}</div>

                    <div style="font-weight: 500; font-size: 0.82rem; color: var(--text-secondary); margin-top: 4px;">Saved</div>
                    ${savedSection}

                    <div style="margin-top: auto; display: flex; flex-direction: column; gap: 6px;">
                        <button class="btn btn-primary btn-block btn-sm" onclick="Portfolio.savePortfolio()">Save Portfolio</button>
                        <button class="btn btn-block btn-sm" onclick="Portfolio.exportJSON()">Export JSON</button>
                        <label class="btn btn-block btn-sm" style="text-align: center; cursor: pointer;">
                            Import JSON <input type="file" accept=".json" style="display: none;" onchange="Portfolio.importJSON(event)">
                        </label>
                    </div>
                </div>

                <!-- Right panel -->
                <div class="portfolio-main">
                    <div style="display: flex; align-items: center; justify-content: space-between;">
                        <div style="font-weight: 600; font-size: 0.95rem;">Strategy Slots (${p.slots.length})</div>
                        <button class="btn btn-primary btn-sm" onclick="Portfolio.addSlot()">+ Add Slot</button>
                    </div>

                    <div id="slots-container">${slotsHTML}</div>

                    <button class="btn btn-primary btn-block" onclick="Portfolio.runBacktest()" id="pf-run-btn">
                        Run Portfolio Backtest
                    </button>
                    <div id="pf-progress"></div>

                    <div id="pf-results">${resultsHTML}</div>
                </div>
            </div>
        `;

        // Apply allocation mode visibility after each full render.
        this._applyAllocationMode();
    },

    _renderSlot(slot, index) {
        const stratNames = Object.keys(this.strategies);
        const stratOpts = stratNames.map(n =>
            `<option value="${n}" ${n === slot.strategy_name ? "selected" : ""}>${n}</option>`
        ).join("");

        const barOpts = this.barTypes.map(bt =>
            `<option value="${bt}" ${bt === slot.bar_type_str ? "selected" : ""}>${App.barTypeLabel(bt)}</option>`
        ).join("");

        // Strategy params — tagged so updateSlotStrategy() can swap them in
        // place without rebuilding the whole slot card. Leg-indexed params
        // (keys like legN_*) also get data-leg-num so _applyLegVisibility()
        // can hide them when the slot's num_legs is reduced.
        const strat = this.strategies[slot.strategy_name] || {};
        const params = strat.params || {};
        const curNumLegs = parseInt(
            slot.strategy_params?.num_legs ?? params.num_legs?.default ?? 99, 10
        ) || 99;
        let paramsHTML = "";
        for (const [key, info] of Object.entries(params)) {
            const val = slot.strategy_params[key] !== undefined ? slot.strategy_params[key] : info.default;
            const legMatch = key.match(/^leg(\d+)_/);
            const legNum = legMatch ? parseInt(legMatch[1], 10) : null;
            const legAttr = legNum !== null ? ` data-leg-num="${legNum}"` : "";
            const hideStyle = legNum !== null && legNum > curNumLegs ? ' style="display: none"' : "";
            if (info.type === "time") {
                // HHMM int <-> "HH:MM" string. 930 -> "09:30".
                // Fields with inherit_zero treat 0 as "inherit" (empty picker);
                // non-inherit fields keep the legacy behaviour where 0 shows as 00:00.
                const vNum = parseInt(val ?? 0, 10) || 0;
                const inheritZero = info.inherit_zero === true;
                const hhmm = (inheritZero && vNum === 0)
                    ? ""
                    : `${String(Math.floor(vNum / 100)).padStart(2, "0")}:${String(vNum % 100).padStart(2, "0")}`;
                const placeholder = info.placeholder ? ` placeholder="${info.placeholder}"` : "";
                const onchange = inheritZero
                    ? `Portfolio.updateSlotParam(${index}, '${key}', this.value === '' ? 0 : parseInt(this.value.replace(':',''), 10))`
                    : `Portfolio.updateSlotParam(${index}, '${key}', parseInt(this.value.replace(':',''), 10))`;
                paramsHTML += `
                    <div class="form-group" data-strat-param="1"${legAttr}${hideStyle}>
                        <label class="form-label">${info.label}</label>
                        <input type="time" class="form-control" value="${hhmm}"${placeholder}
                               onchange="${onchange}">
                    </div>`;
            } else if (typeof info.default === "boolean") {
                paramsHTML += `
                    <div class="form-group" data-strat-param="1"${legAttr}${hideStyle}>
                        <label class="form-label">${info.label}</label>
                        <input type="checkbox" ${val ? "checked" : ""}
                               onchange="Portfolio.updateSlotParam(${index}, '${key}', this.checked)">
                    </div>`;
            } else {
                paramsHTML += `
                    <div class="form-group" data-strat-param="1"${legAttr}${hideStyle}>
                        <label class="form-label">${info.label}</label>
                        <input type="number" class="form-control" value="${val}"
                               ${info.min !== undefined ? `min="${info.min}"` : ""} ${info.max !== undefined ? `max="${info.max}"` : ""}
                               step="${typeof info.default === 'number' && !Number.isInteger(info.default) ? '0.5' : '1'}"
                               onchange="Portfolio.updateSlotParam(${index}, '${key}', ${typeof info.default === 'number' && !Number.isInteger(info.default) ? 'parseFloat(this.value)' : 'parseInt(this.value)'})">
                    </div>`;
            }
        }

        // Exit config
        const ec = slot.exit_config || {};
        const slTypes = ["none", "percentage", "points", "trailing"];
        const tpTypes = ["none", "percentage", "points"];
        const actions = ["close", "re_execute", "reverse"];

        const slTypeOpts = slTypes.map(t => `<option value="${t}" ${(ec.stop_loss_type || "none") === t ? "selected" : ""}>${t}</option>`).join("");
        const tpTypeOpts = tpTypes.map(t => `<option value="${t}" ${(ec.target_type || "none") === t ? "selected" : ""}>${t}</option>`).join("");
        const slActionOpts = actions.map(a => `<option value="${a}" ${(ec.on_sl_action || "close") === a ? "selected" : ""}>${a}</option>`).join("");
        const tpActionOpts = actions.map(a => `<option value="${a}" ${(ec.on_target_action || "close") === a ? "selected" : ""}>${a}</option>`).join("");

        return `
            <div class="slot-card ${slot.enabled === false ? 'disabled' : ''}" id="slot-${index}">
                <div class="slot-card-header">
                    <span class="slot-card-title">#${index + 1} ${slot.strategy_name} on ${App.barTypeLabel(slot.bar_type_str) || "N/A"}</span>
                    <div class="slot-card-actions">
                        <button class="btn btn-xs" onclick="Portfolio.toggleSlot(${index})" title="${slot.enabled !== false ? 'Disable' : 'Enable'}">
                            ${slot.enabled !== false ? "Disable" : "Enable"}
                        </button>
                        <button class="btn btn-xs btn-danger" onclick="Portfolio.removeSlot(${index})">Remove</button>
                    </div>
                </div>

                <div class="slot-params-row">
                    <div class="form-group">
                        <label class="form-label">Strategy</label>
                        <select class="form-control" onchange="Portfolio.updateSlotStrategy(${index}, this.value)">${stratOpts}</select>
                    </div>
                    <div class="form-group">
                        <label class="form-label">Instrument</label>
                        <select class="form-control" onchange="Portfolio.updateSlotField(${index}, 'bar_type_str', this.value)">${barOpts}</select>
                    </div>
                    <div class="form-group" style="max-width: 90px;">
                        <label class="form-label">Trade Size</label>
                        <input type="number" class="form-control" value="${slot.trade_size || 1}" min="1"
                               onchange="Portfolio.updateSlotField(${index}, 'trade_size', parseFloat(this.value))">
                    </div>
                    <div class="form-group alloc-pct-field" style="max-width: 90px;">
                        <label class="form-label">Alloc %</label>
                        <input type="number" class="form-control" value="${slot.allocation_pct || 0}" min="0" max="100" step="0.5"
                               onchange="Portfolio.updateSlotField(${index}, 'allocation_pct', parseFloat(this.value))">
                    </div>
                    ${paramsHTML}
                </div>

                <div class="exit-config-row">
                    <div class="form-group">
                        <label class="form-label">SL Type</label>
                        <select class="form-control" onchange="Portfolio.updateExitField(${index}, 'stop_loss_type', this.value)">${slTypeOpts}</select>
                    </div>
                    <div class="form-group" style="max-width: 80px;">
                        <label class="form-label">SL Value</label>
                        <input type="number" class="form-control" value="${ec.stop_loss_value || 0}" step="0.5" min="0"
                               onchange="Portfolio.updateExitField(${index}, 'stop_loss_value', parseFloat(this.value))">
                    </div>
                    <div class="form-group" style="max-width: 80px;">
                        <label class="form-label">Trail Step</label>
                        <input type="number" class="form-control" value="${ec.trailing_sl_step || 0}" step="0.5" min="0"
                               onchange="Portfolio.updateExitField(${index}, 'trailing_sl_step', parseFloat(this.value))">
                    </div>
                    <div class="form-group" style="max-width: 80px;">
                        <label class="form-label">Trail Offset</label>
                        <input type="number" class="form-control" value="${ec.trailing_sl_offset || 0}" step="0.5" min="0"
                               onchange="Portfolio.updateExitField(${index}, 'trailing_sl_offset', parseFloat(this.value))">
                    </div>
                    <div class="form-group">
                        <label class="form-label">TP Type</label>
                        <select class="form-control" onchange="Portfolio.updateExitField(${index}, 'target_type', this.value)">${tpTypeOpts}</select>
                    </div>
                    <div class="form-group" style="max-width: 80px;">
                        <label class="form-label">TP Value</label>
                        <input type="number" class="form-control" value="${ec.target_value || 0}" step="0.5" min="0"
                               onchange="Portfolio.updateExitField(${index}, 'target_value', parseFloat(this.value))">
                    </div>
                    <div class="form-group" style="max-width: 80px;">
                        <label class="form-label">SL Wait</label>
                        <input type="number" class="form-control" value="${ec.sl_wait_bars || 0}" step="1" min="0"
                               onchange="Portfolio.updateExitField(${index}, 'sl_wait_bars', parseInt(this.value))">
                    </div>
                    <div class="form-group">
                        <label class="form-label">On SL</label>
                        <select class="form-control" onchange="Portfolio.updateExitField(${index}, 'on_sl_action', this.value)">${slActionOpts}</select>
                    </div>
                    <div class="form-group">
                        <label class="form-label">On TP</label>
                        <select class="form-control" onchange="Portfolio.updateExitField(${index}, 'on_target_action', this.value)">${tpActionOpts}</select>
                    </div>
                    <div class="form-group" style="max-width: 70px;">
                        <label class="form-label">Max Re-ex</label>
                        <input type="number" class="form-control" value="${ec.max_re_executions || 0}" step="1" min="0"
                               onchange="Portfolio.updateExitField(${index}, 'max_re_executions', parseInt(this.value))">
                    </div>
                    <div class="form-group" style="max-width: 100px;" title="Slot-level squareoff override. Blank inherits from portfolio.">
                        <label class="form-label">Slot SqOff</label>
                        <input type="time" class="form-control" value="${slot.squareoff_time || ''}"
                               onchange="Portfolio.updateSlotField(${index}, 'squareoff_time', this.value || null)">
                    </div>
                    <div class="form-group" style="max-width: 130px;">
                        <label class="form-label">Slot SqOff TZ</label>
                        ${this._renderTzSelect(slot.squareoff_tz, `Portfolio.updateSlotField(${index}, 'squareoff_tz', this.value || null)`, "(inherit)")}
                    </div>
                    <div class="form-group" style="max-width: 100px;" title="Leg-level squareoff override. Highest priority — beats slot and portfolio.">
                        <label class="form-label">Leg SqOff</label>
                        <input type="time" class="form-control" value="${ec.squareoff_time || ''}"
                               onchange="Portfolio.updateExitField(${index}, 'squareoff_time', this.value || null)">
                    </div>
                    <div class="form-group" style="max-width: 130px;">
                        <label class="form-label">Leg SqOff TZ</label>
                        ${this._renderTzSelect(ec.squareoff_tz, `Portfolio.updateExitField(${index}, 'squareoff_tz', this.value || null)`, "(inherit)")}
                    </div>
                </div>
            </div>
        `;
    },

    updateField(field, value) {
        this.portfolio[field] = value;
        // Allocation mode is the only top-level field whose UI visibility
        // changes with its value. Toggle a body-level class so the CSS rule
        // below can show/hide the per-slot "Alloc %" inputs without a
        // full re-render (the main reason typing in this page felt laggy).
        if (field === "allocation_mode") {
            this._applyAllocationMode();
        }
    },

    /** Show/hide the per-slot "Alloc %" inputs based on allocation_mode.
     *  Called from updateField and after renderApp. */
    _applyAllocationMode() {
        const app = document.getElementById("portfolio-app");
        if (!app) return;
        app.classList.toggle("alloc-percentage", this.portfolio.allocation_mode === "percentage");
    },

    /** Targeted update for a slot field — avoids a full portfolio re-render.
     *  Previously we called renderApp() on every keystroke which destroyed
     *  focus, rebuilt 200-option <select>s for every slot, and felt jittery. */
    updateSlotField(index, field, value) {
        this.portfolio.slots[index][field] = value;
        // Only the title bar depends on bar_type_str; update just that.
        if (field === "bar_type_str") {
            this._updateSlotTitle(index);
        }
    },

    updateSlotParam(index, key, value) {
        if (!this.portfolio.slots[index].strategy_params) {
            this.portfolio.slots[index].strategy_params = {};
        }
        this.portfolio.slots[index].strategy_params[key] = value;
        if (key === "num_legs") {
            this._applyLegVisibility(index);
        }
    },

    /** Re-render only the params inputs for one slot when strategy changes. */
    updateSlotStrategy(index, stratName) {
        const slot = this.portfolio.slots[index];
        slot.strategy_name = stratName;
        const strat = this.strategies[stratName];
        slot.strategy_params = {};
        if (strat && strat.params) {
            for (const [key, info] of Object.entries(strat.params)) {
                slot.strategy_params[key] = info.default;
            }
        }
        this._updateSlotTitle(index);
        this._rerenderSlotParams(index);
    },

    updateExitField(index, field, value) {
        if (!this.portfolio.slots[index].exit_config) {
            this.portfolio.slots[index].exit_config = {};
        }
        this.portfolio.slots[index].exit_config[field] = value;
    },

    /** Update just the title line of a slot card (strategy + instrument label). */
    _updateSlotTitle(index) {
        const card = document.getElementById(`slot-${index}`);
        if (!card) return;
        const titleEl = card.querySelector(".slot-card-title");
        if (!titleEl) return;
        const slot = this.portfolio.slots[index];
        const inst = App.barTypeLabel(slot.bar_type_str) || "N/A";
        titleEl.textContent = `#${index + 1} ${slot.strategy_name} on ${inst}`;
    },

    /** Re-render just the strategy-param form-groups inside one slot —
     *  appended after the fixed fields (strategy, instrument, trade size, alloc). */
    _rerenderSlotParams(index) {
        const card = document.getElementById(`slot-${index}`);
        if (!card) return;
        const row = card.querySelector(".slot-params-row");
        if (!row) return;
        const slot = this.portfolio.slots[index];
        const strat = this.strategies[slot.strategy_name] || {};
        const params = strat.params || {};
        const curNumLegs = parseInt(
            slot.strategy_params?.num_legs ?? params.num_legs?.default ?? 99, 10
        ) || 99;
        // Remove existing dynamic params (everything tagged as strat-param)
        row.querySelectorAll('[data-strat-param="1"]').forEach(n => n.remove());
        // Re-append fresh
        const tmp = document.createElement("div");
        let html = "";
        for (const [key, info] of Object.entries(params)) {
            const val = slot.strategy_params[key] !== undefined ? slot.strategy_params[key] : info.default;
            const legMatch = key.match(/^leg(\d+)_/);
            const legNum = legMatch ? parseInt(legMatch[1], 10) : null;
            const legAttr = legNum !== null ? ` data-leg-num="${legNum}"` : "";
            const hideStyle = legNum !== null && legNum > curNumLegs ? ' style="display: none"' : "";
            if (info.type === "time") {
                const vNum = parseInt(val ?? 0, 10) || 0;
                const inheritZero = info.inherit_zero === true;
                const hhmm = (inheritZero && vNum === 0)
                    ? ""
                    : `${String(Math.floor(vNum / 100)).padStart(2, "0")}:${String(vNum % 100).padStart(2, "0")}`;
                const placeholder = info.placeholder ? ` placeholder="${info.placeholder}"` : "";
                const onchange = inheritZero
                    ? `Portfolio.updateSlotParam(${index}, '${key}', this.value === '' ? 0 : parseInt(this.value.replace(':',''), 10))`
                    : `Portfolio.updateSlotParam(${index}, '${key}', parseInt(this.value.replace(':',''), 10))`;
                html += `
                    <div class="form-group" data-strat-param="1"${legAttr}${hideStyle}>
                        <label class="form-label">${info.label}</label>
                        <input type="time" class="form-control" value="${hhmm}"${placeholder}
                               onchange="${onchange}">
                    </div>`;
            } else if (typeof info.default === "boolean") {
                html += `
                    <div class="form-group" data-strat-param="1"${legAttr}${hideStyle}>
                        <label class="form-label">${info.label}</label>
                        <input type="checkbox" ${val ? "checked" : ""}
                               onchange="Portfolio.updateSlotParam(${index}, '${key}', this.checked)">
                    </div>`;
            } else {
                const parser = typeof info.default === "number" && !Number.isInteger(info.default)
                    ? "parseFloat(this.value)" : "parseInt(this.value)";
                html += `
                    <div class="form-group" data-strat-param="1"${legAttr}${hideStyle}>
                        <label class="form-label">${info.label}</label>
                        <input type="number" class="form-control" value="${val}"
                               ${info.min !== undefined ? `min="${info.min}"` : ""} ${info.max !== undefined ? `max="${info.max}"` : ""}
                               step="${typeof info.default === 'number' && !Number.isInteger(info.default) ? '0.5' : '1'}"
                               onchange="Portfolio.updateSlotParam(${index}, '${key}', ${parser})">
                    </div>`;
            }
        }
        tmp.innerHTML = html;
        while (tmp.firstChild) row.appendChild(tmp.firstChild);
    },

    /** Hide form-groups whose data-leg-num exceeds the slot's current num_legs.
     *  Called from updateSlotParam when num_legs changes, so the leg inputs
     *  collapse without rebuilding the slot card. */
    _applyLegVisibility(index) {
        const card = document.getElementById(`slot-${index}`);
        if (!card) return;
        const slot = this.portfolio.slots[index];
        const strat = this.strategies[slot.strategy_name] || {};
        const fallback = strat.params?.num_legs?.default ?? 99;
        const num = parseInt(slot.strategy_params?.num_legs ?? fallback, 10) || fallback;
        card.querySelectorAll("[data-leg-num]").forEach(el => {
            const n = parseInt(el.getAttribute("data-leg-num"), 10);
            el.style.display = n > num ? "none" : "";
        });
    },

    addSlot() {
        const firstStrat = Object.keys(this.strategies)[0] || "EMA Cross";
        const strat = this.strategies[firstStrat] || {};
        const defaultParams = {};
        if (strat.params) {
            for (const [key, info] of Object.entries(strat.params)) {
                defaultParams[key] = info.default;
            }
        }

        this.slotCounter++;
        this.portfolio.slots.push({
            slot_id: "s" + Date.now().toString(36) + this.slotCounter,
            strategy_name: firstStrat,
            strategy_params: defaultParams,
            bar_type_str: this.barTypes[0] || "",
            trade_size: 1,
            allocation_pct: 0,
            exit_config: {
                stop_loss_type: "none", stop_loss_value: 0,
                trailing_sl_step: 0, trailing_sl_offset: 0,
                target_type: "none", target_value: 0,
                target_lock_trigger: null, target_lock_minimum: null,
                sl_wait_bars: 0, on_sl_action: "close", on_target_action: "close",
                max_re_executions: 0,
                squareoff_time: null, squareoff_tz: null,
            },
            enabled: true,
            squareoff_time: null,
            squareoff_tz: null,
        });
        this.renderApp();
    },

    removeSlot(index) {
        this.portfolio.slots.splice(index, 1);
        this.renderApp();
    },

    toggleSlot(index) {
        const slot = this.portfolio.slots[index];
        slot.enabled = !slot.enabled;
        // Just toggle the dimmed class + update the button label in place —
        // no need to blow away the whole page for a visual nit.
        const card = document.getElementById(`slot-${index}`);
        if (card) {
            card.classList.toggle("disabled", slot.enabled === false);
            const btn = card.querySelector(".slot-card-actions .btn-xs:not(.btn-danger)");
            if (btn) {
                btn.textContent = slot.enabled !== false ? "Disable" : "Enable";
                btn.title = slot.enabled !== false ? "Disable" : "Enable";
            }
        }
    },

    async savePortfolio() {
        const name = document.getElementById("pf-name").value.trim();
        if (!name) { App.toast("Enter a portfolio name.", "error"); return; }
        this.portfolio.name = name;
        try {
            const data = await App.api("/api/portfolios/save", {
                method: "POST",
                body: JSON.stringify(this.portfolio),
            });
            App.toast(data.message, "success");
        } catch (e) {
            App.toast("Save failed: " + e.message, "error");
        }
    },

    async loadSavedList() {
        const listDiv = document.getElementById("saved-list");
        try {
            const data = await App.api("/api/portfolios/list");
            if (data.portfolios.length === 0) {
                listDiv.innerHTML = '<span style="font-size: 0.8rem; color: var(--text-muted);">No saved portfolios.</span>';
                return;
            }
            listDiv.innerHTML = data.portfolios.map(name => `
                <div style="display: flex; justify-content: space-between; align-items: center; padding: 4px 0; border-bottom: 1px solid var(--border-color);">
                    <span style="font-size: 0.8rem; cursor: pointer; color: var(--accent);" onclick="Portfolio.loadSaved('${name}')">${name}</span>
                    <button class="btn btn-xs btn-danger" onclick="Portfolio.deleteSaved('${name}')">Del</button>
                </div>
            `).join("");
        } catch (e) {
            listDiv.innerHTML = `<span style="font-size: 0.8rem; color: var(--danger);">${e.message}</span>`;
        }
    },

    async loadSaved(name) {
        try {
            const data = await App.api(`/api/portfolios/load?name=${encodeURIComponent(name)}`);
            this.portfolio = data.portfolio;
            this._liftUniformSlotDates(this.portfolio);
            this.results = null;
            this.renderApp();
            App.toast(`Loaded "${name}"`, "success");
        } catch (e) {
            App.toast("Load failed: " + e.message, "error");
        }
    },

    async deleteSaved(name) {
        if (!confirm(`Delete portfolio "${name}"?`)) return;
        try {
            await App.api("/api/portfolios/delete", {
                method: "POST", body: JSON.stringify({ name }),
            });
            App.toast(`Deleted "${name}"`, "success");
            this.loadSavedList();
        } catch (e) {
            App.toast("Delete failed: " + e.message, "error");
        }
    },

    async loadTemplate(templateName) {
        try {
            const data = await App.api("/api/portfolios/from-template", {
                method: "POST",
                body: JSON.stringify({ template: templateName, bar_types: this.barTypes }),
            });
            this.portfolio = data.portfolio;
            this._liftUniformSlotDates(this.portfolio);
            this.results = null;
            this.renderApp();
            App.toast(`Template "${templateName}" loaded.`, "success");
        } catch (e) {
            App.toast("Template load failed: " + e.message, "error");
        }
    },

    exportJSON() {
        const name = this.portfolio.name || "portfolio";
        const json = JSON.stringify(this.portfolio, null, 2);
        const blob = new Blob([json], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        a.href = url; a.download = `${name}.json`;
        document.body.appendChild(a); a.click();
        document.body.removeChild(a); URL.revokeObjectURL(url);
    },

    importJSON(event) {
        const file = event.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = (e) => {
            try {
                this.portfolio = JSON.parse(e.target.result);
                this._liftUniformSlotDates(this.portfolio);
                this.results = null;
                this.renderApp();
                App.toast("Portfolio imported.", "success");
            } catch (err) {
                App.toast("Invalid JSON: " + err.message, "error");
            }
        };
        reader.readAsText(file);
    },

    async runBacktest() {
        const enabledSlots = this.portfolio.slots.filter(s => s.enabled !== false);
        if (enabledSlots.length === 0) {
            App.toast("Add at least one enabled slot.", "error");
            return;
        }

        // Sync name/capital/allocation from inputs
        this.portfolio.name = document.getElementById("pf-name").value.trim() || "Unnamed";
        this.portfolio.starting_capital = parseFloat(document.getElementById("pf-capital").value) || 100000;
        this.portfolio.allocation_mode = this.portfolio.allocation_mode || "equal";

        // Validate percentage allocation
        if (this.portfolio.allocation_mode === "percentage") {
            const totalPct = enabledSlots.reduce((sum, s) => sum + (s.allocation_pct || 0), 0);
            if (totalPct <= 0 || totalPct > 100) {
                App.toast(`Allocation percentages must sum to 1-100%. Current: ${totalPct.toFixed(1)}%`, "error");
                return;
            }
        }

        const btn = document.getElementById("pf-run-btn");
        btn.disabled = true;
        btn.textContent = "Running...";

        const progressDiv = document.getElementById("pf-progress");
        progressDiv.innerHTML = `
            <div class="card" style="padding: 16px;">
                <div class="progress-text" id="pf-progress-text">Initializing portfolio backtest...</div>
                <div class="progress-bar-container">
                    <div class="progress-bar-fill" id="pf-progress-bar" style="width: 0%; transition: width 0.3s ease;"></div>
                </div>
                <div id="pf-progress-details" style="margin-top: 8px; font-size: 0.85rem; color: var(--text-secondary);"></div>
                <div id="pf-progress-slots" style="margin-top: 12px;"></div>
            </div>
        `;

        try {
            const response = await fetch("/api/portfolios/backtest", {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify({ portfolio: this.portfolio }),
            });

            const reader = response.body.getReader();
            const decoder = new TextDecoder();
            let buffer = "";
            let slotInfo = [];

            while (true) {
                const { done, value } = await reader.read();
                if (done) break;

                buffer += decoder.decode(value, { stream: true });
                const lines = buffer.split("\n");
                buffer = lines.pop(); // keep incomplete line in buffer

                for (const line of lines) {
                    if (!line.trim()) continue;
                    let evt;
                    try { evt = JSON.parse(line); } catch { continue; }

                    const bar = document.getElementById("pf-progress-bar");
                    const text = document.getElementById("pf-progress-text");
                    const details = document.getElementById("pf-progress-details");
                    const slotsDiv = document.getElementById("pf-progress-slots");

                    if (evt.event === "start") {
                        slotInfo = evt.slots || [];
                        // Render slot checklist
                        if (slotsDiv && slotInfo.length > 0) {
                            slotsDiv.innerHTML = slotInfo.map(s =>
                                `<div id="pf-slot-${s.slot_id}" style="padding: 4px 0; font-size: 0.85rem; color: var(--text-secondary);">
                                    <span class="slot-icon" style="margin-right: 6px;">&#9723;</span>${s.display_name}
                                </div>`
                            ).join("");
                        }

                    } else if (evt.event === "progress") {
                        const pct = evt.total > 0 ? Math.round((evt.completed / evt.total) * 100) : 0;
                        if (bar) bar.style.width = pct + "%";
                        if (text) text.textContent = evt.message || "Processing...";

                        if (evt.phase === "engine") {
                            // Mark all as running initially
                            for (const s of slotInfo) {
                                const el = document.getElementById(`pf-slot-${s.slot_id}`);
                                if (el && !el.dataset.done) {
                                    const icon = el.querySelector(".slot-icon");
                                    if (icon) icon.innerHTML = "&#9881;";
                                    el.style.color = "var(--accent)";
                                }
                            }
                            // Mark completed slot with green checkmark
                            if (evt.completed_slot_id) {
                                const el = document.getElementById(`pf-slot-${evt.completed_slot_id}`);
                                if (el) {
                                    const icon = el.querySelector(".slot-icon");
                                    if (icon) icon.innerHTML = "&#9989;";
                                    el.style.color = "var(--text-primary)";
                                    el.dataset.done = "1";
                                }
                            }
                            if (details) details.textContent = `${evt.slots_completed || 0}/${slotInfo.length} strategies completed`;
                        } else if (evt.phase === "reports") {
                            for (const s of slotInfo) {
                                const el = document.getElementById(`pf-slot-${s.slot_id}`);
                                if (el) {
                                    const icon = el.querySelector(".slot-icon");
                                    if (icon) icon.innerHTML = "&#9989;";
                                    el.style.color = "var(--text-primary)";
                                }
                            }
                            if (details) details.textContent = evt.message;
                        }

                    } else if (evt.event === "complete") {
                        if (bar) bar.style.width = "100%";
                        if (text) text.textContent = `Backtest complete in ${evt.elapsed?.toFixed(1) || "?"}s`;
                        if (details) details.textContent = "";

                        // Mark all slots done
                        for (const s of slotInfo) {
                            const el = document.getElementById(`pf-slot-${s.slot_id}`);
                            if (el) {
                                const icon = el.querySelector(".slot-icon");
                                if (icon) icon.innerHTML = "&#9989;";
                                el.style.color = "var(--text-primary)";
                            }
                        }

                        const results = evt.results;
                        if (results && !results.error) {
                            this.results = results;
                            App.state.portfolioResults = results;
                            const resultsDiv = document.getElementById("pf-results");
                            resultsDiv.innerHTML = this._renderResults();
                            App.toast("Portfolio backtest finished!", "success");
                        } else {
                            throw new Error(results?.error || "Unknown error");
                        }

                    } else if (evt.event === "error") {
                        throw new Error(evt.error || "Backtest failed");
                    }
                }
            }
        } catch (e) {
            const progressDiv = document.getElementById("pf-progress");
            progressDiv.innerHTML = `<div class="alert alert-danger">Backtest failed: ${e.message}</div>`;
        } finally {
            btn.disabled = false;
            btn.textContent = "Run Portfolio Backtest";
        }
    },

    _renderResults() {
        const r = this.results;
        if (!r) return "";

        const pnlClass = r.total_pnl >= 0 ? "positive" : "negative";
        let flagsHTML = "";
        if (r.max_loss_hit) flagsHTML += '<span class="badge badge-danger">Max Loss Hit</span> ';
        if (r.max_profit_hit) flagsHTML += '<span class="badge badge-success">Max Profit Hit</span> ';

        // Per-strategy table
        let perStratRows = "";
        if (r.per_strategy) {
            for (const [sid, sr] of Object.entries(r.per_strategy)) {
                const cls = sr.pnl >= 0 ? "positive" : "negative";
                perStratRows += `<tr>
                    <td>${sr.display_name}</td>
                    <td class="${cls}">${App.currency(sr.pnl)}</td>
                    <td>${sr.trades}</td>
                    <td>${sr.win_rate.toFixed(1)}%</td>
                    <td>${sr.wins}</td>
                    <td>${sr.losses}</td>
                </tr>`;
            }
        }

        return `
            <div class="portfolio-results">
                <div style="display: flex; align-items: center; justify-content: space-between; margin-bottom: 12px;">
                    <span style="font-weight: 600; font-size: 0.95rem;">Results: ${r.portfolio_name || ""}</span>
                    <div>${flagsHTML}
                        <button class="btn btn-sm btn-primary" onclick="App.navigate('portfolio_tearsheet')">Full Tearsheet</button>
                    </div>
                </div>

                <div class="grid-6">
                    ${App.metricHTML("Starting Capital", App.currency(r.starting_capital))}
                    ${App.metricHTML("Final Balance", App.currency(r.final_balance))}
                    ${App.metricHTML("Total P&L", App.currency(r.total_pnl), r.total_return_pct)}
                    ${App.metricHTML("Total Trades", r.total_trades)}
                    ${App.metricHTML("Win Rate", r.win_rate.toFixed(1) + "%")}
                    ${App.metricHTML("Max Drawdown", r.max_drawdown.toFixed(2) + "%")}
                </div>

                ${perStratRows ? `
                <div class="table-container" style="margin-top: 12px;">
                    <table>
                        <thead><tr><th>Strategy</th><th>P&L</th><th>Trades</th><th>Win Rate</th><th>Wins</th><th>Losses</th></tr></thead>
                        <tbody>${perStratRows}</tbody>
                    </table>
                </div>` : ""}
            </div>
        `;
    },
};
