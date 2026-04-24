/**
 * Data Formats Page - Configure CSV format and instrument settings per asset class.
 */

const DataFormats = {
    formats: {},
    editingAsset: null,

    async render(container) {
        try {
            const data = await App.api("/api/data-formats");
            this.formats = data.formats || {};
        } catch (e) {
            container.innerHTML = `<div class="alert alert-danger">Failed to load data formats: ${e.message}</div>`;
            return;
        }

        if (this.editingAsset) {
            this.renderEditForm(container);
        } else {
            this.renderTable(container);
        }
    },

    renderTable(container) {
        let rows = "";
        for (const [key, config] of Object.entries(this.formats)) {
            const csv = config.csv || {};
            const inst = config.instrument || {};
            const isConfigured = csv.filename_pattern !== null && csv.required_columns !== null;

            const statusBadge = isConfigured
                ? '<span class="badge badge-success">Configured</span>'
                : '<span class="badge badge-warning">Not configured</span>';

            const pattern = csv.filename_description || csv.filename_pattern || "—";
            const columns = csv.required_columns ? csv.required_columns.join(", ") : "—";
            const instType = inst.type || "—";
            const quoteCur = inst.quote_currency || "—";

            rows += `
                <tr>
                    <td style="font-weight:600;">${config.label || key}</td>
                    <td>${statusBadge}</td>
                    <td style="max-width:200px; overflow:hidden; text-overflow:ellipsis;">${pattern}</td>
                    <td style="max-width:250px; overflow:hidden; text-overflow:ellipsis;">${columns}</td>
                    <td>${instType}</td>
                    <td>${quoteCur}</td>
                    <td>
                        <div style="display:flex; gap:6px;">
                            <button class="btn btn-sm btn-primary" onclick="DataFormats.edit('${key}')">Edit</button>
                            ${isConfigured ? `<button class="btn btn-sm btn-danger" onclick="DataFormats.reset('${key}', '${config.label || key}')">Reset</button>` : ''}
                        </div>
                    </td>
                </tr>`;
        }

        container.innerHTML = `
            <h1 class="page-title">&#128196; Data Formats</h1>
            <p class="page-subtitle">Configure CSV file format and instrument settings for each asset class.</p>

            <div class="table-container" style="max-height:none;">
                <table>
                    <thead>
                        <tr>
                            <th>Asset Class</th>
                            <th>Status</th>
                            <th>Filename Pattern</th>
                            <th>Required Columns</th>
                            <th>Instrument Type</th>
                            <th>Quote Currency</th>
                            <th>Actions</th>
                        </tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;
    },

    edit(assetClass) {
        this.editingAsset = assetClass;
        this.render(document.getElementById("main-content"));
    },

    renderEditForm(container) {
        const key = this.editingAsset;
        const config = this.formats[key] || {};
        const csv = config.csv || {};
        const inst = config.instrument || {};
        const trading = config.trading || {};

        const v = (val) => val !== null && val !== undefined ? val : "";
        const colsStr = csv.required_columns ? csv.required_columns.join(", ") : "";

        container.innerHTML = `
            <h1 class="page-title">&#9999; ${config.label || key}</h1>
            <p class="page-subtitle">Configure data format for ${config.label || key} asset class.</p>

            <button class="btn" onclick="DataFormats.backToList()" style="margin-bottom:16px;">&larr; Back to list</button>

            <h2 class="section-title">CSV File Format</h2>
            <div class="grid-2">
                <div class="form-group">
                    <label class="form-label">Filename Pattern (Regex)</label>
                    <input type="text" id="df-filename-pattern" class="form-control"
                           value="${v(csv.filename_pattern)}" placeholder="e.g. ^\\d+_[A-Z]+_.+\\.csv$">
                </div>
                <div class="form-group">
                    <label class="form-label">Filename Description</label>
                    <input type="text" id="df-filename-desc" class="form-control"
                           value="${v(csv.filename_description)}" placeholder="e.g. id_SYMBOL_Name.csv">
                </div>
                <div class="form-group">
                    <label class="form-label">Required Columns (comma-separated)</label>
                    <input type="text" id="df-required-columns" class="form-control"
                           value="${colsStr}" placeholder="e.g. ts, open, high, low, close, volume">
                </div>
                <div class="form-group">
                    <label class="form-label">Timestamp Column</label>
                    <input type="text" id="df-ts-column" class="form-control"
                           value="${v(csv.timestamp_column)}" placeholder="e.g. ts or date">
                </div>
                <div class="form-group">
                    <label class="form-label">Timestamp Format</label>
                    <input type="text" id="df-ts-format" class="form-control"
                           value="${v(csv.timestamp_format)}" placeholder="auto or %Y-%m-%d %H:%M:%S">
                </div>
                <div class="form-group">
                    <label class="form-label">Delimiter</label>
                    <input type="text" id="df-delimiter" class="form-control"
                           value="${v(csv.delimiter) || ","}" placeholder=",">
                </div>
            </div>

            <h2 class="section-title" style="margin-top:24px;">Instrument Settings</h2>
            <div class="grid-2">
                <div class="form-group">
                    <label class="form-label">Instrument Type</label>
                    <select id="df-inst-type" class="form-control">
                        <option value="">— Not set —</option>
                        <option value="CurrencyPair" ${inst.type === "CurrencyPair" ? "selected" : ""}>CurrencyPair</option>
                        <option value="Equity" ${inst.type === "Equity" ? "selected" : ""}>Equity</option>
                        <option value="Future" ${inst.type === "Future" ? "selected" : ""}>Future</option>
                        <option value="Option" ${inst.type === "Option" ? "selected" : ""}>Option</option>
                        <option value="CFD" ${inst.type === "CFD" ? "selected" : ""}>CFD</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Quote Currency</label>
                    <input type="text" id="df-quote-currency" class="form-control"
                           value="${v(inst.quote_currency)}" placeholder="e.g. USD, INR, EUR">
                </div>
                <div class="form-group">
                    <label class="form-label">Price Precision (decimals)</label>
                    <input type="number" id="df-price-precision" class="form-control"
                           value="${v(inst.price_precision)}" placeholder="e.g. 2">
                </div>
                <div class="form-group">
                    <label class="form-label">Size Precision (decimals)</label>
                    <input type="number" id="df-size-precision" class="form-control"
                           value="${v(inst.size_precision)}" placeholder="e.g. 0 or 8">
                </div>
                <div class="form-group">
                    <label class="form-label">Currency Type</label>
                    <select id="df-currency-type" class="form-control">
                        <option value="">— Not set —</option>
                        <option value="0" ${inst.currency_type === 0 ? "selected" : ""}>FIAT (0)</option>
                        <option value="1" ${inst.currency_type === 1 ? "selected" : ""}>METAL (1)</option>
                        <option value="2" ${inst.currency_type === 2 ? "selected" : ""}>CRYPTO (2)</option>
                    </select>
                </div>
                <div class="form-group">
                    <label class="form-label">Timeframe</label>
                    <select id="df-timeframe" class="form-control">
                        <option value="">— Not set —</option>
                        <option value="1-MINUTE" ${inst.timeframe === "1-MINUTE" ? "selected" : ""}>1-MINUTE</option>
                        <option value="5-MINUTE" ${inst.timeframe === "5-MINUTE" ? "selected" : ""}>5-MINUTE</option>
                        <option value="15-MINUTE" ${inst.timeframe === "15-MINUTE" ? "selected" : ""}>15-MINUTE</option>
                        <option value="1-HOUR" ${inst.timeframe === "1-HOUR" ? "selected" : ""}>1-HOUR</option>
                        <option value="4-HOUR" ${inst.timeframe === "4-HOUR" ? "selected" : ""}>4-HOUR</option>
                        <option value="1-DAY" ${inst.timeframe === "1-DAY" ? "selected" : ""}>1-DAY</option>
                        <option value="1-WEEK" ${inst.timeframe === "1-WEEK" ? "selected" : ""}>1-WEEK</option>
                    </select>
                </div>
            </div>

            <h2 class="section-title" style="margin-top:24px;">Trading Settings</h2>
            <div class="grid-2">
                <div class="form-group">
                    <label class="form-label">Maker Fee (e.g. 0.001 = 0.1%)</label>
                    <input type="text" id="df-maker-fee" class="form-control"
                           value="${v(trading.maker_fee)}" placeholder="e.g. 0.001">
                </div>
                <div class="form-group">
                    <label class="form-label">Taker Fee</label>
                    <input type="text" id="df-taker-fee" class="form-control"
                           value="${v(trading.taker_fee)}" placeholder="e.g. 0.001">
                </div>
                <div class="form-group">
                    <label class="form-label">Margin Init (e.g. 1.0 = 100%)</label>
                    <input type="text" id="df-margin-init" class="form-control"
                           value="${v(trading.margin_init)}" placeholder="e.g. 1.0">
                </div>
                <div class="form-group">
                    <label class="form-label">Margin Maintenance</label>
                    <input type="text" id="df-margin-maint" class="form-control"
                           value="${v(trading.margin_maint)}" placeholder="e.g. 0.35">
                </div>
            </div>

            <div class="page-divider"></div>
            <div style="display:flex; gap:12px;">
                <button class="btn btn-primary" onclick="DataFormats.save()">Save</button>
                <button class="btn" onclick="DataFormats.backToList()">Cancel</button>
            </div>
        `;
    },

    backToList() {
        this.editingAsset = null;
        this.render(document.getElementById("main-content"));
    },

    async save() {
        const key = this.editingAsset;
        const getVal = (id) => {
            const el = document.getElementById(id);
            if (!el) return null;
            const v = el.value.trim();
            return v === "" ? null : v;
        };
        const getNum = (id) => {
            const v = getVal(id);
            return v !== null ? parseFloat(v) : null;
        };

        // Parse required columns
        const colsRaw = getVal("df-required-columns");
        const cols = colsRaw ? colsRaw.split(",").map(c => c.trim()).filter(c => c) : null;

        const payload = {
            csv: {
                filename_pattern: getVal("df-filename-pattern"),
                filename_description: getVal("df-filename-desc"),
                required_columns: cols,
                timestamp_column: getVal("df-ts-column"),
                timestamp_format: getVal("df-ts-format"),
                delimiter: getVal("df-delimiter") || ",",
            },
            instrument: {
                type: getVal("df-inst-type"),
                quote_currency: getVal("df-quote-currency"),
                price_precision: getNum("df-price-precision"),
                size_precision: getNum("df-size-precision"),
                currency_type: getNum("df-currency-type"),
                timeframe: getVal("df-timeframe"),
            },
            trading: {
                maker_fee: getVal("df-maker-fee"),
                taker_fee: getVal("df-taker-fee"),
                margin_init: getVal("df-margin-init"),
                margin_maint: getVal("df-margin-maint"),
            },
        };

        try {
            await App.api(`/api/data-formats/${key}`, {
                method: "PUT",
                body: JSON.stringify(payload),
            });
            App.toast(`Data format for "${this.formats[key]?.label || key}" saved!`, "success");
            this.editingAsset = null;
            this.render(document.getElementById("main-content"));
        } catch (e) {
            App.toast(`Save failed: ${e.message}`, "error");
        }
    },

    async reset(assetClass, label) {
        if (!confirm(`Reset data format for "${label}"? All settings will be cleared.`)) return;

        try {
            await App.api(`/api/data-formats/${assetClass}/reset`, { method: "POST" });
            App.toast(`Data format for "${label}" reset`, "success");
            this.render(document.getElementById("main-content"));
        } catch (e) {
            App.toast(`Reset failed: ${e.message}`, "error");
        }
    },
};
