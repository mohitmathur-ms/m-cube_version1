/**
 * Users page — admin-only registry editor.
 *
 * Reads/writes the full users registry (user_id, alias, multiplier,
 * allowed_instruments) via /api/users. Identity-only: there's no auth/
 * security on the user_id header, so the page banner makes that explicit.
 *
 * Mirrors the editable-table pattern used by data_formats.js — render the
 * full table, edit cells inline, hit "Save All" to POST the whole registry
 * back. One round-trip; server replaces the file atomically after validation.
 */

const Users = {
    users: [],
    meta: null,

    async render(container) {
        try {
            const data = await App.api("/api/users");
            this.users = Array.isArray(data.users) ? data.users : [];
            this.meta = data._meta || null;
        } catch (e) {
            container.innerHTML = `<div class="alert alert-danger">Failed to load users: ${e.message}</div>`;
            return;
        }
        this._renderTable(container);
    },

    _renderTable(container) {
        const banner = `
            <div class="alert alert-warning" style="margin-bottom:16px;">
                <strong>Identity-only — no security.</strong> The
                <code>X-User-Id</code> header on every API call must match a
                <code>user_id</code> below, but anyone on the network can spoof
                any header. Use this only in trusted environments.
            </div>`;

        const rows = this.users.map((u, i) => this._rowHTML(u, i)).join("");

        container.innerHTML = `
            <h2 class="page-title">User Settings</h2>
            ${banner}
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th style="width:42px;">Delete</th>
                            <th style="width:150px;">User ID</th>
                            <th>Alias</th>
                            <th style="width:100px;">Multiplier</th>
                            <th style="width:110px;" title="Spec §3 Level 3: cumulative loss cap across all this user's portfolios. Blank = no cap.">Max Loss</th>
                            <th style="width:110px;" title="Spec §3 Level 3: cumulative profit cap across all this user's portfolios. Blank = no cap.">Max Profit</th>
                            <th style="width:64px;" title="Spec §6.1: enable a user-level trailing SL that ratchets the Max Loss cap tighter as combined profit grows.">Trail SL</th>
                            <th style="width:96px;" title="Spec §6.1: for every this much combined profit gained, tighten the Max Loss cap.">Trail Every</th>
                            <th style="width:96px;" title="Spec §6.1: amount the Max Loss cap tightens per step.">Trail By</th>
                            <th style="width:64px;" title="Spec §6.1 (target doc): enable a user-level trailing target / profit-lock on combined profit.">Trail TGT</th>
                            <th style="width:96px;" title="Spec §6.1: combined profit at which the profit-lock activates.">TGT Reach</th>
                            <th style="width:96px;" title="Spec §6.1: combined-profit floor locked once the profit-lock activates.">TGT Lock</th>
                            <th style="width:96px;" title="Spec §6.1: for every this much further combined profit, raise the locked floor.">TGT Every</th>
                            <th style="width:96px;" title="Spec §6.1: amount the locked floor rises per step.">TGT By</th>
                            <th>Allowed Instruments</th>
                        </tr>
                    </thead>
                    <tbody id="users-tbody">${rows}</tbody>
                </table>
            </div>
            <div style="margin-top:14px; display:flex; gap:10px;">
                <button class="btn btn-secondary" onclick="Users.addRow()">+ Add User</button>
                <button class="btn btn-primary" onclick="Users.save()">Save All</button>
                <button class="btn btn-secondary" onclick="Users.render(document.getElementById('main-content'))">Reload</button>
            </div>
            <div style="margin-top:18px; color:var(--text-muted, #888); font-size:0.85rem;">
                <p><strong>Field rules:</strong></p>
                <ul style="margin-left:20px;">
                    <li><code>User ID</code> — URL-safe slug <code>[a-z0-9_-]{1,32}</code>. Used as a path component (portfolios/&lt;user_id&gt;/), so changing it after creation orphans existing portfolios. Keep stable.</li>
                    <li><code>Multiplier</code> — positive number. Scales every order quantity for this user before the admin per-symbol cap binds.</li>
                    <li><code>Allowed Instruments</code> — comma-separated bare symbols (e.g. <code>EURUSD, BTCUSD</code>). Leave blank for all instruments.</li>
                    <li>The reserved <code>_default</code> user holds legacy portfolios from before the migration. Don't delete it unless you've reassigned its portfolios.</li>
                </ul>
            </div>
        `;
    },

    _rowHTML(u, i) {
        const allowed = Array.isArray(u.allowed_instruments)
            ? u.allowed_instruments.join(", ")
            : "";
        const maxLoss = (u.max_loss === null || u.max_loss === undefined) ? "" : u.max_loss;
        const maxProfit = (u.max_profit === null || u.max_profit === undefined) ? "" : u.max_profit;
        const tslEvery = (u.trailing_sl_every === null || u.trailing_sl_every === undefined) ? "" : u.trailing_sl_every;
        const tslBy = (u.trailing_sl_by === null || u.trailing_sl_by === undefined) ? "" : u.trailing_sl_by;
        const ttgReach = (u.trailing_tgt_when_reach === null || u.trailing_tgt_when_reach === undefined) ? "" : u.trailing_tgt_when_reach;
        const ttgLock = (u.trailing_tgt_lock === null || u.trailing_tgt_lock === undefined) ? "" : u.trailing_tgt_lock;
        const ttgEvery = (u.trailing_tgt_every === null || u.trailing_tgt_every === undefined) ? "" : u.trailing_tgt_every;
        const ttgBy = (u.trailing_tgt_by === null || u.trailing_tgt_by === undefined) ? "" : u.trailing_tgt_by;
        return `
            <tr data-row="${i}">
                <td style="text-align:center;">
                    <button class="btn btn-xs btn-danger" onclick="Users.deleteRow(${i})" title="Delete">X</button>
                </td>
                <td><input type="text" class="form-control" data-field="user_id" value="${this._escape(u.user_id || "")}" pattern="[a-z0-9_-]{1,32}"></td>
                <td><input type="text" class="form-control" data-field="alias" value="${this._escape(u.alias || "")}"></td>
                <td><input type="number" class="form-control" data-field="multiplier" value="${u.multiplier ?? 1.0}" min="0" step="any"></td>
                <td><input type="number" class="form-control" data-field="max_loss" value="${maxLoss}" min="0" step="any" placeholder="(none)"></td>
                <td><input type="number" class="form-control" data-field="max_profit" value="${maxProfit}" min="0" step="any" placeholder="(none)"></td>
                <td style="text-align:center;"><input type="checkbox" data-field="trailing_sl_enabled" ${u.trailing_sl_enabled ? "checked" : ""}></td>
                <td><input type="number" class="form-control" data-field="trailing_sl_every" value="${tslEvery}" min="0" step="any" placeholder="(none)"></td>
                <td><input type="number" class="form-control" data-field="trailing_sl_by" value="${tslBy}" min="0" step="any" placeholder="(none)"></td>
                <td style="text-align:center;"><input type="checkbox" data-field="trailing_tgt_enabled" ${u.trailing_tgt_enabled ? "checked" : ""}></td>
                <td><input type="number" class="form-control" data-field="trailing_tgt_when_reach" value="${ttgReach}" min="0" step="any" placeholder="(none)"></td>
                <td><input type="number" class="form-control" data-field="trailing_tgt_lock" value="${ttgLock}" min="0" step="any" placeholder="(none)"></td>
                <td><input type="number" class="form-control" data-field="trailing_tgt_every" value="${ttgEvery}" min="0" step="any" placeholder="(none)"></td>
                <td><input type="number" class="form-control" data-field="trailing_tgt_by" value="${ttgBy}" min="0" step="any" placeholder="(none)"></td>
                <td><input type="text" class="form-control" data-field="allowed_instruments" value="${this._escape(allowed)}" placeholder="leave blank for all (e.g. EURUSD, BTCUSD)"></td>
            </tr>`;
    },

    addRow() {
        // Push to local state then re-render. Slug is suggested but editable
        // — admin should adjust before saving. The "u_" prefix avoids
        // colliding with the reserved _default id.
        const fresh = {
            user_id: `u_${Date.now().toString(36)}`,
            alias: "New User",
            multiplier: 1.0,
            max_loss: null,
            max_profit: null,
            trailing_sl_enabled: false,
            trailing_sl_every: null,
            trailing_sl_by: null,
            trailing_tgt_enabled: false,
            trailing_tgt_when_reach: null,
            trailing_tgt_lock: null,
            trailing_tgt_every: null,
            trailing_tgt_by: null,
            allowed_instruments: null,
        };
        this._collectFromDOM();
        this.users.push(fresh);
        this._renderTable(document.getElementById("main-content"));
    },

    deleteRow(i) {
        this._collectFromDOM();
        const u = this.users[i];
        if (u && u.user_id === "_default") {
            if (!confirm("'_default' holds legacy portfolios from before the migration. Deleting it will orphan those files. Continue?")) {
                return;
            }
        }
        this.users.splice(i, 1);
        this._renderTable(document.getElementById("main-content"));
    },

    _collectFromDOM() {
        // Pull current input values back into this.users so re-renders
        // (add/delete/save) don't lose unsaved edits.
        const tbody = document.getElementById("users-tbody");
        if (!tbody) return;
        const rows = tbody.querySelectorAll("tr[data-row]");
        rows.forEach(tr => {
            const i = parseInt(tr.dataset.row, 10);
            if (!Number.isFinite(i) || !this.users[i]) return;
            const get = (k) => tr.querySelector(`[data-field="${k}"]`)?.value;
            const getChecked = (k) => !!tr.querySelector(`[data-field="${k}"]`)?.checked;
            const uid = (get("user_id") || "").trim();
            const alias = (get("alias") || "").trim();
            const mraw = get("multiplier");
            const maxLossRaw = (get("max_loss") || "").trim();
            const maxProfitRaw = (get("max_profit") || "").trim();
            const tslEveryRaw = (get("trailing_sl_every") || "").trim();
            const tslByRaw = (get("trailing_sl_by") || "").trim();
            const ttgReachRaw = (get("trailing_tgt_when_reach") || "").trim();
            const ttgLockRaw = (get("trailing_tgt_lock") || "").trim();
            const ttgEveryRaw = (get("trailing_tgt_every") || "").trim();
            const ttgByRaw = (get("trailing_tgt_by") || "").trim();
            const allowedRaw = (get("allowed_instruments") || "").trim();

            const m = parseFloat(mraw);
            const ml = maxLossRaw === "" ? null : parseFloat(maxLossRaw);
            const mp = maxProfitRaw === "" ? null : parseFloat(maxProfitRaw);
            const tslEvery = tslEveryRaw === "" ? null : parseFloat(tslEveryRaw);
            const tslBy = tslByRaw === "" ? null : parseFloat(tslByRaw);
            const ttgReach = ttgReachRaw === "" ? null : parseFloat(ttgReachRaw);
            const ttgLock = ttgLockRaw === "" ? null : parseFloat(ttgLockRaw);
            const ttgEvery = ttgEveryRaw === "" ? null : parseFloat(ttgEveryRaw);
            const ttgBy = ttgByRaw === "" ? null : parseFloat(ttgByRaw);
            const allowed = allowedRaw
                ? allowedRaw.split(",").map(s => s.trim().toUpperCase()).filter(Boolean)
                : null;

            this.users[i] = {
                user_id: uid,
                alias: alias || uid,
                multiplier: Number.isFinite(m) && m > 0 ? m : 1.0,
                max_loss: Number.isFinite(ml) ? ml : null,
                max_profit: Number.isFinite(mp) ? mp : null,
                trailing_sl_enabled: getChecked("trailing_sl_enabled"),
                trailing_sl_every: Number.isFinite(tslEvery) ? tslEvery : null,
                trailing_sl_by: Number.isFinite(tslBy) ? tslBy : null,
                trailing_tgt_enabled: getChecked("trailing_tgt_enabled"),
                trailing_tgt_when_reach: Number.isFinite(ttgReach) ? ttgReach : null,
                trailing_tgt_lock: Number.isFinite(ttgLock) ? ttgLock : null,
                trailing_tgt_every: Number.isFinite(ttgEvery) ? ttgEvery : null,
                trailing_tgt_by: Number.isFinite(ttgBy) ? ttgBy : null,
                allowed_instruments: allowed,
            };
        });
    },

    async save() {
        this._collectFromDOM();
        const payload = { users: this.users };
        if (this.meta) payload._meta = this.meta;
        try {
            const res = await App.api("/api/users", {
                method: "POST",
                body: JSON.stringify(payload),
            });
            this.users = Array.isArray(res.users) ? res.users : this.users;
            App.toast("Users saved.", "success");
            this._renderTable(document.getElementById("main-content"));
        } catch (e) {
            App.toast(`Save failed: ${e.message}`, "error");
        }
    },

    _escape(s) {
        return String(s).replace(/[&<>"']/g, c => ({
            "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;",
        }[c]));
    },
};
