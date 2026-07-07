/**
 * Visual Verification page — the live, interactive cousin of
 * portfolios/testing2/manual_verification_v2_RESULTS.html.
 *
 * Provide a backtest's Order Book (saved picker OR uploaded CSV / HTML report);
 * the server re-derives every exit across Time · Logic · Price against the
 * catalog's 1-second feed and returns a verdict table. Click a trade row to draw
 * its two Plotly candlestick/line charts (full-trade + trigger zoom) with the
 * stop level and first-touch marker — the same visuals as the static report.
 *
 * Bars for the charts come from the EXISTING /api/data/bars endpoint (cached per
 * day). The Order Book / HTML report contain only trades — the candlesticks need
 * the feed to still be present in ./catalog/.
 */
const VisualVerification = {
    _savedFiles: [],
    _barTypes: [],
    _result: null,        // last analyze response
    _barCache: {},        // `${bar_type}|${YYYY-MM-DD}` -> [bars]
    _selectedOid: null,

    async render(container) {
        container.innerHTML = `
            <style>
                .vv-banner{padding:14px 18px;border-radius:10px;font-weight:700;margin:14px 0;font-size:15px;}
                .vv-banner-pass{background:#0d2818;color:#3fb950;border:1px solid #2ea043;}
                .vv-banner-fail{background:#2d1316;color:#f85149;border:1px solid #cf222e;}
                .vv-banner-warn{background:#2d2206;color:#d29922;border:1px solid #d29922;}
                .vv-table{width:100%;border-collapse:collapse;font-size:13px;}
                .vv-table th,.vv-table td{padding:7px 9px;border-bottom:1px solid var(--border-subtle,#30363d);}
                .vv-table th{font-size:11px;text-transform:uppercase;letter-spacing:.3px;color:var(--text-muted,#8b949e);text-align:left;}
                .vv-table td.num{text-align:right;font-variant-numeric:tabular-nums;} .vv-table td.center{text-align:center;}
                .vv-table td.mono{font-family:"SF Mono",Monaco,Consolas,monospace;}
                .vv-clickable{cursor:pointer;} .vv-clickable:hover td{background:rgba(88,166,255,.10);}
                .vv-pill-pass{background:#0d2818;color:#3fb950;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;}
                .vv-pill-fail{background:#2d1316;color:#f85149;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;}
                .vv-pill-na{background:#2d2206;color:#d29922;padding:1px 8px;border-radius:10px;font-size:11px;font-weight:700;}
                .vv-rfav{color:#d29922;font-weight:600;} .vv-radv{color:#f85149;font-weight:600;} .vv-rat{color:#3fb950;font-weight:600;}
                .vv-realism{background:rgba(88,166,255,.07);border:1px solid var(--border-subtle,#30363d);border-left:4px solid #58a6ff;border-radius:0 8px 8px 0;padding:12px 16px;margin:12px 0;font-size:13px;line-height:1.6;}
                .vv-realism b{color:#58a6ff;}
                .vv-realism .grid{display:flex;flex-wrap:wrap;gap:10px 22px;margin-top:8px;}
                .vv-realism .grid div{font-size:12.5px;} .vv-realism .grid .k{color:var(--text-muted,#8b949e);text-transform:uppercase;font-size:10px;letter-spacing:.3px;display:block;}
                .vv-realism .grid .v{font-size:15px;font-weight:700;}
                .vv-finding{background:#2d1316;border:1px solid #cf222e;border-left:5px solid #f85149;border-radius:8px;padding:12px 16px;margin:12px 0;font-size:13px;line-height:1.6;}
                .vv-finding b{color:#ffb4ae;}
                .vv-explain{margin:12px 0;}
                .vv-explain summary{cursor:pointer;font-weight:600;color:#58a6ff;font-size:13px;}
                .vv-explain table{width:100%;border-collapse:collapse;font-size:12.5px;margin-top:8px;}
                .vv-explain th,.vv-explain td{padding:6px 9px;border-bottom:1px solid var(--border-subtle,#30363d);text-align:left;vertical-align:top;}
                .vv-explain th{font-size:10.5px;text-transform:uppercase;color:var(--text-muted,#8b949e);}
                .vv-tdgrid{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:8px 16px;margin:8px 0 2px;}
                .vv-tdgrid .k{color:var(--text-muted,#8b949e);text-transform:uppercase;font-size:10px;letter-spacing:.3px;display:block;}
                .vv-tdgrid .v{font-size:14px;font-weight:700;}
            </style>
            <h1 class="page-title">Visual Verification</h1>
            <p class="page-subtitle">Re-derive every backtest exit across <b>Time &middot; Logic &middot; Price</b> against the
            1-second catalog bars, <b>plus</b> a diagnostic <b>execution-realism</b> tier (entry correctness, fill realism,
            slippage, gaps, intrabar ordering, P&amp;L realism). Pick a saved order book or upload one, then click a trade to
            see the candlestick proof and its realism breakdown.</p>

            <div class="card" style="padding:16px; margin-bottom:16px;">
                <div style="display:flex; align-items:flex-end; gap:14px; flex-wrap:wrap;">
                    <div>
                        <label style="font-weight:600; font-size:0.8rem; display:block; margin-bottom:4px;">Saved order book</label>
                        <select id="vv-saved" class="form-select" style="min-width:300px;"></select>
                    </div>
                    <div>
                        <label style="font-weight:600; font-size:0.8rem; display:block; margin-bottom:4px;">&hellip; or upload CSV / HTML report</label>
                        <input type="file" id="vv-upload" accept=".csv,.html,.htm" class="form-control" style="min-width:240px;">
                    </div>
                    <div>
                        <label style="font-weight:600; font-size:0.8rem; display:block; margin-bottom:4px;">Feed (bar type)</label>
                        <select id="vv-bartype" class="form-select" style="min-width:260px;">
                            <option value="">Auto-detect from order book</option>
                        </select>
                    </div>
                    <button class="btn btn-primary" onclick="VisualVerification.analyze()">Analyze</button>
                </div>
                <div id="vv-status" style="font-size:0.82rem; color:var(--text-muted); margin-top:10px;"></div>
            </div>

            <div id="vv-banner"></div>
            <div id="vv-table-area"></div>
            <div id="vv-charts" style="margin-top:18px;"></div>
        `;
        await this._loadPickers();
    },

    onShow() {
        this._loadPickers();
    },

    async _loadPickers() {
        try {
            const r = await App.api("/api/orderbook/list");
            this._savedFiles = r.files || [];
        } catch { this._savedFiles = []; }
        try {
            const r = await App.api("/api/data/bar_types");
            this._barTypes = r.bar_types || [];
        } catch { this._barTypes = []; }

        const sel = document.getElementById("vv-saved");
        if (sel) {
            const prev = sel.value;
            sel.innerHTML = '<option value="">— select a saved order book —</option>' +
                this._savedFiles.map(f =>
                    `<option value="${f}">${f.replace(/^order_book_/, "").replace(/\.csv$/, "")}</option>`).join("");
            if ([...sel.options].some(o => o.value === prev)) sel.value = prev;
        }
        const bt = document.getElementById("vv-bartype");
        if (bt) {
            const prev = bt.value;
            bt.innerHTML = '<option value="">Auto-detect from order book</option>' +
                this._barTypes.map(t => `<option value="${t}">${t}</option>`).join("");
            if ([...bt.options].some(o => o.value === prev)) bt.value = prev;
        }
    },

    /* ─── Analyze ──────────────────────────────────────────────────────────── */
    async analyze() {
        const status = document.getElementById("vv-status");
        const upload = document.getElementById("vv-upload");
        const saved = document.getElementById("vv-saved").value;
        const barType = document.getElementById("vv-bartype").value;

        const fd = new FormData();
        if (barType) fd.append("bar_type", barType);

        if (upload && upload.files && upload.files.length) {
            fd.append("file", upload.files[0]);
        } else if (saved) {
            fd.append("file", saved);
        } else {
            App.toast("Pick a saved order book or upload a CSV / HTML report.", "warning");
            return;
        }

        status.textContent = "Analyzing against the 1-second catalog bars…";
        this._clearCharts();
        try {
            const resp = await fetch("/api/visual-verification/analyze", {
                method: "POST", headers: App.userHeaders(), body: fd,
            });
            const data = await resp.json();
            if (!resp.ok) throw new Error(data.error || `HTTP ${resp.status}`);
            this._result = data;
            this._barCache = {};
            this._selectedOid = null;
            this._renderResult();
            status.textContent =
                `Source: ${data.source} · instrument ${data.instrument} · feed ${data.bar_type || "(none)"} · ` +
                (data.feed_in_catalog ? "bars loaded ✓" : "feed NOT in catalog — charts unavailable") +
                (data.config_loaded ? " · per-feature verdict parity ✓" : " · generic verdicts (no portfolio config)");
        } catch (e) {
            status.textContent = "";
            document.getElementById("vv-banner").innerHTML = "";
            document.getElementById("vv-table-area").innerHTML =
                `<div class="alert alert-danger">Analyze failed: ${App._htmlEscape(e.message)}</div>`;
        }
    },

    /* ─── Render verdicts ──────────────────────────────────────────────────── */
    _renderResult() {
        const d = this._result;
        const s = d.summary;
        const banner = document.getElementById("vv-banner");
        const anyFail = s.fail > 0;
        const allPass = s.fail === 0 && s.pass > 0 && s.na === 0;
        let bn, cls;
        if (allPass) { bn = `&#10004; VERIFICATION PASSED — ${s.pass}/${s.n} trades green across Time · Logic · Price`; cls = "vv-banner-pass"; }
        else if (anyFail) { bn = `&#10008; VERIFICATION FAILED — ${s.fail} of ${s.n} trades mismatched`; cls = "vv-banner-fail"; }
        else { bn = `Loaded ${s.n} trades — ${s.pass} verified, ${s.na} not checkable (feed not in catalog)`; cls = "vv-banner-warn"; }
        banner.innerHTML = `<div class="vv-banner ${cls}">${bn}</div>`;

        const rows = d.trades.map((t, i) => {
            const vcls = t.verdict === "PASS" ? "vv-pill-pass" : t.verdict === "FAIL" ? "vv-pill-fail" : "vv-pill-na";
            const clickable = d.feed_in_catalog && t.exit_ist;
            return `<tr class="vv-row ${clickable ? "vv-clickable" : ""}" ${clickable ? `onclick="VisualVerification.showTradeCharts('${t.oid}')"` : ""}>
                <td class="mono">${App._htmlEscape(t.oid)}</td>
                <td>${t.side}</td>
                <td>${App._htmlEscape(t.kind_label || "—")}</td>
                <td class="num">${isFinite(t.F) ? t.F.toFixed(2) : "—"}</td>
                <td class="num">${t.level != null ? t.level.toFixed(2) : "—"}</td>
                <td class="mono">${t.exit_ist ? t.exit_ist.slice(11) : "—"}</td>
                <td class="mono">${t.first_touch ? t.first_touch.slice(11) : "—"}</td>
                <td class="center">${this._mark(t.time_ok)}</td>
                <td class="center">${this._mark(t.logic_ok)}</td>
                <td class="center">${this._mark(t.price_ok)}</td>
                <td class="center" title="Check 1 — recomputed config level == logged SL/TP">${this._mark(t.config_ok)}</td>
                <td class="num">${this._deltaCell(t)}</td>
                <td class="center">${this._mark(t.entry_ok)}</td>
                <td class="center"><span class="${vcls}">${t.verdict}</span></td>
            </tr>`;
        }).join("");

        const parityNote = d.config_loaded === false
            ? `<div class="alert alert-info" style="font-size:12.5px;">Generic verdicts: no portfolio config was found for this order book
               (uploaded file or non-portfolio CSV), so per-feature logic isn't applied — an <b>SL-Wait</b> exit will show a Time
               "delay" (✗) because it fires later than first-touch by design. Analyze a <b>saved portfolio order book</b> for full
               parity with the RESULTS report.</div>`
            : "";
        document.getElementById("vv-table-area").innerHTML = `
            ${this._realismPanel(d)}
            ${parityNote}
            ${this._explainer()}
            ${d.feed_in_catalog ? "" : '<div class="alert alert-warning">The feed for this instrument is not loaded in the catalog, so bar-level checks and charts are unavailable. Load it via <b>Load Data</b> to enable them.</div>'}
            <div class="table-container"><table class="vv-table">
                <thead><tr>
                    <th>OID</th><th>Side</th><th>Type</th><th>Entry F</th><th>Level</th>
                    <th>Exit (IST)</th><th>First touch</th><th>Time</th><th>Logic</th><th>Price</th>
                    <th title="Check 1 — recomputed config level (entry × (1±value) / entry ± points) == logged SL/TP">Config</th>
                    <th title="Realised fill minus the level: +favourable (inflates), −adverse; ↑gap = bar opened past the level">Δ fill−lvl</th>
                    <th title="Entry filled at the signal bar's close (no look-ahead)">Entry</th><th>Verdict</th>
                </tr></thead>
                <tbody>${rows}</tbody>
            </table></div>
            ${d.feed_in_catalog ? '<p class="section-caption" style="margin-top:6px;">Click any trade row to see its candlestick verification + per-trade realism breakdown. Columns A–C (Time/Logic/Price) are the PASS/FAIL verdict; Δ&nbsp;fill−lvl and Entry are diagnostic realism.</p>' : ""}
        `;
    }

    ,
    /* ─── Execution-realism rendering (diagnostic, additive) ───────────────── */
    _deltaCell(t) {
        const r = t.realism;
        if (!r || r.fav_pts == null || !isFinite(r.fav_pts)) return '<span style="color:var(--text-muted)">&ndash;</span>';
        const cls = r.favorable ? "vv-rfav" : (r.adverse ? "vv-radv" : "vv-rat");
        const sign = r.fav_pts >= 0 ? "+" : "";
        const gapMark = r.gap ? ' <span title="bar opened already past the level (gap)">↑gap</span>' : "";
        return `<span class="${cls}">${sign}${r.fav_pts.toFixed(2)}${gapMark}</span>`;
    },

    _realismPanel(d) {
        const s = d.realism;
        if (!s) return "";
        const entry = s.entry_n
            ? `<b>Entry correctness:</b> ${s.entry_ok}/${s.entry_n} entries fill at the signal bar's close (MARKET/GTC, no next-bar-open look-ahead). `
            : "";
        let finding = "";
        if (s.cross_session > 0) {
            finding = `<div class="vv-finding">⚑ <b>${s.cross_session} exit(s) held into a later session.</b>
                These positions carried past the session close and filled on their SL/Target at/after the next open —
                often through a large overnight gap (see the <span class="vv-radv">↑gap</span> rows). On an intraday (MIS)
                portfolio that should be squared off daily, this is worth investigating.</div>`;
        }
        if (!s.n_level) {
            return `<div class="vv-realism">${entry}<b>Exit realism:</b> no SL/Target level exits in this order book
                (e.g. square-off-only) — no fill-vs-level counterfactual applies.</div>${finding}`;
        }
        const inflCls = s.infl_sum > 0 ? "vv-rfav" : (s.infl_sum < 0 ? "vv-radv" : "vv-rat");
        const fmt = (v) => (v >= 0 ? "+" : "") + Math.round(v).toLocaleString();
        return `<div class="vv-realism">
            ${entry}<b>Execution realism (diagnostic — does not change the Time·Logic·Price verdict):</b>
            of ${s.n_level} level exits, <span class="vv-rfav">${s.favorable} filled favourably</span>
            (close beat the level → flatters P&L), <span class="vv-radv">${s.adverse} adverse</span>,
            ${s.at_level} ~at level · <b>${s.gap}</b> gap-through, <b>${s.cross_session}</b> cross-session.
            <div class="grid">
                <div><span class="k">P&L realism (close-fill vs fill-at-level)</span><span class="v ${inflCls}">${fmt(s.infl_sum)}</span></div>
                <div><span class="k">favourable / adverse</span><span class="v">+${Math.round(s.fav_sum).toLocaleString()} / ${Math.round(s.adv_sum).toLocaleString()}</span></div>
                <div><span class="k">slippage (default)</span><span class="v">zero</span></div>
                <div><span class="k">1-tick conservative haircut</span><span class="v vv-radv">−${s.slip_haircut.toFixed(2)}</span></div>
            </div>
            <span class="section-caption">The fill-at-level counterfactual re-prices each level exit AT its trigger level
            (≈ a native move-through stop). A positive net means the bar-close fill flattered the backtest vs filling at the stop.</span>
        </div>${finding}`;
    }

    ,
    _explainer() {
        return `<details class="vv-explain">
            <summary>What the columns mean — the 3 axes + the 6 realism caveats</summary>
            <table>
              <thead><tr><th>Axis</th><th>What it checks</th></tr></thead>
              <tbody>
                <tr><td><b>A · Time</b></td><td>The exit fired on the first bar its level was touched — not earlier (look-ahead), not later (lag), unless a Wait delay applies. PASS/FAIL.</td></tr>
                <tr><td><b>B · Logic</b></td><td>The exit reason matches the condition actually true; exactly one exit per trade. PASS/FAIL.</td></tr>
                <tr><td><b>C · Price</b></td><td>The trigger bar reached the logged level and the fill equals that bar's close (Check 2). PASS/FAIL.</td></tr>
                <tr><td><b>Config</b></td><td><b>Check 1</b> — the level recomputed from the leg's JSON config (percentage: entry × (1±value/100); points: entry ± value) equals the engine's logged <code>SL=</code>/<code>TP=</code>. PASS/FAIL when statically recomputable; <code>–</code> for trailing/ATR/Move-SL legs (level moves per bar) or uploaded books with no config. Click a row for the worked Check 1 + Check 2.</td></tr>
                <tr><td><b>D · Entry correctness</b></td><td>The MARKET entry fills at the signal bar's close (no next-bar-open look-ahead). Diagnostic.</td></tr>
                <tr><td><b>E · Realistic fills</b></td><td>The reduce-only MARKET exit fills at the trigger bar's <em>close</em>, not at the level — the <code>Δ fill−lvl</code> column. Diagnostic.</td></tr>
                <tr><td><b>F · Slippage</b></td><td>The default FillModel applies zero slippage; the panel shows a conservative 1-tick-per-exit haircut. Diagnostic.</td></tr>
                <tr><td><b>G · Gap behaviour</b></td><td><code>↑gap</code> = the bar opened already past the level (intraday/overnight). m-cube's manual market exit bypasses gap-aware native-stop fills. Diagnostic.</td></tr>
                <tr><td><b>H · Intrabar ordering</b></td><td>Inside one bar the engine walks a fixed Open→High→Low→Close path and checks SL before TP. Diagnostic.</td></tr>
                <tr><td><b>I · P&L realism</b></td><td>A fill-at-level counterfactual quantifies how much the close-fill flatters (or hurts) reported P&L vs filling at the stop. Diagnostic.</td></tr>
              </tbody>
            </table>
        </details>`;
    },

    /* ─── Per-row manual-verification recipe (Check 1 + Check 2) ───────────── */
    _recipePanel(t) {
        const r = t.recipe;
        if (!r) return "";
        const f2 = (v) => (v == null || !isFinite(v)) ? "—" : v.toFixed(2);
        const lbl = t.kind === "SL" ? "SL" : "TP";

        // Check 1 — Config → level
        const c1 = r.check1 || {};
        let c1body;
        if (c1.ok == null) {
            c1body = `<span style="color:var(--text-muted)">${App._htmlEscape(c1.note || "not statically recomputable")}</span>`;
        } else {
            c1body = `<code>${App._htmlEscape(c1.formula)}</code> &nbsp; ${this._mark(c1.ok)}
                &nbsp; ${c1.ok ? "equals" : "≠"} logged <b>${lbl}=${f2(c1.logged)}</b>
                <span class="section-caption">(${App._htmlEscape(c1.type)} ${c1.value != null ? c1.value : ""})</span>`;
        }

        // Check 2 — level → actual bar
        const c2 = r.check2 || {};
        let c2body;
        if (!c2.ohlc) {
            c2body = `<span style="color:var(--text-muted)">trigger bar not in catalog — load the feed via <b>Load Data</b> to confirm the breach on the bar.</span>`;
        } else {
            const o = c2.ohlc, ex = c2.extreme_label, exv = f2(c2.extreme_val);
            const cmp = ex === "high" ? "≥" : "≤";
            const rp = c2.reason_price;
            const rpLine = rp == null ? ""
                : ` · reason <code>price=${f2(rp)}</code> = bar ${ex} ${this._mark(c2.reason_price_ok)}`;
            c2body = `bar @ <b>${t.exit_ist ? t.exit_ist.slice(11) : "—"}</b> &nbsp;
                O ${f2(o.open)} · H ${f2(o.high)} · L ${f2(o.low)} · C ${f2(o.close)}<br>
                ${ex} <b>${exv}</b> ${cmp} ${lbl} ${f2(c2.level)} → breach ${this._mark(c2.breach)}${rpLine}<br>
                AVG EXIT <b>${f2(c2.fill)}</b> = bar close ${f2(c2.bar_close)} ${this._mark(c2.fill_ok)}
                <span class="section-caption">(the reduce-only MARKET close fills at the trigger bar's close — detect on the adverse extreme, fill at close)</span>`;
        }

        return `<div class="vv-realism" style="margin:6px 0 12px;border-left-color:#3fb950;">
            <b>Manual-verification recipe — ${t.side} ${App._htmlEscape(t.kind_label || "")}</b>
            <table style="width:100%;border-collapse:collapse;margin-top:8px;font-size:13px;">
              <tr><td style="vertical-align:top;padding:6px 10px 6px 0;white-space:nowrap;"><b>Check 1</b><br><span class="section-caption">Config → level</span></td>
                  <td style="padding:6px 0;">${c1body}</td></tr>
              <tr><td style="vertical-align:top;padding:6px 10px 6px 0;white-space:nowrap;border-top:1px solid var(--border-subtle,#30363d);"><b>Check 2</b><br><span class="section-caption">level → actual bar</span></td>
                  <td style="padding:6px 0;border-top:1px solid var(--border-subtle,#30363d);">${c2body}</td></tr>
            </table>
        </div>`;
    },

    _tradeRealism(t) {
        const r = t.realism;
        const entryTxt = t.entry_ok == null ? "—" : (t.entry_ok ? "✓ at signal-bar close" : "✗ differs from bar close");
        if (!r) {
            return `<div class="vv-realism" style="margin:6px 0 12px;">
                <b>Execution realism:</b> Entry correctness — ${entryTxt}.
                No SL/Target level on this exit (e.g. square-off) — no fill-vs-level counterfactual.</div>`;
        }
        const fmt = (v) => v == null || !isFinite(v) ? "—" : (v >= 0 ? "+" : "") + v.toFixed(2);
        const inflCls = r.infl == null ? "" : (r.infl > 0 ? "vv-rfav" : (r.infl < 0 ? "vv-radv" : "vv-rat"));
        const favCls = r.favorable ? "vv-rfav" : (r.adverse ? "vv-radv" : "vv-rat");
        return `<div class="vv-realism" style="margin:6px 0 12px;">
            <b>Execution realism (diagnostic):</b>
            <div class="vv-tdgrid">
                <div><span class="k">Entry correctness</span><span class="v">${entryTxt}</span></div>
                <div><span class="k">Δ fill − level</span><span class="v ${favCls}">${fmt(r.fav_pts)} pts ${r.favorable ? "(favourable → inflates)" : r.adverse ? "(adverse)" : "(~at level)"}</span></div>
                <div><span class="k">Gap-through</span><span class="v">${r.gap ? "yes — bar opened past the level" : "no"}</span></div>
                <div><span class="k">Cross-session hold</span><span class="v ${r.cross_session ? "vv-radv" : ""}">${r.cross_session ? "yes (held overnight)" : "no"}</span></div>
                <div><span class="k">Reported P&L</span><span class="v">${fmt(t.pnl)}</span></div>
                <div><span class="k">Fill-at-level P&L</span><span class="v">${fmt(r.level_pnl)}</span></div>
                <div><span class="k">P&L realism (infl.)</span><span class="v ${inflCls}">${fmt(r.infl)}</span></div>
                <div><span class="k">1-tick slippage haircut</span><span class="v vv-radv">−${r.slip != null ? r.slip.toFixed(2) : "—"}</span></div>
            </div>
            <span class="section-caption">Fill-at-level = what a native move-through stop would have given. Slippage is zero by default.</span>
        </div>`;
    },

    _mark(v) {
        if (v === true) return '<span style="color:#2ea043;font-weight:700">&#10004;</span>';
        if (v === false) return '<span style="color:#cf222e;font-weight:700">&#10008;</span>';
        return '<span style="color:var(--text-muted)">&ndash;</span>';
    },

    /* ─── Charts ───────────────────────────────────────────────────────────── */
    _clearCharts() {
        const c = document.getElementById("vv-charts");
        if (c) c.innerHTML = "";
    },

    /** UTC-ISO (with offset) -> IST wall-clock ms basis, matching App.formatIST. */
    _istMs(isoUtc) { return Date.parse(App.formatIST(isoUtc, true) + "Z"); },
    /** "YYYY-MM-DD HH:MM:SS" (already IST) -> same ms basis. */
    _wallMs(s) { return Date.parse(s.replace(" ", "T") + "Z"); },

    async _fetchDayBars(barType, dayIso) {
        const key = `${barType}|${dayIso}`;
        if (this._barCache[key]) return this._barCache[key];
        const r = await App.api(`/api/data/bars?bar_type=${encodeURIComponent(barType)}` +
            `&start=${dayIso}&end=${dayIso}&tz_offset=330&limit=100000`);
        const bars = (r.data || []).map(b => ({ ...b, _ms: this._istMs(b.timestamp), _x: App.formatIST(b.timestamp, true) }));
        this._barCache[key] = bars;
        return bars;
    },

    async showTradeCharts(oid) {
        const d = this._result;
        const t = (d.trades || []).find(x => x.oid === oid);
        if (!t) return;
        this._selectedOid = oid;
        const host = document.getElementById("vv-charts");
        host.innerHTML = `<div class="card" style="padding:14px;">
            <h3 style="margin:0 0 4px;">Trade OID ${App._htmlEscape(t.oid)} · ${t.side} · ${App._htmlEscape(t.kind_label || "")}
                <span class="${t.verdict === "PASS" ? "vv-pill-pass" : t.verdict === "FAIL" ? "vv-pill-fail" : "vv-pill-na"}">${t.verdict}</span></h3>
            <p class="section-caption" style="margin-top:0;">Entry ${t.entry_ist} @ ${isFinite(t.F) ? t.F.toFixed(2) : "?"} ·
                level ${t.level != null ? t.level.toFixed(2) : "—"} · exit ${t.exit_ist} @ ${isFinite(t.fill) ? t.fill.toFixed(2) : "?"}
                (trigger-bar close ${t.trig_close != null ? t.trig_close.toFixed(2) : "—"})</p>
            ${this._recipePanel(t)}
            ${this._tradeRealism(t)}
            <div id="vv-chart-full" style="height:340px;"></div>
            <div id="vv-chart-zoom" style="height:340px; margin-top:10px;"></div>
        </div>`;

        try {
            const dayIso = t.exit_ist.slice(0, 10);
            const bars = await this._fetchDayBars(d.bar_type, dayIso);
            if (this._selectedOid !== oid) return; // user clicked another row
            this._plotFull(t, bars);
            this._plotZoom(t, bars);
        } catch (e) {
            host.insertAdjacentHTML("beforeend",
                `<div class="alert alert-danger">Could not load bars: ${App._htmlEscape(e.message)}</div>`);
        }
    },

    _plotFull(t, bars) {
        const entryMs = this._wallMs(t.entry_ist), exitMs = this._wallMs(t.exit_ist);
        const seg = bars.filter(b => b._ms >= entryMs && b._ms <= exitMs);
        if (!seg.length) return;
        const x = seg.map(b => b._x), close = seg.map(b => b.close);
        const traces = [
            { x, y: close, type: "scatter", mode: "lines", name: "close (1s)", line: { color: "#58a6ff", width: 1 } },
            { x: [t.entry_ist.replace(" ", "T")], y: [t.F], type: "scatter", mode: "markers", name: "entry",
              marker: { symbol: "triangle-up", size: 11, color: "#2ea043" } },
            { x: [t.exit_ist.replace(" ", "T")], y: [t.fill], type: "scatter", mode: "markers", name: "exit fill",
              marker: { symbol: "x", size: 11, color: "#cf222e" } },
        ];
        App.plot("vv-chart-full", traces, {
            title: `OID ${t.oid} · ${t.side} — full trade`,
            xaxis: { title: "Time (IST)" }, yaxis: { title: "Price" },
            shapes: this._levelShapes(t), height: 340,
            margin: { l: 56, r: 16, t: 36, b: 36 }, showlegend: true,
        });
    },

    _plotZoom(t, bars) {
        const exitMs = this._wallMs(t.exit_ist);
        const seg = bars.filter(b => b._ms >= exitMs - 70000 && b._ms <= exitMs + 12000);
        if (!seg.length) return;
        const x = seg.map(b => b._x);
        const candle = {
            x, open: seg.map(b => b.open), high: seg.map(b => b.high),
            low: seg.map(b => b.low), close: seg.map(b => b.close),
            type: "candlestick", name: "1s OHLC",
        };
        const fill = { x: [t.exit_ist.replace(" ", "T")], y: [t.fill], type: "scatter", mode: "markers",
            name: "fill", marker: { symbol: "circle", size: 10, color: "#d29922" } };
        const cross = t.is_long ? "low ≤ level" : "high ≥ level";
        App.plot("vv-chart-zoom", [candle, fill], {
            title: `OID ${t.oid} — trigger zoom (first bar ${cross} @ ${t.exit_ist.slice(11)})`,
            xaxis: { title: "Time (IST)", rangeslider: { visible: false } }, yaxis: { title: "Price" },
            shapes: this._levelShapes(t, true), height: 340,
            margin: { l: 56, r: 16, t: 36, b: 36 }, showlegend: true,
        });
    },

    /** Horizontal level line (+ optional vertical trigger marker). */
    _levelShapes(t, withTrigger) {
        const shapes = [];
        if (t.level != null) {
            shapes.push({ type: "line", xref: "paper", x0: 0, x1: 1, y0: t.level, y1: t.level,
                line: { color: "#cf222e", width: 1.2, dash: "dash" } });
        }
        if (withTrigger && t.exit_ist) {
            const xt = t.exit_ist.replace(" ", "T");
            shapes.push({ type: "line", x0: xt, x1: xt, yref: "paper", y0: 0, y1: 1,
                line: { color: "#d29922", width: 1, dash: "dot" } });
        }
        return shapes;
    },
};
