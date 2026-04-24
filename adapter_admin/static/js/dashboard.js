/**
 * Dashboard Page - List all configured adapters in a table.
 */

const Dashboard = {
    adapters: [],

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#128268; Configured Adapters</h1>
            <p class="page-subtitle">Manage your NautilusTrader exchange adapters and connection configurations.</p>
            <div id="adapter-stats"></div>
            <div id="adapter-list"></div>
        `;

        await this.loadAdapters();
    },

    async loadAdapters() {
        const listDiv = document.getElementById("adapter-list");
        const statsDiv = document.getElementById("adapter-stats");

        try {
            const data = await App.api("/api/adapters");
            this.adapters = data.adapters;

            if (this.adapters.length === 0) {
                statsDiv.innerHTML = "";
                listDiv.innerHTML = `
                    <div class="empty-state">
                        <div class="empty-state-icon">&#128268;</div>
                        <div class="empty-state-text">No adapters configured yet.</div>
                        <p class="section-caption" style="margin-top: 8px;">Click <strong>Add Adapter</strong> in the sidebar to get started.</p>
                        <button class="btn btn-primary" style="margin-top: 16px;" onclick="App.navigate('adapter_config')">
                            &#10133; Add Your First Adapter
                        </button>
                    </div>`;
                return;
            }

            // Stats
            const builtIn = this.adapters.filter(a => !a.is_custom).length;
            const custom = this.adapters.filter(a => a.is_custom).length;
            const testnet = this.adapters.filter(a => a.testnet).length;
            const live = this.adapters.length - testnet;

            statsDiv.innerHTML = `
                <div class="grid-4" style="margin-bottom: 24px;">
                    ${App.metricHTML("Total Adapters", this.adapters.length)}
                    ${App.metricHTML("Built-in", builtIn)}
                    ${App.metricHTML("Custom", custom)}
                    ${App.metricHTML("Testnet / Live", `${testnet} / ${live}`)}
                </div>`;

            // Adapter table
            const registry = window._registryCache || {};

            let rows = "";
            for (const adapter of this.adapters) {
                const regEntry = registry[adapter.exchange_type];
                const assetClass = adapter.asset_class
                    ? adapter.asset_class.charAt(0).toUpperCase() + adapter.asset_class.slice(1)
                    : "—";

                const statusBadge = adapter.testnet
                    ? '<span class="badge badge-success">Testnet</span>'
                    : '<span class="badge badge-danger">Live</span>';

                const typeBadge = adapter.is_custom
                    ? '<span class="badge badge-warning">Custom</span>'
                    : '<span class="badge badge-info">Built-in</span>';

                let capBadges = "";
                if (regEntry) {
                    if (regEntry.supports_data) capBadges += '<span class="badge badge-info" style="font-size:0.7rem;">Data</span> ';
                    if (regEntry.supports_exec) capBadges += '<span class="badge badge-info" style="font-size:0.7rem;">Exec</span>';
                }

                const created = adapter.created_at ? adapter.created_at.split("T")[0] : "—";

                rows += `
                    <tr>
                        <td style="font-weight:600;">${adapter.name}</td>
                        <td>${adapter.exchange_type}</td>
                        <td><span class="badge badge-success" style="font-size:0.8rem;">${adapter.venue}</span></td>
                        <td>${assetClass}</td>
                        <td>${statusBadge}</td>
                        <td>${typeBadge}</td>
                        <td>${capBadges}</td>
                        <td>${created}</td>
                        <td>
                            <div style="display:flex; gap:6px;">
                                <button class="btn btn-sm" onclick="Dashboard.editAdapter('${adapter.id}')">Edit</button>
                                <button class="btn btn-sm btn-primary" onclick="Dashboard.testAdapter('${adapter.id}')">Test</button>
                                <button class="btn btn-sm btn-danger" onclick="Dashboard.deleteAdapter('${adapter.id}', '${adapter.name}')">Delete</button>
                            </div>
                        </td>
                    </tr>`;
            }

            listDiv.innerHTML = `
                <div class="table-container" style="max-height:none;">
                    <table>
                        <thead>
                            <tr>
                                <th>Name</th>
                                <th>Exchange</th>
                                <th>Venue</th>
                                <th>Asset Class</th>
                                <th>Environment</th>
                                <th>Type</th>
                                <th>Capabilities</th>
                                <th>Created</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            ${rows}
                        </tbody>
                    </table>
                </div>`;

        } catch (e) {
            listDiv.innerHTML = `<div class="alert alert-danger">Failed to load adapters: ${e.message}</div>`;
        }
    },

    editAdapter(adapterId) {
        const adapter = this.adapters.find(a => a.id === adapterId);
        if (adapter) {
            App.navigate("adapter_config", { editingAdapter: adapter });
        }
    },

    async testAdapter(adapterId) {
        const adapter = this.adapters.find(a => a.id === adapterId);
        App.toast(`Testing connection for ${adapter ? adapter.name : adapterId}...`, "info");

        try {
            const result = await App.api(`/api/adapters/${adapterId}/test`, { method: "POST" });
            if (result.success) {
                App.toast(result.message, "success");
            } else {
                App.toast(result.message, "error", 6000);
            }
        } catch (e) {
            App.toast(`Test failed: ${e.message}`, "error");
        }
    },

    async deleteAdapter(adapterId, name) {
        if (!confirm(`Delete adapter "${name}"? This cannot be undone.`)) return;

        try {
            await App.api(`/api/adapters/${adapterId}`, { method: "DELETE" });
            App.toast(`Deleted adapter "${name}"`, "success");
            this.loadAdapters();
        } catch (e) {
            App.toast(`Delete failed: ${e.message}`, "error");
        }
    },
};
