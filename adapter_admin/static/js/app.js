/**
 * App Router & Utilities for Adapter Admin Panel.
 */

const App = {
    currentPage: "dashboard",

    state: {
        editingAdapter: null,  // Set when editing an existing adapter
    },

    init() {
        this.navigate("dashboard");
    },

    navigate(page, state) {
        this.currentPage = page;
        if (state) {
            Object.assign(this.state, state);
        } else {
            // Clear transient state when navigating without explicit state
            this.state.editingAdapter = null;
        }

        // Update sidebar active state
        document.querySelectorAll(".nav-item").forEach(el => {
            el.classList.toggle("active", el.dataset.page === page);
        });

        const main = document.getElementById("main-content");
        switch (page) {
            case "dashboard":    Dashboard.render(main); break;
            case "adapter_config": AdapterConfig.render(main); break;
            case "custom_adapters": CustomAdapters.render(main); break;
            case "data_formats": DataFormats.render(main); break;
            default: main.innerHTML = '<div class="alert alert-danger">Page not found.</div>';
        }
    },

    // ─── API Helper ───────────────────────────────────────────────────────

    async api(endpoint, options = {}) {
        const defaults = {
            headers: { "Content-Type": "application/json" },
        };
        const config = { ...defaults, ...options };
        if (options.headers) {
            config.headers = { ...defaults.headers, ...options.headers };
        }

        const response = await fetch(endpoint, config);
        const data = await response.json();

        if (!response.ok) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }
        return data;
    },

    // ─── Toast ────────────────────────────────────────────────────────────

    toast(message, type = "info", duration = 4000) {
        const container = document.getElementById("toast-container");
        const toast = document.createElement("div");
        toast.className = `toast toast-${type}`;
        toast.textContent = message;
        container.appendChild(toast);
        setTimeout(() => {
            toast.style.opacity = "0";
            toast.style.transform = "translateX(100%)";
            setTimeout(() => toast.remove(), 300);
        }, duration);
    },

    // ─── Loading ──────────────────────────────────────────────────────────

    showLoading(text = "Loading...") {
        document.getElementById("loading-text").textContent = text;
        document.getElementById("loading-overlay").style.display = "flex";
    },

    hideLoading() {
        document.getElementById("loading-overlay").style.display = "none";
    },

    // ─── Accordion ────────────────────────────────────────────────────────

    accordionHTML(id, title, content, open = false) {
        return `
            <div class="accordion ${open ? 'open' : ''}" id="acc-${id}">
                <div class="accordion-header" onclick="document.getElementById('acc-${id}').classList.toggle('open')">
                    <span>${title}</span>
                    <span class="accordion-arrow">&#9660;</span>
                </div>
                <div class="accordion-body">${content}</div>
            </div>`;
    },

    // ─── Table ────────────────────────────────────────────────────────────

    tableHTML(data, columns) {
        if (!data || data.length === 0) return '<div class="alert alert-info">No data.</div>';
        const cols = columns || Object.keys(data[0]);
        let html = '<div class="table-container"><table><thead><tr>';
        cols.forEach(c => { html += `<th>${c}</th>`; });
        html += '</tr></thead><tbody>';
        data.forEach(row => {
            html += '<tr>';
            cols.forEach(c => { html += `<td>${row[c] ?? ''}</td>`; });
            html += '</tr>';
        });
        html += '</tbody></table></div>';
        return html;
    },

    // ─── Metric ───────────────────────────────────────────────────────────

    metricHTML(label, value) {
        return `<div class="metric"><div class="metric-label">${label}</div><div class="metric-value">${value}</div></div>`;
    },
};

// Initialize on load
document.addEventListener("DOMContentLoaded", () => App.init());
