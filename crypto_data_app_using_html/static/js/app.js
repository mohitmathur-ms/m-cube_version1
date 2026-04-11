/**
 * Main Application - Router, utilities, and shared state.
 */

const App = {
    currentPage: "dashboard",
    state: {
        backtestResults: null,
        backtestConfig: null,
    },

    /** Initialize the app */
    init() {
        this.bindNavigation();
        this.navigate("dashboard");
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

    /** Navigate to a page */
    navigate(page) {
        this.currentPage = page;

        // Update active nav item
        document.querySelectorAll(".nav-item").forEach(item => {
            item.classList.toggle("active", item.dataset.page === page);
        });

        // Render page
        const main = document.getElementById("main-content");
        switch (page) {
            case "dashboard":   Dashboard.render(main); break;
            case "load_data":   LoadData.render(main); break;
            case "view_data":   ViewData.render(main); break;
            case "backtest":    Backtest.render(main); break;
            case "tearsheet":   Tearsheet.render(main); break;
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

    /** Make API call */
    async api(endpoint, options = {}) {
        const url = endpoint.startsWith("http") ? endpoint : endpoint;
        const response = await fetch(url, {
            headers: { "Content-Type": "application/json" },
            ...options,
        });
        if (!response.ok) {
            const error = await response.json().catch(() => ({ error: response.statusText }));
            throw new Error(error.error || response.statusText);
        }
        return response.json();
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
};

// Boot the app when DOM is ready
document.addEventListener("DOMContentLoaded", () => App.init());
