/**
 * Dashboard Page - Home page with navigation cards and catalog status.
 */

const Dashboard = {
    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">Welcome to M_Cube Crypto Dashboard</h1>
            <div class="page-divider"></div>

            <div class="grid-3">
                <div class="card nav-card" onclick="App.navigate('load_data')">
                    <div class="card-title">1. Load Data</div>
                    <div class="card-text">Import daily OHLCV data from your local crypto CSV files.</div>
                    <span class="btn btn-sm btn-primary">Go to Load Data</span>
                </div>
                <div class="card nav-card" onclick="App.navigate('view_data')">
                    <div class="card-title">2. View Data</div>
                    <div class="card-text">Explore loaded data with interactive tables and candlestick charts.</div>
                    <span class="btn btn-sm btn-primary">Go to View Data</span>
                </div>
                <div class="card nav-card" onclick="App.navigate('backtest')">
                    <div class="card-title">3. Run Backtest</div>
                    <div class="card-text">Test trading strategies (EMA Cross, RSI, Bollinger) on your data.</div>
                    <span class="btn btn-sm btn-primary">Go to Backtest</span>
                </div>
                <div class="card nav-card" onclick="App.navigate('tearsheet')">
                    <div class="card-title">4. Tearsheet</div>
                    <div class="card-text">View detailed performance analytics, equity curves, and drawdown charts.</div>
                    <span class="btn btn-sm btn-primary">Go to Tearsheet</span>
                </div>
                <div class="card nav-card" onclick="App.navigate('portfolio')">
                    <div class="card-title">5. Portfolio</div>
                    <div class="card-text">Create multi-strategy portfolios with exit management (SL/TP/trailing).</div>
                    <span class="btn btn-sm btn-primary">Go to Portfolio</span>
                </div>
                <div class="card nav-card" onclick="App.navigate('portfolio_tearsheet')">
                    <div class="card-title">6. Portfolio Tearsheet</div>
                    <div class="card-text">Analyze combined portfolio performance, per-strategy breakdown.</div>
                    <span class="btn btn-sm btn-primary">Go to Tearsheet</span>
                </div>
            </div>

            <div class="page-divider"></div>
            <h2 class="section-title">Catalog Status</h2>
            <div id="catalog-status">
                <div class="alert alert-info">Checking catalog...</div>
            </div>
        `;

        this.loadCatalogStatus();
    },

    /** Refresh catalog status on every nav-in (cheap call, may have changed
     *  while the user was on the Load Data tab). Static page chrome above is
     *  kept in the DOM so the user sees it immediately. */
    onShow(container, { firstVisit }) {
        if (firstVisit) return;  // already loaded by render()
        this.loadCatalogStatus();
    },

    async loadCatalogStatus() {
        const statusDiv = document.getElementById("catalog-status");
        try {
            const data = await App.api("/api/catalog/status");
            if (data.exists) {
                if (data.data_types && data.data_types.length > 0) {
                    statusDiv.innerHTML = `
                        <div class="alert alert-success">Catalog loaded successfully.</div>
                        <div class="card">
                            <pre style="color: var(--text-secondary); font-size: 0.85rem; white-space: pre-wrap;">${JSON.stringify(data.data_types, null, 2)}</pre>
                        </div>`;
                } else {
                    statusDiv.innerHTML = '<div class="alert alert-info">Catalog is empty. Go to <strong>Load Data</strong> to get started.</div>';
                }
            } else {
                statusDiv.innerHTML = '<div class="alert alert-info">No catalog found. Go to <strong>Load Data</strong> to create one.</div>';
            }
        } catch (e) {
            statusDiv.innerHTML = `<div class="alert alert-danger">Failed to check catalog: ${e.message}</div>`;
        }
    },
};
