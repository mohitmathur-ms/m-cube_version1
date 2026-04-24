/**
 * Custom Adapters Page - Upload, manage, and validate custom adapter files.
 */

const CustomAdapters = {
    files: [],

    async render(container) {
        container.innerHTML = `
            <h1 class="page-title">&#128196; Custom Adapters</h1>
            <p class="page-subtitle">Upload custom adapter Python files for exchanges not built into NautilusTrader.</p>

            <div class="grid-2">
                <div>
                    <h2 class="section-title">Upload Custom Adapter</h2>
                    <div class="form-group">
                        <label class="form-label">Select a .py adapter file</label>
                        <input type="file" id="custom-adapter-file-input" class="form-control" accept=".py"
                               onchange="CustomAdapters.uploadFile()">
                    </div>
                    <div id="upload-status" style="margin-top: 12px;"></div>

                    <div class="page-divider"></div>

                    <a href="/api/custom_adapters/template" download="custom_adapter_template.py" class="btn btn-sm" style="margin-bottom: 16px;">
                        &#128196; Download Adapter Template
                    </a>
                </div>

                <div>
                    <h2 class="section-title">Uploaded Files</h2>
                    <div id="files-list">
                        <div class="alert alert-info">Loading...</div>
                    </div>
                </div>
            </div>

            <div class="page-divider"></div>

            <div id="guidelines-section"></div>
        `;

        await Promise.all([
            this.loadFiles(),
            this.loadGuidelines(),
        ]);
    },

    async loadFiles() {
        const listDiv = document.getElementById("files-list");

        try {
            const data = await App.api("/api/custom_adapters/list");
            this.files = data.files;

            if (this.files.length === 0) {
                listDiv.innerHTML = '<div class="alert alert-info">No custom adapter files uploaded yet.</div>';
                return;
            }

            let html = "";
            for (const file of this.files) {
                html += `
                    <div class="card" style="margin-bottom: 8px; padding: 12px 16px; display: flex; justify-content: space-between; align-items: center;">
                        <span style="font-size: 0.9rem;">&#128196; ${file}</span>
                        <button class="btn btn-sm btn-danger" onclick="CustomAdapters.deleteFile('${file}')">Delete</button>
                    </div>`;
            }

            listDiv.innerHTML = html;
        } catch (e) {
            listDiv.innerHTML = `<div class="alert alert-danger">Failed to load files: ${e.message}</div>`;
        }
    },

    async uploadFile() {
        const input = document.getElementById("custom-adapter-file-input");
        const statusDiv = document.getElementById("upload-status");

        if (!input.files || input.files.length === 0) return;

        const file = input.files[0];
        statusDiv.innerHTML = '<div class="alert alert-info">Uploading and validating...</div>';

        const formData = new FormData();
        formData.append("file", file);

        try {
            const response = await fetch("/api/custom_adapters/upload", {
                method: "POST",
                body: formData,
            });
            const responseText = await response.text();
            let result;
            try {
                result = JSON.parse(responseText);
            } catch {
                statusDiv.innerHTML = `<div class="alert alert-danger">Invalid response from server</div>`;
                return;
            }

            if (result.success) {
                statusDiv.innerHTML = `<div class="alert alert-success">${result.message}</div>`;
                input.value = "";
                this.loadFiles();
            } else {
                statusDiv.innerHTML = `<div class="alert alert-danger">${result.error || 'Upload failed'}</div>`;
            }
        } catch (e) {
            statusDiv.innerHTML = `<div class="alert alert-danger">Upload failed: ${e.message}</div>`;
        }
    },

    async deleteFile(filename) {
        if (!confirm(`Delete custom adapter file "${filename}"?`)) return;

        try {
            await App.api(`/api/custom_adapters/${filename}`, { method: "DELETE" });
            App.toast(`Deleted ${filename}`, "success");
            this.loadFiles();
        } catch (e) {
            App.toast(`Delete failed: ${e.message}`, "error");
        }
    },

    async loadGuidelines() {
        const section = document.getElementById("guidelines-section");

        try {
            const data = await App.api("/api/custom_adapters/guidelines");
            const guidelines = data.guidelines || "";

            // Simple markdown to HTML
            let html = guidelines
                .replace(/^### (.+)$/gm, '<h4 style="color:var(--accent);margin:16px 0 8px;">$1</h4>')
                .replace(/^## (.+)$/gm, '<h3 style="margin:20px 0 10px;">$1</h3>')
                .replace(/^# (.+)$/gm, '<h2 class="section-title">$1</h2>')
                .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
                .replace(/`([^`]+)`/g, '<code style="background:var(--bg-card);padding:2px 6px;border-radius:3px;font-size:0.85rem;color:var(--accent);">$1</code>')
                .replace(/```(\w*)\n([\s\S]*?)```/g, '<pre style="background:var(--bg-card);border:1px solid var(--border-color);border-radius:8px;padding:14px;margin:12px 0;font-size:0.82rem;overflow-x:auto;"><code>$2</code></pre>')
                .replace(/\|(.+)\|/g, (match) => {
                    // Simple table row handling
                    const cells = match.split("|").filter(c => c.trim());
                    if (cells.every(c => c.trim().match(/^-+$/))) return '';
                    const tag = cells[0].trim().startsWith("---") ? "td" : "td";
                    return '<tr>' + cells.map(c => `<${tag} style="padding:6px 12px;border:1px solid var(--border-color);">${c.trim()}</${tag}>`).join('') + '</tr>';
                })
                .replace(/\n/g, '<br>');

            section.innerHTML = App.accordionHTML("guidelines", "Adapter Development Guidelines", `<div style="font-size:0.88rem;line-height:1.8;">${html}</div>`);
        } catch (e) {
            section.innerHTML = "";
        }
    },
};
