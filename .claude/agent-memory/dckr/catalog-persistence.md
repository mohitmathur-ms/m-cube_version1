---
name: catalog-persistence
description: How m-cube's ParquetDataCatalog path resolves in-container and how to persist it across runs
metadata:
  type: project
---

The NautilusTrader ParquetDataCatalog persistence wiring for m-cube.

**Path resolution (load-bearing):** `server.py:58` sets
`CATALOG_PATH = str(PROJECT_DIR / "catalog")` where `PROJECT_DIR` is derived from
`server.py`'s own location — NOT the CWD. So in the image (`WORKDIR /app`,
`server.py` at `/app/server.py`) the catalog is always **`/app/catalog`**.
`core/nautilus_loader.py:29` has `DEFAULT_CATALOG_PATH = "./catalog"` but the
server always passes its absolute `CATALOG_PATH`. There is **no env var** to
override the catalog path — it's a module constant (only per-request override via
the `catalog_path`/`path` form field). Don't tell users to set a CATALOG env var.

**Dockerfile already aligns** (`d:\m-cube_version1\Dockerfile`): `mkdir -p
/app/catalog` + chown to uid 10001 (lines 150-151), `VOLUME [".../app/catalog"...]`
(line 162), run example bind-mounts `$PWD/catalog:/app/catalog`. Catalog is
intentionally NOT COPYed into the image. Runs as non-root `USER app` (uid/gid 10001).

**Persist via:** mount something durable at `/app/catalog`. Bind mount
(`./catalog:/app/catalog`) is preferred for this project because host workflows
(CSV ingest inspection, `scripts/aggregate_catalog.py`, the filename fast-path in
`server.py::_bar_type_range_from_files`) need the parquet visible as host files.
Named volume is the alternative (faster, portable, but host-opaque).

**Gotchas:** (1) non-root uid 10001 must be able to write a Linux bind-mounted
catalog (chown host dir or use a named volume). (2) Docker Desktop/WSL2 (this is a
Windows-first repo) bind mounts are slow for the catalog's many-small-parquet
layout — prefer named volume or keep project in WSL2 fs. (3) Mount the whole
`/app/catalog`, never sub-paths, to keep the filename fast-path working.

Related: [[dckr-docs-index]]
