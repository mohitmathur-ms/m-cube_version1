---
name: dckr-docs-index
description: Where the dckr knowledge base lives and which docs cover which container topics
metadata:
  type: reference
---

The dckr knowledge base (curated subset of official docker/docs Markdown) lives at
`d:\m-cube_version1\.claude\agents\dckr_docs\`. Always read `INDEX.md` there first.

High-signal mappings confirmed in use:
- Persist catalog / mount host data → `engine/storage/volumes.md`,
  `engine/storage/bind-mounts.md`, `reference/compose-file/volumes.md`,
  `reference/compose-file/services.md` (volumes short/long syntax ~lines 2162-2212,
  `create_host_path`).
- CPU/mem for ProcessPoolExecutor backtest workers → `engine/containers/resource_constraints.md`.
- Expose Flask 5000 / adapter-admin port → `engine/network/port-publishing.md`.
- Containerize Flask app → `guides/python/containerize.md`, `build/concepts/`.
- Secrets / _USE_* env flags → `reference/compose-file/secrets.md`, `compose/`.

There is NO docker-compose.yml in the repo yet (as of 2026-06-29); the only
Dockerfile is `d:\m-cube_version1\Dockerfile` (multi-stage, python:3.12-slim).
