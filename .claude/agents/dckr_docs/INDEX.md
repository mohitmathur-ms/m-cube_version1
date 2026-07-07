# dckr knowledge-base index

This file maps each **Docker / containerization topic** to the docs that cover
it. `dckr` reads this file first to choose which directory/file to open for a
question.

## Source & scope

This knowledge base is a **curated subset of the official `docker/docs`
repository** (the Markdown behind docs.docker.com). The full repo (~790 MB,
mostly site tooling + git history) was pruned down to the ~400 Markdown pages
that matter for **containerizing an m-cube-like Python / Flask / NautilusTrader
system on a single host**. Orchestration (Swarm/Kubernetes), SaaS/account/
billing, Docker Scout, Hardened Images (DHI), AI/GenAI guides, non-Python
language guides, and all Hugo build machinery were dropped.

All files are `.md` (grep-able). Pages may contain Hugo shortcodes like
`{{< include ... >}}` referencing files under `includes/` — read for meaning.

## Topic → location

| # | Topic | Path | What's there |
|---|-------|------|--------------|
| 1  | **Get started / concepts** | `get-started/` | Docker fundamentals: what an image/container/volume/network is, the build→run loop, overview. Start here for vocabulary. |
| 2  | **Python app guide** ⭐ | `guides/python/` | The most directly applicable guide: `containerize.md`, `develop.md`, `deploy.md`, `configure-github-actions.md`, `lint-format-typing.md`, `secure-supply-chain.md`. Maps a Python app → image → compose → CI. |
| 3  | **Compose guides** | `guides/docker-compose/`, `guides/compose-bake/`, `guides/lab-compose-quickstart.md` | How/why to use Compose, setup, common questions. |
| 4  | **Databases / data services** | `guides/databases.md` | Running stateful/data services in containers (patterns reusable for the catalog). |
| 5  | **Compose manual** | `compose/` | Full Compose usage: multi-service apps, environment, profiles, startup order, lifecycle. |
| 6  | **Build / Dockerfile** | `build/` | Authoring Dockerfiles and building images: `concepts/`, `building/`, `cache/`, `ci/`, `images/`, `buildkit/`, `bake/`, multi-stage, build checks. |
| 7  | **Compose file reference** ⭐ | `reference/compose-file/` | The compose-file spec field-by-field: `services.md`, `volumes.md`, `networks.md`, `secrets.md`, `configs.md`, `deploy.md`, `develop.md`, `build.md`, `profiles.md`, interpolation. |
| 8  | **CLI reference** | `reference/cli/`, `reference/glossary.md` | `docker` / `docker compose` command reference and terminology. |
| 9  | **Engine: containers** ⭐ | `engine/containers/` | `resource_constraints.md` (CPU/memory — pair with the ProcessPoolExecutor workers), `gpu.md`, `start-containers-automatically.md` (restart policies), `runmetrics.md`, `multi-service_container.md`. |
| 10 | **Engine: storage** ⭐ | `engine/storage/` | `volumes.md`, `bind-mounts.md`, `tmpfs.md`, drivers — persisting `./catalog/` and bind-mounting host CSV data paths. |
| 11 | **Engine: network** | `engine/network/` | `port-publishing.md` (expose Flask 5000 + adapter-admin port), drivers, links, firewall. |
| 12 | **Engine: logging** | `engine/logging/` | Log drivers and configuration for container stdout/stderr. |
| 13 | **Engine: daemon** | `engine/daemon/` | dockerd config: proxy, remote access, live-restore, prometheus, troubleshoot. |
| 14 | **Engine: resources & ops** | `engine/manage-resources/`, `engine/cli/`, `engine/install/`, `engine/security/` | Contexts, labels, pruning; CLI config; Linux engine install; rootless/security. |
| 15 | **Docker Desktop (Windows/WSL2)** | `desktop/` | Windows-first relevant pages only: `setup/`, `features/` (incl. WSL & networking), `settings-and-maintenance/`, `use-desktop/`, `troubleshoot-and-support/`. (Mac/previous-versions/enterprise pages were dropped.) |
| 16 | **Includes (shortcode snippets)** | `includes/` | Reusable fragments embedded by the pages above — usually not read directly. |

⭐ = highest-signal for the m-cube containerization use case.

## How to navigate

- **"How do I containerize the Flask app?"** → `guides/python/containerize.md` + `build/concepts/`.
- **"Persist the catalog / mount D:\ data?"** → `engine/storage/volumes.md`, `engine/storage/bind-mounts.md`, `reference/compose-file/volumes.md`.
- **"CPU/memory for the backtest workers?"** → `engine/containers/resource_constraints.md`.
- **"Expose port 5000 / adapter-admin port?"** → `engine/network/port-publishing.md`, `reference/compose-file/services.md`.
- **"Inject `_USE_*` flags / secrets?"** → `reference/compose-file/secrets.md`, `compose/` (environment), `guides/python/develop.md`.
- **"Build & push image in CI?"** → `guides/python/configure-github-actions.md`, `build/ci/`.
