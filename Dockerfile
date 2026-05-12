# syntax=docker/dockerfile:1.7
# ---------------------------------------------------------------------------
# M_Cube backtesting / dashboard image
#
# Build (from project root):
#   DOCKER_BUILDKIT=1 docker build -t mcube:latest .
#
# Run:
#   docker run --rm -p 5000:5000 \
#     -v "$PWD/catalog:/app/catalog" \
#     -v "$PWD/reports:/app/reports" \
#     -v "$PWD/custom_strategies:/app/custom_strategies" \
#     mcube:latest
#
# Why multi-stage:
#   Stage 1 ("builder") creates an isolated venv with all wheels and
#   discards pip's wheel cache + dist-info bloat. Stage 2 copies only
#   the finished venv + application code into a clean slim base, so the
#   runtime image carries no build tools, no pip cache, no source
#   tarballs — just what the app needs to run.
# ---------------------------------------------------------------------------


# ===========================================================================
# Phase 1 — Builder: resolve and install all Python dependencies
# ===========================================================================
# python:3.12-slim-bookworm is the sweet spot:
#   * glibc-based (Debian) so manylinux wheels for numpy/scipy/pyarrow/
#     nautilus_trader install instantly — alpine would force from-source
#     builds that are huge and slow.
#   * "slim" strips docs, tests, and headers — ~45 MB vs ~370 MB full image.
#   * 3.12 is current, has wheels for every pinned dependency, and matches
#     the ">= 3.11" requirement enforced by start.bat.
FROM python:3.12-slim-bookworm AS builder

# Fail fast on any pipe stage; surface real errors during apt/pip work.
SHELL ["/bin/sh", "-eu", "-c"]

# Build-time env:
#   PIP_NO_CACHE_DIR    — do not write a wheel cache we'd then have to discard.
#   PIP_DISABLE_*_CHECK — silence noise / shave a network round-trip.
#   PYTHONDONTWRITEBYTECODE — don't litter the venv with .pyc files; the
#                             runtime stage will compile them in one shot.
ENV PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_ROOT_USER_ACTION=ignore \
    PYTHONDONTWRITEBYTECODE=1

# Isolated venv keeps the runtime copy trivial (one COPY of /opt/venv) and
# guarantees we don't pull in anything from the system site-packages.
RUN python -m venv /opt/venv
ENV PATH="/opt/venv/bin:${PATH}"

WORKDIR /build

# Copy ONLY requirements first so this layer is cached across code edits.
# Touching server.py won't bust the (slow) pip install layer.
COPY requirements.txt ./

# --mount=type=cache speeds up rebuilds dramatically without persisting the
# cache into the final image. --prefer-binary forces wheels (no surprise
# from-source builds on a slim image with no compiler).
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip setuptools wheel \
 && pip install --prefer-binary -r requirements.txt


# ===========================================================================
# Phase 2 — Runtime: minimal image that just runs the server
# ===========================================================================
FROM python:3.12-slim-bookworm AS runtime

# Runtime env:
#   PYTHONUNBUFFERED       — flush stdout/stderr immediately so `docker logs`
#                            shows output in real time.
#   PYTHONDONTWRITEBYTECODE — read-only-friendly; bytecode is precompiled below.
#   PYTHONFAULTHANDLER     — print Python tracebacks on segfault (helpful for
#                            native crashes inside nautilus_trader / pyarrow).
#   PIP_NO_CACHE_DIR       — defensive in case anyone pip-installs at runtime.
#   TZ                     — predictable timestamps in reports / logs.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONFAULTHANDLER=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    TZ=UTC \
    PATH="/opt/venv/bin:${PATH}"

# Tiny set of *runtime* OS deps:
#   tini       — proper PID 1 / signal forwarding so Ctrl-C and `docker stop`
#                actually terminate the Flask process cleanly.
#   ca-certs   — required by `requests` / `urllib` for TLS.
#   tzdata     — ensures the TZ env var resolves to a real zone.
#   libgomp1   — OpenMP runtime needed by numpy/scipy/pyarrow wheels.
# We deliberately skip build-essential, gcc, headers — none of the pinned
# wheels need them at runtime.
RUN apt-get update \
 && apt-get install --no-install-recommends -y \
        tini \
        ca-certificates \
        tzdata \
        libgomp1 \
 && rm -rf /var/lib/apt/lists/* /var/cache/apt/archives/*

# Non-root user. Running Flask as root inside a container is a needless
# privilege; if a vulnerability lets an attacker write files, we'd rather
# they hit a non-root home than /etc.
ARG APP_UID=10001
ARG APP_GID=10001
RUN groupadd --system --gid "${APP_GID}" app \
 && useradd  --system --uid "${APP_UID}" --gid app --home /app --shell /sbin/nologin app

# Copy the prebuilt venv from the builder. This is the ONLY thing that
# survives from stage 1, so all the apt/pip scratch space is left behind.
COPY --from=builder --chown=app:app /opt/venv /opt/venv

WORKDIR /app

# Copy application source. Ordered roughly by churn (low → high) so that
# editing strategies / server.py doesn't invalidate the heavier layers.
# Anything listed in .dockerignore (venv/, reports/, __pycache__, etc.) is
# excluded automatically.
COPY --chown=app:app config/            ./config/
COPY --chown=app:app catalog/           ./catalog/
COPY --chown=app:app core/              ./core/
COPY --chown=app:app strategies/        ./strategies/
COPY --chown=app:app custom_strategies/ ./custom_strategies/
COPY --chown=app:app portfolios/        ./portfolios/
COPY --chown=app:app adapter_admin/     ./adapter_admin/
COPY --chown=app:app scripts/           ./scripts/
COPY --chown=app:app static/            ./static/
COPY --chown=app:app server.py          ./server.py

# Pre-create writable runtime dirs (bind-mount targets at `docker run` time).
# Doing it here means a fresh container without volumes still works.
RUN mkdir -p /app/reports /app/catalog /app/custom_strategies \
 && chown -R app:app /app/reports /app/catalog /app/custom_strategies

# Compile the entire site-packages + app to .pyc up front. Eats ~10 s of
# build time once, saves cold-start latency on every container start.
RUN python -m compileall -q /opt/venv /app || true

USER app

EXPOSE 5000

# Volumes for the things that change between runs / shouldn't bloat the image.
VOLUME ["/app/reports", "/app/catalog", "/app/custom_strategies"]

# Lightweight HTTP healthcheck. Uses stdlib (no curl needed in the image).
# 30s start period gives Flask + nautilus imports time to load.
HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
    CMD python -c "import urllib.request,sys; \
sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:5000/', timeout=3).status < 500 else 1)" \
    || exit 1

# tini reaps zombies and forwards SIGTERM/SIGINT to Flask so graceful
# shutdown actually works. Exec form (no shell) keeps Python as PID-of-app.
ENTRYPOINT ["/usr/bin/tini", "--"]
CMD ["python", "server.py"]
