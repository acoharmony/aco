# syntax=docker/dockerfile:1.7
# Multi-stage build for the Marimo notebook runtime.
#
# Stage 1 (builder) produces a platform-independent .venv from the wheel
# via `uv sync --no-editable`. Stage 2 (runtime) copies only the venv and
# the marimo config/entrypoint, so the final image contains no source tree,
# no .git, and no editable egg-link — only the installed wheel.

# -----------------------------------------------------------------------------
# Stage 1: builder — produce /opt/venv from a non-editable install
# -----------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS builder

# hatch-vcs normally reads the version from `.git/`, but .git is excluded
# from the build context (performance + cleanliness). Instead, the caller
# passes the version explicitly via --build-arg ACOHARMONY_VERSION=X.Y.Z,
# which is forwarded to hatch-vcs through SETUPTOOLS_SCM_PRETEND_VERSION.
ARG ACOHARMONY_VERSION=0.0.0+local

ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    SETUPTOOLS_SCM_PRETEND_VERSION=${ACOHARMONY_VERSION}

WORKDIR /src

# Copy only the files `uv sync` needs to resolve & build the wheel.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src

# --no-editable ensures the venv contains a built copy of the wheel instead
# of an egg-link pointing back at /src. --no-dev skips test/lint deps.
# --extra full pulls in marimo itself plus duckdb, pypdf, opentelemetry, etc.
# which the notebooks depend on at runtime.
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-editable --no-dev --extra full

# -----------------------------------------------------------------------------
# Stage 2: runtime — minimal image with only the built venv
# -----------------------------------------------------------------------------
FROM ghcr.io/astral-sh/uv:python3.13-bookworm-slim AS runtime

LABEL org.opencontainers.image.source=https://github.com/acoharmony/aco \
      aco.type="gui" \
      aco.description="Marimo notebook server with acoharmony pre-installed"

# Create care group + user with IDs matching the host.
RUN groupadd -g 1002 care \
    && groupadd -g 989 docker \
    && useradd -m -u 1002 -g 1002 -G 989 -s /bin/bash care \
    && mkdir -p /home/care/.config/marimo /home/care/.cache/uv \
    && chown -R 1002:1002 /home/care

# Marimo config (custom homepage + HarmonyCares theme).
COPY --chown=1002:1002 deploy/compose/conf/marimo/marimo.toml \
     /home/care/.config/marimo/marimo.toml

# Entrypoint that injects the custom homepage into marimo's index.html.
COPY --chown=1002:1002 deploy/compose/images/marimo/entrypoint.sh \
     /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Copy the prebuilt venv from the builder stage.
COPY --from=builder --chown=1002:1002 /opt/venv /opt/venv

ENV PATH="/opt/venv/bin:${PATH}" \
    VIRTUAL_ENV=/opt/venv \
    HOME=/home/care

USER care
WORKDIR /opt/s3/data/notebooks
EXPOSE 10012

ENTRYPOINT ["entrypoint.sh"]
