# syntax=docker/dockerfile:1.7
# Multi-stage build for the Docusaurus documentation site.
#
# Stage 1 (builder) runs `npm ci` inside the container to produce a clean,
# platform-independent node_modules — so we never bake the host's node_modules
# into the image. Stage 2 (runtime) copies node_modules + source from the
# builder and runs the dev server.

# -----------------------------------------------------------------------------
# Stage 1: builder — produce /app/node_modules via `pnpm install`
#
# We use pnpm (via corepack, shipped with node:22) instead of npm because
# pnpm's content-addressable store avoids the OOM kills that `npm ci`
# historically hits on constrained Docker hosts for this workspace.
# -----------------------------------------------------------------------------
FROM node:22-slim AS builder

WORKDIR /app

# Install ca-certificates + the HarmonyCares / Zscaler root CAs so pnpm can
# talk to the npm registry through the Zscaler SSL-inspection proxy.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY deploy/compose/images/4icli/zscaler-root-ca.crt /usr/local/share/ca-certificates/
COPY deploy/compose/images/4icli/zscaler-root.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

# Activate the pnpm shim bundled with Node 22 via corepack.
RUN corepack enable pnpm

# Copy manifest first so the install layer is cached independently of
# any downstream source edits.
COPY docs/package.json ./

# pnpm install (no --frozen-lockfile because docs/ is gitignored and we do
# not ship a pnpm-lock.yaml in the repo; each build resolves fresh).
RUN --mount=type=cache,target=/root/.local/share/pnpm/store \
    pnpm install --prod=false

# -----------------------------------------------------------------------------
# Stage 2: runtime — copy node_modules + source, run dev server
# -----------------------------------------------------------------------------
FROM node:22-slim AS runtime

LABEL org.opencontainers.image.source=https://github.com/acoharmony/aco \
      aco.type="gui" \
      aco.description="Docusaurus documentation site for ACOHarmony"

# Same Zscaler trust setup as the builder stage — corepack reaches out to
# the npm registry to resolve pnpm's package manager version at runtime,
# and on hosts sitting behind Zscaler those requests fail TLS verification
# without the extra root CAs.
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY deploy/compose/images/4icli/zscaler-root-ca.crt /usr/local/share/ca-certificates/
COPY deploy/compose/images/4icli/zscaler-root.crt /usr/local/share/ca-certificates/
RUN update-ca-certificates
ENV NODE_EXTRA_CA_CERTS=/etc/ssl/certs/ca-certificates.crt

# Activate pnpm in the runtime image as well, so `pnpm start` works inside
# containers (e.g. for `docs-builder` which runs `pnpm run build`).
RUN corepack enable pnpm

WORKDIR /home/care/acoharmony/docs

# Copy the fully-resolved node_modules from the builder stage.
COPY --from=builder --chown=1002:1002 /app/node_modules ./node_modules

# Copy the Docusaurus source and config. Volume mounts in docs.yml override
# these at runtime for hot-reload during development; baking them in also
# lets the image run standalone (e.g. for `docs-builder` production builds).
COPY --chown=1002:1002 docs/package.json ./
COPY --chown=1002:1002 docs/docusaurus.config.ts docs/sidebars.ts \
     docs/babel.config.js docs/tsconfig.json ./
COPY --chown=1002:1002 docs/docs ./docs
COPY --chown=1002:1002 docs/src ./src

RUN mkdir -p .docusaurus build \
    && chown -R 1002:1002 /home/care

ENV HOME=/home/care \
    NODE_ENV=development \
    PATH="/home/care/acoharmony/docs/node_modules/.bin:${PATH}"

USER 1002:1002
EXPOSE 3000
CMD ["pnpm", "start"]
