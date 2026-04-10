#!/usr/bin/env bash
# Build and push the ACOHarmony container images to GHCR.
#
# Runs LOCALLY (not in CI), because the docs/ content lives only on the
# developer workstation and is intentionally not tracked in git. The release
# GitHub Actions workflow ships the wheel; this script ships the images.
#
# Usage:
#     .dev/release-images.sh v0.0.2
#     .dev/release-images.sh v0.0.2 --no-push    # build only, skip push
#
# Each image is tagged both with the version (e.g. v0.0.2) and :latest.
set -euo pipefail

# ---------------------------------------------------------------------------
# Arguments
# ---------------------------------------------------------------------------
if [ "$#" -lt 1 ]; then
    echo "usage: $0 <version-tag> [--no-push]" >&2
    echo "       e.g. $0 v0.0.2" >&2
    exit 2
fi

VERSION="$1"
PUSH=1
if [ "${2:-}" = "--no-push" ]; then
    PUSH=0
fi

if [[ ! "$VERSION" =~ ^v[0-9]+\.[0-9]+\.[0-9]+ ]]; then
    echo "error: version must match vX.Y.Z (got: $VERSION)" >&2
    exit 2
fi

# ---------------------------------------------------------------------------
# Paths — script lives in .dev/, repo root is its parent
# ---------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

# ---------------------------------------------------------------------------
# Image definitions
# ---------------------------------------------------------------------------
# Each entry: "<ghcr-repo>|<dockerfile>|<context>"
# Context is the repo root for multi-stage builds that need src/; for the
# self-contained 4icli image, context is its own subdirectory.
IMAGES=(
    "ghcr.io/acoharmony/marimo|deploy/compose/images/marimo/Dockerfile|."
    "ghcr.io/acoharmony/docusaurus|deploy/compose/images/docs/Dockerfile|."
    "ghcr.io/acoharmony/4icli|Dockerfile|deploy/compose/images/4icli"
)

# ---------------------------------------------------------------------------
# Sanity checks
# ---------------------------------------------------------------------------
if ! command -v docker >/dev/null 2>&1; then
    echo "error: docker is not on PATH" >&2
    exit 1
fi

# docs/ must exist locally since it's gitignored and required by the docs image.
if [ ! -f "$REPO_ROOT/docs/package.json" ]; then
    echo "error: docs/package.json not found — docs/ is gitignored and must" >&2
    echo "       exist on disk for the docusaurus image build." >&2
    exit 1
fi

# Warn loudly if we'd be pushing over an existing remote tag.
if [ "$PUSH" -eq 1 ]; then
    for entry in "${IMAGES[@]}"; do
        repo="${entry%%|*}"
        if docker manifest inspect "${repo}:${VERSION}" >/dev/null 2>&1; then
            echo "warning: ${repo}:${VERSION} already exists in GHCR — it will be overwritten" >&2
        fi
    done
fi

# ---------------------------------------------------------------------------
# Build & tag
# ---------------------------------------------------------------------------
for entry in "${IMAGES[@]}"; do
    repo="${entry%%|*}"
    rest="${entry#*|}"
    dockerfile="${rest%%|*}"
    context="${rest#*|}"

    echo
    echo "==> Building ${repo}:${VERSION}"
    echo "    dockerfile: ${dockerfile}"
    echo "    context:    ${context}"

    # Strip the leading "v" from the tag — hatch-vcs wants a PEP 440 version.
    # Passed via --build-arg so builder stages can forward it to
    # SETUPTOOLS_SCM_PRETEND_VERSION_FOR_ACOHARMONY (ignored by images that
    # don't declare the ARG, so harmless).
    docker build \
        --file "$dockerfile" \
        --build-arg "ACOHARMONY_VERSION=${VERSION#v}" \
        --tag "${repo}:${VERSION}" \
        --tag "${repo}:latest" \
        "$context"
done

# ---------------------------------------------------------------------------
# Push
# ---------------------------------------------------------------------------
if [ "$PUSH" -eq 0 ]; then
    echo
    echo "--no-push given; skipping docker push."
    exit 0
fi

echo
echo "==> Pushing images to GHCR"
for entry in "${IMAGES[@]}"; do
    repo="${entry%%|*}"
    docker push "${repo}:${VERSION}"
    docker push "${repo}:latest"
done

echo
echo "Done. Released ${VERSION} for:"
for entry in "${IMAGES[@]}"; do
    repo="${entry%%|*}"
    echo "  - ${repo}"
done
