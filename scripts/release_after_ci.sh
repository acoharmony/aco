#!/usr/bin/env bash
# Push HEAD, wait for GitHub CI, tag a release, wait for the release workflow,
# then build/push the GHCR images from a clean worktree.
set -euo pipefail

usage() {
    cat <<'EOF'
usage: scripts/release_after_ci.sh [--tag vX.Y.Z] [--branch BRANCH] [--remote REMOTE]
                                   [--skip-images] [--no-push-images] [--from-hook]

Environment:
  ACO_RELEASE_CI_WORKFLOW=CI
  ACO_RELEASE_WORKFLOW=Release
  ACO_RELEASE_BRANCH=main
  ACO_RELEASE_REMOTE=origin
  ACO_RELEASE_NO_PUSH_IMAGES=1
  ACO_RELEASE_SKIP_IMAGES=1
EOF
}

log() { printf '[release] %s\n' "$*"; }
fail() {
    printf '[release] ERROR: %s\n' "$*" >&2
    exit 1
}

require_cmd() {
    command -v "$1" >/dev/null 2>&1 || fail "$1 is required"
}

bool_true() {
    case "${1:-}" in
        1|true|TRUE|yes|YES|on|ON) return 0 ;;
        *) return 1 ;;
    esac
}

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

remote="${ACO_RELEASE_REMOTE:-origin}"
branch="${ACO_RELEASE_BRANCH:-main}"
tag=""
skip_images="${ACO_RELEASE_SKIP_IMAGES:-0}"
no_push_images="${ACO_RELEASE_NO_PUSH_IMAGES:-0}"
from_hook=0

while [ "$#" -gt 0 ]; do
    case "$1" in
        --tag)
            tag="${2:-}"
            [ -n "$tag" ] || fail "--tag requires a value"
            shift 2
            ;;
        --branch)
            branch="${2:-}"
            [ -n "$branch" ] || fail "--branch requires a value"
            shift 2
            ;;
        --remote)
            remote="${2:-}"
            [ -n "$remote" ] || fail "--remote requires a value"
            shift 2
            ;;
        --skip-images)
            skip_images=1
            shift
            ;;
        --no-push-images)
            no_push_images=1
            shift
            ;;
        --from-hook)
            from_hook=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown argument: $1"
            ;;
    esac
done

require_cmd git
require_cmd gh

current_branch="$(git rev-parse --abbrev-ref HEAD)"
if [ "$current_branch" = "HEAD" ]; then
    fail "release must run from a branch, not detached HEAD"
fi

if [ "$current_branch" != "$branch" ]; then
    if [ "$from_hook" -eq 1 ]; then
        log "skipping release hook on $current_branch; configured branch is $branch"
        exit 0
    fi
    fail "current branch is $current_branch, expected $branch"
fi

if [ -n "$(git status --porcelain --untracked-files=no)" ]; then
    log "tracked working tree has uncommitted changes; image build will use a clean release worktree"
fi

head_sha="$(git rev-parse HEAD)"
repo="$(gh repo view --json nameWithOwner --jq .nameWithOwner)"
ci_workflow="${ACO_RELEASE_CI_WORKFLOW:-CI}"
release_workflow="${ACO_RELEASE_WORKFLOW:-Release}"

log "pushing $head_sha to $remote/$branch"
if git rev-parse --abbrev-ref --symbolic-full-name '@{u}' >/dev/null 2>&1; then
    git push
else
    git push -u "$remote" "$branch"
fi

find_run_id() {
    local workflow="$1"
    local jq_filter="$2"
    local run_id=""
    for _ in $(seq 1 60); do
        run_id="$(
            gh run list \
                --repo "$repo" \
                --workflow "$workflow" \
                --limit 20 \
                --json databaseId,headSha,headBranch,status,conclusion \
                --jq "$jq_filter" \
                | head -n 1
        )"
        if [ -n "$run_id" ]; then
            printf '%s\n' "$run_id"
            return 0
        fi
        sleep 5
    done
    return 1
}

log "waiting for $ci_workflow workflow on $head_sha"
ci_run_id="$(find_run_id "$ci_workflow" ".[] | select(.headSha == \"$head_sha\") | .databaseId" \
    || true)"
[ -n "$ci_run_id" ] || fail "could not find $ci_workflow run for $head_sha"
gh run watch "$ci_run_id" --repo "$repo" --exit-status

git fetch "$remote" --tags

next_patch_tag() {
    local latest version major minor patch
    latest="$(git tag --list 'v[0-9]*.[0-9]*.[0-9]*' --sort=-v:refname | head -n 1)"
    [ -n "$latest" ] || {
        printf 'v0.0.1\n'
        return 0
    }
    version="${latest#v}"
    IFS=. read -r major minor patch <<EOF
$version
EOF
    printf 'v%s.%s.%s\n' "$major" "$minor" "$((patch + 1))"
}

if [ -z "$tag" ]; then
    tag="$(next_patch_tag)"
fi

[[ "$tag" =~ ^v[0-9]+\.[0-9]+\.[0-9]+$ ]] || fail "tag must match vX.Y.Z: $tag"

if git rev-parse -q --verify "refs/tags/$tag" >/dev/null; then
    fail "tag already exists locally: $tag"
fi

if git ls-remote --exit-code --tags "$remote" "refs/tags/$tag" >/dev/null 2>&1; then
    fail "tag already exists on $remote: $tag"
fi

log "creating annotated release tag $tag"
git tag --annotate --no-sign "$tag" -m "release: $tag"

log "pushing $tag"
git push "$remote" "$tag"

log "waiting for $release_workflow workflow on $tag"
release_run_id="$(find_run_id "$release_workflow" ".[] | select(.headBranch == \"$tag\") | .databaseId" \
    || true)"
[ -n "$release_run_id" ] || fail "could not find $release_workflow run for $tag"
gh run watch "$release_run_id" --repo "$repo" --exit-status

if bool_true "$skip_images"; then
    log "skip-images enabled; release complete without image build"
    exit 0
fi

require_cmd docker
require_cmd rsync

build_root="$(mktemp -d "${TMPDIR:-/tmp}/acoharmony-release-${tag}.XXXXXX")"
cleanup() {
    git worktree remove --force "$build_root" >/dev/null 2>&1 || rm -rf "$build_root"
}
trap cleanup EXIT

log "creating clean image build worktree at $build_root"
git worktree add --detach "$build_root" "$tag"

if [ -f "$repo_root/docs/package.json" ]; then
    log "copying ignored docs assets into image build worktree"
    mkdir -p "$build_root/docs"
    rsync -a --delete \
        --exclude node_modules \
        --exclude .docusaurus \
        --exclude build \
        "$repo_root/docs/" "$build_root/docs/"
fi

if [ -f "$repo_root/deploy/images/4icli/4icli" ]; then
    log "copying ignored 4icli binary into image build worktree"
    cp -p "$repo_root/deploy/images/4icli/4icli" \
        "$build_root/deploy/images/4icli/4icli"
fi

image_args=("$tag")
if bool_true "$no_push_images"; then
    image_args+=("--no-push")
fi

log "building release images with .dev/release-images.sh ${image_args[*]}"
(
    cd "$build_root"
    ./.dev/release-images.sh "${image_args[@]}"
)

log "release complete: $tag"
