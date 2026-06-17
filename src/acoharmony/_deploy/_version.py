# © 2025 HarmonyCares
# All rights reserved.

"""
Latest-release-tag resolver for image freshness checks.

The release pipeline tags the repo (vX.Y.Z) and `.dev/release-images.sh`
publishes container images at that tag. ``latest_release_tag`` resolves
the newest release tag from the configured remote so a stale checkout
doesn't convince deploys that an old image is current.

Returns ``None`` when no git checkout is reachable (e.g. running from an
installed wheel) — caller must skip the freshness check and warn.
"""

from __future__ import annotations

import os
import re
import subprocess
from collections.abc import Iterable
from pathlib import Path

_RELEASE_TAG_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
_GIT_TIMEOUT_SECONDS = 10


def _find_git_root(start: Path) -> Path | None:
    """Walk upward from ``start`` looking for a ``.git`` directory."""
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def _run_git(repo: Path, args: list[str]) -> str | None:
    """Run a git command in ``repo`` and return stdout on success."""
    env = {**os.environ, "GIT_TERMINAL_PROMPT": "0"}
    try:
        result = subprocess.run(
            ["git", *args],
            cwd=str(repo),
            capture_output=True,
            text=True,
            check=False,
            timeout=_GIT_TIMEOUT_SECONDS,
            env=env,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    return result.stdout


def _release_tag_key(tag: str) -> tuple[int, int, int] | None:
    match = _RELEASE_TAG_RE.fullmatch(tag.strip())
    if match is None:
        return None
    return tuple(int(part) for part in match.groups())


def _newest_release_tag(tags: Iterable[str]) -> str | None:
    release_tags = [
        (key, tag.strip()) for tag in tags if (key := _release_tag_key(tag.strip())) is not None
    ]
    if not release_tags:
        return None
    return max(release_tags, key=lambda item: item[0])[1]


def _remote_release_tags(repo: Path, remote: str = "origin") -> list[str]:
    stdout = _run_git(
        repo,
        ["ls-remote", "--tags", "--refs", remote, "v[0-9]*.[0-9]*.[0-9]*"],
    )
    if stdout is None:
        return []
    tags: list[str] = []
    for line in stdout.splitlines():
        parts = line.split()
        if len(parts) != 2:
            continue
        ref = parts[1]
        if ref.startswith("refs/tags/"):
            tags.append(ref.removeprefix("refs/tags/"))
    return tags


def _local_release_tags(repo: Path) -> list[str]:
    stdout = _run_git(repo, ["tag", "--list", "v[0-9]*.[0-9]*.[0-9]*"])
    if stdout is None:
        return []
    return [line.strip() for line in stdout.splitlines() if line.strip()]


def latest_release_tag() -> str | None:
    """
    Return the most recent ``vX.Y.Z`` tag, or ``None`` if undetermined.

    Uses the remote tag list first so deploys notice published releases
    even when the local checkout is behind. Falls back to local tags when
    the remote is unavailable.
    """
    here = Path(__file__).resolve()
    repo = _find_git_root(here)
    if repo is None:
        return None
    return _newest_release_tag(_remote_release_tags(repo)) or _newest_release_tag(
        _local_release_tags(repo)
    )
