# © 2025 HarmonyCares
# All rights reserved.

"""
Latest-release-tag resolver for image freshness checks.

The release pipeline tags the repo (vX.Y.Z) and `.dev/release-images.sh`
publishes container images at that tag. ``latest_release_tag`` returns
that tag from the in-tree ``.git`` so the deploy machinery can decide
whether the locally-cached image is stale.

Returns ``None`` when no git checkout is reachable (e.g. running from an
installed wheel) — caller must skip the freshness check and warn.
"""

from __future__ import annotations

import subprocess
from pathlib import Path


def _find_git_root(start: Path) -> Path | None:
    """Walk upward from ``start`` looking for a ``.git`` directory."""
    for candidate in (start, *start.parents):
        if (candidate / ".git").exists():
            return candidate
    return None


def latest_release_tag() -> str | None:
    """
    Return the most recent ``vX.Y.Z`` tag, or ``None`` if undetermined.

    Uses ``git describe --tags --abbrev=0`` for the repo containing this
    module. Returns ``None`` when no ``.git`` is reachable or git itself
    fails — callers warn and continue without a freshness check.
    """
    here = Path(__file__).resolve()
    repo = _find_git_root(here)
    if repo is None:
        return None
    try:
        result = subprocess.run(
            ["git", "describe", "--tags", "--abbrev=0"],
            cwd=str(repo),
            capture_output=True,
            text=True,
            check=False,
        )
    except FileNotFoundError:
        return None
    if result.returncode != 0:
        return None
    tag = result.stdout.strip()
    return tag or None
