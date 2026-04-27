# © 2025 HarmonyCares
# All rights reserved.

"""
Pull-on-stale image freshness check for ``aco deploy start/restart``.

Compares the recorded deployed version (per-image, in the deploy state
file) against the current git release tag, pulls only the images whose
recorded version differs (or that have no record), and updates state
after a successful pull. ``force=True`` pulls every acoharmony image
regardless of state.

Falls back to a warn-and-skip when the release tag can't be determined
(e.g. running from an installed wheel) — caller can pass ``--pull`` to
force a full refresh in that case.
"""

from __future__ import annotations

from pathlib import Path

from ._docker import DockerComposeManager
from ._images import service_images
from ._state import DeployStateTracker
from ._version import latest_release_tag


def ensure_latest_images(
    docker: DockerComposeManager,
    tracker: DeployStateTracker,
    services: list[str],
    force: bool = False,
) -> int:
    """
    Pull acoharmony images for ``services`` whose recorded version is
    stale relative to the current git tag (or all of them if ``force``).

    Returns 0 on success, non-zero if a pull failed. A non-acoharmony
    service (no ``ghcr.io/acoharmony/`` image) is silently skipped — the
    freshness check is per-image, not per-service.
    """
    image_map = service_images(docker.compose_file)
    targeted = {svc: image_map[svc] for svc in services if svc in image_map}
    if not targeted:
        return 0

    tag = latest_release_tag()
    if tag is None and not force:
        print(
            "[WARN] Could not determine latest release tag (running outside a "
            "git checkout?). Skipping image freshness check; pass --pull to "
            "force a refresh."
        )
        return 0

    if force:
        to_pull = list(targeted.keys())
        print(f"Force-pulling {len(to_pull)} image(s): {', '.join(to_pull)}")
    else:
        to_pull = []
        for svc, repo in targeted.items():
            record = tracker.get(repo)
            if record is None or record.version != tag:
                to_pull.append(svc)
        if not to_pull:
            print(f"All images already at {tag}; skipping pull.")
            return 0
        print(f"Pulling {len(to_pull)} stale image(s) for {tag}: {', '.join(to_pull)}")

    result = docker.pull(to_pull)
    if result.returncode != 0:
        print("[ERROR] Image pull failed.")
        if result.stderr:
            print(result.stderr)
        return result.returncode

    # Update state for each successfully-pulled service. When force was
    # used without a known tag, we record nothing (no version to anchor).
    if tag is not None:
        for svc in to_pull:
            tracker.record(targeted[svc], tag)

    return 0


def deploy_state_tracker() -> DeployStateTracker:
    """Build a ``DeployStateTracker`` rooted at the project tracking dir."""
    from .._store import StorageBackend

    storage = StorageBackend()
    tracking_dir = Path(storage.get_path("logs")) / "tracking"
    return DeployStateTracker(state_dir=tracking_dir)
