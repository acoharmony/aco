# © 2025 HarmonyCares
# All rights reserved.

"""
Resolve service → image mapping from the compose config.

Used by ``start``/``restart`` to decide which services need a pull
based on recorded vs. current release tag. Filters to acoharmony images
only — third-party base images are out of scope for the freshness check.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

ACOHARMONY_IMAGE_PREFIX = "ghcr.io/acoharmony/"


def service_images(compose_file: Path) -> dict[str, str]:
    """
    Return ``{service: image_repo}`` for acoharmony images only.

    ``image_repo`` is the bare ``ghcr.io/acoharmony/<name>`` (no tag),
    since the deploy state is keyed per-image and the tag is what we're
    comparing.
    """
    result = subprocess.run(
        ["docker", "compose", "-f", str(compose_file), "config", "--format", "json"],
        capture_output=True,
        text=True,
        check=True,
    )
    config = json.loads(result.stdout)
    out: dict[str, str] = {}
    for service_name, service_def in (config.get("services") or {}).items():
        image = service_def.get("image")
        if not isinstance(image, str):
            continue
        if not image.startswith(ACOHARMONY_IMAGE_PREFIX):
            continue
        repo, _, _ = image.partition(":")
        out[service_name] = repo
    return out
