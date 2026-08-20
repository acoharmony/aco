# © 2025 HarmonyCares
# All rights reserved.

"""Runtime environment helpers for deployment subprocesses."""

from __future__ import annotations

import os
from pathlib import Path


def docker_environment() -> dict[str, str]:
    """Return an environment that points Docker at the active user daemon."""
    env = os.environ.copy()
    if env.get("DOCKER_HOST"):
        return env

    runtime_dir = env.get("XDG_RUNTIME_DIR")
    candidates = []
    if runtime_dir:
        candidates.append(Path(runtime_dir) / "docker.sock")
    candidates.append(Path(f"/run/user/{os.getuid()}/docker.sock"))

    for socket in candidates:
        if socket.exists():
            env["DOCKER_HOST"] = f"unix://{socket}"
            break
    return env
