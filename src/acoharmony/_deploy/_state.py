# © 2025 HarmonyCares
# All rights reserved.

"""
State tracker for deployed image versions.

Records the release tag last pulled+deployed for each acoharmony image,
keyed by image repo (``ghcr.io/acoharmony/<name>``). The deploy command
compares the recorded version against the current git tag and only pulls
when they differ. Force-pulls ignore state.

State file: ``{tracking_dir}/deploy_state.json``
Schema: ``{ "<image_repo>": { "version", "deployed_at" } }``
"""

from __future__ import annotations

import json
import os
import tempfile
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


@dataclass
class DeployRecord:
    version: str
    deployed_at: str  # ISO timestamp

    def to_dict(self) -> dict[str, Any]:
        return {"version": self.version, "deployed_at": self.deployed_at}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DeployRecord:
        return cls(version=data["version"], deployed_at=data["deployed_at"])


class DeployStateTracker:
    """JSON-on-disk state, atomic writes, keyed by image repo."""

    def __init__(self, state_dir: Path):
        self.state_dir = state_dir
        self.state_file = state_dir / "deploy_state.json"
        self._cache: dict[str, DeployRecord] = {}
        self._load()

    def _load(self) -> None:
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text())
        except (OSError, json.JSONDecodeError):
            return
        for image_repo, payload in data.items():
            try:
                self._cache[image_repo] = DeployRecord.from_dict(payload)
            except (KeyError, TypeError):
                continue

    def _save(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            repo: record.to_dict() for repo, record in sorted(self._cache.items())
        }
        body = json.dumps(payload, indent=2) + "\n"
        fd, tmp_path = tempfile.mkstemp(
            prefix=".deploy.", suffix=".json.tmp", dir=str(self.state_dir)
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(body)
            os.replace(tmp_path, self.state_file)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def get(self, image_repo: str) -> DeployRecord | None:
        return self._cache.get(image_repo)

    def record(
        self,
        image_repo: str,
        version: str,
        deployed_at: datetime | None = None,
    ) -> DeployRecord:
        record = DeployRecord(
            version=version,
            deployed_at=(deployed_at or datetime.now()).isoformat(),
        )
        self._cache[image_repo] = record
        self._save()
        return record

    def all_records(self) -> dict[str, DeployRecord]:
        return dict(self._cache)
