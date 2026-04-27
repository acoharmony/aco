# © 2025 HarmonyCares
# All rights reserved.

"""
Per-profile state tracker for xfr.

Records what we placed and when. Authoritative for "did *we* drop this
file?" — verifiers layer on top to answer "did the destination tool
pick it up / archive it?"

State file: ``{tracking_dir}/xfr_<profile>_state.json``
Schema: ``{ "<destination_filename>": { "source_path", "placed_at", "source_filename" } }``
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
class XfrPlacement:
    source_filename: str
    source_path: str
    placed_at: str  # ISO timestamp

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_filename": self.source_filename,
            "source_path": self.source_path,
            "placed_at": self.placed_at,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> XfrPlacement:
        return cls(
            source_filename=data["source_filename"],
            source_path=data["source_path"],
            placed_at=data["placed_at"],
        )


class XfrStateTracker:
    """JSON-on-disk state, atomic writes, keyed by *destination* filename."""

    def __init__(self, profile_name: str, state_dir: Path):
        self.profile_name = profile_name
        self.state_dir = state_dir
        self.state_file = state_dir / f"xfr_{profile_name}_state.json"
        self._cache: dict[str, XfrPlacement] = {}
        self._load()

    def _load(self) -> None:
        if not self.state_file.exists():
            return
        try:
            data = json.loads(self.state_file.read_text())
        except (OSError, json.JSONDecodeError):
            return
        for dest_name, payload in data.items():
            try:
                self._cache[dest_name] = XfrPlacement.from_dict(payload)
            except (KeyError, TypeError):
                continue

    def _save(self) -> None:
        self.state_dir.mkdir(parents=True, exist_ok=True)
        payload = {
            name: placement.to_dict()
            for name, placement in sorted(self._cache.items())
        }
        body = json.dumps(payload, indent=2) + "\n"
        fd, tmp_path = tempfile.mkstemp(
            prefix=f".xfr_{self.profile_name}.", suffix=".json.tmp", dir=str(self.state_dir)
        )
        try:
            with os.fdopen(fd, "w") as f:
                f.write(body)
            os.replace(tmp_path, self.state_file)
        except Exception:
            if os.path.exists(tmp_path):
                os.unlink(tmp_path)
            raise

    def has_placed(self, dest_filename: str) -> bool:
        return dest_filename in self._cache

    def get(self, dest_filename: str) -> XfrPlacement | None:
        return self._cache.get(dest_filename)

    def record_placement(
        self,
        dest_filename: str,
        source_path: Path,
        placed_at: datetime | None = None,
    ) -> XfrPlacement:
        placement = XfrPlacement(
            source_filename=source_path.name,
            source_path=str(source_path),
            placed_at=(placed_at or datetime.now()).isoformat(),
        )
        self._cache[dest_filename] = placement
        self._save()
        return placement

    def all_placements(self) -> dict[str, XfrPlacement]:
        return dict(self._cache)
