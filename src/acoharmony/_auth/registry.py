"""Non-secret token registry for public-IP allowlist diagnostics."""

from __future__ import annotations

import hashlib
import os
import tempfile
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from .paths import get_auth_paths
from .public_ip import parse_ip_values


def utc_now_iso() -> str:
    """Return a compact UTC ISO timestamp."""
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def credential_fingerprint(*parts: str | None) -> str | None:
    """Hash credential material without exposing the material itself."""
    material = "\n".join(part for part in parts if part)
    if not material:
        return None
    return "sha256:" + hashlib.sha256(material.encode("utf-8")).hexdigest()


def file_fingerprint(path: Path) -> str | None:
    """Hash a single file's bytes."""
    if not path.is_file():
        return None
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def directory_fingerprint(path: Path) -> str | None:
    """Hash a directory by relative file names and contents."""
    if not path.is_dir():
        return None
    digest = hashlib.sha256()
    found = False
    for child in sorted(p for p in path.rglob("*") if p.is_file()):
        found = True
        rel = child.relative_to(path).as_posix().encode("utf-8")
        digest.update(rel)
        digest.update(b"\0")
        with child.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
        digest.update(b"\0")
    if not found:
        return None
    return "sha256:" + digest.hexdigest()


@dataclass
class TokenRecord:
    """A non-secret record of a portal token's operational envelope."""

    service: str
    entity_id: str | None = None
    registered_ips: list[str] = field(default_factory=list)
    token_label: str | None = None
    credential_fingerprint: str | None = None
    last_verified_at: str | None = None
    updated_at: str | None = None
    notes: str | None = None

    @classmethod
    def from_mapping(cls, service: str, data: dict[str, Any] | None) -> TokenRecord:
        data = data or {}
        try:
            registered_ips = parse_ip_values(data.get("registered_ips", []))
        except ValueError:
            registered_ips = []
        return cls(
            service=service,
            entity_id=data.get("entity_id"),
            registered_ips=registered_ips,
            token_label=data.get("token_label"),
            credential_fingerprint=data.get("credential_fingerprint"),
            last_verified_at=data.get("last_verified_at"),
            updated_at=data.get("updated_at"),
            notes=data.get("notes"),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Serialize record to YAML-friendly mapping."""
        data: dict[str, Any] = {
            "entity_id": self.entity_id,
            "registered_ips": self.registered_ips,
            "token_label": self.token_label,
            "credential_fingerprint": self.credential_fingerprint,
            "last_verified_at": self.last_verified_at,
            "updated_at": self.updated_at,
            "notes": self.notes,
        }
        return {key: value for key, value in data.items() if value not in (None, [], "")}


class AuthRegistry:
    """Read and update the non-secret token registry."""

    def __init__(self, path: Path | None = None):
        self.path = path or get_auth_paths().registry

    def load(self) -> dict[str, Any]:
        """Load raw registry data."""
        if not self.path.exists():
            return {"version": 1, "tokens": {}}
        data = yaml.safe_load(self.path.read_text()) or {}
        if not isinstance(data, dict):
            return {"version": 1, "tokens": {}}
        data.setdefault("version", 1)
        tokens = data.setdefault("tokens", {})
        if not isinstance(tokens, dict):
            data["tokens"] = {}
        return data

    def save(self, data: dict[str, Any]) -> None:
        """Atomically save registry data with owner-only permissions."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rendered = yaml.safe_dump(data, sort_keys=True)
        fd, tmp_name = tempfile.mkstemp(prefix=".registry.", suffix=".yaml", dir=self.path.parent)
        try:
            with os.fdopen(fd, "w") as handle:
                handle.write(rendered)
            os.chmod(tmp_name, 0o600)
            os.replace(tmp_name, self.path)
        finally:
            if os.path.exists(tmp_name):
                os.unlink(tmp_name)

    def get(self, service: str) -> TokenRecord:
        """Return a service token record, defaulting to empty."""
        data = self.load()
        return TokenRecord.from_mapping(service, data.get("tokens", {}).get(service))

    def update(
        self,
        service: str,
        *,
        entity_id: str | None = None,
        registered_ips: list[str] | None = None,
        replace_ips: bool = False,
        token_label: str | None = None,
        credential_fingerprint: str | None = None,
        last_verified_at: str | None = None,
        notes: str | None = None,
    ) -> TokenRecord:
        """Update one token record and persist the registry."""
        data = self.load()
        tokens = data.setdefault("tokens", {})
        current = TokenRecord.from_mapping(service, tokens.get(service))

        if entity_id:
            current.entity_id = entity_id
        if token_label:
            current.token_label = token_label
        if credential_fingerprint:
            current.credential_fingerprint = credential_fingerprint
        if last_verified_at:
            current.last_verified_at = last_verified_at
        if notes:
            current.notes = notes
        if registered_ips is not None:
            normalized = parse_ip_values(registered_ips)
            if replace_ips:
                current.registered_ips = normalized
            else:
                for ip in normalized:
                    if ip not in current.registered_ips:
                        current.registered_ips.append(ip)

        current.updated_at = utc_now_iso()
        tokens[service] = current.to_mapping()
        self.save(data)
        return current
