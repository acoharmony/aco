"""Tests for auth registry helpers."""

from __future__ import annotations

import stat

import pytest

from acoharmony._auth.registry import AuthRegistry, credential_fingerprint


@pytest.mark.unit
def test_registry_update_merges_ips_and_hides_secrets(tmp_path) -> None:
    registry_path = tmp_path / "auth" / "registry.yaml"
    registry = AuthRegistry(registry_path)

    record = registry.update(
        "4icli",
        entity_id="D0259",
        registered_ips=["203.0.113.10"],
        credential_fingerprint=credential_fingerprint("key", "secret"),
    )
    record = registry.update("4icli", registered_ips=["203.0.113.11"])

    assert record.entity_id == "D0259"
    assert record.registered_ips == ["203.0.113.10", "203.0.113.11"]
    assert "secret" not in registry_path.read_text()
    assert stat.S_IMODE(registry_path.stat().st_mode) == 0o600


@pytest.mark.unit
def test_registry_replace_ips(tmp_path) -> None:
    registry = AuthRegistry(tmp_path / "registry.yaml")

    registry.update("acoms", registered_ips=["203.0.113.10"])
    record = registry.update("acoms", registered_ips=["203.0.113.12"], replace_ips=True)

    assert record.registered_ips == ["203.0.113.12"]
