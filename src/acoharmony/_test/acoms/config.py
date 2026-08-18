"""Tests for ACOMS configuration helpers."""

from __future__ import annotations

from pathlib import Path

import pytest

from acoharmony._acoms.config import AcomsConfig, load_deploy_env


@pytest.mark.unit
def test_load_deploy_env_reads_simple_values(tmp_path: Path) -> None:
    deploy = tmp_path / "deploy"
    deploy.mkdir()
    (deploy / ".env").write_text(
        "\n".join(
            [
                "# comment",
                "ACOMS_API_ID=aco-123",
                "ACOMS_API_KEY='quoted-key'",
                'ACOMS_API_SECRET="quoted-secret"',
            ]
        )
    )

    loaded = load_deploy_env(deploy)

    assert loaded["ACOMS_API_ID"] == "aco-123"
    assert loaded["ACOMS_API_KEY"] == "quoted-key"
    assert loaded["ACOMS_API_SECRET"] == "quoted-secret"


@pytest.mark.unit
def test_from_profile_prefers_environment_aco_id(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ACOMS_API_ID", "env-aco")
    monkeypatch.setenv("ACOMS_DEFAULT_YEAR", "2026")

    config = AcomsConfig.from_profile()

    assert config.default_aco_id == "env-aco"
    assert config.default_year == 2026
