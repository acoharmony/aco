"""Tests for auth doctor diagnostics."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from acoharmony._auth.doctor import check_service
from acoharmony._auth.paths import AuthPaths
from acoharmony._auth.public_ip import ProbeResult
from acoharmony._auth.registry import AuthRegistry


def _paths(tmp_path: Path) -> AuthPaths:
    return AuthPaths(
        data_path=tmp_path,
        root=tmp_path / "auth",
        registry=tmp_path / "auth" / "registry.yaml",
        secrets_dir=tmp_path / "auth" / "secrets",
        logs_dir=tmp_path / "auth" / "logs",
        certs_dir=tmp_path / "auth" / "certs",
        cert_bundle=tmp_path / "auth" / "certs" / "zscaler-bundle.pem",
        fouricli_config=tmp_path / "auth" / "secrets" / "4icli" / "config.txt",
        acoms_config_dir=tmp_path / "auth" / "secrets" / "acoms" / "config",
        legacy_fouricli_config=tmp_path / "bronze" / "config.txt",
        legacy_acoms_config_dir=tmp_path / "bronze" / "acoms" / "config",
    )


@pytest.mark.unit
def test_check_service_reports_ip_mismatch(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    paths.fouricli_config.parent.mkdir(parents=True)
    paths.fouricli_config.write_text("hashed-config")
    registry = AuthRegistry(paths.registry)
    registry.update("4icli", entity_id="D0259", registered_ips=["203.0.113.10"])

    monkeypatch.setattr("acoharmony._auth.doctor._deploy_nonsecret_env", lambda: {})
    monkeypatch.setattr(
        "acoharmony._auth.doctor.fetch_container_public_ip",
        lambda *a, **k: ProbeResult("203.0.113.99", source="test"),
    )

    report = check_service(
        "4icli",
        paths=paths,
        registry=registry,
        host_public_ip=ProbeResult("203.0.113.99", source="test"),
        skip_live=True,
    )

    assert report.diagnosis == "IP_MISMATCH"


@pytest.mark.unit
def test_check_service_accepts_registered_secondary_egress_ip(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    paths.fouricli_config.parent.mkdir(parents=True)
    paths.fouricli_config.write_text("hashed-config")
    registry = AuthRegistry(paths.registry)
    registry.update("4icli", entity_id="D0259", registered_ips=["136.226.69.108"])

    observed = ProbeResult(
        "68.134.99.31",
        source="test",
        values=("68.134.99.31", "136.226.69.108"),
    )
    monkeypatch.setattr("acoharmony._auth.doctor._deploy_nonsecret_env", lambda: {})
    monkeypatch.setattr(
        "acoharmony._auth.doctor.fetch_container_public_ip",
        lambda *a, **k: observed,
    )

    report = check_service(
        "4icli",
        paths=paths,
        registry=registry,
        host_public_ip=observed,
        skip_live=True,
    )

    assert report.diagnosis == "CHECK_SKIPPED"


@pytest.mark.unit
def test_check_service_reports_missing_config(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    paths = _paths(tmp_path)
    registry = AuthRegistry(paths.registry)
    registry.update("acoms", entity_id="A2671", registered_ips=["203.0.113.10"])

    monkeypatch.setattr("acoharmony._auth.doctor._deploy_nonsecret_env", lambda: {})
    monkeypatch.setattr(
        "acoharmony._auth.doctor.fetch_container_public_ip",
        lambda *a, **k: ProbeResult("203.0.113.10", source="test"),
    )

    report = check_service(
        "acoms",
        paths=paths,
        registry=registry,
        host_public_ip=ProbeResult("203.0.113.10", source="test"),
        skip_live=True,
    )

    assert report.diagnosis == "MISSING_CONFIG"


@pytest.mark.unit
def test_check_service_treats_empty_config_dir_as_missing(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    paths = _paths(tmp_path)
    paths.acoms_config_dir.mkdir(parents=True)
    registry = AuthRegistry(paths.registry)

    monkeypatch.setattr("acoharmony._auth.doctor._deploy_nonsecret_env", lambda: {})
    monkeypatch.setattr(
        "acoharmony._auth.doctor.fetch_container_public_ip",
        lambda *a, **k: ProbeResult("203.0.113.10", source="test"),
    )

    report = check_service(
        "acoms",
        paths=paths,
        registry=registry,
        host_public_ip=ProbeResult("203.0.113.10", source="test"),
        skip_live=True,
    )

    assert report.diagnosis == "MISSING_CONFIG"
    assert report.config.warning is not None
    assert "contains no config files" in report.config.warning


@pytest.mark.unit
def test_register_ip_command_updates_registry(tmp_path, monkeypatch: pytest.MonkeyPatch) -> None:
    from acoharmony._auth import cli as auth_cli

    paths = _paths(tmp_path)
    monkeypatch.setattr(auth_cli, "get_auth_paths", lambda: paths)
    monkeypatch.setattr(auth_cli, "_legacy_entity_id", lambda service: "D0259")

    rc = auth_cli.cmd_register_ip(
        SimpleNamespace(
            service="4icli",
            ip=["203.0.113.10"],
            entity_id=None,
            token_label="test token",
            replace=False,
            notes=None,
        )
    )

    assert rc == 0
    record = AuthRegistry(paths.registry).get("4icli")
    assert record.entity_id == "D0259"
    assert record.registered_ips == ["203.0.113.10"]


@pytest.mark.unit
def test_register_ip_requires_explicit_ip_when_probe_is_ambiguous(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from acoharmony._auth import cli as auth_cli

    paths = _paths(tmp_path)
    monkeypatch.setattr(auth_cli, "get_auth_paths", lambda: paths)
    monkeypatch.setattr(
        auth_cli,
        "fetch_public_ip",
        lambda: ProbeResult(
            "68.134.99.31",
            source="test",
            values=("68.134.99.31", "136.226.69.108"),
        ),
    )

    rc = auth_cli.cmd_register_ip(
        SimpleNamespace(
            service="4icli",
            ip=None,
            entity_id=None,
            token_label=None,
            replace=False,
            notes=None,
        )
    )

    assert rc == 1
    assert "Multiple public IPs observed" in capsys.readouterr().out
    assert AuthRegistry(paths.registry).get("4icli").registered_ips == []
