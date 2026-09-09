"""Tests for ACOMS CLI setup helpers."""

from __future__ import annotations

import argparse
from types import SimpleNamespace

import pytest


def _patch_deploy(tmp_path, monkeypatch):
    deploy = tmp_path / "deploy"
    (deploy / "images" / "acoms").mkdir(parents=True)
    env = deploy / ".env"
    env.write_text(
        "ACOMS_API_KEY=old-key\nACOMS_API_SECRET=old-secret\nACOMS_API_ID=A2671\nACOMS_ENV=prod\n"
    )
    bootstrap = deploy / "images" / "acoms" / "bootstrap.sh"
    bootstrap.write_text("#!/bin/sh\nexit 0\n")
    bootstrap.chmod(0o755)
    monkeypatch.setattr("acoharmony._acoms.cli._find_deploy_dir", lambda: deploy)
    return env


@pytest.mark.unit
def test_acoms_setup_writes_env_and_runs_bootstrap(tmp_path, monkeypatch):
    from acoharmony._acoms.cli import cmd_setup

    env = _patch_deploy(tmp_path, monkeypatch)
    inputs = iter(["new-key", "A9999", "prod"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    monkeypatch.setattr("acoharmony._acoms.cli.getpass", lambda *a, **k: "new-secret")

    calls = []

    def fake_run(cmd, env=None, check=False):
        calls.append((cmd, dict(env or {}), check))
        return SimpleNamespace(returncode=0)

    monkeypatch.setattr("acoharmony._acoms.cli.subprocess.run", fake_run)

    rc = cmd_setup(argparse.Namespace())

    assert rc == 0
    assert calls[0][0][0].endswith("bootstrap.sh")
    text = env.read_text()
    assert "ACOMS_API_KEY=new-key" in text
    assert "ACOMS_API_SECRET=new-secret" in text
    assert "ACOMS_API_ID=A9999" in text


@pytest.mark.unit
def test_acoms_setup_aborts_when_user_declines_same_credentials(tmp_path, monkeypatch):
    from acoharmony._acoms.cli import cmd_setup

    _patch_deploy(tmp_path, monkeypatch)
    inputs = iter(["", "", "", "n"])
    monkeypatch.setattr("builtins.input", lambda *a, **k: next(inputs))
    monkeypatch.setattr("acoharmony._acoms.cli.getpass", lambda *a, **k: "")
    monkeypatch.setattr(
        "acoharmony._acoms.cli.subprocess.run",
        lambda *a, **k: SimpleNamespace(returncode=0),
    )

    rc = cmd_setup(argparse.Namespace())

    assert rc == 1
