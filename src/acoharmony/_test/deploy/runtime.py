# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_runtime.py."""

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path

import pytest

from acoharmony._deploy._runtime import docker_environment


class TestDockerEnvironment:
    @pytest.mark.unit
    def test_preserves_explicit_docker_host(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("DOCKER_HOST", "unix:///tmp/explicit.sock")
        assert docker_environment()["DOCKER_HOST"] == "unix:///tmp/explicit.sock"

    @pytest.mark.unit
    def test_uses_xdg_runtime_rootless_socket(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        socket = tmp_path / "docker.sock"
        socket.touch()
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
        assert docker_environment()["DOCKER_HOST"] == f"unix://{socket}"

    @pytest.mark.unit
    def test_leaves_unset_when_no_socket(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.delenv("DOCKER_HOST", raising=False)
        monkeypatch.setenv("XDG_RUNTIME_DIR", str(tmp_path))
        monkeypatch.setattr("acoharmony._deploy._runtime.os.getuid", lambda: 999999)
        assert "DOCKER_HOST" not in docker_environment()
