# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_docker.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestDocker:
    """Test suite for _docker."""

    @pytest.mark.unit
    def test_up(self, tmp_path) -> None:
        """Test up function."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._docker import DockerComposeManager

        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("version: '3'")

        mgr = DockerComposeManager(compose_file)
        with patch.object(mgr, "_run_compose", return_value=MagicMock(returncode=0)) as mock_run:
            result = mgr.up(["svc1", "svc2"], detach=True, no_build=True)
            assert result.returncode == 0
            mock_run.assert_called_once_with(["up", "-d", "--no-build", "svc1", "svc2"])

    @pytest.mark.unit
    def test_down(self, tmp_path) -> None:
        """Test down function."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._docker import DockerComposeManager

        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("version: '3'")

        mgr = DockerComposeManager(compose_file)
        with patch.object(mgr, "_run_compose", return_value=MagicMock(returncode=0)) as mock_run:
            result = mgr.down()
            assert result.returncode == 0
            mock_run.assert_called_once_with(["down"])

    @pytest.mark.unit
    def test_stop(self, tmp_path) -> None:
        """Test stop function."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._docker import DockerComposeManager

        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("version: '3'")

        mgr = DockerComposeManager(compose_file)
        with patch.object(mgr, "_run_compose", return_value=MagicMock(returncode=0)) as mock_run:
            result = mgr.stop(["svc1"])
            assert result.returncode == 0
            mock_run.assert_called_once_with(["stop", "svc1"])

    @pytest.mark.unit
    def test_restart(self, tmp_path) -> None:
        """Test restart function."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._docker import DockerComposeManager

        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("version: '3'")

        mgr = DockerComposeManager(compose_file)
        with patch.object(mgr, "_run_compose", return_value=MagicMock(returncode=0)) as mock_run:
            result = mgr.restart(["svc1"])
            assert result.returncode == 0
            mock_run.assert_called_once_with(["restart", "svc1"])

    @pytest.mark.unit
    def test_ps(self, tmp_path) -> None:
        """Test ps function."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._docker import DockerComposeManager

        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("version: '3'")

        mgr = DockerComposeManager(compose_file)
        with patch.object(mgr, "_run_compose", return_value=MagicMock(returncode=0)) as mock_run:
            result = mgr.ps()
            assert result.returncode == 0
            mock_run.assert_called_once_with(["ps"], check=False)

    @pytest.mark.unit
    def test_dockercomposemanager_init(self, tmp_path) -> None:
        """Test DockerComposeManager initialization."""
        from acoharmony._deploy._docker import DockerComposeManager

        compose_file = tmp_path / "docker-compose.yml"
        compose_file.write_text("version: '3'")

        mgr = DockerComposeManager(compose_file)
        assert mgr.compose_file == compose_file
        assert mgr.compose_dir == tmp_path

    @pytest.mark.unit
    def test_dockercomposemanager_init_missing_file(self, tmp_path) -> None:
        """Test DockerComposeManager raises when file missing."""
        from acoharmony._deploy._docker import DockerComposeManager

        with pytest.raises(FileNotFoundError, match="Docker Compose file not found"):
            DockerComposeManager(tmp_path / "nonexistent.yml")

