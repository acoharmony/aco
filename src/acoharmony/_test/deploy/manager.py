"""Tests for acoharmony._deploy._manager module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import subprocess
from pathlib import Path
from unittest.mock import patch

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._deploy._manager is not None


class TestDeploymentManager:
    @pytest.mark.unit
    def test_init_file_found(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()
            assert mgr.compose_file == compose

    @pytest.mark.unit
    def test_init_file_not_found(self, tmp_path):
        with patch.object(Path, "cwd", return_value=tmp_path):
            with pytest.raises(FileNotFoundError, match="Docker Compose file not found"):
                DeploymentManager()

    @pytest.mark.unit
    def test_service_groups_keys(self):
        assert "root" in DeploymentManager.SERVICE_GROUPS
        assert "infrastructure" in DeploymentManager.SERVICE_GROUPS
        assert "analytics" in DeploymentManager.SERVICE_GROUPS
        assert "development" in DeploymentManager.SERVICE_GROUPS

    @pytest.mark.unit
    def test_get_service_list_with_services(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()
            result = mgr._get_service_list(["svc1", "svc2"], None)
            assert result == ["svc1", "svc2"]

    @pytest.mark.unit
    def test_get_service_list_with_group(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()
            result = mgr._get_service_list(None, "analytics")
            assert "marimo" in result

    @pytest.mark.unit
    def test_get_service_list_group_shorthand(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()
            result = mgr._get_service_list(["root"], None)
            assert "4icli" in result

    @pytest.mark.unit
    def test_get_service_list_unknown_group(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()
            with pytest.raises(ValueError, match="Unknown service group"):
                mgr._get_service_list(None, "nonexistent")

    @pytest.mark.unit
    def test_get_service_list_none(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()
            result = mgr._get_service_list(None, None)
            assert result == []

    @pytest.mark.unit
    def test_execute_start(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose") as mock_run:
            code = mgr.execute_command("start", services=["svc1"])
            assert code == 0
            mock_run.assert_called_once()
            assert "up" in mock_run.call_args[0][0]

    @pytest.mark.unit
    def test_execute_stop(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose"):
            code = mgr.execute_command("stop")
            assert code == 0

    @pytest.mark.unit
    def test_execute_restart(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose"):
            code = mgr.execute_command("restart")
            assert code == 0

    @pytest.mark.unit
    def test_execute_status(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose"):
            code = mgr.execute_command("status")
            assert code == 0

    @pytest.mark.unit
    def test_execute_logs_with_follow_and_tail(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose") as mock_run:
            code = mgr.execute_command("logs", follow=True, tail=100)
            assert code == 0
            args = mock_run.call_args[0][0]
            assert "-f" in args
            assert "--tail" in args

    @pytest.mark.unit
    def test_execute_ps(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose"):
            code = mgr.execute_command("ps")
            assert code == 0

    @pytest.mark.unit
    def test_execute_logs_without_follow_and_tail(self, tmp_path):
        """Cover branches 119->121 (follow=False) and 121->123 (tail=None)."""
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch.object(mgr, "_run_compose") as mock_run:
            code = mgr.execute_command("logs", services=["svc1"])
            assert code == 0
            args = mock_run.call_args[0][0]
            assert args[0] == "logs"
            assert "-f" not in args
            assert "--tail" not in args
            assert "svc1" in args

    @pytest.mark.unit
    def test_execute_unknown_action(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with pytest.raises(ValueError, match="Unknown action"):
            mgr.execute_command("explode")

    @pytest.mark.unit
    def test_run_compose(self, tmp_path):
        compose = tmp_path / "deploy" / "compose" / "docker-compose.yml"
        compose.parent.mkdir(parents=True)
        compose.write_text("version: '3'")

        with patch.object(Path, "cwd", return_value=tmp_path):
            mgr = DeploymentManager()

        with patch("subprocess.run") as mock_run:
            mock_run.return_value = subprocess.CompletedProcess([], 0)
            mgr._run_compose(["ps"])
            mock_run.assert_called_once()
            cmd = mock_run.call_args[0][0]
            assert cmd[0] == "docker"
            assert cmd[1] == "compose"
