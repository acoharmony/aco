# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_core.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestCore:
    """Test suite for _core."""

    @pytest.mark.unit
    def test_execute_command(self) -> None:
        """Test execute_command function."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._core import DeploymentManager

        mgr = MagicMock(spec=DeploymentManager)
        mgr.execute_command = DeploymentManager.execute_command.__get__(mgr, DeploymentManager)

        mock_cmd_instance = MagicMock()
        mock_cmd_instance.execute.return_value = 0
        mock_cmd_class = MagicMock(return_value=mock_cmd_instance)

        with patch("acoharmony._deploy._core.get_deploy_command", return_value=mock_cmd_class):
            result = mgr.execute_command("start", services=["svc1"])
            assert result == 0
            mock_cmd_class.assert_called_once_with(mgr)
            mock_cmd_instance.execute.assert_called_once_with(services=["svc1"])

    @pytest.mark.unit
    def test_get_services_for_group(self) -> None:
        """Test get_services_for_group function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._core import DeploymentManager

        mgr = MagicMock()
        mgr.get_services_for_group = DeploymentManager.get_services_for_group.__get__(mgr, DeploymentManager)
        mgr.service_mapper.get_group_services.return_value = ["redis", "postgres"]

        result = mgr.get_services_for_group("data")
        assert result == ["redis", "postgres"]
        mgr.service_mapper.get_group_services.assert_called_once_with("data")

    @pytest.mark.unit
    def test_get_all_services(self) -> None:
        """Test get_all_services function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._core import DeploymentManager

        mgr = MagicMock()
        mgr.get_all_services = DeploymentManager.get_all_services.__get__(mgr, DeploymentManager)
        mgr.service_mapper.get_all_services.return_value = ["redis", "postgres", "minio"]

        result = mgr.get_all_services()
        assert result == ["redis", "postgres", "minio"]

    @pytest.mark.unit
    def test_validate_services(self) -> None:
        """Test validate_services function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._core import DeploymentManager

        mgr = MagicMock()
        mgr.validate_services = DeploymentManager.validate_services.__get__(mgr, DeploymentManager)
        mgr.service_mapper.validate_services.return_value = (["redis"], ["unknown"])

        valid, invalid = mgr.validate_services(["redis", "unknown"])
        assert valid == ["redis"]
        assert invalid == ["unknown"]

    @pytest.mark.unit
    def test_deploymentmanager_init(self) -> None:
        """Test DeploymentManager initialization."""
        from unittest.mock import MagicMock, patch

        from acoharmony._deploy._core import DeploymentManager

        with patch.object(DeploymentManager, "_get_profile", return_value="dev"), \
             patch.object(DeploymentManager, "_get_compose_path", return_value="/fake/docker-compose.yml"), \
             patch("acoharmony._deploy._core.DockerComposeManager") as MockDocker, \
             patch("acoharmony._deploy._core.ProfileServiceMapper") as MockMapper:
            mgr = DeploymentManager(profile="test")
            assert mgr.profile == "test"
            MockDocker.assert_called_once()
            MockMapper.assert_called_once_with("test")

    @pytest.mark.unit
    def test_find_compose_fallback_not_found(self, tmp_path, monkeypatch) -> None:
        from unittest.mock import patch

        from acoharmony._deploy._core import DeploymentManager

        monkeypatch.chdir(tmp_path)
        mgr = DeploymentManager.__new__(DeploymentManager)

        # Patch Path(__file__) to point inside tmp_path so traversal
        # never finds docker-compose.yml
        with patch(
            "acoharmony._deploy._core.__file__",
            str(tmp_path / "fake" / "_core.py"),
        ):
            with pytest.raises(FileNotFoundError, match="docker-compose"):
                mgr._get_compose_path()

    @pytest.mark.unit
    def test_start_no_services(self) -> None:
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = []
        cmd = StartCommand(manager=manager)
        result = cmd.execute(services=None)
        assert result == 1
