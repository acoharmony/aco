# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_restart.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestRestart:
    """Test suite for _restart."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        """Test execute function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._restart import RestartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.validate_services.return_value = (["svc1"], [])
        manager.profile = "dev"
        manager.docker.restart.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = RestartCommand(manager=manager)
        result = cmd.execute(services=None, group=None)
        assert result == 0
        manager.docker.restart.assert_called_once()

    @pytest.mark.unit
    def test_restartcommand_init(self) -> None:
        """Test RestartCommand initialization."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._restart import RestartCommand

        manager = MagicMock()
        cmd = RestartCommand(manager=manager)
        assert cmd.manager is manager

