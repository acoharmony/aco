# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_start.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestStart:
    """Test suite for _start."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        """Test execute function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1", "svc2"]
        manager.profile = "dev"
        manager.docker.up.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = StartCommand(manager=manager)
        result = cmd.execute(services=None)
        assert result == 0
        manager.docker.up.assert_called_once()

    @pytest.mark.unit
    def test_startcommand_init(self) -> None:
        """Test StartCommand initialization."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        cmd = StartCommand(manager=manager)
        assert cmd.manager is manager

