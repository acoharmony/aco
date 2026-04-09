# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_ps.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestPs:
    """Test suite for _ps."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        """Test execute function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._ps import PsCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.docker.ps.return_value = MagicMock(returncode=0, stdout="NAME  STATUS\nsvc1  running")

        cmd = PsCommand(manager=manager)
        result = cmd.execute(services=None, group=None)
        assert result == 0
        manager.docker.ps.assert_called_once()

    @pytest.mark.unit
    def test_pscommand_init(self) -> None:
        """Test PsCommand initialization."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._ps import PsCommand

        manager = MagicMock()
        cmd = PsCommand(manager=manager)
        assert cmd.manager is manager

