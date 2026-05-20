# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_stop.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestStop:
    """Test suite for _stop."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        """Test execute function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._stop import StopCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.profile = "dev"
        manager.docker.down.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = StopCommand(manager=manager)
        # Stop all services (no services/group provided)
        result = cmd.execute(services=None, group=None)
        assert result == 0
        manager.docker.down.assert_called_once()

    @pytest.mark.unit
    def test_stopcommand_init(self) -> None:
        """Test StopCommand initialization."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._stop import StopCommand

        manager = MagicMock()
        cmd = StopCommand(manager=manager)
        assert cmd.manager is manager

