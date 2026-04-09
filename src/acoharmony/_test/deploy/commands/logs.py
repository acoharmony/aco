# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_logs.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestLogs:
    """Test suite for _logs."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        """Test execute function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._logs import LogsCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.validate_services.return_value = (["svc1"], [])
        manager.docker.logs.return_value = MagicMock(returncode=0, stdout="log output", stderr="")

        cmd = LogsCommand(manager=manager)
        result = cmd.execute(services=None, group=None, follow=False, tail=10)
        assert result == 0
        manager.docker.logs.assert_called_once()

    @pytest.mark.unit
    def test_logscommand_init(self) -> None:
        """Test LogsCommand initialization."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._logs import LogsCommand

        manager = MagicMock()
        cmd = LogsCommand(manager=manager)
        assert cmd.manager is manager

