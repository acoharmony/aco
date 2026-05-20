# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_status.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestStatus:
    """Test suite for _status."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        """Test execute function."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._status import StatusCommand

        manager = MagicMock()
        manager.profile = "dev"
        manager.compose_path = "/fake/docker-compose.yml"
        manager.service_mapper.is_production.return_value = False
        manager.service_mapper.list_groups.return_value = ["data", "app"]
        manager.get_services_for_group.return_value = ["svc1"]
        manager.get_all_services.return_value = ["svc1", "svc2"]
        manager.docker.ps.return_value = MagicMock(stdout="NAME  STATUS\nsvc1  running")

        cmd = StatusCommand(manager=manager)
        result = cmd.execute()
        assert result == 0

    @pytest.mark.unit
    def test_statuscommand_init(self) -> None:
        """Test StatusCommand initialization."""
        from unittest.mock import MagicMock

        from acoharmony._deploy._commands._status import StatusCommand

        manager = MagicMock()
        cmd = StatusCommand(manager=manager)
        assert cmd.manager is manager

