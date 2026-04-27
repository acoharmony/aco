# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_restart.py."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest


class TestRestart:
    """Test suite for _restart."""

    @pytest.mark.unit
    def test_execute_calls_up_after_freshness(self) -> None:
        from acoharmony._deploy._commands._restart import RestartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.validate_services.return_value = (["svc1"], [])
        manager.docker.up.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = RestartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._restart.ensure_latest_images",
            return_value=0,
        ):
            rc = cmd.execute(services=None, group=None)
        assert rc == 0
        # restart is implemented as up -d so a freshly-pulled image is
        # actually swapped in (compose restart alone would not).
        manager.docker.up.assert_called_once()
        manager.docker.restart.assert_not_called()

    @pytest.mark.unit
    def test_execute_returns_pull_failure(self) -> None:
        from acoharmony._deploy._commands._restart import RestartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.validate_services.return_value = (["svc1"], [])

        cmd = RestartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._restart.ensure_latest_images",
            return_value=3,
        ):
            rc = cmd.execute(services=None, group=None)
        assert rc == 3
        manager.docker.up.assert_not_called()

    @pytest.mark.unit
    def test_execute_forwards_pull_flag(self) -> None:
        from acoharmony._deploy._commands._restart import RestartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.validate_services.return_value = (["svc1"], [])
        manager.docker.up.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = RestartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._restart.ensure_latest_images",
            return_value=0,
        ) as freshness:
            cmd.execute(services=None, group=None, pull=True)
        assert freshness.call_args.kwargs["force"] is True

    @pytest.mark.unit
    def test_restartcommand_init(self) -> None:
        from acoharmony._deploy._commands._restart import RestartCommand

        manager = MagicMock()
        cmd = RestartCommand(manager=manager)
        assert cmd.manager is manager
