# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_commands/_start.py."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest


class TestStart:
    """Test suite for _start."""

    @pytest.mark.unit
    def test_execute(self) -> None:
        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1", "svc2"]
        manager.profile = "dev"
        manager.docker.up.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = StartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._start.ensure_latest_images",
            return_value=0,
        ):
            result = cmd.execute(services=None)
        assert result == 0
        manager.docker.up.assert_called_once()

    @pytest.mark.unit
    def test_execute_skips_freshness_when_build(self) -> None:
        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.docker.up.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = StartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._start.ensure_latest_images"
        ) as freshness:
            cmd.execute(services=None, build=True)
        freshness.assert_not_called()

    @pytest.mark.unit
    def test_execute_returns_pull_failure(self) -> None:
        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]

        cmd = StartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._start.ensure_latest_images",
            return_value=2,
        ):
            rc = cmd.execute(services=None)
        assert rc == 2
        manager.docker.up.assert_not_called()

    @pytest.mark.unit
    def test_execute_forwards_pull_flag(self) -> None:
        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        manager.service_mapper.is_production.return_value = False
        manager.get_all_services.return_value = ["svc1"]
        manager.docker.up.return_value = MagicMock(returncode=0, stdout="", stderr="")

        cmd = StartCommand(manager=manager)
        with patch(
            "acoharmony._deploy._commands._start.ensure_latest_images",
            return_value=0,
        ) as freshness:
            cmd.execute(services=None, pull=True)
        assert freshness.call_args.kwargs["force"] is True

    @pytest.mark.unit
    def test_startcommand_init(self) -> None:
        from acoharmony._deploy._commands._start import StartCommand

        manager = MagicMock()
        cmd = StartCommand(manager=manager)
        assert cmd.manager is manager
