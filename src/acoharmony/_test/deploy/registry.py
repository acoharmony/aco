# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_registry.py."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestRegistry:
    """Tests for command registry functions."""

    @pytest.mark.unit
    def test_register_and_retrieve(self) -> None:
        @register_deploy_command("_test_reg")
        class _TestCmd:
            pass

        assert DEPLOY_COMMAND_REGISTRY["_test_reg"] is _TestCmd
        assert get_deploy_command("_test_reg") is _TestCmd
        # cleanup
        del DEPLOY_COMMAND_REGISTRY["_test_reg"]

    @pytest.mark.unit
    def test_decorator_returns_class_unchanged(self) -> None:
        @register_deploy_command("_test_dec")
        class _Orig:
            pass

        assert _Orig.__name__ == "_Orig"
        del DEPLOY_COMMAND_REGISTRY["_test_dec"]

    @pytest.mark.unit
    def test_get_unknown_command_raises(self) -> None:
        with pytest.raises(ValueError, match="Unknown deploy command: 'nope'"):
            get_deploy_command("nope")

    @pytest.mark.unit
    def test_get_unknown_lists_available(self) -> None:
        with pytest.raises(ValueError, match="Available commands:"):
            get_deploy_command("zzz_unknown")

    @pytest.mark.unit
    def test_list_deploy_commands(self) -> None:
        # Commands are registered by importing the _commands package
        cmds = list_deploy_commands()
        assert isinstance(cmds, list)
        for expected in ["start", "stop", "restart", "status", "logs", "ps", "build"]:
            assert expected in cmds
        assert cmds == sorted(cmds)

    @pytest.mark.unit
    def test_command_exists_true(self) -> None:
        assert command_exists("start") is True

    @pytest.mark.unit
    def test_command_exists_false(self) -> None:
        assert command_exists("nonexistent_cmd") is False
