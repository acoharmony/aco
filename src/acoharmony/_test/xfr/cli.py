# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _xfr.cli module."""

from __future__ import annotations

import argparse
from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._xfr.cli import (
    _tracker,
    add_subparsers,
    cmd_xfr_list,
    cmd_xfr_send,
    cmd_xfr_status,
    dispatch,
)
from acoharmony._xfr.profile import (
    LiteralPatternRule,
    TransferProfile,
    register_profile,
)


def _register_test_profile(tmp_dirs):
    profile = TransferProfile(
        name="ut",
        description="unit test profile",
        source_dirs=(tmp_dirs["src"],),
        destination=tmp_dirs["dst"],
        source_rule=LiteralPatternRule(patterns=("*.zip",), date_floor=None),
    )
    register_profile(profile)
    return profile


def _ns(**kwargs):
    return argparse.Namespace(**kwargs)


class TestTrackerHelper:
    @pytest.mark.unit
    def test_uses_storage_logs_path(self, tmp_path: Path):
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            stub = Stub.return_value
            stub.get_path.return_value = tmp_path
            t = _tracker("hdai")
        assert t.profile_name == "hdai"
        assert t.state_file == tmp_path / "tracking" / "xfr_hdai_state.json"


class TestCmdXfrList:
    @pytest.mark.unit
    def test_empty(self, capsys):
        with patch("acoharmony._xfr.cli.list_profiles", return_value=[]):
            rc = cmd_xfr_list(_ns())
        assert rc == 0
        assert "No xfr profiles" in capsys.readouterr().out

    @pytest.mark.unit
    def test_lists_registered(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        rc = cmd_xfr_list(_ns())
        out = capsys.readouterr().out
        assert rc == 0
        assert "ut" in out
        assert "unit test profile" in out


class TestCmdXfrStatus:
    @pytest.mark.unit
    def test_pending_files_displayed(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        (tmp_dirs["src"] / "a.zip").touch()
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_status(_ns(profile="ut", show="pending", limit=10))
        out = capsys.readouterr().out
        assert rc == 0
        assert "pending" in out
        assert "a.zip" in out

    @pytest.mark.unit
    def test_show_all_filters(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        (tmp_dirs["src"] / "a.zip").touch()
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_status(_ns(profile="ut", show="all", limit=10))
        out = capsys.readouterr().out
        assert rc == 0
        assert "a.zip" in out

    @pytest.mark.unit
    def test_no_files_returns_early(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        # Empty source directory
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_status(_ns(profile="ut", show="pending", limit=10))
        out = capsys.readouterr().out
        assert rc == 0
        assert "Total in scope: 0" in out

    @pytest.mark.unit
    def test_limit_applied(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        for i in range(5):
            (tmp_dirs["src"] / f"f{i}.zip").touch()
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_status(_ns(profile="ut", show="pending", limit=2))
        out = capsys.readouterr().out
        assert rc == 0
        assert "showing 2 of 5" in out

    @pytest.mark.unit
    def test_rename_shown_in_output(self, tmp_dirs, capsys):
        # Custom profile with a rename: status should show "src -> dest"
        profile = TransferProfile(
            name="ut",
            description="rename test",
            source_dirs=(tmp_dirs["src"],),
            destination=tmp_dirs["dst"],
            source_rule=LiteralPatternRule(patterns=("*.zip",), date_floor=None),
            rename=lambda n: f"renamed_{n}",
        )
        register_profile(profile)
        (tmp_dirs["src"] / "a.zip").touch()
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_status(_ns(profile="ut", show="pending", limit=10))
        out = capsys.readouterr().out
        assert rc == 0
        assert "-> renamed_a.zip" in out

    @pytest.mark.unit
    def test_default_limit_when_missing(self, tmp_dirs):
        _register_test_profile(tmp_dirs)
        (tmp_dirs["src"] / "a.zip").touch()
        # args has no `limit` attribute
        args = argparse.Namespace(profile="ut", show="pending")
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_status(args)
        assert rc == 0


class TestCmdXfrSend:
    @pytest.mark.unit
    def test_no_pending_short_circuits(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_send(_ns(profile="ut", dry_run=False))
        assert rc == 0
        assert "No pending files" in capsys.readouterr().out

    @pytest.mark.unit
    def test_dry_run(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        (tmp_dirs["src"] / "a.zip").touch()
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_send(_ns(profile="ut", dry_run=True))
        out = capsys.readouterr().out
        assert rc == 0
        assert "DRY RUN" in out
        assert not (tmp_dirs["dst"] / "a.zip").exists()

    @pytest.mark.unit
    def test_real_send(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        (tmp_dirs["src"] / "a.zip").write_bytes(b"data")
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = cmd_xfr_send(_ns(profile="ut", dry_run=False))
        out = capsys.readouterr().out
        assert rc == 0
        assert "[OK]" in out
        assert (tmp_dirs["dst"] / "a.zip").read_bytes() == b"data"

    @pytest.mark.unit
    def test_send_returns_1_on_error(self, tmp_dirs):
        _register_test_profile(tmp_dirs)
        (tmp_dirs["src"] / "a.zip").write_bytes(b"data")
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            with patch(
                "acoharmony._xfr.transfer._copy", side_effect=OSError("nope")
            ):
                rc = cmd_xfr_send(_ns(profile="ut", dry_run=False))
        assert rc == 1


class TestAddSubparsers:
    @pytest.mark.unit
    def test_wires_subcommands(self):
        parser = argparse.ArgumentParser()
        sub = parser.add_subparsers(dest="command")
        add_subparsers(sub)
        # Help prints without crashing
        args = parser.parse_args(["xfr", "list"])
        assert args.command == "xfr"
        assert args.xfr_command == "list"


class TestDispatch:
    @pytest.mark.unit
    def test_routes_list(self, tmp_dirs, capsys):
        _register_test_profile(tmp_dirs)
        rc = dispatch(_ns(xfr_command="list"))
        assert rc == 0

    @pytest.mark.unit
    def test_routes_status(self, tmp_dirs):
        _register_test_profile(tmp_dirs)
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = dispatch(_ns(xfr_command="status", profile="ut", show="pending", limit=10))
        assert rc == 0

    @pytest.mark.unit
    def test_routes_send(self, tmp_dirs):
        _register_test_profile(tmp_dirs)
        with patch("acoharmony._xfr.cli.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = tmp_dirs["state"]
            rc = dispatch(_ns(xfr_command="send", profile="ut", dry_run=False))
        assert rc == 0

    @pytest.mark.unit
    def test_unknown_command_prints_help(self, capsys):
        # Build a real parser so _xfr_parser exists
        parser = argparse.ArgumentParser(prog="xfr")
        ns = _ns(xfr_command=None, _xfr_parser=parser)
        rc = dispatch(ns)
        assert rc == 1

    @pytest.mark.unit
    def test_unknown_command_no_parser(self):
        rc = dispatch(_ns(xfr_command=None))
        assert rc == 1
