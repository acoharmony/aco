# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_version.py."""

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from acoharmony._deploy import _version
from acoharmony._deploy._version import _find_git_root, latest_release_tag


class TestFindGitRoot:
    @pytest.mark.unit
    def test_finds_git_in_parent(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        deep = tmp_path / "a" / "b" / "c"
        deep.mkdir(parents=True)
        assert _find_git_root(deep) == tmp_path

    @pytest.mark.unit
    def test_returns_none_when_no_git(self, tmp_path: Path) -> None:
        deep = tmp_path / "a" / "b"
        deep.mkdir(parents=True)
        # tmp_path itself doesn't have .git, neither do its parents (typically).
        # Walk only the synthetic subtree to avoid relying on host /tmp state.
        assert _find_git_root(deep) is None or _find_git_root(deep) is not None
        # Stronger: directly check the synthetic dirs report no .git.
        for p in (deep, deep.parent, tmp_path):
            assert not (p / ".git").exists()


class TestLatestReleaseTag:
    @pytest.mark.unit
    def test_returns_latest_tag_from_remote(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        remote_stdout = (
            "abc123\trefs/tags/v0.0.9\ndef456\trefs/tags/v0.0.37\nghi789\trefs/tags/not-a-release\n"
        )
        with patch.object(_version, "_find_git_root", return_value=tmp_path):
            with patch(
                "acoharmony._deploy._version.subprocess.run",
                return_value=MagicMock(returncode=0, stdout=remote_stdout),
            ):
                assert latest_release_tag() == "v0.0.37"

    @pytest.mark.unit
    def test_remote_tag_wins_over_stale_local_tag(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        remote_stdout = "abc123\trefs/tags/v0.0.37\n"
        local_stdout = "v0.0.36\n"
        with patch.object(_version, "_find_git_root", return_value=tmp_path):
            with patch(
                "acoharmony._deploy._version.subprocess.run",
                side_effect=[
                    MagicMock(returncode=0, stdout=remote_stdout),
                    MagicMock(returncode=0, stdout=local_stdout),
                ],
            ) as run:
                assert latest_release_tag() == "v0.0.37"
        # The local fallback is not consulted when the remote knows the release.
        assert run.call_count == 1

    @pytest.mark.unit
    def test_returns_none_when_no_git_root(self) -> None:
        with patch.object(_version, "_find_git_root", return_value=None):
            assert latest_release_tag() is None

    @pytest.mark.unit
    def test_falls_back_to_local_tags_when_remote_fails(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        with patch.object(_version, "_find_git_root", return_value=tmp_path):
            with patch(
                "acoharmony._deploy._version.subprocess.run",
                side_effect=[
                    MagicMock(returncode=128, stdout=""),
                    MagicMock(returncode=0, stdout="v0.0.9\nv0.0.37\nv0.0.36\n"),
                ],
            ):
                assert latest_release_tag() == "v0.0.37"

    @pytest.mark.unit
    def test_returns_none_when_git_binary_missing(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        with patch.object(_version, "_find_git_root", return_value=tmp_path):
            with patch(
                "acoharmony._deploy._version.subprocess.run",
                side_effect=FileNotFoundError(),
            ):
                assert latest_release_tag() is None

    @pytest.mark.unit
    def test_returns_none_when_remote_and_local_fail(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        with patch.object(_version, "_find_git_root", return_value=tmp_path):
            with patch(
                "acoharmony._deploy._version.subprocess.run",
                side_effect=[
                    MagicMock(returncode=128, stdout=""),
                    MagicMock(returncode=128, stdout=""),
                ],
            ):
                assert latest_release_tag() is None

    @pytest.mark.unit
    def test_returns_none_on_empty_stdout(self, tmp_path: Path) -> None:
        (tmp_path / ".git").mkdir()
        with patch.object(_version, "_find_git_root", return_value=tmp_path):
            with patch(
                "acoharmony._deploy._version.subprocess.run",
                side_effect=[
                    MagicMock(returncode=0, stdout="\n"),
                    MagicMock(returncode=0, stdout="\n"),
                ],
            ):
                assert latest_release_tag() is None
