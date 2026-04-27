# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_freshness.py."""


from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from acoharmony._deploy._freshness import (
    deploy_state_tracker,
    ensure_latest_images,
)
from acoharmony._deploy._state import DeployStateTracker


def _docker(returncode: int = 0, stderr: str = "") -> MagicMock:
    docker = MagicMock()
    docker.compose_file = Path("/fake/compose.yml")
    docker.pull.return_value = MagicMock(returncode=returncode, stderr=stderr, stdout="")
    return docker


class TestEnsureLatestImages:
    @pytest.mark.unit
    def test_no_acoharmony_services_skips_pull(self, tmp_path: Path) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        with patch(
            "acoharmony._deploy._freshness.service_images", return_value={}
        ):
            rc = ensure_latest_images(docker, tracker, ["postgres"])
        assert rc == 0
        docker.pull.assert_not_called()

    @pytest.mark.unit
    def test_pulls_when_state_missing(self, tmp_path: Path) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={"4icli": "ghcr.io/acoharmony/4icli"},
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value="v0.0.20",
            ):
                rc = ensure_latest_images(docker, tracker, ["4icli"])
        assert rc == 0
        docker.pull.assert_called_once_with(["4icli"])
        assert tracker.get("ghcr.io/acoharmony/4icli").version == "v0.0.20"

    @pytest.mark.unit
    def test_skips_pull_when_state_matches(self, tmp_path: Path) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        tracker.record("ghcr.io/acoharmony/4icli", "v0.0.20")
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={"4icli": "ghcr.io/acoharmony/4icli"},
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value="v0.0.20",
            ):
                rc = ensure_latest_images(docker, tracker, ["4icli"])
        assert rc == 0
        docker.pull.assert_not_called()

    @pytest.mark.unit
    def test_pulls_only_stale_subset(self, tmp_path: Path) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        tracker.record("ghcr.io/acoharmony/4icli", "v0.0.20")
        # marimo has no record → stale.
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={
                "4icli": "ghcr.io/acoharmony/4icli",
                "marimo": "ghcr.io/acoharmony/marimo",
            },
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value="v0.0.20",
            ):
                rc = ensure_latest_images(docker, tracker, ["4icli", "marimo"])
        assert rc == 0
        docker.pull.assert_called_once_with(["marimo"])

    @pytest.mark.unit
    def test_force_pulls_all(self, tmp_path: Path) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        tracker.record("ghcr.io/acoharmony/4icli", "v0.0.20")
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={
                "4icli": "ghcr.io/acoharmony/4icli",
                "marimo": "ghcr.io/acoharmony/marimo",
            },
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value="v0.0.20",
            ):
                rc = ensure_latest_images(
                    docker, tracker, ["4icli", "marimo"], force=True
                )
        assert rc == 0
        called_args = docker.pull.call_args.args[0]
        assert set(called_args) == {"4icli", "marimo"}

    @pytest.mark.unit
    def test_no_tag_warns_and_skips_when_not_force(
        self, tmp_path: Path, capsys: pytest.CaptureFixture[str]
    ) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={"4icli": "ghcr.io/acoharmony/4icli"},
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value=None,
            ):
                rc = ensure_latest_images(docker, tracker, ["4icli"])
        assert rc == 0
        docker.pull.assert_not_called()
        captured = capsys.readouterr()
        assert "Could not determine latest release tag" in captured.out

    @pytest.mark.unit
    def test_force_pulls_even_without_tag(self, tmp_path: Path) -> None:
        docker = _docker()
        tracker = DeployStateTracker(tmp_path)
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={"4icli": "ghcr.io/acoharmony/4icli"},
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value=None,
            ):
                rc = ensure_latest_images(
                    docker, tracker, ["4icli"], force=True
                )
        assert rc == 0
        docker.pull.assert_called_once_with(["4icli"])
        # No version known → no record written.
        assert tracker.get("ghcr.io/acoharmony/4icli") is None

    @pytest.mark.unit
    def test_pull_failure_returns_nonzero(self, tmp_path: Path) -> None:
        docker = _docker(returncode=2, stderr="boom")
        tracker = DeployStateTracker(tmp_path)
        with patch(
            "acoharmony._deploy._freshness.service_images",
            return_value={"4icli": "ghcr.io/acoharmony/4icli"},
        ):
            with patch(
                "acoharmony._deploy._freshness.latest_release_tag",
                return_value="v0.0.20",
            ):
                rc = ensure_latest_images(docker, tracker, ["4icli"])
        assert rc == 2
        # State is NOT updated when pull fails.
        assert tracker.get("ghcr.io/acoharmony/4icli") is None


class TestDeployStateTrackerFactory:
    @pytest.mark.unit
    def test_uses_logs_tracking_dir(self, tmp_path: Path) -> None:
        with patch("acoharmony._store.StorageBackend") as Stub:
            Stub.return_value.get_path.return_value = str(tmp_path)
            tracker = deploy_state_tracker()
        assert tracker.state_dir == tmp_path / "tracking"
