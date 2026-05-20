# © 2025 HarmonyCares
"""Tests for acoharmony/_deploy/_state.py."""


from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._deploy._state import DeployRecord, DeployStateTracker


class TestDeployRecord:
    @pytest.mark.unit
    def test_round_trip(self) -> None:
        rec = DeployRecord(version="v1.2.3", deployed_at="2026-04-27T12:00:00")
        assert DeployRecord.from_dict(rec.to_dict()) == rec


class TestDeployStateTracker:
    @pytest.mark.unit
    def test_starts_empty(self, tmp_path: Path) -> None:
        tracker = DeployStateTracker(tmp_path)
        assert tracker.all_records() == {}
        assert tracker.get("ghcr.io/acoharmony/anything") is None

    @pytest.mark.unit
    def test_record_persists_and_reloads(self, tmp_path: Path) -> None:
        tracker = DeployStateTracker(tmp_path)
        tracker.record(
            "ghcr.io/acoharmony/4icli",
            "v0.0.20",
            deployed_at=datetime(2026, 4, 27, 12, 0, 0),
        )
        # New tracker reads from disk.
        reloaded = DeployStateTracker(tmp_path)
        rec = reloaded.get("ghcr.io/acoharmony/4icli")
        assert rec is not None
        assert rec.version == "v0.0.20"
        assert rec.deployed_at == "2026-04-27T12:00:00"

    @pytest.mark.unit
    def test_unreadable_state_file_starts_fresh(self, tmp_path: Path) -> None:
        (tmp_path / "deploy_state.json").write_text("not-json-{")
        tracker = DeployStateTracker(tmp_path)
        assert tracker.all_records() == {}

    @pytest.mark.unit
    def test_state_file_with_bad_record_skipped(self, tmp_path: Path) -> None:
        (tmp_path / "deploy_state.json").write_text(
            '{"ghcr.io/acoharmony/4icli": {"only": "garbage"}, '
            '"ghcr.io/acoharmony/marimo": {"version": "v1", "deployed_at": "t"}}'
        )
        tracker = DeployStateTracker(tmp_path)
        assert tracker.get("ghcr.io/acoharmony/4icli") is None
        assert tracker.get("ghcr.io/acoharmony/marimo") is not None

    @pytest.mark.unit
    def test_save_atomic_temp_cleanup_on_error(self, tmp_path: Path) -> None:
        tracker = DeployStateTracker(tmp_path)
        with patch("acoharmony._deploy._state.os.replace", side_effect=OSError("nope")):
            with pytest.raises(OSError, match="nope"):
                tracker.record("ghcr.io/acoharmony/4icli", "v1")
        leftovers = [p for p in tmp_path.iterdir() if p.name.endswith(".json.tmp")]
        assert leftovers == []

    @pytest.mark.unit
    def test_state_file_unreadable_io_error(self, tmp_path: Path) -> None:
        path = tmp_path / "deploy_state.json"
        path.write_text("{}")
        with patch.object(Path, "read_text", side_effect=OSError("nope")):
            tracker = DeployStateTracker(tmp_path)
        assert tracker.all_records() == {}

    @pytest.mark.unit
    def test_record_uses_now_when_no_timestamp(self, tmp_path: Path) -> None:
        tracker = DeployStateTracker(tmp_path)
        tracker.record("ghcr.io/acoharmony/4icli", "v1")
        rec = tracker.get("ghcr.io/acoharmony/4icli")
        # Just confirm a parseable ISO timestamp landed.
        datetime.fromisoformat(rec.deployed_at)

    @pytest.mark.unit
    def test_save_creates_state_dir_lazily(self, tmp_path: Path) -> None:
        nested = tmp_path / "does" / "not" / "exist"
        tracker = DeployStateTracker(nested)
        tracker.record("ghcr.io/acoharmony/4icli", "v1")
        assert (nested / "deploy_state.json").exists()
