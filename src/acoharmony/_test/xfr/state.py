# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _xfr.state module."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._xfr.state import XfrPlacement, XfrStateTracker


class TestXfrPlacement:
    @pytest.mark.unit
    def test_round_trip(self):
        p = XfrPlacement(
            source_filename="foo.zip",
            source_path="/abs/foo.zip",
            placed_at="2026-04-27T10:00:00",
        )
        assert XfrPlacement.from_dict(p.to_dict()) == p


class TestXfrStateTracker:
    @pytest.mark.unit
    def test_starts_empty(self, tmp_path: Path):
        t = XfrStateTracker("p", tmp_path)
        assert t.all_placements() == {}
        assert not t.has_placed("anything")
        assert t.get("anything") is None

    @pytest.mark.unit
    def test_record_then_persist_then_load(self, tmp_path: Path):
        src = tmp_path / "src" / "foo.zip"
        src.parent.mkdir()
        src.touch()

        t = XfrStateTracker("p", tmp_path)
        t.record_placement("foo.zip", src)

        # File on disk should exist with our data
        assert (tmp_path / "xfr_p_state.json").exists()

        # Reload — fresh tracker reads previous state
        t2 = XfrStateTracker("p", tmp_path)
        assert t2.has_placed("foo.zip")
        assert t2.get("foo.zip").source_filename == "foo.zip"

    @pytest.mark.unit
    def test_unreadable_state_file_starts_fresh(self, tmp_path: Path):
        state_file = tmp_path / "xfr_p_state.json"
        state_file.write_text("{not valid json")
        t = XfrStateTracker("p", tmp_path)
        assert t.all_placements() == {}

    @pytest.mark.unit
    def test_state_file_with_bad_record_skipped(self, tmp_path: Path):
        state_file = tmp_path / "xfr_p_state.json"
        state_file.write_text(json.dumps({
            "good.zip": {
                "source_filename": "good.zip",
                "source_path": "/x/good.zip",
                "placed_at": "now",
            },
            "bad.zip": {"missing_required_keys": True},
        }))
        t = XfrStateTracker("p", tmp_path)
        assert "good.zip" in t.all_placements()
        assert "bad.zip" not in t.all_placements()

    @pytest.mark.unit
    def test_record_uses_explicit_timestamp(self, tmp_path: Path):
        src = tmp_path / "foo.zip"
        src.touch()
        t = XfrStateTracker("p", tmp_path)
        ts = datetime(2026, 1, 1, 12, 0, 0)
        placement = t.record_placement("foo.zip", src, placed_at=ts)
        assert placement.placed_at == "2026-01-01T12:00:00"

    @pytest.mark.unit
    def test_save_atomic_temp_cleanup_on_error(self, tmp_path: Path):
        src = tmp_path / "foo.zip"
        src.touch()
        t = XfrStateTracker("p", tmp_path)

        # Force os.replace to fail mid-save and verify no .tmp orphan remains.
        with patch("acoharmony._xfr.state.os.replace", side_effect=OSError("boom")):
            with pytest.raises(OSError, match="boom"):
                t.record_placement("foo.zip", src)

        leftovers = [p for p in tmp_path.iterdir() if p.name.endswith(".tmp")]
        assert leftovers == []

    @pytest.mark.unit
    def test_save_creates_tracking_dir_lazily(self, tmp_path: Path):
        nested = tmp_path / "sub" / "tracking"
        # Don't pre-create — tracker should mkdir on first save.
        t = XfrStateTracker("p", nested)
        src = tmp_path / "foo.zip"
        src.touch()
        t.record_placement("foo.zip", src)
        assert (nested / "xfr_p_state.json").exists()

    @pytest.mark.unit
    def test_state_file_unreadable_io_error(self, tmp_path: Path, monkeypatch):
        state_file = tmp_path / "xfr_p_state.json"
        state_file.write_text("{}")
        # Force the read to raise OSError
        original_read = Path.read_text

        def bad_read(self, *a, **k):
            if self == state_file:
                raise OSError("disk on fire")
            return original_read(self, *a, **k)

        monkeypatch.setattr(Path, "read_text", bad_read)
        t = XfrStateTracker("p", tmp_path)
        assert t.all_placements() == {}
