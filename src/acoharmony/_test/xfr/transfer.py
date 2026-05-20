# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _xfr.transfer module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._xfr.profile import (
    LiteralPatternRule,
    TransferProfile,
)
from acoharmony._xfr.state import XfrStateTracker
from acoharmony._xfr.transfer import (
    FileStatus,
    _copy,
    send_pending,
)


def _profile(src: Path, dst: Path, *, rename=None, verifier=None) -> TransferProfile:
    return TransferProfile(
        name="t",
        description="",
        source_dirs=(src,),
        destination=dst,
        source_rule=LiteralPatternRule(patterns=("*.zip",), date_floor=None),
        rename=rename,
        verifier=verifier,
    )


class TestCopy:
    @pytest.mark.unit
    def test_copies_content(self, tmp_path: Path):
        src = tmp_path / "src.zip"
        src.write_bytes(b"hello")
        dst = tmp_path / "out" / "dst.zip"
        _copy(src, dst)
        assert dst.read_bytes() == b"hello"

    @pytest.mark.unit
    def test_creates_parent(self, tmp_path: Path):
        src = tmp_path / "src.zip"
        src.write_bytes(b"x")
        dst = tmp_path / "deep" / "nest" / "dst.zip"
        _copy(src, dst)
        assert dst.exists()

    @pytest.mark.unit
    def test_no_tempfile_left_behind(self, tmp_path: Path):
        src = tmp_path / "src.zip"
        src.write_bytes(b"x")
        dst = tmp_path / "dst.zip"
        _copy(src, dst)
        leftovers = [p for p in tmp_path.iterdir() if p.name.endswith(".xfrtmp")]
        assert leftovers == []


class TestSendPending:
    @pytest.mark.unit
    def test_copies_pending(self, tmp_dirs):
        src = tmp_dirs["src"] / "a.zip"
        src.write_bytes(b"data")
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        records = send_pending(profile, tracker)
        assert len(records) == 1
        assert records[0].status == FileStatus.PLACED
        assert (tmp_dirs["dst"] / "a.zip").read_bytes() == b"data"
        assert tracker.has_placed("a.zip")

    @pytest.mark.unit
    def test_dry_run_does_not_copy_or_record(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").write_bytes(b"data")
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        records = send_pending(profile, tracker, dry_run=True)
        assert records[0].status == FileStatus.DRY_RUN
        assert not (tmp_dirs["dst"] / "a.zip").exists()
        assert not tracker.has_placed("a.zip")

    @pytest.mark.unit
    def test_skips_already_placed(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").write_bytes(b"data")
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        tracker.record_placement("a.zip", tmp_dirs["src"] / "a.zip")
        records = send_pending(profile, tracker)
        # Already in_flight (per state) — selector excludes from pending.
        assert records == []

    @pytest.mark.unit
    def test_foreign_destination_file_skipped_and_recorded(self, tmp_dirs):
        # File exists at destination but tracker doesn't know about it.
        (tmp_dirs["src"] / "a.zip").write_bytes(b"new")
        (tmp_dirs["dst"] / "a.zip").write_bytes(b"foreign")
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        records = send_pending(profile, tracker)
        assert len(records) == 1
        assert records[0].status == FileStatus.SKIPPED_DUPLICATE
        # Tracker now records it (so future runs treat as in_flight).
        assert tracker.has_placed("a.zip")
        # We did NOT overwrite the foreign content.
        assert (tmp_dirs["dst"] / "a.zip").read_bytes() == b"foreign"

    @pytest.mark.unit
    def test_error_recorded_continues_others(self, tmp_dirs):
        (tmp_dirs["src"] / "ok.zip").write_bytes(b"ok")
        (tmp_dirs["src"] / "bad.zip").write_bytes(b"bad")
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])

        original = __import__("acoharmony._xfr.transfer", fromlist=["_copy"])._copy

        def flaky(src, dest):
            if src.name == "bad.zip":
                raise OSError("disk full")
            return original(src, dest)

        with patch("acoharmony._xfr.transfer._copy", side_effect=flaky):
            records = send_pending(profile, tracker)

        statuses = {r.source.source_filename: r.status for r in records}
        assert statuses["ok.zip"] == FileStatus.PLACED
        assert statuses["bad.zip"] == FileStatus.ERROR
        bad_record = [r for r in records if r.source.source_filename == "bad.zip"][0]
        assert "disk full" in bad_record.error

    @pytest.mark.unit
    def test_rename_applied(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").write_bytes(b"data")
        profile = _profile(
            tmp_dirs["src"],
            tmp_dirs["dst"],
            rename=lambda n: f"renamed_{n}",
        )
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        records = send_pending(profile, tracker)
        assert (tmp_dirs["dst"] / "renamed_a.zip").exists()
        assert tracker.has_placed("renamed_a.zip")
        assert records[0].source.dest_filename == "renamed_a.zip"
