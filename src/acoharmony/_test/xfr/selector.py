# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _xfr.selector module."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._xfr.profile import (
    DirectoryVerifier,
    LiteralPatternRule,
    TransferProfile,
)
from acoharmony._xfr.selector import pending_only, select_files
from acoharmony._xfr.state import XfrStateTracker


def _profile(src: Path, dst: Path, verifier=None) -> TransferProfile:
    return TransferProfile(
        name="t",
        description="",
        source_dirs=(src,),
        destination=dst,
        source_rule=LiteralPatternRule(patterns=("*.zip",), date_floor=None),
        verifier=verifier,
    )


class TestSelectFiles:
    @pytest.mark.unit
    def test_pending_when_nothing_done(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").touch()
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        result = select_files(profile, tracker)
        assert len(result) == 1
        assert result[0].state == "pending"
        assert result[0].source_filename == "a.zip"
        assert result[0].dest_filename == "a.zip"

    @pytest.mark.unit
    def test_in_flight_when_we_placed_but_no_verifier_signal(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").touch()
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        tracker.record_placement("a.zip", tmp_dirs["src"] / "a.zip")
        result = select_files(profile, tracker)
        assert result[0].state == "in_flight"

    @pytest.mark.unit
    def test_placed_when_directory_verifier_sees_destination(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").touch()
        (tmp_dirs["dst"] / "a.zip").touch()
        profile = _profile(
            tmp_dirs["src"],
            tmp_dirs["dst"],
            verifier=DirectoryVerifier(destination=tmp_dirs["dst"]),
        )
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        tracker.record_placement("a.zip", tmp_dirs["src"] / "a.zip")
        result = select_files(profile, tracker)
        assert result[0].state == "placed"

    @pytest.mark.unit
    def test_sent_when_log_verifier_returns_sent(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").touch()

        class _StubVerifier:
            def state_for(self, name):
                return "sent" if name == "a.zip" else None

        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"], verifier=_StubVerifier())
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        result = select_files(profile, tracker)
        assert result[0].state == "sent"

    @pytest.mark.unit
    def test_archived_state_takes_precedence(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").touch()

        class _ArchiveVerifier:
            def state_for(self, name):
                return "archived"

        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"], verifier=_ArchiveVerifier())
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        tracker.record_placement("a.zip", tmp_dirs["src"] / "a.zip")
        result = select_files(profile, tracker)
        assert result[0].state == "archived"

    @pytest.mark.unit
    def test_skips_when_source_disappeared_between_listing_and_lookup(self, tmp_dirs):
        # Race: rule sees the file, but find_source_path returns None.
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        with patch.object(
            profile.source_rule, "applicable_filenames", return_value=["ghost.zip"]
        ):
            result = select_files(profile, tracker)
        assert result == []


class TestPendingOnly:
    @pytest.mark.unit
    def test_filters(self, tmp_dirs):
        (tmp_dirs["src"] / "a.zip").touch()
        (tmp_dirs["src"] / "b.zip").touch()
        profile = _profile(tmp_dirs["src"], tmp_dirs["dst"])
        tracker = XfrStateTracker("t", tmp_dirs["state"])
        tracker.record_placement("a.zip", tmp_dirs["src"] / "a.zip")
        result = select_files(profile, tracker)
        pending = pending_only(result)
        assert [p.source_filename for p in pending] == ["b.zip"]
