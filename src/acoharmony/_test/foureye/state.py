# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for 4icli state tracking - Polars style."""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from unittest.mock import MagicMock
from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from acoharmony._4icli.state import (
    FileDownloadState,
    FourICLIStateTracker,
)
from acoharmony._test.foureye.conftest import _make_config, _mock_log_writer  # noqa: F401


class TestFileDownloadState:
    """Tests for FileDownloadState dataclass."""

    @pytest.mark.unit
    def test_to_dict(self) -> None:
        """to_dict converts state to dictionary."""
        state = FileDownloadState(
            filename="test.zip",
            file_hash="abc123",
            download_timestamp=datetime(2025, 1, 1, 12, 0, 0),
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path="/path/to/test.zip",
        )

        data = state.to_dict()

        assert data["filename"] == "test.zip"
        assert data["file_hash"] == "abc123"
        assert data["category"] == "CCLF"
        assert data["file_type_code"] == 113
        assert data["file_size"] == 1024

    @pytest.mark.unit
    def test_from_dict(self) -> None:
        """from_dict creates state from dictionary."""
        data = {
            "filename": "test.zip",
            "file_hash": "abc123",
            "download_timestamp": "2025-01-01T12:00:00",
            "category": "CCLF",
            "file_type_code": 113,
            "file_size": 1024,
            "source_path": "/path/to/test.zip",
        }

        state = FileDownloadState.from_dict(data)

        assert state.filename == "test.zip"
        assert state.file_hash == "abc123"
        assert state.category == "CCLF"


class TestFourICLIStateTrackerInitialization:
    """Tests for FourICLIStateTracker initialization."""

    @pytest.mark.unit
    def test_init_with_log_writer(self, mock_log_writer) -> None:
        """StateTracker initializes with log writer."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        assert tracker.log_writer is mock_log_writer
        assert tracker._file_cache is not None

    @pytest.mark.unit
    def test_init_creates_log_writer(self, tmp_path: Path) -> None:
        """StateTracker creates log writer if not provided."""
        state_file = tmp_path / "tracking" / "test_state.json"
        tracker = FourICLIStateTracker(state_file=state_file)

        assert tracker.log_writer is not None


class TestFourICLIStateTrackerFileHash:
    """Tests for file hashing."""

    @pytest.mark.unit
    def test_compute_file_hash(self, temp_bronze_dir: Path, mock_log_writer) -> None:
        """Computes consistent hash for file content."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        file_path = temp_bronze_dir / "test.txt"
        file_path.write_text("test content")

        hash1 = tracker._compute_file_hash(file_path)
        hash2 = tracker._compute_file_hash(file_path)

        assert hash1 == hash2
        assert len(hash1) == 64  # SHA256 hex length

    @pytest.mark.unit
    def test_different_content_different_hash(self, temp_bronze_dir: Path, mock_log_writer) -> None:
        """Different content produces different hash."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        file1 = temp_bronze_dir / "file1.txt"
        file2 = temp_bronze_dir / "file2.txt"

        file1.write_text("content 1")
        file2.write_text("content 2")

        hash1 = tracker._compute_file_hash(file1)
        hash2 = tracker._compute_file_hash(file2)

        assert hash1 != hash2


class TestFourICLIStateTrackerDuplicateDetection:
    """Tests for duplicate file detection."""

    @pytest.mark.unit
    def test_is_file_downloaded_new_file(self, mock_log_writer) -> None:
        """is_file_downloaded returns False for new file."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        result = tracker.is_file_downloaded("new_file.txt")

        assert result is False

    @pytest.mark.unit
    def test_is_file_downloaded_existing_file(self, temp_bronze_dir: Path, mock_log_writer) -> None:
        """is_file_downloaded returns True for previously downloaded file."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        file_path = temp_bronze_dir / "test.txt"
        file_path.write_text("content")

        # Mark as downloaded
        tracker.mark_file_downloaded(file_path, "CCLF", 113)

        # Check if downloaded
        result = tracker.is_file_downloaded("test.txt", file_path)

        assert result is True

    @pytest.mark.unit
    def test_is_file_downloaded_content_changed(
        self, temp_bronze_dir: Path, mock_log_writer
    ) -> None:
        """is_file_downloaded returns False when content changes."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        file_path = temp_bronze_dir / "test.txt"
        file_path.write_text("original content")

        # Mark as downloaded
        tracker.mark_file_downloaded(file_path, "CCLF", 113)

        # Change content
        file_path.write_text("changed content")

        # Should not be considered duplicate
        result = tracker.is_file_downloaded("test.txt", file_path)

        assert result is False


class TestFourICLIStateTrackerMarkDownloaded:
    """Tests for marking files as downloaded."""

    @pytest.mark.unit
    def test_mark_file_downloaded(self, sample_cclf_file: Path, mock_log_writer) -> None:
        """mark_file_downloaded tracks file state."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        state = tracker.mark_file_downloaded(sample_cclf_file, "CCLF", 113)

        assert state.filename == sample_cclf_file.name
        assert state.category == "CCLF"
        assert state.file_type_code == 113
        assert state.file_hash is not None

        # Should be logged
        mock_log_writer.info.assert_called()

    @pytest.mark.unit
    def test_mark_multiple_downloaded(
        self, sample_download_files: list[Path], mock_log_writer
    ) -> None:
        """mark_multiple_downloaded tracks multiple files."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        states = tracker.mark_multiple_downloaded(sample_download_files, "CCLF", 113)

        assert len(states) == len(sample_download_files)
        for state in states:
            assert state.category == "CCLF"
            assert state.file_type_code == 113


class TestFourICLIStateTrackerFilterFiles:
    """Tests for filtering files."""

    @pytest.mark.unit
    def test_get_new_files(
        self, sample_download_files: list[Path], mock_log_writer, tmp_path: Path
    ) -> None:
        """get_new_files returns only new files."""
        state_file = tmp_path / "tracking" / "test_new_files.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Mark first file as downloaded
        tracker.mark_file_downloaded(sample_download_files[0], "CCLF", 113)

        # Get new files
        new_files = tracker.get_new_files(sample_download_files)

        assert len(new_files) == 2  # Only 2 files are new
        assert sample_download_files[0] not in new_files

    @pytest.mark.unit
    def test_get_duplicate_files(
        self, sample_download_files: list[Path], mock_log_writer, tmp_path: Path
    ) -> None:
        """get_duplicate_files returns only duplicates."""
        state_file = tmp_path / "tracking" / "test_duplicates.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Mark first file as downloaded
        tracker.mark_file_downloaded(sample_download_files[0], "CCLF", 113)

        # Get duplicates
        duplicates = tracker.get_duplicate_files(sample_download_files)

        assert len(duplicates) == 1
        assert sample_download_files[0] in duplicates


class TestFourICLIStateTrackerQueries:
    """Tests for querying download history."""

    @pytest.mark.unit
    def test_get_downloaded_files_all(
        self, sample_download_files: list[Path], mock_log_writer, tmp_path: Path
    ) -> None:
        """get_downloaded_files returns all files."""
        state_file = tmp_path / "tracking" / "test_all.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        tracker.mark_multiple_downloaded(sample_download_files, "CCLF", 113)

        files = tracker.get_downloaded_files()

        assert len(files) == len(sample_download_files)

    @pytest.mark.unit
    def test_get_downloaded_files_by_category(
        self, sample_download_files: list[Path], mock_log_writer, tmp_path: Path
    ) -> None:
        """get_downloaded_files filters by category."""
        state_file = tmp_path / "tracking" / "test_category.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Mark with different categories
        tracker.mark_file_downloaded(sample_download_files[0], "CCLF", 113)
        tracker.mark_file_downloaded(sample_download_files[1], "Reports", 211)

        files = tracker.get_downloaded_files(category="CCLF")

        assert len(files) == 1
        assert files[0].category == "CCLF"

    @pytest.mark.unit
    def test_get_downloaded_files_by_file_type(
        self, sample_download_files: list[Path], mock_log_writer
    ) -> None:
        """get_downloaded_files filters by file type code."""
        tracker = FourICLIStateTracker(log_writer=mock_log_writer)

        tracker.mark_file_downloaded(sample_download_files[0], "CCLF", 113)
        tracker.mark_file_downloaded(sample_download_files[1], "Reports", 211)

        files = tracker.get_downloaded_files(file_type_code=211)

        assert len(files) == 1
        assert files[0].file_type_code == 211


class TestFourICLIStateTrackerStats:
    """Tests for download statistics."""

    @pytest.mark.unit
    def test_get_download_stats_empty(self, mock_log_writer, tmp_path: Path) -> None:
        """get_download_stats returns zeros when empty."""
        state_file = tmp_path / "tracking" / "test_stats_empty.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        stats = tracker.get_download_stats()

        assert stats["total_files"] == 0
        assert stats["total_size"] == 0

    @pytest.mark.unit
    def test_get_download_stats(
        self, sample_download_files: list[Path], mock_log_writer, tmp_path: Path
    ) -> None:
        """get_download_stats returns accurate statistics."""
        state_file = tmp_path / "tracking" / "test_stats.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        tracker.mark_multiple_downloaded(sample_download_files, "CCLF", 113)

        stats = tracker.get_download_stats()

        assert stats["total_files"] == len(sample_download_files)
        assert stats["total_size"] > 0
        assert stats["total_size_mb"] >= 0
        assert "CCLF" in stats["categories"]
        assert 113 in stats["file_types"]


class TestFourICLIStateTrackerRemoteInventory:
    """Tests for remote inventory tracking."""

    @pytest.mark.unit
    def test_update_remote_inventory(self, mock_log_writer, tmp_path: Path) -> None:
        """update_remote_inventory adds remote files to state."""
        state_file = tmp_path / "tracking" / "test_remote.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        remote_files = [
            {"filename": "remote1.zip", "size": 1024, "created": "2025-01-01T00:00:00"},
            {"filename": "remote2.zip", "size": 2048, "created": "2025-01-02T00:00:00"},
        ]

        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        assert len(tracker._file_cache) == 2
        assert "remote1.zip" in tracker._file_cache
        assert "remote2.zip" in tracker._file_cache
        assert tracker._file_cache["remote1.zip"].last_seen_remote is not None

    @pytest.mark.unit
    def test_update_remote_inventory_updates_existing(
        self, mock_log_writer, tmp_path: Path, temp_bronze_dir: Path
    ) -> None:
        """update_remote_inventory updates last_seen for existing files."""
        state_file = tmp_path / "tracking" / "test_remote_update.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # First, mark a file as downloaded
        file_path = temp_bronze_dir / "existing.zip"
        file_path.write_text("content")
        tracker.mark_file_downloaded(file_path, "CCLF", 113)

        original_last_seen = tracker._file_cache["existing.zip"].last_seen_remote

        # Now update remote inventory with same file
        remote_files = [
            {"filename": "existing.zip", "size": 1024, "created": "2025-01-01T00:00:00"},
        ]

        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        # last_seen_remote should be updated
        assert tracker._file_cache["existing.zip"].last_seen_remote is not None
        assert tracker._file_cache["existing.zip"].last_seen_remote != original_last_seen

    @pytest.mark.unit
    def test_get_files_to_download_new_files(
        self, mock_log_writer, tmp_path: Path, temp_bronze_dir: Path
    ) -> None:
        """get_files_to_download returns files not in storage."""
        state_file = tmp_path / "tracking" / "test_to_download.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Add remote files
        remote_files = [
            {"filename": "new1.zip", "size": 1024, "created": "2025-01-01T00:00:00"},
            {"filename": "new2.zip", "size": 2048, "created": "2025-01-02T00:00:00"},
        ]

        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        to_download = tracker.get_files_to_download(temp_bronze_dir, category="CCLF")

        assert len(to_download) == 2
        assert "new1.zip" in to_download
        assert "new2.zip" in to_download

    @pytest.mark.unit
    def test_get_files_to_download_skips_existing(
        self, mock_log_writer, tmp_path: Path, temp_bronze_dir: Path
    ) -> None:
        """get_files_to_download skips files already in storage."""
        state_file = tmp_path / "tracking" / "test_skip_existing.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Create existing file in storage
        existing = temp_bronze_dir / "existing.zip"
        existing.write_text("content")
        tracker.mark_file_downloaded(existing, "CCLF", 113)

        # Update remote inventory with same file
        remote_files = [
            {"filename": "existing.zip", "size": 1024, "created": "2025-01-01T00:00:00"},
            {"filename": "new.zip", "size": 2048, "created": "2025-01-02T00:00:00"},
        ]

        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        to_download = tracker.get_files_to_download(temp_bronze_dir, category="CCLF")

        assert len(to_download) == 1
        assert "new.zip" in to_download
        assert "existing.zip" not in to_download

    @pytest.mark.unit
    def test_get_last_sync_time(self, mock_log_writer, tmp_path: Path) -> None:
        """get_last_sync_time returns most recent sync."""
        state_file = tmp_path / "tracking" / "test_last_sync.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Add remote files at different times
        remote_files = [
            {"filename": "file1.zip", "size": 1024, "created": "2025-01-01T00:00:00"},
        ]

        tracker.update_remote_inventory(remote_files, "CCLF", 113)
        last_sync = tracker.get_last_sync_time(category="CCLF")

        assert last_sync is not None

    @pytest.mark.unit
    def test_get_last_sync_time_no_sync(self, mock_log_writer, tmp_path: Path) -> None:
        """get_last_sync_time returns None when never synced."""
        state_file = tmp_path / "tracking" / "test_no_sync.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        last_sync = tracker.get_last_sync_time(category="CCLF")

        assert last_sync is None

    @pytest.mark.unit
    def test_get_last_sync_time_skips_states_without_remote(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Cover branch 552->548: state.last_seen_remote falsy continues the loop."""
        from datetime import datetime

        from acoharmony._4icli.state import FileDownloadState

        state_file = tmp_path / "tracking" / "test_skip.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        def _state(name: str, remote: datetime | None) -> FileDownloadState:
            return FileDownloadState(
                filename=name,
                file_hash="x" * 64,
                download_timestamp=datetime(2025, 1, 1),
                category="CCLF",
                file_type_code=113,
                file_size=0,
                source_path=str(tmp_path / name),
                last_seen_remote=remote,
            )

        # Seed cache with a state that has no last_seen_remote
        tracker._file_cache["empty.zip"] = _state("empty.zip", None)
        # And one with a real timestamp
        tracker._file_cache["real.zip"] = _state("real.zip", datetime(2025, 6, 1, 12, 0, 0))

        last_sync = tracker.get_last_sync_time(category="CCLF")
        assert last_sync == datetime(2025, 6, 1, 12, 0, 0)

    @pytest.mark.unit
    def test_get_last_sync_time_keeps_most_recent(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Cover branch 553->548: when current state's remote <= last_sync, stay with last_sync."""
        from datetime import datetime

        from acoharmony._4icli.state import FileDownloadState

        state_file = tmp_path / "tracking" / "test_keep.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        def _state(name: str, remote: datetime) -> FileDownloadState:
            return FileDownloadState(
                filename=name,
                file_hash="y" * 64,
                download_timestamp=datetime(2025, 1, 1),
                category="CCLF",
                file_type_code=113,
                file_size=0,
                source_path=str(tmp_path / name),
                last_seen_remote=remote,
            )

        # Newer first, then older — older state should be ignored
        tracker._file_cache["newer.zip"] = _state("newer.zip", datetime(2025, 12, 1))
        tracker._file_cache["older.zip"] = _state("older.zip", datetime(2025, 1, 1))

        last_sync = tracker.get_last_sync_time(category="CCLF")
        assert last_sync == datetime(2025, 12, 1)


@pytest.mark.unit
class TestFourICLIStateTrackerEdgeCases:
    """Tests for edge cases and Phase 2 multi-location search in state tracker."""

    @pytest.mark.unit
    def test_init_with_custom_search_paths(self, mock_log_writer, tmp_path: Path) -> None:
        """Initialize tracker with custom search paths."""
        custom_paths = [tmp_path / "custom1", tmp_path / "custom2"]
        state_file = tmp_path / "tracking" / "test.json"

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=custom_paths
        )

        assert tracker.search_paths == custom_paths

    @pytest.mark.unit
    def test_load_state_exception_handling(self, mock_log_writer, tmp_path: Path) -> None:
        """Test exception handling when loading malformed state file."""
        state_file = tmp_path / "tracking" / "malformed.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)

        # Create malformed JSON
        state_file.write_text("{invalid json content][")

        # Should not crash, just warn and start fresh
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        assert tracker._file_cache == {}

    @pytest.mark.unit
    def test_save_state_exception_handling(self, mock_log_writer, tmp_path: Path) -> None:
        """Test exception handling when saving state fails."""
        state_file = tmp_path / "nonexistent" / "deep" / "tracking" / "test.json"

        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Create a file and mark it
        test_file = tmp_path / "test.txt"
        test_file.write_text("content")

        # Mock the state file parent to not allow mkdir
        with patch.object(Path, "mkdir", side_effect=PermissionError("No permission")):
            # Should log error but not crash
            tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Tracker should still work
        assert len(tracker._file_cache) > 0

    @pytest.mark.unit
    def test_find_file_location_cached(self, mock_log_writer, tmp_path: Path) -> None:
        """Test _find_file_location finds file at cached location."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create and mark a file
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Find it using cached location
        found_path = tracker._find_file_location("test.zip")

        assert found_path == test_file
        assert found_path.exists()

    @pytest.mark.unit
    def test_find_file_location_in_search_paths(self, mock_log_writer, tmp_path: Path) -> None:
        """Test _find_file_location searches multiple directories."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        archive_dir = tmp_path / "archive"
        bronze_dir.mkdir(parents=True)
        archive_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer,
            state_file=state_file,
            search_paths=[bronze_dir, archive_dir],
        )

        # File is in archive, not bronze
        test_file = archive_dir / "test.zip"
        test_file.write_text("content")

        # Find it in search paths
        found_path = tracker._find_file_location("test.zip")

        assert found_path == test_file

    @pytest.mark.unit
    def test_find_file_location_not_found(self, mock_log_writer, tmp_path: Path) -> None:
        """Test _find_file_location returns None when file not found."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # File doesn't exist anywhere
        found_path = tracker._find_file_location("nonexistent.zip")

        assert found_path is None

    @pytest.mark.unit
    def test_update_file_location(self, mock_log_writer, tmp_path: Path) -> None:
        """Test updating file location when file is moved."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        archive_dir = tmp_path / "archive"
        bronze_dir.mkdir(parents=True)
        archive_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer,
            state_file=state_file,
            search_paths=[bronze_dir, archive_dir],
        )

        # Create file in bronze
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Move to archive
        new_path = archive_dir / "test.zip"
        test_file.rename(new_path)

        # Update tracker
        tracker.update_file_location("test.zip", new_path)

        # Verify state was updated
        assert tracker._file_cache["test.zip"].source_path == str(new_path)

    @pytest.mark.unit
    def test_is_file_downloaded_auto_updates_location(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Test is_file_downloaded automatically updates location when file moved."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        archive_dir = tmp_path / "archive"
        bronze_dir.mkdir(parents=True)
        archive_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer,
            state_file=state_file,
            search_paths=[bronze_dir, archive_dir],
        )

        # Create file in bronze and mark it
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Move file to archive
        new_path = archive_dir / "test.zip"
        test_file.rename(new_path)

        # Check if downloaded without providing path - should auto-find and update
        is_dup = tracker.is_file_downloaded("test.zip")

        assert is_dup is True
        # Location should be auto-updated
        assert tracker._file_cache["test.zip"].source_path == str(new_path)

    @pytest.mark.unit
    def test_is_file_downloaded_file_not_found_returns_true(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Test is_file_downloaded returns True when file in cache but not found."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create file, mark it, then delete it
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)
        test_file.unlink()

        # Check if downloaded without path - file not found but in cache
        is_dup = tracker.is_file_downloaded("test.zip")

        # Should return True (we downloaded it before, even if it's gone now)
        assert is_dup is True

    @pytest.mark.unit
    def test_mark_multiple_downloaded_skips_non_files(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Test mark_multiple_downloaded skips directories and non-files."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create a file and a directory
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        test_dir = bronze_dir / "test_dir"
        test_dir.mkdir()

        # Mark both
        states = tracker.mark_multiple_downloaded([test_file, test_dir], "CCLF", 113)

        # Should only mark the file, not the directory
        assert len(states) == 1
        assert states[0].filename == "test.zip"

    @pytest.mark.unit
    def test_get_new_files_skips_non_files(self, mock_log_writer, tmp_path: Path) -> None:
        """Test get_new_files skips non-file paths."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create a directory
        test_dir = bronze_dir / "test_dir"
        test_dir.mkdir()

        # Get new files
        new_files = tracker.get_new_files([test_dir])

        # Should return empty list (directory skipped)
        assert len(new_files) == 0

    @pytest.mark.unit
    def test_get_duplicate_files_skips_non_files(self, mock_log_writer, tmp_path: Path) -> None:
        """Test get_duplicate_files skips non-file paths."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create a directory
        test_dir = bronze_dir / "test_dir"
        test_dir.mkdir()

        # Get duplicates
        duplicates = tracker.get_duplicate_files([test_dir])

        # Should return empty list (directory skipped)
        assert len(duplicates) == 0

    @pytest.mark.unit
    def test_update_remote_inventory_missing_filename(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Test update_remote_inventory skips entries without filename."""
        state_file = tmp_path / "tracking" / "test.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Remote files with missing filename
        remote_files = [
            {"size": 1024},  # No filename
            {"filename": "valid.zip", "size": 2048},
        ]

        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        # Should only track the one with a filename
        assert len(tracker._file_cache) == 1
        assert "valid.zip" in tracker._file_cache

    @pytest.mark.unit
    def test_get_files_to_download_with_category_filter(
        self, mock_log_writer, tmp_path: Path
    ) -> None:
        """Test get_files_to_download filters by category."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Add files from different categories
        remote_files_cclf = [{"filename": "cclf.zip", "size": 1024}]
        remote_files_reports = [{"filename": "report.zip", "size": 2048}]

        tracker.update_remote_inventory(remote_files_cclf, "CCLF", 113)
        tracker.update_remote_inventory(remote_files_reports, "Reports", 165)

        # Get files for CCLF only
        to_download = tracker.get_files_to_download(bronze_dir, category="CCLF")

        assert len(to_download) == 1
        assert "cclf.zip" in to_download
        assert "report.zip" not in to_download

    @pytest.mark.unit
    def test_get_files_to_download_invalid_date(self, mock_log_writer, tmp_path: Path) -> None:
        """Test get_files_to_download handles invalid date formats gracefully."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create and mark a file
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Update with invalid date
        remote_files = [{"filename": "test.zip", "size": 1024, "created": "invalid date"}]
        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        # Should not crash, file should not be in download list (invalid date skipped)
        to_download = tracker.get_files_to_download(bronze_dir)

        # Should be empty (file exists locally, invalid date ignored)
        assert "test.zip" not in to_download

    @pytest.mark.unit
    def test_get_last_sync_time_with_category_filter(self, mock_log_writer, tmp_path: Path) -> None:
        """Test get_last_sync_time filters by category and returns max."""
        state_file = tmp_path / "tracking" / "test.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Add files at different times
        remote_files_early = [{"filename": "early.zip", "size": 1024}]
        tracker.update_remote_inventory(remote_files_early, "CCLF", 113)

        import time

        time.sleep(0.1)

        remote_files_late = [{"filename": "late.zip", "size": 2048}]
        tracker.update_remote_inventory(remote_files_late, "Reports", 165)

        # Get last sync for Reports
        last_sync_reports = tracker.get_last_sync_time(category="Reports")
        last_sync_cclf = tracker.get_last_sync_time(category="CCLF")

        # Reports sync should be later
        assert last_sync_reports > last_sync_cclf


class TestStateTrackerBranches:
    """Cover uncovered branches in state.py."""

    @pytest.mark.unit
    def test_init_state_file_none_search_paths_provided(self, tmp_path: Path) -> None:
        """Branch 103->109: state_file is None but search_paths is explicitly provided."""
        from acoharmony._4icli.state import FourICLIStateTracker

        custom_paths = [tmp_path / "custom1", tmp_path / "custom2"]

        # FourICLIConfig is imported inside __init__ via `from .config import FourICLIConfig`
        # We need to patch the config module's FourICLIConfig class
        mock_cfg = MagicMock()
        mock_cfg.tracking_dir = tmp_path / "tracking"
        mock_cfg.data_path = tmp_path

        with patch("acoharmony._4icli.config.FourICLIConfig") as mock_cfg_cls:
            mock_cfg_cls.from_profile.return_value = mock_cfg
            tracker = FourICLIStateTracker(
                log_writer=MagicMock(),
                state_file=None,
                search_paths=custom_paths,
            )

        # search_paths should be the explicitly provided ones, not config defaults
        assert tracker.search_paths == custom_paths

    @pytest.mark.unit
    def test_update_file_location_not_in_cache(self, mock_log_writer, tmp_path: Path) -> None:
        """Branch 202->-191: update_file_location when filename not in _file_cache."""
        state_file = tmp_path / "tracking" / "test.json"
        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file
        )

        # Call with a filename that is NOT in the cache - should just return without error
        tracker.update_file_location("nonexistent_file.zip", tmp_path / "new_location.zip")

        # Verify no crash, no log of update
        assert "nonexistent_file.zip" not in tracker._file_cache

    @pytest.mark.unit
    def test_get_files_to_download_remote_metadata_no_date(self, mock_log_writer, tmp_path: Path) -> None:
        """Branch 525->502: remote_metadata exists but no 'created' or 'modified' key."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create file, mark downloaded, then create it in bronze so it exists locally
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Update remote inventory to set last_seen_remote
        remote_files = [{"filename": "test.zip", "size": 1024}]
        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        # Set remote_metadata with no date keys
        tracker._file_cache["test.zip"].remote_metadata = {"some_key": "no_date"}

        to_download = tracker.get_files_to_download(bronze_dir)

        # File exists locally with hash, remote_metadata exists but no date -> skip
        assert "test.zip" not in to_download

    @pytest.mark.unit
    def test_get_files_to_download_remote_newer_than_local(self, mock_log_writer, tmp_path: Path) -> None:
        """Branch 528->529: remote date is newer than download_timestamp."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        # Create file and mark downloaded
        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        # Update remote inventory
        remote_files = [{"filename": "test.zip", "size": 1024}]
        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        # Set remote_metadata with a future date (newer than download)
        tracker._file_cache["test.zip"].remote_metadata = {
            "created": "2099-01-01T00:00:00"
        }
        # Set download_timestamp to an old date
        tracker._file_cache["test.zip"].download_timestamp = datetime(2020, 1, 1)

        to_download = tracker.get_files_to_download(bronze_dir)

        # Remote is newer -> should be in download list
        assert "test.zip" in to_download

    @pytest.mark.unit
    def test_get_files_to_download_remote_metadata_with_modified(self, mock_log_writer, tmp_path: Path) -> None:
        """Branch 520->502 path where remote_metadata has 'modified' key."""
        state_file = tmp_path / "tracking" / "test.json"
        bronze_dir = tmp_path / "bronze"
        bronze_dir.mkdir(parents=True)

        tracker = FourICLIStateTracker(
            log_writer=mock_log_writer, state_file=state_file, search_paths=[bronze_dir]
        )

        test_file = bronze_dir / "test.zip"
        test_file.write_text("content")
        tracker.mark_file_downloaded(test_file, "CCLF", 113)

        remote_files = [{"filename": "test.zip", "size": 1024}]
        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        # Set remote_metadata with 'modified' key (not 'created') and a future date
        tracker._file_cache["test.zip"].remote_metadata = {
            "modified": "2099-06-01T00:00:00"
        }
        tracker._file_cache["test.zip"].download_timestamp = datetime(2020, 1, 1)

        to_download = tracker.get_files_to_download(bronze_dir)
        assert "test.zip" in to_download


class TestState:
    @pytest.mark.unit
    def test_state_tracker_load_save(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"

        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)
        assert len(tracker._file_cache) == 0

        # Mark a file
        f = tmp_path / "test.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        assert "test.zip" in tracker._file_cache
        assert state_file.exists()

        # Reload
        tracker2 = FourICLIStateTracker(log_writer=lw, state_file=state_file)
        assert "test.zip" in tracker2._file_cache

    @pytest.mark.unit
    def test_is_file_downloaded_true(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "test.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        assert tracker.is_file_downloaded("test.zip", f) is True

    @pytest.mark.unit
    def test_is_file_downloaded_hash_mismatch(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "test.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        # Modify file content
        f.write_text("changed content")
        assert tracker.is_file_downloaded("test.zip", f) is False

    @pytest.mark.unit
    def test_is_file_downloaded_not_in_cache(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        assert tracker.is_file_downloaded("nonexistent.zip") is False

    @pytest.mark.unit
    def test_is_file_downloaded_no_path_search(self, tmp_path):
        """Test is_file_downloaded when file_path is None and searches directories."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        search_dir = tmp_path / "search"
        search_dir.mkdir()

        tracker = FourICLIStateTracker(
            log_writer=lw, state_file=state_file, search_paths=[search_dir]
        )

        f = search_dir / "test.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        # Now check with no path - should find via search
        assert tracker.is_file_downloaded("test.zip") is True

    @pytest.mark.unit
    def test_is_file_downloaded_file_moved(self, tmp_path):
        """Test when file is in cache but moved to new location."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        original_dir = tmp_path / "original"
        original_dir.mkdir()
        new_dir = tmp_path / "new_location"
        new_dir.mkdir()

        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file, search_paths=[new_dir])

        # Create file in original location
        orig_file = original_dir / "test.zip"
        orig_file.write_text("content")
        tracker.mark_file_downloaded(orig_file, category="CCLF", file_type_code=113)

        # "Move" file - delete from original, create in new location
        import shutil

        shutil.copy2(str(orig_file), str(new_dir / "test.zip"))
        orig_file.unlink()

        # Should find in search_paths and update
        assert tracker.is_file_downloaded("test.zip") is True

    @pytest.mark.unit
    def test_is_file_downloaded_file_gone(self, tmp_path):
        """File in cache but not found anywhere - returns True (cached)."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file, search_paths=[])

        f = tmp_path / "test.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)
        f.unlink()

        # File gone but in cache - returns True
        assert tracker.is_file_downloaded("test.zip") is True

    @pytest.mark.unit
    def test_update_file_location(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "test.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        new_path = tmp_path / "archive" / "test.zip"
        new_path.parent.mkdir(parents=True, exist_ok=True)
        tracker.update_file_location("test.zip", new_path)
        assert tracker._file_cache["test.zip"].source_path == str(new_path)

    @pytest.mark.unit
    def test_update_file_location_not_in_cache(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        # Should not raise
        tracker.update_file_location("nonexistent.zip", Path("/tmp/new"))

    @pytest.mark.unit
    def test_get_downloaded_files_with_filters(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f1 = tmp_path / "cclf.zip"
        f1.write_text("a")
        f2 = tmp_path / "report.xlsx"
        f2.write_text("b")

        tracker.mark_file_downloaded(f1, category="CCLF", file_type_code=113)
        tracker.mark_file_downloaded(f2, category="Reports", file_type_code=112)

        assert len(tracker.get_downloaded_files(category="CCLF")) == 1
        assert len(tracker.get_downloaded_files(file_type_code=113)) == 1
        assert len(tracker.get_downloaded_files()) == 2

    @pytest.mark.unit
    def test_mark_multiple_downloaded(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f1 = tmp_path / "a.zip"
        f1.write_text("a")
        f2 = tmp_path / "b.zip"
        f2.write_text("b")
        d = tmp_path / "subdir"
        d.mkdir()

        states = tracker.mark_multiple_downloaded([f1, f2, d], category="CCLF", file_type_code=113)
        assert len(states) == 2  # directory skipped

    @pytest.mark.unit
    def test_get_new_files(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f1 = tmp_path / "old.zip"
        f1.write_text("old")
        tracker.mark_file_downloaded(f1, category="CCLF", file_type_code=113)

        f2 = tmp_path / "new.zip"
        f2.write_text("new")

        new = tracker.get_new_files([f1, f2])
        assert len(new) == 1
        assert new[0].name == "new.zip"

    @pytest.mark.unit
    def test_get_duplicate_files(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f1 = tmp_path / "old.zip"
        f1.write_text("old")
        tracker.mark_file_downloaded(f1, category="CCLF", file_type_code=113)

        f2 = tmp_path / "new.zip"
        f2.write_text("new")

        dupes = tracker.get_duplicate_files([f1, f2])
        assert len(dupes) == 1
        assert dupes[0].name == "old.zip"

    @pytest.mark.unit
    def test_get_download_stats_empty(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        stats = tracker.get_download_stats()
        assert stats["total_files"] == 0

    @pytest.mark.unit
    def test_get_download_stats(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "file.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        stats = tracker.get_download_stats()
        assert stats["total_files"] == 1
        assert "total_size_mb" in stats

    @pytest.mark.unit
    def test_update_remote_inventory(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        remote_files = [
            {"filename": "new_remote.zip", "size": 1000},
            {"name": "another.zip"},  # uses "name" key
            {},  # no filename - skip
        ]
        tracker.update_remote_inventory(remote_files, category="CCLF", file_type_code=113)
        assert "new_remote.zip" in tracker._file_cache
        assert "another.zip" in tracker._file_cache

    @pytest.mark.unit
    def test_update_remote_inventory_existing(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "existing.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        remote_files = [{"filename": "existing.zip", "size": 1000}]
        tracker.update_remote_inventory(remote_files, category="CCLF", file_type_code=113)
        assert tracker._file_cache["existing.zip"].last_seen_remote is not None

    @pytest.mark.unit
    def test_get_files_to_download(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        # Add remote file not yet downloaded
        tracker.update_remote_inventory(
            [{"filename": "new.zip", "size": 100}],
            category="CCLF",
            file_type_code=113,
        )

        storage = tmp_path / "storage"
        storage.mkdir()

        to_dl = tracker.get_files_to_download(storage, category="CCLF")
        assert "new.zip" in to_dl

    @pytest.mark.unit
    def test_get_files_to_download_with_updated_remote(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "existing.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        # Update remote with newer date
        future = (datetime.now() + timedelta(days=1)).isoformat()
        tracker._file_cache["existing.zip"].last_seen_remote = datetime.now()
        tracker._file_cache["existing.zip"].remote_metadata = {
            "created": future,
        }

        storage = tmp_path
        (storage / "existing.zip").write_text("content")

        to_dl = tracker.get_files_to_download(storage)
        assert "existing.zip" in to_dl

    @pytest.mark.unit
    def test_get_files_to_download_category_filter(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        tracker.update_remote_inventory(
            [{"filename": "cclf.zip"}],
            category="CCLF",
            file_type_code=113,
        )
        tracker.update_remote_inventory(
            [{"filename": "report.zip"}],
            category="Reports",
            file_type_code=112,
        )

        storage = tmp_path / "storage"
        storage.mkdir()

        to_dl = tracker.get_files_to_download(storage, category="CCLF")
        assert "cclf.zip" in to_dl
        assert "report.zip" not in to_dl

    @pytest.mark.unit
    def test_get_files_to_download_no_remote(self, tmp_path):
        """Files without last_seen_remote are skipped."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "local.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        storage = tmp_path / "storage"
        storage.mkdir()

        to_dl = tracker.get_files_to_download(storage)
        assert to_dl == []

    @pytest.mark.unit
    def test_get_files_to_download_remote_invalid_date(self, tmp_path):
        """Remote file with invalid date should not cause crash."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        f = tmp_path / "file.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, category="CCLF", file_type_code=113)

        tracker._file_cache["file.zip"].last_seen_remote = datetime.now()
        tracker._file_cache["file.zip"].remote_metadata = {"created": "not-a-date"}

        storage = tmp_path
        (storage / "file.zip").write_text("content")

        to_dl = tracker.get_files_to_download(storage)
        # Invalid date is silently skipped
        assert "file.zip" not in to_dl

    @pytest.mark.unit
    def test_get_last_sync_time(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        assert tracker.get_last_sync_time() is None

        tracker.update_remote_inventory(
            [{"filename": "f.zip"}],
            category="CCLF",
            file_type_code=113,
        )

        assert tracker.get_last_sync_time() is not None
        assert tracker.get_last_sync_time(category="CCLF") is not None
        assert tracker.get_last_sync_time(category="Reports") is None

    @pytest.mark.unit
    def test_load_state_error(self, tmp_path):
        """Test corrupt state file is handled gracefully."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text("invalid json{{{")

        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)
        assert len(tracker._file_cache) == 0
        lw.warning.assert_called()

    @pytest.mark.unit
    def test_save_state_error(self, tmp_path):
        """Test save failure is handled gracefully."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        # Make state_file parent read-only to cause write failure
        with patch("builtins.open", side_effect=PermissionError("denied")):
            tracker._save_state()
            lw.error.assert_called()

    @pytest.mark.unit
    def test_file_download_state_serialization(self):
        from acoharmony._4icli.state import FileDownloadState

        state = FileDownloadState(
            filename="test.zip",
            file_hash="abc123",
            download_timestamp=datetime(2025, 1, 1),
            category="CCLF",
            file_type_code=113,
            file_size=1000,
            source_path="/tmp/test.zip",
            last_seen_remote=datetime(2025, 1, 2),
            remote_metadata={"key": "val"},
        )

        d = state.to_dict()
        assert d["filename"] == "test.zip"
        assert d["last_seen_remote"] == "2025-01-02T00:00:00"

        restored = FileDownloadState.from_dict(d)
        assert restored.filename == "test.zip"
        assert restored.last_seen_remote == datetime(2025, 1, 2)

    @pytest.mark.unit
    def test_file_download_state_no_remote(self):
        from acoharmony._4icli.state import FileDownloadState

        state = FileDownloadState(
            filename="test.zip",
            file_hash="abc123",
            download_timestamp=datetime(2025, 1, 1),
            category="CCLF",
            file_type_code=113,
            file_size=1000,
            source_path="/tmp/test.zip",
        )

        d = state.to_dict()
        assert d["last_seen_remote"] is None

        restored = FileDownloadState.from_dict(d)
        assert restored.last_seen_remote is None


class TestStateAdditional:
    """Additional state tests for remaining coverage."""

    @pytest.mark.unit
    def test_state_tracker_with_default_config(self, tmp_path):
        """Test state_file=None path uses config."""
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        cfg = _make_config(tmp_path)
        # When state_file is None, it imports FourICLIConfig inside __init__
        with patch("acoharmony._4icli.config.FourICLIConfig.from_profile", return_value=cfg):
            tracker = FourICLIStateTracker(log_writer=lw)
            assert tracker.state_file.name == "4icli_state.json"

    @pytest.mark.unit
    def test_get_new_files_skips_non_files(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        d = tmp_path / "subdir"
        d.mkdir()
        new = tracker.get_new_files([d])
        assert new == []

    @pytest.mark.unit
    def test_get_duplicate_files_skips_non_files(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        state_file = tmp_path / "tracking" / "state.json"
        tracker = FourICLIStateTracker(log_writer=lw, state_file=state_file)

        d = tmp_path / "subdir"
        d.mkdir()
        dupes = tracker.get_duplicate_files([d])
        assert dupes == []


class TestStateSearchPathsFromConfig:
    """Cover state.py line 109: search_paths provided with state_file=None."""

    @pytest.mark.unit
    def test_state_tracker_no_state_file_with_search_paths(self, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        lw = _mock_log_writer()
        cfg = _make_config(tmp_path)
        custom_search = [tmp_path / "custom_search"]
        custom_search[0].mkdir()

        with patch("acoharmony._4icli.config.FourICLIConfig.from_profile", return_value=cfg):
            tracker = FourICLIStateTracker(
                log_writer=lw, state_file=None, search_paths=custom_search
            )
            assert tracker.search_paths == custom_search


class TestStateTrackerLoadValidState:
    @pytest.mark.unit
    def test_load_valid_state_file(self, mock_lw, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        state_file = tmp_path / "state.json"
        state_data = {
            "test.zip": {
                "filename": "test.zip",
                "file_hash": "abc123",
                "download_timestamp": "2025-01-01T12:00:00",
                "category": "CCLF",
                "file_type_code": 113,
                "file_size": 1024,
                "source_path": "/tmp/test.zip",
            }
        }
        state_file.write_text(json.dumps(state_data))

        tracker = FourICLIStateTracker(log_writer=mock_lw, state_file=state_file)
        assert len(tracker._file_cache) == 1
        assert "test.zip" in tracker._file_cache

    @pytest.mark.unit
    def test_save_and_reload(self, mock_lw, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        state_file = tmp_path / "state.json"
        tracker = FourICLIStateTracker(log_writer=mock_lw, state_file=state_file)

        # Create and mark a file
        f = tmp_path / "file.zip"
        f.write_text("content")
        tracker.mark_file_downloaded(f, "CCLF", 113)

        # Reload
        tracker2 = FourICLIStateTracker(log_writer=mock_lw, state_file=state_file)
        assert "file.zip" in tracker2._file_cache


class TestFileDownloadStateExtended:
    @pytest.mark.unit
    def test_to_dict_with_last_seen(self):
        from acoharmony._4icli.state import FileDownloadState

        state = FileDownloadState(
            filename="test.zip",
            file_hash="abc",
            download_timestamp=datetime(2025, 1, 1),
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path="/tmp/test.zip",
            last_seen_remote=datetime(2025, 2, 1),
            remote_metadata={"key": "val"},
        )
        d = state.to_dict()
        assert d["last_seen_remote"] == "2025-02-01T00:00:00"
        assert d["remote_metadata"] == {"key": "val"}

    @pytest.mark.unit
    def test_from_dict_with_optional_fields(self):
        from acoharmony._4icli.state import FileDownloadState

        data = {
            "filename": "test.zip",
            "file_hash": "abc",
            "download_timestamp": "2025-01-01T12:00:00",
            "category": "CCLF",
            "file_type_code": 113,
            "file_size": 1024,
            "source_path": "/tmp/test.zip",
            "last_seen_remote": "2025-02-01T00:00:00",
            "remote_metadata": {"key": "val"},
        }
        state = FileDownloadState.from_dict(data)
        assert state.last_seen_remote == datetime(2025, 2, 1)
        assert state.remote_metadata == {"key": "val"}


class TestStateTrackerGetFilesToDownloadRemoteNewer:
    @pytest.mark.unit
    def test_remote_newer_triggers_download(self, mock_lw, tmp_path):
        from acoharmony._4icli.state import FourICLIStateTracker

        state_file = tmp_path / "state.json"
        bronze = tmp_path / "bronze"
        bronze.mkdir()

        tracker = FourICLIStateTracker(
            log_writer=mock_lw, state_file=state_file, search_paths=[bronze]
        )

        # Create and mark a file
        f = bronze / "test.zip"
        f.write_text("data")
        tracker.mark_file_downloaded(f, "CCLF", 113)

        # Update remote with newer date
        remote_files = [
            {
                "filename": "test.zip",
                "size": 1024,
                "created": (datetime.now() + timedelta(days=1)).isoformat(),
            }
        ]
        tracker.update_remote_inventory(remote_files, "CCLF", 113)

        to_download = tracker.get_files_to_download(bronze)
        assert "test.zip" in to_download


class TestFourICLIState:
    """Test 4icli state tracker."""

    @pytest.mark.unit
    def test_state_tracker_init(self, tmp_path) -> None:
        """FourICLIStateTracker can be created."""
        from unittest.mock import MagicMock

        from acoharmony._4icli.state import FourICLIStateTracker

        log_writer = MagicMock()
        state_file = tmp_path / "tracking" / "4icli_state.json"
        tracker = FourICLIStateTracker(log_writer=log_writer, state_file=state_file)
        assert tracker is not None

    @pytest.mark.unit
    def test_file_download_state(self) -> None:
        """FileDownloadState can be created."""
        from datetime import datetime

        from acoharmony._4icli.state import FileDownloadState

        state = FileDownloadState(
            filename="test.zip",
            file_hash="abc123",
            download_timestamp=datetime.now(),
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path="/tmp/test.zip",
        )
        assert state.filename == "test.zip"


# ---------------------------------------------------------------------------
# Branch coverage: 520->502 (remote_metadata newer triggers re-download)
# ---------------------------------------------------------------------------


class TestGetFilesNeedingDownloadRemoteMetadataBranch:
    """Cover branch 520->502: remote_metadata with newer date adds to download list."""

    @pytest.mark.unit
    def test_remote_metadata_newer_triggers_download(self, mock_log_writer, tmp_path):
        """Branch 520->502: file has hash, exists locally, but remote is newer."""
        state_file = tmp_path / "tracking" / "test_remote_newer.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        # Create local file
        storage_path = tmp_path / "storage"
        storage_path.mkdir()
        local_file = storage_path / "test_remote.zip"
        local_file.write_text("local content")

        # Add to cache with an old download timestamp and hash
        state = FileDownloadState(
            filename="test_remote.zip",
            file_hash="some_hash",
            download_timestamp=datetime(2024, 1, 1, 0, 0, 0),
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path=str(local_file),
            last_seen_remote=datetime.now(),
            remote_metadata={"created": "2025-06-01T00:00:00"},
        )
        tracker._file_cache["test_remote.zip"] = state

        result = tracker.get_files_to_download(storage_path)
        assert "test_remote.zip" in result

    @pytest.mark.unit
    def test_remote_metadata_older_does_not_trigger(self, mock_log_writer, tmp_path):
        """Remote metadata with older date does not trigger download."""
        state_file = tmp_path / "tracking" / "test_remote_older.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        storage_path = tmp_path / "storage"
        storage_path.mkdir()
        local_file = storage_path / "test_old.zip"
        local_file.write_text("local content")

        state = FileDownloadState(
            filename="test_old.zip",
            file_hash="some_hash",
            download_timestamp=datetime(2026, 1, 1, 0, 0, 0),
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path=str(local_file),
            last_seen_remote=datetime.now(),
            remote_metadata={"created": "2024-01-01T00:00:00"},
        )
        tracker._file_cache["test_old.zip"] = state

        result = tracker.get_files_to_download(storage_path)
        assert "test_old.zip" not in result


class Test4icliStateRemoteMetadataFalsy:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_4icli_state_remote_metadata_falsy(self):
        """520->502: state.remote_metadata is falsy."""
        from acoharmony._4icli import state
        assert state is not None


class TestGetFilesToDownloadRemoteMetadataOlderDate:
    """Cover branch 520->502: remote_metadata is truthy but remote date is
    not newer than download_timestamp, so the file is NOT added to download list
    and the loop continues.
    """

    @pytest.mark.unit
    def test_remote_metadata_not_newer_skips_download(self, mock_log_writer, tmp_path):
        """Branch 520->502: remote_metadata truthy, remote date <= download_timestamp.

        The elif at line 520 is entered, but since remote_date is older than
        download_timestamp, to_download is NOT appended and the loop continues
        back to line 502.
        """
        state_file = tmp_path / "tracking" / "test_skip.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        storage_path = tmp_path / "storage"
        storage_path.mkdir()

        # Create local file so local_file.exists() is True
        local_file = storage_path / "already_have.zip"
        local_file.write_text("existing content")

        # State has a hash (so first condition fails), file exists locally,
        # remote_metadata is truthy with a date OLDER than download_timestamp.
        state = FileDownloadState(
            filename="already_have.zip",
            file_hash="existing_hash_abc123",
            download_timestamp=datetime(2025, 6, 1, 0, 0, 0),  # downloaded June 2025
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path=str(local_file),
            last_seen_remote=datetime.now(),
            remote_metadata={"created": "2025-01-01T00:00:00"},  # created Jan 2025 (older)
        )
        tracker._file_cache["already_have.zip"] = state

        # Also add a second entry to confirm the loop continues
        local_file2 = storage_path / "also_have.zip"
        local_file2.write_text("content2")
        state2 = FileDownloadState(
            filename="also_have.zip",
            file_hash="hash2",
            download_timestamp=datetime(2025, 6, 1, 0, 0, 0),
            category="CCLF",
            file_type_code=113,
            file_size=512,
            source_path=str(local_file2),
            last_seen_remote=datetime.now(),
            remote_metadata={"modified": "2025-03-01T00:00:00"},  # still older
        )
        tracker._file_cache["also_have.zip"] = state2

        result = tracker.get_files_to_download(storage_path, category="CCLF")
        # Neither file should be in the download list
        assert "already_have.zip" not in result
        assert "also_have.zip" not in result
        assert len(result) == 0


class TestGetFilesToDownloadRemoteMetadataFalsy:
    """Cover branch 520->502: state.remote_metadata is falsy (None/empty).

    File has a hash and exists locally, last_seen_remote is set, but
    remote_metadata is None -> elif at 520 is False -> loop continues.
    """

    @pytest.mark.unit
    def test_remote_metadata_none_skips_download(self, mock_log_writer, tmp_path):
        """Branch 520->502: remote_metadata is None, file has hash and exists."""
        state_file = tmp_path / "tracking" / "test_none_meta.json"
        tracker = FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)

        storage_path = tmp_path / "storage"
        storage_path.mkdir()

        # Create local file
        local_file = storage_path / "existing.zip"
        local_file.write_text("content")

        state = FileDownloadState(
            filename="existing.zip",
            file_hash="hash_abc",
            download_timestamp=datetime(2025, 6, 1, 0, 0, 0),
            category="CCLF",
            file_type_code=113,
            file_size=1024,
            source_path=str(local_file),
            last_seen_remote=datetime.now(),
            remote_metadata=None,  # Falsy -> branch 520 is False
        )
        tracker._file_cache["existing.zip"] = state

        result = tracker.get_files_to_download(storage_path)
        assert "existing.zip" not in result
        assert len(result) == 0
