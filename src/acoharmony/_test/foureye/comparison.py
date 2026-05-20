# © 2025 HarmonyCares
# All rights reserved.

"""Tests for comparison module - comparing inventory to local files."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import json
from collections import Counter
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from acoharmony._4icli.comparison import (
    compare_inventory,
    export_to_csv,
    format_size,
    save_not_downloaded_state,
    scan_all_storage_locations,
    scan_directory,
)
from acoharmony._4icli.config import FourICLIConfig
from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult


def create_test_inventory(files: list[FileInventoryEntry]) -> InventoryResult:
    """Helper to create test inventory with proper aggregations."""
    files_by_year = Counter(f.year for f in files)
    files_by_category = Counter(f.category for f in files)
    categories = list(files_by_category.keys())
    years = sorted({f.year for f in files})

    return InventoryResult(
        apm_id="D0259",
        categories=categories,
        years=years,
        total_files=len(files),
        files_by_year=dict(files_by_year),
        files_by_category=dict(files_by_category),
        files=files,
        started_at=datetime.now(),
    )


class TestScanDirectory:
    """Tests for scan_directory function."""

    @pytest.mark.unit
    def test_scan_empty_directory(self, tmp_path: Path) -> None:
        """Scanning empty directory returns empty set."""
        empty_dir = tmp_path / "empty"
        empty_dir.mkdir()

        files = scan_directory(empty_dir, "empty")

        assert files == set()

    @pytest.mark.unit
    def test_scan_directory_with_files(self, tmp_path: Path) -> None:
        """Scanning directory returns all top-level files."""
        test_dir = tmp_path / "test"
        test_dir.mkdir()

        # Create test files
        (test_dir / "file1.zip").write_text("data1")
        (test_dir / "file2.zip").write_text("data2")
        (test_dir / "file3.txt").write_text("data3")

        files = scan_directory(test_dir, "test")

        assert files == {"file1.zip", "file2.zip", "file3.txt"}

    @pytest.mark.unit
    def test_scan_directory_non_recursive(self, tmp_path: Path) -> None:
        """Non-recursive scan only returns top-level files."""
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        subdir = test_dir / "subdir"
        subdir.mkdir()

        # Top-level file
        (test_dir / "top_level.zip").write_text("top")

        # Nested file
        (subdir / "nested.zip").write_text("nested")

        # Non-recursive scan (default)
        files = scan_directory(test_dir, "test", recursive=False)

        assert files == {"top_level.zip"}
        assert "nested.zip" not in files

    @pytest.mark.unit
    def test_scan_directory_recursive(self, tmp_path: Path) -> None:
        """Recursive scan returns files from subdirectories."""
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        subdir = test_dir / "subdir"
        subdir.mkdir()

        # Top-level file
        (test_dir / "top_level.zip").write_text("top")

        # Nested file
        (subdir / "nested.zip").write_text("nested")

        # Recursive scan
        files = scan_directory(test_dir, "test", recursive=True)

        assert files == {"top_level.zip", "nested.zip"}

    @pytest.mark.unit
    def test_scan_nonexistent_directory(self, tmp_path: Path, capsys) -> None:
        """Scanning nonexistent directory returns empty set with warning."""
        nonexistent = tmp_path / "nonexistent"

        files = scan_directory(nonexistent, "nonexistent")

        assert files == set()

        # Check warning was printed
        captured = capsys.readouterr()
        assert "Warning" in captured.out
        assert "does not exist" in captured.out

    @pytest.mark.unit
    def test_scan_directory_ignores_subdirs(self, tmp_path: Path) -> None:
        """Non-recursive scan ignores subdirectories."""
        test_dir = tmp_path / "test"
        test_dir.mkdir()
        subdir = test_dir / "subdir"
        subdir.mkdir()

        (test_dir / "file.zip").write_text("data")

        files = scan_directory(test_dir, "test", recursive=False)

        # Should only have the file, not the subdirectory name
        assert files == {"file.zip"}


class TestScanAllStorageLocations:
    """Tests for scan_all_storage_locations function."""

    @pytest.mark.unit
    def test_scan_all_locations_empty(self, mock_config: FourICLIConfig) -> None:
        """Scanning all locations with no files returns empty set."""
        # Ensure directories exist but are empty
        mock_config.bronze_dir.mkdir(parents=True, exist_ok=True)
        mock_config.archive_dir.mkdir(parents=True, exist_ok=True)

        files = scan_all_storage_locations(mock_config)

        assert files == set()

    @pytest.mark.unit
    def test_scan_all_locations_bronze_only(self, mock_config: FourICLIConfig) -> None:
        """Scanning finds files only in bronze."""
        mock_config.bronze_dir.mkdir(parents=True, exist_ok=True)
        mock_config.archive_dir.mkdir(parents=True, exist_ok=True)

        (mock_config.bronze_dir / "bronze_file.zip").write_text("data")

        files = scan_all_storage_locations(mock_config)

        assert files == {"bronze_file.zip"}

    @pytest.mark.unit
    def test_scan_all_locations_archive_only(self, mock_config: FourICLIConfig) -> None:
        """Scanning finds files only in archive."""
        mock_config.bronze_dir.mkdir(parents=True, exist_ok=True)
        mock_config.archive_dir.mkdir(parents=True, exist_ok=True)

        (mock_config.archive_dir / "archive_file.zip").write_text("data")

        files = scan_all_storage_locations(mock_config)

        assert files == {"archive_file.zip"}

    @pytest.mark.unit
    def test_scan_all_locations_both(self, mock_config: FourICLIConfig) -> None:
        """Scanning finds files in both bronze and archive."""
        mock_config.bronze_dir.mkdir(parents=True, exist_ok=True)
        mock_config.archive_dir.mkdir(parents=True, exist_ok=True)

        (mock_config.bronze_dir / "bronze_file.zip").write_text("data")
        (mock_config.archive_dir / "archive_file.zip").write_text("data")

        files = scan_all_storage_locations(mock_config)

        assert files == {"bronze_file.zip", "archive_file.zip"}

    @pytest.mark.unit
    def test_scan_all_locations_deduplicates(self, mock_config: FourICLIConfig) -> None:
        """Scanning deduplicates files with same name in different locations."""
        mock_config.bronze_dir.mkdir(parents=True, exist_ok=True)
        mock_config.archive_dir.mkdir(parents=True, exist_ok=True)

        # Same filename in both locations
        (mock_config.bronze_dir / "same_file.zip").write_text("data1")
        (mock_config.archive_dir / "same_file.zip").write_text("data2")

        files = scan_all_storage_locations(mock_config)

        # Should only appear once in the set
        assert files == {"same_file.zip"}


class TestCompareInventory:
    """Tests for compare_inventory function."""

    @pytest.mark.unit
    def test_compare_all_missing(self) -> None:
        """All files in inventory are missing locally."""
        # Create inventory with 3 files
        inventory = create_test_inventory([
            FileInventoryEntry(
                filename="file1.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=1000,
                last_updated="2024-01-01T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="file2.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=2000,
                last_updated="2024-01-02T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="file3.zip",
                category="REPORTS",
                file_type_code=11,
                year=2025,
                size_bytes=3000,
                last_updated="2024-01-03T00:00:00Z",
            ),
        ])

        # No local files
        local_files = set()

        result = compare_inventory(inventory, local_files)

        assert result["total_inventory"] == 3
        assert result["have_count"] == 0
        assert result["missing_count"] == 3
        assert result["total_size_bytes"] == 6000
        assert result["missing_by_year"] == {2024: 2, 2025: 1}
        assert result["missing_by_category"] == {"CCLF": 2, "REPORTS": 1}

    @pytest.mark.unit
    def test_compare_all_present(self) -> None:
        """All files in inventory are present locally."""
        inventory = create_test_inventory([
            FileInventoryEntry(
                filename="file1.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=1000,
                last_updated="2024-01-01T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="file2.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=2000,
                last_updated="2024-01-02T00:00:00Z",
            ),
        ])

        # All files present locally
        local_files = {"file1.zip", "file2.zip"}

        result = compare_inventory(inventory, local_files)

        assert result["total_inventory"] == 2
        assert result["have_count"] == 2
        assert result["missing_count"] == 0
        assert result["total_size_bytes"] == 0
        assert result["missing_by_year"] == {}
        assert result["missing_by_category"] == {}

    @pytest.mark.unit
    def test_compare_mixed(self) -> None:
        """Some files present, some missing."""
        inventory = create_test_inventory([
            FileInventoryEntry(
                filename="file1.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=1000,
                last_updated="2024-01-01T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="file2.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=2000,
                last_updated="2024-01-02T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="file3.zip",
                category="REPORTS",
                file_type_code=11,
                year=2025,
                size_bytes=3000,
                last_updated="2024-01-03T00:00:00Z",
            ),
        ])

        # Only file1.zip present locally
        local_files = {"file1.zip"}

        result = compare_inventory(inventory, local_files)

        assert result["total_inventory"] == 3
        assert result["have_count"] == 1
        assert result["missing_count"] == 2
        assert result["total_size_bytes"] == 5000  # file2 + file3
        assert result["missing_by_year"] == {2024: 1, 2025: 1}
        assert result["missing_by_category"] == {"CCLF": 1, "REPORTS": 1}

    @pytest.mark.unit
    def test_compare_with_year_filter(self) -> None:
        """Comparison respects year filter."""
        inventory = create_test_inventory([
            FileInventoryEntry(
                filename="file2024.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=1000,
                last_updated="2024-01-01T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="file2025.zip",
                category="CCLF",
                file_type_code=8,
                year=2025,
                size_bytes=2000,
                last_updated="2025-01-01T00:00:00Z",
            ),
        ])

        local_files = set()

        # Filter to only 2024
        result = compare_inventory(inventory, local_files, year_filter=2024)

        assert result["total_inventory"] == 1
        assert result["missing_count"] == 1
        assert result["missing"][0].filename == "file2024.zip"

    @pytest.mark.unit
    def test_compare_with_category_filter(self) -> None:
        """Comparison respects category filter."""
        inventory = create_test_inventory([
            FileInventoryEntry(
                filename="cclf_file.zip",
                category="CCLF",
                file_type_code=8,
                year=2024,
                size_bytes=1000,
                last_updated="2024-01-01T00:00:00Z",
            ),
            FileInventoryEntry(
                filename="report_file.zip",
                category="REPORTS",
                file_type_code=11,
                year=2024,
                size_bytes=2000,
                last_updated="2024-01-01T00:00:00Z",
            ),
        ])

        local_files = set()

        # Filter to only CCLF
        result = compare_inventory(inventory, local_files, category_filter="CCLF")

        assert result["total_inventory"] == 1
        assert result["missing_count"] == 1
        assert result["missing"][0].filename == "cclf_file.zip"


class TestFormatSize:
    """Tests for format_size function."""

    @pytest.mark.unit
    def test_format_bytes(self) -> None:
        """Format bytes."""
        assert format_size(500) == "500.00 B"

    @pytest.mark.unit
    def test_format_kilobytes(self) -> None:
        """Format kilobytes."""
        assert format_size(1024) == "1.00 KB"
        assert format_size(1536) == "1.50 KB"

    @pytest.mark.unit
    def test_format_megabytes(self) -> None:
        """Format megabytes."""
        assert format_size(1024 * 1024) == "1.00 MB"
        assert format_size(1024 * 1024 * 2.5) == "2.50 MB"

    @pytest.mark.unit
    def test_format_gigabytes(self) -> None:
        """Format gigabytes."""
        assert format_size(1024 * 1024 * 1024) == "1.00 GB"

    @pytest.mark.unit
    def test_format_terabytes(self) -> None:
        """Format terabytes."""
        assert format_size(1024 * 1024 * 1024 * 1024) == "1.00 TB"

    @pytest.mark.unit
    def test_format_petabytes(self) -> None:
        """Format petabytes."""
        assert format_size(1024 * 1024 * 1024 * 1024 * 1024) == "1.00 PB"

    @pytest.mark.unit
    def test_format_none(self) -> None:
        """Format None returns N/A."""
        assert format_size(None) == "N/A"

    @pytest.mark.unit
    def test_format_zero(self) -> None:
        """Format zero bytes."""
        assert format_size(0) == "0.00 B"


class TestSaveNotDownloadedState:
    """Tests for save_not_downloaded_state function."""

    @pytest.mark.unit
    def test_save_empty_state(self, tmp_path: Path) -> None:
        """Saving empty state creates valid JSON."""
        import json

        output_path = tmp_path / "state.json"

        save_not_downloaded_state([], output_path)

        assert output_path.exists()

        with open(output_path) as f:
            data = json.load(f)

        assert data["total_missing"] == 0
        assert data["total_size_bytes"] == 0
        assert data["files"] == []

    @pytest.mark.unit
    def test_save_with_files(self, tmp_path: Path) -> None:
        """Saving state with files creates complete JSON."""
        import json

        missing_files = [
            FileInventoryEntry(
                filename="file1.zip",
                category="CCLF",
                year=2024,
                size_bytes=1000,
                last_updated="2024-01-01T00:00:00Z",
                file_type_code=8,
            ),
            FileInventoryEntry(
                filename="file2.zip",
                category="REPORTS",
                year=2025,
                size_bytes=2000,
                last_updated="2025-01-01T00:00:00Z",
                file_type_code=11,
            ),
        ]

        output_path = tmp_path / "state.json"

        save_not_downloaded_state(missing_files, output_path)

        assert output_path.exists()

        with open(output_path) as f:
            data = json.load(f)

        assert data["total_missing"] == 2
        assert data["total_size_bytes"] == 3000
        assert len(data["files"]) == 2
        assert data["missing_by_year"] == {"2024": 1, "2025": 1}
        assert data["missing_by_category"] == {"CCLF": 1, "REPORTS": 1}
        assert data["missing_by_type_code"] == {"8": 1, "11": 1}

    @pytest.mark.unit
    def test_save_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Saving state creates parent directories if needed."""
        output_path = tmp_path / "nested" / "dirs" / "state.json"

        save_not_downloaded_state([], output_path)

        assert output_path.exists()
        assert output_path.parent.exists()


class TestExportToCsv:
    """Tests for export_to_csv function."""

    @pytest.mark.unit
    def test_export_empty_csv(self, tmp_path: Path) -> None:
        """Exporting empty list creates CSV with headers."""
        import csv

        output_path = tmp_path / "export.csv"

        export_to_csv([], output_path)

        assert output_path.exists()

        with open(output_path, newline="") as f:
            reader = csv.reader(f)
            rows = list(reader)

        # Should have header row
        assert len(rows) == 1
        assert rows[0][0] == "filename"

    @pytest.mark.unit
    def test_export_with_files(self, tmp_path: Path) -> None:
        """Exporting files creates CSV with data."""
        import csv

        missing_files = [
            FileInventoryEntry(
                filename="file1.zip",
                category="CCLF",
                year=2024,
                size_bytes=1024 * 1024,  # 1 MB
                last_updated="2024-01-01T00:00:00Z",
                file_type_code=8,
            ),
            FileInventoryEntry(
                filename="file2.zip",
                category="REPORTS",
                year=2025,
                size_bytes=2048 * 1024,  # 2 MB
                last_updated="2025-01-01T00:00:00Z",
                file_type_code=11,
            ),
        ]

        output_path = tmp_path / "export.csv"

        export_to_csv(missing_files, output_path)

        assert output_path.exists()

        with open(output_path, newline="") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        assert len(rows) == 2
        assert rows[0]["filename"] == "file1.zip"
        assert rows[0]["category"] == "CCLF"
        assert rows[0]["year"] == "2024"
        assert rows[0]["file_type_code"] == "8"
        assert "MB" in rows[0]["size_formatted"]

    @pytest.mark.unit
    def test_export_creates_parent_dirs(self, tmp_path: Path) -> None:
        """Exporting CSV creates parent directories if needed."""
        output_path = tmp_path / "nested" / "export.csv"

        export_to_csv([], output_path)

        assert output_path.exists()
        assert output_path.parent.exists()


class TestComparison:
    @pytest.mark.unit
    def test_scan_directory(self, tmp_path):
        from acoharmony._4icli.comparison import scan_directory
        (tmp_path / "file1.csv").write_text("data")
        (tmp_path / "file2.txt").write_text("data")
        result = scan_directory(tmp_path)
        assert isinstance(result, set)
        assert "file1.csv" in result

    @pytest.mark.unit
    def test_scan_directory_empty(self, tmp_path):
        from acoharmony._4icli.comparison import scan_directory
        result = scan_directory(tmp_path)
        assert len(result) == 0

    @pytest.mark.unit
    def test_scan_directory_recursive(self, tmp_path):
        from acoharmony._4icli.comparison import scan_directory
        sub = tmp_path / "sub"
        sub.mkdir()
        (sub / "nested.csv").write_text("data")
        result = scan_directory(tmp_path, recursive=True)
        assert "nested.csv" in result

    @pytest.mark.unit
    def test_format_size(self):
        from acoharmony._4icli.comparison import format_size
        assert "B" in format_size(100)
        assert "KB" in format_size(2048)
        assert "MB" in format_size(2_000_000)

    @pytest.mark.unit
    def test_format_size_none(self):
        from acoharmony._4icli.comparison import format_size
        result = format_size(None)
        assert result is not None

    def _make_file_entry(self, filename="a.zip", year=2024, category="CCLF",
                         file_type_code=113, size_bytes=100):
        entry = MagicMock()
        entry.filename = filename
        entry.year = year
        entry.category = category
        entry.file_type_code = file_type_code
        entry.size_bytes = size_bytes
        entry.to_dict.return_value = {
            "filename": filename, "year": year, "category": category,
            "file_type_code": file_type_code, "size_bytes": size_bytes,
        }
        return entry

    @pytest.mark.unit
    def test_save_not_downloaded_state(self, tmp_path):
        from acoharmony._4icli.comparison import save_not_downloaded_state
        files = [self._make_file_entry()]
        out = tmp_path / "state.json"
        save_not_downloaded_state(files, out)
        assert out.exists()
        data = json.loads(out.read_text())
        assert "files" in data
        assert data["total_missing"] == 1

    @pytest.mark.unit
    def test_export_to_csv(self, tmp_path):
        from acoharmony._4icli.comparison import export_to_csv
        files = [self._make_file_entry()]
        out = tmp_path / "missing.csv"
        export_to_csv(files, out)
        assert out.exists()

    @pytest.mark.unit
    def test_scan_directory_nonexistent(self):
        from acoharmony._4icli.comparison import scan_directory
        result = scan_directory(Path("/nonexistent_dir_xyz"))
        assert len(result) == 0


class TestComparison2:
    @pytest.mark.unit
    def test_compare_inventory_with_state_tracker(self):
        from acoharmony._4icli.comparison import compare_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        inv = InventoryResult(
            apm_id="D0259",
            categories=["CCLF"],
            years=[2025],
            total_files=2,
            files_by_year={2025: 2},
            files_by_category={"CCLF": 2},
            files=[
                FileInventoryEntry(filename="have.zip", category="CCLF", file_type_code=113, year=2025, size_bytes=100),
                FileInventoryEntry(filename="missing.zip", category="CCLF", file_type_code=113, year=2025, size_bytes=200),
            ],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )

        # State tracker has "tracked.zip" in its cache
        mock_tracker = MagicMock()
        mock_state = MagicMock()
        mock_state.filename = "have.zip"
        mock_tracker._file_cache = {"have.zip": mock_state}

        results = compare_inventory(inv, set(), state_tracker=mock_tracker)
        assert results["have_count"] == 1
        assert results["missing_count"] == 1
        assert results["total_size_bytes"] == 200

    @pytest.mark.unit
    def test_compare_with_filters(self):
        from acoharmony._4icli.comparison import compare_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        inv = InventoryResult(
            apm_id="D0259",
            categories=["CCLF", "Reports"],
            years=[2024, 2025],
            total_files=2,
            files_by_year={2024: 1, 2025: 1},
            files_by_category={"CCLF": 1, "Reports": 1},
            files=[
                FileInventoryEntry(filename="f1.zip", category="CCLF", file_type_code=113, year=2024),
                FileInventoryEntry(filename="f2.zip", category="Reports", file_type_code=112, year=2025),
            ],
            started_at=datetime.now(),
        )

        results = compare_inventory(inv, set(), year_filter=2025, category_filter="Reports")
        assert results["total_inventory"] == 1
        assert results["missing_count"] == 1


class TestCompareInventoryWithStateTracker:
    @pytest.mark.unit
    def test_compare_with_state_tracker(self, mock_lw, tmp_path):
        from acoharmony._4icli.comparison import compare_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult
        from acoharmony._4icli.state import FileDownloadState

        files = [
            FileInventoryEntry(filename="file1.zip", category="CCLF", file_type_code=113,
                               year=2025, size_bytes=1000),
            FileInventoryEntry(filename="file2.zip", category="CCLF", file_type_code=113,
                               year=2025, size_bytes=2000),
        ]
        inventory = InventoryResult(
            apm_id="D0259", categories=["CCLF"], years=[2025], total_files=2,
            files_by_year={2025: 2}, files_by_category={"CCLF": 2},
            files=files, started_at=datetime.now(),
        )

        # Mock state tracker with file2.zip tracked
        state_tracker = MagicMock()
        state_tracker._file_cache = {
            "file2.zip": FileDownloadState(
                filename="file2.zip", file_hash="abc", download_timestamp=datetime.now(),
                category="CCLF", file_type_code=113, file_size=2000, source_path="/tmp/file2.zip"
            )
        }

        result = compare_inventory(inventory, set(), state_tracker=state_tracker)
        assert result["have_count"] == 1  # file2.zip from state tracker
        assert result["missing_count"] == 1

    @pytest.mark.unit
    def test_compare_missing_by_type_code(self):
        from acoharmony._4icli.comparison import compare_inventory
        from acoharmony._4icli.inventory import FileInventoryEntry, InventoryResult

        files = [
            FileInventoryEntry(filename="f1.zip", category="CCLF", file_type_code=113,
                               year=2025, size_bytes=None),
            FileInventoryEntry(filename="f2.zip", category="CCLF", file_type_code=None,
                               year=2025, size_bytes=500),
        ]
        inventory = InventoryResult(
            apm_id="D0259", categories=["CCLF"], years=[2025], total_files=2,
            files_by_year={2025: 2}, files_by_category={"CCLF": 2},
            files=files, started_at=datetime.now(),
        )

        result = compare_inventory(inventory, set())
        # file_type_code=None should not be in missing_by_type_code
        assert result["missing_by_type_code"] == {113: 1}
        # size_bytes=None should be excluded from total_size
        assert result["total_size_bytes"] == 500

