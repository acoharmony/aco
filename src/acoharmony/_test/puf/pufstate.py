"""Tests for acoharmony._puf.puf_state module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path
from datetime import datetime

import acoharmony

from acoharmony._puf.puf_state import (
    PUFFileEntry,
    PUFInventoryState,
    PUFStateTracker,
    scan_directory,
)
from acoharmony._puf.models import (
    DownloadTask,
    FileMetadata,
    FileCategory,
    RuleType,
)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._puf.puf_state is not None


def _make_file_entry(
    year="2024",
    rule_type="Final",
    file_key="test_file",
    url="https://example.com/test.zip",
    category="addenda",
    downloaded=False,
    zip_filename="test.zip",
    extracted=False,
    error_message=None,
    schema_mapping=None,
    dataset_key="pfs",
):
    """Helper to create a PUFFileEntry for tests."""
    return PUFFileEntry(
        year=year,
        rule_type=rule_type,
        file_key=file_key,
        url=url,
        category=category,
        downloaded=downloaded,
        zip_filename=zip_filename,
        extracted=extracted,
        error_message=error_message,
        schema_mapping=schema_mapping,
        dataset_key=dataset_key,
    )


def _make_download_task(
    key="test_file",
    url="https://example.com/test.zip",
    category=FileCategory.ADDENDA,
    year="2024",
    rule_type=RuleType.FINAL,
):
    """Helper to create a DownloadTask for tests."""
    file_meta = FileMetadata(key=key, url=url, category=category)
    return DownloadTask(
        file_metadata=file_meta,
        year=year,
        rule_type=rule_type,
    )


class TestPUFInventoryStatePostInit:
    """Tests for PUFInventoryState.__post_init__ branch 158→161."""

    @pytest.mark.unit
    def test_post_init_with_puffileentry_objects(self):
        """Branch 158→161: file_entry is already PUFFileEntry, not dict."""
        entry = _make_file_entry(year="2024", file_key="already_obj")
        state = PUFInventoryState(
            files={"2024:Final:already_obj": entry}
        )
        # The entry should remain a PUFFileEntry, not be re-converted
        assert isinstance(state.files["2024:Final:already_obj"], PUFFileEntry)
        assert state.files["2024:Final:already_obj"].file_key == "already_obj"


class TestPUFFileEntryFromDownloadTaskEmptyUrl:
    """Tests for PUFFileEntry.from_download_task branch 115→124 (empty url)."""

    @pytest.mark.unit
    def test_from_download_task_empty_url_string(self):
        """Branch 115→124: url_str is empty, zip_filename should be None."""
        file_meta = MagicMock()
        file_meta.url = ""
        file_meta.key = "test_key"
        file_meta.category = FileCategory.ADDENDA
        file_meta.metadata = {}
        file_meta.schema_mapping = None

        task = MagicMock()
        task.year = "2024"
        task.rule_type = RuleType.FINAL
        task.file_metadata = file_meta

        entry = PUFFileEntry.from_download_task(task)
        assert entry.zip_filename is None


class TestGetNeededDownloadsBranches:
    """Tests for get_needed_downloads branches 422→427, 424→427, 427→418."""

    @pytest.mark.unit
    def test_year_inv_is_none(self):
        """Branch 422→427: year not found in inventory, file_meta stays None."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        # Add a pending entry for a year that doesn't exist in the PFS inventory
        entry = _make_file_entry(
            year="1900",
            rule_type="Final",
            file_key="nonexistent_file",
        )
        tracker.state.add_file(entry)

        with patch("acoharmony._puf.puf_state.pfs_inventory") as mock_inv:
            mock_inv.get_year.return_value = None
            tasks = tracker.get_needed_downloads()

        # No tasks because year_inv is None -> file_meta is None
        assert tasks == []

    @pytest.mark.unit
    def test_rule_is_none(self):
        """Branch 424→427: year found but rule not found, file_meta stays None."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(
            year="2024",
            rule_type="Final",
            file_key="some_file",
        )
        tracker.state.add_file(entry)

        mock_year_inv = MagicMock()
        mock_year_inv.get_rule.return_value = None

        with patch("acoharmony._puf.puf_state.pfs_inventory") as mock_inv:
            mock_inv.get_year.return_value = mock_year_inv
            tasks = tracker.get_needed_downloads()

        # No tasks because rule is None -> file_meta is None
        assert tasks == []

    @pytest.mark.unit
    def test_file_meta_not_in_rule(self):
        """Branch 427→418: year & rule found but file_key not in rule.files."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(
            year="2024",
            rule_type="Final",
            file_key="missing_file",
        )
        tracker.state.add_file(entry)

        mock_rule = MagicMock()
        mock_rule.files = {}  # No files in rule

        mock_year_inv = MagicMock()
        mock_year_inv.get_rule.return_value = mock_rule

        with patch("acoharmony._puf.puf_state.pfs_inventory") as mock_inv:
            mock_inv.get_year.return_value = mock_year_inv
            tasks = tracker.get_needed_downloads()

        # file_meta is None because file_key not in rule.files
        assert tasks == []


class TestScanFilesystemBranches:
    """Tests for scan_filesystem branches 530→544, 537→544, 549→553."""

    @pytest.mark.unit
    def test_no_zip_filename_skips_cite_check(self):
        """Branch 530→544: entry.zip_filename is None, skip cite corpus check."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(zip_filename=None)
        tracker.state.add_file(entry)

        with patch("acoharmony._puf.puf_state.get_workspace_path") as mock_ws:
            mock_ws.return_value = Path("/tmp/fake_workspace_pufstate_test")
            with patch("acoharmony._puf.puf_state.scan_directory", return_value=set()):
                stats = tracker.scan_filesystem()

        assert stats["found_in_cite"] == 0

    @pytest.mark.unit
    def test_cite_corpus_no_matching_files(self):
        """Branch 537→544: zip_filename present but no matching cite files."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(zip_filename="myfile.zip", file_key="myfile")
        tracker.state.add_file(entry)

        def mock_scan(directory, label="directory", recursive=False):
            # Return empty set for cite corpus
            return set()

        with patch("acoharmony._puf.puf_state.get_workspace_path") as mock_ws:
            mock_ws.return_value = Path("/tmp/fake_workspace_pufstate_test2")
            with patch("acoharmony._puf.puf_state.scan_directory", side_effect=mock_scan):
                stats = tracker.scan_filesystem()

        assert stats["found_in_cite"] == 0
        assert entry.found_in_cite_corpus is False

    @pytest.mark.unit
    def test_extracted_files_found_marks_extracted(self):
        """Branch 549→553: matching extracted files found, entry not yet extracted."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(
            zip_filename="myfile.zip",
            file_key="myfile",
            extracted=False,
        )
        tracker.state.add_file(entry)

        call_count = [0]

        def mock_scan(directory, label="directory", recursive=False):
            call_count[0] += 1
            dir_str = str(directory)
            if "archive" in dir_str:
                return set()
            if "pufs" in dir_str:
                # Return a file that matches the base_name pattern
                return {"myfile_data.csv"}
            if "cites" in dir_str:
                return set()
            return set()

        with patch("acoharmony._puf.puf_state.get_workspace_path") as mock_ws:
            ws = Path("/tmp/fake_workspace_pufstate_test3")
            mock_ws.return_value = ws
            # Make pufs dir "exist"
            with patch.object(Path, "exists", return_value=True):
                with patch("acoharmony._puf.puf_state.scan_directory", side_effect=mock_scan):
                    stats = tracker.scan_filesystem()

        assert entry.extracted is True
        assert entry.extraction_timestamp is not None
        assert stats["marked_extracted"] == 1
        assert "myfile_data.csv" in entry.extracted_files


# ---------------------------------------------------------------------------
# Branch coverage: 537->544 (cite corpus found, not downloaded -> marks downloaded)
# ---------------------------------------------------------------------------


class TestScanFilesystemCiteCorpusDownloadBranch:
    """Cover branch 537->544: matching cite files found, entry not downloaded."""

    @pytest.mark.unit
    def test_cite_match_marks_downloaded(self):
        """Branch 537->544: cite match found and entry.downloaded=False -> marks downloaded."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(
            zip_filename="myfile.zip",
            file_key="myfile",
            downloaded=False,
        )
        tracker.state.add_file(entry)

        def mock_scan(directory, label="directory", recursive=False):
            dir_str = str(directory)
            if "cites" in dir_str:
                # Return a file matching base_name "myfile"
                return {"myfile_corpus_data.txt"}
            return set()

        with patch("acoharmony._puf.puf_state.get_workspace_path") as mock_ws:
            ws = Path("/tmp/fake_workspace_cite_dl_test")
            mock_ws.return_value = ws
            with patch.object(Path, "exists", return_value=True):
                with patch("acoharmony._puf.puf_state.scan_directory", side_effect=mock_scan):
                    stats = tracker.scan_filesystem()

        assert entry.found_in_cite_corpus is True
        assert entry.downloaded is True
        assert entry.download_timestamp is not None
        assert stats["found_in_cite"] == 1
        assert stats["marked_downloaded"] >= 1


# ---------------------------------------------------------------------------
# Branch coverage: 549->553 (extracted files found, entry not extracted)
# Already covered by test_extracted_files_found_marks_extracted above
# ---------------------------------------------------------------------------


class TestScanFilesystemExtractedAlreadyTrue:
    """Cover branch 549->553: matching extracted files found but entry already extracted."""

    @pytest.mark.unit
    def test_extracted_files_found_but_already_marked(self):
        """Branch 549->553: entry.extracted is already True, no re-marking."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(
            zip_filename="myfile.zip",
            file_key="myfile",
            extracted=True,  # already extracted
        )
        tracker.state.add_file(entry)

        def mock_scan(directory, label="directory", recursive=False):
            dir_str = str(directory)
            if "pufs" in dir_str:
                return {"myfile_data.csv"}
            return set()

        with patch("acoharmony._puf.puf_state.get_workspace_path") as mock_ws:
            ws = Path("/tmp/fake_workspace_already_extracted_test")
            mock_ws.return_value = ws
            with patch.object(Path, "exists", return_value=True):
                with patch("acoharmony._puf.puf_state.scan_directory", side_effect=mock_scan):
                    stats = tracker.scan_filesystem()

        # Already extracted, so marked_extracted should NOT increment
        assert stats["marked_extracted"] == 0
        # But extracted_files should still be set
        assert "myfile_data.csv" in entry.extracted_files


class TestScanFilesystemCiteCorpusAlreadyDownloaded:
    """Cover branch 537->544: cite match found but entry already downloaded."""

    @pytest.mark.unit
    def test_cite_match_already_downloaded_no_remark(self):
        """Branch 537->544: cite match found but entry.downloaded already True."""
        tracker = PUFStateTracker(state=PUFInventoryState())
        entry = _make_file_entry(
            zip_filename="myfile.zip",
            file_key="myfile",
            downloaded=True,  # already downloaded
        )
        tracker.state.add_file(entry)

        def mock_scan(directory, label="directory", recursive=False):
            dir_str = str(directory)
            if "cites" in dir_str:
                return {"myfile_corpus_data.txt"}
            return set()

        with patch("acoharmony._puf.puf_state.get_workspace_path") as mock_ws:
            ws = Path("/tmp/fake_workspace_cite_already_dl_test")
            mock_ws.return_value = ws
            with patch.object(Path, "exists", return_value=True):
                with patch("acoharmony._puf.puf_state.scan_directory", side_effect=mock_scan):
                    stats = tracker.scan_filesystem()

        assert entry.found_in_cite_corpus is True
        assert stats["found_in_cite"] == 1
        # marked_downloaded should NOT increment since already downloaded
        assert stats["marked_downloaded"] == 0
