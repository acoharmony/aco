"""Tests for acoharmony._cite.state module."""


# Magic auto-import: brings in ALL exports from module under test
from dataclasses import dataclass
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import hashlib
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._cite.state is not None


# ===========================================================================
# 3. CitationFileState tests
# ===========================================================================


class TestCitationFileState:
    """Tests for CitationFileState dataclass serialization."""

    def _make_state(self, **overrides):
        defaults = {
            "filename": "test.xml",
            "file_hash": "abc123",
            "process_timestamp": datetime(2025, 1, 15, 10, 30, 0),
            "source_type": "pubmed",
            "file_size": 1024,
            "source_path": "/data/raw/test.xml",
            "corpus_path": "/data/corpus/test.xml",
            "record_count": 42,
            "last_modified": datetime(2025, 1, 14, 8, 0, 0),
            "metadata": {"extra": "info"},
        }
        defaults.update(overrides)
        from acoharmony._cite.state import CitationFileState

        return CitationFileState(**defaults)

    @pytest.mark.unit
    def test_to_dict(self):
        state = self._make_state()
        d = state.to_dict()
        assert d["filename"] == "test.xml"
        assert d["file_hash"] == "abc123"
        assert d["process_timestamp"] == "2025-01-15T10:30:00"
        assert d["source_type"] == "pubmed"
        assert d["file_size"] == 1024
        assert d["source_path"] == "/data/raw/test.xml"
        assert d["corpus_path"] == "/data/corpus/test.xml"
        assert d["record_count"] == 42
        assert d["last_modified"] == "2025-01-14T08:00:00"
        assert d["metadata"] == {"extra": "info"}

    @pytest.mark.unit
    def test_to_dict_none_optionals(self):
        state = self._make_state(
            corpus_path=None, record_count=None, last_modified=None, metadata=None
        )
        d = state.to_dict()
        assert d["corpus_path"] is None
        assert d["record_count"] is None
        assert d["last_modified"] is None
        assert d["metadata"] is None

    @pytest.mark.unit
    def test_roundtrip(self):
        from acoharmony._cite.state import CitationFileState

        state = self._make_state()
        d = state.to_dict()
        restored = CitationFileState.from_dict(d)
        assert restored.filename == state.filename
        assert restored.file_hash == state.file_hash
        assert restored.process_timestamp == state.process_timestamp
        assert restored.source_type == state.source_type
        assert restored.file_size == state.file_size
        assert restored.source_path == state.source_path
        assert restored.corpus_path == state.corpus_path
        assert restored.record_count == state.record_count
        assert restored.last_modified == state.last_modified
        assert restored.metadata == state.metadata

    @pytest.mark.unit
    def test_from_dict_minimal(self):
        from acoharmony._cite.state import CitationFileState

        data = {
            "filename": "a.json",
            "file_hash": "h",
            "process_timestamp": "2025-06-01T12:00:00",
            "source_type": "crossref",
            "file_size": 500,
            "source_path": "/tmp/a.json",
        }
        s = CitationFileState.from_dict(data)
        assert s.filename == "a.json"
        assert s.corpus_path is None
        assert s.record_count is None
        assert s.last_modified is None
        assert s.metadata is None


# ===========================================================================
# 4. CiteStateTracker tests
# ===========================================================================


class TestCiteStateTracker:
    """Tests for CiteStateTracker with real temp filesystem."""

    def _make_tracker(self, tmp_path):
        from acoharmony._cite.state import CiteStateTracker

        state_file = tmp_path / "tracking" / "cite_state.json"
        mock_lw = MagicMock()
        return CiteStateTracker(log_writer=mock_lw, state_file=state_file, search_paths=[tmp_path])

    def _create_file(self, tmp_path, name="data.xml", content="hello world"):
        p = tmp_path / name
        p.write_text(content, encoding="utf-8")
        return p

    @pytest.mark.unit
    def test_create_fresh(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        assert tracker._file_cache == {}

    @pytest.mark.unit
    def test_mark_file_processed(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        state = tracker.mark_file_processed(fp, "pubmed", record_count=10, metadata={"k": "v"})
        assert state.filename == "data.xml"
        assert state.source_type == "pubmed"
        assert state.record_count == 10
        assert state.file_size > 0
        assert state.file_hash != ""
        assert state.metadata == {"k": "v"}

    @pytest.mark.unit
    def test_is_file_processed_true(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        tracker.mark_file_processed(fp, "pubmed")
        assert tracker.is_file_processed("data.xml", fp) is True

    @pytest.mark.unit
    def test_is_file_processed_false_not_cached(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        assert tracker.is_file_processed("missing.xml") is False

    @pytest.mark.unit
    def test_is_file_processed_hash_mismatch(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        tracker.mark_file_processed(fp, "pubmed")
        # Modify file content
        fp.write_text("changed content", encoding="utf-8")
        assert tracker.is_file_processed("data.xml", fp) is False

    @pytest.mark.unit
    def test_is_file_processed_file_gone_returns_true(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        tracker.mark_file_processed(fp, "pubmed")
        fp.unlink()
        # File is in cache but physical file gone -> returns True (line 264)
        assert tracker.is_file_processed("data.xml") is True

    @pytest.mark.unit
    def test_save_and_load_state(self, tmp_path):
        from acoharmony._cite.state import CiteStateTracker

        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        tracker.mark_file_processed(fp, "pubmed", record_count=5)

        # Create fresh tracker pointing to same state file
        state_file = tmp_path / "tracking" / "cite_state.json"
        mock_lw = MagicMock()
        tracker2 = CiteStateTracker(
            log_writer=mock_lw, state_file=state_file, search_paths=[tmp_path]
        )
        assert "data.xml" in tracker2._file_cache
        assert tracker2._file_cache["data.xml"].record_count == 5

    @pytest.mark.unit
    def test_load_state_corrupt_file(self, tmp_path):
        state_file = tmp_path / "tracking" / "cite_state.json"
        state_file.parent.mkdir(parents=True, exist_ok=True)
        state_file.write_text("NOT VALID JSON", encoding="utf-8")
        from acoharmony._cite.state import CiteStateTracker

        mock_lw = MagicMock()
        tracker = CiteStateTracker(log_writer=mock_lw, state_file=state_file, search_paths=[])
        assert tracker._file_cache == {}
        mock_lw.warning.assert_called()

    @pytest.mark.unit
    def test_mark_multiple_processed(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        f1 = self._create_file(tmp_path, "a.xml", "aaa")
        f2 = self._create_file(tmp_path, "b.xml", "bbb")
        f3 = tmp_path / "nonexistent.xml"  # does not exist
        states = tracker.mark_multiple_processed([f1, f2, f3], "crossref")
        assert len(states) == 2
        assert {s.filename for s in states} == {"a.xml", "b.xml"}

    @pytest.mark.unit
    def test_get_new_files(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        f1 = self._create_file(tmp_path, "old.xml", "old")
        f2 = self._create_file(tmp_path, "new.xml", "new")
        f3 = tmp_path / "ghost.xml"  # does not exist
        tracker.mark_file_processed(f1, "pubmed")
        new = tracker.get_new_files([f1, f2, f3])
        assert len(new) == 1
        assert new[0].name == "new.xml"

    @pytest.mark.unit
    def test_get_processing_stats_empty(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        stats = tracker.get_processing_stats()
        assert stats["total_files"] == 0
        assert stats["total_size"] == 0
        assert stats["total_records"] == 0
        assert stats["source_types"] == {}

    @pytest.mark.unit
    def test_get_processing_stats_populated(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        f1 = self._create_file(tmp_path, "a.xml", "aaa")
        f2 = self._create_file(tmp_path, "b.xml", "bbbbb")
        tracker.mark_file_processed(f1, "pubmed", record_count=10)
        tracker.mark_file_processed(f2, "crossref", record_count=20)
        stats = tracker.get_processing_stats()
        assert stats["total_files"] == 2
        assert stats["total_records"] == 30
        assert stats["total_size"] > 0
        assert "total_size_mb" in stats
        assert stats["source_types"]["pubmed"] == 1
        assert stats["source_types"]["crossref"] == 1

    @pytest.mark.unit
    def test_clear_file_status(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        tracker.mark_file_processed(fp, "pubmed")
        assert "data.xml" in tracker._file_cache
        tracker.clear_file_status("data.xml")
        assert "data.xml" not in tracker._file_cache

    @pytest.mark.unit
    def test_clear_file_status_nonexistent(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        # Should not raise
        tracker.clear_file_status("missing.xml")

    @pytest.mark.unit
    def test_get_processed_files_all(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        f1 = self._create_file(tmp_path, "a.xml", "aaa")
        f2 = self._create_file(tmp_path, "b.xml", "bbb")
        tracker.mark_file_processed(f1, "pubmed")
        tracker.mark_file_processed(f2, "crossref")
        all_files = tracker.get_processed_files()
        assert len(all_files) == 2

    @pytest.mark.unit
    def test_get_processed_files_filtered(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        f1 = self._create_file(tmp_path, "a.xml", "aaa")
        f2 = self._create_file(tmp_path, "b.xml", "bbb")
        tracker.mark_file_processed(f1, "pubmed")
        tracker.mark_file_processed(f2, "crossref")
        pm_files = tracker.get_processed_files(source_type="pubmed")
        assert len(pm_files) == 1
        assert pm_files[0].source_type == "pubmed"

    @pytest.mark.unit
    def test_mark_file_processed_with_corpus_path(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        corpus = tmp_path / "corpus" / "data.xml"
        state = tracker.mark_file_processed(fp, "pubmed", corpus_path=corpus)
        assert state.corpus_path == str(corpus)

    @pytest.mark.unit
    def test_update_file_location(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path, "data.xml")
        tracker.mark_file_processed(fp, "pubmed")
        new_path = tmp_path / "moved" / "data.xml"
        tracker.update_file_location("data.xml", new_path)
        assert tracker._file_cache["data.xml"].source_path == str(new_path)

    @pytest.mark.unit
    def test_update_file_location_missing_file(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        # Should not raise for non-cached file
        tracker.update_file_location("nonexistent.xml", tmp_path / "x.xml")

    @pytest.mark.unit
    def test_find_file_location_cached_path(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path, "data.xml")
        tracker.mark_file_processed(fp, "pubmed")
        found = tracker._find_file_location("data.xml")
        assert found == fp

    @pytest.mark.unit
    def test_find_file_location_search_paths(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        # Create file in search path but not in cache
        fp = self._create_file(tmp_path, "new.xml")
        found = tracker._find_file_location("new.xml")
        assert found == fp

    @pytest.mark.unit
    def test_find_file_location_not_found(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        assert tracker._find_file_location("ghost.xml") is None

    @pytest.mark.unit
    def test_is_file_processed_finds_in_different_location(self, tmp_path):
        """When file is found at a different path than cached, location is updated."""
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path, "data.xml", "content")
        tracker.mark_file_processed(fp, "pubmed")
        # Move file
        subdir = tmp_path / "sub"
        subdir.mkdir()
        new_fp = subdir / "data.xml"
        fp.rename(new_fp)
        # Add subdir as search path
        tracker.search_paths.append(subdir)
        # is_file_processed with no path should search and find at new location
        result = tracker.is_file_processed("data.xml")
        assert result is True

    @pytest.mark.unit
    def test_save_state_error_handled(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path)
        tracker.mark_file_processed(fp, "pubmed")
        # Make state file read-only directory to cause save failure
        with patch.object(tracker, "state_file", new=Path("/proc/nonexistent/state.json")):
            tracker._save_state()  # should not raise
            tracker.log_writer.error.assert_called()

    @pytest.mark.unit
    def test_compute_file_hash(self, tmp_path):
        tracker = self._make_tracker(tmp_path)
        fp = self._create_file(tmp_path, "hash_test.txt", "hello")
        expected = hashlib.sha256(b"hello").hexdigest()
        assert tracker._compute_file_hash(fp) == expected


# ===========================================================================
# 10. CiteStateTracker default init path (with StorageBackend mock)
# ===========================================================================


class TestCiteStateTrackerDefaultInit:
    """Test the default initialization path that uses StorageBackend."""

    @patch("acoharmony._store.StorageBackend")
    @pytest.mark.unit
    def test_default_init_with_path_objects(self, mock_storage_cls, tmp_path):
        from acoharmony._cite.state import CiteStateTracker

        mock_storage = MagicMock()
        mock_storage.get_path.side_effect = lambda key: {
            "logs": tmp_path / "logs",
            "cites/raw": tmp_path / "cites" / "raw",
            "cites/corpus": tmp_path / "cites" / "corpus",
        }[key]
        mock_storage_cls.return_value = mock_storage
        mock_lw = MagicMock()
        tracker = CiteStateTracker(log_writer=mock_lw)
        assert tracker.state_file == tmp_path / "logs" / "tracking" / "cite_state.json"
        assert len(tracker.search_paths) == 2

    @patch("acoharmony._store.StorageBackend")
    @pytest.mark.unit
    def test_default_init_with_string_paths(self, mock_storage_cls, tmp_path):
        from acoharmony._cite.state import CiteStateTracker

        mock_storage = MagicMock()
        mock_storage.get_path.side_effect = lambda key: {
            "logs": str(tmp_path / "logs"),
            "cites/raw": str(tmp_path / "cites" / "raw"),
            "cites/corpus": str(tmp_path / "cites" / "corpus"),
        }[key]
        mock_storage_cls.return_value = mock_storage
        mock_lw = MagicMock()
        tracker = CiteStateTracker(log_writer=mock_lw)
        assert tracker.state_file == Path(str(tmp_path / "logs")) / "tracking" / "cite_state.json"

    @patch("acoharmony._store.StorageBackend")
    @pytest.mark.unit
    def test_default_init_with_search_paths_override(self, mock_storage_cls, tmp_path):
        from acoharmony._cite.state import CiteStateTracker

        mock_storage = MagicMock()
        mock_storage.get_path.side_effect = lambda key: {
            "logs": tmp_path / "logs",
        }.get(key, tmp_path)
        mock_storage_cls.return_value = mock_storage
        mock_lw = MagicMock()
        tracker = CiteStateTracker(log_writer=mock_lw, search_paths=[tmp_path / "custom"])
        assert tracker.search_paths == [tmp_path / "custom"]
