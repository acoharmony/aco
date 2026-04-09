"""Tests for acoharmony._puf.utils module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._puf.utils is not None


def _make_task(url="https://example.com/file.zip", key="test_file",
               year="2024", rule_type="Final", force_refresh=False):
    """Create a mock DownloadTask."""
    file_meta = MagicMock()
    file_meta.url = url
    file_meta.key = key
    file_meta.category = "addenda"
    file_meta.format = "zip"
    file_meta.description = "Test file"
    file_meta.schema_mapping = None

    task = MagicMock()
    task.file_metadata = file_meta
    task.year = year
    task.rule_type = rule_type
    task.force_refresh = force_refresh
    task.priority = 5
    task.tags = ["test"]
    task.to_cite_kwargs.return_value = {"url": url, "force_refresh": force_refresh}
    return task


class TestBatchDownload:
    """Tests for batch_download uncovered branches."""

    @pytest.mark.unit
    def test_skip_existing_file(self):
        """Branch 97→98, 97→112: skip already processed file."""
        from acoharmony._puf.utils import batch_download

        task = _make_task()
        mock_state = MagicMock()
        mock_state.is_file_processed.return_value = True

        with (
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
            patch("acoharmony._puf.utils.StorageBackend"),
            patch("acoharmony._transforms._cite.transform_cite"),
            patch("acoharmony._expressions._cite_download.build_url_hash_expr",
                  return_value=pl.lit("abc123").alias("url_hash")),
            patch("acoharmony._expressions._cite_download.build_content_extension_expr",
                  return_value=pl.lit("zip").alias("content_extension")),
        ):
            results = batch_download([task], delay_between_downloads=0)

        assert results["skipped"] == 1
        assert results["downloaded"] == 0

    @pytest.mark.unit
    def test_rate_limiting_delay(self):
        """Branch 148→149: delay between downloads (not last)."""
        from acoharmony._puf.utils import batch_download

        task1 = _make_task(key="f1")
        task2 = _make_task(key="f2")

        mock_state = MagicMock()
        mock_state.is_file_processed.return_value = False

        mock_lf = MagicMock()
        mock_df = MagicMock()
        mock_df.__getitem__ = MagicMock(return_value=["hash1"])
        mock_lf.collect.return_value = mock_df

        with (
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
            patch("acoharmony._puf.utils.StorageBackend"),
            patch("acoharmony._transforms._cite.transform_cite", return_value=mock_lf),
            patch("acoharmony._expressions._cite_download.build_url_hash_expr",
                  return_value=pl.lit("abc").alias("url_hash")),
            patch("acoharmony._expressions._cite_download.build_content_extension_expr",
                  return_value=pl.lit("zip").alias("content_extension")),
            patch("acoharmony._puf.utils.time.sleep") as mock_sleep,
        ):
            results = batch_download([task1, task2], delay_between_downloads=0.01)

        # sleep called once (between task1 and task2, not after task2)
        assert mock_sleep.call_count == 1
        assert results["downloaded"] == 2


class TestCheckDownloadStatus:
    """Tests for check_download_status uncovered branches."""

    @pytest.mark.unit
    def test_processed_and_not_processed(self):
        """Branches 222→250, 235→236, 235→238: processed and not processed tasks."""
        from acoharmony._puf.utils import check_download_status

        task_processed = _make_task(url="https://example.com/a.zip", key="a")
        task_not_processed = _make_task(url="https://example.com/b.zip", key="b")

        call_count = [0]

        def mock_is_processed(filename):
            call_count[0] += 1
            return call_count[0] == 1  # First call returns True, second False

        mock_state = MagicMock()
        mock_state.is_file_processed.side_effect = mock_is_processed

        with (
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
            patch("acoharmony._expressions._cite_download.build_url_hash_expr",
                  return_value=pl.lit("hash").alias("url_hash")),
            patch("acoharmony._expressions._cite_download.build_content_extension_expr",
                  return_value=pl.lit("zip").alias("content_extension")),
        ):
            status = check_download_status([task_processed, task_not_processed])

        assert status["processed"] == 1
        assert status["not_processed"] == 1
        assert status["total"] == 2
        assert len(status["details"]) == 2


class TestGetCorpusFilesForYear:
    """Tests for get_corpus_files_for_year uncovered branches."""

    @pytest.mark.unit
    def test_with_matching_files(self, tmp_path):
        """Branch 287→285: iterate over filtered rows and collect corpus paths."""
        from acoharmony._puf.utils import get_corpus_files_for_year

        corpus_dir = tmp_path / "cites" / "corpus"
        corpus_dir.mkdir(parents=True)
        corpus_file = corpus_dir / "abc.parquet"
        corpus_file.write_text("data")

        state_df = pl.DataFrame({
            "metadata": ['{"year": "2024", "rule_type": "Final"}'],
            "corpus_path": [str(corpus_file)],
        })

        mock_state = MagicMock()
        mock_state.get_state.return_value = state_df

        mock_storage = MagicMock()
        mock_storage.get_path.return_value = str(corpus_dir)

        with (
            patch("acoharmony._puf.utils.StorageBackend", return_value=mock_storage),
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
        ):
            files = get_corpus_files_for_year("2024")

        assert len(files) == 1
        assert files[0] == corpus_file


class TestValidateFileDownloads:
    """Tests for validate_file_downloads uncovered branches."""

    @pytest.mark.unit
    def test_valid_file_with_size_check(self, tmp_path):
        """Branch 355→367: check_file_size=True with valid file."""
        from acoharmony._puf.utils import validate_file_downloads

        corpus_dir = tmp_path / "cites" / "corpus"
        corpus_dir.mkdir(parents=True)
        corpus_file = corpus_dir / "hashval.parquet"
        corpus_file.write_text("real data content")

        task = _make_task()

        mock_state = MagicMock()
        mock_state.is_file_processed.return_value = True

        mock_storage = MagicMock()
        mock_storage.get_path.return_value = str(corpus_file)

        with (
            patch("acoharmony._puf.utils.StorageBackend", return_value=mock_storage),
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
            patch("acoharmony._expressions._cite_download.build_url_hash_expr",
                  return_value=pl.lit("hashval").alias("url_hash")),
            patch("acoharmony._expressions._cite_download.build_content_extension_expr",
                  return_value=pl.lit("zip").alias("content_extension")),
        ):
            result = validate_file_downloads([task], check_file_size=True)

        assert result["valid"] == 1
        assert result["invalid"] == 0


class TestGetCorpusFilesCorpusPathNotExists:
    """Cover branch 287->285: corpus_path is present but file doesn't exist."""

    @pytest.mark.unit
    def test_corpus_path_present_but_file_missing(self, tmp_path):
        """Branch 287->285: corpus_path in row but file doesn't exist on disk, skipped."""
        from acoharmony._puf.utils import get_corpus_files_for_year

        state_df = pl.DataFrame({
            "metadata": ['{"year": "2024", "rule_type": "Final"}'],
            "corpus_path": ["/nonexistent/path/fake.parquet"],
        })

        mock_state = MagicMock()
        mock_state.get_state.return_value = state_df

        mock_storage = MagicMock()
        mock_storage.get_path.return_value = str(tmp_path)

        with (
            patch("acoharmony._puf.utils.StorageBackend", return_value=mock_storage),
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
        ):
            files = get_corpus_files_for_year("2024")

        # File doesn't exist, so it should not be included
        assert len(files) == 0

    @pytest.mark.unit
    def test_corpus_path_none_skipped(self, tmp_path):
        """Branch 287->285: corpus_path is None, skipped."""
        from acoharmony._puf.utils import get_corpus_files_for_year

        state_df = pl.DataFrame({
            "metadata": ['{"year": "2024", "rule_type": "Final"}'],
            "corpus_path": [None],
        })

        mock_state = MagicMock()
        mock_state.get_state.return_value = state_df

        mock_storage = MagicMock()
        mock_storage.get_path.return_value = str(tmp_path)

        with (
            patch("acoharmony._puf.utils.StorageBackend", return_value=mock_storage),
            patch("acoharmony._puf.utils.CiteStateTracker", return_value=mock_state),
        ):
            files = get_corpus_files_for_year("2024")

        assert len(files) == 0
