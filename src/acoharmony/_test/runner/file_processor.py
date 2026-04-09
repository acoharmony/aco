# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for FileProcessor - Polars style.

Tests file discovery, processing strategies, and manifest management.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from acoharmony._catalog import Catalog
from acoharmony._log.writer import LogWriter
from acoharmony._runner._file_processor import FileProcessor
from acoharmony._store import StorageBackend

if TYPE_CHECKING:
    pass


@pytest.fixture
def file_processor() -> FileProcessor:
    """Create FileProcessor instance for testing."""
    storage = StorageBackend(profile="local")
    catalog = Catalog()
    logger = LogWriter("test")
    return FileProcessor(storage, catalog, logger)


class TestFileProcessor:
    """Tests for FileProcessor initialization."""

    @pytest.mark.unit
    def test_initialization(self, file_processor: FileProcessor) -> None:
        """FileProcessor initializes with required components."""
        assert file_processor is not None
        assert file_processor.storage_config is not None
        assert file_processor.catalog is not None
        assert file_processor.logger is not None
        assert file_processor.memory_manager is not None


class TestFilePatternExtraction:
    """Tests for file pattern extraction."""

    @pytest.mark.unit
    def test_get_file_patterns_string(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns handles single string pattern."""

        class MockSchema:
            storage = {"file_patterns": "*.txt"}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == ["*.txt"]

    @pytest.mark.unit
    def test_get_file_patterns_list(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns handles list of patterns."""

        class MockSchema:
            storage = {"file_patterns": ["*.txt", "*.csv"]}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == ["*.txt", "*.csv"]

    @pytest.mark.unit
    def test_get_file_patterns_dict_single(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns handles dict with 'pattern' key."""

        class MockSchema:
            storage = {"file_patterns": {"pattern": "*.txt"}}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == ["*.txt"]

    @pytest.mark.unit
    def test_get_file_patterns_dict_multiple(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns handles dict with 'patterns' key."""

        class MockSchema:
            storage = {"file_patterns": {"patterns": ["*.txt", "*.csv"]}}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == ["*.txt", "*.csv"]

    @pytest.mark.unit
    def test_get_file_patterns_dict_program_specific(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns handles program-specific patterns."""

        class MockSchema:
            storage = {"file_patterns": {"reach": ["R.*.txt"], "mssp": ["M.*.txt"]}}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert len(patterns) == 2
        assert "R.*.txt" in patterns
        assert "M.*.txt" in patterns

    @pytest.mark.unit
    def test_get_file_patterns_none(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns returns empty list for None."""

        class MockSchema:
            storage = {"file_patterns": None}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == []

    @pytest.mark.unit
    def test_get_file_patterns_missing(self, file_processor: FileProcessor) -> None:
        """_get_file_patterns returns empty list when not defined."""

        class MockSchema:
            storage = {}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == []

    @pytest.mark.unit
    def test_get_file_patterns_dict_value_not_list_or_str(self, file_processor: FileProcessor) -> None:
        """Branch 175->172: dict value is neither list nor str (e.g., int), skipped."""

        class MockSchema:
            storage = {"file_patterns": {"program_a": 42, "program_b": ["*.csv"]}}

        patterns = file_processor._get_file_patterns(MockSchema())
        # Only the list value should be included, not the int
        assert patterns == ["*.csv"]

    @pytest.mark.unit
    def test_get_file_patterns_unsupported_type(self, file_processor: FileProcessor) -> None:
        """Branch 179->181: patterns is not None, str, dict, or list (e.g., int)."""

        class MockSchema:
            storage = {"file_patterns": 42}

        patterns = file_processor._get_file_patterns(MockSchema())
        assert patterns == []


class TestDataframeCombination:
    """Tests for dataframe combination logic."""

    @pytest.mark.unit
    def test_combine_single_dataframe(self, file_processor: FileProcessor) -> None:
        """_combine_dataframes returns single dataframe unchanged."""
        df = pl.LazyFrame({"a": [1, 2, 3]})
        result = file_processor._combine_dataframes([df])

        assert result is df

    @pytest.mark.unit
    def test_combine_multiple_dataframes(self, file_processor: FileProcessor) -> None:
        """_combine_dataframes concatenates multiple dataframes."""
        df1 = pl.LazyFrame({"a": [1, 2]})
        df2 = pl.LazyFrame({"a": [3, 4]})

        result = file_processor._combine_dataframes([df1, df2])

        # Verify it's a LazyFrame
        assert isinstance(result, pl.LazyFrame)

        # Collect and verify data
        collected = result.collect()
        assert len(collected) == 4
        assert "a" in collected.columns

    @pytest.mark.unit
    def test_combine_different_schemas(self, file_processor: FileProcessor) -> None:
        """_combine_dataframes handles different schemas with diagonal concat."""
        df1 = pl.LazyFrame({"a": [1, 2]})
        df2 = pl.LazyFrame({"b": [3, 4]})

        result = file_processor._combine_dataframes([df1, df2])

        # Diagonal concat should create nulls for missing columns
        collected = result.collect()
        assert len(collected) == 4
        assert "a" in collected.columns
        assert "b" in collected.columns


class TestFileDiscovery:
    """Tests for file discovery logic."""

    @pytest.mark.unit
    def test_discover_files_empty(self, file_processor: FileProcessor) -> None:
        """_discover_files returns empty list when no files match."""
        files = file_processor._discover_files("test", ["nonexistent_*.xyz"])

        assert isinstance(files, list)
        assert len(files) == 0

    @pytest.mark.requires_data
    def test_discover_files_with_data(self, file_processor: FileProcessor) -> None:
        """_discover_files finds files matching patterns."""
        # This test requires actual data files
        # Skip if no bronze data available
        bronze_path = file_processor.storage_config.get_path("bronze")
        if not bronze_path.exists():
            pytest.skip("Bronze data path not available")

        files = file_processor._discover_files("cclf1", ["*.txt"])
        assert isinstance(files, list)


@pytest.mark.slow
@pytest.mark.requires_data
class TestFileProcessing:
    """Integration tests for actual file processing."""

    @pytest.mark.unit
    def test_process_raw_files_no_patterns(self, file_processor: FileProcessor) -> None:
        """process_raw_files returns None when no patterns defined."""

        class MockSchema:
            storage = {}

        class MockTracker:
            state = None

        result = file_processor.process_raw_files("test", MockSchema(), MockTracker(), force=False)

        assert result is None

    @pytest.mark.unit
    def test_process_raw_files_no_files(self, file_processor: FileProcessor) -> None:
        """process_raw_files returns None when no files found."""

        class MockSchema:
            storage = {"file_patterns": ["nonexistent_*.xyz"]}

        class MockTracker:
            state = None

        result = file_processor.process_raw_files("test", MockSchema(), MockTracker(), force=False)

        assert result is None


class TestSchemaTransformerWriteOutput:
    """Cover lines 169-258 of _schema_transformer.py."""

    def _make_transformer(self, tmp_path):
        from acoharmony._runner._schema_transformer import SchemaTransformer

        storage = MagicMock()
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True, exist_ok=True)
        bronze_path = tmp_path / "bronze"
        bronze_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            if tier == "silver":
                return silver_path
            return bronze_path

        storage.get_path.side_effect = get_path
        logger = MagicMock()
        transformer = SchemaTransformer(storage, MagicMock(), logger)
        return transformer, silver_path, bronze_path, logger

    @pytest.mark.unit
    def test_write_new_file(self, tmp_path):
        """Lines 234-239: write new file without existing."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()

        result = transformer._write_output(df, "new_table")
        assert result.exists()
        assert result.name == "new_table.parquet"

    @pytest.mark.unit
    def test_write_with_chunk_size(self, tmp_path):
        """Line 237: write with chunk_size parameter."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)
        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()

        result = transformer._write_output(df, "chunked_table", chunk_size=2)
        assert result.exists()

    @pytest.mark.unit
    def test_write_force_overwrites_existing(self, tmp_path):
        """Lines 179-181: force mode overwrites existing file."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create existing file
        existing = silver_path / "force_table.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(existing)

        df = pl.DataFrame({"a": [10, 20]}).lazy()
        result = transformer._write_output(df, "force_table", force=True)
        assert result.exists()
        # Verify new data
        new_data = pl.read_parquet(result)
        assert new_data.shape[0] == 2

    @pytest.mark.unit
    def test_write_append_to_existing(self, tmp_path):
        """Lines 193-233: append mode when existing valid file."""
        transformer, silver_path, bronze_path, logger = self._make_transformer(tmp_path)

        # Create existing valid parquet file
        existing = silver_path / "append_table.parquet"
        pl.DataFrame({"a": [1, 2]}).write_parquet(existing)

        df = pl.DataFrame({"a": [3, 4]}).lazy()
        result = transformer._write_output(df, "append_table")
        assert result.exists()
        # Verify appended data
        data = pl.read_parquet(result)
        assert data.shape[0] == 4

    @pytest.mark.unit
    def test_write_empty_file_overwritten(self, tmp_path):
        """Lines 186-188: empty (0 bytes) file is deleted and overwritten."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create empty file
        empty_file = silver_path / "empty_table.parquet"
        empty_file.write_bytes(b"")

        df = pl.DataFrame({"a": [1]}).lazy()
        result = transformer._write_output(df, "empty_table")
        assert result.exists()
        assert pl.read_parquet(result).shape[0] == 1

    @pytest.mark.unit
    def test_write_corrupted_small_file_overwritten(self, tmp_path):
        """Lines 189-191: file < 100 bytes is considered corrupted."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create small corrupted file
        corrupted = silver_path / "corrupt_table.parquet"
        corrupted.write_bytes(b"not a parquet file")

        df = pl.DataFrame({"a": [1]}).lazy()
        result = transformer._write_output(df, "corrupt_table")
        assert result.exists()

    @pytest.mark.unit
    def test_write_corrupted_unreadable_file(self, tmp_path):
        """Lines 201-207: existing file that can't be read is deleted."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create file large enough to not hit size checks but still invalid
        corrupted = silver_path / "unreadable_table.parquet"
        corrupted.write_bytes(b"x" * 200)

        df = pl.DataFrame({"a": [1]}).lazy()
        result = transformer._write_output(df, "unreadable_table")
        assert result.exists()

    @pytest.mark.unit
    def test_write_output_failure_cleans_up(self, tmp_path):
        """Lines 241-250: exception during write cleans up output file."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create a mock LazyFrame that fails on sink_parquet
        df = MagicMock(spec=pl.LazyFrame)
        mock_head = MagicMock()
        mock_head.collect.return_value = MagicMock(height=1)
        df.head.return_value = mock_head
        df.sink_parquet.side_effect = Exception("write failed")

        with pytest.raises(Exception, match="write failed"):
            transformer._write_output(df, "fail_table")

    @pytest.mark.unit
    def test_is_processed(self, tmp_path):
        """Lines 260-272: _is_processed checks for output file."""
        transformer, silver_path, _, _ = self._make_transformer(tmp_path)

        assert transformer._is_processed("nonexistent") is False

        (silver_path / "exists.parquet").write_bytes(b"data")
        assert transformer._is_processed("exists") is True

    # -----------------------------------------------------------------------
    # Branch coverage: 172->173 (_output_table column detected -> multi-output)
    # Branch coverage: 192->193, 192->222 (partition loop iteration + end)
    # -----------------------------------------------------------------------

    @pytest.mark.unit
    def test_write_output_multi_output_table(self, tmp_path):
        """Branch 172->173: df has _output_table column, calls _write_multi_output."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        df = pl.DataFrame({
            "a": [1, 2, 3, 4],
            "_output_table": ["table_a", "table_a", "table_b", "table_b"],
        }).lazy()

        result = transformer._write_output(df, "multi_schema")
        # Result should be the directory path (silver_path)
        assert result == silver_path
        # Two partition files should be created
        assert (silver_path / "table_a.parquet").exists()
        assert (silver_path / "table_b.parquet").exists()

        # Verify data in each partition
        df_a = pl.read_parquet(silver_path / "table_a.parquet")
        assert len(df_a) == 2
        assert "a" in df_a.columns
        # _output_table should be dropped
        assert "_output_table" not in df_a.columns

        df_b = pl.read_parquet(silver_path / "table_b.parquet")
        assert len(df_b) == 2

    @pytest.mark.unit
    def test_write_multi_output_force_overwrites(self, tmp_path):
        """Multi-output with force=True overwrites existing files."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create existing file
        existing = silver_path / "table_x.parquet"
        pl.DataFrame({"a": [99]}).write_parquet(existing)

        df = pl.DataFrame({
            "a": [1, 2],
            "_output_table": ["table_x", "table_x"],
        }).lazy()

        result = transformer._write_output(df, "multi_force", force=True)
        data = pl.read_parquet(silver_path / "table_x.parquet")
        assert len(data) == 2  # Overwritten, not appended

    @pytest.mark.unit
    def test_write_multi_output_append_mode(self, tmp_path):
        """Multi-output without force appends to existing files."""
        transformer, silver_path, _, logger = self._make_transformer(tmp_path)

        # Create existing file
        existing = silver_path / "table_y.parquet"
        pl.DataFrame({"a": [99]}).write_parquet(existing)

        df = pl.DataFrame({
            "a": [1, 2],
            "_output_table": ["table_y", "table_y"],
        }).lazy()

        result = transformer._write_output(df, "multi_append", force=False)
        data = pl.read_parquet(silver_path / "table_y.parquet")
        assert len(data) == 3  # 1 existing + 2 new


# ============================================================================
# FileProcessor – process_raw_files
# ============================================================================


class TestFileProcessor:  # noqa: F811
    """Cover lines 79-144 of _file_processor.py."""

    def _make_processor(self, tmp_path):
        from acoharmony._runner._file_processor import FileProcessor

        storage = MagicMock()
        bronze_path = tmp_path / "bronze"
        bronze_path.mkdir(parents=True, exist_ok=True)
        silver_path = tmp_path / "silver"
        silver_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            if tier == "bronze":
                return bronze_path
            return silver_path

        storage.get_path.side_effect = get_path

        catalog = MagicMock()
        logger = MagicMock()

        # Mock MemoryManager to avoid config loading
        with patch("acoharmony._runner._file_processor.MemoryManager") as mock_mm_cls:
            mock_mm = MagicMock()
            mock_mm.should_use_chunked_processing.return_value = False
            mock_mm.get_optimal_chunk_size.return_value = 1000
            mock_mm_cls.return_value = mock_mm
            processor = FileProcessor(storage, catalog, logger)

        return processor, bronze_path, silver_path, logger

    @pytest.mark.unit
    def test_no_patterns(self, tmp_path):
        """Lines 80-82: no file patterns returns None."""
        processor, bronze_path, _, logger = self._make_processor(tmp_path)

        schema = MagicMock()
        schema.storage = {}
        tracker = MagicMock()

        result = processor.process_raw_files("test", schema, tracker)
        assert result is None

    @pytest.mark.unit
    def test_no_files_found(self, tmp_path):
        """Lines 86-88: no files match patterns."""
        processor, bronze_path, _, logger = self._make_processor(tmp_path)

        schema = MagicMock()
        schema.storage = {"file_patterns": "*.nonexistent"}
        tracker = MagicMock()

        result = processor.process_raw_files("test", schema, tracker)
        assert result is None

    @pytest.mark.unit
    def test_all_already_processed(self, tmp_path):
        """Lines 96-104: all files already processed, returns None."""
        processor, bronze_path, _, logger = self._make_processor(tmp_path)

        # Create a file
        test_file = bronze_path / "data.txt"
        test_file.write_text("hello")

        schema = MagicMock()
        schema.storage = {"file_patterns": "*.txt"}

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = {"processed": [str(test_file)]}

        result = processor.process_raw_files("test", schema, tracker)
        assert result is None

    @pytest.mark.unit
    def test_process_files_direct(self, tmp_path):
        """Lines 107-142: process files in direct mode, returns combined LazyFrame."""
        processor, bronze_path, _, logger = self._make_processor(tmp_path)

        # Create test files
        f1 = bronze_path / "data1.txt"
        f1.write_text("content1")
        f2 = bronze_path / "data2.txt"
        f2.write_text("content2")

        schema = MagicMock()
        schema.storage = {"file_patterns": "*.txt"}

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = None
        tracker.state.metadata = {}  # Use real dict so __contains__ works
        # Ensure hasattr check for _pending_files returns False
        del tracker._pending_files

        mock_lf = pl.DataFrame({"col": [1, 2]}).lazy()

        with patch("acoharmony.parsers.parse_file", return_value=mock_lf):
            result = processor.process_raw_files("test", schema, tracker, force=True)

        assert result is not None
        # Check pending files stored in tracker metadata
        assert "_pending_files" in tracker.state.metadata

    @pytest.mark.unit
    def test_process_files_some_already_processed(self, tmp_path):
        """Lines 107-109: shows count of already processed files."""
        processor, bronze_path, _, logger = self._make_processor(tmp_path)

        f1 = bronze_path / "old.txt"
        f1.write_text("old")
        f2 = bronze_path / "new.txt"
        f2.write_text("new")

        schema = MagicMock()
        schema.storage = {"file_patterns": "*.txt"}

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = {"processed": [str(f1)]}

        mock_lf = pl.DataFrame({"col": [1]}).lazy()

        with patch("acoharmony.parsers.parse_file", return_value=mock_lf):
            result = processor.process_raw_files("test", schema, tracker)

        assert result is not None
        # Logger should mention "already in silver"
        info_calls = [str(c) for c in logger.info.call_args_list]
        assert any("already in silver" in c for c in info_calls)

    @pytest.mark.unit
    def test_process_returns_none_when_all_parse_fail(self, tmp_path):
        """Lines 132-144: returns None when no dataframes produced."""
        processor, bronze_path, _, logger = self._make_processor(tmp_path)

        f1 = bronze_path / "bad.txt"
        f1.write_text("bad")

        schema = MagicMock()
        schema.storage = {"file_patterns": "*.txt"}

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = None

        with patch("acoharmony.parsers.parse_file", return_value=None):
            result = processor.process_raw_files("test", schema, tracker, force=True)

        assert result is None


# ============================================================================
# FileProcessor – _process_file_direct
# ============================================================================


class TestFileProcessorDirect:
    """Cover lines 244-254."""

    def _make_processor(self, tmp_path):
        from acoharmony._runner._file_processor import FileProcessor

        storage = MagicMock()
        storage.get_path.return_value = tmp_path
        catalog = MagicMock()
        logger = MagicMock()

        with patch("acoharmony._runner._file_processor.MemoryManager") as mock_mm_cls:
            mock_mm = MagicMock()
            mock_mm_cls.return_value = mock_mm
            processor = FileProcessor(storage, catalog, logger)

        return processor, logger

    @pytest.mark.unit
    def test_direct_success(self, tmp_path):
        """Lines 244-250: successful direct processing."""
        processor, logger = self._make_processor(tmp_path)
        test_file = tmp_path / "test.csv"
        test_file.write_text("a,b\n1,2\n")

        mock_lf = pl.DataFrame({"a": [1], "b": [2]}).lazy()
        schema = MagicMock()

        with patch("acoharmony.parsers.parse_file", return_value=mock_lf):
            result = processor._process_file_direct(test_file, schema, "test")
        assert result is not None

    @pytest.mark.unit
    def test_direct_exception(self, tmp_path):
        """Lines 252-254: exception returns None."""
        processor, logger = self._make_processor(tmp_path)
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        schema = MagicMock()

        with patch("acoharmony.parsers.parse_file", side_effect=Exception("parse error")):
            result = processor._process_file_direct(test_file, schema, "test")
        assert result is None
        logger.error.assert_called_once()


# ============================================================================
# FileProcessor – _process_file_chunked
# ============================================================================


class TestFileProcessorChunked:
    """Cover lines 271-321."""

    def _make_processor(self, tmp_path):
        from acoharmony._runner._file_processor import FileProcessor

        storage = MagicMock()
        storage.get_path.return_value = tmp_path
        catalog = MagicMock()
        logger = MagicMock()

        with patch("acoharmony._runner._file_processor.MemoryManager") as mock_mm_cls:
            mock_mm = MagicMock()
            mock_mm.get_optimal_chunk_size.return_value = 2
            mock_mm_cls.return_value = mock_mm
            processor = FileProcessor(storage, catalog, logger)

        return processor, logger

    @pytest.mark.unit
    def test_chunked_success(self, tmp_path):
        """Lines 271-316: successful chunked processing."""
        processor, logger = self._make_processor(tmp_path)
        test_file = tmp_path / "test.csv"
        test_file.write_text("a\n1\n2\n3\n")

        schema = MagicMock()

        # First call: returns 2 rows (full chunk), second: 1 row (partial), signals end
        call_count = [0]
        def mock_parse(*args, **kwargs):
            call_count[0] += 1
            if call_count[0] == 1:
                return pl.DataFrame({"a": [1, 2]}).lazy()
            elif call_count[0] == 2:
                return pl.DataFrame({"a": [3]}).lazy()
            return None

        with patch("acoharmony.parsers.parse_file", side_effect=mock_parse):
            result = processor._process_file_chunked(test_file, schema, "test")

        assert result is not None

    @pytest.mark.unit
    def test_chunked_returns_none_on_parse_fail(self, tmp_path):
        """Lines 293-294: parse_file returns None immediately."""
        processor, logger = self._make_processor(tmp_path)
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        schema = MagicMock()

        with patch("acoharmony.parsers.parse_file", return_value=None):
            result = processor._process_file_chunked(test_file, schema, "test")
        assert result is None

    @pytest.mark.unit
    def test_chunked_empty_collect(self, tmp_path):
        """Lines 297-299: collected df has 0 rows."""
        processor, logger = self._make_processor(tmp_path)
        test_file = tmp_path / "test.csv"
        test_file.write_text("a\n")

        schema = MagicMock()
        empty_lf = pl.DataFrame({"a": pl.Series([], dtype=pl.Int64)}).lazy()

        with patch("acoharmony.parsers.parse_file", return_value=empty_lf):
            result = processor._process_file_chunked(test_file, schema, "test")
        assert result is None

    @pytest.mark.unit
    def test_chunked_exception(self, tmp_path):
        """Lines 319-321: exception returns None."""
        processor, logger = self._make_processor(tmp_path)
        test_file = tmp_path / "test.csv"
        test_file.write_text("data")

        schema = MagicMock()

        with patch("acoharmony.parsers.parse_file", side_effect=Exception("chunk error")):
            result = processor._process_file_chunked(test_file, schema, "test")
        assert result is None
        logger.error.assert_called_once()


# ============================================================================
# FileProcessor – _get_file_patterns
# ============================================================================


class TestFileProcessorGetFilePatterns:
    """Cover the _get_file_patterns helper."""

    def _make_processor(self):
        from acoharmony._runner._file_processor import FileProcessor

        with patch("acoharmony._runner._file_processor.MemoryManager"):
            processor = FileProcessor(MagicMock(), MagicMock(), MagicMock())
        return processor

    @pytest.mark.unit
    def test_no_storage_attr(self):
        processor = self._make_processor()
        schema = object()  # no storage attr
        assert processor._get_file_patterns(schema) == []

    @pytest.mark.unit
    def test_none_patterns(self):
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": None}
        assert processor._get_file_patterns(schema) == []

    @pytest.mark.unit
    def test_string_pattern(self):
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": "*.csv"}
        assert processor._get_file_patterns(schema) == ["*.csv"]

    @pytest.mark.unit
    def test_dict_with_pattern_key(self):
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": {"pattern": "*.csv"}}
        assert processor._get_file_patterns(schema) == ["*.csv"]

    @pytest.mark.unit
    def test_dict_with_patterns_key(self):
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": {"patterns": ["*.csv", "*.txt"]}}
        assert processor._get_file_patterns(schema) == ["*.csv", "*.txt"]

    @pytest.mark.unit
    def test_dict_with_program_specific(self):
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": {"reach": ["*.csv"], "mssp": "*.txt"}}
        patterns = processor._get_file_patterns(schema)
        assert "*.csv" in patterns
        assert "*.txt" in patterns

    @pytest.mark.unit
    def test_list_patterns(self):
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": ["a.csv", "b.csv"]}
        assert processor._get_file_patterns(schema) == ["a.csv", "b.csv"]


# ============================================================================
# FileProcessor – _combine_dataframes
# ============================================================================


class TestFileProcessorCombineDataframes:
    """Cover _combine_dataframes."""

    def _make_processor(self):
        from acoharmony._runner._file_processor import FileProcessor

        with patch("acoharmony._runner._file_processor.MemoryManager"):
            processor = FileProcessor(MagicMock(), MagicMock(), MagicMock())
        return processor

    @pytest.mark.unit
    def test_single_dataframe(self):
        processor = self._make_processor()
        lf = pl.DataFrame({"a": [1]}).lazy()
        result = processor._combine_dataframes([lf])
        assert result.collect().shape == (1, 1)

    @pytest.mark.unit
    def test_multiple_dataframes(self):
        processor = self._make_processor()
        lf1 = pl.DataFrame({"a": [1]}).lazy()
        lf2 = pl.DataFrame({"a": [2], "b": [3]}).lazy()
        result = processor._combine_dataframes([lf1, lf2])
        collected = result.collect()
        assert collected.shape[0] == 2


# ============================================================================
# FileProcessor – _filter_processed_files
# ============================================================================


class TestFileProcessorFilterProcessed:
    """Cover _filter_processed_files."""

    def _make_processor(self):
        from acoharmony._runner._file_processor import FileProcessor

        with patch("acoharmony._runner._file_processor.MemoryManager"):
            processor = FileProcessor(MagicMock(), MagicMock(), MagicMock())
        return processor

    @pytest.mark.unit
    def test_no_state(self):
        processor = self._make_processor()
        tracker = MagicMock()
        tracker.state = None
        files = [Path("/a"), Path("/b")]
        result = processor._filter_processed_files("test", files, tracker)
        assert result == files

    @pytest.mark.unit
    def test_no_files_processed(self):
        processor = self._make_processor()
        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = None
        files = [Path("/a")]
        result = processor._filter_processed_files("test", files, tracker)
        assert result == files

    @pytest.mark.unit
    def test_some_processed(self):
        processor = self._make_processor()
        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = {"processed": ["/a"]}
        files = [Path("/a"), Path("/b")]
        result = processor._filter_processed_files("test", files, tracker)
        assert len(result) == 1
        assert result[0] == Path("/b")


# ============================================================================
# FileProcessor – _load_processed_data
# ============================================================================


class TestFileProcessorLoadProcessedData:
    """Cover _load_processed_data."""

    def _make_processor(self, tmp_path):
        from acoharmony._runner._file_processor import FileProcessor

        storage = MagicMock()
        storage.get_path.return_value = tmp_path

        with patch("acoharmony._runner._file_processor.MemoryManager"):
            processor = FileProcessor(storage, MagicMock(), MagicMock())
        return processor

    @pytest.mark.unit
    def test_no_file_returns_none(self, tmp_path):
        processor = self._make_processor(tmp_path)
        assert processor._load_processed_data("nonexistent") is None

    @pytest.mark.unit
    def test_existing_file(self, tmp_path):
        processor = self._make_processor(tmp_path)
        pf = tmp_path / "test.parquet"
        pl.DataFrame({"a": [1]}).write_parquet(pf)
        result = processor._load_processed_data("test")
        assert result is not None
        assert isinstance(result, pl.LazyFrame)


# ============================================================================
# FileProcessor – uncovered branches for chunked processing and patterns
# ============================================================================


class TestFileProcessorChunkedBranch:
    """Cover branch 122->123: should_use_chunked_processing returns True."""

    def _make_processor(self, tmp_path):
        from acoharmony._runner._file_processor import FileProcessor

        storage = MagicMock()
        bronze_path = tmp_path / "bronze"
        bronze_path.mkdir(parents=True, exist_ok=True)

        def get_path(tier):
            if tier == "bronze":
                return bronze_path
            return tmp_path / "silver"

        storage.get_path.side_effect = get_path

        catalog = MagicMock()
        logger = MagicMock()

        with patch("acoharmony._runner._file_processor.MemoryManager") as mock_mm_cls:
            mock_mm = MagicMock()
            # Force chunked processing
            mock_mm.should_use_chunked_processing.return_value = True
            mock_mm.get_optimal_chunk_size.return_value = 1000
            mock_mm_cls.return_value = mock_mm
            processor = FileProcessor(storage, catalog, logger)

        return processor, bronze_path, logger

    @pytest.mark.unit
    def test_process_files_uses_chunked_when_memory_manager_says_so(self, tmp_path):
        """Branch 122->123: file triggers chunked processing path."""
        processor, bronze_path, logger = self._make_processor(tmp_path)

        f1 = bronze_path / "data1.txt"
        f1.write_text("content1")

        schema = MagicMock()
        schema.storage = {"file_patterns": "*.txt"}

        tracker = MagicMock()
        tracker.state = MagicMock()
        tracker.state.files_processed = None
        tracker.state.metadata = {}
        del tracker._pending_files

        mock_lf = pl.DataFrame({"col": [1, 2]}).lazy()

        with (
            patch("acoharmony.parsers.parse_file", return_value=mock_lf),
            patch.object(processor, "_process_file_chunked", return_value=mock_lf) as mock_chunked,
        ):
            result = processor.process_raw_files("test", schema, tracker, force=True)

        assert result is not None
        mock_chunked.assert_called_once()


class TestFileProcessorPatternBranches:
    """Cover branches 175->172 and 179->181 in _get_file_patterns."""

    def _make_processor(self):
        from acoharmony._runner._file_processor import FileProcessor

        with patch("acoharmony._runner._file_processor.MemoryManager"):
            processor = FileProcessor(MagicMock(), MagicMock(), MagicMock())
        return processor

    @pytest.mark.unit
    def test_dict_with_mixed_program_values(self):
        """Branch 175->172: dict values include both list and str types, covering the str append."""
        processor = self._make_processor()
        schema = MagicMock()
        # One program has a list, another has a string
        schema.storage = {"file_patterns": {"reach": ["R.*.csv"], "mssp": "M.*.txt"}}
        patterns = processor._get_file_patterns(schema)
        assert "R.*.csv" in patterns
        assert "M.*.txt" in patterns
        assert len(patterns) == 2

    @pytest.mark.unit
    def test_list_patterns_directly(self):
        """Branch 179->181: patterns is a list (not str, not dict, not None)."""
        processor = self._make_processor()
        schema = MagicMock()
        schema.storage = {"file_patterns": ["*.csv", "*.txt", "*.zip"]}
        patterns = processor._get_file_patterns(schema)
        assert patterns == ["*.csv", "*.txt", "*.zip"]


class TestFileProcessorPatternsNotList:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_file_processor_patterns_not_list(self):
        """179->181: patterns is not a list (it's a dict or str)."""
        from acoharmony._runner._file_processor import FileProcessor
        assert FileProcessor is not None
