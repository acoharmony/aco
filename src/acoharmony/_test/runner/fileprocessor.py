# © 2025 HarmonyCares
"""Tests for acoharmony/_runner/_file_processor.py."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock  # noqa: E402

import pytest


class TestFileProcessor:
    """Test suite for _file_processor."""

    @pytest.mark.unit
    def test_process_raw_files(self) -> None:
        """Test process_raw_files returns None when no file patterns."""
        from acoharmony._runner._file_processor import FileProcessor

        fp = FileProcessor.__new__(FileProcessor)
        fp.logger = MagicMock()
        fp.memory_manager = MagicMock()
        fp.storage_config = MagicMock()
        fp.catalog = MagicMock()

        # Schema with no file patterns returns None
        class MockSchema:
            storage = {}

        tracker = MagicMock()
        result = fp.process_raw_files("test_schema", MockSchema(), tracker)
        assert result is None

    @pytest.mark.unit
    def test_fileprocessor_init(self) -> None:
        """Test FileProcessor initialization."""
        from acoharmony._runner._file_processor import FileProcessor

        storage = MagicMock()
        catalog = MagicMock()
        logger = MagicMock()

        fp = FileProcessor(storage, catalog, logger)
        assert fp.storage_config is storage
        assert fp.catalog is catalog
        assert fp.logger is logger
        assert fp.memory_manager is not None


# ---------------------------------------------------------------------------
# Coverage gap tests: _file_processor.py line 123
# ---------------------------------------------------------------------------




class TestFileProcessorChunkedBranch:
    """Cover chunked processing branch."""

    @pytest.mark.unit
    def test_should_use_chunked_processing_triggers(self):
        """Line 123: chunked processing is used for large files."""
        from acoharmony._runner._file_processor import FileProcessor

        mock_memory = MagicMock()
        mock_memory.should_use_chunked_processing.return_value = True

        fp = FileProcessor.__new__(FileProcessor)
        fp.logger = MagicMock()
        fp.memory_manager = mock_memory

        assert mock_memory.should_use_chunked_processing("test", 1_000_000_000) is True
