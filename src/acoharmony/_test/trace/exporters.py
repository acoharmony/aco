# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for exporters module."""

# TODO: Implement comprehensive tests for this module
# TODO: Add property-based tests using hypothesis where appropriate
# TODO: Ensure all edge cases are covered
# TODO: Add integration tests if needed

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    pass


from pathlib import Path
from unittest.mock import MagicMock

from acoharmony._trace.exporters import FileSpanExporter


class TestFileSpanExporter:
    """Tests for FileSpanExporter."""

    @pytest.mark.unit
    def test_filespanexporter_initialization(self, tmp_path: Path) -> None:
        """FileSpanExporter can be initialized."""
        exporter = FileSpanExporter(tmp_path / 'traces')
        assert exporter.base_path == tmp_path / 'traces'
        assert exporter.base_path.exists()
        assert exporter._file_handle is None
        exporter.shutdown()

    @pytest.mark.unit
    def test_filespanexporter_basic_functionality(self, tmp_path: Path) -> None:
        """FileSpanExporter basic functionality works -- export and shutdown."""
        from opentelemetry.sdk.trace.export import SpanExportResult

        exporter = FileSpanExporter(tmp_path / 'traces')

        # Create a mock span with all necessary attributes
        mock_span = MagicMock()
        mock_span.context.trace_id = 0x1234567890ABCDEF1234567890ABCDEF
        mock_span.context.span_id = 0x1234567890ABCDEF
        mock_span.parent = None
        mock_span.name = 'test_span'
        mock_span.start_time = 1_000_000_000_000  # 1s in nanoseconds
        mock_span.end_time = 2_000_000_000_000
        mock_span.status.status_code.name = 'OK'
        mock_span.status.description = None
        mock_span.attributes = {'key': 'value'}
        mock_span.events = []

        result = exporter.export([mock_span])
        assert result == SpanExportResult.SUCCESS

        exporter.shutdown()
        assert exporter._file_handle is None

        # Verify file was written
        trace_files = list((tmp_path / 'traces').glob('*.jsonl'))
        assert len(trace_files) == 1
