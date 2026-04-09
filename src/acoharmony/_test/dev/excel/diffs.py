"""Tests for acoharmony._dev.excel.diffs module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony
from acoharmony._dev.excel.diffs import ExcelDiffAnalyzer


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.excel.diffs is not None


class TestCompareSheetsNoMatchingIndex:
    """Cover branch 402->399: inner loop doesn't find matching sheet_index."""

    @pytest.mark.unit
    def test_sheet_not_present_in_some_files(self):
        """When a file has sheets but none match the target index, skip it."""
        analyzer = ExcelDiffAnalyzer(analysis_files=[])
        # Manually populate analyses: file_a has sheet index 0 and 1,
        # file_b only has sheet index 0. When iterating sheet_idx=1,
        # file_b's inner loop should exhaust without break (402->399).
        analyzer.analyses = {
            "file_a.json": {
                "sheets": {
                    "Sheet1": {"sheet_index": 0, "sheet_name": "First", "dimensions": {"rows": 10, "cols": 5}, "potential_headers": []},
                    "Sheet2": {"sheet_index": 1, "sheet_name": "Second", "dimensions": {"rows": 20, "cols": 3}, "potential_headers": []},
                }
            },
            "file_b.json": {
                "sheets": {
                    "Sheet1": {"sheet_index": 0, "sheet_name": "First", "dimensions": {"rows": 10, "cols": 5}, "potential_headers": []},
                }
            },
        }
        comparisons = analyzer.compare_sheets()
        # Should have 2 sheets (max index is 1 -> range(2))
        assert len(comparisons) == 2
        # Sheet index 1: only file_a should be present
        sheet1_comp = comparisons[1]
        assert "file_a.json" in sheet1_comp.files_present
        assert "file_b.json" not in sheet1_comp.files_present
