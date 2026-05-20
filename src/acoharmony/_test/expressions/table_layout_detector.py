from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._table_layout_detector import TableLayout, TableLayoutDetector


class TestTableLayoutDetectorUnknown:
    """Cover UNKNOWN layout return."""

    @pytest.mark.unit
    def test_detect_unknown_layout(self):
        """Line 349: low confidence returns UNKNOWN."""
        df = pl.DataFrame({'a': [None, None, None], 'b': [None, None, None]})
        result = TableLayoutDetector.detect(df)
        assert isinstance(result, TableLayout)


class TestIsLongKeyValueFewRows:
    """Cover branch 178→183: 2-column DF with fewer than 3 rows skips row-count bonus."""

    @pytest.mark.unit
    def test_two_columns_fewer_than_three_rows(self):
        """With only 2 rows the height>=3 guard is False, jumping to line 183."""
        df = pl.DataFrame({
            'key': ['Name', 'Age'],
            'value': ['Alice', '30'],
        })
        is_match, confidence, reasons = TableLayoutDetector.is_long_key_value(df)
        # 2 columns → +0.4, height<3 → no +0.3, high density → +0.3, avg_length>5 → may add +0.2
        assert 'Has 2 columns' in reasons
        # The row-count reason must NOT appear because height < 3
        assert not any('rows' in r and 'sufficient' in r for r in reasons)


class TestIsLongKeyValueLabelStrings:
    """Cover branch 196→197: avg_length > 5 adds label-like reason."""

    @pytest.mark.unit
    def test_first_column_label_like_strings(self):
        """Long strings in first column trigger avg_length > 5 branch."""
        df = pl.DataFrame({
            'key': ['Patient Name', 'Date of Birth', 'Insurance Provider', 'Phone Number'],
            'value': ['Alice', '1990-01-01', 'BCBS', '555-1234'],
        })
        is_match, confidence, reasons = TableLayoutDetector.is_long_key_value(df)
        assert 'First column has label-like strings' in reasons


class TestIsWideTabularSingleRow:
    """Cover branch 229→235: wide DF with < 2 rows skips row-count bonus."""

    @pytest.mark.unit
    def test_wide_table_single_row(self):
        """With 1 data row the height>=2 guard is False, jumping to line 235."""
        df = pl.DataFrame({
            'col_a': ['x'],
            'col_b': ['y'],
            'col_c': ['z'],
        })
        is_match, confidence, reasons = TableLayoutDetector.is_wide_tabular(df)
        # Has >2 columns → +0.4, height<2 → no +0.3
        assert f'Has {len(df.columns)} columns' in reasons
        # The data-rows reason must NOT appear
        assert not any('data rows' in r for r in reasons)


class TestIsWideTabularLowDensity:
    """Cover branch 243→247: avg_density < 0.5 skips density reason."""

    @pytest.mark.unit
    def test_sparse_wide_table(self):
        """Wide table where most columns are null gives avg_density < 0.5."""
        df = pl.DataFrame({
            'a': ['x', None, None, None],
            'b': [None, None, None, None],
            'c': [None, None, None, None],
        })
        is_match, confidence, reasons = TableLayoutDetector.is_wide_tabular(df)
        # Density reason should NOT appear
        assert not any('density' in r.lower() for r in reasons)


class TestIsMultiLevelHeaderFewRows:
    """Cover branch 284→298: df.height < 3 skips the header-row string check."""

    @pytest.mark.unit
    def test_multi_level_header_fewer_than_three_rows(self):
        """With fewer than 3 rows, the header-row pattern check is skipped."""
        df = pl.DataFrame({
            'col_0': ['Header1'],
            'col_1': ['Header2'],
        })
        is_match, confidence, reasons = TableLayoutDetector.is_multi_level_header(df)
        # Generic column names → +0.5, but no first-rows check
        assert 'Has generic column names (likely raw Excel columns)' in reasons
        assert not any('First rows' in r for r in reasons)


class TestIsMultiLevelHeaderNonStringValue:
    """Cover branches 290→291 and 294→298: non-string value in first rows
    makes first_rows_all_strings False."""

    @pytest.mark.unit
    def test_non_string_in_first_rows(self):
        """A numeric value in the first rows sets first_rows_all_strings=False."""
        df = pl.DataFrame({
            'col_0': ['Header1', 'Header2', 'Header3', 'data'],
            'col_1': [1, 2, 3, 4],
        })
        is_match, confidence, reasons = TableLayoutDetector.is_multi_level_header(df)
        # Generic names → +0.5, but first_rows_all_strings is False → no +0.3
        assert 'Has generic column names (likely raw Excel columns)' in reasons
        assert not any('First rows are all strings' in r for r in reasons)


class TestDetectEmptyDataFrame:
    """Cover branch 323→324: detect() on empty DataFrame returns UNKNOWN."""

    @pytest.mark.unit
    def test_detect_empty_dataframe(self):
        """Empty DataFrame (0 rows) returns UNKNOWN immediately."""
        df = pl.DataFrame()
        result = TableLayoutDetector.detect(df)
        assert result == TableLayout.UNKNOWN

    @pytest.mark.unit
    def test_detect_zero_rows(self):
        """DataFrame with columns but 0 rows returns UNKNOWN."""
        df = pl.DataFrame({'a': [], 'b': []}).cast(pl.Utf8)
        result = TableLayoutDetector.detect(df)
        assert result == TableLayout.UNKNOWN


class TestDetectLowConfidenceUnknown:
    """Cover branch 346→349: best_confidence < 0.5 returns UNKNOWN."""

    @pytest.mark.unit
    def test_detect_low_confidence(self):
        """A single-column, single-row DF won't match any layout well."""
        df = pl.DataFrame({'a': [None]})
        result = TableLayoutDetector.detect(df)
        assert result == TableLayout.UNKNOWN


class TestDetectWithDetailsEmpty:
    """Cover branches 366→367: detect_with_details on empty DataFrame."""

    @pytest.mark.unit
    def test_details_empty_dataframe(self):
        """Empty DataFrame returns UNKNOWN result with 0 confidence."""
        df = pl.DataFrame()
        result = TableLayoutDetector.detect_with_details(df)
        assert result.layout == TableLayout.UNKNOWN
        assert result.confidence == 0.0
        assert 'Empty DataFrame' in result.reasons
        assert result.column_count == 0
        assert result.row_count == 0

    @pytest.mark.unit
    def test_details_zero_rows_with_columns(self):
        """DataFrame with columns but 0 rows also hits the empty branch."""
        df = pl.DataFrame({'a': [], 'b': []}).cast(pl.Utf8)
        result = TableLayoutDetector.detect_with_details(df)
        assert result.layout == TableLayout.UNKNOWN
        assert result.confidence == 0.0


class TestDetectWithDetailsNonEmpty:
    """Cover branch 366→376: detect_with_details on a non-empty DataFrame."""

    @pytest.mark.unit
    def test_details_wide_tabular(self):
        """A well-formed wide table returns details with high confidence."""
        df = pl.DataFrame({
            'name': ['Alice', 'Bob', 'Carol'],
            'age': ['30', '25', '40'],
            'city': ['NYC', 'LA', 'CHI'],
        })
        result = TableLayoutDetector.detect_with_details(df)
        assert result.layout == TableLayout.WIDE_TABULAR
        assert result.confidence >= 0.5
        assert result.column_count == 3
        assert result.row_count == 3


class TestDetectWithDetailsLowConfidence:
    """Cover branch 395→396: best_confidence < 0.5 in detect_with_details."""

    @pytest.mark.unit
    def test_details_low_confidence_unknown(self):
        """Single-column DF produces low confidence → UNKNOWN with override reasons."""
        df = pl.DataFrame({'a': [None]})
        result = TableLayoutDetector.detect_with_details(df)
        assert result.layout == TableLayout.UNKNOWN
        assert 'No layout matched with sufficient confidence' in result.reasons


class TestDetectWithDetailsHighConfidence:
    """Cover branch 395→399: best_confidence >= 0.5 in detect_with_details."""

    @pytest.mark.unit
    def test_details_high_confidence(self):
        """A key-value DF has confidence >= 0.5 and returns matching layout."""
        df = pl.DataFrame({
            'key': ['Patient Name', 'Date of Birth', 'Insurance', 'Phone'],
            'value': ['Alice', '1990-01-01', 'BCBS', '555-1234'],
        })
        result = TableLayoutDetector.detect_with_details(df)
        assert result.layout == TableLayout.LONG_KEY_VALUE
        assert result.confidence >= 0.5
        assert result.column_count == 2
        assert result.row_count == 4
