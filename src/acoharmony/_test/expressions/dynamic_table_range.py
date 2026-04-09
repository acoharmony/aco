from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._dynamic_table_range import (
    DynamicRangeConfig,
    DynamicTableRangeExpression,
    TableDimensions,
)


class TestDynamicTableRange:
    """Tests for _dynamic_table_range expression builders."""

    @pytest.mark.unit
    def test_table_dimensions_properties(self):
        td = TableDimensions(header_row=0, data_start_row=1, data_end_row=10, first_col=0, last_col=4)
        assert td.total_rows == 10
        assert td.total_cols == 5

    @pytest.mark.unit
    def test_dynamic_range_config_defaults(self):
        config = DynamicRangeConfig()
        assert config.min_header_row == 0
        assert config.max_header_row == 10
        assert 'Total' in config.end_markers
        assert config.min_density_threshold == 0.3
        assert config.empty_row_threshold == 3

    @pytest.mark.unit
    def test_calculate_row_density(self):
        df = pl.DataFrame({'a': ['x', None], 'b': ['y', None], 'c': [None, None]})
        density = DynamicTableRangeExpression.calculate_row_density(df, 0)
        assert abs(density - 2.0 / 3) < 0.01
        density_empty = DynamicTableRangeExpression.calculate_row_density(df, 1)
        assert density_empty == 0.0

    @pytest.mark.unit
    def test_calculate_row_density_out_of_bounds(self):
        df = pl.DataFrame({'a': [1]})
        assert DynamicTableRangeExpression.calculate_row_density(df, 99) == 0.0

    @pytest.mark.unit
    def test_has_end_marker(self):
        df = pl.DataFrame({'a': ['Data', 'Total', 'More'], 'b': [1, 2, 3]})
        assert DynamicTableRangeExpression.has_end_marker(df, 0, ['Total']) is False
        assert DynamicTableRangeExpression.has_end_marker(df, 1, ['Total']) is True
        assert DynamicTableRangeExpression.has_end_marker(df, 99, ['Total']) is False

    @pytest.mark.unit
    def test_detect_header_row(self):
        config = DynamicRangeConfig()
        df = pl.DataFrame({'a': [None, 'ColA', 'val1'], 'b': [None, 'ColB', 'val2'], 'c': [None, 'ColC', 'val3']})
        header = DynamicTableRangeExpression.detect_header_row(df, config)
        assert header == 1

    @pytest.mark.unit
    def test_detect_header_row_fallback(self):
        config = DynamicRangeConfig()
        df = pl.DataFrame({'a': [None, None], 'b': [None, None], 'c': [None, None]})
        header = DynamicTableRangeExpression.detect_header_row(df, config)
        assert header == config.min_header_row

    @pytest.mark.unit
    def test_detect_end_row_with_marker(self):
        config = DynamicRangeConfig()
        df = pl.DataFrame({'a': ['ColA', 'val1', 'val2', 'Total'], 'b': ['ColB', '10', '20', '30']})
        end = DynamicTableRangeExpression.detect_end_row(df, 1, config)
        assert end == 2

    @pytest.mark.unit
    def test_detect_end_row_with_empty_rows(self):
        config = DynamicRangeConfig(empty_row_threshold=2)
        df = pl.DataFrame({'a': ['ColA', 'val1', None, None, 'extra'], 'b': ['ColB', 'val2', None, None, 'extra']})
        end = DynamicTableRangeExpression.detect_end_row(df, 1, config)
        assert end == 1

    @pytest.mark.unit
    def test_detect_last_column(self):
        df = pl.DataFrame({'a': ['val1'], 'b': ['val2'], 'c': [None]})
        last = DynamicTableRangeExpression.detect_last_column(df)
        assert last == 1

    @pytest.mark.unit
    def test_detect_full(self):
        config = DynamicRangeConfig()
        df = pl.DataFrame({'a': [None, 'Name', 'Alice', 'Bob', 'Total'], 'b': [None, 'Age', '30', '25', '55'], 'c': [None, None, None, None, None]})
        dims = DynamicTableRangeExpression.detect(df, config)
        assert dims.header_row == 1
        assert dims.data_start_row == 2
        assert dims.data_end_row == 3
        assert dims.last_col == 1
        assert dims.total_rows == 2
        assert dims.total_cols == 2

    @pytest.mark.unit
    def test_detect_end_row_no_marker_no_empty_threshold(self):
        """Branch 303->323: loop completes without hitting marker or empty threshold.

        All rows are dense data rows, no end marker present, and the empty
        row threshold is never reached, so the loop exhausts all rows and
        falls through to ``return last_data_row``.
        """
        config = DynamicRangeConfig(empty_row_threshold=10, end_markers=[])
        df = pl.DataFrame({
            'a': ['Header', 'val1', 'val2', 'val3'],
            'b': ['Header', 'val4', 'val5', 'val6'],
        })
        end = DynamicTableRangeExpression.detect_end_row(df, 1, config)
        assert end == 3  # last data row index

    @pytest.mark.unit
    def test_detect_last_column_all_null(self):
        """Branch 343->354: all columns are null so loop doesn't break early.

        When every column contains only nulls, the backward loop finds no
        non-null column and completes without breaking, returning
        ``len(df.columns) - 1`` as the default.
        """
        df = pl.DataFrame({
            'a': pl.Series([None, None], dtype=pl.Utf8),
            'b': pl.Series([None, None], dtype=pl.Utf8),
            'c': pl.Series([None, None], dtype=pl.Utf8),
        })
        last = DynamicTableRangeExpression.detect_last_column(df)
        assert last == 2  # defaults to last column index
