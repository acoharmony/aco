from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import datetime

import polars as pl
import pytest

from acoharmony._expressions._file_version import FileVersionExpression


class TestFileVersionExpression:

    @pytest.mark.unit
    def test_filter_most_recent_by_filename(self):
        df = pl.DataFrame({'source_filename': ['Report_A.xlsx', 'Report_B.xlsx', 'Report_A.xlsx'], 'data': [1, 2, 3]})
        expr = FileVersionExpression.filter_most_recent_by_filename()
        result = df.filter(expr)
        assert len(result) == 1
        assert result['source_filename'][0] == 'Report_B.xlsx'

    @pytest.mark.unit
    def test_filter_most_recent_by_processed_at(self):
        df = pl.DataFrame({'processed_at': [datetime(2024, 1, 1), datetime(2024, 6, 1), datetime(2024, 1, 1)], 'data': [1, 2, 3]})
        expr = FileVersionExpression.filter_most_recent_by_processed_at()
        result = df.filter(expr)
        assert len(result) == 1
        assert result['data'][0] == 2

    @pytest.mark.unit
    def test_filter_most_recent_source_file(self):
        df = pl.DataFrame({'source_file': ['/a/b.csv', '/a/c.csv'], 'x': [1, 2]})
        expr = FileVersionExpression.filter_most_recent_source_file()
        result = df.filter(expr)
        assert result['source_file'][0] == '/a/c.csv'

    @pytest.mark.unit
    def test_get_most_recent_filename(self):
        df = pl.DataFrame({'source_filename': ['A.csv', 'Z.csv']})
        expr = FileVersionExpression.get_most_recent_filename()
        result = df.select(expr)
        assert result[0, 0] == 'Z.csv'

    @pytest.mark.unit
    def test_add_file_version_rank(self):
        df = pl.DataFrame({'source_filename': ['A.csv', 'C.csv', 'B.csv']})
        expr = FileVersionExpression.add_file_version_rank()
        result = df.with_columns(expr)
        assert 'file_version_rank' in result.columns

    @pytest.mark.unit
    def test_filter_most_recent_by_date_in_filename(self):
        expr = FileVersionExpression.filter_most_recent_by_date_in_filename()
        assert isinstance(expr, pl.Expr)

    @pytest.mark.unit
    def test_keep_only_most_recent_file(self):
        expr = FileVersionExpression.keep_only_most_recent_file()
        assert isinstance(expr, pl.Expr)
