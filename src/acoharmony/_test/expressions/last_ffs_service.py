from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._last_ffs_service import LastFfsServiceExpression


class TestLastFfsServiceExpression:

    @pytest.mark.unit
    def test_build_last_ffs_aggregations(self):
        exprs = LastFfsServiceExpression.build_last_ffs_aggregations()
        assert len(exprs) == 2

    @pytest.mark.unit
    def test_build_last_ffs_provider_aggregations(self):
        exprs = LastFfsServiceExpression.build_last_ffs_provider_aggregations()
        assert len(exprs) == 4

    @pytest.mark.unit
    def test_build_last_ffs_metadata_expr(self):
        df = pl.DataFrame({'bene_mbi_id': ['M1']})
        result = df.select(LastFfsServiceExpression.build_last_ffs_metadata_expr())
        assert 'bene_mbi' in result.columns

    @pytest.mark.unit
    def test_build_last_ffs_select_columns(self):
        cols = LastFfsServiceExpression.build_last_ffs_select_columns()
        assert isinstance(cols, list)
        assert all(isinstance(c, pl.Expr) for c in cols)
        assert len(cols) == 6
