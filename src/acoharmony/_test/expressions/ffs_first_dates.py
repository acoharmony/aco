from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._ffs_first_dates import FfsFirstDatesExpression


class TestFfsFirstDatesExpression:

    @pytest.mark.unit
    def test_build_ffs_first_aggregations(self):
        exprs = FfsFirstDatesExpression.build_ffs_first_aggregations()
        assert isinstance(exprs, list)
        assert len(exprs) == 2

    @pytest.mark.unit
    def test_build_ffs_first_metadata_expr(self):
        df = pl.DataFrame({'bene_mbi_id': ['MBI1']})
        exprs = FfsFirstDatesExpression.build_ffs_first_metadata_expr()
        result = df.select(exprs)
        assert 'bene_mbi' in result.columns

    @pytest.mark.unit
    def test_build_ffs_first_select_columns(self):
        cols = FfsFirstDatesExpression.build_ffs_first_select_columns()
        assert isinstance(cols, list)
        assert all(isinstance(c, pl.Expr) for c in cols)
        assert len(cols) == 4
