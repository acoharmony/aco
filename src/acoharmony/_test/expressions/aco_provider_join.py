from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_provider_join import (
    build_null_provider_columns_expr,
    build_provider_attribution_select_expr,
)


class TestAcoProviderJoin:
    """Tests for _aco_provider_join expression builders."""

    @pytest.mark.unit
    def test_build_provider_attribution_select_expr(self):
        exprs = build_provider_attribution_select_expr()
        assert len(exprs) == 13

    @pytest.mark.unit
    def test_build_null_provider_columns_expr(self):
        exprs = build_null_provider_columns_expr()
        assert len(exprs) == 12
        df = pl.DataFrame({'x': [1]})
        result = df.select(exprs)
        assert 'mssp_tin' in result.columns
        assert 'latest_aco_id' in result.columns
        for col in result.columns:
            assert result[col][0] is None
