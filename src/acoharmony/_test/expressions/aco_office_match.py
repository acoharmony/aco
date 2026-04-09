from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_office_match import (
    build_direct_office_select_expr,
    build_fuzzy_office_select_expr,
    build_office_location_alias_expr,
)


class TestAcoOfficeMatch:
    """Tests for _aco_office_match expression builders."""

    @pytest.mark.unit
    def test_build_direct_office_select_expr(self):
        exprs = build_direct_office_select_expr()
        assert len(exprs) == 3
        df = pl.DataFrame({'zip_code': ['62701'], 'office_name': ['Springfield Office'], 'market': ['IL']})
        result = df.select(exprs)
        assert result.columns == ['zip_code', 'office_name', 'market']

    @pytest.mark.unit
    def test_build_fuzzy_office_select_expr(self):
        exprs = build_fuzzy_office_select_expr()
        assert len(exprs) == 4
        df = pl.DataFrame({'zip_code': ['62701'], 'office_distance': [5.2], 'office_name': ['Springfield Office'], 'market': ['IL']})
        result = df.select(exprs)
        assert 'office_distance' in result.columns

    @pytest.mark.unit
    def test_build_office_location_alias_expr(self):
        df = pl.DataFrame({'market': ['NY']})
        result = df.select(build_office_location_alias_expr())
        assert result['office_location'][0] == 'NY'

    @pytest.mark.unit
    def test_build_office_location_alias_custom_col(self):
        df = pl.DataFrame({'my_market': ['IL']})
        result = df.select(build_office_location_alias_expr('my_market'))
        assert result['office_location'][0] == 'IL'
