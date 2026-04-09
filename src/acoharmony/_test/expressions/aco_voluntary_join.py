from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_voluntary_join import (
    build_valid_voluntary_alignment_expr,
    build_voluntary_alignment_select_expr,
)


class TestAcoVoluntaryJoin:
    """Tests for _aco_voluntary_join expression builders."""

    @pytest.mark.unit
    def test_build_voluntary_alignment_select_expr(self):
        exprs = build_voluntary_alignment_select_expr()
        assert len(exprs) == 18

    @pytest.mark.unit
    def test_build_valid_voluntary_alignment_expr(self):
        df = pl.DataFrame({'sva_provider_valid': [True, False, None, True], 'current_program': ['REACH', 'REACH', 'REACH', 'MSSP']})
        result = df.select(build_valid_voluntary_alignment_expr())
        assert result['has_valid_voluntary_alignment'].to_list() == [True, False, False, False]

    @pytest.mark.unit
    def test_build_valid_voluntary_alignment_expr_custom_cols(self):
        df = pl.DataFrame({'prov_valid': [True], 'prog': ['REACH']})
        result = df.select(build_valid_voluntary_alignment_expr('prov_valid', 'prog'))
        assert result['has_valid_voluntary_alignment'][0] is True
