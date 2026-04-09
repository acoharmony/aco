from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._aco_transition_termed_bar import build_termed_bar_expr


class TestAcoTransitionTermedBar:
    """Tests for _aco_transition_termed_bar expression builders."""

    @pytest.mark.unit
    def test_build_termed_bar_expr(self):
        df = pl.DataFrame({'ym_202412_reach': [True, True, False], 'ym_202501_reach': [False, True, False], 'last_reach_date': [date(2024, 11, 30), date(2025, 1, 15), None]})
        result = df.select(build_termed_bar_expr(2025, 2024, current_year_months=[1]))
        assert result['termed_bar_2024'][0] is True
        assert result['termed_bar_2024'][1] is False
        assert result['termed_bar_2024'][2] is False

    @pytest.mark.unit
    def test_build_termed_bar_expr_default_months(self):
        """Line 43->44: current_year_months=None defaults to all 12 months."""
        cols: dict = {
            "ym_202412_reach": [True],
            "last_reach_date": [date(2024, 11, 30)],
        }
        for m in range(1, 13):
            cols[f"ym_2025{m:02d}_reach"] = [False]
        df = pl.DataFrame(cols)
        result = df.select(build_termed_bar_expr(2025, 2024))
        assert result["termed_bar_2024"][0] is True

    @pytest.mark.unit
    def test_get_bar_terminations(self):
        bar_df = pl.LazyFrame({'bene_mbi': ['MBI1', 'MBI2', 'MBI3'], 'bene_date_of_term': [date(2024, 6, 15), date(2024, 12, 1), None]})
        result = get_bar_terminations(bar_df, 2024).collect()
        assert result.height == 2
        assert 'MBI1' in result['current_mbi'].to_list()
