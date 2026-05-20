from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_transition_total_lost import build_total_lost_expr


class TestAcoTransitionTotalLost:
    """Tests for _aco_transition_total_lost expression builders."""

    @pytest.mark.unit
    def test_build_total_lost_expr_lost(self):
        df = pl.DataFrame({'ym_202412_reach': [True], 'ym_202501_reach': [False], 'ym_202502_reach': [False], 'ym_202503_reach': [False]})
        result = df.select(build_total_lost_expr(2025, 2024, current_year_months=[1, 2, 3]))
        assert result['lost_2024_to_2025'][0] is True

    @pytest.mark.unit
    def test_build_total_lost_expr_retained(self):
        df = pl.DataFrame({'ym_202412_reach': [True], 'ym_202501_reach': [True], 'ym_202502_reach': [False], 'ym_202503_reach': [False]})
        result = df.select(build_total_lost_expr(2025, 2024, current_year_months=[1, 2, 3]))
        assert result['lost_2024_to_2025'][0] is False

    @pytest.mark.unit
    def test_build_total_lost_expr_not_prev(self):
        df = pl.DataFrame({'ym_202412_reach': [False], 'ym_202501_reach': [False]})
        result = df.select(build_total_lost_expr(2025, 2024, current_year_months=[1]))
        assert result['lost_2024_to_2025'][0] is False

    @pytest.mark.unit
    def test_build_total_lost_expr_default_months(self):
        """Branch 36->37: current_year_months=None defaults to all 12 months."""
        cols = {f'ym_202{5}{m:02d}_reach': [False] for m in range(1, 13)}
        cols['ym_202412_reach'] = [True]
        df = pl.DataFrame(cols)
        result = df.select(build_total_lost_expr(2025, 2024, current_year_months=None))
        assert result['lost_2024_to_2025'][0] is True
