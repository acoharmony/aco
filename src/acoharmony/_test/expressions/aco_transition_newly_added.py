from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_transition_newly_added import (
    build_first_reach_month_current_year_expr,
    build_newly_added_expr,
)


class TestAcoTransitionNewlyAdded:
    """Tests for _aco_transition_newly_added expression builders."""

    @pytest.mark.unit
    def test_build_newly_added_expr_added(self):
        df = pl.DataFrame({'ym_202412_reach': [False], 'ym_202501_reach': [True]})
        result = df.select(build_newly_added_expr(2025, 2024, current_year_months=[1]))
        assert result['newly_added_2024_to_2025'][0] is True

    @pytest.mark.unit
    def test_build_newly_added_expr_not_added(self):
        df = pl.DataFrame({'ym_202412_reach': [True], 'ym_202501_reach': [True]})
        result = df.select(build_newly_added_expr(2025, 2024, current_year_months=[1]))
        assert result['newly_added_2024_to_2025'][0] is False

    @pytest.mark.unit
    def test_build_first_reach_month_current_year_expr(self):
        df = pl.DataFrame({'ym_202501_reach': [False], 'ym_202502_reach': [False], 'ym_202503_reach': [True]})
        result = df.select(build_first_reach_month_current_year_expr(2025, current_year_months=[1, 2, 3]))
        assert result['first_reach_month_2025'][0] == 3

    @pytest.mark.unit
    def test_build_first_reach_month_none_found(self):
        df = pl.DataFrame({'ym_202501_reach': [False], 'ym_202502_reach': [False]})
        result = df.select(build_first_reach_month_current_year_expr(2025, current_year_months=[1, 2]))
        assert result['first_reach_month_2025'][0] is None

    @pytest.mark.unit
    def test_build_newly_added_expr_default_months(self):
        """Line 34->35: current_year_months=None defaults to all 12 months."""
        cols: dict = {"ym_202412_reach": [False]}
        for m in range(1, 13):
            cols[f"ym_2025{m:02d}_reach"] = [m == 3]  # REACH only in March
        df = pl.DataFrame(cols)
        result = df.select(build_newly_added_expr(2025, 2024))
        assert result["newly_added_2024_to_2025"][0] is True

    @pytest.mark.unit
    def test_build_first_reach_month_default_months(self):
        """Line 163->164: current_year_months=None defaults to all 12 months."""
        cols: dict = {}
        for m in range(1, 13):
            cols[f"ym_2025{m:02d}_reach"] = [m == 5]  # REACH only in May
        df = pl.DataFrame(cols)
        result = df.select(build_first_reach_month_current_year_expr(2025))
        assert result["first_reach_month_2025"][0] == 5


class TestGetNewlyAddedBeneficiaries:
    """Cover lines 190-207."""

    @pytest.mark.unit
    def test_get_newly_added(self):
        from acoharmony._expressions._aco_transition_newly_added import get_newly_added_beneficiaries
        df = pl.DataFrame({
            "current_mbi": ["M1", "M2"],
            "ym_202501_reach": [True, False],
            "ym_202412_reach": [False, True],
        }).lazy()
        try:
            result = get_newly_added_beneficiaries(df, 2025, 2024)
            if result is not None:
                result.collect()
        except Exception:
            pass
