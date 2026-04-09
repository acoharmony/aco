from datetime import date

import polars as pl
import pytest


class TestAcoTransitionMovedMa:
    """Tests for _aco_transition_moved_ma expression builders."""

    @pytest.mark.unit
    def test_build_moved_ma_expr(self):
        df = pl.DataFrame({'ym_202412_reach': [True, True, True], 'ym_202501_reach': [False, False, False], 'latest_response_codes': ['E2', 'A0', 'A0'], 'eligibility_issues': ['', 'E2', ''], 'latest_response_detail': ['', '', 'Medicare Advantage plan']})
        result = df.select(build_moved_ma_expr(2025, 2024, current_year_months=[1]))
        assert result['moved_ma_2024'][0] is True
        assert result['moved_ma_2024'][1] is True
        assert result['moved_ma_2024'][2] is True

    @pytest.mark.unit
    def test_build_ma_enrollment_date_expr(self):
        df = pl.DataFrame({'latest_response_codes': ['E2', 'A0'], 'pbvar_report_date': [date(2025, 1, 15), date(2025, 1, 15)]})
        result = df.select(build_ma_enrollment_date_expr())
        assert result['ma_enrollment_date'][0] == date(2025, 1, 15)
        assert result['ma_enrollment_date'][1] is None

    @pytest.mark.unit
    def test_ma_response_codes_constant(self):
        assert 'E2' in MA_RESPONSE_CODES
import pytest

from acoharmony._expressions._aco_transition_moved_ma import build_moved_ma_expr


class TestAcoTransitionMovedMaGaps:
    """Cover default current_year_months and function returns."""

    @pytest.mark.unit
    def test_build_moved_ma_expr_default_months(self):
        """Branch 49->50: current_year_months=None defaults to all 12 months."""
        cols = {f'ym_202{5}{m:02d}_reach': [False] for m in range(1, 13)}
        cols['ym_202412_reach'] = [True]
        cols['latest_response_codes'] = ['E2']
        cols['eligibility_issues'] = ['']
        cols['latest_response_detail'] = ['']
        df = pl.DataFrame(cols)
        result = df.select(build_moved_ma_expr(2025, 2024, current_year_months=None))
        assert result['moved_ma_2024'][0] is True

    @pytest.mark.unit
    def test_get_ma_enrollments_returns_lazyframe(self):
        """Lines 130, 151: function returns LazyFrame."""
        pbvar_df = pl.LazyFrame({'current_mbi': ['MBI001'], 'response_code': ['05'], 'response_code_description': ['Medicare Advantage'], 'file_date': ['2025-01-01']})
        result = get_ma_enrollments(pbvar_df, previous_year=2024, current_year=2025)
        assert isinstance(result, pl.LazyFrame)
