from datetime import date

import polars as pl
import pytest


class TestAcoTemporalSummary:
    """Tests for _aco_temporal_summary expression builders."""

    @pytest.mark.unit
    def test_build_has_ffs_service_expr(self):
        df = pl.DataFrame({'ever_ffs': [True, False]})
        result = df.select(build_has_ffs_service_expr())
        assert result['has_ffs_service'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_ffs_claim_count_proxy_expr(self):
        df = pl.DataFrame({'months_in_ffs': [5, 12]})
        result = df.select(build_ffs_claim_count_proxy_expr())
        assert result['ffs_claim_count'].to_list() == [5, 12]

    @pytest.mark.unit
    def test_build_has_demographics_expr(self):
        df = pl.DataFrame({'birth_date': [date(1950, 1, 1), None]})
        result = df.select(build_has_demographics_expr())
        assert result['has_demographics'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_mbi_stability_expr(self):
        df = pl.DataFrame({'previous_mbi_count': [0, 1, 3]})
        result = df.select(build_mbi_stability_expr())
        assert result['mbi_stability'].to_list() == ['Stable', 'Changed', 'Multiple']

    @pytest.mark.unit
    def test_build_current_provider_tin_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_current_provider_tin_expr())
        assert result['current_provider_tin'][0] is None
        assert result['current_provider_tin'].dtype == pl.String

    @pytest.mark.unit
    def test_build_ffs_first_date_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_ffs_first_date_expr())
        assert result['ffs_first_date'][0] is None
        assert result['ffs_first_date'].dtype == pl.Date

    @pytest.mark.unit
    def test_build_summary_statistics_exprs(self):
        exprs = build_summary_statistics_exprs()
        assert len(exprs) == 6
        df = pl.DataFrame({'ever_ffs': [True], 'months_in_ffs': [6], 'birth_date': [date(1950, 1, 1)], 'previous_mbi_count': [0]})
        result = df.select(exprs)
        assert result['has_ffs_service'][0] is True
        assert result['mbi_stability'][0] == 'Stable'
