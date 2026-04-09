from datetime import date

import polars as pl
import pytest


class TestAcoTemporalAlr:
    """Tests for _aco_temporal_alr expression builders."""

    @pytest.mark.unit
    def test_build_alr_mbi_crosswalk_expr(self):
        mbi_map = {'OLD': 'NEW'}
        df = pl.DataFrame({'bene_mbi': ['OLD', 'SAME']})
        result = df.select(build_alr_mbi_crosswalk_expr(mbi_map))
        assert result['current_mbi'].to_list() == ['NEW', 'SAME']

    @pytest.mark.unit
    def test_build_alr_program_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_alr_program_expr())
        assert result['program'][0] == 'MSSP'

    @pytest.mark.unit
    def test_build_alr_file_date_expr(self):
        df = pl.DataFrame({'file_date': ['2024-06-15']})
        result = df.select(build_alr_file_date_expr())
        assert result['file_date_parsed'][0] == date(2024, 6, 15)

    @pytest.mark.unit
    def test_build_alr_preparation_exprs(self):
        exprs = build_alr_preparation_exprs({'OLD': 'NEW'})
        assert len(exprs) == 4

    @pytest.mark.unit
    def test_build_alr_select_expr(self):
        exprs = build_alr_select_expr()
        assert len(exprs) == 6
