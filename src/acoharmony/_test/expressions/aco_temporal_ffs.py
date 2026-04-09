from datetime import date

import polars as pl
import pytest


class TestAcoTemporalFfs:
    """Tests for _aco_temporal_ffs expression builders."""

    @pytest.mark.unit
    def test_build_ffs_mbi_crosswalk_expr(self):
        mbi_map = {'OLD_MBI': 'NEW_MBI'}
        df = pl.DataFrame({'bene_mbi': ['OLD_MBI', 'KEEP_MBI']})
        result = df.select(build_ffs_mbi_crosswalk_expr(mbi_map))
        assert result['current_mbi'].to_list() == ['NEW_MBI', 'KEEP_MBI']

    @pytest.mark.unit
    def test_build_ffs_select_expr(self):
        df = pl.DataFrame({'current_mbi': ['MBI1'], 'ffs_first_date': [date(2024, 1, 15)], 'claim_count': [5]})
        result = df.select(build_ffs_select_expr())
        assert result['has_ffs_service'][0] is True
        assert result['ffs_claim_count'][0] == 5
