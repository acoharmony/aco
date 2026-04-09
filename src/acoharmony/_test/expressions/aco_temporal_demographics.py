from datetime import date

import polars as pl
import pytest


class TestAcoTemporalDemographics:
    """Tests for _aco_temporal_demographics expression builders."""

    @pytest.mark.unit
    def test_build_demographics_mbi_expr(self):
        df = pl.DataFrame({'current_bene_mbi_id': ['MBI123']})
        result = df.select(build_demographics_mbi_expr())
        assert result['current_mbi'][0] == 'MBI123'

    @pytest.mark.unit
    def test_build_demographics_select_expr(self):
        df = pl.DataFrame({'current_mbi': ['MBI1'], 'bene_dob': [date(1950, 5, 15)], 'bene_death_dt': [None], 'bene_sex_cd': ['M'], 'bene_race_cd': ['1'], 'bene_fips_state_cd': ['17'], 'bene_fips_cnty_cd': ['167'], 'bene_zip_cd': ['62701']})
        result = df.select(build_demographics_select_expr())
        assert result.columns == ['current_mbi', 'birth_date', 'death_date', 'sex', 'race', 'ethnicity', 'state', 'county', 'zip_code']
        assert result['birth_date'][0] == date(1950, 5, 15)
        assert result['ethnicity'][0] is None
