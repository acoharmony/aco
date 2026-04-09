from datetime import date

import polars as pl
import pytest


class TestAcoMetrics:
    """Tests for _aco_metrics expression builders."""

    @pytest.mark.unit
    def test_build_consolidated_program_expr(self):
        df = pl.DataFrame({'ever_reach': [True, True, False, False], 'ever_mssp': [True, False, True, False]})
        result = df.select(build_consolidated_program_expr())
        assert result['consolidated_program'].to_list() == ['BOTH', 'REACH', 'MSSP', 'NONE']

    @pytest.mark.unit
    def test_build_total_aligned_months_expr(self):
        df = pl.DataFrame({'months_in_reach': [10, 0], 'months_in_mssp': [5, 12]})
        result = df.select(build_total_aligned_months_expr())
        assert result['total_aligned_months'].to_list() == [15, 12]

    @pytest.mark.unit
    def test_build_primary_alignment_source_expr(self):
        df = pl.DataFrame({'has_valid_voluntary_alignment': [True, False, False], 'ever_reach': [True, True, False], 'ever_mssp': [False, False, False]})
        result = df.select(build_primary_alignment_source_expr())
        assert result['primary_alignment_source'].to_list() == ['VOLUNTARY', 'CLAIMS', 'NONE']

    @pytest.mark.unit
    def test_build_is_currently_aligned_expr(self):
        df = pl.DataFrame({'current_program': ['REACH', 'None', 'None'], 'has_valid_voluntary_alignment': [False, True, False]})
        result = df.select(build_is_currently_aligned_expr())
        assert result['is_currently_aligned'].to_list() == [True, True, False]

    @pytest.mark.unit
    def test_build_has_voluntary_alignment_filled_expr(self):
        df = pl.DataFrame({'has_voluntary_alignment': [True, None, False]})
        result = df.select(build_has_voluntary_alignment_filled_expr())
        assert result['has_voluntary_alignment'].to_list() == [True, False, False]

    @pytest.mark.unit
    def test_build_has_valid_historical_sva_expr(self):
        df = pl.DataFrame({'ever_reach': [True, False, True], 'has_voluntary_alignment': [True, True, False]})
        result = df.select(build_has_valid_historical_sva_expr())
        assert result['has_valid_historical_sva'].to_list() == [True, False, False]

    @pytest.mark.unit
    def test_build_has_program_transition_expr(self):
        df = pl.DataFrame({'ever_reach': [True, False], 'ever_mssp': [True, True]})
        result = df.select(build_has_program_transition_expr())
        assert result['has_program_transition'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_has_continuous_enrollment_expr(self):
        df = pl.DataFrame({'enrollment_gaps': [0, 3]})
        result = df.select(build_has_continuous_enrollment_expr())
        assert result['has_continuous_enrollment'].to_list() == [True, False]

    @pytest.mark.unit
    def test_build_bene_death_date_expr(self):
        df = pl.DataFrame({'death_date': [date(2024, 6, 15)]})
        result = df.select(build_bene_death_date_expr())
        assert result['bene_death_date'][0] == date(2024, 6, 15)

    @pytest.mark.unit
    def test_build_crosswalk_mapping_exprs(self):
        df = pl.DataFrame({'current_mbi': ['MBI1', 'MBI2'], 'bene_mbi': ['OLD_MBI1', 'MBI2']})
        result = df.select(build_crosswalk_mapping_exprs())
        assert result['mapping_type'].to_list() == ['xref', 'direct']
        assert result['prvs_num'][0] == 'OLD_MBI1'
        assert result['prvs_num'][1] is None

    @pytest.mark.unit
    def test_build_mssp_recruitment_exprs(self):
        df = pl.DataFrame({'current_program': ['MSSP', 'MSSP', 'MSSP', 'REACH'], 'has_valid_voluntary_alignment': [False, True, False, False], 'has_voluntary_alignment': [False, False, True, False]})
        result = df.select(build_mssp_recruitment_exprs())
        assert result['mssp_sva_recruitment_target'].to_list() == [True, False, True, False]
        assert result['mssp_to_reach_status'][0] == 'needs_initial_sva'
        assert result['mssp_to_reach_status'][1] == 'ready_for_reach'
        assert result['mssp_to_reach_status'][2] == 'needs_renewal'
        assert result['mssp_to_reach_status'][3] is None

    @pytest.mark.unit
    def test_build_provider_validation_exprs(self):
        df = pl.DataFrame({'voluntary_provider_tin': ['TIN1', None, 'TIN3'], 'aligned_provider_tin': ['TIN1', 'TIN2', None], 'voluntary_provider_npi': ['NPI1', None, 'NPI3'], 'aligned_provider_npi': ['NPI1', 'NPI2', None]})
        result = df.select(build_provider_validation_exprs())
        assert result['sva_tin_match'][0] is True
        assert result['sva_tin_match'][1] is None
        assert result['sva_npi_match'][0] is True

    @pytest.mark.unit
    def test_build_pbvar_integration_exprs(self):
        df = pl.DataFrame({'last_sva_submission_date': [date(2024, 6, 1), None, date(2024, 1, 1)], 'pbvar_report_date': [date(2024, 3, 1), date(2024, 3, 1), date(2024, 6, 1)], 'latest_response_codes': ['A0', 'A0', None]})
        result = df.select(build_pbvar_integration_exprs())
        assert result['sva_submitted_after_pbvar'][0] is True
        assert result['sva_submitted_after_pbvar'][1] is None
        assert result['needs_sva_refresh_from_pbvar'][2] is False

    @pytest.mark.unit
    def test_build_previous_program_expr(self):
        df = pl.DataFrame({'has_program_transition': [True, True, False], 'current_program': ['REACH', 'MSSP', 'REACH']})
        result = df.select(build_previous_program_expr())
        assert result['previous_program'].to_list() == ['MSSP', 'REACH', None]

    @pytest.mark.unit
    def test_build_program_transitions_expr(self):
        df = pl.DataFrame({'program_switches': [3, None]})
        result = df.select(build_program_transitions_expr())
        assert result['program_transitions'].to_list() == [3, 0]

    @pytest.mark.unit
    def test_build_program_transitions_expr_custom_fallback(self):
        df = pl.DataFrame({'program_switches': [None]})
        result = df.select(build_program_transitions_expr(fallback_value=5))
        assert result['program_transitions'][0] == 5
