from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._aco_metadata import (
    build_data_completeness_expr,
    build_data_date_exprs,
    build_lineage_transform_expr,
    build_source_tables_expr,
)


class TestAcoMetadata:
    """Tests for _aco_metadata expression builders."""

    @pytest.mark.unit
    def test_build_data_completeness_expr(self):
        df = pl.DataFrame({'has_demographics': [True, False]})
        result = df.select(build_data_completeness_expr())
        assert result['data_completeness'].to_list() == ['COMPLETE', 'PARTIAL']

    @pytest.mark.unit
    def test_build_lineage_transform_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_lineage_transform_expr())
        assert result['lineage_transform'][0] == 'consolidated_alignment_v3'
        result2 = df.select(build_lineage_transform_expr('v4'))
        assert result2['lineage_transform'][0] == 'v4'

    @pytest.mark.unit
    def test_build_lineage_processed_at_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_lineage_processed_at_expr())
        assert result['lineage_processed_at'][0] is not None

    @pytest.mark.unit
    def test_build_data_date_exprs(self):
        df = pl.DataFrame({'observable_start': [date(2024, 1, 1)], 'observable_end': [date(2024, 12, 31)]})
        result = df.select(build_data_date_exprs())
        assert result.columns == ['data_start_date', 'data_end_date']
        assert result['data_start_date'][0] == date(2024, 1, 1)

    @pytest.mark.unit
    def test_build_source_tables_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_source_tables_expr())
        assert 'aco_alignment' in result['source_tables'][0]
        result2 = df.select(build_source_tables_expr('table_a,table_b'))
        assert result2['source_tables'][0] == 'table_a,table_b'

    @pytest.mark.unit
    def test_build_last_updated_expr(self):
        df = pl.DataFrame({'x': [1]})
        result = df.select(build_last_updated_expr())
        assert result['last_updated'][0] is not None

    @pytest.mark.unit
    def test_build_has_opt_out_expr(self):
        df = pl.DataFrame({'has_email_opt_out': [True, False, False, True], 'has_mail_opt_out': [False, True, False, True]})
        result = df.select(build_has_opt_out_expr())
        assert result['has_opt_out'].to_list() == [True, True, False, True]

    @pytest.mark.unit
    def test_build_sva_action_needed_expr(self):
        df = pl.DataFrame({'has_voluntary_alignment': [True, False, False], 'has_valid_voluntary_alignment': [False, False, True], 'ever_mssp': [False, True, False]})
        result = df.select(build_sva_action_needed_expr())
        assert result['sva_action_needed'].to_list() == ['RENEWAL_NEEDED', 'SVA_ELIGIBLE', 'NO_ACTION']

    @pytest.mark.unit
    def test_build_outreach_priority_expr(self):
        df = pl.DataFrame({'sva_action_needed': ['RENEWAL_NEEDED', 'SVA_ELIGIBLE', 'NO_ACTION']})
        result = df.select(build_outreach_priority_expr())
        assert result['outreach_priority'].to_list() == ['HIGH', 'MEDIUM', 'LOW']
