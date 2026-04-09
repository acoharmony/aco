from datetime import date

import polars as pl
import pytest


class TestVoluntaryAlignmentConsolidated:
    """Tests for _voluntary_alignment_consolidated expression builders."""

    @pytest.mark.unit
    def test_build_days_in_funnel_expr(self):
        df = pl.DataFrame({'first_outreach_date': [date(2024, 1, 1)], 'last_outreach_date': [date(2024, 3, 1)]})
        result = df.select(build_days_in_funnel_expr())
        assert result['days_in_funnel'][0] == 60

    @pytest.mark.unit
    def test_build_alignment_journey_status_expr(self):
        df = pl.DataFrame({'pbvar_aligned': [True, False, False, False, False], 'sva_signature_count': [0, 1, 0, 0, 0], 'emails_opened': [0, 0, 1, 0, 0], 'emails_clicked': [0, 0, 0, 0, 0], 'total_touchpoints': [1, 1, 1, 1, 0]})
        result = df.select(build_alignment_journey_status_expr())
        assert result['alignment_journey_status'].to_list() == ['Aligned', 'Signed', 'Engaged', 'Contacted No Response', 'Never Contacted']

    @pytest.mark.unit
    def test_build_outreach_response_status_expr(self):
        df = pl.DataFrame({'email_complained': [True, False, False, False, False], 'email_unsubscribed': [False, True, False, False, False], 'emails_opened': [0, 0, 1, 0, 0], 'emails_clicked': [0, 0, 0, 0, 0], 'total_touchpoints': [1, 1, 1, 1, 0]})
        result = df.select(build_outreach_response_status_expr())
        assert result['outreach_response_status'].to_list() == ['Complained', 'Unsubscribed', 'Email Engaged', 'No Response', 'Never Contacted']

    @pytest.mark.unit
    def test_build_data_quality_exprs(self):
        exprs = build_data_quality_exprs()
        assert len(exprs) == 3
        df = pl.DataFrame({'x': [1]})
        result = df.select(exprs)
        assert result['invalid_email_after_death'][0] is False

    @pytest.mark.unit
    def test_build_ffs_status_exprs(self):
        exprs = build_ffs_status_exprs()
        assert len(exprs) == 5
        df = pl.DataFrame({'x': [1]})
        result = df.select(exprs)
        assert result['has_ffs_service'][0] is False
        assert result['ffs_first_date'][0] is None
