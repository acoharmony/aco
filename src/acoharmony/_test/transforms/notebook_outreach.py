"""Tests for _transforms.notebook_outreach module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import polars as pl
import pytest
import acoharmony


class TestNotebookOutreach:
    """Tests for notebook outreach."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._notebook_outreach is not None

    @pytest.mark.unit
    def test_analyze_outreach_effectiveness_exists(self):
        assert callable(analyze_outreach_effectiveness)



class TestAnalyzeOutreachEffectiveness:
    """Tests for analyze_outreach_effectiveness."""

    def _make_outreach_df(self):
        return pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002", "MBI003", "MBI004"],
                "has_voluntary_outreach": [True, True, False, False],
                "voluntary_email_count": [2, 0, 0, 0],
                "voluntary_letter_count": [0, 1, 0, 0],
                "voluntary_emails_opened": [1, 0, 0, 0],
                "voluntary_emails_clicked": [1, 0, 0, 0],
                "has_valid_voluntary_alignment": [True, False, True, False],
            }
        ).lazy()

    @pytest.mark.unit
    def test_basic_outreach_metrics(self):
        """Calculate basic outreach effectiveness."""
        df = self._make_outreach_df()
        result = analyze_outreach_effectiveness(df)

        assert result["total_population"] == 4
        assert result["total_contacted"] == 2
        assert result["total_emailed"] == 1
        assert result["total_with_valid_sva"] == 2
        assert result["contacted_to_sva_rate"] == pytest.approx(50.0)
        assert result["current_metrics"] is None

    @pytest.mark.unit
    def test_with_selected_ym(self):
        """Calculate outreach metrics with current month filtering."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_voluntary_outreach": [True, False],
                "voluntary_email_count": [1, 0],
                "voluntary_letter_count": [0, 0],
                "voluntary_emails_opened": [1, 0],
                "voluntary_emails_clicked": [0, 0],
                "has_valid_voluntary_alignment": [True, False],
                "ym_202401_reach": [True, False],
                "ym_202401_mssp": [False, True],
            }
        ).lazy()

        result = analyze_outreach_effectiveness(df, selected_ym="202401")
        assert result["current_metrics"] is not None
        assert result["current_metrics"]["total"] == 2

    @pytest.mark.unit
    def test_with_most_recent_ym(self):
        """Calculate outreach with most_recent_ym for REACH context."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_voluntary_outreach": [True, True],
                "voluntary_email_count": [1, 1],
                "voluntary_letter_count": [0, 0],
                "voluntary_emails_opened": [0, 0],
                "voluntary_emails_clicked": [0, 0],
                "has_valid_voluntary_alignment": [True, True],
                "ym_202401_reach": [True, False],
            }
        ).lazy()

        result = analyze_outreach_effectiveness(df, most_recent_ym="202401")
        assert result["contacted_sva_in_reach"] == 1


class TestCalculateEnhancedCampaignPerformance:
    """Tests for calculate_enhanced_campaign_performance."""

    @pytest.mark.unit
    def test_basic_performance(self):
        """Calculate basic campaign performance metrics."""
        emails_df = pl.DataFrame(
            {
                "campaign": [
                    "ACO Voluntary Alignment Q1",
                    "ACO Voluntary Alignment Q1",
                    "Other Campaign",
                ],
                "mbi": ["MBI001", "MBI002", "MBI003"],
                "status": ["Delivered", "Bounced", "Delivered"],
                "has_been_opened": ["true", "false", "false"],
                "has_been_clicked": ["true", "false", "false"],
            }
        ).lazy()

        mailed_df = pl.DataFrame(
            {
                "campaign_name": [
                    "ACO Voluntary Alignment Q1",
                    "ACO Voluntary Alignment Q1",
                ],
                "mbi": ["MBI001", "MBI002"],
                "status": ["Delivered", "Failed"],
            }
        ).lazy()

        result = calculate_enhanced_campaign_performance(emails_df, mailed_df)
        assert "email" in result
        assert "mail" in result
        assert result["email"]["total_sent"] == 2
        assert result["mail"]["total_sent"] == 2

    @pytest.mark.unit
    def test_empty_campaigns(self):
        """Handle empty campaign data."""
        emails_df = pl.DataFrame(
            {
                "campaign": ["Other Campaign"],
                "mbi": ["MBI001"],
                "status": ["Delivered"],
                "has_been_opened": ["false"],
                "has_been_clicked": ["false"],
            }
        ).lazy()

        mailed_df = pl.DataFrame(
            {
                "campaign_name": ["Other Campaign"],
                "mbi": ["MBI001"],
                "status": ["Delivered"],
            }
        ).lazy()

        result = calculate_enhanced_campaign_performance(emails_df, mailed_df)
        assert result["email"]["total_sent"] == 0
        assert result["mail"]["total_sent"] == 0


class TestCalculateQuarterlyCampaignEffectiveness:
    """Tests for calculate_quarterly_campaign_effectiveness."""

    @pytest.mark.unit
    def test_basic_quarterly_metrics(self):
        """Calculate quarterly campaign effectiveness."""
        df_enriched = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_valid_voluntary_alignment": [True, False],
            }
        ).lazy()

        email_by_campaign = pl.DataFrame(
            {
                "campaign_period": ["Q1", "Q1"],
                "mbi": ["MBI001", "MBI002"],
                "emails_sent": [2, 1],
                "opened": [True, False],
                "clicked": [True, False],
            }
        ).lazy()

        mailed_by_campaign = pl.DataFrame(
            {
                "campaign_period": ["Q1"],
                "mbi": ["MBI001"],
                "letters_sent": [1],
            }
        ).lazy()

        result = calculate_quarterly_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )
        assert isinstance(result, pl.DataFrame)
        assert "campaign_period" in result.columns


class TestCalculateOfficeCampaignEffectiveness:
    """Tests for calculate_office_campaign_effectiveness."""

    @pytest.mark.unit
    def test_no_office_column_returns_empty(self):
        """Returns empty DataFrame when office_name column missing."""
        df_enriched = pl.DataFrame(
            {
                "current_mbi": ["MBI001"],
                "has_valid_voluntary_alignment": [True],
            }
        ).lazy()

        email_by_campaign = pl.DataFrame(
            {"campaign_period": ["Q1"], "mbi": ["MBI001"], "emails_sent": [1], "opened": [True], "clicked": [True]}
        ).lazy()

        mailed_by_campaign = pl.DataFrame(
            {"campaign_period": ["Q1"], "mbi": ["MBI001"], "letters_sent": [1]}
        ).lazy()

        result = calculate_office_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )
        assert isinstance(result, pl.DataFrame)
        assert result.height == 0

    @pytest.mark.unit
    def test_with_office_column(self):
        """Calculate office-level campaign effectiveness."""
        df_enriched = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_valid_voluntary_alignment": [True, False],
                "office_name": ["Office A", "Office A"],
            }
        ).lazy()

        email_by_campaign = pl.DataFrame(
            {
                "campaign_period": ["Q1", "Q1"],
                "mbi": ["MBI001", "MBI002"],
                "emails_sent": [2, 1],
                "opened": [True, False],
                "clicked": [True, False],
            }
        ).lazy()

        mailed_by_campaign = pl.DataFrame(
            {
                "campaign_period": ["Q1"],
                "mbi": ["MBI001"],
                "letters_sent": [1],
            }
        ).lazy()

        result = calculate_office_campaign_effectiveness(
            df_enriched, email_by_campaign, mailed_by_campaign
        )
        assert isinstance(result, pl.DataFrame)
        assert "office_name" in result.columns


# ===================== Coverage gap: lines 213-216, 635-636, 645-646 =====================

class TestAnalyzeOutreachReachBranches:
    """Test analyze_outreach_effectiveness when only has_reach or has_mssp."""

    @pytest.mark.unit
    def test_only_has_mssp_column(self):
        """When only mssp column exists, use mssp as aligned_expr (line 216)."""


        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B", "C"],
            "has_valid_sva": [True, False, True],
            "ym_202401_mssp": [True, False, True],
            "voluntary_email_count": [1, 0, 2],
            "voluntary_emails_opened": [1, 0, 0],
            "voluntary_emails_clicked": [0, 0, 1],
        }).lazy()

        result = analyze_outreach_effectiveness(df_enriched, selected_ym="202401")
        assert result is not None
        assert "total_population" in result

    @pytest.mark.unit
    def test_only_has_reach_column(self):
        """When only reach column exists, use reach as aligned_expr (line 214)."""


        df_enriched = pl.DataFrame({
            "current_mbi": ["A", "B"],
            "has_valid_sva": [True, False],
            "ym_202401_reach": [True, False],
            "voluntary_email_count": [1, 0],
            "voluntary_emails_opened": [1, 0],
            "voluntary_emails_clicked": [0, 0],
        }).lazy()

        result = analyze_outreach_effectiveness(df_enriched, selected_ym="202401")
        assert result is not None


class TestEnhancedCampaignPerformanceEmpty:
    """Test empty email/mail stats path (lines 635-636, 645-646)."""

    @pytest.mark.unit
    def test_empty_email_and_mail_stats(self):
        """When email/mail stats are empty, default to zeros."""
        # Empty dataframes with explicit types to avoid null type issues
        email_df = pl.DataFrame(
            schema={
                "campaign": pl.Utf8,
                "mbi": pl.Utf8,
                "has_been_opened": pl.Utf8,
                "has_been_clicked": pl.Utf8,
                "send_datetime": pl.Utf8,
            }
        ).lazy()
        mail_df = pl.DataFrame(
            schema={
                "campaign_name": pl.Utf8,
                "mbi": pl.Utf8,
                "status": pl.Utf8,
                "send_datetime": pl.Utf8,
            }
        ).lazy()

        result = calculate_enhanced_campaign_performance(email_df, mail_df)
        assert result is not None
        assert result["email"]["total_sent"] == 0
        assert result["mail"]["total_sent"] == 0


# ---------------------------------------------------------------------------
# Coverage gap tests: _notebook_outreach.py lines 635-636, 645-646
# ---------------------------------------------------------------------------


class TestCampaignPerformanceEmptyStats:
    """Cover empty email/mail stats branches."""

    @pytest.mark.unit
    def test_empty_email_stats_defaults(self):
        """Lines 635-636: when voluntary_email_stats is empty, defaults to 0."""
        email_df = pl.DataFrame({
            "campaign_name": ["Other Campaign"],
            "mbi": ["MBI1"],
            "has_been_delivered": ["true"],
            "has_been_opened": ["false"],
            "has_been_clicked": ["false"],
        }).lazy()

        mail_df = pl.DataFrame({
            "campaign_name": ["Other Campaign"],
            "mbi": ["MBI1"],
            "status": ["Delivered"],
            "send_datetime": ["2024-01-01"],
        }).lazy()

        result = calculate_enhanced_campaign_performance(email_df, mail_df)
        assert result["email"]["total_sent"] == 0
        assert result["email"]["delivery_rate"] == 0.0
        assert result["mail"]["total_sent"] == 0
        assert result["mail"]["delivery_rate"] == 0.0


# ---------------------------------------------------------------------------
# New coverage-gap tests for uncovered branches
# ---------------------------------------------------------------------------


class TestReachColNotInSchema:
    """Cover branch 136->150: most_recent_ym set but reach column missing."""

    @pytest.mark.unit
    def test_most_recent_ym_without_reach_col(self):
        """When most_recent_ym is given but ym_X_reach column is absent, skip REACH lookup."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_voluntary_outreach": [True, False],
                "voluntary_email_count": [1, 0],
                "voluntary_letter_count": [0, 0],
                "voluntary_emails_opened": [1, 0],
                "voluntary_emails_clicked": [0, 0],
                "has_valid_voluntary_alignment": [True, False],
                # No ym_202401_reach column!
            }
        ).lazy()

        result = analyze_outreach_effectiveness(df, most_recent_ym="202401")
        # Should fall through to outreach_metrics with contacted_sva_in_reach = 0
        assert result["contacted_sva_in_reach"] == 0
        assert result["current_metrics"] is None


class TestContactedSvaToReachEmpty:
    """Cover branch 147->150: reach col exists but contacted_sva_to_reach is empty."""

    @pytest.mark.unit
    def test_contacted_sva_to_reach_empty_result(self):
        """When contacted_sva_to_reach collected DF has 0 rows, skip extraction."""
        from unittest.mock import patch

        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_voluntary_outreach": [True, True],
                "voluntary_email_count": [1, 1],
                "voluntary_letter_count": [0, 0],
                "voluntary_emails_opened": [0, 0],
                "voluntary_emails_clicked": [0, 0],
                "has_valid_voluntary_alignment": [True, True],
                "ym_202401_reach": [True, False],
            }
        ).lazy()

        # The 5th .collect() call is the contacted_sva_to_reach one (after
        # overall_stats, contacted_to_sva, email_opened_to_sva,
        # email_clicked_to_sva, not_contacted_sva = 5 calls before it, so 6th).
        # Actually let's count: overall_stats(1), contacted_to_sva(2),
        # email_opened_to_sva(3), email_clicked_to_sva(4), not_contacted_sva(5),
        # contacted_sva_to_reach(6).
        empty_reach_df = pl.DataFrame(
            schema={
                "contacted_sva_count": pl.UInt32,
                "contacted_sva_in_reach": pl.UInt32,
            }
        )

        collect_call_count = [0]
        original_collect = pl.LazyFrame.collect

        def mock_collect(self, *args, **kwargs):
            collect_call_count[0] += 1
            if collect_call_count[0] == 6:
                return empty_reach_df
            return original_collect(self, *args, **kwargs)

        with patch.object(pl.LazyFrame, 'collect', mock_collect):
            result = analyze_outreach_effectiveness(df, most_recent_ym="202401")

        # contacted_sva_in_reach should remain 0 because len(empty_reach_df) == 0
        assert result["contacted_sva_in_reach"] == 0


class TestSelectedYmNeitherReachNorMssp:
    """Cover branch 207->332: selected_ym set but neither reach nor mssp column exists."""

    @pytest.mark.unit
    def test_selected_ym_no_alignment_columns(self):
        """When selected_ym is given but no reach/mssp columns, current_metrics stays None."""
        df = pl.DataFrame(
            {
                "current_mbi": ["MBI001", "MBI002"],
                "has_voluntary_outreach": [True, False],
                "voluntary_email_count": [1, 0],
                "voluntary_letter_count": [0, 0],
                "voluntary_emails_opened": [0, 0],
                "voluntary_emails_clicked": [0, 0],
                "has_valid_voluntary_alignment": [True, False],
                # No ym_202401_reach or ym_202401_mssp columns
            }
        ).lazy()

        result = analyze_outreach_effectiveness(df, selected_ym="202401")
        assert result["current_metrics"] is None


class TestEmptyEmailMailStatsHeight:
    """Cover branches 624->635 and 639->645: .height == 0 on collected stats."""

    @pytest.mark.unit
    def test_email_stats_height_zero(self):
        """Line 624->635: voluntary_email_stats.height == 0 branch."""
        from unittest.mock import patch, MagicMock

        # Real mail DF so it takes the normal path for mail
        emails_df = pl.DataFrame(
            {
                "campaign": ["ACO Voluntary Alignment Q1"],
                "mbi": ["MBI001"],
                "status": ["Delivered"],
                "has_been_opened": ["true"],
                "has_been_clicked": ["false"],
            }
        ).lazy()
        mailed_df = pl.DataFrame(
            {
                "campaign_name": ["ACO Voluntary Alignment Q1"],
                "mbi": ["MBI001"],
                "status": ["Delivered"],
            }
        ).lazy()

        # We need to intercept only the first .collect() for email stats
        # Use a side_effect to return empty DF first time, then real data
        original_fn = calculate_enhanced_campaign_performance.__wrapped__ if hasattr(calculate_enhanced_campaign_performance, '__wrapped__') else calculate_enhanced_campaign_performance

        # Instead of mocking, construct a DF whose collect produces height 0 by
        # relying on a group_by that yields no groups on empty input.
        # Actually, simplest: just mock the result directly.

        empty_email_stats = pl.DataFrame(
            schema={
                "total_emails_sent": pl.UInt32,
                "emails_delivered": pl.UInt32,
                "emails_opened": pl.UInt32,
                "emails_clicked": pl.UInt32,
                "unique_recipients": pl.UInt32,
            }
        )
        empty_mail_stats = pl.DataFrame(
            schema={
                "total_letters_sent": pl.UInt32,
                "letters_delivered": pl.UInt32,
                "unique_recipients": pl.UInt32,
            }
        )

        assert empty_email_stats.height == 0
        assert empty_mail_stats.height == 0

        # Patch the function at module level to inject empty stats
        import acoharmony._transforms._notebook_outreach as mod

        orig_func = mod.calculate_enhanced_campaign_performance

        def patched(emails_df, mailed_df):
            """Call original but replace collected stats with empty ones."""
            # We need to run the actual code path, so we monkey-patch
            # LazyFrame.collect to return empty on the right calls.
            collect_call_count = [0]
            original_collect = pl.LazyFrame.collect

            def mock_collect(self, *args, **kwargs):
                collect_call_count[0] += 1
                result = original_collect(self, *args, **kwargs)
                # The 1st and 2nd collect calls are for email_stats and mail_stats
                if collect_call_count[0] == 1:
                    return empty_email_stats
                elif collect_call_count[0] == 2:
                    return empty_mail_stats
                return result

            with patch.object(pl.LazyFrame, 'collect', mock_collect):
                return orig_func(emails_df, mailed_df)

        result = patched(emails_df, mailed_df)

        assert result["email"]["total_sent"] == 0
        assert result["email"]["delivery_rate"] == 0.0
        assert result["email"]["open_rate"] == 0.0
        assert result["email"]["click_rate"] == 0.0
        assert result["mail"]["total_sent"] == 0
        assert result["mail"]["delivery_rate"] == 0.0
