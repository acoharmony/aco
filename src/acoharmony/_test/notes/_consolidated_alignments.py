# © 2025 HarmonyCares
"""Tests for acoharmony._notes._consolidated_alignments."""

from __future__ import annotations

from datetime import date
from pathlib import Path
from unittest.mock import patch

import polars as pl
import pytest

from acoharmony._notes import ConsolidatedAlignmentsPlugins


def _consolidated_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "current_mbi": ["M1", "M2", "M3"],
            "consolidated_program": ["REACH", "MSSP", "FFS"],
            "death_date": [None, None, None],
            "ym_202401_reach": [True, False, False],
            "ym_202401_mssp": [False, True, False],
            "ym_202401_ffs": [False, False, True],
            "ym_202402_reach": [True, False, False],
            "ym_202402_mssp": [False, True, False],
            "ym_202402_ffs": [False, False, True],
            "has_voluntary_alignment": [True, False, False],
            "months_in_reach": [12, 0, 0],
            "months_in_mssp": [0, 6, 0],
            "office_location": ["Detroit", "Detroit", "Lansing"],
            "has_valid_voluntary_alignment": [True, False, False],
            "has_voluntary_outreach": [True, True, False],
            "voluntary_email_count": [1, 1, 0],
            "voluntary_letter_count": [0, 1, 0],
        }
    )


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


class TestLoaders:
    @pytest.mark.unit
    def test_load_consolidated_missing(self, tmp_path: Path) -> None:
        out = ConsolidatedAlignmentsPlugins().load_consolidated(tmp_path)
        assert out.collect().is_empty()

    @pytest.mark.unit
    def test_load_consolidated(self, tmp_path: Path) -> None:
        _consolidated_df().write_parquet(tmp_path / "consolidated_alignment.parquet")
        out = ConsolidatedAlignmentsPlugins().load_consolidated(tmp_path)
        assert out.collect().height == 3

    @pytest.mark.unit
    def test_load_emails_missing(self, tmp_path: Path) -> None:
        assert ConsolidatedAlignmentsPlugins().load_emails(tmp_path).collect().is_empty()

    @pytest.mark.unit
    def test_load_mailed_missing(self, tmp_path: Path) -> None:
        assert ConsolidatedAlignmentsPlugins().load_mailed(tmp_path).collect().is_empty()


# ---------------------------------------------------------------------------
# Filters / utilities
# ---------------------------------------------------------------------------


class TestLivingFilter:
    @pytest.mark.unit
    def test_no_death_cols(self) -> None:
        df = pl.LazyFrame({"x": [1]})
        # No death cols → expression is lit(True)
        out = ConsolidatedAlignmentsPlugins().living_filter(df)
        # Use the expression on the frame
        assert df.filter(out).collect().height == 1

    @pytest.mark.unit
    def test_with_death_date(self) -> None:
        df = pl.LazyFrame({"x": [1, 2], "death_date": [None, date(2024, 1, 1)]})
        out = ConsolidatedAlignmentsPlugins().living_filter(df)
        assert df.filter(out).collect().height == 1

    @pytest.mark.unit
    def test_invalid_lazy(self) -> None:
        # Empty LazyFrame still works
        df = pl.LazyFrame()
        out = ConsolidatedAlignmentsPlugins().living_filter(df)
        assert df.filter(out).collect().is_empty()

    @pytest.mark.unit
    def test_none_input(self) -> None:
        # `df is None` early-exits to lit(True) without touching schema
        out = ConsolidatedAlignmentsPlugins().living_filter(None)
        assert pl.LazyFrame({"x": [1]}).filter(out).collect().height == 1

    @pytest.mark.unit
    def test_collect_schema_raises(self) -> None:
        from unittest.mock import MagicMock

        bad = MagicMock()
        bad.collect_schema.side_effect = RuntimeError("boom")
        out = ConsolidatedAlignmentsPlugins().living_filter(bad)
        # Falls back to lit(True) when schema introspection fails
        assert pl.LazyFrame({"x": [1]}).filter(out).collect().height == 1

    @pytest.mark.unit
    def test_with_bene_death_date(self) -> None:
        # Cover line 57 — `bene_death_date` column branch
        df = pl.LazyFrame(
            {
                "x": [1, 2],
                "bene_death_date": [None, date(2024, 1, 1)],
            }
        )
        out = ConsolidatedAlignmentsPlugins().living_filter(df)
        assert df.filter(out).collect().height == 1


class TestExtractYearMonths:
    @pytest.mark.unit
    def test_extracts(self) -> None:
        df = _consolidated_df().lazy()
        most_recent, ym = ConsolidatedAlignmentsPlugins().extract_year_months(df)
        assert most_recent == "202402"
        assert ym == ["202401", "202402"]

    @pytest.mark.unit
    def test_no_ym_cols(self) -> None:
        df = pl.LazyFrame({"x": [1]})
        out = ConsolidatedAlignmentsPlugins().extract_year_months(df)
        assert out == (None, [])


class TestBasicStats:
    @pytest.mark.unit
    def test_basic(self) -> None:
        out = ConsolidatedAlignmentsPlugins().basic_stats(_consolidated_df().lazy())
        assert out["total_records"] == 3


# ---------------------------------------------------------------------------
# Distributions
# ---------------------------------------------------------------------------


class TestDistributions:
    @pytest.mark.unit
    def test_historical(self) -> None:
        out = ConsolidatedAlignmentsPlugins().historical_program_distribution(
            _consolidated_df().lazy()
        )
        assert out.height >= 1

    @pytest.mark.unit
    def test_current(self) -> None:
        out = ConsolidatedAlignmentsPlugins().current_program_distribution(
            _consolidated_df().lazy(), "202402"
        )
        assert out.height >= 1


# ---------------------------------------------------------------------------
# Selected month enrollment
# ---------------------------------------------------------------------------


class TestSelectedMonthEnrollment:
    @pytest.mark.unit
    def test_no_selected(self) -> None:
        assert ConsolidatedAlignmentsPlugins().selected_month_enrollment(
            _consolidated_df().lazy(), None
        ) is None

    @pytest.mark.unit
    def test_returns_counts(self) -> None:
        out = ConsolidatedAlignmentsPlugins().selected_month_enrollment(
            _consolidated_df().lazy(), "202401"
        )
        assert out["REACH"] == 1
        assert out["MSSP"] == 1
        assert out["FFS"] == 1
        assert out["Not Enrolled"] == 0

    @pytest.mark.unit
    def test_missing_columns(self) -> None:
        # No ym_999999_* columns → counts stay zero
        out = ConsolidatedAlignmentsPlugins().selected_month_enrollment(
            _consolidated_df().lazy(), "999999"
        )
        assert out["REACH"] == 0
        assert out["Not Enrolled"] == 3


# ---------------------------------------------------------------------------
# Sample preview
# ---------------------------------------------------------------------------


class TestSample:
    @pytest.mark.unit
    def test_sample_includes_current_mbi(self) -> None:
        out = ConsolidatedAlignmentsPlugins().sample(
            _consolidated_df().lazy(), sample_size=2
        )
        assert "current_mbi" in out.columns
        assert out.height == 2

    @pytest.mark.unit
    def test_no_known_columns(self) -> None:
        df = pl.LazyFrame({"x": [1, 2, 3]})
        out = ConsolidatedAlignmentsPlugins().sample(df, sample_size=2)
        assert out.height == 2


# ---------------------------------------------------------------------------
# SVA action categories
# ---------------------------------------------------------------------------


class TestSvaActionCategories:
    @pytest.mark.unit
    def test_groups(self) -> None:
        df = pl.LazyFrame(
            {
                "sva_action_needed": ["renew", "renew", "new"],
            }
        )
        out = ConsolidatedAlignmentsPlugins().sva_action_categories(df)
        as_dict = {row["sva_action_needed"]: row["count"] for row in out.iter_rows(named=True)}
        assert as_dict["renew"] == 2


# ---------------------------------------------------------------------------
# Delegation tests (stubs)
# ---------------------------------------------------------------------------


class TestDelegationStubs:
    @pytest.mark.unit
    def test_alignment_trends_delegates(self) -> None:
        df = _consolidated_df().lazy()
        with patch(
            "acoharmony._transforms._notebook_trends.calculate_alignment_trends_over_time",
            return_value=pl.DataFrame({"year_month": ["202401"]}),
        ) as fn:
            out = ConsolidatedAlignmentsPlugins().alignment_trends(df, ["202401"])
        fn.assert_called_once()
        assert out.height == 1

    @pytest.mark.unit
    def test_transitions_delegates(self) -> None:
        df = _consolidated_df().lazy()
        with patch(
            "acoharmony._transforms._notebook_transitions.calculate_alignment_transitions",
            return_value=pl.DataFrame({"transition": ["A→B"]}),
        ) as fn:
            out = ConsolidatedAlignmentsPlugins().transitions(df, "202401", "202402")
        fn.assert_called_once()
        assert out.height == 1

    @pytest.mark.unit
    def test_office_enrollment_delegates(self) -> None:
        df = _consolidated_df().lazy()
        with patch(
            "acoharmony._transforms._notebook_office_stats.calculate_office_enrollment_stats",
            return_value=pl.DataFrame({"office_name": ["Detroit"]}),
        ) as fn:
            out = ConsolidatedAlignmentsPlugins().office_enrollment(df, "202401")
        fn.assert_called_once()
        assert out.height == 1

    @pytest.mark.unit
    def test_voluntary_outreach_delegates(self) -> None:
        empty = pl.LazyFrame()
        with patch(
            "acoharmony._transforms._notebook_utilities.prepare_voluntary_outreach_data",
            return_value=(empty, empty, empty, empty),
        ) as fn:
            out = ConsolidatedAlignmentsPlugins().voluntary_outreach_data(
                pl.LazyFrame(), pl.LazyFrame()
            )
        fn.assert_called_once()
        assert "email_mbis" in out
        assert "mailed_mbis" in out

    @pytest.mark.unit
    def test_quarterly_campaign_delegates(self) -> None:
        with patch(
            "acoharmony._transforms._notebook_outreach.calculate_quarterly_campaign_effectiveness",
            return_value=pl.DataFrame({"campaign_period": ["2024_Q1"]}),
        ) as fn:
            out = ConsolidatedAlignmentsPlugins().quarterly_campaign_effectiveness(
                pl.LazyFrame(), pl.LazyFrame(), pl.LazyFrame()
            )
        fn.assert_called_once()
        assert out.height == 1

    @pytest.mark.unit
    def test_office_campaign_delegates(self) -> None:
        with patch(
            "acoharmony._transforms._notebook_outreach.calculate_office_campaign_effectiveness",
            return_value=pl.DataFrame({"office_name": ["X"]}),
        ) as fn:
            ConsolidatedAlignmentsPlugins().office_campaign_effectiveness(
                pl.LazyFrame(), pl.LazyFrame(), pl.LazyFrame()
            )
        fn.assert_called_once()

    @pytest.mark.unit
    def test_office_alignment_types_delegates(self) -> None:
        with patch(
            "acoharmony._transforms._notebook_office_stats.calculate_office_alignment_types",
            return_value=pl.DataFrame({"office_name": ["X"]}),
        ) as fn:
            ConsolidatedAlignmentsPlugins().office_alignment_types(pl.LazyFrame(), "202401")
        fn.assert_called_once()

    @pytest.mark.unit
    def test_office_program_dist_delegates(self) -> None:
        with patch(
            "acoharmony._transforms._notebook_office_stats.calculate_office_program_distribution",
            return_value=pl.DataFrame({"office_name": ["X"]}),
        ) as fn:
            ConsolidatedAlignmentsPlugins().office_program_distribution(pl.LazyFrame(), "202401")
        fn.assert_called_once()

    @pytest.mark.unit
    def test_office_transitions_delegates(self) -> None:
        with patch(
            "acoharmony._transforms._notebook_office_stats.calculate_office_transition_stats",
            return_value=pl.DataFrame({"office_name": ["X"]}),
        ) as fn:
            ConsolidatedAlignmentsPlugins().office_transitions(pl.LazyFrame())
        fn.assert_called_once()

    @pytest.mark.unit
    def test_enhanced_campaign_delegates(self) -> None:
        with patch(
            "acoharmony._transforms._notebook_outreach.calculate_enhanced_campaign_performance",
            return_value={"emails": {}, "mail": {}},
        ) as fn:
            ConsolidatedAlignmentsPlugins().enhanced_campaign_performance(
                pl.LazyFrame(), pl.LazyFrame()
            )
        fn.assert_called_once()
