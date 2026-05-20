"""Tests for _transforms.aco_alignment_transitions module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock  # noqa: E402

import polars as pl  # noqa: E402
import pytest
import acoharmony


class TestAcoAlignmentTransitions:
    """Tests for ACO alignment transitions transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._aco_alignment_transitions is not None

    @pytest.mark.unit
    def test_apply_transform_exists(self):
        assert callable(apply_transform)





def _make_logger():
    return MagicMock()


def _make_temporal_df(
    current_year: int = 2025,
    previous_year: int = 2024,
    include_months: list[int] | None = None,
):
    """Build a LazyFrame with temporal columns for two years.

    Creates ym_YYYYMM_reach columns for specified months.
    """
    if include_months is None:
        include_months = list(range(1, 13))

    data = {
        "current_mbi": ["MBI001", "MBI002"],
        "current_program": ["REACH", "MSSP"],
        "current_aco_id": ["ACO1", "ACO2"],
    }

    # Add temporal columns for previous year (all 12 months)
    for m in range(1, 13):
        data[f"ym_{previous_year}{m:02d}_reach"] = [True, True]
        data[f"ym_{previous_year}{m:02d}_mssp"] = [False, True]
        data[f"ym_{previous_year}{m:02d}_first_claim"] = [False, False]

    # Add temporal columns for current year
    for m in include_months:
        # MBI001 is still aligned, MBI002 lost in current year
        data[f"ym_{current_year}{m:02d}_reach"] = [True, False]
        data[f"ym_{current_year}{m:02d}_mssp"] = [False, False]
        data[f"ym_{current_year}{m:02d}_first_claim"] = [False, False]

    # Add columns needed by various transition expressions
    data["bar_termed_date"] = [None, None]
    data["sva_expiration_date"] = [None, None]
    data["last_provider_activity_date"] = [None, None]
    data["ma_enrollment_date"] = [None, None]
    data["last_activity_date"] = [None, None]
    data["last_reach_date"] = [None, None]
    data["last_mssp_date"] = [None, None]
    data["ffs_first_date"] = [None, None]
    data["death_date"] = [None, None]
    data["last_outreach_date"] = [None, None]
    data["has_voluntary_alignment"] = [None, None]
    data["sva_provider_valid"] = [None, None]
    data["latest_response_codes"] = [None, None]
    data["latest_response_detail"] = [None, None]
    data["eligibility_issues"] = [None, None]
    data["last_valid_signature_date"] = [None, None]
    data["last_sva_submission_date"] = [None, None]
    data["has_acceptance"] = [None, None]
    data["first_reach_date"] = [None, None]
    data["pbvar_report_date"] = [None, None]
    data["provider_valid"] = [None, None]
    data["bene_first_name"] = ["Alice", "Bob"]
    data["bene_last_name"] = ["Smith", "Jones"]
    data["office_location"] = ["West", "East"]
    data["voluntary_alignment_type"] = [None, None]
    data["bene_date_of_term"] = [None, None]

    schema = {}
    for k in data.keys():
        if k.startswith("ym_"):
            schema[k] = pl.Boolean
        elif k in ("bar_termed_date", "sva_expiration_date", "last_provider_activity_date",
                    "ma_enrollment_date", "last_activity_date", "last_reach_date",
                    "last_mssp_date", "ffs_first_date", "death_date", "last_outreach_date",
                    "last_valid_signature_date", "last_sva_submission_date", "first_reach_date",
                    "pbvar_report_date", "bene_date_of_term"):
            schema[k] = pl.Date
        elif k in ("has_voluntary_alignment", "sva_provider_valid", "has_acceptance", "provider_valid"):
            schema[k] = pl.Boolean
        else:
            schema[k] = pl.Utf8

    return pl.DataFrame(data, schema=schema).lazy()


class TestTransitionsIdempotency:
    """Test idempotency guard."""

    @pytest.mark.unit
    def test_skip_when_already_calculated(self):

        df = _make_temporal_df().collect().with_columns(
            pl.lit(True).alias("_transitions_calculated")
        ).lazy()
        logger = _make_logger()
        result = apply_transform(df, {}, MagicMock(), logger, force=False)
        collected = result.collect()
        assert "_transitions_calculated" in collected.columns
        logger.info.assert_any_call("Transitions already calculated, skipping")

    @pytest.mark.unit
    def test_force_bypasses_idempotency(self):

        df = _make_temporal_df().collect().with_columns(
            pl.lit(True).alias("_transitions_calculated")
        ).lazy()
        logger = _make_logger()
        result = apply_transform(df, {}, MagicMock(), logger, force=True,
                                 current_year=2025, previous_year=2024)
        collected = result.collect()
        assert "transition_analysis_current_year" in collected.columns


class TestTransitionsYearDetection:
    """Test year detection from temporal columns."""

    @pytest.mark.unit
    def test_auto_detect_years_from_temporal_columns(self):

        df = _make_temporal_df(current_year=2025, previous_year=2024)
        logger = _make_logger()
        result = apply_transform(df, {}, MagicMock(), logger)
        collected = result.collect()

        assert collected["transition_analysis_current_year"][0] == 2025
        assert collected["transition_analysis_previous_year"][0] == 2024

    @pytest.mark.unit
    def test_explicit_years_override(self):

        df = _make_temporal_df(current_year=2025, previous_year=2024)
        logger = _make_logger()
        result = apply_transform(
            df, {}, MagicMock(), logger,
            current_year=2025, previous_year=2024
        )
        collected = result.collect()
        assert collected["transition_analysis_current_year"][0] == 2025

    @pytest.mark.unit
    def test_fallback_to_calendar_year_no_temporal_cols(self):

        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
        }).lazy()
        logger = _make_logger()
        # No temporal columns -> falls back to calendar year, then warns no prev/curr cols
        result = apply_transform(df, {}, MagicMock(), logger)
        collected = result.collect()
        assert "_transitions_calculated" in collected.columns

    @pytest.mark.unit
    def test_only_current_year_provided(self):

        df = _make_temporal_df(current_year=2025, previous_year=2024)
        logger = _make_logger()
        result = apply_transform(
            df, {}, MagicMock(), logger,
            current_year=2025  # previous_year=None -> auto-detect from temporal cols
        )
        collected = result.collect()
        assert collected["transition_analysis_previous_year"][0] == 2024

    @pytest.mark.unit
    def test_only_previous_year_provided(self):
        """When previous_year is given but current_year is None, current_year is
        auto-detected from temporal columns while previous_year stays as-is
        (covers branch 105->108)."""
        df = _make_temporal_df(current_year=2025, previous_year=2024)
        logger = _make_logger()
        result = apply_transform(
            df, {}, MagicMock(), logger,
            current_year=None, previous_year=2024,
        )
        collected = result.collect()
        assert collected["transition_analysis_current_year"][0] == 2025
        assert collected["transition_analysis_previous_year"][0] == 2024

    @pytest.mark.unit
    def test_no_temporal_cols_current_year_set_previous_none(self):
        """Branch 112->114: no temporal columns, current_year already set so
        the `if current_year is None` at line 112 is False and we jump to 114."""
        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
        }).lazy()
        logger = _make_logger()
        # current_year provided, previous_year=None -> enters outer if,
        # no ym_ cols -> else block, current_year is not None -> skip 113
        result = apply_transform(
            df, {}, MagicMock(), logger,
            current_year=2025, previous_year=None,
        )
        collected = result.collect()
        assert "_transitions_calculated" in collected.columns

    @pytest.mark.unit
    def test_no_temporal_cols_previous_year_set_current_none(self):
        """Branch 114->117: no temporal columns, previous_year already set so
        the `if previous_year is None` at line 114 is False and we jump to 117."""
        df = pl.DataFrame({
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
        }).lazy()
        logger = _make_logger()
        # previous_year provided, current_year=None -> enters outer if,
        # no ym_ cols -> else block, previous_year is not None -> skip 115
        result = apply_transform(
            df, {}, MagicMock(), logger,
            current_year=None, previous_year=2024,
        )
        collected = result.collect()
        assert "_transitions_calculated" in collected.columns


class TestTransitionsMissingYearColumns:
    """Test when required temporal columns are missing for a year."""

    @pytest.mark.unit
    def test_no_previous_year_columns(self):

        # Only current year columns, no previous year
        data = {
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
        }
        for _m in range(1, 13):
            data["ym_202501_reach"] = [True]
        df = pl.DataFrame(data).lazy()
        logger = _make_logger()
        result = apply_transform(df, {}, MagicMock(), logger,
                                 current_year=2025, previous_year=2024)
        collected = result.collect()
        assert "_transitions_calculated" in collected.columns

    @pytest.mark.unit
    def test_no_current_year_columns(self):

        data = {
            "current_mbi": ["MBI001"],
            "current_program": ["MSSP"],
        }
        for m in range(1, 13):
            data[f"ym_2024{m:02d}_reach"] = [True]
        df = pl.DataFrame(data).lazy()
        logger = _make_logger()
        result = apply_transform(df, {}, MagicMock(), logger,
                                 current_year=2025, previous_year=2024)
        collected = result.collect()
        assert "_transitions_calculated" in collected.columns


class TestTransitionsFullPipeline:
    """Test the full transition analysis pipeline."""

    @pytest.mark.unit
    def test_all_transition_columns_created(self):

        df = _make_temporal_df(current_year=2025, previous_year=2024)
        logger = _make_logger()
        result = apply_transform(
            df, {}, MagicMock(), logger,
            current_year=2025, previous_year=2024
        )
        collected = result.collect()

        expected_cols = [
            "_transitions_calculated",
            "transition_analysis_current_year",
            "transition_analysis_previous_year",
            "transition_analysis_date",
        ]
        for col in expected_cols:
            assert col in collected.columns, f"Missing column: {col}"

    @pytest.mark.unit
    def test_transition_marks_processed(self):

        df = _make_temporal_df()
        logger = _make_logger()
        result = apply_transform(df, {}, MagicMock(), logger,
                                 current_year=2025, previous_year=2024)
        collected = result.collect()
        assert collected["_transitions_calculated"].to_list() == [True, True]


class TestCalculateTransitionSummary:
    """Test the summary aggregation function."""

    @pytest.mark.unit
    def test_summary_aggregation(self):

        df = pl.DataFrame({
            "transition_category_2024": ["Lost", "Lost", "Retained", "Newly Added"],
            "current_program": ["MSSP", "REACH", "MSSP", "REACH"],
        }).lazy()

        summary = calculate_transition_summary(df, previous_year=2024)
        assert summary.height > 0
        assert "count" in summary.columns
        # "Lost" should have count 2
        lost_row = summary.filter(pl.col("transition_category_2024") == "Lost")
        assert lost_row["count"][0] == 2
