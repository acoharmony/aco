"""Tests for acoharmony._expressions.reach_bnmr module."""

import polars as pl
import pytest

from acoharmony._expressions.reach_bnmr import ReachBNMRMultiTableExpression


class TestReachBNMRMultiTableExpressionBuild:
    """Tests for the build() classmethod that splits data by sheet_type."""

    @pytest.mark.unit
    def test_build_concatenates_duplicate_table_names(self):
        """Cover branch 109: table_name already in results triggers concat.

        SHEET_TO_TABLE maps both "financial_settlement" and "claims"
        to "reach_bnmr_claims", so the second one hits the concat branch.
        """
        df = pl.DataFrame({
            "sheet_type": [
                "report_parameters",
                "financial_settlement",
                "claims",
                "risk",
            ],
            "col_a": ["rp_val", "fs_val", "cl_val", "rk_val"],
        }).lazy()

        results = ReachBNMRMultiTableExpression.build(df)

        # reach_bnmr_claims should contain rows from both sheet types
        assert "reach_bnmr_claims" in results
        claims_df = results["reach_bnmr_claims"].collect()
        sheet_types = claims_df["sheet_type"].to_list()
        assert "financial_settlement" in sheet_types
        assert "claims" in sheet_types
        assert len(claims_df) == 2

    @pytest.mark.unit
    def test_build_creates_metadata_table(self):
        """Cover line 103-104: metadata_lf added to results."""
        df = pl.DataFrame({
            "sheet_type": ["report_parameters", "county"],
            "col_a": ["meta_val", "county_val"],
        }).lazy()

        results = ReachBNMRMultiTableExpression.build(df)
        assert "reach_bnmr_metadata" in results
        meta_df = results["reach_bnmr_metadata"].collect()
        assert len(meta_df) == 1
        assert meta_df["sheet_type"][0] == "report_parameters"

    @pytest.mark.unit
    def test_build_risk_concatenation(self):
        """Cover branch 109 again: riskscore_ad and risk both map to reach_bnmr_risk."""
        df = pl.DataFrame({
            "sheet_type": [
                "report_parameters",
                "riskscore_ad",
                "risk",
            ],
            "col_a": ["rp", "rs_ad", "rk"],
        }).lazy()

        results = ReachBNMRMultiTableExpression.build(df)
        assert "reach_bnmr_risk" in results
        risk_df = results["reach_bnmr_risk"].collect()
        assert len(risk_df) == 2


class TestReachBnmrMetadataIsNotNone:
    """Auto-generated coverage test."""

    @pytest.mark.unit
    def test_reach_bnmr_metadata_is_not_none(self):
        """103->107: metadata_lf IS None (LazyFrame filter always returns non-None, so this needs empty df)."""
        from acoharmony._expressions.reach_bnmr import ReachBNMRMultiTableExpression
        df = pl.DataFrame({"sheet_type": ["claims"], "col": ["v"]}).lazy()
        results = ReachBNMRMultiTableExpression.build(df)
        assert "reach_bnmr_metadata" in results


class TestReachBnmrMetadataLfIsNone:
    """Cover branch 103->107: metadata_lf is None (False branch)."""

    @pytest.mark.unit
    def test_metadata_lf_none_skips_metadata_table(self):
        """103->107: when metadata_lf is None, 'reach_bnmr_metadata' is not added."""
        from unittest.mock import patch
        from acoharmony._expressions.reach_bnmr import ReachBNMRMultiTableExpression

        df = pl.DataFrame({
            "sheet_type": ["financial_settlement", "claims"],
            "col_a": ["fs_val", "cl_val"],
        }).lazy()

        # Patch LazyFrame.filter to return None for the report_parameters filter
        original_filter = pl.LazyFrame.filter

        def patched_filter(self, *args, **kwargs):
            result = original_filter(self, *args, **kwargs)
            # Check if this is the report_parameters filter by inspecting the args
            # We return None to trigger the False branch
            try:
                expr_str = str(args[0])
                if "report_parameters" in expr_str:
                    return None
            except Exception:
                pass
            return result

        with patch.object(pl.LazyFrame, "filter", patched_filter):
            results = ReachBNMRMultiTableExpression.build(df)

        # metadata key should NOT be present since metadata_lf was None
        assert "reach_bnmr_metadata" not in results
        # Other tables should still be present
        assert "reach_bnmr_claims" in results
