from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._expressions._reach_bnmr_multi_table import ReachBnmrMultiTableExpression
from acoharmony._expressions.reach_bnmr import ReachBNMRMultiTableExpression


class TestReachBnmrMultiTableExpression:

    @pytest.mark.unit
    def test_class_attributes(self):
        assert 'report_parameters' in ReachBnmrMultiTableExpression.METADATA_SHEET_TYPES
        assert 'claims' in ReachBnmrMultiTableExpression.DATA_SHEET_MAPPING
        assert 'source_filename' in ReachBnmrMultiTableExpression.NATURAL_KEY_FIELDS

    @pytest.mark.unit
    def test_build_concatenates_same_table_sheets(self):
        """Cover branch 109: table_name already in results -> concat."""
        import polars as pl

        # Create DataFrame with two sheet_types that map to the same table.
        # SHEET_TO_TABLE has "financial_settlement" -> "reach_bnmr_claims"
        # and "claims" -> "reach_bnmr_claims", so both should be concatenated.
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

        # "reach_bnmr_claims" should contain rows from both sheet types
        assert "reach_bnmr_claims" in results
        claims_df = results["reach_bnmr_claims"].collect()
        sheet_types = claims_df["sheet_type"].to_list()
        assert "financial_settlement" in sheet_types
        assert "claims" in sheet_types
        assert len(claims_df) == 2

    @pytest.mark.unit
    def test_build_creates_metadata_table(self):
        """Cover branch 103: metadata_lf is not None -> add to results."""
        import polars as pl

        df = pl.DataFrame({
            "sheet_type": ["report_parameters", "county"],
            "col_a": ["meta_val", "county_val"],
        }).lazy()

        results = ReachBNMRMultiTableExpression.build(df)
        assert "reach_bnmr_metadata" in results
        meta_df = results["reach_bnmr_metadata"].collect()
        assert len(meta_df) == 1
        assert meta_df["sheet_type"][0] == "report_parameters"
