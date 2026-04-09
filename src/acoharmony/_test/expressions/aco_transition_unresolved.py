# © 2025 HarmonyCares
# All rights reserved.





# =============================================================================
# Tests for aco_transition_unresolved
# =============================================================================












# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest


class TestAcoTransitionUnresolved:
    """Test cases for expression builders."""

    @pytest.mark.unit
    def test_build_unresolved_expr(self):

        df = pl.DataFrame({
            "ym_202412_reach": [True],
            "ym_202501_reach": [False],
            "termed_bar_2024": [False],
            "expired_sva_2024": [False],
            "lost_provider_2024": [False],
            "moved_ma_2024": [False],
            "inactive_2024": [False],
            "death_date": pl.Series([None], dtype=pl.Date),
        })
        result = df.select(build_unresolved_expr(2025, 2024, current_year_months=[1]))
        assert result["unresolved_2024"][0] is True










    @pytest.mark.unit
    def test_build_potential_unresolved_reasons_expr(self):
        df = pl.DataFrame({
            "latest_response_codes": [None, "A0", "A0"],
            "sva_provider_valid": [True, None, True],
            "has_voluntary_alignment": [True, True, None],
        })
        result = df.select(build_potential_unresolved_reasons_expr())
        assert result["unresolved_reason"][0] == "Missing response codes"
        assert result["unresolved_reason"][1] == "Provider validation unknown"
        assert result["unresolved_reason"][2] == "Voluntary alignment status unknown"










    @pytest.mark.unit
    def test_build_transition_category_expr(self):
        df = pl.DataFrame({
            "death_date": [date(2024, 6, 1), None, None, None, None, None, None, None, None],
            "moved_ma_2024": [False, True, False, False, False, False, False, False, False],
            "termed_bar_2024": [False, False, True, False, False, False, False, False, False],
            "expired_sva_2024": [False, False, False, True, False, False, False, False, False],
            "lost_provider_2024": [False, False, False, False, True, False, False, False, False],
            "inactive_2024": [False, False, False, False, False, True, False, False, False],
            "unresolved_2024": [False, False, False, False, False, False, True, False, False],
            "lost_2024_to_2025": [False, False, False, False, False, False, False, True, False],
        })
        result = df.select(build_transition_category_expr(2024, 2025))
        cats = result["transition_category_2024"].to_list()
        assert cats == [
            "Deceased", "Moved to MA", "Termed on BAR", "Expired SVA",
            "Lost Provider", "Inactive", "Unresolved", "Lost (Other)", "Retained",
        ]










    @pytest.mark.unit
    def test_build_unresolved_expr_default_months(self):
        """Branch 42->43: current_year_months=None defaults to all 12 months."""
        cols = {f"ym_202{5}{m:02d}_reach": [False] for m in range(1, 13)}
        cols["ym_202412_reach"] = [True]
        cols["termed_bar_2024"] = [False]
        cols["expired_sva_2024"] = [False]
        cols["lost_provider_2024"] = [False]
        cols["moved_ma_2024"] = [False]
        cols["inactive_2024"] = [False]
        cols["death_date"] = pl.Series([None], dtype=pl.Date)
        df = pl.DataFrame(cols)
        result = df.select(build_unresolved_expr(2025, 2024, current_year_months=None))
        assert result["unresolved_2024"][0] is True










    @pytest.mark.unit
    def test_get_unresolved_losses(self):
        """Lines 117, 138: returns LazyFrame of unresolved investigations."""

        alignment_df = pl.LazyFrame({
            "current_mbi": ["MBI001"],
            "unresolved_2024": [True],
            "bene_first_name": ["John"],
            "bene_last_name": ["Doe"],
            "office_location": ["NYC"],
            "last_reach_date": ["2024-01-01"],
            "last_mssp_date": ["2024-02-01"],
            "last_outreach_date": ["2024-03-01"],
            "has_voluntary_alignment": [False],
            "provider_valid": [True],
            "latest_response_codes": ["01"],
            "death_date": [None],
        })
        result = get_unresolved_losses(alignment_df, previous_year=2024, current_year=2025)
        assert isinstance(result, pl.LazyFrame)


















