from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._aco_transition_lost_provider import build_lost_provider_expr


class TestAcoTransitionLostProvider:
    """Tests for _aco_transition_lost_provider expression builders."""

    @pytest.mark.unit
    def test_build_lost_provider_expr(self):
        df = pl.DataFrame({'ym_202412_reach': [True, True], 'ym_202501_reach': [False, False], 'sva_provider_valid': [False, True], 'has_voluntary_alignment': [True, True]})
        result = df.select(build_lost_provider_expr(2025, 2024, current_year_months=[1]))
        assert result['lost_provider_2024'][0] is True
        assert result['lost_provider_2024'][1] is False


class TestBuildLostProviderExprCurrentYearMonthsNone:
    """Cover 42→43: current_year_months is None → defaults to range(1, 13)."""

    @pytest.mark.unit
    def test_none_months_defaults_to_all_twelve(self):
        """When current_year_months=None, all 12 months are checked."""
        # Build a DataFrame with all 12 months of current year columns (all False)
        # and December of previous year as True
        data = {
            "ym_202412_reach": [True, True],
            "sva_provider_valid": [False, True],
            "has_voluntary_alignment": [True, True],
        }
        # Add all 12 months for 2025
        for m in range(1, 13):
            data[f"ym_2025{m:02d}_reach"] = [False, False]

        df = pl.DataFrame(data)
        result = df.select(
            build_lost_provider_expr(2025, 2024, current_year_months=None)
        )
        # Row 0: was REACH, not in any 2025 month, provider invalid, had voluntary → True
        assert result["lost_provider_2024"][0] is True
        # Row 1: provider is valid → False
        assert result["lost_provider_2024"][1] is False


class TestIdentifyLostProviders:
    """Cover identify_lost_providers lines 94-139."""

    @pytest.mark.unit
    def test_identifies_lost_providers(self):
        from acoharmony._expressions._aco_transition_lost_provider import get_lost_providers

        alignment_df = pl.DataFrame({
            "current_mbi": ["M1", "M2", "M3"],
            "alignment_year": [2024, 2024, 2024],
            "voluntary_provider_npi": ["NPI1", "NPI2", "NPI3"],
            "voluntary_provider_tin": ["TIN1", "TIN2", "TIN3"],
            "voluntary_provider_name": ["Dr. A", "Dr. B", "Dr. C"],
        }).lazy()

        # NPI1 and NPI3 are not in current year roster → lost
        participant_list = pl.DataFrame({
            "performance_year": [2025, 2025],
            "npi": ["NPI2", "NPI_OTHER"],
            "tin": ["TIN2", "TIN_OTHER"],
        }).lazy()

        pvar_df = pl.DataFrame({"id": [1]}).lazy()

        result = get_lost_providers(
            alignment_df, participant_list, pvar_df,
            previous_year=2024, current_year=2025,
        ).collect()

        assert "current_mbi" in result.columns
        assert "loss_reason" in result.columns
        # M1 and M3 should be lost (NPI1 and NPI3 not in roster)
        lost_mbis = set(result["current_mbi"].to_list())
        assert "M1" in lost_mbis
        assert "M3" in lost_mbis
        assert "M2" not in lost_mbis
