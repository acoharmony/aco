# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._provider_attribution module."""

import polars as pl
import pytest

from acoharmony._expressions._provider_attribution import ProviderAttributionExpression


class TestMsspProviderNameExpr:
    """Cover build_mssp_provider_name_expr (line 63)."""

    @pytest.mark.unit
    def test_name_from_first_last(self):
        df = pl.DataFrame({
            "first_name": ["John", None],
            "last_name": ["Doe", None],
            "tin_legal_bus_name": ["ACME Corp", "Beta LLC"],
        })
        result = df.select(ProviderAttributionExpression.build_mssp_provider_name_expr())
        assert result["mssp_provider_name"][0] == "Doe, John"
        assert result["mssp_provider_name"][1] == "Beta LLC"


class TestMsspProviderSelectExpr:
    """Cover build_mssp_provider_select_expr (line 84)."""

    @pytest.mark.unit
    def test_select_columns(self):
        df = pl.DataFrame({
            "last_ffs_tin": ["TIN1"],
            "last_ffs_npi": ["NPI1"],
        })
        result = df.select(ProviderAttributionExpression.build_mssp_provider_select_expr())
        assert "mssp_tin" in result.columns
        assert "mssp_npi" in result.columns


class TestReachAttributionTypeBarExpr:
    """Cover build_reach_attribution_type_bar_expr (line 100)."""

    @pytest.mark.unit
    def test_voluntary_and_claims(self):
        df = pl.DataFrame({
            "voluntary_alignment_type": ["SVA", None, None],
            "claims_based_flag": [None, "Y", "N"],
        })
        result = df.select(ProviderAttributionExpression.build_reach_attribution_type_bar_expr())
        vals = result["reach_attribution_type"].to_list()
        assert vals[0] == "Voluntary"
        assert vals[1] == "Claims-based"
        assert vals[2] == "Unknown"


class TestReachAttributionTypeVolExpr:
    """Cover build_reach_attribution_type_vol_expr (line 120)."""

    @pytest.mark.unit
    def test_sva_pbvar_none(self):
        df = pl.DataFrame({
            "sva_signature_count": [1, 0, 0],
            "pbvar_aligned": [False, True, False],
        })
        result = df.select(ProviderAttributionExpression.build_reach_attribution_type_vol_expr())
        vals = result["reach_attribution_type"].to_list()
        assert vals[0] == "Voluntary"
        assert vals[1] == "Claims-based"
        assert vals[2] is None


class TestAlignedProviderExprs:
    """Cover TIN/NPI/org/practitioner exprs (lines 138,156,174,192)."""

    @pytest.mark.unit
    def test_aligned_provider_by_program(self):
        df = pl.DataFrame({
            "current_program": ["REACH", "MSSP", "OTHER"],
            "reach_tin": ["RT1", "RT2", "RT3"],
            "mssp_tin": ["MT1", "MT2", "MT3"],
            "reach_npi": ["RN1", "RN2", "RN3"],
            "mssp_npi": ["MN1", "MN2", "MN3"],
            "reach_provider_name": ["RP1", "RP2", "RP3"],
            "mssp_provider_name": ["MP1", "MP2", "MP3"],
        })
        tin = df.select(ProviderAttributionExpression.build_aligned_provider_tin_expr())
        npi = df.select(ProviderAttributionExpression.build_aligned_provider_npi_expr())
        org = df.select(ProviderAttributionExpression.build_aligned_provider_org_expr())
        prac = df.select(ProviderAttributionExpression.build_aligned_practitioner_name_expr())

        assert tin["aligned_provider_tin"].to_list() == ["RT1", "MT2", None]
        assert npi["aligned_provider_npi"].to_list() == ["RN1", "MN2", None]
        assert org["aligned_provider_org"].to_list() == ["RP1", "MP2", None]
        assert prac["aligned_practitioner_name"].to_list() == ["RP1", "MP2", None]


class TestLatestAcoIdExpr:
    """Cover build_latest_aco_id_expr (line 210)."""

    @pytest.mark.unit
    def test_alias(self):
        df = pl.DataFrame({"current_aco_id": ["ACO1"]})
        result = df.select(ProviderAttributionExpression.build_latest_aco_id_expr())
        assert result["latest_aco_id"][0] == "ACO1"


class TestFinalSelectExpr:
    """Cover build_provider_attribution_final_select (line 221)."""

    @pytest.mark.unit
    def test_returns_list(self):
        exprs = ProviderAttributionExpression.build_provider_attribution_final_select()
        assert isinstance(exprs, list)
        assert len(exprs) == 13
