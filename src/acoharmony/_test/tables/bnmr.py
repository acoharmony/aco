# © 2025 HarmonyCares
# All rights reserved.

"""Tests for BNMR benchmark table ORM models."""

import pytest

from acoharmony._tables._bnmr import (
    BNMR_REPORT_TYPES,
    BnmrBenchmarkHistoricalAd,
    BnmrBenchmarkHistoricalEsrd,
    BnmrCap,
    BnmrClaims,
    BnmrCounty,
    BnmrDataStopLossPayout,
    BnmrFinancialSettlement,
    BnmrHeba,
    BnmrMetadataMixin,
    BnmrReportParameters,
    BnmrRisk,
    BnmrRiskscoreAd,
    BnmrRiskscoreEsrd,
    BnmrStopLossCharge,
    BnmrStopLossClaims,
    BnmrStopLossCounty,
    BnmrStopLossPayout,
    BnmrUspcc,
)


ALL_MODELS = [
    BnmrReportParameters,
    BnmrFinancialSettlement,
    BnmrClaims,
    BnmrRisk,
    BnmrCounty,
    BnmrUspcc,
    BnmrHeba,
    BnmrCap,
    BnmrRiskscoreAd,
    BnmrRiskscoreEsrd,
    BnmrBenchmarkHistoricalAd,
    BnmrBenchmarkHistoricalEsrd,
    BnmrStopLossCharge,
    BnmrStopLossPayout,
    BnmrStopLossCounty,
    BnmrStopLossClaims,
    BnmrDataStopLossPayout,
]


class TestReportTypes:
    @pytest.mark.unit
    def test_known_codes(self):
        assert "Q1" in BNMR_REPORT_TYPES
        assert "SP" in BNMR_REPORT_TYPES
        assert "S1" in BNMR_REPORT_TYPES
        assert "P1" in BNMR_REPORT_TYPES
        assert "PSA1" in BNMR_REPORT_TYPES

    @pytest.mark.unit
    def test_all_values_are_strings(self):
        for k, v in BNMR_REPORT_TYPES.items():
            assert isinstance(k, str)
            assert isinstance(v, str)


class TestAllModelsInstantiate:
    @pytest.mark.unit
    @pytest.mark.parametrize("model", ALL_MODELS, ids=lambda m: m.__name__)
    def test_default_instantiation(self, model):
        """Every model can be instantiated with all defaults (None)."""
        instance = model()
        assert instance is not None
        # report_type should be on every model (from mixin)
        assert hasattr(instance, "report_type")
        assert instance.report_type is None

    @pytest.mark.unit
    @pytest.mark.parametrize("model", ALL_MODELS, ids=lambda m: m.__name__)
    def test_report_type_settable(self, model):
        instance = model(report_type="Q1")
        assert instance.report_type == "Q1"

    @pytest.mark.unit
    @pytest.mark.parametrize("model", ALL_MODELS, ids=lambda m: m.__name__)
    def test_has_provenance_fields(self, model):
        instance = model()
        assert hasattr(instance, "source_filename")
        assert hasattr(instance, "processed_at")
        assert hasattr(instance, "performance_year")
        assert hasattr(instance, "aco_id")


class TestSpecificModels:
    @pytest.mark.unit
    def test_claims_fields(self):
        row = BnmrClaims(
            report_type="Q1",
            perf_yr="2025",
            clndr_yr="2025",
            clndr_mnth="3",
            bnmrk="AD",
            clm_pmt_amt_agg=1500.0,
            sqstr_amt_agg=30.0,
        )
        assert row.perf_yr == "2025"
        assert row.clm_pmt_amt_agg == 1500.0
        assert row.sqstr_amt_agg == 30.0

    @pytest.mark.unit
    def test_risk_fields(self):
        row = BnmrRisk(
            bene_dcnt=100,
            elig_mnths=1200,
            raw_risk_score=1.05,
            va_cat="N",
        )
        assert row.bene_dcnt == 100
        assert row.va_cat == "N"

    @pytest.mark.unit
    def test_county_fields(self):
        row = BnmrCounty(
            cty_accrl_cd="48439",
            cty_rate=1050.0,
            adj_cty_pmt=77700.0,
            bene_dcnt=74,
            elig_mnths=74,
        )
        assert row.cty_accrl_cd == "48439"
        assert row.adj_cty_pmt == 77700.0

    @pytest.mark.unit
    def test_uspcc_fields(self):
        row = BnmrUspcc(
            bnmrk="AD",
            uspcc=1023.31,
            ucc_hosp_adj=14.78,
            adj_ffs_uspcc=1038.09,
        )
        assert row.uspcc == 1023.31

    @pytest.mark.unit
    def test_cap_old_format(self):
        row = BnmrCap(
            pmt_mnth="2024-03",
            aco_bpcc_amt_total=1000.0,
            aco_epcc_amt_total_seq=200.0,
        )
        assert row.aco_bpcc_amt_total == 1000.0
        assert row.aco_bpcc_amt_post_seq_paid is None

    @pytest.mark.unit
    def test_cap_new_format(self):
        row = BnmrCap(
            pmt_mnth="2026-03",
            aco_bpcc_amt_post_seq_paid=500.0,
            aco_tcc_amt_post_seq_paid=625.0,
        )
        assert row.aco_bpcc_amt_post_seq_paid == 500.0
        assert row.aco_bpcc_amt_total is None

    @pytest.mark.unit
    def test_benchmark_historical_by_fields(self):
        row = BnmrBenchmarkHistoricalAd(
            by1_value="2021",
            by2_value="2022",
            by3_value="2023",
            claims_benchmark=1050.0,
        )
        assert row.by1_value == "2021"
        assert row.by3_value == "2023"

    @pytest.mark.unit
    def test_riskscore_named_fields(self):
        row = BnmrRiskscoreAd(
            normalized_risk_score_claims_py="1.15",
            capped_risk_score_claims_py="1.03",
            benchmark_risk_score_claims_py="1.04",
        )
        assert row.normalized_risk_score_claims_py == "1.15"

    @pytest.mark.unit
    def test_stop_loss_county_fields(self):
        row = BnmrStopLossCounty(
            avg_payout_pct=0.0176,
            adj_avg_payout_pct=31.5,
            ad_ry_avg_pbpm=1103.75,
        )
        assert row.avg_payout_pct == 0.0176

    @pytest.mark.unit
    def test_data_stop_loss_payout_buckets(self):
        row = BnmrDataStopLossPayout(
            aco_stoploss_payout_b0=100.0,
            aco_stoploss_payout_b1=200.0,
            aco_stoploss_payout_b2=300.0,
            aco_stoploss_payout_total=600.0,
        )
        assert row.aco_stoploss_payout_total == 600.0

    @pytest.mark.unit
    def test_heba_fields(self):
        row = BnmrHeba(
            heba_up_mnths=500,
            heba_down_mnths=100,
            heba_up_amt=5000.0,
            heba_down_amt=-1000.0,
        )
        assert row.heba_up_mnths == 500
        assert row.heba_down_amt == -1000.0

    @pytest.mark.unit
    def test_financial_settlement_fields(self):
        row = BnmrFinancialSettlement(
            line_number="1",
            line_description="Benchmark Before Discount",
            ad_value=50000.0,
            esrd_value=10000.0,
            total_value=60000.0,
        )
        assert row.total_value == 60000.0

    @pytest.mark.unit
    def test_stop_loss_claims_minimal(self):
        row = BnmrStopLossClaims(perf_yr="2025")
        assert row.perf_yr == "2025"
