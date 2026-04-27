# © 2025 HarmonyCares
"""Tests for acoharmony._notes._panels (PanelPlugins)."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import polars as pl
import pytest

from acoharmony._notes import PanelPlugins, UIPlugins


@pytest.fixture
def panel():
    ui = UIPlugins()
    ui._mo = MagicMock()
    p = PanelPlugins(ui)
    p._mo = ui._mo
    return p


class TestIdentityPanel:
    @pytest.mark.unit
    def test_renders_with_demographics(self, panel):
        df = pl.DataFrame(
            {
                "bene_dob": [date(1955, 1, 1)],
                "bene_sex_cd": ["1"],
                "bene_age": [70],
                "bene_fips_state_cd": ["36"],
                "bene_zip_cd": ["10001"],
                "bene_mdcr_stus_cd": ["10"],
                "bene_dual_stus_cd": ["NA"],
            }
        )
        panel.identity_panel("M", "HC", "M", ["M"], df)
        panel._mo.Html.assert_called_once()
        html = panel._mo.Html.call_args.args[0]
        assert "Male" in html
        assert "NY" in html
        assert "10001" in html

    @pytest.mark.unit
    def test_decodes_female(self, panel):
        df = pl.DataFrame(
            {
                "bene_dob": [None],
                "bene_sex_cd": ["2"],
                "bene_age": [None],
                "bene_fips_state_cd": ["XX"],
                "bene_zip_cd": [None],
                "bene_mdcr_stus_cd": [None],
                "bene_dual_stus_cd": [None],
            }
        )
        panel.identity_panel("M", None, "M", [], df)
        html = panel._mo.Html.call_args.args[0]
        assert "Female" in html
        assert "Not Mapped" in html  # hcmpi=None branch
        assert "None" in html  # empty history branch

    @pytest.mark.unit
    def test_demographics_missing(self, panel):
        panel.identity_panel("M", "HC", "M", ["M"], None)
        html = panel._mo.Html.call_args.args[0]
        assert "Not found in beneficiary table" in html


class TestAlignmentPanel:
    @pytest.mark.unit
    def test_no_alignment(self, panel):
        panel.alignment_panel(None)
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_renders_year_program_grid(self, panel):
        df = pl.DataFrame(
            {
                "current_mbi": ["A"],
                "ym_202401_reach": [True],
                "ym_202402_reach": [True],
                "ym_202401_mssp": [False],
                "ym_202312_reach_first_claim": [True],  # excluded
                "has_voluntary_alignment": [False],
                "has_valid_voluntary_alignment": [False],
                "has_valid_historical_sva": [False],
            }
        )
        panel.alignment_panel(df)
        panel._mo.vstack.assert_called_once()
        # ui.table should have been called with a dataframe summarizing 2024
        assert panel._mo.ui.table.called

    @pytest.mark.unit
    def test_voluntary_and_sva_callouts(self, panel):
        df = pl.DataFrame(
            {
                "current_mbi": ["A"],
                "has_voluntary_alignment": [True],
                "voluntary_alignment_type": ["Type1"],
                "voluntary_alignment_date": [date(2024, 1, 1)],
                "voluntary_provider_name": ["Dr. Foo"],
                "voluntary_provider_npi": ["12345"],
                "has_valid_voluntary_alignment": [True],
                "has_valid_historical_sva": [True],
                "first_sva_submission_date": [date(2024, 2, 1)],
                "last_sva_submission_date": [date(2024, 6, 1)],
                "sva_action_needed": ["Review"],
            }
        )
        panel.alignment_panel(df)
        # Voluntary + SVA + month rollup → at least 2 callout calls.
        assert panel._mo.callout.call_count >= 2

    @pytest.mark.unit
    def test_only_voluntary_sva(self, panel):
        df = pl.DataFrame(
            {
                "current_mbi": ["A"],
                "has_voluntary_alignment": [False],
                "has_valid_voluntary_alignment": [True],
                "has_valid_historical_sva": [False],
            }
        )
        panel.alignment_panel(df)
        assert panel._mo.callout.called

    @pytest.mark.unit
    def test_only_historical_sva(self, panel):
        df = pl.DataFrame(
            {
                "current_mbi": ["A"],
                "has_voluntary_alignment": [False],
                "has_valid_voluntary_alignment": [False],
                "has_valid_historical_sva": [True],
            }
        )
        panel.alignment_panel(df)
        assert panel._mo.callout.called

    @pytest.mark.unit
    def test_unknown_program_skipped(self, panel):
        df = pl.DataFrame(
            {
                "current_mbi": ["A"],
                "ym_202401_other": [True],  # program not in {reach,mssp,ffs}
                "has_voluntary_alignment": [False],
                "has_valid_voluntary_alignment": [False],
                "has_valid_historical_sva": [False],
            }
        )
        panel.alignment_panel(df)
        # Renders the year row with all zeros (year_program["2024"] created but no program incremented)
        panel._mo.ui.table.assert_called()

    @pytest.mark.unit
    def test_no_aligned_months(self, panel):
        df = pl.DataFrame(
            {
                "current_mbi": ["A"],
                "ym_short_reach": [True],  # parts[1]="short" len 5 → skipped
                "has_voluntary_alignment": [False],
                "has_valid_voluntary_alignment": [False],
                "has_valid_historical_sva": [False],
            }
        )
        panel.alignment_panel(df)
        md_calls = [
            c.args[0] for c in panel._mo.md.call_args_list if isinstance(c.args[0], str)
        ]
        assert any("No aligned months" in s for s in md_calls)


class TestConditionsPanel:
    @pytest.mark.unit
    def test_no_conditions(self, panel):
        panel.conditions_panel(None)
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_renders_active_list(self, panel):
        df = pl.DataFrame(
            {
                "person_id": ["A"],
                "condition_count": [2],
                "diabetes": [True],
                "asthma": [False],
                "ckd": [True],
            }
        )
        panel.conditions_panel(df)
        html = panel._mo.Html.call_args.args[0]
        assert "<li>diabetes</li>" in html
        assert "<li>ckd</li>" in html
        assert "<li>asthma</li>" not in html
        assert "Active Conditions:</strong> 2" in html

    @pytest.mark.unit
    def test_no_active_conditions(self, panel):
        df = pl.DataFrame({"person_id": ["A"], "condition_count": [0], "diabetes": [False]})
        panel.conditions_panel(df)
        html = panel._mo.Html.call_args.args[0]
        assert "<li>None</li>" in html


class TestSpendUtilPanel:
    @pytest.mark.unit
    def test_empty(self, panel):
        panel.spend_util_panel(pl.DataFrame())
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_renders_two_tables(self, panel):
        df = pl.DataFrame(
            {
                "year": [2024],
                "inpatient_spend": [10.0],
                "outpatient_spend": [5.0],
                "snf_spend": [0.0],
                "hospice_spend": [0.0],
                "home_health_spend": [0.0],
                "part_b_carrier_spend": [0.0],
                "pharmacy_spend": [3.0],
                "total_spend": [18.0],
                "ip_admissions": [1],
                "er_visits": [0],
                "em_visits": [2],
                "awv_visits": [1],
                "pharmacy_claims_count": [4],
            }
        )
        panel.spend_util_panel(df)
        # Two tables (spend, util)
        assert panel._mo.ui.table.call_count == 2


class TestClaimLinesPanel:
    @pytest.mark.unit
    def test_empty_both(self, panel):
        panel.claim_lines_panel(None, None)
        # Two callouts (medical missing + pharmacy missing)
        assert panel._mo.callout.call_count == 2

    @pytest.mark.unit
    def test_medical_only(self, panel):
        df = pl.DataFrame({"a": [1]})
        panel.claim_lines_panel(df, None)
        assert panel._mo.callout.call_count == 1
        panel._mo.ui.table.assert_called()

    @pytest.mark.unit
    def test_pharmacy_only(self, panel):
        df = pl.DataFrame({"a": [1]})
        panel.claim_lines_panel(None, df)
        assert panel._mo.callout.call_count == 1


class TestEligibilityPanel:
    @pytest.mark.unit
    def test_no_member_ids(self, panel):
        panel.eligibility_panel(None, [])
        panel._mo.callout.assert_called_once()
        assert "Member IDs" in panel._mo.callout.call_args.args[0]

    @pytest.mark.unit
    def test_no_records(self, panel):
        panel.eligibility_panel(None, ["A"])
        panel._mo.callout.assert_called_once()
        assert "No eligibility records" in panel._mo.callout.call_args.args[0]

    @pytest.mark.unit
    def test_with_records(self, panel):
        df = pl.DataFrame({"member_id": ["A"], "plan": ["P"]})
        panel.eligibility_panel(df, ["A"])
        panel._mo.ui.table.assert_called_once()


def _medical_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "claim_id": ["c1", "c1", "c2"],
            "claim_type": ["inst", "inst", "prof"],
            "claim_start_date": [date(2024, 1, 1), date(2024, 2, 1), date(2024, 3, 1)],
            "paid_amount": [10.0, 20.0, 30.0],
            "allowed_amount": [12.0, 22.0, 33.0],
            "charge_amount": [15.0, 25.0, 40.0],
            "member_id": ["M1", "M1", "M2"],
            "rendering_npi": ["NPI1", None, "NPI2"],
            "place_of_service_code": ["11", "11", None],
            "hcpcs_code": ["99213", None, "99214"],
            "diagnosis_code_1": ["D1", "D1", "D2"],
            "diagnosis_code_2": [None, "D3", None],
            "diagnosis_code_3": [None, None, None],
        }
    )


def _pharmacy_df() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "claim_id": ["r1", "r1", "r2"],
            "ndc_code": ["N1", "N1", "N2"],
            "member_id": ["M1", "M1", "M2"],
            "prescribing_provider_npi": ["P1", None, "P2"],
            "dispensing_provider_npi": ["D1", None, "D2"],
            "paid_amount": [3.0, 5.0, 7.0],
            "allowed_amount": [3.0, 5.0, 7.0],
            "quantity": [30, 30, 60],
            "days_supply": [30, 30, 60],
            "copayment_amount": [1.0, 0.0, 2.0],
            "deductible_amount": [0.0, 0.0, 1.0],
        }
    )


class TestClaimsSummaryPanel:
    @pytest.mark.unit
    def test_medical_empty(self, panel):
        panel.claims_summary_panel(None, "medical")
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_pharmacy_empty(self, panel):
        panel.claims_summary_panel(None, "pharmacy")
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_medical_renders_cards_and_accordion(self, panel):
        panel.claims_summary_panel(_medical_df(), "medical")
        panel._mo.vstack.assert_called_once()
        panel._mo.accordion.assert_called_once()

    @pytest.mark.unit
    def test_pharmacy_renders_cards_and_accordion(self, panel):
        panel.claims_summary_panel(_pharmacy_df(), "pharmacy")
        panel._mo.vstack.assert_called_once()
        panel._mo.accordion.assert_called_once()


class TestClaimsDetailPanel:
    @pytest.mark.unit
    def test_medical_empty(self, panel):
        panel.claims_detail_panel(None, "medical")
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_pharmacy_empty(self, panel):
        panel.claims_detail_panel(None, "pharmacy")
        panel._mo.callout.assert_called_once()

    @pytest.mark.unit
    def test_medical_renders(self, panel):
        panel.claims_detail_panel(_medical_df(), "medical")
        # accordion + line-table
        panel._mo.accordion.assert_called_once()
        assert panel._mo.ui.table.call_count >= 5  # 4 inside accordion + 1 line table

    @pytest.mark.unit
    def test_pharmacy_renders(self, panel):
        panel.claims_detail_panel(_pharmacy_df(), "pharmacy")
        panel._mo.accordion.assert_called_once()
        assert panel._mo.ui.table.call_count >= 5

    @pytest.mark.unit
    def test_medical_no_diagnosis_columns(self, panel):
        df = _medical_df().drop(["diagnosis_code_1", "diagnosis_code_2", "diagnosis_code_3"])
        panel.claims_detail_panel(df, "medical")
        panel._mo.accordion.assert_called_once()
        # No diagnosis frame → "No diagnosis codes found" markdown branch
        accordion_dict = panel._mo.accordion.call_args.args[0]
        assert "📊 Top Diagnoses (by frequency)" in accordion_dict
