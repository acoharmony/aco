# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._mer_reconciliation."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._mer_reconciliation import MerReconciliationExpression


class TestClaimTypeLabelExpr:
    """Maps ``clm_type_cd`` → human-readable CMS claim-type label."""

    @pytest.mark.unit
    def test_all_known_codes_map_correctly(self):
        """Every code the MER ships with must have a stable label."""
        df = pl.DataFrame(
            {
                "clm_type_cd": ["10", "20", "30", "40", "50", "60", "71", "72", "81", "82"],
            }
        )
        result = df.with_columns(MerReconciliationExpression.claim_type_label_expr())
        labels = result["claim_type_label"].to_list()
        assert labels == [
            "HHA",
            "SNF",
            "SNF - Swing Beds",
            "Outpatient",
            "Hospice",
            "Inpatient",
            "Physician",
            "DME/Physician",
            "DMERC/non-DMEPOS",
            "DMERC/DMEPOS",
        ]

    @pytest.mark.unit
    def test_unknown_code_is_labeled_unknown(self):
        """A code not in the known set must be labeled explicitly, not nulled."""
        df = pl.DataFrame({"clm_type_cd": ["99", "XX", None]})
        result = df.with_columns(MerReconciliationExpression.claim_type_label_expr())
        labels = result["claim_type_label"].to_list()
        assert labels == ["Unknown (99)", "Unknown (XX)", "Unknown (null)"]


class TestClaimTypePartExpr:
    """Maps ``clm_type_cd`` → 'Part A' / 'Part B' per CMS MER convention."""

    @pytest.mark.unit
    def test_part_a_codes(self):
        """Inpatient, SNF, SNF-Swing, HHA, Hospice = Part A."""
        df = pl.DataFrame({"clm_type_cd": ["10", "20", "30", "50", "60"]})
        result = df.with_columns(MerReconciliationExpression.claim_type_part_expr())
        assert result["claim_type_part"].to_list() == ["Part A"] * 5

    @pytest.mark.unit
    def test_part_b_codes(self):
        """Outpatient (40), Physician (71), DME/Phys (72), DMERC (81/82) = Part B.

        Note: MER classifies Outpatient (40) as Part B, not Part A, which can
        surprise people used to the TOB 013x inpatient/outpatient split.
        """
        df = pl.DataFrame({"clm_type_cd": ["40", "71", "72", "81", "82"]})
        result = df.with_columns(MerReconciliationExpression.claim_type_part_expr())
        assert result["claim_type_part"].to_list() == ["Part B"] * 5

    @pytest.mark.unit
    def test_unknown_code_is_unknown_part(self):
        df = pl.DataFrame({"clm_type_cd": ["99", None]})
        result = df.with_columns(MerReconciliationExpression.claim_type_part_expr())
        assert result["claim_type_part"].to_list() == ["Unknown", "Unknown"]


class TestPbpmExpr:
    """Derives PBPM from net expenditure and eligible member-months."""

    @pytest.mark.unit
    def test_pbpm_happy_path(self):
        """PBPM = net_expenditure / eligible_member_months, rounded to 2 dp."""
        df = pl.DataFrame(
            {
                "net_expenditure": [10000.00, 5000.00, 12345.67],
                "eligible_member_months": [100, 50, 123],
            },
            schema={
                "net_expenditure": pl.Decimal(scale=2),
                "eligible_member_months": pl.Int64,
            },
        )
        result = df.with_columns(MerReconciliationExpression.pbpm_expr())
        vals = result["pbpm"].to_list()
        assert vals[0] == pytest.approx(100.0)
        assert vals[1] == pytest.approx(100.0)
        assert vals[2] == pytest.approx(100.37, abs=0.01)

    @pytest.mark.unit
    def test_pbpm_zero_member_months_is_null(self):
        """Division by zero is null, not a crash or inf."""
        df = pl.DataFrame(
            {
                "net_expenditure": [10000.00, 0.00],
                "eligible_member_months": [0, 0],
            },
            schema={
                "net_expenditure": pl.Decimal(scale=2),
                "eligible_member_months": pl.Int64,
            },
        )
        result = df.with_columns(MerReconciliationExpression.pbpm_expr())
        assert result["pbpm"].to_list() == [None, None]


class TestAsOfDeliveryFilter:
    """Point-in-time filter: keep only rows whose source file_date ≤ cutoff."""

    @pytest.mark.unit
    def test_keeps_rows_on_or_before_cutoff(self):
        """Rows whose file_date is strictly ≤ cutoff survive."""
        df = pl.DataFrame(
            {
                "file_date": ["2024-01-31", "2024-02-28", "2024-03-31", "2024-04-30"],
                "marker": ["a", "b", "c", "d"],
            }
        )
        result = df.filter(
            MerReconciliationExpression.as_of_delivery_filter("2024-02-28")
        )
        assert result["marker"].to_list() == ["a", "b"]

    @pytest.mark.unit
    def test_cutoff_before_all_rows_returns_empty(self):
        df = pl.DataFrame({"file_date": ["2024-01-31"], "marker": ["a"]})
        result = df.filter(
            MerReconciliationExpression.as_of_delivery_filter("2023-12-31")
        )
        assert result.height == 0

    @pytest.mark.unit
    def test_cutoff_after_all_rows_keeps_all(self):
        df = pl.DataFrame(
            {"file_date": ["2024-01-31", "2024-02-28"], "marker": ["a", "b"]}
        )
        result = df.filter(
            MerReconciliationExpression.as_of_delivery_filter("2099-12-31")
        )
        assert result["marker"].to_list() == ["a", "b"]


class TestSrvcMonthDateExpr:
    """Derives a proper first-of-month Date from (clndr_yr, clndr_mnth) strings."""

    @pytest.mark.unit
    def test_pads_single_digit_month(self):
        """clndr_mnth='1' must become 2024-01-01, not 2024-1-01."""
        df = pl.DataFrame({"clndr_yr": ["2024", "2024"], "clndr_mnth": ["1", "12"]})
        result = df.with_columns(MerReconciliationExpression.srvc_month_date_expr())
        dates = result["srvc_month_date"].to_list()
        from datetime import date

        assert dates == [date(2024, 1, 1), date(2024, 12, 1)]

    @pytest.mark.unit
    def test_already_padded_month_is_accepted(self):
        df = pl.DataFrame({"clndr_yr": ["2025"], "clndr_mnth": ["07"]})
        result = df.with_columns(MerReconciliationExpression.srvc_month_date_expr())
        from datetime import date

        assert result["srvc_month_date"][0] == date(2025, 7, 1)


class TestNetExpenditureExpr:
    """Aliases total_exp_amt_agg to net_expenditure for downstream use."""

    @pytest.mark.unit
    def test_uses_total_exp_amt_agg_directly(self):
        """CMS already nets the total in total_exp_amt_agg; don't recompute it.

        Reason: reproducing the component arithmetic (clm_pmt_amt_agg minus
        sqstr, apa_rdctn, pcc_rdctn, tcc_rdctn, apo_rdctn, etc) invites
        off-by-sign bugs that silently break reconciliation. Using the
        pre-netted CMS total means any diff exposed by the tie-out is
        genuinely upstream, not arithmetic drift here.
        """
        df = pl.DataFrame(
            {
                "total_exp_amt_agg": [12345.67, 0.0, -200.50],
                "clm_pmt_amt_agg": [99999.0, 99999.0, 99999.0],
            }
        )
        result = df.with_columns(
            MerReconciliationExpression.net_expenditure_expr()
        )
        vals = result["net_expenditure"].to_list()
        assert vals[0] == pytest.approx(12345.67)
        assert vals[1] == pytest.approx(0.0)
        assert vals[2] == pytest.approx(-200.50)
        # Must not be clm_pmt_amt_agg
        assert vals[0] != pytest.approx(99999.0)

    @pytest.mark.unit
    def test_null_total_exp_amt_agg_propagates_as_null(self):
        """A null in the CMS total stays null — no silent-zero substitution."""
        df = pl.DataFrame({"total_exp_amt_agg": [None, 42.00]})
        result = df.with_columns(
            MerReconciliationExpression.net_expenditure_expr()
        )
        assert result["net_expenditure"].to_list() == [None, pytest.approx(42.00)]


class TestProgramExpr:
    """Derives 'REACH' or 'MSSP' from aco_id prefix."""

    @pytest.mark.unit
    def test_reach_aco_id(self):
        """D-prefix ACO IDs are REACH participants."""
        df = pl.DataFrame({"aco_id": ["D0259", "D1234"]})
        result = df.with_columns(MerReconciliationExpression.program_expr())
        assert result["program"].to_list() == ["REACH", "REACH"]

    @pytest.mark.unit
    def test_mssp_aco_id(self):
        """A-prefix ACO IDs are MSSP participants."""
        df = pl.DataFrame({"aco_id": ["A5678", "A0001"]})
        result = df.with_columns(MerReconciliationExpression.program_expr())
        assert result["program"].to_list() == ["MSSP", "MSSP"]

    @pytest.mark.unit
    def test_unknown_prefix_is_unknown(self):
        df = pl.DataFrame({"aco_id": ["X9999", None]})
        result = df.with_columns(MerReconciliationExpression.program_expr())
        assert result["program"].to_list() == ["Unknown", "Unknown"]
