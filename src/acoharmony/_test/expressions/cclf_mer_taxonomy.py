# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._cclf_mer_taxonomy."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._cclf_mer_taxonomy import CclfMerTaxonomyExpression


class TestMerClaimTypeFilter:
    """``mer_claim_type_filter`` keeps only rows with canonical MER codes."""

    @pytest.mark.unit
    def test_keeps_canonical_codes(self):
        """10/20/30/40/50/60/71/72/81/82 all survive."""
        df = pl.DataFrame(
            {
                "clm_type_cd": [
                    "10", "20", "30", "40", "50", "60", "71", "72", "81", "82"
                ],
                "marker": list("abcdefghij"),
            }
        )
        result = df.filter(CclfMerTaxonomyExpression.mer_claim_type_filter())
        assert result.height == 10
        assert set(result["marker"].to_list()) == set("abcdefghij")

    @pytest.mark.unit
    def test_drops_out_of_taxonomy_codes(self):
        """Stray CMS codes like 61 ("inpatient denied") do not belong in MER buckets."""
        df = pl.DataFrame(
            {
                "clm_type_cd": ["60", "61", "40", "99", None],
                "marker": ["a", "b", "c", "d", "e"],
            }
        )
        result = df.filter(CclfMerTaxonomyExpression.mer_claim_type_filter())
        assert result["marker"].to_list() == ["a", "c"]


class TestServiceYearMonthExpr:
    """``service_year_month_expr`` derives YYYYMM int from ``clm_from_dt``."""

    @pytest.mark.unit
    def test_extracts_year_month_from_date(self):
        """clm_from_dt is already a pl.Date; year*100+month = YYYYMM."""
        df = pl.DataFrame(
            {
                "clm_from_dt": [
                    date(2025, 1, 15),
                    date(2025, 3, 31),
                    date(2023, 12, 1),
                ],
            }
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.service_year_month_expr()
        )
        assert result["year_month"].to_list() == [202501, 202503, 202312]

    @pytest.mark.unit
    def test_null_date_propagates_as_null(self):
        """Missing date → null year_month, no crash."""
        df = pl.DataFrame(
            {"clm_from_dt": [None, date(2024, 6, 1)]},
            schema={"clm_from_dt": pl.Date},
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.service_year_month_expr()
        )
        assert result["year_month"].to_list() == [None, 202406]


class TestNetHeaderPaymentExpr:
    """``net_header_payment_expr`` flips sign on cancellations (CCLF1 only)."""

    @pytest.mark.unit
    def test_originals_and_adjustments_keep_sign(self):
        """Type '0' (original) and '2' (adjustment) stay positive."""
        df = pl.DataFrame(
            {
                "clm_adjsmt_type_cd": ["0", "2", "0"],
                "clm_pmt_amt": [100.0, 50.0, 200.0],
            }
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.net_header_payment_expr()
        )
        vals = result["net_payment"].to_list()
        assert vals == [pytest.approx(100.0), pytest.approx(50.0), pytest.approx(200.0)]

    @pytest.mark.unit
    def test_cancellations_are_negated(self):
        """Type '1' (cancellation) flips to negative per CCLF IG §5.3.1."""
        df = pl.DataFrame(
            {
                "clm_adjsmt_type_cd": ["1", "1"],
                "clm_pmt_amt": [75.0, 300.50],
            }
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.net_header_payment_expr()
        )
        vals = result["net_payment"].to_list()
        assert vals == [pytest.approx(-75.0), pytest.approx(-300.50)]

    @pytest.mark.unit
    def test_mixed_rows(self):
        df = pl.DataFrame(
            {
                "clm_adjsmt_type_cd": ["0", "1", "2", "0", "1"],
                "clm_pmt_amt": [10.0, 10.0, 10.0, 10.0, 10.0],
            }
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.net_header_payment_expr()
        )
        vals = result["net_payment"].to_list()
        assert vals == [
            pytest.approx(10.0),
            pytest.approx(-10.0),
            pytest.approx(10.0),
            pytest.approx(10.0),
            pytest.approx(-10.0),
        ]


class TestNetLinePaymentExpr:
    """``net_line_payment_expr`` does the same for CCLF5/6 line-level payments."""

    @pytest.mark.unit
    def test_originals_keep_sign(self):
        df = pl.DataFrame(
            {
                "clm_adjsmt_type_cd": ["0", "0"],
                "clm_line_cvrd_pd_amt": [25.0, 50.0],
            }
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.net_line_payment_expr()
        )
        assert result["net_payment"].to_list() == [
            pytest.approx(25.0),
            pytest.approx(50.0),
        ]

    @pytest.mark.unit
    def test_cancellations_are_negated(self):
        df = pl.DataFrame(
            {
                "clm_adjsmt_type_cd": ["1", "1"],
                "clm_line_cvrd_pd_amt": [25.0, 50.0],
            }
        )
        result = df.with_columns(
            CclfMerTaxonomyExpression.net_line_payment_expr()
        )
        assert result["net_payment"].to_list() == [
            pytest.approx(-25.0),
            pytest.approx(-50.0),
        ]


class TestAsOfDeliveryFilter:
    """Point-in-time filter on CCLF ``file_date``.

    Reuses the same semantics as MerReconciliationExpression.as_of_delivery_filter
    but lives here too so CCLF-side code does not have to reach across modules.
    """

    @pytest.mark.unit
    def test_keeps_rows_on_or_before_cutoff(self):
        df = pl.DataFrame(
            {
                "file_date": ["2024-01-31", "2024-02-28", "2024-03-31"],
                "marker": ["a", "b", "c"],
            }
        )
        result = df.filter(
            CclfMerTaxonomyExpression.as_of_delivery_filter("2024-02-28")
        )
        assert result["marker"].to_list() == ["a", "b"]

    @pytest.mark.unit
    def test_cutoff_before_all_drops_everything(self):
        df = pl.DataFrame(
            {"file_date": ["2024-05-01"], "marker": ["a"]}
        )
        result = df.filter(
            CclfMerTaxonomyExpression.as_of_delivery_filter("2024-04-30")
        )
        assert result.height == 0
