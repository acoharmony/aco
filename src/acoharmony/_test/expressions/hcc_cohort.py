# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_cohort module."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import polars as pl
import pytest


class TestClassifyCohortScalar:
    @pytest.mark.unit
    @pytest.mark.parametrize("orec,expected", [
        ("0", "AD"),
        ("1", "AD"),
        ("2", "ESRD"),
        ("3", "ESRD"),
        (0, "AD"),
        (2, "ESRD"),
    ])
    def test_basic(self, orec, expected):
        assert classify_cohort(orec) == expected

    @pytest.mark.unit
    def test_null_orec_defaults_to_ad(self):
        assert classify_cohort(None) == "AD"

    @pytest.mark.unit
    def test_crec_preferred_over_orec(self):
        """When both are provided, CREC wins — current status trumps
        historical entitlement."""
        assert classify_cohort(orec="0", crec="2") == "ESRD"
        assert classify_cohort(orec="2", crec="0") == "AD"

    @pytest.mark.unit
    def test_out_of_range_value_defaults_to_ad(self):
        assert classify_cohort("9") == "AD"


class TestBuildAdVsEsrdExpr:
    @pytest.mark.unit
    def test_orec_only(self):
        df = pl.DataFrame({
            "original_reason_entitlement_code": ["0", "1", "2", "3", None, "7"],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr(crec_col=None).alias("cohort")
        )
        assert result["cohort"].to_list() == ["AD", "AD", "ESRD", "ESRD", "AD", "AD"]

    @pytest.mark.unit
    def test_crec_takes_precedence(self):
        """When CREC is populated it dominates OREC. Null CREC falls
        back to OREC."""
        df = pl.DataFrame({
            "original_reason_entitlement_code": ["0", "2", "0"],
            "medicare_status_code": ["2", None, "3"],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr().alias("cohort")
        )
        # Row 1: CREC=2 overrides OREC=0 → ESRD
        # Row 2: CREC=null, OREC=2 → ESRD
        # Row 3: CREC=3 (ESRD+DIB) → ESRD (NOT AD even though OREC=0)
        assert result["cohort"].to_list() == ["ESRD", "ESRD", "ESRD"]

    @pytest.mark.unit
    def test_numeric_orec_column_cast_works(self):
        """Integer-coded OREC classifies correctly via the String cast."""
        df = pl.DataFrame({
            "original_reason_entitlement_code": [0, 2, 3],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr(crec_col=None).alias("cohort")
        )
        assert result["cohort"].to_list() == ["AD", "ESRD", "ESRD"]


class TestEsrdEntitlementCodes:
    @pytest.mark.unit
    def test_codes_constant_matches_expectation(self):
        """OREC values 2 and 3 indicate ESRD status; 0 (OASI) and 1
        (DIB) do not. Locking this in per the constants documented in
        hccinfhir and the CMS OREC codebook."""
        assert ESRD_ENTITLEMENT_CODES == frozenset({"2", "3"})
