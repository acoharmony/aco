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
    def test_crec_still_honored_for_esrd(self):
        """Legacy CREC parameter — when passed with an OREC-scheme value
        (2 or 3), it still marks ESRD. Pre-MSTAT callers used this."""
        assert classify_cohort(orec="0", crec="2") == "ESRD"

    @pytest.mark.unit
    def test_crec_does_not_veto_esrd_orec(self):
        """A later fix: CREC does NOT override an ESRD OREC back to AD.
        ESRD by either signal wins — this is the ``silent under-match``
        case prior logic would have hit when CREC was repurposed as a
        non-ESRD Medicare Status Code."""
        assert classify_cohort(orec="2", crec="0") == "ESRD"

    @pytest.mark.unit
    def test_out_of_range_value_defaults_to_ad(self):
        assert classify_cohort("9") == "AD"

    @pytest.mark.unit
    @pytest.mark.parametrize("mstat,expected", [
        ("11", "ESRD"),  # Aged + ESRD
        ("21", "ESRD"),  # Disabled + ESRD
        ("31", "ESRD"),  # ESRD only
        ("10", "AD"),    # Aged
        ("20", "AD"),    # Disabled
        ("NA", "AD"),
        (None, "AD"),
    ])
    def test_mstat_flags_esrd_when_orec_blank(self, mstat, expected):
        """The primary fix: MSTAT ∈ {11,21,31} classifies as ESRD even
        when OREC is blank or non-ESRD. This catches ~1,700 benes per
        roster silently missing criterion (b) under OREC-only logic."""
        assert classify_cohort(orec="", medicare_status_code=mstat) == expected

    @pytest.mark.unit
    def test_mstat_mixed_with_nonesrd_orec(self):
        """OREC='1' (disability only) + MSTAT='21' (disabled + ESRD) =
        ESRD. Either signal is sufficient."""
        assert classify_cohort(orec="1", medicare_status_code="21") == "ESRD"


class TestBuildAdVsEsrdExpr:
    @pytest.mark.unit
    def test_orec_only(self):
        df = pl.DataFrame({
            "original_reason_entitlement_code": ["0", "1", "2", "3", None, "7"],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr(mstat_col=None).alias("cohort")
        )
        assert result["cohort"].to_list() == ["AD", "AD", "ESRD", "ESRD", "AD", "AD"]

    @pytest.mark.unit
    def test_mstat_flags_esrd_when_orec_blank(self):
        """MSTAT ∈ {11,21,31} classifies as ESRD even when OREC is blank
        or non-ESRD — the core fix for silent ESRD under-matching."""
        df = pl.DataFrame({
            "original_reason_entitlement_code": ["0", "", "1", "0", None],
            "medicare_status_code":             ["10", "21", "11", "31", "20"],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr().alias("cohort")
        )
        # Row 1: OREC=0, MSTAT=10  → AD   (no ESRD signal)
        # Row 2: OREC=blank, MSTAT=21 → ESRD (silently misclassified)
        # Row 3: OREC=1, MSTAT=11  → ESRD (MSTAT catches aged + ESRD)
        # Row 4: OREC=0, MSTAT=31  → ESRD (MSTAT catches ESRD only)
        # Row 5: OREC=null, MSTAT=20 → AD
        assert result["cohort"].to_list() == ["AD", "ESRD", "ESRD", "ESRD", "AD"]

    @pytest.mark.unit
    def test_orec_and_mstat_are_or_not_coalesce(self):
        """Either signal is sufficient — an ESRD OREC with a non-ESRD
        MSTAT still classifies ESRD. Prior coalesce logic would have
        flipped it back to AD."""
        df = pl.DataFrame({
            "original_reason_entitlement_code": ["2", "3"],
            "medicare_status_code":             ["10", "20"],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr().alias("cohort")
        )
        assert result["cohort"].to_list() == ["ESRD", "ESRD"]

    @pytest.mark.unit
    def test_numeric_orec_column_cast_works(self):
        """Integer-coded OREC classifies correctly via the String cast."""
        df = pl.DataFrame({
            "original_reason_entitlement_code": [0, 2, 3],
        })
        result = df.with_columns(
            build_ad_vs_esrd_expr(mstat_col=None).alias("cohort")
        )
        assert result["cohort"].to_list() == ["AD", "ESRD", "ESRD"]


class TestEsrdEntitlementCodes:
    @pytest.mark.unit
    def test_codes_constant_matches_expectation(self):
        """OREC values 2 and 3 indicate ESRD status; 0 (OASI) and 1
        (DIB) do not. Locking this in per the constants documented in
        hccinfhir and the CMS OREC codebook."""
        assert ESRD_ENTITLEMENT_CODES == frozenset({"2", "3"})

    @pytest.mark.unit
    def test_mstat_esrd_codes_constant(self):
        """MSTAT values 11 (Aged+ESRD), 21 (Disabled+ESRD), 31 (ESRD
        only) flag current ESRD status per the CCLF8 ``bene_mdcr_stus_cd``
        codebook."""
        from acoharmony._expressions._hcc_cohort import ESRD_MEDICARE_STATUS_CODES
        assert ESRD_MEDICARE_STATUS_CODES == frozenset({"11", "21", "31"})
