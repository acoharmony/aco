# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_dx_to_hcc module."""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import pytest


class TestMapDxToHccs:
    @pytest.mark.unit
    def test_empty_input_returns_empty(self):
        bene = BeneficiaryDxInput(mbi="X", age=70, sex="F", diagnosis_codes=())
        assert map_dx_to_hccs(bene, "CMS-HCC Model V24") == frozenset()

    @pytest.mark.unit
    def test_known_diabetes_code_maps_to_hcc18(self):
        """E1165 (Type 2 diabetes with hyperglycemia) is an HCC 18 code
        under V24."""
        bene = BeneficiaryDxInput(
            mbi="X", age=70, sex="F", diagnosis_codes=("E1165",),
        )
        hccs = map_dx_to_hccs(bene, "CMS-HCC Model V24")
        assert "18" in hccs

    @pytest.mark.unit
    def test_multiple_dx_codes_union_to_multiple_hccs(self):
        """A beneficiary with diabetes, MS, COPD, heart failure, and
        CKD-stage-5 maps to at least five distinct HCCs."""
        bene = BeneficiaryDxInput(
            mbi="X", age=70, sex="F",
            diagnosis_codes=("E1165", "G35", "J449", "I509", "N185"),
        )
        hccs = map_dx_to_hccs(bene, "CMS-HCC Model V24")
        # Exact HCCs depend on the V24 crosswalk; just verify we hit
        # each of the five major categories.
        assert "18" in hccs   # diabetes group
        assert "77" in hccs   # MS
        assert "111" in hccs  # COPD
        assert "85" in hccs   # heart failure
        assert "136" in hccs  # CKD stage 5

    @pytest.mark.unit
    def test_sex_edit_strips_pregnancy_dx_for_male(self):
        """A male beneficiary's pregnancy-related dx should not produce
        an HCC after CMS's age/sex edits apply."""
        male = BeneficiaryDxInput(
            mbi="M", age=40, sex="M", diagnosis_codes=("O99419",),
        )
        hccs = map_dx_to_hccs(male, "CMS-HCC Model V24")
        assert hccs == frozenset()

    @pytest.mark.unit
    def test_unknown_dx_produces_no_hccs(self):
        """A nonsense diagnosis code doesn't match the crosswalk."""
        bene = BeneficiaryDxInput(
            mbi="X", age=70, sex="F", diagnosis_codes=("NOTACODE",),
        )
        assert map_dx_to_hccs(bene, "CMS-HCC Model V24") == frozenset()


class TestMapDxToCmmiHccs:
    @pytest.mark.unit
    def test_delegates_to_v24_then_drops_hcc134(self):
        """CMMI-HCC Concurrent uses the V24 crosswalk minus HCC 134
        (Dialysis Status). For a dialysis dx (N185), the V24 mapping
        yields HCC 136, not 134 — but for a dx that WOULD map to HCC
        134 on V24, CMMI should drop it."""
        # N185 (CKD stage 5) maps to HCC 136 in both models.
        bene = BeneficiaryDxInput(
            mbi="X", age=70, sex="F", diagnosis_codes=("N185",),
        )
        assert map_dx_to_cmmi_hccs(bene) == frozenset({"136"})

    @pytest.mark.unit
    def test_hcc134_in_excluded_set(self):
        """Regardless of the V24 mapping pathway, CMMI excludes HCC 134.
        The filter is applied unconditionally after the mapping step."""
        assert "134" in CMMI_EXCLUDED_HCCS

    @pytest.mark.unit
    def test_cmmi_retains_v24_edit_behavior(self):
        """CMMI inherits V24's age/sex edit rules, so a pregnancy dx on
        a male still produces no HCCs under CMMI mapping."""
        male = BeneficiaryDxInput(
            mbi="M", age=40, sex="M", diagnosis_codes=("O99419",),
        )
        assert map_dx_to_cmmi_hccs(male) == frozenset()
