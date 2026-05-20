# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._hcc_cmmi_concurrent_coefficients module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import pytest


class TestCmmiConcurrent2023Counts:
    """Lock in the segment counts CMS states in Appendix B of the PY2023
    Risk Adjustment PDF. Any transcription drift breaks these."""

    @pytest.mark.unit
    def test_age_sex_has_24_cells(self):
        assert len(CMMI_CONCURRENT_2023_AGE_SEX) == 24

    @pytest.mark.unit
    def test_hcc_has_85_entries(self):
        assert len(CMMI_CONCURRENT_2023_HCC) == 85

    @pytest.mark.unit
    def test_post_kidney_has_four_indicators(self):
        assert len(CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT) == 4

    @pytest.mark.unit
    def test_payment_hcc_count_has_eleven_buckets(self):
        assert len(CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT) == 11

    @pytest.mark.unit
    def test_age_lt_65_interaction_has_four_terms(self):
        assert len(CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION) == 4

    @pytest.mark.unit
    def test_total_coefficients_is_128(self):
        assert TOTAL_COEFFICIENTS == 128

    @pytest.mark.unit
    def test_hcc134_is_absent(self):
        """HCC 134 Dialysis Status is deliberately excluded from the
        CMMI-HCC Concurrent model per Appendix B Modified Hierarchies
        note (PDF page 41). Its absence from the coefficient dict is
        load-bearing."""
        assert "134" not in CMMI_CONCURRENT_2023_HCC

    @pytest.mark.unit
    def test_all_hierarchy_codes_are_known_hccs(self):
        """Every HCC referenced in the hierarchy table must be a
        coefficient-bearing HCC."""
        all_hierarchy_codes = set(CMMI_CONCURRENT_2023_HIERARCHIES.keys()) | {
            code
            for subs in CMMI_CONCURRENT_2023_HIERARCHIES.values()
            for code in subs
        }
        assert all_hierarchy_codes.issubset(set(CMMI_CONCURRENT_2023_HCC.keys()))


class TestSpecificCoefficients:
    """Lock in a handful of published values as spot-checks against
    future transcription drift."""

    @pytest.mark.unit
    def test_hiv_aids_coefficient(self):
        # HCC 1, HIV/AIDS — PY2023 Appendix B PDF page 38.
        assert CMMI_CONCURRENT_2023_HCC["1"] == 0.2847

    @pytest.mark.unit
    def test_metastatic_cancer_coefficient(self):
        # HCC 8 — highest-value common HCC, good anchor.
        assert CMMI_CONCURRENT_2023_HCC["8"] == 2.7247

    @pytest.mark.unit
    def test_respiratory_dependence_coefficient(self):
        # HCC 82 — the largest coefficient in the HCC table (4.4570).
        assert CMMI_CONCURRENT_2023_HCC["82"] == 4.4570

    @pytest.mark.unit
    def test_cerebral_palsy_is_zero(self):
        # HCC 74 — the only HCC with a published 0.0000 coefficient.
        # Keeping this test locks in the fact that CMS deliberately
        # zero-weights this HCC in the concurrent model.
        assert CMMI_CONCURRENT_2023_HCC["74"] == 0.0

    @pytest.mark.unit
    def test_f70_74_age_sex(self):
        assert CMMI_CONCURRENT_2023_AGE_SEX["F70_74"] == 0.1949

    @pytest.mark.unit
    def test_m95_gt_age_sex(self):
        assert CMMI_CONCURRENT_2023_AGE_SEX["M95_GT"] == 0.2279

    @pytest.mark.unit
    def test_count_of_hccs_gte_15(self):
        # Largest count bucket — good sanity anchor.
        assert CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT[">=15"] == 5.2582

    @pytest.mark.unit
    def test_cystic_fibrosis_age_lt_65_interaction(self):
        # HCC 110 × age<65 — highest-value interaction term.
        assert CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION["110"] == 1.2052


class TestHierarchyRules:
    """Spot-check the hierarchy rules from Table A-1 of the PY2023 PDF."""

    @pytest.mark.unit
    def test_cancer_hierarchy_dominant_drops_all_subordinates(self):
        # HCC 8 (Metastatic) dominates HCCs 9, 10, 11, 12.
        assert CMMI_CONCURRENT_2023_HIERARCHIES["8"] == ("9", "10", "11", "12")

    @pytest.mark.unit
    def test_diabetes_hierarchy(self):
        assert CMMI_CONCURRENT_2023_HIERARCHIES["17"] == ("18", "19")
        assert CMMI_CONCURRENT_2023_HIERARCHIES["18"] == ("19",)

    @pytest.mark.unit
    def test_quadriplegia_paraplegia_cascade(self):
        # Per Table A-1, HCC 70 drops 71, 72, 103, 104, 169.
        assert CMMI_CONCURRENT_2023_HIERARCHIES["70"] == ("71", "72", "103", "104", "169")

    @pytest.mark.unit
    def test_hcc135_not_a_dominant_in_cmmi_model(self):
        """Per the Modified Hierarchies note: HCC 135 (Acute Renal
        Failure) is separated from the CKD hierarchy in the CMMI model
        — it should NOT drop HCCs 136, 137, 138 (unlike in the
        standard V24 CMS-HCC hierarchy)."""
        assert "135" not in CMMI_CONCURRENT_2023_HIERARCHIES


class TestPy2025Calibration:
    """Verify the PY2025 calibration — CMMI published the PY2025 Risk
    Adjustment paper on 2024-10-03. At transcription time, a
    segment-by-segment numeric diff showed ZERO value changes between
    PY2023 and PY2025 Table B-1. This test locks in that finding: the
    PY2025 dicts are aliases of the PY2023 dicts."""

    @pytest.mark.unit
    def test_py2025_age_sex_is_alias_of_py2023(self):
        assert CMMI_CONCURRENT_2025_AGE_SEX is CMMI_CONCURRENT_2023_AGE_SEX

    @pytest.mark.unit
    def test_py2025_hcc_is_alias_of_py2023(self):
        assert CMMI_CONCURRENT_2025_HCC is CMMI_CONCURRENT_2023_HCC

    @pytest.mark.unit
    def test_py2025_post_kidney_is_alias_of_py2023(self):
        assert (
            CMMI_CONCURRENT_2025_POST_KIDNEY_TRANSPLANT
            is CMMI_CONCURRENT_2023_POST_KIDNEY_TRANSPLANT
        )

    @pytest.mark.unit
    def test_py2025_count_is_alias_of_py2023(self):
        assert (
            CMMI_CONCURRENT_2025_PAYMENT_HCC_COUNT
            is CMMI_CONCURRENT_2023_PAYMENT_HCC_COUNT
        )

    @pytest.mark.unit
    def test_py2025_age_lt_65_is_alias_of_py2023(self):
        assert (
            CMMI_CONCURRENT_2025_HCC_AGE_LT_65_INTERACTION
            is CMMI_CONCURRENT_2023_HCC_AGE_LT_65_INTERACTION
        )


class TestCalibrationByPy:
    @pytest.mark.unit
    @pytest.mark.parametrize("py", [2023, 2024, 2025, 2026])
    def test_calibration_for_known_py(self, py):
        assert calibration_for_py(py) is CMMI_CONCURRENT_2023

    @pytest.mark.unit
    def test_fallback_for_unknown_py(self):
        assert calibration_for_py(2099) is CMMI_CONCURRENT_2023
