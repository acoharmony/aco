# © 2025 HarmonyCares
# All rights reserved.

"""Smoke tests for expressions._quality_measure_risk_factor_coefficients module."""

from __future__ import annotations

import pytest


class TestQualityMeasureCoefficients:
    """Verify that ACR coefficient tables load and have expected structure."""

    @pytest.mark.unit
    def test_module_imports_and_all_tables_present(self):
        from acoharmony._expressions._quality_measure_risk_factor_coefficients import (
            ACR_MEDICINE_COEFFICIENTS,
            ACR_SURGERY_GYNECOLOGY_COEFFICIENTS,
            ACR_CARDIORESPIRATORY_COEFFICIENTS,
            ACR_CARDIOVASCULAR_COEFFICIENTS,
            ACR_NEUROLOGY_COEFFICIENTS,
            ACR_COEFFICIENTS_BY_COHORT,
            EXPECTED_COUNTS,
            TOTAL_COEFFICIENTS,
        )

        assert isinstance(ACR_MEDICINE_COEFFICIENTS, list)
        assert isinstance(ACR_SURGERY_GYNECOLOGY_COEFFICIENTS, list)
        assert isinstance(ACR_CARDIORESPIRATORY_COEFFICIENTS, list)
        assert isinstance(ACR_CARDIOVASCULAR_COEFFICIENTS, list)
        assert isinstance(ACR_NEUROLOGY_COEFFICIENTS, list)
        assert isinstance(ACR_COEFFICIENTS_BY_COHORT, dict)
        assert isinstance(EXPECTED_COUNTS, dict)
        assert isinstance(TOTAL_COEFFICIENTS, int)

    @pytest.mark.unit
    def test_expected_counts_match_actual_lengths(self):
        from acoharmony._expressions._quality_measure_risk_factor_coefficients import (
            ACR_COEFFICIENTS_BY_COHORT,
            EXPECTED_COUNTS,
        )

        for cohort, expected_len in EXPECTED_COUNTS.items():
            actual = ACR_COEFFICIENTS_BY_COHORT[cohort]
            assert len(actual) == expected_len, (
                f"Cohort {cohort!r}: expected {expected_len} rows, got {len(actual)}"
            )

    @pytest.mark.unit
    def test_total_coefficients_is_366(self):
        from acoharmony._expressions._quality_measure_risk_factor_coefficients import (
            TOTAL_COEFFICIENTS,
        )

        assert TOTAL_COEFFICIENTS == 366

    @pytest.mark.unit
    def test_each_row_has_required_keys(self):
        from acoharmony._expressions._quality_measure_risk_factor_coefficients import (
            ACR_COEFFICIENTS_BY_COHORT,
        )

        required_keys = {"factor", "prevalence_pct", "coefficient", "std_err", "p_value"}
        for cohort, rows in ACR_COEFFICIENTS_BY_COHORT.items():
            for row in rows:
                assert required_keys == set(row.keys()), (
                    f"Cohort {cohort!r} row missing keys: {row}"
                )

    @pytest.mark.unit
    def test_cohort_keys_match_expected_counts_keys(self):
        from acoharmony._expressions._quality_measure_risk_factor_coefficients import (
            ACR_COEFFICIENTS_BY_COHORT,
            EXPECTED_COUNTS,
        )

        assert set(ACR_COEFFICIENTS_BY_COHORT.keys()) == set(EXPECTED_COUNTS.keys())

    @pytest.mark.unit
    def test_p_values_are_strings(self):
        from acoharmony._expressions._quality_measure_risk_factor_coefficients import (
            ACR_COEFFICIENTS_BY_COHORT,
        )

        for cohort, rows in ACR_COEFFICIENTS_BY_COHORT.items():
            for row in rows:
                assert isinstance(row["p_value"], str), (
                    f"Cohort {cohort!r}: p_value not str in {row}"
                )
