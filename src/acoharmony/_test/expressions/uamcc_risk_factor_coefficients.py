# © 2025 HarmonyCares
# All rights reserved.

"""Smoke tests for expressions._uamcc_risk_factor_coefficients module."""

from __future__ import annotations

import pytest


class TestUamccCoefficients:
    """Verify that UAMCC_COEFFICIENTS loads and has expected structure."""

    @pytest.mark.unit
    def test_module_imports_and_list_exists(self):
        from acoharmony._expressions._uamcc_risk_factor_coefficients import (
            UAMCC_COEFFICIENTS,
            EXPECTED_COUNT,
        )

        assert isinstance(UAMCC_COEFFICIENTS, list)
        assert len(UAMCC_COEFFICIENTS) == EXPECTED_COUNT

    @pytest.mark.unit
    def test_expected_count_is_54(self):
        from acoharmony._expressions._uamcc_risk_factor_coefficients import EXPECTED_COUNT

        assert EXPECTED_COUNT == 54

    @pytest.mark.unit
    def test_each_row_has_required_keys(self):
        from acoharmony._expressions._uamcc_risk_factor_coefficients import UAMCC_COEFFICIENTS

        required_keys = {"factor", "prevalence_pct", "coefficient", "std_err", "p_value"}
        for row in UAMCC_COEFFICIENTS:
            assert required_keys == set(row.keys()), f"Row missing keys: {row}"

    @pytest.mark.unit
    def test_intercept_row_present(self):
        from acoharmony._expressions._uamcc_risk_factor_coefficients import UAMCC_COEFFICIENTS

        factors = [r["factor"] for r in UAMCC_COEFFICIENTS]
        assert "Intercept" in factors

    @pytest.mark.unit
    def test_intercept_has_numeric_coefficient(self):
        from acoharmony._expressions._uamcc_risk_factor_coefficients import UAMCC_COEFFICIENTS

        intercept = next(r for r in UAMCC_COEFFICIENTS if r["factor"] == "Intercept")
        assert isinstance(intercept["coefficient"], float)

    @pytest.mark.unit
    def test_reference_row_has_none_coefficient(self):
        """The reference age cell ('Age <70 (ref)') must have coefficient=None."""
        from acoharmony._expressions._uamcc_risk_factor_coefficients import UAMCC_COEFFICIENTS

        ref_rows = [r for r in UAMCC_COEFFICIENTS if "(ref)" in r["factor"]]
        assert len(ref_rows) >= 1
        for r in ref_rows:
            assert r["coefficient"] is None

    @pytest.mark.unit
    def test_p_values_are_strings(self):
        from acoharmony._expressions._uamcc_risk_factor_coefficients import UAMCC_COEFFICIENTS

        for row in UAMCC_COEFFICIENTS:
            assert isinstance(row["p_value"], str), f"p_value not str in: {row}"
