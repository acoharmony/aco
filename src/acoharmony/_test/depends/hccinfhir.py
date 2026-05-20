# © 2025 HarmonyCares
# All rights reserved.

"""
Vendored hccinfhir sanity tests.

These tests live alongside the vendored copy rather than as part of the
upstream test suite because we are verifying the **vendoring mechanics**
(import paths, data file resolution, stub replacements), not the
underlying HCC calculation math — that's upstream's responsibility.

If any of these break, either:
  1. Someone updated the vendored copy without reapplying the three
     mechanical patches documented in ``_depends/hccinfhir/VENDORING.md``,
  2. Someone moved the ``data/`` directory without updating
     ``utils.py``'s ``importlib.resources.path`` call, or
  3. Upstream shipped a new version with incompatible surface.

All three are scenarios the test should catch loudly.
"""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest


class TestImport:
    """The vendored package must be importable from its new location."""

    @pytest.mark.unit
    def test_top_level_import(self):
        """``from acoharmony._depends import hccinfhir`` works without error."""
        from acoharmony._depends import hccinfhir

        assert hccinfhir is not None

    @pytest.mark.unit
    def test_primary_symbols_exposed(self):
        """The surface we actually use is reachable from the vendored root."""
        from acoharmony._depends.hccinfhir import (
            Demographics,
            HCCInFHIR,
            RAFResult,
        )

        assert HCCInFHIR is not None
        assert Demographics is not None
        assert RAFResult is not None


class TestStubSamplesBehavior:
    """The sample API was replaced with stubs that fail loudly at call time.

    Import-time symbol presence is verified (because ``__init__.py`` still
    imports them), but any actual invocation must raise
    ``NotImplementedError`` pointing at the upstream repo.
    """

    @pytest.mark.unit
    def test_samples_symbol_exists(self):
        """``SampleData`` still importable so the package init succeeds."""
        from acoharmony._depends.hccinfhir import SampleData

        assert SampleData is not None

    @pytest.mark.unit
    def test_samples_method_raises_not_implemented(self):
        """Calling a sample method raises with a clear message."""
        from acoharmony._depends.hccinfhir import SampleData

        with pytest.raises(NotImplementedError, match="vendoring"):
            SampleData.get_eob_sample(1)

    @pytest.mark.unit
    def test_samples_function_raises_not_implemented(self):
        """Module-level convenience functions also raise."""
        from acoharmony._depends.hccinfhir import get_eob_sample

        with pytest.raises(NotImplementedError, match="vendoring"):
            get_eob_sample(1)


class TestCalculateFromDiagnosisV28:
    """A known V28 calculation must produce a deterministic score.

    This is the integration test that exercises the full vendored pipeline:
      - __init__.py import chain
      - utils.py data file loading (importlib.resources with the
        rewritten package path)
      - model_calculate → model_demographics → model_dx_to_cc →
        model_hierarchies → model_interactions → model_coefficients
      - Returns a typed RAFResult

    The expected score and HCC are baked from the vendored 2026 CMS
    coefficient table — if upstream ships a new coefficient CSV the
    expected values here will need to be updated (and that update
    should be a deliberate, reviewed PR since it changes reconciliation
    targets).
    """

    @pytest.mark.unit
    def test_81_year_old_male_with_e119_produces_hcc_38(self):
        """81yo M non-dual community with E11.9 → HCC 38 (Diabetes w/o Complications).

        Expected risk_score: 0.737 (V28 / 2026 coefficients).
        """
        from acoharmony._depends.hccinfhir import Demographics, HCCInFHIR

        engine = HCCInFHIR(model_name="CMS-HCC Model V28")
        result = engine.calculate_from_diagnosis(
            diagnosis_codes=["E119"],
            demographics=Demographics(
                age=81,
                sex="M",
                dual_elgbl_cd="NA",
                orec="0",
                crec="0",
                disabled=False,
                esrd=False,
            ),
        )

        assert result.hcc_list == ["38"]
        assert result.cc_to_dx == {"38": {"E119"}}
        # Tight tolerance — this is a published CMS coefficient lookup,
        # not a floating-point accumulation. If it drifts, something in
        # the coefficient table or mapping table changed and we need
        # to know.
        assert result.risk_score == pytest.approx(0.737, abs=0.001)
