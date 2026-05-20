# © 2025 HarmonyCares
# All rights reserved.

"""
HCC risk score calculation surface.

Thin adapter over the vendored ``hccinfhir`` library
(``src/acoharmony/_depends/hccinfhir``). This module is the one place in
acoharmony that imports directly from the vendored code — every other
call site should go through the public functions defined here. That way:

1. If the vendored library gets replaced or forked, the blast radius is
   one file, not the whole codebase.
2. Project-wide conventions (Polars frames, point-in-time filtering,
   logging) stay encapsulated in one adapter instead of leaking
   hccinfhir's per-row Python API into our transforms.
3. Historical-coefficient loading (BNMR reconciliation for PY2023 /
   PY2024) can be wired in here without touching the engine itself —
   see the ``coefficients_filename`` and companion data-file arguments
   on :class:`HCCEngine`.

The engine is **stateless across calls**. Each invocation of
``score_patient`` runs the vendored pipeline end-to-end. Construction
does load the coefficient CSVs into memory, so callers that score many
patients should reuse a single engine instance instead of spinning one
up per call.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from ._depends.hccinfhir import Demographics, HCCInFHIR, RAFResult

# Public type aliases so callers never have to import from _depends directly.
ModelName = Literal[
    "CMS-HCC Model V22",
    "CMS-HCC Model V24",
    "CMS-HCC Model V28",
    "CMS-HCC ESRD Model V21",
    "CMS-HCC ESRD Model V24",
]


@dataclass(frozen=True)
class RiskScore:
    """
    Typed result of a single-patient risk score calculation.

    Attributes:
        risk_score: The total RAF (risk adjustment factor) — the number
            that gets multiplied into capitation calculations.
        hcc_list: Active HCC category labels after hierarchies, edits,
            and interactions have been applied. Order is not guaranteed.
        cc_to_dx: Mapping from each active HCC to the set of diagnosis
            codes that contributed to it. Useful for diagnostic auditing
            when a reconciliation bucket drifts.
    """

    risk_score: float
    hcc_list: tuple[str, ...]
    cc_to_dx: dict[str, frozenset[str]]

    @classmethod
    def _from_raf(cls, raf: RAFResult) -> "RiskScore":
        """Adapt a vendored ``RAFResult`` into the acoharmony-typed shape."""
        return cls(
            risk_score=float(raf.risk_score),
            hcc_list=tuple(raf.hcc_list),
            cc_to_dx={k: frozenset(v) for k, v in raf.cc_to_dx.items()},
        )


class HCCEngine:
    """
    Per-model HCC scoring engine backed by vendored hccinfhir.

    Each engine instance loads its coefficient tables once at construction.
    To score many patients against the same model (the common case for
    reconciliation work) construct one engine and reuse it across calls.

    Args:
        model_name: CMS HCC model to use. Defaults to V28 (current CMS
            Medicare Advantage model as of PY2025).
        coefficients_filename: Override the default 2026 coefficients
            CSV with a historical table (e.g. for PY2023 / PY2024
            reconciliation). Must be a path string the vendored engine
            can resolve via its internal loader. When ``None``, uses the
            default data file shipped with the vendored hccinfhir copy.
        dx_cc_mapping_filename: Override the default diagnosis-to-HCC
            mapping table. Used alongside ``coefficients_filename`` when
            reconciling historical performance years whose mapping
            rules differed from the current CMS tables.
        hierarchies_filename: Override the default HCC hierarchies
            table. Less commonly customized than the coefficient /
            mapping pair but supported by the underlying engine.

    Notes:
        The vendored hccinfhir 0.3.3 ships reference data for PY2025
        and PY2026 only. Historical PY2023 / PY2024 reconciliation
        requires supplying explicit coefficient / mapping filenames —
        acquiring and vendoring those tables is a separate task
        (tracked in the BNMR reconciliation PR arc).
    """

    def __init__(
        self,
        model_name: ModelName = "CMS-HCC Model V28",
        *,
        coefficients_filename: str | None = None,
        dx_cc_mapping_filename: str | None = None,
        hierarchies_filename: str | None = None,
    ):
        kwargs: dict[str, str] = {}
        if coefficients_filename is not None:
            kwargs["coefficients_filename"] = coefficients_filename
        if dx_cc_mapping_filename is not None:
            kwargs["dx_cc_mapping_filename"] = dx_cc_mapping_filename
        if hierarchies_filename is not None:
            kwargs["hierarchies_filename"] = hierarchies_filename
        self._engine = HCCInFHIR(model_name=model_name, **kwargs)
        self._model_name: ModelName = model_name

    @property
    def model_name(self) -> ModelName:
        return self._model_name

    def score_patient(
        self,
        diagnosis_codes: list[str],
        *,
        age: int,
        sex: Literal["M", "F"],
        dual_elgbl_cd: str = "NA",
        orec: str = "0",
        crec: str = "0",
        disabled: bool = False,
        esrd: bool = False,
    ) -> RiskScore:
        """
        Compute a risk score for a single patient.

        Args:
            diagnosis_codes: ICD-10-CM diagnosis codes observed during the
                risk score collection period. Codes can include periods
                (``E11.9``) or not (``E119``); the vendored engine
                normalizes both.
            age: Patient age in years, as of the risk score attribution
                date (not today's date).
            sex: ``"M"`` or ``"F"`` (the CMS demographic categorizer
                only supports binary codes).
            dual_elgbl_cd: CMS dual-eligibility code. ``"NA"`` for
                non-dual, or one of the CMS dual codes (``"02"``,
                ``"04"``, ``"08"``, ``"NA"``, ...). Affects coefficient
                selection in models that split duals out.
            orec: Original Reason for Entitlement Code. ``"0"`` = aged,
                ``"1"`` = disability, ``"2"`` = ESRD, ``"3"`` =
                disability-then-ESRD.
            crec: Current Reason for Entitlement Code. Same values as
                ``orec``.
            disabled: Whether the patient is currently under-65 disabled.
            esrd: Whether the patient currently has ESRD.

        Returns:
            ``RiskScore`` with the total RAF, active HCC list, and the
            diagnosis → HCC attribution map.
        """
        demographics = Demographics(
            age=age,
            sex=sex,
            dual_elgbl_cd=dual_elgbl_cd,
            orec=orec,
            crec=crec,
            disabled=disabled,
            esrd=esrd,
        )
        raf = self._engine.calculate_from_diagnosis(
            diagnosis_codes=diagnosis_codes,
            demographics=demographics,
        )
        return RiskScore._from_raf(raf)
