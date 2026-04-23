# © 2025 HarmonyCares
# All rights reserved.

"""
ICD-10 diagnosis → HCC mapping driver.

Wraps the vendored hccinfhir mapping so downstream scoring modules get
a uniform interface: give us a list of ICD-10 codes plus the target
model, get back the set of HCCs the beneficiary carries.

Both the CMS-HCC (V22/V24/V28) and CMS-HCC ESRD (V21/V24) models ship
with their own dx-to-HCC crosswalk inside hccinfhir; hccinfhir handles
those natively. The CMMI-HCC Concurrent model does NOT ship its own
crosswalk in hccinfhir. Per the PY2023 Risk Adjustment Rev. 1.1 paper
(Appendix A, Table A-1 header and the parent section VI discussion),
the CMMI-HCC Concurrent model uses the SAME 86 V24 HCCs (minus
HCC 134) as the CMS-HCC V24 model. The only CMMI-specific parts are
(a) the coefficients and (b) the modified hierarchy rules — both of
which live in ``_hcc_cmmi_concurrent_coefficients``.

So for the CMMI mapping step we reuse hccinfhir's V24 dx-to-CC crosswalk
directly, applying the CMMI-specific HCC-134 exclusion after the mapping
returns results. The age/sex edits that hccinfhir applies to V24 before
mapping (``model_edits.apply_edits``) are equally applicable to CMMI
since CMMI inherits V24's HCC definitions.

Source attribution
------------------

V24 dx→CC crosswalk: hccinfhir ``data/ra_dx_to_cc_2025.csv`` (10,137
mappings for ``CMS-HCC Model V24``). Same file carries V22, V28, and
ESRD V21/V24 mappings.

Age/sex edit rules: hccinfhir ``data/ra_dx_edits.csv``. These encode
CMS's ICD-10 edit set — e.g. sex-specific diagnoses that only map
when the beneficiary's sex matches the expected value for the code.

CMMI model-specific exclusions:

    HCC 134 Dialysis Status is excluded from the CMMI-HCC Concurrent
    model per the Modified Hierarchies note in the PY2023 Risk
    Adjustment PDF (page 41, ``$BRONZE/PY2023 ACO REACH KCC Risk
    Adjustment.txt`` line 1741).

We drop HCC 134 from the CMMI mapping output so callers don't have to.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Sequence

from acoharmony._depends.hccinfhir.defaults import (
    dx_to_cc_default,
    edits_default,
)
from acoharmony._depends.hccinfhir.model_dx_to_cc import apply_mapping
from acoharmony._depends.hccinfhir.model_edits import apply_edits


# HCCs that appear in the V24 dx→CC crosswalk but are NOT paid by the
# CMMI-HCC Concurrent model. Only HCC 134 today. Kept as a frozenset so
# future Modified Hierarchies notes can be added without touching the
# mapping logic.
CMMI_EXCLUDED_HCCS: frozenset[str] = frozenset({"134"})


@dataclass(frozen=True)
class BeneficiaryDxInput:
    """Inputs the mapper needs to apply age/sex edits correctly."""

    mbi: str
    age: int
    sex: str          # 'F' or 'M'
    diagnosis_codes: tuple[str, ...]


def map_dx_to_hccs(
    bene: BeneficiaryDxInput,
    model_name: str,
) -> frozenset[str]:
    """
    Return the set of HCCs a beneficiary carries under the given
    hccinfhir ``ModelName``.

    Steps:
        1. ``apply_mapping`` — returns ``{CC: {dx, ...}}`` (despite the
           misleading name, hccinfhir's output is keyed by CC, not by
           diagnosis).
        2. ``apply_edits`` — remove or override HCCs where CMS's age/sex
           edits apply (e.g., pregnancy diagnosis on a male beneficiary).
        3. Flatten the surviving CCs into a set.

    The result is a flat frozenset of HCC codes, suitable for passing
    directly to the CMS-HCC / CMMI driver as the ``hccs`` list.
    Hierarchies (HCC dominance) are NOT applied here — callers decide
    whether to apply the model-specific hierarchy (the CMS-HCC models
    apply theirs inside ``calculate_raf``; the CMMI-HCC driver calls
    ``apply_cmmi_hierarchy`` on the output of this function).
    """
    # Despite its name, hccinfhir's ``apply_mapping`` returns a
    # ``{CC: {dx, dx, ...}}`` dict — i.e. CCs are keys, diagnoses are
    # the values. Pass straight through to ``apply_edits`` which
    # expects that same shape.
    cc_to_dx = apply_mapping(
        diagnoses=list(bene.diagnosis_codes),
        model_name=model_name,  # type: ignore[arg-type]
        dx_to_cc_mapping=dx_to_cc_default,
    )

    edited = apply_edits(
        cc_to_dx=cc_to_dx,
        age=bene.age,
        sex=bene.sex,
        model_name=model_name,  # type: ignore[arg-type]
        edits_mapping=edits_default,
    )

    # Any CC with at least one surviving dx counts.
    return frozenset(cc for cc, dx_set in edited.items() if dx_set)


def map_dx_to_cmmi_hccs(bene: BeneficiaryDxInput) -> frozenset[str]:
    """
    Convenience: map ICD-10 codes to the HCCs used by the CMMI-HCC
    Concurrent model.

    Uses the V24 dx→CC crosswalk with V24 age/sex edits (since CMMI
    inherits V24's HCC definitions and edits), then drops the HCCs
    the CMMI model excludes (currently just HCC 134).
    """
    v24_hccs = map_dx_to_hccs(bene, model_name="CMS-HCC Model V24")
    return v24_hccs - CMMI_EXCLUDED_HCCS
