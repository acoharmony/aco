# © 2025 HarmonyCares
# All rights reserved.

"""
Centralized field validators for Pydantic dataclass models.

 reusable validator functions and decorators for
common healthcare data patterns (MBI, NPI, TIN, ICD codes, etc.).

The validators are designed to be used with Pydantic dataclasses and provide
consistent validation logic across all table models.
"""

from .field_validators import (
    # Field factories (drop-in Field replacements with baked-in patterns)
    CPT,
    DRG,
    HCPCS,
    HICN,
    ICD9,
    ICD10,
    MBI,
    NDC,
    NPI,
    REV,
    TIN,
    # Pattern registry
    VALIDATION_PATTERNS,
    ZIP5,
    ZIP9,
    cpt_validator,
    # Date formats
    date_yyyymmdd_validator,
    drg_validator,
    hcpcs_validator,
    hicn_validator,
    icd_9_validator,
    # Medical codes
    icd_10_validator,
    # Medicare identifiers
    mbi_validator,
    ndc_validator,
    npi_validator,
    # Validator factory
    pattern_validator,
    revenue_code_validator,
    tin_validator,
    # Geographic
    zip5_validator,
    zip9_validator,
)

__all__ = [
    # Field factories
    "MBI",
    "NPI",
    "TIN",
    "HICN",
    "ICD10",
    "ICD9",
    "CPT",
    "HCPCS",
    "NDC",
    "DRG",
    "REV",
    "ZIP5",
    "ZIP9",
    # Validator factory
    "pattern_validator",
    # Class-level validators
    "mbi_validator",
    "npi_validator",
    "tin_validator",
    "hicn_validator",
    "icd_10_validator",
    "icd_9_validator",
    "cpt_validator",
    "hcpcs_validator",
    "ndc_validator",
    "drg_validator",
    "revenue_code_validator",
    "zip5_validator",
    "zip9_validator",
    "date_yyyymmdd_validator",
    # Patterns
    "VALIDATION_PATTERNS",
]
