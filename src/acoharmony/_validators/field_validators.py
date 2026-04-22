# © 2025 HarmonyCares
# All rights reserved.

"""
Reusable field validators using decorator patterns.

 a centralized registry of validation patterns and
factory functions to create field validators for Pydantic dataclasses.

"""

import re
from collections.abc import Callable

from pydantic import Field as _Field
from pydantic import field_validator

VALIDATION_PATTERNS = {
    # Medicare identifiers
    "mbi": {
        "pattern": r"^[1-9AC-HJ-NP-RT-Y][AC-HJ-NP-RT-Y\d]{10}$",
        "description": "Medicare Beneficiary Identifier (11 chars, specific format)",
        "examples": ["1AC2HJ3RT4Y"],
    },
    "npi": {
        "pattern": r"^\d{10}$",
        "description": "National Provider Identifier (10 digits)",
        "examples": ["1234567890"],
    },
    "tin": {
        "pattern": r"^\d{9}$",
        "description": "Tax Identification Number (9 digits)",
        "examples": ["123456789"],
    },
    # Medical codes
    "icd_10": {
        "pattern": r"^[A-TV-Z]\d{2}",
        "description": "ICD-10 diagnosis code (letter + 2+ digits)",
        "examples": ["A01", "Z9989"],
    },
    "icd_9": {
        "pattern": r"^\d{3}",
        "description": "ICD-9 diagnosis code (3+ digits)",
        "examples": ["001", "99999"],
    },
    "cpt": {
        "pattern": r"^\d{5}$",
        "description": "CPT procedure code (5 digits)",
        "examples": ["99213"],
    },
    "hcpcs": {
        "pattern": r"^[A-Z]\d{4}$",
        "description": "HCPCS procedure code (letter + 4 digits)",
        "examples": ["J1234"],
    },
    "ndc": {
        "pattern": r"^\d{5}-\d{4}-\d{2}$|^\d{11}$",
        "description": "National Drug Code (5-4-2 or 11 digits)",
        "examples": ["12345-6789-01", "12345678901"],
    },
    "drg": {
        "pattern": r"^\d{3}$",
        "description": "Diagnosis Related Group (3 digits)",
        "examples": ["001", "999"],
    },
    "revenue_code": {
        "pattern": r"^\d{4}$",
        "description": "Revenue center code (4 digits)",
        "examples": ["0450"],
    },
    "zip5": {
        "pattern": r"^\d{5}$",
        "description": "5-digit ZIP code",
        "examples": ["12345"],
    },
    "zip9": {
        "pattern": r"^\d{5}-?\d{4}$",
        "description": "9-digit ZIP code (with or without hyphen)",
        "examples": ["12345-6789", "123456789"],
    },
    "date_yyyymmdd": {
        "pattern": r"^\d{8}$",
        "description": "Date in YYYYMMDD format",
        "examples": ["20250101"],
    },
    "date_ccyymmdd": {
        "pattern": r"^\d{8}$",
        "description": "Date in CCYYMMDD format",
        "examples": ["20250101"],
    },
}


def pattern_validator(field_name: str, pattern_type: str) -> Callable:
    """
    Create a Pydantic field validator for a specific pattern type.

        This is the core factory function that creates reusable validators
        using decorator patterns and syntactic sugar.

        Args:
            field_name: Name of the field to validate
            pattern_type: Type of pattern from VALIDATION_PATTERNS registry

        Returns:
            Callable: Pydantic field_validator that can be attached to a dataclass

        Raises:
            KeyError: If pattern_type not in VALIDATION_PATTERNS
    """
    if pattern_type not in VALIDATION_PATTERNS:
        raise KeyError(
            f"Unknown pattern type: {pattern_type}. "
            f"Available: {', '.join(VALIDATION_PATTERNS.keys())}"
        )

    pattern_info = VALIDATION_PATTERNS[pattern_type]
    pattern = pattern_info["pattern"]
    description = pattern_info["description"]

    @field_validator(field_name, mode="after")
    @classmethod
    def validator(cls, v: str | None) -> str | None:
        """Validate field matches required pattern."""
        if v is None:
            return v

        # Allow empty strings for optional fields
        if v == "":
            return v

        # Validate against pattern
        if not re.match(pattern, v):
            raise ValueError(
                f"Invalid {pattern_type} format for {field_name}: '{v}'. Expected: {description}"
            )

        return v

    # Set a descriptive name for the validator
    validator.__name__ = f"validate_{field_name}_{pattern_type}"
    validator.__doc__ = f"Validate {field_name} matches {description}"

    return validator


def mbi_validator(field_name: str) -> Callable:
    """
    Create a validator for Medicare Beneficiary Identifier fields.

        Validates 11-character MBI format: position 1 is 1-9/A/C-H/J-N/P-R/T-Y,
        positions 2-11 are A/C-H/J-N/P-R/T-Y/0-9.

        Args:
            field_name: Name of the MBI field

        Returns:
            Pydantic field_validator for MBI format

    """
    return pattern_validator(field_name, "mbi")


def npi_validator(field_name: str) -> Callable:
    """
    Create a validator for National Provider Identifier fields.

        Validates 10-digit NPI format.

        Args:
            field_name: Name of the NPI field

        Returns:
            Pydantic field_validator for NPI format
    """
    return pattern_validator(field_name, "npi")


def tin_validator(field_name: str) -> Callable:
    """
    Create a validator for Tax Identification Number fields.

        Validates 9-digit TIN format.

        Args:
            field_name: Name of the TIN field

        Returns:
            Pydantic field_validator for TIN format
    """
    return pattern_validator(field_name, "tin")


def icd_10_validator(field_name: str) -> Callable:
    """
    Create a validator for ICD-10 diagnosis code fields.

        Validates ICD-10 format: letter (A-T, V-Z) followed by 2+ digits.

        Args:
            field_name: Name of the ICD-10 field

        Returns:
            Pydantic field_validator for ICD-10 format
    """
    return pattern_validator(field_name, "icd_10")


def icd_9_validator(field_name: str) -> Callable:
    """
    Create a validator for ICD-9 diagnosis code fields.

        Validates ICD-9 format: 3+ digits.

        Args:
            field_name: Name of the ICD-9 field

        Returns:
            Pydantic field_validator for ICD-9 format
    """
    return pattern_validator(field_name, "icd_9")


def cpt_validator(field_name: str) -> Callable:
    """
    Create a validator for CPT procedure code fields.

        Validates 5-digit CPT format.

        Args:
            field_name: Name of the CPT field

        Returns:
            Pydantic field_validator for CPT format
    """
    return pattern_validator(field_name, "cpt")


def hcpcs_validator(field_name: str) -> Callable:
    """
    Create a validator for HCPCS procedure code fields.

        Validates HCPCS format: letter followed by 4 digits.

        Args:
            field_name: Name of the HCPCS field

        Returns:
            Pydantic field_validator for HCPCS format
    """
    return pattern_validator(field_name, "hcpcs")


def ndc_validator(field_name: str) -> Callable:
    """
    Create a validator for National Drug Code fields.

        Validates NDC format: 5-4-2 (with hyphens) or 11 digits (no hyphens).

        Args:
            field_name: Name of the NDC field

        Returns:
            Pydantic field_validator for NDC format
    """
    return pattern_validator(field_name, "ndc")


def drg_validator(field_name: str) -> Callable:
    """
    Create a validator for Diagnosis Related Group fields.

        Validates 3-digit DRG format.

        Args:
            field_name: Name of the DRG field

        Returns:
            Pydantic field_validator for DRG format
    """
    return pattern_validator(field_name, "drg")


def revenue_code_validator(field_name: str) -> Callable:
    """
    Create a validator for revenue center code fields.

        Validates 4-digit revenue code format.

        Args:
            field_name: Name of the revenue code field

        Returns:
            Pydantic field_validator for revenue code format
    """
    return pattern_validator(field_name, "revenue_code")


def zip5_validator(field_name: str) -> Callable:
    """
    Create a validator for 5-digit ZIP code fields.

        Validates 5-digit ZIP format.

        Args:
            field_name: Name of the ZIP code field

        Returns:
            Pydantic field_validator for 5-digit ZIP format
    """
    return pattern_validator(field_name, "zip5")


def zip9_validator(field_name: str) -> Callable:
    """
    Create a validator for 9-digit ZIP code fields.

        Validates 9-digit ZIP format (with or without hyphen).

        Args:
            field_name: Name of the ZIP code field

        Returns:
            Pydantic field_validator for 9-digit ZIP format
    """
    return pattern_validator(field_name, "zip9")


def date_yyyymmdd_validator(field_name: str) -> Callable:
    """
    Create a validator for YYYYMMDD date format fields.

        Validates 8-digit YYYYMMDD date format (common in CCLF files).

        Args:
            field_name: Name of the date field

        Returns:
            Pydantic field_validator for YYYYMMDD format
    """
    return pattern_validator(field_name, "date_yyyymmdd")


def get_validator_for_pattern(field_name: str, pattern_type: str) -> Callable:
    """
    Get a validator function for a specific pattern type.

        This is an alias for pattern_validator() for backward compatibility
        and explicit usage.

        Args:
            field_name: Name of the field to validate
            pattern_type: Type of pattern from VALIDATION_PATTERNS

        Returns:
            Pydantic field_validator
    """
    return pattern_validator(field_name, pattern_type)


# ---------------------------------------------------------------------------
# Field factories: drop-in replacements for Field() that bake in a pattern.
#
# Usage:
#   bene_mbi_id: str = MBI(description="Medicare Beneficiary Identifier")
#   npi_num: str | None = NPI(default=None, description="Provider NPI")
# ---------------------------------------------------------------------------


def _pattern_field(pattern_key: str, **kwargs):
    """Create a Pydantic Field with a baked-in validation pattern."""
    kwargs["pattern"] = VALIDATION_PATTERNS[pattern_key]["pattern"]
    return _Field(**kwargs)


def MBI(**kwargs):
    """Field with Medicare Beneficiary Identifier pattern."""
    return _pattern_field("mbi", **kwargs)


def NPI(**kwargs):
    """Field with National Provider Identifier (10-digit) pattern."""
    return _pattern_field("npi", **kwargs)


def TIN(**kwargs):
    """Field with Tax Identification Number (9-digit) pattern."""
    return _pattern_field("tin", **kwargs)


def ICD10(**kwargs):
    """Field with ICD-10 diagnosis code pattern."""
    return _pattern_field("icd_10", **kwargs)


def ICD9(**kwargs):
    """Field with ICD-9 diagnosis code pattern."""
    return _pattern_field("icd_9", **kwargs)


def CPT(**kwargs):
    """Field with CPT procedure code (5-digit) pattern."""
    return _pattern_field("cpt", **kwargs)


def HCPCS(**kwargs):
    """Field with HCPCS procedure code pattern."""
    return _pattern_field("hcpcs", **kwargs)


def NDC(**kwargs):
    """Field with National Drug Code pattern."""
    return _pattern_field("ndc", **kwargs)


def DRG(**kwargs):
    """Field with Diagnosis Related Group (3-digit) pattern."""
    return _pattern_field("drg", **kwargs)


def REV(**kwargs):
    """Field with Revenue center code (4-digit) pattern."""
    return _pattern_field("revenue_code", **kwargs)


def ZIP5(**kwargs):
    """Field with 5-digit ZIP code pattern."""
    return _pattern_field("zip5", **kwargs)


def ZIP9(**kwargs):
    """Field with 9-digit ZIP code pattern."""
    return _pattern_field("zip9", **kwargs)


def list_available_patterns() -> list[str]:
    """
    List all available validation patterns.

        Returns:
            List of pattern type names
    """
    return list(VALIDATION_PATTERNS.keys())


def get_pattern_info(pattern_type: str) -> dict:
    """
    Get detailed information about a validation pattern.

        Args:
            pattern_type: Type of pattern

        Returns:
            Dictionary with pattern, description, and examples

        Raises:
            KeyError: If pattern_type not found
    """
    return VALIDATION_PATTERNS[pattern_type].copy()
