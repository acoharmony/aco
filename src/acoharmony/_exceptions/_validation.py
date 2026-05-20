# © 2025 HarmonyCares
# All rights reserved.

"""
Validation-related exceptions.

Exceptions for input validation failures, type checking errors,
and data quality issues detected before processing.
"""

from __future__ import annotations

from ._base import ACOHarmonyException
from ._registry import register_exception


@register_exception(
    error_code="VALIDATION_001",
    category="validation",
    why_template="Input validation failed due to type mismatch, missing data, or constraint violation",
    how_template="Check input parameters match expected types and required fields are provided",
)
class ValidationError(ACOHarmonyException):
    """
    Base class for validation errors.

        Raised when input validation fails before processing begins. This ensures
        fail-fast behavior with clear error messages rather than cryptic errors
        deep in execution.

        Missing columns:

        Schema validation:
    """

    pass


@register_exception(
    error_code="VALIDATION_002",
    category="validation",
    why_template="Required columns are missing from input DataFrame",
    how_template="Ensure input DataFrame has all required columns before processing",
)
class MissingColumnsError(ValidationError):
    """Raised when required columns are missing from a DataFrame."""

    pass


@register_exception(
    error_code="VALIDATION_003",
    category="validation",
    why_template="Input parameter has incorrect type",
    how_template="Pass correct type for the parameter",
)
class TypeValidationError(ValidationError):
    """Raised when input parameter has wrong type."""

    pass


@register_exception(
    error_code="VALIDATION_004",
    category="validation",
    why_template="Input DataFrame is empty",
    how_template="Ensure input data source has at least one row",
)
class EmptyDataError(ValidationError):
    """Raised when input DataFrame/LazyFrame is empty."""

    pass


@register_exception(
    error_code="VALIDATION_005",
    category="validation",
    why_template="File or directory path does not exist",
    how_template="Check the path is correct and file/directory exists",
)
class PathValidationError(ValidationError):
    """Raised when expected file or directory path does not exist."""

    pass


@register_exception(
    error_code="VALIDATION_006",
    category="validation",
    why_template="File format is not supported",
    how_template="Use one of the supported file formats",
)
class FileFormatValidationError(ValidationError):
    """Raised when file format is not supported."""

    pass
