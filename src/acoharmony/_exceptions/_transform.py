# © 2025 HarmonyCares
# All rights reserved.

"""
Transform-related exceptions.

Exceptions for data transformation errors, schema mismatches,
and pipeline execution failures.
"""

from __future__ import annotations

from ._base import ACOHarmonyException
from ._registry import register_exception


@register_exception(
    error_code="TRANSFORM_001",
    category="transform",
    why_template="Data transformation failed due to data or schema issues",
    how_template="Verify input data matches schema definition and all required columns are present",
)
class TransformError(ACOHarmonyException):
    """Base class for transformation errors."""

    pass


@register_exception(
    error_code="TRANSFORM_002",
    category="transform",
    why_template="Transform schema is invalid or malformed",
    how_template="Check schema YAML syntax and required fields",
)
class TransformSchemaError(ACOHarmonyException):
    """Raised when transform schema is invalid."""

    pass


@register_exception(
    error_code="TRANSFORM_003",
    category="transform",
    why_template="Transform source data is missing or inaccessible",
    how_template="Verify source files exist in expected location",
)
class TransformSourceError(ACOHarmonyException):
    """Raised when transform source data cannot be accessed."""

    pass


@register_exception(
    error_code="TRANSFORM_004",
    category="transform",
    why_template="Transform output cannot be written",
    how_template="Check storage permissions and available disk space",
)
class TransformOutputError(ACOHarmonyException):
    """Raised when transform output cannot be written."""

    pass
