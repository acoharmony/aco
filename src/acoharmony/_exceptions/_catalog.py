# © 2025 HarmonyCares
# All rights reserved.

"""
Catalog-related exceptions.

Exceptions for catalog operations, schema management,
and metadata handling.
"""

from __future__ import annotations

from ._base import ACOHarmonyException
from ._registry import register_exception


@register_exception(
    error_code="CATALOG_001",
    category="catalog",
    why_template="Catalog operation failed",
    how_template="Check catalog configuration and schema definitions",
)
class CatalogError(ACOHarmonyException):
    """Base class for catalog errors."""

    pass


@register_exception(
    error_code="CATALOG_002",
    category="catalog",
    why_template="Table not found in catalog",
    how_template="Verify table name and check available tables with list_tables()",
)
class TableNotFoundError(ACOHarmonyException):
    """Raised when table is not found in catalog."""

    pass


@register_exception(
    error_code="CATALOG_003",
    category="catalog",
    why_template="Schema registration failed",
    how_template="Check schema YAML is valid and all required fields are present",
)
class SchemaRegistrationError(ACOHarmonyException):
    """Raised when schema cannot be registered."""

    pass
