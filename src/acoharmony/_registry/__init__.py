# © 2025 HarmonyCares
# All rights reserved.

"""
Centralized schema registry with decorator-based registration.

 a registry system that connects Pydantic models
with their parsers, transforms, and metadata through decorators.

"""

from .base import CallableRegistry, Registry, TypeRegistry
from .decorators import (
    register_schema,
    with_four_icli,
    with_metadata,
    with_parser,
    with_polars,
    with_record_types,
    with_sheets,
    with_storage,
)
from .registry import (
    SchemaRegistry,
    get_full_table_config,
    get_schema,
    list_registered_schemas,
)

__all__ = [
    # Decorators
    "register_schema",
    "with_parser",
    "with_metadata",
    "with_storage",
    "with_record_types",
    "with_sheets",
    "with_four_icli",
    "with_polars",
    # Registry access
    "SchemaRegistry",
    "get_schema",
    "get_full_table_config",
    "list_registered_schemas",
    # Generic registries (Phase 4)
    "Registry",
    "TypeRegistry",
    "CallableRegistry",
]
