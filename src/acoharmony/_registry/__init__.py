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
    with_adr,
    with_deduplication,
    with_foreign_keys,
    with_four_icli,
    with_keys,
    with_lineage,
    with_metadata,
    with_parser,
    with_polars,
    with_record_types,
    with_sheets,
    with_sources,
    with_staging,
    with_standardization,
    with_storage,
    with_transform,
    with_tuva,
    with_xref,
)
from .registry import (
    SchemaRegistry,
    get_adr_for_schema,
    get_deduplication_for_schema,
    get_four_icli_for_schema,
    get_full_table_config,
    get_keys_for_schema,
    get_metadata_for_schema,
    get_parser_for_schema,
    get_record_types_for_schema,
    get_schema,
    get_sheets_for_schema,
    get_staging_for_schema,
    get_standardization_for_schema,
    get_storage_for_schema,
    get_transform_for_schema,
    get_tuva_for_schema,
    get_xref_for_schema,
    list_registered_schemas,
)

__all__ = [
    # Decorators
    "register_schema",
    "with_parser",
    "with_transform",
    "with_lineage",
    "with_metadata",
    "with_storage",
    "with_deduplication",
    "with_adr",
    "with_standardization",
    "with_tuva",
    "with_xref",
    "with_staging",
    "with_keys",
    "with_foreign_keys",
    "with_record_types",
    "with_sheets",
    "with_four_icli",
    "with_polars",
    "with_sources",
    # Registry access
    "SchemaRegistry",
    "get_schema",
    "get_parser_for_schema",
    "get_transform_for_schema",
    "get_metadata_for_schema",
    "get_storage_for_schema",
    "get_deduplication_for_schema",
    "get_adr_for_schema",
    "get_standardization_for_schema",
    "get_tuva_for_schema",
    "get_xref_for_schema",
    "get_staging_for_schema",
    "get_keys_for_schema",
    "get_record_types_for_schema",
    "get_sheets_for_schema",
    "get_four_icli_for_schema",
    "get_full_table_config",
    "list_registered_schemas",
    # Generic registries (Phase 4)
    "Registry",
    "TypeRegistry",
    "CallableRegistry",
]
