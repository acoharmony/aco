# © 2025 HarmonyCares
# All rights reserved.

"""
Decorator-based schema registration with syntactic sugar.

 decorators that register Pydantic models with
the schema registry and attach metadata dynamically.

"""

import builtins
from collections.abc import Callable
from typing import Any, TypeVar

from .registry import SchemaRegistry

T = TypeVar("T")


def register_schema(
    name: str,
    version: int | str = 1,
    tier: str = "bronze",
    description: str = "",
    file_patterns: dict[str, str | list[str]] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Register a Pydantic dataclass as a schema with metadata.

        This is the primary decorator for schema registration. It:
        - Registers the model in the global SchemaRegistry
        - Attaches metadata to the model class
        - Adds convenience methods for metadata access

        Args:
            name: Schema name (e.g., "cclf1", "consolidated_alignment")
            version: Schema version number or string
            tier: Medallion tier (bronze, silver, gold)
            description: Human-readable description
            file_patterns: File pattern mappings by source
            **kwargs: Additional metadata fields

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        # Build metadata dictionary
        metadata = {
            "name": name,
            "version": version,
            "tier": tier,
            "description": description,
        }

        # Add file patterns if provided
        if file_patterns is not None:
            metadata["file_patterns"] = file_patterns

        # Add any additional kwargs
        metadata.update(kwargs)

        # Store metadata on the class
        cls._schema_metadata = metadata  # type: ignore

        # Register with global registry
        SchemaRegistry.register(
            schema_name=name,
            model_class=cls,
            metadata=metadata,
            parser_config=getattr(cls, "_parser_config", None),
        )

        # Push any configs already attached by decorators that ran before us
        # (decorators apply bottom-up, so @with_* below @register_schema run first)
        _config_registry_map = {
            "_storage_config": "_storage",
            "_record_types_config": "_record_types",
            "_sheets_config": "_sheets",
            "_four_icli_config": "_four_icli",
            "_polars_config": "_polars",
        }
        for cls_attr, reg_attr in _config_registry_map.items():
            val = getattr(cls, cls_attr, None)
            if val is not None:
                getattr(SchemaRegistry, reg_attr)[name] = val

        staging_val = getattr(cls, "_staging_source", None)
        if staging_val is not None:
            SchemaRegistry._staging[name] = staging_val

        # Add convenience class methods for metadata access
        @classmethod
        def schema_name(cls_inner) -> str:
            """Get the schema name."""
            return name

        @classmethod
        def schema_metadata(cls_inner) -> dict[str, Any]:
            """Get complete schema metadata."""
            return cls_inner._schema_metadata.copy()  # type: ignore

        @classmethod
        def schema_version(cls_inner) -> int | str:
            """Get schema version."""
            return version

        @classmethod
        def schema_tier(cls_inner) -> str:
            """Get medallion tier (bronze, silver, gold)."""
            return tier

        @classmethod
        def schema_description(cls_inner) -> str:
            """Get schema description."""
            return description

        @classmethod
        def get_file_patterns(cls_inner) -> dict[str, str | list[str]]:
            """Get file patterns by source."""
            return file_patterns or {}

        # Attach methods to class
        cls.schema_name = schema_name  # type: ignore
        cls.schema_metadata = schema_metadata  # type: ignore
        cls.schema_version = schema_version  # type: ignore
        cls.schema_tier = schema_tier  # type: ignore
        cls.schema_description = schema_description  # type: ignore
        cls.get_file_patterns = get_file_patterns  # type: ignore

        return cls

    return decorator


def with_parser(
    type: str,
    delimiter: str | None = None,
    encoding: str = "utf-8",
    has_header: bool = False,
    embedded_transforms: bool = False,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach parser configuration to a schema model.

        This decorator specifies how files should be parsed for this schema.
        It should be used in combination with @register_schema().

        Args:
            type: Parser type (fixed_width, delimited, parquet, json, excel, etc.)
            delimiter: Field delimiter for delimited files
            encoding: File encoding
            has_header: Whether file has header row
            embedded_transforms: Whether parser applies transforms via YAML field configs
            **kwargs: Additional parser-specific configuration (can include 'transforms' dict)

        Returns:
            Decorator function

    """

    def decorator(cls: builtins.type[T]) -> builtins.type[T]:
        # Build parser config
        _parser_cfg = {
            "type": type,
            "encoding": encoding,
            "has_header": has_header,
            "embedded_transforms": embedded_transforms,
        }

        if delimiter is not None:
            _parser_cfg["delimiter"] = delimiter

        # Add any additional kwargs (including 'transforms' dict if provided)
        _parser_cfg.update(kwargs)

        # Store on class
        cls._parser_config = _parser_cfg  # type: ignore

        # Add convenience method
        @classmethod
        def parser_config(cls_inner) -> dict[str, Any]:
            """Get parser configuration."""
            return cls_inner._parser_config.copy()  # type: ignore

        cls.parser_config = parser_config  # type: ignore

        # Update registry if already registered
        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._parsers[schema_name] = _parser_cfg

        return cls

    return decorator


def with_storage(
    tier: str | None = None,
    file_patterns: dict[str, str | list[str]] | None = None,
    medallion_layer: str | None = None,
    unity_catalog: str = "main",
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach storage configuration to a schema model.

        Args:
            tier: Storage tier (bronze, silver, gold)
            file_patterns: File pattern mappings by source
            medallion_layer: Medallion architecture layer
            unity_catalog: Unity Catalog name
            **kwargs: Per-tier configs (silver={output_name:...}, gold={...}, tracking={...})

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _storage_cfg: dict[str, Any] = {"unity_catalog": unity_catalog}

        if tier is not None:
            _storage_cfg["tier"] = tier

        if file_patterns is not None:
            _storage_cfg["file_patterns"] = file_patterns

        if medallion_layer is not None:
            _storage_cfg["medallion_layer"] = medallion_layer

        _storage_cfg.update(kwargs)

        cls._storage_config = _storage_cfg  # type: ignore

        @classmethod
        def storage_config(cls_inner) -> dict[str, Any]:
            """Get storage configuration."""
            return cls_inner._storage_config.copy()  # type: ignore

        cls.storage_config = storage_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._storage[schema_name] = _storage_cfg

        return cls

    return decorator


def with_staging(source: str) -> Callable[[type[T]], type[T]]:
    """
    Declare that this schema inherits from a staging/parent table.

        Args:
            source: Name of the parent staging table

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        cls._staging_source = source  # type: ignore

        @classmethod
        def staging_source(cls_inner) -> str:
            """Get staging source table name."""
            return cls_inner._staging_source  # type: ignore

        cls.staging_source = staging_source  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._staging[schema_name] = source

        return cls

    return decorator


def with_record_types(
    record_types: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach multi-record type definitions for TPARC-style files.

        Args:
            record_types: Dict mapping record type keys to their column definitions
            **kwargs: Additional config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _record_types_cfg: dict[str, Any] = {}

        if record_types is not None:
            _record_types_cfg["record_types"] = record_types

        _record_types_cfg.update(kwargs)

        cls._record_types_config = _record_types_cfg  # type: ignore

        @classmethod
        def record_types_config(cls_inner) -> dict[str, Any]:
            """Get record types configuration."""
            return cls_inner._record_types_config.copy()  # type: ignore

        cls.record_types_config = record_types_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._record_types[schema_name] = _record_types_cfg

        return cls

    return decorator


def with_sheets(
    sheets: list[dict[str, Any]] | None = None,
    matrix_fields: list[dict[str, Any]] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach multi-sheet Excel and matrix field configuration.

        Args:
            sheets: List of sheet definitions
            matrix_fields: List of matrix field extraction configs
            **kwargs: Additional config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _sheets_cfg: dict[str, Any] = {}

        if sheets is not None:
            _sheets_cfg["sheets"] = sheets
        if matrix_fields is not None:
            _sheets_cfg["matrix_fields"] = matrix_fields

        _sheets_cfg.update(kwargs)

        cls._sheets_config = _sheets_cfg  # type: ignore

        @classmethod
        def sheets_config(cls_inner) -> dict[str, Any]:
            """Get sheets/matrix configuration."""
            return cls_inner._sheets_config.copy()  # type: ignore

        cls.sheets_config = sheets_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._sheets[schema_name] = _sheets_cfg

        return cls

    return decorator


def with_four_icli(
    category: str = "",
    file_type_code: int | None = None,
    file_pattern: str = "",
    extract_zip: bool = True,
    refresh_frequency: str = "weekly",
    default_date_filter: dict[str, Any] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach 4iCLI (CMS Datahub) integration configuration.

        Args:
            category: CMS file category
            file_type_code: Numeric file type identifier
            file_pattern: Glob pattern for file matching
            extract_zip: Whether to extract ZIP files
            refresh_frequency: How often data refreshes
            default_date_filter: Default date filter settings
            **kwargs: Additional 4iCLI config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _four_icli_cfg: dict[str, Any] = {
            "category": category,
            "fileTypeCode": file_type_code,
            "filePattern": file_pattern,
            "extractZip": extract_zip,
            "refreshFrequency": refresh_frequency,
        }

        if default_date_filter is not None:
            _four_icli_cfg["defaultDateFilter"] = default_date_filter

        _four_icli_cfg.update(kwargs)

        cls._four_icli_config = _four_icli_cfg  # type: ignore

        @classmethod
        def four_icli_config(cls_inner) -> dict[str, Any]:
            """Get 4iCLI configuration."""
            return cls_inner._four_icli_config.copy()  # type: ignore

        cls.four_icli_config = four_icli_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._four_icli[schema_name] = _four_icli_cfg

        return cls

    return decorator


def with_polars(
    lazy_evaluation: bool = True,
    drop_columns: list[str] | None = None,
    string_trim: bool = False,
    categorical_columns: list[str] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach Polars-specific processing configuration.

        Args:
            lazy_evaluation: Whether to use lazy evaluation
            drop_columns: Columns to drop during processing
            string_trim: Whether to trim string values
            categorical_columns: Columns to convert to categorical type
            **kwargs: Additional Polars config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _polars_cfg: dict[str, Any] = {
            "lazy_evaluation": lazy_evaluation,
            "string_trim": string_trim,
        }

        if drop_columns is not None:
            _polars_cfg["drop_columns"] = drop_columns
        if categorical_columns is not None:
            _polars_cfg["categorical_columns"] = categorical_columns

        _polars_cfg.update(kwargs)

        cls._polars_config = _polars_cfg  # type: ignore

        @classmethod
        def polars_config(cls_inner) -> dict[str, Any]:
            """Get Polars processing configuration."""
            return cls_inner._polars_config.copy()  # type: ignore

        cls.polars_config = polars_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._polars[schema_name] = _polars_cfg

        return cls

    return decorator


def with_metadata(**metadata: Any) -> Callable[[type[T]], type[T]]:
    """
    Attach additional metadata to a schema model.

        This decorator allows adding arbitrary metadata fields beyond
        the standard ones provided by @register_schema().

        Args:
            **metadata: Key-value pairs of metadata

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        # Get existing metadata or create new
        if not hasattr(cls, "_schema_metadata"):
            cls._schema_metadata = {}  # type: ignore

        # Update with new metadata
        cls._schema_metadata.update(metadata)  # type: ignore

        # Update registry if already registered
        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name and schema_name in SchemaRegistry._metadata:
            SchemaRegistry._metadata[schema_name].update(metadata)

        return cls

    return decorator
