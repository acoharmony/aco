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
            transform_config=getattr(cls, "_transform_config", None),
        )

        # Push any configs already attached by decorators that ran before us
        # (decorators apply bottom-up, so @with_* below @register_schema run first)
        _config_registry_map = {
            "_storage_config": "_storage",
            "_deduplication_config": "_deduplication",
            "_adr_config": "_adr",
            "_standardization_config": "_standardization",
            "_tuva_config": "_tuva",
            "_xref_config": "_xref",
            "_keys_config": "_keys",
            "_foreign_keys_config": "_foreign_keys",
            "_record_types_config": "_record_types",
            "_sheets_config": "_sheets",
            "_four_icli_config": "_four_icli",
            "_polars_config": "_polars",
            "_lineage_config": "_lineage",
        }
        for cls_attr, reg_attr in _config_registry_map.items():
            val = getattr(cls, cls_attr, None)
            if val is not None:
                getattr(SchemaRegistry, reg_attr)[name] = val

        staging_val = getattr(cls, "_staging_source", None)
        if staging_val is not None:
            SchemaRegistry._staging[name] = staging_val

        sources_val = getattr(cls, "_sources_config", None)
        if sources_val is not None:
            SchemaRegistry._sources[name] = sources_val

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


def with_transform(
    type: str | None = None,
    name: str | None = None,
    depends_on: list[str] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach transform configuration to a schema model.

        This decorator specifies how data should be transformed for this schema.
        It should be used in combination with @register_schema().

        Args:
            type: Transform type (deprecated - use name instead)
            name: Transform implementation name (references _transforms/{name}.py)
            depends_on: List of schema names this transform depends on
            **kwargs: Additional transform-specific configuration

        Returns:
            Decorator function

    """

    def decorator(cls: builtins.type[T]) -> builtins.type[T]:
        # Build transform config
        _transform_cfg = {}

        if type:
            _transform_cfg["type"] = type

        if name:
            _transform_cfg["name"] = name

        if depends_on:
            _transform_cfg["depends_on"] = depends_on

        # Add any additional kwargs
        _transform_cfg.update(kwargs)

        # Store on class
        cls._transform_config = _transform_cfg  # type: ignore

        # Add convenience method
        @classmethod
        def transform_config(cls_inner) -> dict[str, Any]:
            """Get transform configuration."""
            return cls_inner._transform_config.copy()  # type: ignore

        cls.transform_config = transform_config  # type: ignore

        # Update registry if already registered
        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._transforms[schema_name] = _transform_cfg

        return cls

    return decorator


def with_lineage(
    depends_on: list[str] | None = None, produces: list[str] | None = None, **kwargs: Any
) -> Callable[[type[T]], type[T]]:
    """
    Attach data lineage information to a schema model.

        This decorator specifies dependencies (upstream schemas) and outputs
        (downstream schemas) for data lineage tracking.

        Args:
            depends_on: List of schema names this schema depends on (upstream)
            produces: List of schema names this schema produces (downstream)
            **kwargs: Additional lineage metadata

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        # Build lineage config
        _lineage_cfg = {}

        if depends_on:
            _lineage_cfg["depends_on"] = depends_on

        if produces:
            _lineage_cfg["produces"] = produces

        # Add any additional kwargs
        _lineage_cfg.update(kwargs)

        # Store on class
        cls._lineage_config = _lineage_cfg  # type: ignore

        # Add convenience method
        @classmethod
        def lineage_config(cls_inner) -> dict[str, Any]:
            """Get lineage configuration."""
            return cls_inner._lineage_config.copy()  # type: ignore

        cls.lineage_config = lineage_config  # type: ignore

        # Update registry if already registered
        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._lineage[schema_name] = _lineage_cfg

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


def with_deduplication(
    key: list[str] | None = None,
    sort_by: list[str] | None = None,
    keep: str = "last",
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach deduplication configuration to a schema model.

        Args:
            key: Column(s) that form the deduplication key
            sort_by: Column(s) to sort by before deduplication
            keep: Which record to keep ("first" or "last")
            **kwargs: Additional deduplication config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _dedup_cfg: dict[str, Any] = {"keep": keep}

        if key is not None:
            _dedup_cfg["key"] = key

        if sort_by is not None:
            _dedup_cfg["sort_by"] = sort_by

        _dedup_cfg.update(kwargs)

        cls._deduplication_config = _dedup_cfg  # type: ignore

        @classmethod
        def deduplication_config(cls_inner) -> dict[str, Any]:
            """Get deduplication configuration."""
            return cls_inner._deduplication_config.copy()  # type: ignore

        cls.deduplication_config = deduplication_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._deduplication[schema_name] = _dedup_cfg

        return cls

    return decorator


def with_adr(
    adjustment_column: str | None = None,
    amount_fields: list[str] | None = None,
    key_columns: list[str] | None = None,
    sort_columns: list[str] | None = None,
    sort_descending: list[bool] | None = None,
    rank_by: list[str] | None = None,
    rank_partition: list[str] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach ADR (Adjustment, Deduplication, Ranking) configuration.

        Args:
            adjustment_column: Column containing adjustment type codes
            amount_fields: Columns containing monetary amounts
            key_columns: Columns for ADR deduplication key
            sort_columns: Columns for ADR sort order
            sort_descending: Sort direction per column
            rank_by: Columns to rank records by
            rank_partition: Columns to partition ranking by
            **kwargs: Additional ADR config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _adr_cfg: dict[str, Any] = {}

        if adjustment_column is not None:
            _adr_cfg["adjustment_column"] = adjustment_column
        if amount_fields is not None:
            _adr_cfg["amount_fields"] = amount_fields
        if key_columns is not None:
            _adr_cfg["key_columns"] = key_columns
        if sort_columns is not None:
            _adr_cfg["sort_columns"] = sort_columns
        if sort_descending is not None:
            _adr_cfg["sort_descending"] = sort_descending
        if rank_by is not None:
            _adr_cfg["rank_by"] = rank_by
        if rank_partition is not None:
            _adr_cfg["rank_partition"] = rank_partition

        _adr_cfg.update(kwargs)

        cls._adr_config = _adr_cfg  # type: ignore

        @classmethod
        def adr_config(cls_inner) -> dict[str, Any]:
            """Get ADR configuration."""
            return cls_inner._adr_config.copy()  # type: ignore

        cls.adr_config = adr_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._adr[schema_name] = _adr_cfg

        return cls

    return decorator


def with_standardization(
    rename_columns: dict[str, str] | None = None,
    add_columns: list[dict[str, str]] | None = None,
    add_computed: dict[str, str] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach standardization configuration to a schema model.

        Args:
            rename_columns: Mapping of old column names to new names
            add_columns: List of columns to add with static values
            add_computed: Mapping of column names to computation functions
            **kwargs: Additional standardization config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _std_cfg: dict[str, Any] = {}

        if rename_columns is not None:
            _std_cfg["rename_columns"] = rename_columns
        if add_columns is not None:
            _std_cfg["add_columns"] = add_columns
        if add_computed is not None:
            _std_cfg["add_computed"] = add_computed

        _std_cfg.update(kwargs)

        cls._standardization_config = _std_cfg  # type: ignore

        @classmethod
        def standardization_config(cls_inner) -> dict[str, Any]:
            """Get standardization configuration."""
            return cls_inner._standardization_config.copy()  # type: ignore

        cls.standardization_config = standardization_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._standardization[schema_name] = _std_cfg

        return cls

    return decorator


def with_tuva(
    models: dict[str, list[str]] | None = None,
    inject: list[str] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach Tuva Health integration configuration.

        Args:
            models: Dict of model categories to model name lists
                    (e.g., {"intermediate": ["int_enrollment"], "final": ["eligibility"]})
            inject: List of models to inject as Polars-native overrides
            **kwargs: Additional Tuva config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _tuva_cfg: dict[str, Any] = {}

        if models is not None:
            _tuva_cfg["models"] = models
        if inject is not None:
            _tuva_cfg["inject"] = inject

        _tuva_cfg.update(kwargs)

        cls._tuva_config = _tuva_cfg  # type: ignore

        @classmethod
        def tuva_config(cls_inner) -> dict[str, Any]:
            """Get Tuva Health integration configuration."""
            return cls_inner._tuva_config.copy()  # type: ignore

        cls.tuva_config = tuva_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._tuva[schema_name] = _tuva_cfg

        return cls

    return decorator


def with_xref(
    table: str | None = None,
    join_key: str | None = None,
    xref_key: str | None = None,
    current_column: str | None = None,
    output_column: str | None = None,
    description: str = "",
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach crosswalk/xref configuration to a schema model.

        Args:
            table: Reference table name for crosswalk
            join_key: Column in this table to join on
            xref_key: Column in reference table to match
            current_column: Column in reference table with current value
            output_column: Output column name for resolved value
            description: Description of the crosswalk
            **kwargs: Additional xref config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _xref_cfg: dict[str, Any] = {}

        if description:
            _xref_cfg["description"] = description
        if table is not None:
            _xref_cfg["table"] = table
        if join_key is not None:
            _xref_cfg["join_key"] = join_key
        if xref_key is not None:
            _xref_cfg["xref_key"] = xref_key
        if current_column is not None:
            _xref_cfg["current_column"] = current_column
        if output_column is not None:
            _xref_cfg["output_column"] = output_column

        _xref_cfg.update(kwargs)

        cls._xref_config = _xref_cfg  # type: ignore

        @classmethod
        def xref_config(cls_inner) -> dict[str, Any]:
            """Get crosswalk/xref configuration."""
            return cls_inner._xref_config.copy()  # type: ignore

        cls.xref_config = xref_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._xref[schema_name] = _xref_cfg

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


def with_keys(
    primary_key: list[str] | None = None,
    natural_key: list[str] | None = None,
    deduplication_key: list[str] | None = None,
    foreign_keys: list[dict[str, str]] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach key definitions to a schema model.

        Args:
            primary_key: Primary key columns
            natural_key: Natural/business key columns
            deduplication_key: Deduplication key columns
            foreign_keys: Foreign key references
            **kwargs: Additional key config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _keys_cfg: dict[str, Any] = {}

        if primary_key is not None:
            _keys_cfg["primary_key"] = primary_key
        if natural_key is not None:
            _keys_cfg["natural_key"] = natural_key
        if deduplication_key is not None:
            _keys_cfg["deduplication_key"] = deduplication_key
        if foreign_keys is not None:
            _keys_cfg["foreign_keys"] = foreign_keys

        _keys_cfg.update(kwargs)

        cls._keys_config = _keys_cfg  # type: ignore

        @classmethod
        def keys_config(cls_inner) -> dict[str, Any]:
            """Get key definitions."""
            return cls_inner._keys_config.copy()  # type: ignore

        cls.keys_config = keys_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._keys[schema_name] = _keys_cfg

        return cls

    return decorator


def with_foreign_keys(
    description: str = "",
    keys: list[dict[str, str]] | None = None,
    **kwargs: Any,
) -> Callable[[type[T]], type[T]]:
    """
    Attach foreign key relationship definitions.

        Args:
            description: Description of the relationships
            keys: List of foreign key definitions
            **kwargs: Additional config

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _fk_cfg: dict[str, Any] = {}

        if description:
            _fk_cfg["description"] = description
        if keys is not None:
            _fk_cfg["keys"] = keys

        _fk_cfg.update(kwargs)

        cls._foreign_keys_config = _fk_cfg  # type: ignore

        @classmethod
        def foreign_keys_config(cls_inner) -> dict[str, Any]:
            """Get foreign key definitions."""
            return cls_inner._foreign_keys_config.copy()  # type: ignore

        cls.foreign_keys_config = foreign_keys_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._foreign_keys[schema_name] = _fk_cfg

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
            "filePattern": file_pattern,
            "extractZip": extract_zip,
            "refreshFrequency": refresh_frequency,
        }

        if file_type_code is not None:
            _four_icli_cfg["fileTypeCode"] = file_type_code

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


def with_sources(*sources: str) -> Callable[[type[T]], type[T]]:
    """
    Attach data source declarations to a schema model.

        Args:
            *sources: Source names this schema draws from

        Returns:
            Decorator function

    """

    def decorator(cls: type[T]) -> type[T]:
        _sources_list = list(sources)

        cls._sources_config = _sources_list  # type: ignore

        @classmethod
        def sources_config(cls_inner) -> list[str]:
            """Get data sources."""
            return cls_inner._sources_config.copy()  # type: ignore

        cls.sources_config = sources_config  # type: ignore

        schema_name = getattr(cls, "_schema_metadata", {}).get("name")
        if schema_name:
            SchemaRegistry._sources[schema_name] = _sources_list

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
