# © 2025 HarmonyCares
# All rights reserved.

"""
Central schema registry for runtime schema discovery and metadata access.

This module maintains a global registry of all registered schemas with
their associated parsers, transforms, and metadata.
"""

from typing import Any


class SchemaRegistry:
    """
    Global registry for schema models and their metadata.

        This registry connects Pydantic dataclass models with:
        - Schema metadata (name, version, tier, description)
        - Parser configuration (type, delimiter, encoding, etc.)
        - File patterns and storage configuration

        The registry is populated automatically via decorators.
    """

    # Schema name -> model class
    _schemas: dict[str, type] = {}

    # Schema name -> metadata
    _metadata: dict[str, dict[str, Any]] = {}

    # Schema name -> parser config
    _parsers: dict[str, dict[str, Any]] = {}

    # Schema name -> storage config
    _storage: dict[str, dict[str, Any]] = {}

    # Schema name -> staging config
    _staging: dict[str, str] = {}

    # Schema name -> sheets config
    _sheets: dict[str, dict[str, Any]] = {}

    # Schema name -> fourIcli config
    _four_icli: dict[str, dict[str, Any]] = {}

    # Schema name -> polars config
    _polars: dict[str, dict[str, Any]] = {}

    # Schema name -> record_types config (TPARC multi-record support)
    _record_types: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(
        cls,
        schema_name: str,
        model_class: type,
        metadata: dict[str, Any],
        parser_config: dict[str, Any] | None = None,
    ) -> None:
        """
        Register a schema model with the registry.

                Args:
                    schema_name: Unique schema name
                    model_class: Pydantic dataclass model
                    metadata: Schema metadata (version, tier, description, etc.)
                    parser_config: Parser configuration
        """
        cls._schemas[schema_name] = model_class
        cls._metadata[schema_name] = metadata

        if parser_config:
            cls._parsers[schema_name] = parser_config

    @classmethod
    def get_schema(cls, schema_name: str) -> type | None:
        """
        Get the model class for a schema.

                Args:
                    schema_name: Schema name

                Returns:
                    Model class or None if not found
        """
        return cls._schemas.get(schema_name)

    @classmethod
    def get_metadata(cls, schema_name: str) -> dict[str, Any]:
        """
        Get metadata for a schema.

                Args:
                    schema_name: Schema name

                Returns:
                    Metadata dictionary
        """
        return cls._metadata.get(schema_name, {})

    @classmethod
    def get_parser_config(cls, schema_name: str) -> dict[str, Any]:
        """
        Get parser configuration for a schema.

                Args:
                    schema_name: Schema name

                Returns:
                    Parser config dictionary
        """
        return cls._parsers.get(schema_name, {})

    @classmethod
    def list_schemas(cls) -> list[str]:
        """
        List all registered schema names.

                Returns:
                    List of schema names
        """
        return list(cls._schemas.keys())

    @classmethod
    def list_schemas_by_tier(cls, tier: str) -> list[str]:
        """
        List schemas in a specific tier (bronze, silver, gold).

                Args:
                    tier: Medallion tier name

                Returns:
                    List of schema names in that tier
        """
        return [name for name, meta in cls._metadata.items() if meta.get("tier") == tier]

    @classmethod
    def list_schemas_by_parser(cls, parser_type: str) -> list[str]:
        """
        List schemas using a specific parser type.

                Args:
                    parser_type: Parser type (fixed_width, delimited, parquet, etc.)

                Returns:
                    List of schema names
        """
        return [name for name, config in cls._parsers.items() if config.get("type") == parser_type]

    @classmethod
    def get_storage_config(cls, schema_name: str) -> dict[str, Any]:
        """Get storage configuration for a schema."""
        return cls._storage.get(schema_name, {})

    @classmethod
    def get_staging_source(cls, schema_name: str) -> str | None:
        """Get staging source table name for a schema."""
        return cls._staging.get(schema_name)

    @classmethod
    def get_record_types_config(cls, schema_name: str) -> dict[str, Any]:
        """Get record types configuration for a schema (TPARC multi-record)."""
        return cls._record_types.get(schema_name, {})

    @classmethod
    def get_sheets_config(cls, schema_name: str) -> dict[str, Any]:
        """Get sheets/matrix configuration for a schema."""
        return cls._sheets.get(schema_name, {})

    @classmethod
    def get_four_icli_config(cls, schema_name: str) -> dict[str, Any]:
        """Get 4iCLI configuration for a schema."""
        return cls._four_icli.get(schema_name, {})

    @classmethod
    def get_polars_config(cls, schema_name: str) -> dict[str, Any]:
        """Get Polars processing configuration for a schema."""
        return cls._polars.get(schema_name, {})

    @classmethod
    def get_full_table_config(cls, schema_name: str) -> dict[str, Any]:
        """Get complete table configuration combining all registered metadata.

        Returns a dict assembled from all registered decorator data for this schema.
        """
        config: dict[str, Any] = {}

        meta = cls.get_metadata(schema_name)
        if meta:
            config.update(meta)

        parser = cls.get_parser_config(schema_name)
        if parser:
            config["file_format"] = parser

        storage = cls.get_storage_config(schema_name)
        if storage:
            config["storage"] = storage

        staging = cls.get_staging_source(schema_name)
        if staging:
            config["staging"] = staging

        record_types = cls.get_record_types_config(schema_name)
        if record_types:
            config["record_types"] = record_types.get("record_types")

        sheets = cls.get_sheets_config(schema_name)
        if sheets:
            config.update(sheets)

        four_icli = cls.get_four_icli_config(schema_name)
        if four_icli:
            config["fourIcli"] = four_icli

        polars = cls.get_polars_config(schema_name)
        if polars:
            config["polars"] = polars

        # Extract columns from the Pydantic model's fields
        model_cls = cls.get_schema(schema_name)
        if model_cls is not None and "columns" not in config:
            columns = cls._extract_columns(model_cls)
            if columns:
                config["columns"] = columns

        return config

    @classmethod
    def _extract_columns(cls, model_cls: type) -> list[dict[str, Any]]:
        """Extract column definitions from a Pydantic dataclass model."""
        import dataclasses

        columns: list[dict[str, Any]] = []
        if not dataclasses.is_dataclass(model_cls):
            return columns

        _type_map = {
            "str": "string",
            "int": "integer",
            "float": "float",
            "bool": "boolean",
            "date": "date",
            "Decimal": "decimal",
        }

        for dc_field in dataclasses.fields(model_cls):
            col: dict[str, Any] = {"name": dc_field.name}

            # Resolve data_type from annotation
            ann = (
                dc_field.type
                if isinstance(dc_field.type, str)
                else getattr(dc_field.type, "__name__", str(dc_field.type))
            )
            # Strip Optional/None union
            for py_type, schema_type in _type_map.items():
                if py_type in ann:
                    col["data_type"] = schema_type
                    break
            else:
                col["data_type"] = "string"

            # Extract description from Field metadata
            if dc_field.metadata:
                pydantic_field = dc_field.default
                if hasattr(pydantic_field, "description") and pydantic_field.description:
                    col["description"] = pydantic_field.description

            # Determine required vs optional
            default = dc_field.default
            if default is dataclasses.MISSING:
                col["required"] = True
            elif hasattr(default, "default"):
                if default.default is not None and default.default is not dataclasses.MISSING:
                    col["default"] = default.default
                if hasattr(default, "description") and default.description:
                    col["description"] = default.description

            # Extract fixed-width position and format metadata from json_schema_extra
            pydantic_field = dc_field.default
            extra = getattr(pydantic_field, "json_schema_extra", None)
            if extra and isinstance(extra, dict):
                for pos_key in ("start_pos", "end_pos", "length", "width", "date_format", "source_name"):
                    if pos_key in extra:
                        col[pos_key] = extra[pos_key]

            # Default date_format for date fields if not explicitly set
            if col.get("data_type") == "date" and "date_format" not in col:
                col["date_format"] = "%Y-%m-%d"

            columns.append(col)

        return columns

    @classmethod
    def clear(cls) -> None:
        """Clear all registered schemas (primarily for testing)."""
        cls._schemas.clear()
        cls._metadata.clear()
        cls._parsers.clear()
        cls._storage.clear()
        cls._staging.clear()
        cls._record_types.clear()
        cls._sheets.clear()
        cls._four_icli.clear()
        cls._polars.clear()


# Convenience functions for accessing the registry
def get_schema(schema_name: str) -> type | None:
    """Get model class for a schema."""
    return SchemaRegistry.get_schema(schema_name)


def list_registered_schemas() -> list[str]:
    """List all registered schema names."""
    return SchemaRegistry.list_schemas()


def get_full_table_config(schema_name: str) -> dict[str, Any]:
    """Get complete table configuration for a schema."""
    return SchemaRegistry.get_full_table_config(schema_name)
