# © 2025 HarmonyCares
# All rights reserved.

"""
Data transformation utilities for parsed data.
"""

import logging
from typing import Any

import polars as pl

from ._date_handler import apply_date_parsing

logger = logging.getLogger(__name__)


def apply_column_types(lf: pl.LazyFrame, schema: Any) -> pl.LazyFrame:
    """
    Apply data type conversions based on schema definitions.

        This function converts column data types according to the schema,
        handling type casting, date parsing, and numeric conversions.

        Args:
            lf: LazyFrame with raw parsed data
            schema: TableMetadata with column type definitions

        Returns:
            pl.LazyFrame: Data with proper column types

        Type Mappings:
            - str/string: UTF-8 string
            - int/integer/int64: 64-bit integer
            - float/float64/decimal: 64-bit float
            - date: Date type (kept as string for transform handling)
            - boolean: Boolean

        Features:
            - Safe type casting with error handling
            - Null value preservation
            - Custom type conversion rules
    """
    columns = schema.columns if hasattr(schema, "columns") else []

    if not columns:
        return lf

    existing_cols = lf.collect_schema().names()

    for col_def in columns:
        col_name = col_def.get("output_name", col_def.get("name"))
        if col_name not in existing_cols:
            continue

        data_type = col_def.get("data_type", col_def.get("dtype", "string")).lower()

        if data_type in ["int", "integer", "int64"]:
            lf = lf.with_columns(pl.col(col_name).cast(pl.Int64, strict=False))
        elif data_type in ["float", "float64", "decimal"]:
            lf = lf.with_columns(pl.col(col_name).cast(pl.Float64, strict=False))
        elif data_type == "date":
            # Dates are parsed by apply_date_parsing() in apply_schema_transformations()
            # — skip casting here to avoid format conflicts.
            pass
        elif data_type == "boolean":
            # Handle boolean conversion from various string representations
            # Polars doesn't support direct Utf8View -> Boolean casting, so we need to map values
            # Support: 1/0, true/false, t/f, yes/no, y/n (case insensitive)
            lf = lf.with_columns(
                pl.when(
                    pl.col(col_name)
                    .cast(pl.String)
                    .str.to_lowercase()
                    .is_in(["1", "true", "t", "yes", "y"])
                )
                .then(pl.lit(True))
                .when(
                    pl.col(col_name)
                    .cast(pl.String)
                    .str.to_lowercase()
                    .is_in(["0", "false", "f", "no", "n", ""])
                )
                .then(pl.lit(False))
                .otherwise(None)
                .alias(col_name)
            )

    return lf


def apply_schema_transformations(lf: pl.LazyFrame, schema: Any) -> pl.LazyFrame:
    """
    Apply comprehensive schema-driven transformations to parsed data.

        This is the central transformation hub that ensures data consistency
        across different file formats. It applies transformations in a specific
        order to handle dependencies and maintain data integrity.

        Args:
            lf: LazyFrame with typed data
            schema: TableMetadata with transformation rules

        Returns:
            pl.LazyFrame: Transformed data ready for processing

        Positional Mapping Example:
            File columns: ['col1', 'col2', 'col3']
            Schema columns: [
                {'name': 'claim_id', 'output_name': 'id'},
                {'name': 'amount', 'data_type': 'float'},
                {'name': 'status', 'keep': False}
            ]
            Result: Maps col1->id, col2->amount (as float), drops col3
    """
    # Parsers now handle positional mapping and renaming
    # This function just applies additional transformations

    lf.collect_schema().names()
    polars_config = schema.polars if hasattr(schema, "polars") else {}

    # Apply Polars-specific configurations
    if polars_config:
        # Convert columns to categorical if specified
        categorical_cols = polars_config.get("categorical_columns", [])
        for col in categorical_cols:
            if col in lf.collect_schema().names():
                lf = lf.with_columns(pl.col(col).cast(pl.Categorical))

        # Trim string columns if specified
        if polars_config.get("string_trim", False):
            for col in lf.collect_schema().names():
                # Only trim string columns
                if lf.collect_schema()[col] == pl.Utf8:
                    lf = lf.with_columns(pl.col(col).str.strip_chars())

        # Drop specified columns
        drop_cols = polars_config.get("drop_columns", [])
        if drop_cols:
            lf = lf.drop([c for c in drop_cols if c in lf.collect_schema().names()])

        # Handle decimal columns with precision
        decimal_cols = polars_config.get("decimal_columns", [])
        if decimal_cols:
            for col_spec in decimal_cols:
                if isinstance(col_spec, dict):
                    for col_name, _precision in col_spec.items():
                        if col_name in lf.collect_schema().names():
                            # Cast string to float, handling any format issues
                            lf = lf.with_columns(pl.col(col_name).cast(pl.Float64, strict=False))

    # Apply date parsing as final step
    # This ensures column names are correct before parsing
    lf = apply_date_parsing(lf, schema)

    return lf
