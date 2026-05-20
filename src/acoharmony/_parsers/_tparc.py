# © 2025 HarmonyCares
# All rights reserved.

"""
TPARC file parser implementation.
"""

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method
from ._registry import register_parser


@register_parser("tparc", metadata={"description": "Third Party Administrator Record format"})
@parser_method(
    threshold=5.0,
    validate_path="file_path",
)
def parse_tparc(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse TPARC (Third Party Administrator Record) multi-record type files.

        TPARC files contain multiple record types identified by a type indicator
        in the first field. Each record type has its own schema and field layout.
        This parser splits records by type and processes each according to its
        specific schema.

        Args:
            file_path: Path to the TPARC file
            schema: TableMetadata with record type definitions
            limit: Optional total row limit across all record types

        Returns:
            pl.LazyFrame: Combined data from all record types with type indicator

        TPARC Format:
            - First field: Record type indicator (e.g., 'ALR', 'BEN', 'CLM')
            - Remaining fields: Type-specific data layout
            - Mixed record types within single file
            - Often pipe-delimited

        Schema Requirements:
            Must define 'record_types' with layouts for each type:
            {
                'record_types': {
                    'ALR': {'columns': [...]},
                    'BEN': {'columns': [...]},
                    'CLM': {'columns': [...]}
                }
            }

        Processing:
            1. Read entire file
            2. Split by record type indicator
            3. Parse each type with its schema
            4. Combine with record type column
    """
    # Check for record_types in schema (dict or TableMetadata)
    has_record_types = False
    if isinstance(schema, dict):
        has_record_types = "record_types" in schema
        delimiter = schema.get("delimiter", "|")
        record_types = schema.get("record_types", {})
    else:
        has_record_types = hasattr(schema, "record_types") and schema.record_types
        delimiter = (
            schema.file_format.get("delimiter", "|")
            if hasattr(schema, "file_format") and schema.file_format
            else "|"
        )
        record_types = schema.record_types if has_record_types else {}

    if not has_record_types:
        raise ValueError("TPARC schema must define record_types")

    # Read the entire file first to split by record type
    with open(file_path, encoding="utf-8") as f:
        lines = f.readlines()

    if limit:
        lines = lines[:limit]

    # Group lines by record type
    grouped_records: dict[str, list[str]] = {}
    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Get record type from first field
        parts = line.split(delimiter, 1)
        if not parts:
            continue

        record_type = parts[0]
        if record_type in record_types:
            if record_type not in grouped_records:
                grouped_records[record_type] = []
            grouped_records[record_type].append(line)

    # Parse each record type group
    dfs = []
    for record_type, records in grouped_records.items():
        if not records:
            continue

        type_schema = record_types[record_type]

        # Create temporary file or parse from memory
        # For efficiency, we'll parse from memory using Polars
        parsed_records = []
        for record in records:
            fields = record.split(delimiter)
            parsed_records.append(fields)

        # Generate column names based on actual data width
        if parsed_records:
            num_cols = max(len(record) for record in parsed_records)
            column_names = [f"field_{i}" for i in range(num_cols)]
        else:
            continue

        # Pad records to same length if needed
        for i, record in enumerate(parsed_records):
            if len(record) < num_cols:
                parsed_records[i] = record + [""] * (num_cols - len(record))

        # Create DataFrame for this record type
        df = pl.DataFrame(parsed_records, schema=column_names, orient="row")

        # Map schema columns to field positions if defined
        if "columns" in type_schema:
            # Map named columns to their positions
            for idx, col_def in enumerate(type_schema["columns"]):
                col_name = col_def["name"]
                field_name = f"field_{idx}"
                if field_name in df.columns and field_name != col_name:
                    df = df.rename({field_name: col_name})

        # Add record type indicator
        df = df.with_columns(pl.lit(record_type).alias("record_type"))

        # Apply column types if defined in schema
        if "columns" in type_schema:
            for col_def in type_schema["columns"]:
                col_name = col_def["name"]
                if "dtype" in col_def and col_name in df.columns:
                    dtype_map = {
                        "str": pl.Utf8,
                        "int": pl.Int64,
                        "float": pl.Float64,
                        "date": pl.Utf8,
                        "bool": pl.Boolean,
                    }
                    target_dtype = dtype_map.get(col_def["dtype"], pl.Utf8)
                    try:
                        df = df.with_columns(pl.col(col_name).cast(target_dtype))
                    except:  # ALLOWED: Type casting fallback - keep as string if cast fails  # noqa: E722
                        pass  # Keep as string if casting fails

        dfs.append(df)

    # Combine all record types
    if not dfs:
        # Return empty frame with expected structure
        return pl.DataFrame().lazy()

    # Union all DataFrames with diagonal strategy to handle different schemas
    combined = pl.concat(dfs, how="diagonal")

    return combined.lazy()


@register_parser("multi_record")
def parse_multi_record(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Generic parser for multi-record type files.

        Similar to TPARC but more flexible for various multi-record formats.

        Args:
            file_path: Path to multi-record file
            schema: Schema with record type definitions
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Combined data from all record types
    """
    return parse_tparc(file_path, schema, limit)
