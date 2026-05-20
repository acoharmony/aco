# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive file parsing system for healthcare data formats.

 a flexible, schema-driven parsing framework for processing
various healthcare data file formats. It handles the complexities of different
file structures, date extraction patterns, column mappings, and data type
conversions while maintaining consistency across diverse data sources.

Note:
    All parsing functions maintain lazy evaluation until explicitly
    collected, allowing for efficient query optimization and minimal
    memory usage.
"""

from pathlib import Path
from typing import Any

import polars as pl

# Import all implementations from the _parsers module
# Force registration of all parsers
from ._parsers import (
    ParserRegistry,
    add_source_tracking,
    apply_column_types,
    apply_schema_transformations,
    extract_file_date,
)


def parse_file(
    file_path: str | Path,
    schema: Any,
    add_tracking: bool = True,
    schema_name: str | None = None,
    limit: int | None = None,
    **kwargs,
) -> pl.LazyFrame:
    """
    Main entry point for parsing healthcare data files.

        This function automatically detects the file format from the schema
        and applies the appropriate parser. It handles all supported file
        formats transparently, applying schema transformations and optional
        source tracking.

        Args:
            file_path: Path to the file to parse
            schema: TableMetadata object with format and column definitions
            add_tracking: Whether to add source tracking columns
            schema_name: Name of schema for tracking (defaults to schema.name)
            limit: Optional number of rows to parse (for testing)
            **kwargs: Additional parser-specific arguments

        Returns:
            pl.LazyFrame: Parsed data with schema applied and tracking added

    """
    file_path = Path(file_path)

    if not file_path.exists():
        raise FileNotFoundError(f"File not found: {file_path}")

    # Get format from schema or detect from extension
    file_format = schema.file_format if hasattr(schema, "file_format") else {}
    format_type = file_format.get("type", "auto")

    if format_type == "auto":
        # Detect format from file extension
        ext = file_path.suffix.lower()
        format_map = {
            ".csv": "csv",
            ".txt": "delimited",  # Default to delimited for .txt
            ".xlsx": "excel",
            ".xls": "excel",
            ".parquet": "parquet",
            ".json": "json",
            ".jsonl": "ndjson",
            ".ndjson": "ndjson",
        }
        format_type = format_map.get(ext, "delimited")

        # Check for specific patterns in filename
        filename_upper = file_path.name.upper()
        if "CCLF" in filename_upper:
            format_type = "fixed_width"
        elif "TPARC" in filename_upper:
            format_type = "tparc"

    # Get the parser from registry
    parser = ParserRegistry.get_parser(format_type)

    if not parser:
        raise ValueError(f"No parser registered for format: {format_type}")

    # Parse the file
    lf = parser(file_path, schema, limit=limit, **kwargs)

    # Apply column type conversions
    lf = apply_column_types(lf, schema)

    # Apply schema transformations
    lf = apply_schema_transformations(lf, schema)

    # Add source tracking if requested
    if add_tracking:
        if not schema_name:
            schema_name = schema.name if hasattr(schema, "name") else "unknown"

        # Extract date from filename if possible
        file_date = extract_file_date(file_path.name, schema)

        # Get medallion layer from schema if available
        medallion_layer = None
        if hasattr(schema, "medallion_layer"):
            medallion_layer = schema.medallion_layer

        lf = add_source_tracking(
            lf,
            source_file=str(file_path),  # Pass full path, function will extract name
            schema_name=schema_name,
            file_date=file_date,
            medallion_layer=medallion_layer,
        )

    return lf


def parse_json(
    file_path: str | Path, schema: Any = None, limit: int | None = None, **kwargs
) -> pl.LazyFrame:
    """
    Parse JSON data files.

        Args:
            file_path: Path to JSON file
            schema: Optional schema for column selection
            limit: Optional row limit
            **kwargs: Additional parser arguments

        Returns:
            pl.LazyFrame: Parsed JSON data
    """
    parser = ParserRegistry.get_parser("json")
    return parser(Path(file_path), schema or {}, limit=limit, **kwargs)


def parse_parquet(
    file_path: str | Path, schema: Any = None, limit: int | None = None
) -> pl.LazyFrame:
    """
    Parse Apache Parquet files.

        Args:
            file_path: Path to Parquet file
            schema: Optional schema (for consistency)
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed Parquet data
    """
    parser = ParserRegistry.get_parser("parquet")
    return parser(Path(file_path), schema or {}, limit=limit)


def parse_csv(file_path: str | Path, schema: Any = None, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse CSV files.

        Args:
            file_path: Path to CSV file
            schema: Optional schema with column definitions
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed CSV data
    """
    parser = ParserRegistry.get_parser("csv")
    return parser(Path(file_path), schema or {}, limit=limit)


def parse_excel(
    file_path: str | Path,
    schema: Any = None,
    limit: int | None = None,
    sheet_name: str | None = None,
) -> pl.LazyFrame:
    """
    Parse Excel files.

        Args:
            file_path: Path to Excel file
            schema: Optional schema with column definitions
            limit: Optional row limit
            sheet_name: Optional sheet to read

        Returns:
            pl.LazyFrame: Parsed Excel data
    """
    parser = ParserRegistry.get_parser("excel")
    return parser(Path(file_path), schema or {}, limit=limit, sheet_name=sheet_name)


def parse_fixed_width(
    file_path: str | Path, schema: Any, limit: int | None = None, encoding: str = "utf-8"
) -> pl.LazyFrame:
    """
    Parse fixed-width format files.

        Args:
            file_path: Path to fixed-width file
            schema: Schema with column positions
            limit: Optional row limit
            encoding: File encoding

        Returns:
            pl.LazyFrame: Parsed fixed-width data
    """
    parser = ParserRegistry.get_parser("fixed_width")
    return parser(Path(file_path), schema, limit=limit, encoding=encoding)


def parse_delimited(
    file_path: str | Path,
    schema: Any = None,
    limit: int | None = None,
    delimiter: str | None = None,
) -> pl.LazyFrame:
    """
    Parse delimited text files.

        Args:
            file_path: Path to delimited file
            schema: Optional schema with column definitions
            limit: Optional row limit
            delimiter: Field delimiter character(s)

        Returns:
            pl.LazyFrame: Parsed delimited data
    """
    parser = ParserRegistry.get_parser("delimited")
    return parser(Path(file_path), schema or {}, limit=limit, delimiter=delimiter)


# Private internal function for TPARC (kept for compatibility)
def _parse_tparc(file_path: str | Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse TPARC multi-record type files.

        Args:
            file_path: Path to TPARC file
            schema: Schema with record type definitions
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed TPARC data
    """
    parser = ParserRegistry.get_parser("tparc")
    return parser(Path(file_path), schema, limit=limit)


# Export public API
__all__ = [
    "parse_file",
    "parse_json",
    "parse_parquet",
    "parse_csv",
    "parse_excel",
    "parse_fixed_width",
    "parse_delimited",
    "extract_file_date",
    "add_source_tracking",
    "apply_column_types",
    "apply_schema_transformations",
    "ParserRegistry",
]
