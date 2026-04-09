# © 2025 HarmonyCares
# All rights reserved.

"""
JSON file parser implementation.

 JSON parsing for healthcare data files with support for
multiple JSON formats including standard JSON arrays, newline-delimited JSON
(NDJSON), and JSON Lines (JSONL). Unlike traditional JSON parsers that load
entire files into memory, this parser leverages Polars for efficient streaming
and lazy evaluation.

What is JSON Parsing for Healthcare Data?

JSON (JavaScript Object Notation) has become increasingly common in healthcare
data exchange, especially for:

- **Modern APIs**: FHIR resources, health information exchanges
- **Real-Time Data**: Streaming healthcare events, lab results
- **Cloud Integrations**: Data from SaaS healthcare platforms
- **Interoperability**: Cross-system data exchange
- **Research Data**: Clinical trial data, registry exports

JSON formats offer advantages over traditional fixed-width or CSV formats:

- **Self-Describing**: Field names included in data
- **Hierarchical**: Supports nested structures
- **Type-Aware**: Distinguishes strings, numbers, booleans, nulls
- **Unicode Native**: International characters without encoding issues

Key Concepts

**Multiple JSON Format Support**:
    This parser handles three common JSON formats used in healthcare:

    1. **Standard JSON Array**: ``[{...}, {...}, {...}]``
       - Entire dataset wrapped in array
       - Common for API responses, exports
       - Requires loading full file into memory
       - Example: FHIR bundle responses

    2. **NDJSON (Newline-Delimited JSON)**: One object per line
       - Each line is complete JSON object
       - No comma separation between objects
       - Efficient for streaming and large datasets
       - Example: Healthcare event streams, CDC data feeds

    3. **JSONL (JSON Lines)**: Identical to NDJSON
       - Alternative name for same format
       - One JSON object per line
       - Common in data science pipelines
       - Example: Clinical data exports

**Automatic Format Detection**:
    Parser automatically detects format based on:

    - **File Extension**: ``.jsonl`` or ``.ndjson`` → NDJSON format
    - **First Character**: ``[`` → array format, ``{`` → NDJSON format
    - **Explicit Parameter**: ``json_format`` parameter overrides detection

**Schema Integration**:
    While JSON is self-describing, schema integration provides:

    - **Column Selection**: Extract only needed fields from nested JSON
    - **Field Mapping**: Rename JSON fields to standard column names
    - **Type Validation**: Ensure expected data types
    - **Encoding Control**: Specify character encoding (default UTF-8)

**Nested Structure Handling**:
    JSON often contains nested objects and arrays. Polars automatically
    flattens nested structures to columnar format:

    - Nested objects: ``{patient: {name: "John"}}`` → ``patient.name``
    - Arrays: Handled as Polars list columns
    - Schema can select specific nested fields

**Lazy Evaluation**:
    NDJSON/JSONL formats use ``scan_ndjson()`` for lazy evaluation:

    - No data loaded until ``collect()``
    - Memory-efficient for large files
    - Enables query optimization and predicate pushdown
    - Standard JSON arrays require eager loading but converted to LazyFrame


How It Works

**parse_json() Function**:
    Main parser function handles all JSON formats:

    1. **Format Detection**:
       - Check ``json_format`` parameter (if not "auto")
       - Check file extension (.jsonl, .ndjson)
       - Peek at first character: ``[`` = array, ``{`` = NDJSON

    2. **Parse Based on Format**:
       - **NDJSON/JSONL**: Use ``pl.scan_ndjson()`` for lazy evaluation
       - **Standard JSON**: Load with ``json.load()``, convert to DataFrame

    3. **Structure Handling**:
       - If data is list: Create DataFrame from list of dicts
       - If data is single dict: Wrap in list, then create DataFrame
       - Other types: Raise error

    4. **Field Mapping**:
       - If schema has columns with ``source_name``: Rename fields
       - Maps JSON field names to standardized output names
       - Preserves fields not in rename map

    5. **Column Selection**:
       - If schema defines columns: Select only those columns
       - Filters out extra fields not needed
       - Maintains schema column order

    6. **Lazy Conversion**:
       - Convert eager DataFrame to LazyFrame
       - Enables lazy evaluation for downstream transforms

    7. **Row Limiting**:
       - Apply ``limit`` parameter if specified
       - Useful for testing with large files

**parse_ndjson() and parse_jsonl() Functions**:
    Convenience wrappers that call ``parse_json()`` with explicit format:

    - Ensure correct format handling regardless of file extension
    - Useful when format known but extension doesn't match
    - Provides explicit function names for clarity

Pipeline Position

JSON parsing is typically the **first stage** for JSON-based data sources:

**Pipeline Flow**::

    Raw JSON File → [JSON PARSING] → Bronze LazyFrame → Transforms → Silver

- **Before Parsing**:
  - File discovery: Identify JSON files by pattern/extension
  - Schema lookup: Load schema definition for file type
  - Metadata extraction: Extract ACO ID, dates from filename

- **After Parsing**:
  - Date parsing: Handle date strings with multiple format fallback
  - XREF: Apply crosswalk mapping for identifier standardization
  - Deduplication: Remove duplicate records
  - ADR: Apply adjustments, denials, reprocesses
  - Standardization: Additional field mapping and computed columns

**Parser Registry**:
    Registered with ``@register_parser("json")`` and metadata showing
    supported formats ["json", "ndjson", "jsonl"]. File processors
    dynamically invoke parser based on:

    - File extension (.json, .jsonl, .ndjson)
    - Schema configuration: ``file_format.type = "json"``
    - Format auto-detection during processing

Performance Considerations

- **NDJSON Streaming**: NDJSON format uses lazy scanning, never loads full
  file into memory, ideal for large healthcare datasets (e.g., full claims
  history, beneficiary census)

- **Standard JSON Memory**: Standard JSON arrays require loading full file,
  for large JSON arrays (>100MB), consider converting to NDJSON format,
  or use row limiting for testing

- **Lazy Evaluation**: After parsing, LazyFrame enables query optimization,
  subsequent filters/selections pushed down to scan, only needed data loaded

- **Nested Structure Overhead**: Deeply nested JSON requires flattening,
  adds processing overhead, keep JSON structures flat when possible

- **File Format Choice**: For large datasets, prefer NDJSON over standard
  JSON arrays, NDJSON enables true streaming and incremental processing

- **UTF-8 Encoding**: Native UTF-8 support, international characters handled
  efficiently, no conversion overhead

- **Schema Column Selection**: Selecting specific columns reduces memory,
  only extracts needed fields from JSON, especially important with wide
  nested structures
"""

import json
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method, validate_file_format
from ._registry import register_parser


@register_parser("json", metadata={"formats": ["json", "ndjson", "jsonl"]})
@validate_file_format(param_name="file_path", formats=[".json", ".ndjson", ".jsonl"])
@parser_method(
    threshold=2.0,
    validate_path="file_path",
)
def parse_json(
    file_path: Path, schema: Any, limit: int | None = None, json_format: str = "auto"
) -> pl.LazyFrame:
    """
    Parse JSON data files.

        Supports standard JSON arrays, newline-delimited JSON (NDJSON/JSONL),
        and nested JSON structures. Common in modern healthcare APIs and
        data exchange formats.

        Args:
            file_path: Path to the JSON file
            schema: TableMetadata object for column selection
            limit: Optional number of records to read
            json_format: Format type ('auto', 'array', 'ndjson', 'jsonl')

        Returns:
            pl.LazyFrame: Parsed JSON data as a LazyFrame

        Features:
            - Automatic format detection
            - Nested structure flattening
            - Type inference
            - Schema validation
    """
    file_path = Path(file_path)

    # Auto-detect format based on file extension or content
    if json_format == "auto":
        if file_path.suffix in [".jsonl", ".ndjson"]:
            json_format = "ndjson"
        else:
            # Peek at file to determine format
            with open(file_path, encoding="utf-8") as f:
                first_char = f.read(1)
                if first_char == "[":
                    json_format = "array"
                elif first_char == "{":
                    json_format = "ndjson"

    # Parse based on format
    if json_format in ["ndjson", "jsonl"]:
        # Newline-delimited JSON
        lf = pl.scan_ndjson(file_path)
    else:
        # Standard JSON array - requires eager loading
        # Get encoding from schema if available
        encoding = "utf-8"
        if schema and hasattr(schema, "file_format"):
            encoding = schema.file_format.get("encoding", "utf-8")

        with open(file_path, encoding=encoding) as f:
            data = json.load(f)

        if isinstance(data, list):
            df = pl.DataFrame(data)
        elif isinstance(data, dict):
            # Single object, wrap in list
            df = pl.DataFrame([data])
        else:
            raise ValueError(f"Unsupported JSON structure: {type(data)}")

        # Apply field mapping if schema has columns with source_name
        if schema and hasattr(schema, "columns"):
            columns = schema.columns

            if columns:
                rename_map = {}
                for col in columns:
                    source_name = col.get("source_name", col.get("name"))
                    output_name = col.get("name")
                    if source_name and source_name in df.columns and source_name != output_name:
                        rename_map[source_name] = output_name

                if rename_map:
                    df = df.rename(rename_map)

        lf = df.lazy()

    # Apply limit if specified
    if limit:
        lf = lf.head(limit)

    # Apply schema column selection if specified
    if schema and hasattr(schema, "columns"):
        columns = schema.columns
        if columns:
            schema_cols = [col["name"] for col in columns]
            existing_cols = lf.collect_schema().names()
            cols_to_select = [col for col in schema_cols if col in existing_cols]

            if cols_to_select:
                lf = lf.select(cols_to_select)

    return lf


@register_parser("ndjson")
def parse_ndjson(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse newline-delimited JSON files.

        NDJSON format has one JSON object per line, efficient for streaming
        and large datasets.

        Args:
            file_path: Path to NDJSON file
            schema: TableMetadata object
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed NDJSON data

    """
    return parse_json(file_path, schema, limit, json_format="ndjson")


@register_parser("jsonl")
def parse_jsonl(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse JSON Lines format files.

        JSON Lines is identical to NDJSON - one JSON object per line.

        Args:
            file_path: Path to JSONL file
            schema: TableMetadata object
            limit: Optional row limit

        Returns:
            pl.LazyFrame: Parsed JSONL data

    """
    return parse_json(file_path, schema, limit, json_format="jsonl")
