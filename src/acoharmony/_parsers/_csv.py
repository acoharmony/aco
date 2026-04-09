# © 2025 HarmonyCares
# All rights reserved.

"""
CSV file parser implementation.

 schema-driven CSV parsing for healthcare data files. Unlike
traditional CSV parsers that infer types, this parser enforces strict schema-based
column naming and typing to ensure data consistency and correctness.

What is Schema-Driven CSV Parsing?

Schema-driven parsing maps CSV columns by **position** (not header names) to a
predefined schema, ensuring consistent data structures regardless of source file
variations. This approach is critical for healthcare data where:

- **Column Names Vary**: Different sources use different header names for the same data
- **Type Safety Matters**: Healthcare codes, IDs must be treated as strings, not numbers
- **Data Quality**: No silent type inference errors (e.g., leading zeros dropped from NPIs)
- **Reproducibility**: Same schema always produces same output structure

Key Concepts

**Position-Based Mapping**:
    Columns are mapped by their **position** in the file, not by header names.
    Schema defines: [Column 0 → 'provider_id', Column 1 → 'provider_name', ...]
    File headers can be anything; position determines mapping.

**No Type Inference**:
    All columns are read as strings initially. Schema explicitly defines types
    (int, float, date, boolean, string). This prevents:

    - Leading zeros dropped from NPIs, tax IDs, claim IDs
    - Dates parsed incorrectly (MM/DD vs DD/MM ambiguity)
    - Numeric codes treated as numbers (ICD codes like "038.9")

**Schema Definition**:
    Schema specifies for each column:

    - ``name``: Internal column name
    - ``output_name``: Final column name in output
    - ``data_type``: Explicit type (string, int, float, date, boolean)
    - Position in schema list determines position mapping

**UTF-8 Encoding**:
    All files are read as UTF-8 to handle international characters in provider
    names, addresses, beneficiary names.

Common Use Cases

1. **Provider Roster Files**:
   Parse provider lists with NPIs, tax IDs, names, addresses. Schema ensures
   NPIs are strings (preserving leading zeros), names are UTF-8, dates are
   properly typed.

2. **Beneficiary Enrollment Files**:
   Parse member enrollment/eligibility files with beneficiary IDs, dates,
   demographics. Schema maps varying column names to standard fields, enforces
   date formats, preserves MBI identifiers as strings.

3. **Claims Export Files**:
   Parse claim detail exports from external systems. Schema handles different
   column orders/names, ensures claim IDs stay as strings, amounts as floats,
   dates as proper date types.

4. **Quality Measure Reports**:
   Parse quality measure calculation results. Schema standardizes varying report
   formats, ensures measure IDs are strings, scores are floats, dates are typed.

5. **Financial Reconciliation Files**:
   Parse payment/reconciliation files with dollar amounts, dates, transaction IDs.
   Schema ensures amounts are floats, IDs are strings, dates are standardized.

How It Works

**parse_csv() Function**:
    1. Scan CSV file with Polars, read all columns as strings (no inference)
    2. Get actual column names from file header
    3. Build position-based rename map: actual_col[i] → schema_col[i].output_name
    4. Build type cast expressions based on schema data_type definitions
    5. Apply renaming and type casting in lazy evaluation pipeline
    6. Return LazyFrame for further processing

**Type Casting Logic**:
    - ``date``: Keep as string (parsed later in transformations for flexibility)
    - ``int/integer``: Cast to Int64 with non-strict (allows nulls)
    - ``float/decimal/float64``: Cast to Float64 with non-strict
    - ``boolean``: Cast to Boolean with non-strict
    - ``string`` or unknown: Keep as Utf8 string

Pipeline Position

CSV parsing is the **first stage** in the data ingestion pipeline:

**Pipeline Flow**::

    Raw CSV File → [CSV PARSING] → Bronze LazyFrame → Transforms → Silver

- **File Discovery**: Find CSV files matching patterns
- **Schema Lookup**: Load schema definition for file type
- **Parse CSV**: This module - read and structure data
- **Transformations**: Apply XREF, dedup, ADR, standardization, etc.
- **Materialization**: Write to Bronze/Silver/Gold tables

**Parser Registry**:
    This parser is registered with ``@register_parser("csv")`` so it can be
    dynamically invoked by file processors based on file extension or schema
    configuration.

Performance Considerations

- **Lazy Evaluation**: Returns LazyFrame, no data loaded until collect()
- **Streaming**: Polars can stream large CSV files without loading into memory
- **Schema Overhead**: Position mapping and type casting are low-cost operations
- **No Inference**: Skipping type inference is faster than scanning file
- **Row Limiting**: ``limit`` parameter useful for testing/development
- **UTF-8 Native**: Modern encoding, no conversion overhead
"""

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method, validate_file_format
from ._registry import register_parser


@register_parser("csv")
@validate_file_format(param_name="file_path", formats=[".csv"])
@parser_method(
    threshold=2.0,
    validate_path="file_path",
)
def parse_csv(file_path: Path, schema: Any, limit: int | None = None) -> pl.LazyFrame:
    """
    Parse comma-separated value (CSV) files.

        CSV is a common format for data exchange, especially for smaller datasets
        and reports. This parser handles standard CSV files with proper quoting
        and escaping.

        CRITICAL: This parser is SCHEMA-DRIVEN:
        - Maps columns by POSITION to schema names
        - Forces all columns to schema-defined types
        - Never infers types

        Args:
            file_path: Path to the CSV file
            schema: TableMetadata object containing column definitions
            limit: Optional number of rows to read

        Returns:
            pl.LazyFrame: Lazily loaded CSV data with applied schema

        Features:
            - Schema-driven column naming and typing
            - No type inference
            - UTF-8 encoding support
            - Handles varying column counts
    """
    # Parse CSV without type inference
    lf = pl.scan_csv(
        file_path,
        has_header=True,
        encoding="utf8",
        try_parse_dates=False,  # Never infer dates
        infer_schema_length=0,  # Don't infer types - all strings
        n_rows=limit,
    )

    # Get actual columns in file
    actual_columns = lf.collect_schema().names()
    actual_col_count = len(actual_columns)

    # Apply schema-driven renaming and typing
    if schema and hasattr(schema, "columns"):
        # Build mapping by position
        rename_map = {}
        cast_exprs = []

        for i, col_def in enumerate(schema.columns):
            if i >= actual_col_count:
                # Schema has more columns than file
                break

            # Map actual column name to schema output name
            actual_name = actual_columns[i]
            output_name = col_def.get("output_name", col_def.get("name"))
            rename_map[actual_name] = output_name

            # Determine data type
            data_type = col_def.get("data_type", "string").lower()

            # Build cast expression
            # For dates, keep as string - will parse in transformations
            if data_type == "date":
                cast_exprs.append(pl.col(output_name).cast(pl.Utf8, strict=False))
            elif data_type in ["int", "integer"]:
                cast_exprs.append(pl.col(output_name).cast(pl.Int64, strict=False))
            elif data_type in ["float", "decimal", "float64"]:
                cast_exprs.append(pl.col(output_name).cast(pl.Float64, strict=False))
            elif data_type == "boolean":
                # Handle boolean conversion from various string representations
                # Polars doesn't support direct Utf8View -> Boolean casting
                # Support: 1/0, true/false, t/f, yes/no, y/n (case insensitive)
                boolean_expr = (
                    pl.when(
                        pl.col(output_name)
                        .cast(pl.String)
                        .str.to_lowercase()
                        .is_in(["1", "true", "t", "yes", "y"])
                    )
                    .then(pl.lit(True))
                    .when(
                        pl.col(output_name)
                        .cast(pl.String)
                        .str.to_lowercase()
                        .is_in(["0", "false", "f", "no", "n", ""])
                    )
                    .then(pl.lit(False))
                    .otherwise(None)
                    .alias(output_name)
                )
                cast_exprs.append(boolean_expr)
            else:
                # Keep as string
                cast_exprs.append(pl.col(output_name))

        # Apply renaming
        if rename_map:
            lf = lf.rename(rename_map)

        # Select only mapped columns and cast types
        if cast_exprs:
            lf = lf.select(cast_exprs)

    return lf
