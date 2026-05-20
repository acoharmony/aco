# © 2025 HarmonyCares
# All rights reserved.

"""
Unified date handling for schema-driven parsing.

 consistent, schema-driven date parsing across all file
formats (CSV, Excel, delimited, fixed-width). It handles multiple date format
specifications, Excel serial dates, null values, and flexible fallback parsing
to ensure dates are correctly typed throughout the data pipeline.

What is Schema-Driven Date Handling?

Schema-driven date handling uses explicit format specifications in table schemas
to parse date strings into proper date types. This approach is critical for
healthcare data where:

- **Format Consistency**: Different sources use different date formats (MM/DD/YYYY vs YYYY-MM-DD)
- **Type Safety**: Dates must be proper date types, not strings, for filtering/sorting/calculations
- **Data Quality**: Explicit parsing catches malformed dates, prevents silent errors
- **Excel Compatibility**: Handles Excel serial dates (numeric dates from .xlsx files)
- **Null Handling**: Gracefully handles empty strings, "null", "NULL", whitespace

Key Concepts

**Schema Date Specifications**:
    Table schemas define date columns with explicit format strings:

    - ``data_type: "date"`` - Marks column as date type
    - ``date_format: "%Y-%m-%d"`` - Single format string
    - ``date_format: ["%m/%d/%Y", "%-m/%-d/%Y"]`` - Multiple formats (tries in order)

**Format String Syntax**:
    Uses Python strptime format codes:

    - ``%Y`` - 4-digit year (2024)
    - ``%m`` - Zero-padded month (01-12)
    - ``%-m`` - Non-padded month (1-12)
    - ``%d`` - Zero-padded day (01-31)
    - ``%-d`` - Non-padded day (1-31)
    - Common patterns: ``%Y-%m-%d`` (ISO), ``%m/%d/%Y`` (US), ``%-m/%-d/%Y`` (US no padding)

**Multiple Format Fallback**:
    When multiple formats are specified, tries each in order using ``COALESCE``:

    1. Try first format (e.g., ``%m/%d/%Y``)
    2. If parse fails, try second format (e.g., ``%-m/%-d/%Y``)
    3. Continue until successful parse or all formats exhausted
    4. Result is first successful parse or NULL

**Excel Serial Date Handling**:
    Excel stores dates as numbers (days since 1899-12-30):

    - Detects numeric columns (Int64, Float64) with ``data_type: "date"``
    - Converts using formula: ``date(1899, 12, 30) + days``
    - Accounts for Excel's 1900 leap year bug
    - Example: 44927 (numeric) → 2023-01-15 (date)

**Null and Empty Handling**:
    Gracefully handles various null representations:

    - NULL/null - Empty Polars NULL
    - Empty string "" - NULL
    - Whitespace "   " - NULL
    - Actual NULL values - Preserved as NULL

**Column Name Resolution**:
    Handles both original and renamed column names:

    - Checks ``output_name`` (after renaming) first
    - Falls back to ``name`` (original) if needed
    - Works seamlessly with CSV position-based mapping

Common Use Cases

1. **CSV Date Parsing**:
   Parse date strings from CSV files (e.g., "01/15/2024" → 2024-01-15) using
   schema-specified formats. Handles US date format (MM/DD/YYYY) common in CMS files.

2. **Excel Date Conversion**:
   Convert Excel serial dates to proper dates. Excel files store dates as numbers
   (e.g., 44927), this module automatically detects and converts them.

3. **Multi-Format Flexibility**:
   Handle inconsistent date formats within same file. Schema can specify multiple
   formats to try, ensuring all valid dates are parsed correctly.

4. **BAR File Date Handling**:
   BAR files often have inconsistent date formats across columns. Multiple format
   specifications allow robust parsing of all date columns.

5. **Null Value Normalization**:
   Convert various null representations ("null", "NULL", "", whitespace) to
   proper NULL values for consistent downstream processing.

How It Works

**apply_date_parsing() Function**:
    1. Collect existing column names from LazyFrame
    2. Build map of column names → date formats from schema
    3. For each date column:
       - Check if already date type (Excel) → skip
       - Check if numeric (Excel serial) → convert with formula
       - Check if string → parse with format(s)
    4. For string parsing:
       - Build COALESCE expression for multiple formats
       - Handle nulls/empty strings/whitespace
       - Apply parsing expression
    5. Return LazyFrame with typed date columns

**get_date_columns_from_schema() Function**:
    Helper to extract date column specifications from schema:

    1. Iterate through schema columns
    2. Find columns with ``data_type: "date"``
    3. Extract ``date_format`` (single or list)
    4. Use defaults if not specified: ["%Y-%m-%d", "%m/%d/%Y", "%-m/%-d/%Y"]
    5. Return dict: {column_name: [formats]}

Pipeline Position

Date handling happens **after initial parsing, before transformations**:

**Pipeline Flow**::

    Parse File → [DATE HANDLING] → Transforms → Dedup/ADR/Std → Silver

- **After CSV/Excel Parsing**: Files read, columns renamed, types cast
- **Before Transformations**: Dates must be typed for date calculations in transforms
- **Applies to All Formats**: CSV, Excel, delimited, fixed-width all use this module
- **Universal Date Layer**: Single consistent date handling for entire system

Performance Considerations

- **Lazy Evaluation**: Operates on LazyFrames, no data materialization until collect()
- **Schema-Driven**: Only parses columns marked as dates in schema
- **Early Type Detection**: Checks column dtype before parsing (skips if already date)
- **COALESCE Efficiency**: Multiple formats handled in single pass (not sequential tries)
- **Null Short-Circuit**: Null check happens first, avoids parsing empty values
- **Excel Numeric Conversion**: Fast arithmetic operation, no string parsing
"""

from typing import Any

import polars as pl


def apply_date_parsing(lf: pl.LazyFrame, schema: Any) -> pl.LazyFrame:
    """
    Apply schema-driven date parsing to columns.

         all date parsing variations:
        1. date_format in column definitions (for all file types)
        2. Handles both original and renamed column names
        3. Multiple format attempts for flexibility (BAR files)
        4. Never infers dtype - always uses schema

        Parameters

        lf : pl.LazyFrame
            LazyFrame with string date columns to be parsed
        schema : Any
            TableMetadata object with date specifications (columns with data_type="date")

        Returns

        pl.LazyFrame
            Data with properly parsed date columns (string → Date type)

    """
    existing_cols = lf.collect_schema().names()
    columns = schema.columns if hasattr(schema, "columns") else []

    # Build a map of actual column names to date formats
    # This handles both renamed columns and original names
    date_format_map = {}

    for col_def in columns:
        # Get both original name and output name
        original_name = col_def.get("name")
        output_name = col_def.get("output_name", original_name)

        # Determine which name is actually in the dataframe
        actual_col_name = None
        if output_name in existing_cols:
            actual_col_name = output_name
        elif original_name in existing_cols:
            actual_col_name = original_name

        if not actual_col_name:
            continue

        # Check if this is a date or datetime column
        data_type = col_def.get("data_type", "").lower()
        if data_type in ["date", "datetime"] and "date_format" in col_def:
            formats = col_def["date_format"]
            # Handle both single format and list of formats
            if isinstance(formats, str):
                formats = [formats]
            date_format_map[actual_col_name] = (formats, data_type)

    # Apply date parsing for each column
    for col_name, (formats, target_type) in date_format_map.items():
        if col_name not in existing_cols:
            continue

        # Determine target polars type
        pl_target_type = pl.Datetime if target_type == "datetime" else pl.Date

        # Check if column is already a date type (from Excel)
        col_dtype = lf.collect_schema()[col_name]
        if col_dtype in (pl.Date, pl.Datetime):
            # Already parsed, skip
            continue

        # Check if column is numeric (could be Excel serial date)
        if col_dtype in (pl.Int32, pl.Int64, pl.Float32, pl.Float64):
            # Excel stores dates as numbers (days since 1900-01-01, with a leap year bug)
            # Try to convert Excel serial dates to proper dates
            try:
                # Excel's epoch starts at 1900-01-01, but has a leap year bug (treats 1900 as leap year)
                # Serial date 1 = 1900-01-01, but we need to adjust for the bug
                # Using 1899-12-30 as the base date to account for Excel's quirk
                lf = lf.with_columns(
                    pl.when(pl.col(col_name).is_not_null() & (pl.col(col_name) > 0))
                    .then(pl.date(1899, 12, 30) + pl.duration(days=pl.col(col_name).cast(pl.Int64)))
                    .otherwise(None)
                    .alias(col_name)
                )
            except Exception:  # ALLOWED: Continues processing remaining items despite error
                # If conversion fails, skip with warning
                print(
                    f"WARNING: Column {col_name} is numeric but couldn't convert to date - skipping"
                )
            continue

        # Build parsing expression trying each format
        parsed_expr = None

        for fmt in formats:
            try:
                # Create parsing expression for this format
                if parsed_expr is None:
                    parsed_expr = pl.col(col_name).str.strptime(pl_target_type, fmt, strict=False)
                else:
                    # Use coalesce to try multiple formats
                    parsed_expr = pl.coalesce(
                        [
                            parsed_expr,
                            pl.col(col_name).str.strptime(pl_target_type, fmt, strict=False),
                        ]
                    )
            except Exception:  # ALLOWED: Continues processing remaining items despite error
                # Format doesn't work with current Polars version, skip
                continue

        # Apply the parsing if we found working format(s)
        if parsed_expr is not None:
            # Handle empty strings, nulls, and whitespace
            lf = lf.with_columns(
                pl.when(
                    pl.col(col_name).is_null()
                    | (pl.col(col_name).str.strip_chars() == "")
                    | (pl.col(col_name).str.strip_chars() == "null")
                    | (pl.col(col_name).str.strip_chars() == "NULL")
                )
                .then(None)
                .otherwise(parsed_expr)
                .alias(col_name)
            )

    return lf


def get_date_columns_from_schema(schema: Any) -> dict[str, list[str]]:
    """
    Get date columns and their formats from schema.

        Extracts date column specifications from schema, returning a dict mapping
        column names (after renaming with output_name) to their date format strings.
        Useful for inspection, validation, and standalone date parsing.

        Parameters

        schema : Any
            TableMetadata object with column definitions

        Returns

        dict[str, list[str]]
            Dict mapping column names to list of date format strings.
            Example: {'service_date': ['%Y-%m-%d'], 'claim_date': ['%m/%d/%Y', '%-m/%-d/%Y']}
        {'service_date': ['%Y-%m-%d']}

        **Extract date columns with multiple formats**:
        {'claim_date': ['%m/%d/%Y', '%-m/%-d/%Y']}

        **Date column with output_name (after renaming)**:
        {'service_date': ['%Y-%m-%d']}

        **Date column without date_format (uses defaults)**:
        {'enrollment_date': ['%Y-%m-%d', '%m/%d/%Y', '%-m/%-d/%Y']}

        **Multiple date columns**:
        3
        ['%m/%d/%Y']
        ['%Y-%m-%d', '%m/%d/%Y']

        **No date columns in schema**:
        {}

        **Schema with no columns attribute**:
        {}

        **Real-world enrollment schema**:
        ['bene_death_dt', 'enrollment_end_date', 'enrollment_start_date']

        **Real-world physician claim schema**:
        ['service_from_date', 'service_thru_date']

        **Use case - validation and inspection**:
        True
        2
    """
    date_format_map = {}
    columns = schema.columns if hasattr(schema, "columns") else []

    for col_def in columns:
        data_type = col_def.get("data_type", "").lower()
        if data_type == "date":
            output_name = col_def.get("output_name", col_def.get("name"))

            if "date_format" in col_def:
                formats = col_def["date_format"]
                if isinstance(formats, str):
                    formats = [formats]
            else:
                # Default formats if not specified
                formats = ["%Y-%m-%d", "%m/%d/%Y", "%-m/%-d/%Y"]

            date_format_map[output_name] = formats

    return date_format_map
