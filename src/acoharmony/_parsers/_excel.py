# © 2025 HarmonyCares
# All rights reserved.

"""
Schema-driven Excel file parser for healthcare data with multi-sheet support.

Provides robust Excel (.xlsx, .xls) file parsing capabilities specifically designed
for healthcare data files commonly used in manual data entry, reporting, quality
measures, provider rosters, and administrative data exchange. Unlike generic Excel
parsers that rely on file headers and type inference, this parser enforces strict
schema-driven column mapping and typing to ensure data consistency and correctness
across varying file formats and sources.

What is Schema-Driven Excel Parsing?

Schema-driven Excel parsing maps Excel columns by **position** (not header names) to
a predefined schema, ensuring consistent data structures regardless of how the Excel
file is formatted. This approach is critical for healthcare data where:

- **Column names vary**: Different sources use different header names for the same data
  (e.g., "MBI" vs "Beneficiary ID" vs "Member ID")
- **Type safety matters**: Healthcare identifiers (NPIs, MBIs, claim IDs) must be
  strings to preserve leading zeros
- **Data quality counts**: No silent type inference errors that could drop leading
  zeros, misinterpret dates, or convert codes to numbers
- **Reproducibility required**: Same schema always produces same output structure

Key Concepts

**Position-Based Mapping**
    Columns are mapped by their position in the Excel file, not by header names.
    The schema defines: Column A (position 0) → provider_id, Column B (position 1) →
    provider_name, etc. File headers can be anything - the position determines the mapping.

**No Type Inference**
    All columns are read from Excel and then explicitly cast to types defined in the
    schema (int/float/date/boolean/string). This prevents common Excel problems:
    - Leading zeros dropped from NPIs, tax IDs, claim IDs
    - Dates parsed incorrectly (MM/DD vs DD/MM ambiguity)
    - Numeric codes treated as numbers (ICD codes like "038.9")
    - Boolean values stored inconsistently ("1"/"0"/"Yes"/"No"/"TRUE"/"FALSE")

**Multi-Sheet Support**
    Excel workbooks often contain multiple sheets. The parser can:
    - Read a specific sheet by name (sheet_name parameter)
    - Use schema configuration to specify default sheet (file_format.sheet_name)
    - Default to first sheet if not specified

**Schema Configuration Options**
    The schema's file_format section controls Excel-specific parsing behavior:
    - sheet_name: Name of sheet to read (defaults to first sheet)
    - skip_rows: Number of rows to skip before data starts
    - header_row: Which row contains headers (or None for no header)
    - has_header: Boolean indicating if file has headers (sets header_row=None if False)

**Excel-Specific Data Types**
    Excel stores data differently than text files:
    - Dates stored as serial numbers (days since 1899-12-30)
    - Numbers stored with full precision (no string conversion loss)
    - Boolean values represented multiple ways (1/0, TRUE/FALSE, "Yes"/"No")
    - Strings may contain international characters (UTF-8 support)

Common Use Cases

**Provider Roster Files**
    Parse Excel provider lists with NPIs, tax IDs, names, addresses, specialties.
    Schema ensures NPIs are strings preserving leading zeros (e.g., "0123456789"),
    names handle international characters, and dates are properly typed.

**Quality Measure Reports**
    Parse quality measure calculation results delivered as Excel files.
    Schema standardizes varying report formats, ensures measure IDs stay as strings
    (e.g., "BCS-E", "COL-E"), scores are floats, and dates are typed.

**Manual Data Entry Forms**
    Parse Excel forms filled out manually for beneficiary enrollment, voluntary
    alignments, or provider updates. Schema maps varying column names to standard
    fields, handles inconsistent boolean representations, and validates data types.

**Beneficiary Enrollment Files**
    Parse member enrollment/eligibility files delivered as Excel spreadsheets.
    Schema maps varying column names to standard fields, enforces date formats,
    and preserves MBI identifiers as strings.

**Financial Reconciliation Reports**
    Parse payment reconciliation files with dollar amounts, dates, transaction IDs.
    Schema ensures amounts are floats with full precision, IDs are strings preserving
    format, and dates are standardized.

**Multi-Sheet Workbooks**
    Parse Excel workbooks with multiple sheets (e.g., "Summary", "Detail", "Notes").
    Specify which sheet to read by name or use schema configuration for default sheet.

How It Works

**parse_excel() Function**
    Main parser function that handles all Excel file types (.xlsx, .xls).

    1. **Read Sheet Configuration**
       - Get sheet name from parameter or schema file_format.sheet_name
       - Get skip_rows and header_row from schema file_format
       - Use calamine engine (fast Excel reader)

    2. **Read Excel File**
       - Use pl.read_excel() with calamine engine for performance
       - Apply sheet selection if specified
       - Apply skip_rows and header_row if configured

    3. **Apply Row Limit**
       - If limit parameter provided, take first N rows
       - Useful for testing and development

    4. **Build Position-Based Column Mapping**
       - Get actual column count in file
       - Map schema columns by position to actual columns
       - Stop if schema has more columns than file

    5. **Rename Columns**
       - Take only columns we have schema mappings for
       - Rename to schema-defined output names
       - Ignore original Excel headers

    6. **Cast Data Types**
       - Build casting expressions for each column based on schema data_type
       - int/integer → Int64 (non-strict, allows nulls)
       - float/decimal → Float64 (non-strict, allows nulls)
       - boolean → Boolean (handles "1"/"0"/"TRUE"/"FALSE"/"Yes"/"No"/"T"/"F")
       - string → Utf8 (preserves leading zeros and formats)
       - date → Keep as string initially, parse later in apply_date_parsing()

    7. **Apply Date Parsing**
       - Delegate to apply_date_parsing() for schema-defined date columns
       - Handles Excel serial dates (numeric days since 1899-12-30)
       - Handles date strings with multiple format fallback

    8. **Return LazyFrame**
       - Return LazyFrame for lazy evaluation and streaming
       - Actual file reading happens on collect()

Pipeline Position

Excel parsing is typically the first stage in data ingestion for Excel-based sources:

    Raw Excel File → [EXCEL PARSING] → Bronze LazyFrame → Transforms → Silver

**Before Parsing**:
    - File discovery: Find Excel files matching patterns
    - Schema lookup: Load schema definition for file type
    - ACO ID and date extraction: Metadata from filename

**After Parsing**:
    - Date parsing: Parse date columns with flexible formats
    - XREF: Apply crosswalk mapping for current identifiers
    - Deduplication: Remove duplicate records
    - ADR: Apply adjustments, denials, reprocesses
    - Standardization: Rename columns and add computed fields

**Parser Registry**:
    This parser is registered with @register_parser("excel") and
    file processors based on:
    - File extension: .xlsx or .xls
    - Schema configuration: file_format.type = "excel"

Performance Considerations

**Excel Memory Usage**
    Excel files are loaded entirely into memory during parsing. Large Excel files
    (>100MB or >100K rows) may require significant memory. For better performance:
    - Convert large Excel files to CSV or Parquet format
    - Use row limiting (limit parameter) for testing
    - Process Excel files in batches if possible

**Calamine Engine**
    Uses calamine (Rust-based Excel reader) for fast parsing. Calamine is significantly
    faster than openpyxl or xlrd engines and handles both .xlsx and .xls formats.

**Lazy Evaluation**
    Returns LazyFrame for lazy evaluation. After parsing, the DataFrame is converted
    to LazyFrame so subsequent transformations can be optimized and streamed.

**Type Casting**
    Schema-driven type casting is low-cost. Columns are only cast if their current
    type doesn't match the target type, avoiding unnecessary conversions.

**Date Parsing**
    Excel serial dates (numeric) are converted efficiently using arithmetic. String
    dates use apply_date_parsing() with multiple format fallback for robustness.

**Sheet Selection**
    Reading a specific sheet is efficient - only that sheet's data is loaded into
    memory, not the entire workbook.
"""

import fnmatch
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method, validate_file_format
from ._date_handler import apply_date_parsing
from ._registry import register_parser


def get_sheet_names(file_path: Path) -> list[str]:
    """
    Extract all sheet names from an Excel file.

    Parameters
    ----------
    file_path : Path
        Path to the Excel file (.xlsx)

    Returns
    -------
    list[str]
        List of sheet names in the workbook
    """
    with zipfile.ZipFile(file_path, "r") as z:
        wb = z.read("xl/workbook.xml")
        root = ET.fromstring(wb)
        ns = {"main": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
        sheets = root.findall(".//main:sheet", ns)
        return [sheet.get("name") for sheet in sheets]


def find_matching_sheet(
    file_path: Path, sheet_patterns: str | list[str]
) -> str | None:
    """
    Find the first sheet name that matches any of the given patterns.

    Parameters
    ----------
    file_path : Path
        Path to the Excel file
    sheet_patterns : str | list[str]
        Single pattern or list of patterns to match against sheet names.
        Supports fnmatch wildcards (*, ?, [seq], [!seq])

    Returns
    -------
    str | None
        Name of the first matching sheet, or None if no match found

    Examples
    --------
    >>> find_matching_sheet(path, "Patient Level")
    'Patient Level'
    >>> find_matching_sheet(path, ["Patient Level", "HC Reach Report ????-??-??"])
    'HC Reach Report 2025-10-31'
    >>> find_matching_sheet(path, "HC Reach Report *")
    'HC Reach Report 2025-10-31'
    """
    available_sheets = get_sheet_names(file_path)

    # Convert single pattern to list
    if isinstance(sheet_patterns, str):
        sheet_patterns = [sheet_patterns]

    # Try each pattern until we find a match
    for pattern in sheet_patterns:
        for sheet_name in available_sheets:
            if fnmatch.fnmatch(sheet_name, pattern):
                return sheet_name

    return None


def detect_header_row(
    file_path: Path, sheet_name: str | None, schema: Any, max_search_rows: int = 5
) -> int | None:
    """
    Auto-detect which row contains the column headers.

    Reads the first few rows of the Excel file and looks for rows that contain
    expected column names from the schema. Returns the 0-based index of the
    header row.

    Parameters
    ----------
    file_path : Path
        Path to the Excel file
    sheet_name : str | None
        Name of sheet to read (or None for first sheet)
    schema : Any
        Schema object with column definitions
    max_search_rows : int
        Maximum number of rows to search for headers (default 5)

    Returns
    -------
    int | None
        0-based index of the header row, or None if not found

    Examples
    --------
    File with headers in row 1:
        Row 0: "Demographic Data" (section header)
        Row 1: "MBI", "Patient First Name", ... → Returns 1

    File with headers in row 2:
        Row 0: "Demographic Data"
        Row 1: (empty)
        Row 2: "MBI", "Patient First Name", ... → Returns 2
    """
    try:
        # Get expected column names from schema
        expected_names = set()

        # Handle both dict-based schemas and Pydantic dataclass schemas
        if schema and hasattr(schema, "columns"):
            # Dict-based schema
            for col_def in schema.columns:
                # Add both the schema name and output_name
                if "name" in col_def:
                    col_name = col_def["name"].lower()
                    expected_names.add(col_name)
                    # Add variations for Patient/Beneficiary
                    if "patient" in col_name:
                        expected_names.add(col_name.replace("patient", "beneficiary"))
                    elif "beneficiary" in col_name:
                        expected_names.add(col_name.replace("beneficiary", "patient"))

                if "output_name" in col_def:
                    # Convert output_name to potential source names
                    output = col_def["output_name"].lower()
                    expected_names.add(output)
                    # Convert underscore to space for matching file headers
                    expected_names.add(output.replace("_", " "))

                    # Add variations (e.g., "mbi" → "beneficiary mbi id")
                    if "mbi" in output:
                        expected_names.update(["mbi", "beneficiary mbi id", "beneficiary id"])

                    # Add Patient/Beneficiary variations for both underscore and space versions
                    if "patient" in output:
                        beneficiary_variant = output.replace("patient", "beneficiary")
                        expected_names.add(beneficiary_variant)
                        expected_names.add(beneficiary_variant.replace("_", " "))
                    elif "beneficiary" in output:
                        patient_variant = output.replace("beneficiary", "patient")
                        expected_names.add(patient_variant)
                        expected_names.add(patient_variant.replace("_", " "))
        elif schema and hasattr(schema, "__dataclass_fields__"):
            # Pydantic dataclass schema
            from pydantic.fields import FieldInfo
            for field_name, field_info in schema.__dataclass_fields__.items():
                # Add the field name itself
                expected_names.add(field_name.lower().replace("_", " "))
                expected_names.add(field_name.lower())

                # Check for alias in field metadata
                if isinstance(field_info.default, FieldInfo):
                    if hasattr(field_info.default, "alias") and field_info.default.alias:
                        expected_names.add(field_info.default.alias.lower())

                # Add variations for common fields
                if field_name.lower() == "mbi":
                    expected_names.update(["mbi", "beneficiary mbi id", "beneficiary id"])
                elif "patient" in field_name.lower() or "beneficiary" in field_name.lower():
                    # Add both patient and beneficiary variations
                    patient_variant = field_name.lower().replace("beneficiary", "patient")
                    beneficiary_variant = field_name.lower().replace("patient", "beneficiary")
                    expected_names.add(patient_variant.replace("_", " "))
                    expected_names.add(beneficiary_variant.replace("_", " "))

        if not expected_names:
            return None

        # Read first few rows without headers
        read_options = {"source": file_path, "engine": "calamine", "read_options": {"header_row": None, "n_rows": max_search_rows}}
        if sheet_name:
            read_options["sheet_name"] = sheet_name

        df = pl.read_excel(**read_options)

        # Check each row to see if it contains expected column names
        for row_idx in range(min(max_search_rows, len(df))):
            row_values = df.row(row_idx, named=False)

            # Convert row values to lowercase strings for comparison
            row_strings = set()
            for val in row_values:
                if val and isinstance(val, str):
                    row_strings.add(val.lower().strip())

            # Check if this row contains at least 2 expected column names
            matches = row_strings.intersection(expected_names)
            if len(matches) >= 2:
                return row_idx

        return None

    except Exception:  # ALLOWED: Fallback behavior if detection fails
        return None


@register_parser("excel", metadata={"extensions": [".xlsx", ".xls"]})
@validate_file_format(param_name="file_path", formats=[".xlsx", ".xls"])
@parser_method(
    threshold=3.0,
    validate_path="file_path",
)
def parse_excel(
    file_path: Path, schema: Any, limit: int | None = None, sheet_name: str | None = None
) -> pl.LazyFrame:
    """
    Parse Excel spreadsheet files (.xlsx, .xls).

        Excel files are commonly used for manual data entry and reporting.
        This parser handles both modern (.xlsx) and legacy (.xls) formats,
        with support for sheet selection and data type preservation.

        CRITICAL: This parser is SCHEMA-DRIVEN:
        - Ignores headers in the file
        - Maps columns by POSITION to schema names
        - Forces all columns to correct types from schema

        Parameters

        file_path : Path
            Path to the Excel file (.xlsx or .xls)
        schema : Any
            TableMetadata object containing column definitions with:
            - columns: List of column definitions (name, output_name, data_type, position)
            - file_format: Dict with sheet_name, skip_rows, header_row, has_header
        limit : int | None, optional
            Maximum number of rows to read (useful for testing), by default None
        sheet_name : str | None, optional
            Name of sheet to read (overrides schema.file_format.sheet_name), by default None

        Returns

        pl.LazyFrame
            Lazily loaded Excel data with schema-defined columns and types
    """
    # Get sheet name and other options from schema or parameter
    file_format = {}
    if schema and hasattr(schema, "file_format"):
        file_format = schema.file_format

    if not sheet_name:
        sheet_name = file_format.get("sheet_name")

    # If sheet_name is provided (string or list), find matching sheet using pattern matching
    if sheet_name:
        matched_sheet = find_matching_sheet(file_path, sheet_name)
        if matched_sheet is None:
            available_sheets = get_sheet_names(file_path)
            patterns = sheet_name if isinstance(sheet_name, list) else [sheet_name]
            raise ValueError(
                f"No sheet found matching patterns {patterns}. "
                f"Available sheets: {available_sheets}"
            )
        sheet_name = matched_sheet

    # Build read_options for the underlying engine
    engine_read_options: dict[str, Any] = {}

    # Handle skip_rows for calamine engine (maps to skip_rows in fastexcel)
    if "skip_rows" in file_format:
        engine_read_options["skip_rows"] = file_format["skip_rows"]

    # Handle header_row option with auto-detection support
    header_row_value = file_format.get("header_row") if "header_row" in file_format else None

    # Auto-detect header row if not specified or set to "auto"
    if header_row_value == "auto" or (header_row_value is None and "has_header" not in file_format):
        detected_header_row = detect_header_row(file_path, sheet_name, schema)
        if detected_header_row is not None:
            engine_read_options["header_row"] = detected_header_row
        # else: leave unset, polars will use default behavior
    elif "has_header" in file_format and not file_format["has_header"]:
        engine_read_options["header_row"] = None
    elif header_row_value is not None and header_row_value != "auto":
        engine_read_options["header_row"] = header_row_value

    # Read Excel file
    read_options: dict[str, Any] = {
        "source": file_path,
        "engine": "calamine",  # Fast Excel reader
    }

    if sheet_name:
        read_options["sheet_name"] = sheet_name

    if engine_read_options:
        read_options["read_options"] = engine_read_options

    # Read the Excel file
    df = pl.read_excel(**read_options)

    # Apply limit if needed
    if limit:
        df = df.head(limit)

    # Map source columns to schema fields.
    if schema and hasattr(schema, "columns"):
        if file_format.get("header_driven"):
            df = _apply_header_driven_mapping(df, schema)
        else:
            df = _apply_positional_mapping(df, schema)

    # Apply date parsing for date columns
    df_lazy = df.lazy()
    df_lazy = apply_date_parsing(df_lazy, schema)

    return df_lazy


def _schema_output_name(col_def: dict[str, Any]) -> str:
    """Resolve the destination column name for a schema column definition."""
    return col_def.get("output_name", col_def.get("name"))


def _schema_polars_dtype(col_def: dict[str, Any]) -> Any | None:
    """Resolve the Polars dtype for a schema column definition, or None to skip casting."""
    data_type = col_def.get("data_type", "string").lower()
    if data_type in ("int", "integer"):
        return pl.Int64
    if data_type in ("float", "decimal"):
        return pl.Float64
    if data_type == "boolean":
        return pl.Boolean
    if data_type == "string":
        return pl.Utf8
    return None


def _cast_columns_to_schema(df: pl.DataFrame, dtypes: dict[str, Any]) -> pl.DataFrame:
    """
    Cast columns to schema-declared dtypes in-place on a DataFrame.

    Skips columns whose current dtype already matches. Boolean cast accepts
    common string truthy/falsy representations (0/1, true/false, yes/no, ...).
    """
    if not dtypes:
        return df

    cast_exprs = []
    for col_name in df.columns:
        target_dtype = dtypes.get(col_name)
        if target_dtype is None:
            cast_exprs.append(pl.col(col_name))
            continue

        current_dtype = df.schema[col_name]
        if target_dtype == pl.Int64 and current_dtype not in (pl.Int64, pl.Int32):
            cast_exprs.append(pl.col(col_name).cast(pl.Int64, strict=False))
        elif target_dtype == pl.Float64 and current_dtype not in (pl.Float64, pl.Float32):
            cast_exprs.append(pl.col(col_name).cast(pl.Float64, strict=False))
        elif target_dtype == pl.Boolean and current_dtype != pl.Boolean:
            cast_exprs.append(
                pl.when(
                    pl.col(col_name)
                    .cast(pl.Utf8)
                    .is_in(["1", "true", "True", "TRUE", "T", "t", "yes", "Yes", "YES"])
                )
                .then(True)
                .when(
                    pl.col(col_name)
                    .cast(pl.Utf8)
                    .is_in(["0", "false", "False", "FALSE", "F", "f", "no", "No", "NO"])
                )
                .then(False)
                .otherwise(None)
                .alias(col_name)
            )
        elif target_dtype == pl.Utf8 and current_dtype != pl.Utf8:
            cast_exprs.append(pl.col(col_name).cast(pl.Utf8, strict=False))
        else:
            cast_exprs.append(pl.col(col_name))

    return df.select(cast_exprs)


def _apply_positional_mapping(df: pl.DataFrame, schema: Any) -> pl.DataFrame:
    """
    Map file columns to schema columns by ordinal position.

    This is the default behavior — file headers are ignored; the Nth file
    column becomes the Nth schema column. Extra file columns are dropped.
    """
    actual_col_count = len(df.columns)
    new_columns: list[str] = []
    dtypes: dict[str, Any] = {}

    for i, col_def in enumerate(schema.columns):
        if i >= actual_col_count:
            break
        output_name = _schema_output_name(col_def)
        new_columns.append(output_name)
        target_dtype = _schema_polars_dtype(col_def)
        if target_dtype is not None:
            dtypes[output_name] = target_dtype

    if not new_columns:
        return df

    df = df.select(df.columns[: len(new_columns)])
    df.columns = new_columns
    return _cast_columns_to_schema(df, dtypes)


def _normalize_header(name: str) -> str:
    """Normalize an Excel header for alias matching: lowercase, collapse whitespace, strip."""
    if name is None:
        return ""
    # NBSP and other whitespace collapse to single space, then underscores too.
    cleaned = " ".join(str(name).replace("\xa0", " ").replace("_", " ").split())
    return cleaned.lower()


def _apply_header_driven_mapping(df: pl.DataFrame, schema: Any) -> pl.DataFrame:
    """
    Map file columns to schema fields by matching headers against declared aliases.

    Schemas opt in via ``@with_parser(header_driven=True)``. Every schema field
    that participates must declare its source header(s) via pydantic ``alias=``
    or ``validation_alias=AliasChoices(...)``. File headers are normalized
    (lowercase, whitespace/underscore-insensitive) before matching, so
    ``"Entity ID"`` and ``"Entity_ID"`` both bind to the same field.

    File columns whose headers don't match any alias are dropped. Schema fields
    with no matching file column are added as nulls so the downstream output
    shape is stable across heterogeneous source layouts.
    """
    # Build alias → output_name lookup for the schema.
    alias_to_output: dict[str, str] = {}
    dtypes: dict[str, Any] = {}
    schema_output_order: list[str] = []
    schema_output_dtype: dict[str, Any] = {}

    for col_def in schema.columns:
        output_name = _schema_output_name(col_def)
        schema_output_order.append(output_name)
        target_dtype = _schema_polars_dtype(col_def)
        if target_dtype is not None:
            schema_output_dtype[output_name] = target_dtype

        # The output_name itself is always a valid alias (matches a file that
        # already uses the canonical schema header).
        alias_to_output.setdefault(_normalize_header(output_name), output_name)

        for alias in col_def.get("aliases", []) or []:
            alias_to_output.setdefault(_normalize_header(alias), output_name)

    # Rename file columns whose normalized header matches a known alias; drop
    # unknown columns. If multiple file columns map to the same field, only
    # the first one wins (subsequent ones are dropped).
    rename_map: dict[str, str] = {}
    claimed: set[str] = set()
    for file_col in df.columns:
        output_name = alias_to_output.get(_normalize_header(file_col))
        if output_name and output_name not in claimed:
            rename_map[file_col] = output_name
            claimed.add(output_name)

    if rename_map:
        df = df.rename(rename_map)

    # Drop file columns that didn't bind to any schema field.
    df = df.select([c for c in df.columns if c in claimed])

    # Add schema fields that weren't found in the file as null columns.
    missing = [name for name in schema_output_order if name not in claimed]
    if missing:
        df = df.with_columns([pl.lit(None, dtype=pl.Utf8).alias(name) for name in missing])
        # Track these so casts give them the correct dtype.
        for name in missing:
            target = schema_output_dtype.get(name)
            if target is not None:
                dtypes[name] = target

    # Cast file-sourced columns to declared dtypes too.
    for name in claimed:
        target = schema_output_dtype.get(name)
        if target is not None:
            dtypes[name] = target

    # Reorder to schema order so downstream consumers see a stable layout.
    df = df.select(schema_output_order)

    return _cast_columns_to_schema(df, dtypes)
