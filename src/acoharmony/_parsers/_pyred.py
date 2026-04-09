# © 2025 HarmonyCares
# All rights reserved.

"""
PYRED (Provider-Specific Payment Reduction Report) parser.

 a specialized parser for PYRED Excel files that extends
the base excel_multi_sheet parser with PYRED-specific metadata extraction logic.
"""

import re
from pathlib import Path
from typing import Any

import polars as pl

from ._excel_multi_sheet import ExcelMultiSheetConfig, parse_sheet_matrix
from ._transformations import apply_date_parsing


def extract_pyred_metadata(file_path: Path, sheet_index: int) -> dict[str, str | None]:
    """
    Extract PYRED-specific metadata from Excel file with fallback logic.

        PYRED files have inconsistent row structures across different file versions:
        - Some files: Row 2 has year, Row 3 col 7 has period
        - Other files: Row 1 has both year and period at different columns

        This function searches multiple locations to handle all variations.

        Parameters

        file_path : Path
            Path to PYRED Excel file
        sheet_index : int
            Sheet index to read from (0-based)

        Returns

        dict[str, str | None]
            Dictionary with 'performance_year' and 'report_period' keys
    """
    try:
        df = pl.read_excel(
            file_path, sheet_id=sheet_index + 1, read_options={"header_row": None, "skip_rows": 0}
        )

        performance_year = None
        report_period = None

        # Search for performance year in first 4 rows, column 0
        for row_idx in range(min(4, len(df))):
            cell_value = df.row(row_idx)[0] if len(df.row(row_idx)) > 0 else None
            if cell_value and "Performance Year" in str(cell_value):
                match = re.search(r"\d{4}", str(cell_value))
                if match:
                    performance_year = match.group(0)
                    break

        # Search for report period in multiple known locations
        # Different file versions have report period in different cells
        search_locations = [
            (1, 4),  # Row 1, Col 4 (newer file format)
            (3, 7),  # Row 3, Col 7 (older file format with blank row)
        ]

        for row_idx, col_idx in search_locations:
            if row_idx < len(df):
                row = df.row(row_idx)
                if col_idx < len(row):
                    cell_value = row[col_idx]
                    if cell_value and "experience" in str(cell_value).lower():
                        match = re.search(r"([A-Za-z]+\s+\d{4})", str(cell_value))
                        if match:
                            report_period = match.group(1)
                            break

        return {
            "performance_year": performance_year,
            "report_period": report_period,
        }

    except Exception:  # ALLOWED: Returns None to indicate error
        # If extraction fails, return None values
        return {
            "performance_year": None,
            "report_period": None,
        }


def parse_pyred(
    file_path: Path,
    schema: Any,
    limit: int | None = None,
    sheet_types: list[str] | None = None,
) -> pl.LazyFrame:
    """
    Parse PYRED (Provider-Specific Payment Reduction Report) Excel files.

        This parser extends the base excel_multi_sheet parser with PYRED-specific
        metadata extraction logic. It handles the inconsistent file structures found
        across different PYRED file versions while using the schema-driven approach
        for column mapping and data extraction.

        Parameters

        file_path : Path
            Path to PYRED Excel workbook
        schema : Any
            Schema object with sheets configuration from pyred.yml
        limit : int | None, optional
            Max rows per sheet (for testing/debugging)
        sheet_types : list[str] | None, optional
            Filter to specific sheet types (e.g., ['physician', 'hh'])
            If None, processes all sheets: inpatient, snf, hh, hospice, outpatient, physician

        Returns

        pl.LazyFrame
            Combined data from all sheets with these columns:
            - All schema-defined columns (provider_type, NPIs, names, etc.)
            - sheet_type: Sheet identifier
            - performance_year: Extracted from header (e.g., "2025")
            - report_period: Extracted from header (e.g., "Jan 2025")

        Parse specific sheets only:
        ['physician', 'hh']
    """
    # Get configuration from schema
    if isinstance(schema, dict):
        file_format = schema.get("file_format", {})
        sheets_list = schema.get("sheets", [])
    elif hasattr(schema, "file_format"):
        file_format = (
            schema.file_format if isinstance(schema.file_format, dict) else vars(schema.file_format)
        )
        sheets_list = getattr(schema, "sheets", None)
        if sheets_list is None:
            sheets_list = []
        elif not isinstance(sheets_list, list):
            sheets_list = list(sheets_list)
    else:
        raise ValueError("Schema must have file_format for PYRED parsing")

    if "sheet_config" not in file_format:
        raise ValueError("Schema must have file_format.sheet_config for PYRED parsing")

    if not sheets_list:
        raise ValueError("Schema must have 'sheets' list for PYRED parsing")

    # Parse sheet_config into Pydantic model
    sheet_config_dict = file_format["sheet_config"]
    if not isinstance(sheet_config_dict, dict):
        sheet_config_dict = vars(sheet_config_dict)
    config = ExcelMultiSheetConfig(**sheet_config_dict)

    # Process each sheet
    all_sheets = []

    for sheet_def in sheets_list:
        if not isinstance(sheet_def, dict):
            sheet_def = vars(sheet_def)

        sheet_type = sheet_def.get("sheet_type")
        if sheet_types and sheet_type not in sheet_types:
            continue

        sheet_index = sheet_def.get("sheet_index")
        columns_raw = sheet_def.get("columns", [])

        # Convert columns to dict format
        columns = []
        for col in columns_raw:
            if isinstance(col, dict):
                columns.append(col)
            else:
                columns.append(vars(col))

        # Parse this sheet using base excel_multi_sheet logic
        df_sheet, col_header_metadata = parse_sheet_matrix(file_path, sheet_index, config, columns)

        # Add sheet_type column
        df_sheet = df_sheet.with_columns(pl.lit(sheet_type).alias("sheet_type"))

        # Add extracted header metadata as columns (if any)
        # Only add if column doesn't already exist to avoid duplicates
        existing_columns = set(df_sheet.columns)
        for _col_name, metadata_dict in col_header_metadata.items():
            for field_name, field_value in metadata_dict.items():
                if field_name not in existing_columns:
                    df_sheet = df_sheet.with_columns(
                        pl.lit(field_value, dtype=pl.Utf8).alias(field_name)
                    )
                    existing_columns.add(field_name)

        # Extract and add PYRED-specific metadata
        pyred_metadata = extract_pyred_metadata(file_path, sheet_index)
        df_sheet = df_sheet.with_columns(
            [
                pl.lit(pyred_metadata["performance_year"], dtype=pl.Utf8).alias("performance_year"),
                pl.lit(pyred_metadata["report_period"], dtype=pl.Utf8).alias("report_period"),
            ]
        )

        # Apply row limit if specified
        if limit:
            df_sheet = df_sheet.head(limit)

        all_sheets.append(df_sheet)

    if not all_sheets:
        raise ValueError(f"No sheets found matching sheet_types={sheet_types}")

    # Combine all sheets using diagonal (allows different column sets per sheet)
    df_combined = pl.concat(all_sheets, how="diagonal_relaxed")

    # Convert to lazy frame
    df_lazy = df_combined.lazy()

    # Apply date parsing transformations from schema
    df_lazy = apply_date_parsing(df_lazy, schema)

    return df_lazy
