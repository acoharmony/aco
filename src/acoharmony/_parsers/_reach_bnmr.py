"""
REACH BNMR (Benchmark Report) parser.

 a specialized parser for REACH BNMR Excel files that extends
the base excel_multi_sheet parser with BNMR-specific metadata extraction logic.

REACH BNMR files have evolved over time with different sheet counts:
- 17 sheets (2025+): Full structure with historical benchmark sheets
- 16 sheets (mid-2024): Transitional version
- 15 sheets (2023-early 2024): Earlier version without historical benchmark sheets
- 3 sheets (rare): Truncated preliminary reports

Key differences across versions:
- v17: Sheets 0-7 metadata, sheets 8-16 data (historical benchmarks in sheets 2-3)
- v15: Sheets 0-5 metadata, sheets 6-14 data (no historical benchmark sheets)

Note: MSSP BNMRK files have different structure and use mssp_bnmr parser.

Key features:
- Version detection based on sheet count
- Dynamic sheet index mapping for different versions
- Dynamic year extraction (years vary by performance year)
- Matrix field extraction (global metadata from sheet 0)
- Named field extraction (specific calculations from metadata sheets)
- Error handling for sheets with formula errors
"""

import re
from pathlib import Path
from typing import Any

import polars as pl

from acoharmony._exceptions import ParseError
from ._excel_multi_sheet import ExcelMultiSheetConfig, parse_sheet_matrix
from ._transformations import apply_date_parsing


def is_sheet_empty(file_path: Path, sheet_index: int) -> bool:
    """
Check if an Excel sheet is empty (0 rows or only header row).

Parameters
----------
file_path : Path
    Path to Excel file
sheet_index : int
    Sheet index to check (0-based)

Returns
-------
bool
    True if sheet is empty or has only header row
"""
    try:
        df = pl.read_excel(
            file_path,
            sheet_id=sheet_index + 1,
            read_options={"header_row": None, "skip_rows": 0, "n_rows": 2},
        )
        return len(df) == 0
    except Exception:
        return True


def list_bnmr_sheet_names(file_path: Path) -> list[str]:
    """
    Return the sheet name list of a BNMR workbook in worksheet order.

    Uses openpyxl in read-only mode (fastest / cheapest way to get sheet
    names without parsing any cell data). Returns an empty list if the
    workbook cannot be opened, so callers can treat "no sheets found" as
    a graceful skip rather than a crash.
    """
    try:
        import openpyxl

        wb = openpyxl.load_workbook(file_path, read_only=True)
        try:
            return list(wb.sheetnames)
        finally:
            wb.close()
    except Exception:
        return []


def resolve_bnmr_sheet_indices(
    file_path: Path, schema_sheet_names: list[str]
) -> tuple[list[str], dict[str, int | None]]:
    """
    Resolve each schema-declared ``sheet_name`` to its actual index in the file.

    BNMR files have evolved through at least eight distinct layouts across
    PY2023–PY2025 deliveries. Rather than maintain a fragile
    sheet-count-to-mapping lookup table, this resolver reads the actual
    sheet names from the workbook and looks each one up by name:

    - **Exact match first.** A schema sheet named ``DATA_CAP`` matches
      an actual sheet named ``DATA_CAP`` (case-insensitive).
    - **Not found → None.** If a schema sheet is not present in the file
      (e.g. ``BENCHMARK_HISTORICAL_AD`` in a PY2023 file that predates
      the historical-benchmark split), its entry in the returned
      dictionary is ``None`` and downstream code skips the sheet.

    Returns:
        A ``(actual_sheets, mapping)`` tuple where ``actual_sheets`` is
        the full ordered list of worksheet names (for diagnostics) and
        ``mapping`` is ``{schema_sheet_name: actual_index | None}``.

    This replaces the old sheet-count-driven ``detect_bnmr_version``
    which hard-coded mappings for 15-, 16-, and 17-sheet layouts and
    silently dropped sheets whose index shifted in undocumented layouts
    (e.g. the Sept 2025 16-sheet files where DATA_CAP is at index 14,
    not the index 14/15 the old detector assumed for 16-sheet files).
    """
    actual_sheets = list_bnmr_sheet_names(file_path)
    actual_lower = {name.upper(): i for i, name in enumerate(actual_sheets)}

    mapping: dict[str, int | None] = {}
    for schema_name in schema_sheet_names:
        key = schema_name.upper()
        mapping[schema_name] = actual_lower.get(key)

    return actual_sheets, mapping


def extract_dynamic_years(
    file_path: Path, sheet_index: int, year_header_row: int, year_columns: list[int]
) -> dict[int, str]:
    """
Extract year values dynamically from a specific row.

    Parameters

    file_path : Path
        Path to BNMR Excel file
    sheet_index : int
        Sheet index to read from (0-based)
    year_header_row : int
        Row index where year headers are located
    year_columns : list[int]
        Column indices to extract years from

    Returns

    dict[int, str]
        Mapping of column index to year string (e.g., {2: '2017', 3: '2018'})
"""
    try:
        df = pl.read_excel(
            file_path,
            sheet_id=sheet_index + 1,
            read_options={"header_row": None, "skip_rows": 0},
            infer_schema_length=0,
        )

        if year_header_row >= len(df):
            return {}

        year_row = df.row(year_header_row)
        year_map = {}

        for col_idx in year_columns:
            if not col_idx < len(year_row):
                continue
            cell_value = year_row[col_idx]
            if not cell_value:
                continue

            match = re.search(r'\d{4}', str(cell_value))
            if not match:
                continue
            year_map[col_idx] = match.group(0)

        return year_map

    except Exception as e:
        raise ParseError(
            f"Failed to extract dynamic years from sheet {sheet_index}",
            original_error=e,
            why="Could not read Excel sheet or extract year values from header row",
            how="Check Excel file is valid and sheet contains expected structure",
            metadata={
                "file_path": str(file_path),
                "sheet_index": sheet_index,
                "year_header_row": year_header_row,
            },
        ) from e


def extract_reference_year(
    file_path: Path, sheet_index: int, ry_row: int, ry_col: int
) -> str | None:
    """
Extract reference year (e.g., "RY2022") from risk score sheets.

    Parameters

    file_path : Path
        Path to BNMR Excel file
    sheet_index : int
        Sheet index to read from (0-based)
    ry_row : int
        Row index where reference year is located
    ry_col : int
        Column index where reference year is located

    Returns

    str | None
        Reference year string (e.g., "RY2022") or None if not found
"""
    try:
        if sheet_index == 6:
            df = pl.read_excel(
                file_path,
                sheet_id=sheet_index + 1,
                engine="xlsx2csv",
                has_header=False,
            )
        else:
            df = pl.read_excel(
                file_path,
                sheet_id=sheet_index + 1,
                read_options={"header_row": None, "skip_rows": 0, "n_rows": ry_row + 1},
            )

        if ry_row >= len(df):
            return None

        row = df.row(ry_row)
        if ry_col < len(row):
            cell_value = row[ry_col]
            if cell_value:
                match = re.search(r'RY\d{4}', str(cell_value))
                if match:
                    return match.group(0)

        return None
    except Exception:
        return None


def extract_named_fields(
    file_path: Path, sheet_index: int, named_fields_config: list
) -> dict[str, Any]:
    """
Extract named fields (specific calculation results) from a sheet.

    Named fields are specific cell values that represent important calculations
    (e.g., benchmark_all_aligned_total from row 20, col 4 of financial_settlement).

    Parameters

    file_path : Path
        Path to BNMR Excel file
    sheet_index : int
        Sheet index to read from (0-based)
    named_fields_config : list
        List of named field configurations with row, column, field_name

    Returns

    dict[str, Any]
        Dictionary of field_name -> value for all named fields
"""
    if not named_fields_config:
        return {}

    named_values = {}

    try:
        df = pl.read_excel(
            file_path,
            sheet_id=sheet_index + 1,
            read_options={"header_row": None, "skip_rows": 0},
            infer_schema_length=0,
        )

        for field_config in named_fields_config:
            if isinstance(field_config, dict):
                row_idx = field_config.get("row")
                col_idx = field_config.get("column")
                field_name = field_config.get("field_name")
                field_config.get("data_type", "string")
            else:
                row_idx = getattr(field_config, "row", None)
                col_idx = getattr(field_config, "column", None)
                field_name = getattr(field_config, "field_name", None)
                getattr(field_config, "data_type", "string")

            if row_idx is None or col_idx is None or not field_name:
                continue

            try:
                if row_idx < len(df) and col_idx < len(df.columns):
                    row = df.row(row_idx)
                    if col_idx < len(row):
                        cell_value = row[col_idx]
                        named_values[field_name] = cell_value
                    else:  # pragma: no cover – len(row) == len(df.columns), guarded by line 312
                        named_values[field_name] = None
                else:
                    named_values[field_name] = None
            except Exception:
                named_values[field_name] = None

    except Exception as e:
        raise ParseError(
            f"Failed to read sheet {sheet_index} for named field extraction",
            original_error=e,
            why="Could not read Excel sheet to extract named fields",
            how="Check Excel file is valid and sheet index is correct",
            metadata={
                "file_path": str(file_path),
                "sheet_index": sheet_index,
            },
        ) from e

    return named_values


def extract_bnmr_matrix_fields(file_path: Path, schema: Any) -> dict[str, Any]:
    """
Extract matrix fields (global metadata) from BNMR file.

    Matrix fields are specific cell values extracted from known positions,
    typically from sheet 0 (REPORT_PARAMETERS).

    Parameters

    file_path : Path
        Path to BNMR Excel file
    schema : Any
        Schema object with matrix_fields configuration

    Returns

    dict[str, Any]
        Dictionary of field_name -> value for all matrix fields
"""
    if isinstance(schema, dict):
        matrix_fields_config = schema.get("matrix_fields", [])
    else:
        matrix_fields_config = getattr(schema, "matrix_fields", [])

    if not matrix_fields_config:
        return {}

    metadata = {}

    if is_sheet_empty(file_path, 0):
        print("Sheet 0 is empty, skipping matrix field extraction")
        return {}

    try:
        df = pl.read_excel(
            file_path,
            sheet_id=1,
            read_options={"header_row": None, "skip_rows": 0},
        )

        for field_config in matrix_fields_config:
            if isinstance(field_config, dict):
                matrix = field_config.get("matrix")
                field_name = field_config.get("field_name")
                field_config.get("data_type")
                extract_pattern = field_config.get("extract_pattern")
            else:
                matrix = getattr(field_config, "matrix", None)
                field_name = getattr(field_config, "field_name", None)
                getattr(field_config, "data_type", "string")
                extract_pattern = getattr(field_config, "extract_pattern", None)

            if not matrix or not field_name:
                continue

            sheet_idx, row_idx, col_idx = matrix

            if sheet_idx != 0:
                continue

            try:
                if row_idx < len(df):
                    row = df.row(row_idx)
                    if col_idx < len(row):
                        cell_value = row[col_idx]

                        if cell_value is not None:
                            value_str = str(cell_value)

                            if extract_pattern:
                                match = re.search(extract_pattern, value_str)
                                if match:
                                    metadata[field_name] = match.group(0)
                                else:
                                    metadata[field_name] = None
                            else:
                                metadata[field_name] = cell_value
                        else:
                            metadata[field_name] = None
                    else:
                        metadata[field_name] = None
                else:
                    metadata[field_name] = None
            except Exception:
                metadata[field_name] = None

    except Exception as e:
        raise ParseError(
            "Failed to read sheet 0 for matrix field extraction",
            original_error=e,
            why="Could not read Excel sheet 0 to extract global metadata fields",
            how="Check Excel file is valid and sheet 0 (REPORT_PARAMETERS) exists",
            metadata={"file_path": str(file_path)},
        ) from e

    return metadata


def parse_reach_bnmr(
    file_path: Path,
    schema: Any,
    limit: int | None = None,
    sheet_types: list[str] | None = None,
) -> pl.LazyFrame:
    """
Parse BNMR (Benchmark Report) Excel files.

    This parser handles the complex structure of BNMR files with:
    - Dynamic year extraction for sheets with varying year columns
    - Matrix field extraction for global metadata
    - Named field extraction for specific calculation results
    - Standard DATA_ sheet parsing for tabular data
    - Error handling for sheets with formula errors

    Parameters

    file_path : Path
        Path to BNMR Excel workbook
    schema : Any
        Schema object with sheets configuration from bnmr.yml
    limit : int | None, optional
        Max rows per sheet (for testing/debugging)
    sheet_types : list[str] | None, optional
        Filter to specific sheet types
        If None, processes all sheets

    Returns

    pl.LazyFrame
        Combined data from all sheets with:
        - All schema-defined columns
        - sheet_type identifier
        - Matrix fields (global metadata like performance_year, aco_id)
        - Named fields (specific calculation results)

    Parse only DATA_ sheets:
    ['claims', 'risk', 'county']
"""
    # Extract file_format and sheets from schema
    if isinstance(schema, dict):
        file_format = schema.get("file_format", {})
        sheets_list = schema.get("sheets", [])
    elif hasattr(schema, "file_format"):
        # Namespace / object schema
        file_format = (
            schema.file_format
            if isinstance(schema.file_format, dict)
            else vars(schema.file_format)
        )
        sheets_list = getattr(schema, "sheets", None)
        if sheets_list is None:
            sheets_list = []
        elif not isinstance(sheets_list, list):
            sheets_list = list(sheets_list)
    else:
        raise ValueError("Schema must have file_format for BNMR parsing")

    if "sheet_config" not in file_format:
        raise ValueError("Schema must have file_format.sheet_config for BNMR parsing")

    if not sheets_list:
        raise ValueError("Schema must have 'sheets' list for BNMR parsing")

    # Build config from sheet_config
    sheet_config_dict = file_format["sheet_config"]
    if not isinstance(sheet_config_dict, dict):
        sheet_config_dict = vars(sheet_config_dict)
    config = ExcelMultiSheetConfig(**sheet_config_dict)

    # Extract global metadata (matrix fields)
    global_metadata = extract_bnmr_matrix_fields(file_path, schema)

    # Resolve schema sheet names → actual file sheet indices via name lookup.
    # This replaces the old sheet-count-based version detector, which was
    # brittle against CMS layout revisions (see commit message for details).
    schema_sheet_names_list: list[str] = []
    for sheet_def in sheets_list:
        sd = sheet_def if isinstance(sheet_def, dict) else vars(sheet_def)
        name = sd.get("sheet_name")
        if name:
            schema_sheet_names_list.append(name)
    actual_sheets, name_to_index = resolve_bnmr_sheet_indices(
        file_path, schema_sheet_names_list
    )
    print(
        f"Resolved BNMR sheets: {len(actual_sheets)} in file, "
        f"{sum(1 for v in name_to_index.values() if v is not None)} "
        f"of {len(schema_sheet_names_list)} schema sheets matched"
    )

    # Sheet types that use "metadata" (form-like, row/column position) parsing
    # rather than tabular header-matched parsing. Everything else uses the
    # tabular ``parse_sheet_matrix`` path. This set is driven by sheet_type
    # (semantics), not by schema index (position), so new layouts don't break
    # the dispatch.
    METADATA_SHEET_TYPES = {
        "report_parameters",
        "financial_settlement",
        "benchmark_historical_ad",
        "benchmark_historical_esrd",
        "riskscore_ad",
        "riskscore_esrd",
        "stop_loss_charge",
        "stop_loss_payout",
    }

    # Process each sheet
    all_sheets = []

    for sheet_def in sheets_list:
        if not isinstance(sheet_def, dict):
            sheet_def = vars(sheet_def)

        sheet_type = sheet_def.get("sheet_type")
        if sheet_types and sheet_type not in sheet_types:
            continue

        schema_sheet_name = sheet_def.get("sheet_name")
        schema_sheet_index = sheet_def.get("sheet_index")

        # Resolve to actual file index via sheet_name lookup. If the
        # schema row has no sheet_name (shouldn't happen but defensive),
        # fall back to None to skip gracefully.
        actual_sheet_index = (
            name_to_index.get(schema_sheet_name) if schema_sheet_name else None
        )

        # Skip sheets not present in this file
        if actual_sheet_index is None:
            print(
                f"Skipping sheet '{schema_sheet_name}' ({sheet_type}) - "
                f"not present in this BNMR file"
            )
            continue

        sheet_index = actual_sheet_index
        columns_raw = sheet_def.get("columns", [])

        # Skip empty sheets
        if is_sheet_empty(file_path, sheet_index):
            print(f"Skipping empty sheet {sheet_index} ({sheet_type})")
            continue

        # Convert column configs to dicts
        columns = []
        for col in columns_raw:
            if isinstance(col, dict):
                columns.append(col)
            else:
                columns.append(vars(col))

        try:
            if sheet_type not in METADATA_SHEET_TYPES:
                # Data sheets: use parse_sheet_matrix
                df_sheet, col_header_metadata = parse_sheet_matrix(
                    file_path, sheet_index, config, columns
                )

                # Apply column header metadata
                for _col_name, metadata_dict in col_header_metadata.items():
                    for field_name, field_value in metadata_dict.items():
                        df_sheet = df_sheet.with_columns(
                            pl.lit(field_value, dtype=pl.Utf8).alias(field_name)
                        )
            else:
                # Metadata sheets: read raw and extract by position
                df_sheet = pl.read_excel(
                    file_path,
                    sheet_id=sheet_index + 1,
                    read_options={"header_row": None, "skip_rows": 0},
                    infer_schema_length=0,
                )

                # Build select expressions for column extraction
                select_exprs = []
                for col_def in columns:
                    position = col_def.get("position")
                    name = col_def.get("name")
                    col_def.get("data_type", "string")

                    if position is not None and position < len(df_sheet.columns):
                        excel_col_name = df_sheet.columns[position]
                        select_exprs.append(pl.col(excel_col_name).alias(name))
                    elif position is not None:
                        # Position out of range: add null column
                        select_exprs.append(pl.lit(None).alias(name))
                    # else: position is None, skip

                if select_exprs:
                    df_sheet = df_sheet.select(select_exprs)

                # Handle dynamic columns (year-based columns)
                dynamic_columns_config = sheet_def.get("dynamic_columns")
                if dynamic_columns_config:
                    if isinstance(dynamic_columns_config, dict):
                        year_header_row = dynamic_columns_config.get("year_header_row")
                        year_columns = dynamic_columns_config.get("year_columns", [])
                        year_column_prefix = dynamic_columns_config.get(
                            "year_column_prefix", "year_"
                        )
                    else:
                        year_header_row = getattr(dynamic_columns_config, "year_header_row", None)
                        year_columns = getattr(dynamic_columns_config, "year_columns", [])
                        year_column_prefix = getattr(
                            dynamic_columns_config, "year_column_prefix", "year_"
                        )

                    if year_header_row is not None and year_columns:
                        year_map = extract_dynamic_years(
                            file_path, sheet_index, year_header_row, year_columns
                        )

                        rename_dict = {}
                        for col_idx, year_value in year_map.items():
                            for col_def in columns:
                                if col_def.get("position") != col_idx:
                                    continue
                                old_name = col_def.get("name")
                                new_name = f"{year_column_prefix}{year_value}"
                                if old_name in df_sheet.columns:  # pragma: no cover – select at line 591 guarantees name exists
                                    rename_dict[old_name] = new_name
                                break

                        if rename_dict:
                            df_sheet = df_sheet.rename(rename_dict)

        except Exception as e:
            raise ParseError(
                f"Failed to read BNMR sheet {sheet_index} ({sheet_type})",
                original_error=e,
                why="Could not read or parse Excel sheet",
                how="Check Excel file is valid and sheet contains expected structure",
                metadata={
                    "file_path": str(file_path),
                    "sheet_index": sheet_index,
                    "sheet_type": sheet_type,
                },
            ) from e

        # Add sheet_type column
        df_sheet = df_sheet.with_columns(pl.lit(sheet_type).alias("sheet_type"))

        # Extract and add named fields
        named_fields_config = sheet_def.get("named_fields", [])
        if named_fields_config:
            named_fields_list = []
            for nf in named_fields_config:
                if isinstance(nf, dict):
                    named_fields_list.append(nf)
                else:
                    named_fields_list.append(vars(nf))

            named_field_values = extract_named_fields(file_path, sheet_index, named_fields_list)

            for field_name, field_value in named_field_values.items():
                df_sheet = df_sheet.with_columns(pl.lit(field_value).alias(field_name))

        # Add global metadata columns
        for field_name, field_value in global_metadata.items():
            df_sheet = df_sheet.with_columns(
                pl.lit(field_value, dtype=pl.Utf8).alias(field_name)
            )

        # Apply limit if specified
        if limit:
            df_sheet = df_sheet.head(limit)

        all_sheets.append(df_sheet)

    if not all_sheets:
        raise ValueError(f"No sheets found matching sheet_types={sheet_types}")

    # Combine all sheets
    df_combined = pl.concat(all_sheets, how="diagonal_relaxed")

    # Convert to lazy
    df_lazy = df_combined.lazy()

    # Apply date parsing
    df_lazy = apply_date_parsing(df_lazy, schema)

    # Add processing metadata
    from datetime import datetime
    from pathlib import Path

    filename_only = Path(file_path).name
    schema_name = schema.name if hasattr(schema, "name") else "reach_bnmr"

    df_lazy = df_lazy.with_columns([
        pl.lit(datetime.now().isoformat()).alias("processed_at"),
        pl.lit(schema_name).alias("source_file"),
        pl.lit(filename_only).alias("source_filename"),
    ])

    return df_lazy
