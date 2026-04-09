#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""
Schema introspection module for discovering file structures.

This module analyzes data files to extract:
- Sheet names from Excel files
- Column headers from CSV/TXT files
- Data types and sample values
- Structure metadata for schema generation

"""

import csv
from pathlib import Path
from typing import Any

import polars as pl


def introspect_excel(file_path: Path, sample_rows: int = 5) -> dict[str, Any]:
    """
    Introspect Excel file structure.

        Args:
            file_path: Path to Excel file
            sample_rows: Number of sample rows to include

        Returns:
            Dictionary with file metadata:
            {
                'file_type': 'excel',
                'sheets': {
                    'Sheet1': {
                        'columns': ['col1', 'col2', ...],
                        'row_count': 100,
                        'dtypes': {'col1': 'int64', 'col2': 'object', ...},
                        'sample_data': [...],
                        'has_header': True
                    },
                }
            }
    """
    result = {
        'file_type': 'excel',
        'file_path': str(file_path),
        'sheets': {}
    }

    try:
        # polars.read_excel with sheet_id=0 returns a dict of {sheet_name: DataFrame} for all sheets
        sheets_dict = pl.read_excel(file_path, sheet_id=0, read_options={"n_rows": sample_rows + 1})

        for sheet_name, df in sheets_dict.items():
            try:
                sheet_info = {
                    'columns': df.columns,
                    'column_count': len(df.columns),
                    'row_count': len(df),  # Note: only sample rows
                    'dtypes': {col: str(dtype) for col, dtype in df.schema.items()},
                    'has_header': True,
                    'sample_data': df.head(sample_rows).to_dicts() if len(df) > 0 else []
                }

                result['sheets'][sheet_name] = sheet_info

            except Exception as e:  # ALLOWED: Dev tool - returns error dict, continues with remaining sheets
                result['sheets'][sheet_name] = {
                    'error': str(e)
                }

    except Exception as e:  # ALLOWED: Dev tool - returns error dict instead of raising
        result['error'] = str(e)

    return result


def introspect_csv(file_path: Path, delimiter: str = ',', sample_rows: int = 5) -> dict[str, Any]:
    """
    Introspect CSV file structure.

        Args:
            file_path: Path to CSV file
            delimiter: CSV delimiter (default: ',')
            sample_rows: Number of sample rows to include

        Returns:
            Dictionary with file metadata
    """
    result = {
        'file_type': 'csv',
        'file_path': str(file_path),
        'delimiter': delimiter
    }

    try:
        # Try to detect delimiter if not specified
        with open(file_path, encoding='utf-8', errors='replace') as f:
            sample = f.read(4096)
            sniffer = csv.Sniffer()
            try:
                detected_delimiter = sniffer.sniff(sample).delimiter
                if delimiter == ',':  # Only use detected if we're using default
                    delimiter = detected_delimiter
                    result['delimiter'] = delimiter
                    result['delimiter_detected'] = True
            except:  # ALLOWED: Dev tool - delimiter detection fallback, uses default if detection fails  # noqa: E722
                result['delimiter_detected'] = False

        # Read with polars
        df = pl.read_csv(file_path, separator=delimiter, n_rows=sample_rows + 1, encoding='utf-8', truncate_ragged_lines=True)

        result['columns'] = df.columns
        result['column_count'] = len(df.columns)
        result['row_count'] = len(df)  # Note: only sample rows
        result['dtypes'] = {col: str(dtype) for col, dtype in df.schema.items()}
        result['sample_data'] = df.head(sample_rows).to_dicts() if len(df) > 0 else []
        result['has_header'] = True  # Polars assumes this

    except Exception as e:  # ALLOWED: Dev tool - returns error dict instead of raising
        result['error'] = str(e)

    return result


def introspect_delimited(file_path: Path, delimiter: str = '|', has_header: bool = False, sample_rows: int = 5) -> dict[str, Any]:
    """
    Introspect delimited text file (pipe, semicolon, etc.).

        Args:
            file_path: Path to delimited file
            delimiter: Delimiter character
            has_header: Whether file has header row
            sample_rows: Number of sample rows to include

        Returns:
            Dictionary with file metadata
    """
    result = {
        'file_type': 'delimited',
        'file_path': str(file_path),
        'delimiter': delimiter,
        'has_header': has_header
    }

    try:
        if has_header:
            df = pl.read_csv(file_path, separator=delimiter, n_rows=sample_rows + 1, encoding='utf-8', truncate_ragged_lines=True)
            result['columns'] = df.columns
        else:
            # No header - use generic column names
            df = pl.read_csv(file_path, separator=delimiter, has_header=False, n_rows=sample_rows + 1, encoding='utf-8', truncate_ragged_lines=True)
            result['columns'] = [f'column_{i}' for i in range(len(df.columns))]

        result['column_count'] = len(df.columns)
        result['row_count'] = len(df)  # Note: only sample rows
        result['dtypes'] = {col: str(dtype) for col, dtype in zip(result['columns'], df.schema.values(), strict=False)}
        result['sample_data'] = df.head(sample_rows).to_dicts() if len(df) > 0 else []

    except Exception as e:  # ALLOWED: Dev tool - returns error dict instead of raising
        result['error'] = str(e)

    return result


def introspect_file(file_path: Path | str, **kwargs) -> dict[str, Any]:
    """
    Auto-detect and introspect file structure.

        Args:
            file_path: Path to file
            **kwargs: Additional arguments passed to specific introspection functions

        Returns:
            Dictionary with file metadata
    """
    file_path = Path(file_path)

    if not file_path.exists():
        return {
            'error': f'File not found: {file_path}'
        }

    # Detect file type by extension
    ext = file_path.suffix.lower()

    if ext in ['.xlsx', '.xls']:
        return introspect_excel(file_path, **kwargs)
    elif ext == '.csv':
        delimiter = kwargs.pop('delimiter', ',')
        return introspect_csv(file_path, delimiter=delimiter, **kwargs)
    elif ext == '.txt':
        # Try to detect delimiter
        delimiter = kwargs.pop('delimiter', '|')
        has_header = kwargs.pop('has_header', False)
        return introspect_delimited(file_path, delimiter=delimiter, has_header=has_header, **kwargs)
    else:
        return {
            'error': f'Unsupported file type: {ext}',
            'file_path': str(file_path)
        }


def generate_schema_template(metadata: dict[str, Any], schema_name: str | None = None) -> dict[str, Any]:
    """
    Generate a schema template from introspection metadata.

        Args:
            metadata: Metadata from introspection
            schema_name: Optional schema name (defaults to filename)

        Returns:
            Schema template dictionary
    """
    if 'error' in metadata:
        return {'error': metadata['error']}

    file_path = Path(metadata.get('file_path', ''))
    if not schema_name:
        schema_name = file_path.stem

    schema = {
        'version': 2,
        'name': schema_name,
        'description': f'Schema for {file_path.name}',
        'file_format': {
            'type': metadata.get('file_type', 'unknown')
        }
    }

    # Add format-specific details
    if metadata['file_type'] == 'excel':
        schema['file_format']['encoding'] = 'utf-8'
        # If multiple sheets, note them
        if len(metadata.get('sheets', {})) > 1:
            schema['file_format']['sheet_name'] = 0  # Default to first sheet
            schema['_notes'] = {
                'sheets': list(metadata.get('sheets', {}).keys()),
                'message': 'This file has multiple sheets. Update sheet_name as needed.'
            }
    elif metadata['file_type'] == 'csv':
        schema['file_format']['delimiter'] = metadata.get('delimiter', ',')
        schema['file_format']['header'] = True
        schema['file_format']['encoding'] = 'utf-8'
    elif metadata['file_type'] == 'delimited':
        schema['file_format']['delimiter'] = metadata.get('delimiter', '|')
        schema['file_format']['header'] = metadata.get('has_header', False)
        schema['file_format']['encoding'] = 'utf-8'

    # Generate columns
    columns = []

    if metadata['file_type'] == 'excel':
        # For Excel, use first sheet by default
        sheets = metadata.get('sheets', {})
        if sheets:
            first_sheet = list(sheets.values())[0]
            for col_name in first_sheet.get('columns', []):
                dtype = first_sheet.get('dtypes', {}).get(col_name, 'object')
                columns.append({
                    'name': col_name,
                    'output_name': col_name.lower().replace(' ', '_').replace('-', '_'),
                    'data_type': _polars_to_schema_type(dtype),
                    'description': f'{col_name} column'
                })
    else:
        # For CSV/delimited
        for col_name in metadata.get('columns', []):
            dtype = metadata.get('dtypes', {}).get(col_name, 'object')
            columns.append({
                'name': col_name,
                'output_name': col_name.lower().replace(' ', '_').replace('-', '_'),
                'data_type': _polars_to_schema_type(dtype),
                'description': f'{col_name} column'
            })

    schema['columns'] = columns

    return schema


def _polars_to_schema_type(polars_dtype: str) -> str:
    """Convert polars dtype to schema type."""
    dtype_lower = str(polars_dtype).lower()

    if 'int' in dtype_lower:
        return 'integer'
    elif 'float' in dtype_lower or 'double' in dtype_lower:
        return 'float'
    elif 'bool' in dtype_lower:
        return 'boolean'
    elif 'datetime' in dtype_lower or 'timestamp' in dtype_lower:
        return 'timestamp'
    elif 'date' in dtype_lower:
        return 'date'
    elif 'str' in dtype_lower or 'utf8' in dtype_lower:
        return 'string'
    else:
        return 'string'


def introspect_directory(directory: Path | str, pattern: str = "*", file_type_code: int | None = None) -> dict[str, Any]:
    """
    Introspect all files matching pattern in a directory.

        Args:
            directory: Directory to scan
            pattern: File pattern (glob)
            file_type_code: Optional file type code to filter by schema

        Returns:
            Dictionary mapping filenames to their metadata
    """
    directory = Path(directory)
    results = {}

    for file_path in directory.glob(pattern):
        if file_path.is_file():
            try:
                metadata = introspect_file(file_path, sample_rows=3)
                results[file_path.name] = metadata
            except Exception as e:  # ALLOWED: Dev tool - stores error in result dict, continues with remaining files
                results[file_path.name] = {'error': str(e)}

    return results
