# © 2025 HarmonyCares
# All rights reserved.

"""
Custom Excel parser for participant list files using positional column mapping.

Handles both ACO REACH Participant List (51 columns) and HarmonyCares Provider List
(27 columns) by mapping columns by position to schema-defined output names.
"""

from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import parser_method, validate_file_format
from ._date_handler import apply_date_parsing
from ._registry import register_parser


@register_parser("participant_list_excel", metadata={"extensions": [".xlsx"]})
@validate_file_format(param_name="file_path", formats=[".xlsx", ".xls"])
@parser_method(threshold=3.0, validate_path="file_path")
def parse_participant_list_excel(
    file_path: Path, schema: Any, limit: int | None = None, sheet_name: str | None = None
) -> pl.LazyFrame:
    """
    Parse participant list Excel files using positional column mapping.

    Detects format based on column count and maps to schema output names by position.

    Args:
        file_path: Path to Excel file
        schema: TableMetadata with column definitions
        limit: Optional row limit
        sheet_name: Optional sheet name

    Returns:
        LazyFrame with normalized participant list data
    """
    # Read Excel with headers preserved
    df = pl.read_excel(file_path, sheet_id=1, engine="calamine")

    if limit:
        df = df.head(limit)

    # Detect format based on column count
    num_cols = len(df.columns)

    if num_cols == 51:
        # ACO REACH format - map to output names from schema
        df = _normalize_reach_format(df, schema)
    elif num_cols == 27:
        # HarmonyCares format - map and add missing columns
        df = _normalize_harmonycares_format(df, schema, file_path)
    else:
        raise ValueError(
            f"Unknown participant list format in {file_path.name}. "
            f"Expected 51 (REACH) or 27 (HarmonyCares) columns, got {num_cols}"
        )

    # Apply date parsing
    df_lazy = df.lazy()
    df_lazy = apply_date_parsing(df_lazy, schema)

    return df_lazy


def _normalize_reach_format(df: pl.DataFrame, schema: Any) -> pl.DataFrame:
    """
    Normalize ACO REACH format using schema-driven positional mapping.

    REACH format has 51 columns that map 1:1 to the schema definition.
    """
    if not schema or not hasattr(schema, "columns"):
        raise ValueError("Schema is required for REACH format normalization")

    # Extract output names from schema in order
    output_names = [col.get("output_name") for col in schema.columns]

    # Build positional rename map
    rename_map = {}
    for i, old_col in enumerate(df.columns):
        if i < len(output_names):
            rename_map[old_col] = output_names[i]
        else:
            # Extra columns beyond schema - keep original name
            rename_map[old_col] = old_col

    return df.rename(rename_map)


def _normalize_harmonycares_format(df: pl.DataFrame, schema: Any, file_path: Path) -> pl.DataFrame:
    """
    Normalize HarmonyCares format using positional mapping and schema.

    HarmonyCares format has 27 columns that map to a subset of the schema,
    plus additional HarmonyCares-specific columns. Missing REACH columns are
    filled with nulls, and entity information is populated from file data or defaults.
    """
    import re

    if not schema or not hasattr(schema, "columns"):
        raise ValueError("Schema is required for HarmonyCares format normalization")

    # HarmonyCares 27-column format positional mapping
    # Maps to schema positions or HarmonyCares-specific names
    harmonycares_mapping = [
        ("provider_type", 4),           # 0 -> schema position 4
        ("provider_class", 5),          # 1 -> schema position 5
        ("base_provider_tin", 10),      # 2 -> schema position 10
        ("organization_npi", 11),       # 3 -> schema position 11
        ("ccn", 12),                    # 4 -> schema position 12
        ("legacy_ccn", None),           # 5 -> HarmonyCares-specific
        ("individual_npi", 7),          # 6 -> schema position 7
        ("provider_legal_business_name", 6),  # 7 -> schema position 6
        ("individual_last_name", 9),    # 8 -> schema position 9
        ("individual_first_name", 8),   # 9 -> schema position 8
        ("legacy_provider", None),      # 10 -> HarmonyCares-specific
        ("legacy_tin_values", None),    # 11 -> HarmonyCares-specific
        ("address_line_1", None),       # 12 -> HarmonyCares-specific
        ("address_line_2", None),       # 13 -> HarmonyCares-specific
        ("city_name", None),            # 14 -> HarmonyCares-specific
        ("state_cd", None),             # 15 -> HarmonyCares-specific
        ("county", None),               # 16 -> HarmonyCares-specific
        ("zip5_code", None),            # 17 -> HarmonyCares-specific
        ("zip4_code", None),            # 18 -> HarmonyCares-specific
        ("cehrt_attestation", 25),      # 19 -> schema position 25
        ("cehrt_id", 26),               # 20 -> schema position 26
        ("low_volume_exception", 27),   # 21 -> schema position 27
        ("mips_exception", 28),         # 22 -> schema position 28
        ("mips_reweighting_exception", 29),  # 23 -> schema position 29
        ("other", 30),                  # 24 -> schema position 30
        ("email", 50),                  # 25 -> schema position 50
        ("provider_agreement_signature", None),  # 26 -> HarmonyCares-specific
    ]

    # Build rename map from original column names to output names
    rename_map = {}
    for i, (output_name, _schema_pos) in enumerate(harmonycares_mapping):
        if i < len(df.columns):
            rename_map[df.columns[i]] = output_name

    df = df.rename(rename_map)

    # Extract performance year from filename
    # Format: "D0259 Provider List - M-DD-YYYY HH.MM.SS.xlsx"
    performance_year = None
    match = re.search(r"-\s*(\d{1,2})-(\d{1,2})-(\d{4})", file_path.name)
    if match:
        year = match.group(3)
        performance_year = f"PY{year}"

    # Get entity information from file or use defaults
    # ACO D0259 = HarmonyCares ACO LLC, TIN 881823607
    entity_id = "D0259"
    entity_tin = "881823607"
    entity_legal_business_name = "HarmonyCares ACO LLC"

    # Add entity columns
    if "entity_id" not in df.columns:
        df = df.with_columns(pl.lit(entity_id, dtype=pl.Utf8).alias("entity_id"))
    if "entity_tin" not in df.columns:
        df = df.with_columns(pl.lit(entity_tin, dtype=pl.Utf8).alias("entity_tin"))
    if "entity_legal_business_name" not in df.columns:
        df = df.with_columns(pl.lit(entity_legal_business_name, dtype=pl.Utf8).alias("entity_legal_business_name"))
    if "performance_year" not in df.columns and performance_year:
        df = df.with_columns(pl.lit(performance_year, dtype=pl.Utf8).alias("performance_year"))

    # Get all schema output names to identify missing columns
    schema_output_names = [col.get("output_name") for col in schema.columns]
    current_cols = set(df.columns)

    # Add missing schema columns with nulls
    for output_name in schema_output_names:
        if output_name not in current_cols:
            df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(output_name))

    return df
