# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for monthly_expenditure_report parser."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from acoharmony._parsers._excel_multi_sheet import parse_excel_multi_sheet
from acoharmony.tables import TableManager

if TYPE_CHECKING:
    pass


# Integration tests using real files


@pytest.mark.integration
def test_parse_monthly_expenditure_report_real_file() -> None:
    """Test monthly expenditure report parser with real file.

    Verifies that the parser correctly handles:
    - Multiple sheets with different data types
    - Negative numbers in numeric columns (which caused "can't convert negative int to unsigned")
    - Openpyxl fallback mechanism for problematic Excel files
    """
    file_path = Path(
        "/opt/s3/data/workspace/bronze/REACH.D0259.MEXPR.01.PY2023.D230208.T3894790.xlsx"
    )

    # Skip if file doesn't exist
    if not file_path.exists():
        pytest.skip(f"Test file not found: {file_path}")

    # Load schema
    table_mgr = TableManager()
    schema = table_mgr.get_table_metadata("mexpr")

    # Parse file
    result = parse_excel_multi_sheet(file_path, schema)
    df = result.collect()

    # Verify results
    assert len(df) > 0, "Should parse at least some rows"

    # Verify sheet types are present
    sheet_types = df["sheet_type"].unique().to_list()
    # At minimum, data sheets should be present
    assert "data_claims" in sheet_types, "Missing expected sheet: data_claims"
    assert "data_enroll" in sheet_types, "Missing expected sheet: data_enroll"

    # Verify sheet_type column exists
    assert "sheet_type" in df.columns, "Missing sheet_type column"

    # Verify _output_table column exists
    assert "_output_table" in df.columns, "Missing _output_table column"

    # Verify data_claims sheet has data
    data_claims_df = df.filter(pl.col("sheet_type") == "data_claims")
    assert len(data_claims_df) > 0, "data_claims sheet should have data"

    # Verify data_enroll sheet has data
    data_enroll_df = df.filter(pl.col("sheet_type") == "data_enroll")
    assert len(data_enroll_df) > 0, "data_enroll sheet should have data"

    print(f"\n[OK] Successfully parsed monthly expenditure report: {file_path.name}")
    print(f"  Total rows: {len(df)}")
    print(f"  Sheet types: {sheet_types}")
    print(f"  Columns: {df.columns[:10]}")


# Function tests


@pytest.mark.unit
def test_monthly_expenditure_report_schema() -> None:
    """Verify monthly expenditure report schema is correctly configured."""
    table_mgr = TableManager()
    schema = table_mgr.get_table_metadata("mexpr")

    # Verify schema basics
    assert schema["name"] == "mexpr"
    assert schema["file_format"]["type"] == "excel_multi_sheet"
    assert schema["file_format"]["parser"] == "excel_multi_sheet"

    # Verify sheets configuration
    assert "sheets" in schema
    sheets = schema["sheets"]
    assert len(sheets) >= 2, "Should have at least 2 sheets defined"

    # Verify data sheets are present
    sheet_types = [s["sheet_type"] for s in sheets]
    assert "data_claims" in sheet_types, "Missing data_claims sheet"
    assert "data_enroll" in sheet_types, "Missing data_enroll sheet"
