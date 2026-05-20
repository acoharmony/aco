"""Test TPARC file reading with semicolon delimiter."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path

import polars as pl
import pytest

from acoharmony._catalog import Catalog


@pytest.mark.unit
def test_tparc_schema_exists():
    """Test that TPARC schema is loaded correctly."""
    catalog = Catalog()
    schema = catalog.get_schema("tparc")

    assert schema is not None, "TPARC schema should exist"
    assert schema.name == "tparc"
    assert "Claims Reduction" in schema.description
    assert schema.file_format["type"] == "tparc"
    assert schema.file_format["has_header"] is False
    assert len(schema.columns) >= 20  # Named fields in the schema


@pytest.mark.unit
def test_tparc_file_reading(tmp_path):
    """Test reading synthetic TPARC data via the tparc parser."""
    from acoharmony._parsers._tparc import parse_tparc

    catalog = Catalog()
    schema = catalog.get_schema("tparc")

    # Build synthetic TPARC lines using ";" delimiter (matches schema file_format).
    # CLMH has 9 named columns in the schema; CLML has more.
    clmh_fields = [
        "CLMH", "1", "PAT001", "PLAN01", "CLM001", "MEM001",
        "", "20250101", "20250131",
    ]
    clml_fields = [
        "CLML", "2", "", "1234567890", "123456789",
        "J0123", "", "", "", "20250101", "20250131",
        "5", "100.00", "80.00", "60.00", "10.00", "5.00",
        "", "", "2.50", "", "", "1.25",
        "0450", "99213", "25", "", "", "", "",
        "PCTL001", "11", "1", "MA01", "CO", "",
    ]

    tparc_file = tmp_path / "P.D1234.TPARC.RP.D250101.T0001.txt"
    tparc_file.write_text(
        ";".join(clmh_fields) + "\n" + ";".join(clml_fields) + "\n"
    )

    lf = parse_tparc(tparc_file, schema)
    df = lf.collect()

    # Should have rows from both record types
    assert df.shape[0] == 2, f"Should parse both CLMH and CLML rows, got {df.shape[0]}"
    assert "record_type" in df.columns
    record_types = df["record_type"].unique().to_list()
    assert "CLMH" in record_types
    assert "CLML" in record_types


@pytest.mark.unit
def test_tparc_payment_reduction_columns():
    """Test that payment reduction columns are properly defined."""
    catalog = Catalog()
    schema = catalog.get_schema("tparc")

    # Check for key payment reduction columns
    column_names = [col["name"] for col in schema.columns]

    # Verify key payment-related columns exist
    expected_columns = [
        "sequestration_amt",
        "pcc_reduction_amt",
    ]

    for col in expected_columns:
        assert col in column_names, f"Payment column {col} should be defined"


@pytest.mark.unit
def test_tparc_date_columns():
    """Test that date columns are defined in schema."""
    catalog = Catalog()
    schema = catalog.get_schema("tparc")

    column_names = [col["name"] for col in schema.columns]
    assert "from_date" in column_names
    assert "thru_date" in column_names
