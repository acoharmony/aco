"""
Unit tests for BNEX (Beneficiary Data Sharing Opt-Out) file processing.

Tests XML parsing, schema validation, and data transformations for BNEX files.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import tempfile
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import polars as pl
import pytest

from acoharmony._parsers._xml import parse_xml
from acoharmony._catalog import Catalog

if TYPE_CHECKING:
    pass


class TestBnexXmlParser:
    """Tests for BNEX XML parsing."""

    @pytest.mark.unit
    def test_parse_bnex_xml_basic(self) -> None:
        """Parse basic BNEX XML structure."""

        xml_data = """<?xml version="1.0" encoding="UTF-8"?>
<PFDCACOBeneData>
<Beneficiarys>
<Beneficiary><MBI>5XU6K54UD46</MBI><HICN></HICN><FirstName>PATRICIA</FirstName><MiddleName>J</MiddleName><LastName>YOUNK</LastName><DOB>19270412</DOB><Gender>F</Gender><BeneExcReasons><BeneExcReason>PC</BeneExcReason></BeneExcReasons></Beneficiary>
<Beneficiary><MBI>3Y36NU6PP43</MBI><HICN></HICN><FirstName>WANDA</FirstName><MiddleName>L</MiddleName><LastName>DEWITT</LastName><DOB>19470330</DOB><Gender>F</Gender><BeneExcReasons><BeneExcReason>PC</BeneExcReason></BeneExcReasons></Beneficiary>
</Beneficiarys>
</PFDCACOBeneData>"""

        # Write to temp file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(xml_data)
            temp_path = Path(f.name)

        try:
            schema = {"file_format": {"row_tag": "Beneficiary"}}
            df = parse_xml(temp_path, schema).collect()

            assert len(df) == 2
            assert "MBI" in df.columns
            assert "FirstName" in df.columns
            assert "LastName" in df.columns
            assert "DOB" in df.columns
            assert "Gender" in df.columns
        finally:
            temp_path.unlink()

    @pytest.mark.unit
    def test_parse_bnex_with_empty_fields(self) -> None:
        """Parse BNEX XML with empty HICN and MiddleName fields."""

        xml_data = """<?xml version="1.0" encoding="UTF-8"?>
<PFDCACOBeneData>
<Beneficiarys>
<Beneficiary><MBI>5EU7C43HY53</MBI><HICN></HICN><FirstName>DWIGHT</FirstName><MiddleName></MiddleName><LastName>BENNINGTON</LastName><DOB>19410621</DOB><Gender>M</Gender><BeneExcReasons><BeneExcReason>BD</BeneExcReason></BeneExcReasons></Beneficiary>
</Beneficiarys>
</PFDCACOBeneData>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(xml_data)
            temp_path = Path(f.name)

        try:
            schema = {"file_format": {"row_tag": "Beneficiary"}}
            df = parse_xml(temp_path, schema).collect()

            assert len(df) == 1
            # Empty fields should be handled (null or empty string)
            assert df["MBI"][0] == "5EU7C43HY53"
            assert df["FirstName"][0] == "DWIGHT"
        finally:
            temp_path.unlink()

    @pytest.mark.unit
    def test_parse_bnex_multiple_exclusion_reasons(self) -> None:
        """Parse BNEX with multiple BeneExcReason values."""

        xml_data = """<?xml version="1.0" encoding="UTF-8"?>
<PFDCACOBeneData>
<Beneficiarys>
<Beneficiary><MBI>9JU0QK9NY33</MBI><HICN></HICN><FirstName>CATHERINE</FirstName><MiddleName>E</MiddleName><LastName>VERPLANCKEN</LastName><DOB>19171114</DOB><Gender>F</Gender><BeneExcReasons><BeneExcReason>BD</BeneExcReason><BeneExcReason>PC</BeneExcReason></BeneExcReasons></Beneficiary>
</Beneficiarys>
</PFDCACOBeneData>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(xml_data)
            temp_path = Path(f.name)

        try:
            schema = {"file_format": {"row_tag": "Beneficiary"}}
            df = parse_xml(temp_path, schema).collect()

            assert len(df) == 1
            # Should handle multiple reasons (pipe-separated)
            assert df["MBI"][0] == "9JU0QK9NY33"
            assert "BeneExcReasons" in df.columns
            # Multiple reasons should be joined with |
            assert "BD" in df["BeneExcReasons"][0]
            assert "PC" in df["BeneExcReasons"][0]
        finally:
            temp_path.unlink()

    @pytest.mark.unit
    def test_parse_bnex_date_format(self) -> None:
        """Test DOB is in YYYYMMDD format."""

        xml_data = """<?xml version="1.0" encoding="UTF-8"?>
<PFDCACOBeneData>
<Beneficiarys>
<Beneficiary><MBI>5XU6K54UD46</MBI><HICN></HICN><FirstName>PATRICIA</FirstName><MiddleName>J</MiddleName><LastName>YOUNK</LastName><DOB>19270412</DOB><Gender>F</Gender><BeneExcReasons><BeneExcReason>PC</BeneExcReason></BeneExcReasons></Beneficiary>
</Beneficiarys>
</PFDCACOBeneData>"""

        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write(xml_data)
            temp_path = Path(f.name)

        try:
            schema = {"file_format": {"row_tag": "Beneficiary"}}
            df = parse_xml(temp_path, schema).collect()

            assert len(df) == 1
            dob = df["DOB"][0]
            # DOB should be string in YYYYMMDD format
            assert len(dob) == 8
            assert dob == "19270412"
        finally:
            temp_path.unlink()


class TestBnexDataTransformations:
    """Tests for BNEX data transformations."""

    @pytest.mark.unit
    def test_parse_dob_to_date(self) -> None:
        """Test parsing DOB from YYYYMMDD string to date."""
        df = pl.DataFrame({
            "DOB": ["19270412", "19470330", "19410621"]
        })

        # Parse DOB to date
        df = df.with_columns([
            pl.concat_str([
                pl.col("DOB").str.slice(0, 4),
                pl.lit("-"),
                pl.col("DOB").str.slice(4, 2),
                pl.lit("-"),
                pl.col("DOB").str.slice(6, 2),
            ]).str.to_date("%Y-%m-%d").alias("bene_date_of_birth")
        ])

        assert "bene_date_of_birth" in df.columns
        assert df["bene_date_of_birth"][0].year == 1927
        assert df["bene_date_of_birth"][0].month == 4
        assert df["bene_date_of_birth"][0].day == 12

    @pytest.mark.unit
    def test_calculate_age_from_dob(self) -> None:
        """Test calculating beneficiary age from DOB."""

        df = pl.DataFrame({
            "DOB": ["19270412", "19470330", "20000101"]
        })

        # Parse DOB and calculate age
        today = date.today()
        df = df.with_columns([
            pl.concat_str([
                pl.col("DOB").str.slice(0, 4),
                pl.lit("-"),
                pl.col("DOB").str.slice(4, 2),
                pl.lit("-"),
                pl.col("DOB").str.slice(6, 2),
            ]).str.to_date("%Y-%m-%d").alias("bene_date_of_birth")
        ]).with_columns([
            ((pl.lit(today) - pl.col("bene_date_of_birth")).dt.total_days() / 365.25).cast(pl.Int32).alias("age")
        ])

        assert "age" in df.columns
        # Age should be reasonable for beneficiaries
        assert df["age"][0] > 90  # Born in 1927
        assert df["age"][1] > 70  # Born in 1947
        assert df["age"][2] < 30  # Born in 2000

    @pytest.mark.unit
    def test_exclusion_reason_categorization(self) -> None:
        """Test categorizing exclusion reasons."""
        df = pl.DataFrame({
            "BeneExcReason": ["PC", "BD", "PC", "OT"]
        })

        # Categorize reasons
        df = df.with_columns([
            pl.when(pl.col("BeneExcReason") == "PC")
            .then(pl.lit("Patient Choice"))
            .when(pl.col("BeneExcReason") == "BD")
            .then(pl.lit("Beneficiary Death"))
            .otherwise(pl.lit("Other"))
            .alias("exclusion_category")
        ])

        assert df["exclusion_category"][0] == "Patient Choice"
        assert df["exclusion_category"][1] == "Beneficiary Death"
        assert df["exclusion_category"][2] == "Patient Choice"
        assert df["exclusion_category"][3] == "Other"

    @pytest.mark.unit
    def test_gender_normalization(self) -> None:
        """Test gender field normalization."""
        df = pl.DataFrame({
            "Gender": ["F", "M", "F", "M"]
        })

        # Expand gender codes
        df = df.with_columns([
            pl.when(pl.col("Gender") == "M")
            .then(pl.lit("Male"))
            .when(pl.col("Gender") == "F")
            .then(pl.lit("Female"))
            .otherwise(pl.lit("Unknown"))
            .alias("gender_full")
        ])

        assert df["gender_full"][0] == "Female"
        assert df["gender_full"][1] == "Male"


@pytest.mark.integration
class TestBnexFileProcessing:
    """Integration tests for processing actual BNEX files."""

    @pytest.mark.unit
    def test_bnex_file_exists(self) -> None:
        """Test that BNEX files exist in bronze tier."""

        bronze_path = Path("/opt/s3/data/workspace/bronze")
        bnex_files = list(bronze_path.glob("P.A*.BNEX.Y*.D*.T*.xml"))

        # Should have at least one BNEX file
        assert len(bnex_files) > 0

    @pytest.mark.slow
    def test_parse_actual_bnex_file(self) -> None:
        """Test parsing an actual BNEX XML file."""


        bronze_path = Path("/opt/s3/data/workspace/bronze")
        bnex_files = sorted(bronze_path.glob("P.A*.BNEX.Y*.D*.T*.xml"))

        if len(bnex_files) == 0:
            pytest.skip("No BNEX files available for testing")

        # Use most recent file
        bnex_file = bnex_files[-1]

        # Read XML file
        schema = {"file_format": {"row_tag": "Beneficiary"}}
        df = parse_xml(bnex_file, schema).collect()

        # Basic validations
        assert len(df) > 0
        assert "MBI" in df.columns
        assert "FirstName" in df.columns
        assert "LastName" in df.columns
        assert "DOB" in df.columns
        assert "Gender" in df.columns

        # All MBIs should be 11 characters
        mbi_lengths = df.select(pl.col("MBI").str.len_chars()).to_series()
        assert mbi_lengths.min() == 11
        assert mbi_lengths.max() == 11

        # All DOBs should be 8 characters (YYYYMMDD)
        dob_lengths = df.select(pl.col("DOB").str.len_chars()).to_series()
        assert dob_lengths.min() == 8
        assert dob_lengths.max() == 8

        # Gender should be M or F
        genders = df["Gender"].unique().to_list()
        assert all(g in ["M", "F"] for g in genders)

    @pytest.mark.slow
    def test_bnex_schema_compliance(self) -> None:
        """Test that BNEX files comply with schema definition."""


        # Load schema
        catalog = Catalog()
        schema = catalog.get_table_metadata("bnex")

        assert schema is not None
        assert schema.name == "bnex"
        assert schema.file_format["type"] == "xml"

        # Check columns defined (column names are now lowercase)
        column_names = [col["name"] for col in schema.columns]
        assert "mbi" in column_names
        assert "firstname" in column_names
        assert "lastname" in column_names
        assert "dob" in column_names
        assert "gender" in column_names
        assert "beneexcreason" in column_names


@pytest.mark.unit
class TestBnexSchemaValidation:
    """Tests for BNEX schema validation."""

    @pytest.mark.unit
    def test_bnex_schema_exists(self) -> None:
        """Test that BNEX schema can be loaded."""

        catalog = Catalog()
        schema = catalog.get_table_metadata("bnex")

        assert schema is not None
        assert schema.name == "bnex"

    @pytest.mark.unit
    def test_bnex_file_patterns(self) -> None:
        """Test BNEX file patterns are correct."""

        catalog = Catalog()
        schema = catalog.get_table_metadata("bnex")

        assert schema is not None
        assert "mssp" in schema.storage["file_patterns"]
        patterns = schema.storage["file_patterns"]["mssp"]
        assert any("BNEX" in p for p in patterns)
        assert any(".xml" in p for p in patterns)

    @pytest.mark.unit
    def test_bnex_required_columns(self) -> None:
        """Test that expected BNEX columns are present."""

        catalog = Catalog()
        schema = catalog.get_table_metadata("bnex")

        assert schema is not None

        # Check expected columns exist (names are now lowercase)
        col_names = [col["name"] for col in schema.columns]
        assert "mbi" in col_names
        assert "firstname" in col_names
        assert "lastname" in col_names
        assert "dob" in col_names
        assert "gender" in col_names
        assert "beneexcreason" in col_names

    @pytest.mark.unit
    def test_bnex_output_names(self) -> None:
        """Test that column names are properly defined."""

        catalog = Catalog()
        schema = catalog.get_table_metadata("bnex")

        assert schema is not None

        # Find specific columns by lowercase name
        mbi_col = next((col for col in schema.columns if col["name"] == "mbi"), None)
        assert mbi_col is not None

        first_name_col = next((col for col in schema.columns if col["name"] == "firstname"), None)
        assert first_name_col is not None

        dob_col = next((col for col in schema.columns if col["name"] == "dob"), None)
        assert dob_col is not None
