"""Tests for SVA normalization transform."""

from __future__ import annotations

from datetime import date
from unittest.mock import MagicMock

import polars as pl

from acoharmony._catalog import Catalog
from acoharmony._transforms._sva import apply_transform
from acoharmony.parsers import parse_file


def test_sva_parser_reads_sva_data_sheet_and_transform_canonicalizes(tmp_path):
    from openpyxl import Workbook

    path = tmp_path / "D0259SVA20260513.xlsx"
    workbook = Workbook()
    instructions = workbook.active
    instructions.title = "INSTRUCTIONS"
    instructions.append(["Instructions", "Do not import this sheet"])
    instructions.append(["ACO ID", "Not an MBI"])

    data = workbook.create_sheet("SVA_DATA")
    data.append(
        [
            "ACO ID",
            "Beneficiary's MBI",
            "Beneficiary's First Name",
            "Beneficiary's Last Name",
            "Beneficiary's Street Address",
            "City",
            "State",
            "Zip",
            "Provider Name/Primary place the Beneficiary receives care (as it appears on the signed SVA letter)",
            "Name of Individual  Participant Provider associated w/ attestation",
            "iNPI for Individual  Participant Provider  column J?",
            "TIN for Individual Participant Provider  column J?",
            "Signature Date on SVA letter",
            "Response Code (CMS to fill out)",
        ]
    )
    data.append(
        [
            "D0259",
            "1EG4TE5MK73",
            "Jane",
            "Doe",
            "123 Main",
            "Detroit",
            "MI",
            "48201",
            "HarmonyCares",
            "Dr Example",
            "1234567890",
            "123456789",
            "04/03/2026",
            "00",
        ]
    )
    workbook.save(path)

    schema = Catalog().get_table_metadata("sva")
    parsed = parse_file(path, schema, add_tracking=True, schema_name="sva")
    result = apply_transform(parsed, {}, None, MagicMock()).collect()

    assert result.height == 1
    assert result["bene_mbi"][0] == "1EG4TE5MK73"
    assert result["sva_signature_date"][0] == date(2026, 4, 3)
    assert result["source_filename"][0] == path.name
    assert "beneficiary_s_mbi" not in result.columns


def test_sva_transform_filters_instruction_rows_and_coalesces_canonical_columns():
    df = pl.DataFrame(
        {
            "aco_id": ["Instructions", "D0259"],
            "beneficiary_s_mbi": ["not an mbi", " 1eg4te5mk73 "],
            "beneficiary_s_first_name": ["ignore", "Jane"],
            "signature_date_on_sva_letter": ["not a date", "2026-02-11"],
            "response_code_cms_to_fill_out": [None, "a0"],
        }
    ).lazy()

    result = apply_transform(df, {}, None, MagicMock()).collect()

    assert result.height == 1
    assert result["bene_mbi"][0] == "1EG4TE5MK73"
    assert result["sva_response_code"][0] == "A0"
