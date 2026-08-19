"""Tests for ACOMS parser helpers."""

from __future__ import annotations

import pytest

from acoharmony._acoms.models import AcomsCategory, infer_file_type_code
from acoharmony._acoms.parser import (
    _parse_size_to_bytes,
    detect_auth_errors,
    extract_file_count,
    extract_filenames,
    parse_datahub_file_types,
    parse_datahub_output,
)


@pytest.mark.unit
def test_parse_acoms_view_output() -> None:
    stdout = """
 acoms-cli - ACO-MS CLI

----------------------------------------------------------------------------

 Found 2 files.

 List of Files

 1 of 2 - P.A2671.ACO.ZCY26.D260211.T1435560.zip (68.8 MB) Last Updated: 2026-02-12T12:34:44.000Z
 2 of 2 - P.A2671.ACO.MBIY26.D260203.T1916300.txt (620.1 KB) Last Updated: 2026-02-09T18:27:46.000Z

----------------------------------------------------------------------------
"""
    result = parse_datahub_output(stdout)

    assert result.total_files == 2
    assert result.errors is None
    assert [file_entry.filename for file_entry in result.files] == [
        "P.A2671.ACO.ZCY26.D260211.T1435560.zip",
        "P.A2671.ACO.MBIY26.D260203.T1916300.txt",
    ]
    assert result.files[0].size_bytes == int(68.8 * 1024**2)
    assert result.files[1].size_bytes == int(620.1 * 1024)
    assert result.files[0].last_updated == "2026-02-12T12:34:44.000Z"


@pytest.mark.unit
def test_parse_acoms_file_type_catalog() -> None:
    stdout = """
 acoms-cli - ACO-MS CLI

List Of Datahub Folders & File Types :

Claim and Claim Line Feed (CCLF) Files
-----Claim and Claim Line Feeds (CCLF) - Weekly, Code 305
-----Claim and Claim Line Feeds (CCLF), Code 113

Reports
-----Historical Benchmark Report, Code 115
-----Other Reports, Code 112
"""
    definitions = parse_datahub_file_types(stdout)

    assert [(item.category, item.name, item.code) for item in definitions] == [
        ("CCLF", "Claim and Claim Line Feeds (CCLF) - Weekly", 305),
        ("CCLF", "Claim and Claim Line Feeds (CCLF)", 113),
        ("Reports", "Historical Benchmark Report", 115),
        ("Reports", "Other Reports", 112),
    ]


@pytest.mark.unit
def test_parse_size_and_extract_helpers() -> None:
    assert _parse_size_to_bytes("1.5 MB") == int(1.5 * 1024**2)
    assert _parse_size_to_bytes("620.1 KB") == int(620.1 * 1024)
    assert _parse_size_to_bytes("bad") is None
    assert extract_file_count(" Found 7 files.") == 7
    assert extract_filenames(" Found 1 files.\n 1 of 1 - file.txt (1 KB)\n") == ["file.txt"]


@pytest.mark.unit
def test_detect_auth_errors() -> None:
    hits = detect_auth_errors("request failed: unauthorized\nall done")
    assert hits == ["request failed: unauthorized"]


@pytest.mark.unit
def test_category_normalization_alias() -> None:
    assert AcomsCategory("CCLF") is AcomsCategory.CCLF


@pytest.mark.unit
def test_infer_file_type_codes_from_known_acoms_names() -> None:
    assert (
        infer_file_type_code(
            "P.A2671.ACO.ZCWY26.S260101.E260217.D260218.T1104240.zip",
            "CCLF",
        )
        == 305
    )
    assert infer_file_type_code("P.A2671.ACO.ZCY26.D260211.T1435560.zip", "CCLF") == 113
    assert infer_file_type_code("P.A2671.ACO.ZCR23.D240227.T1505320.zip", "CCLF") == 113
    assert (
        infer_file_type_code("P.A2671.ACO.MBIY26.D260203.T1916300.txt", "Monthly Exclusion Files")
        == 183
    )
    assert (
        infer_file_type_code("P.A2671.ACO.MBIR22.D230214.T1010580.txt", "Monthly Exclusion Files")
        == 183
    )
    assert (
        infer_file_type_code("P.A2671.BNEX.Y26.D260203.T1910490.xml", "Monthly Exclusion Files")
        == 114
    )
    assert (
        infer_file_type_code("A2671.01.SBMON.D260304.T1517558.zip", "Shadow Bundles Data Files")
        == 244
    )
    assert infer_file_type_code("P.A2671.ACO.BNMRK.D269999.T0000000.zip", "Reports") == 115
    assert infer_file_type_code("P.A2671.ACO.QEXPU.D269999.T0100000.zip", "Reports") == 118
    assert infer_file_type_code("P.A2671.ACO.QQR.D239999.T0100001.zip", "Reports") == 133
    assert infer_file_type_code("P.A2671.ACO.PATB.D249999.T0000000.zip", "Reports") == 134
    assert infer_file_type_code("P.A2671.ACO.STLMT.D249999.T1111111.zip", "Reports") == 120
    assert infer_file_type_code("P.A2671.QMCQM.D259999.T0400000.zip", "Reports") == 306
