"""Tests for ACOMS metadata registered from _tables."""

from __future__ import annotations

import pytest

from acoharmony._acoms.inventory import (
    _load_schema_acoms_patterns,
    _match_schema_acoms_file_type_code,
)


@pytest.mark.unit
def test_tables_register_acoms_blocks() -> None:
    from acoharmony import _tables as _  # noqa: F401
    from acoharmony._registry import SchemaRegistry, get_full_table_config

    cclf = SchemaRegistry.get_acoms_config("cclf1")
    assert cclf["category"] == "CCLF"
    assert cclf["fileTypeCode"] == 113
    assert cclf["fileTypeCodes"] == [113, 305]

    full = get_full_table_config("cclf1")
    assert full["acoms"]["category"] == "CCLF"

    assert SchemaRegistry.get_acoms_config("bnex")["fileTypeCode"] == 114
    assert SchemaRegistry.get_acoms_config("mbi_crosswalk")["fileTypeCode"] == 183
    assert SchemaRegistry.get_acoms_config("shadow_bundle_reach")["fileTypeCode"] == 244
    assert SchemaRegistry.get_acoms_config("quarterly_quality_report")["fileTypeCode"] == 133
    assert SchemaRegistry.get_acoms_config("participant_list")["fileTypeCode"] == 134
    assert (
        SchemaRegistry.get_acoms_config("quarterly_beneficiary_level_quality_report")[
            "fileTypeCode"
        ]
        == 306
    )

    alr = SchemaRegistry.get_acoms_config("alr")
    assert alr["fileTypeCode"] == 116
    assert alr["fileTypeCodes"] == [116, 129, 131]


@pytest.mark.unit
def test_acoms_schema_patterns_match_live_filename_shapes() -> None:
    patterns = _load_schema_acoms_patterns()

    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.ZCY26.D260211.T1435560.zip",
            "CCLF",
            patterns,
        )
        == 113
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.ZCR23.D240227.T1505320.zip",
            "CCLF",
            patterns,
        )
        == 113
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.ZCWY26.S260101.E260217.D260218.T1104240.zip",
            "CCLF",
            patterns,
        )
        == 305
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.BNEX.Y26.D260203.T1910490.xml",
            "Monthly Exclusion Files",
            patterns,
        )
        == 114
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.MBIY26.D260203.T1916300.txt",
            "Monthly Exclusion Files",
            patterns,
        )
        == 183
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.MBIR22.D230214.T1010580.txt",
            "Monthly Exclusion Files",
            patterns,
        )
        == 183
    )
    assert (
        _match_schema_acoms_file_type_code(
            "A2671.Q4.SBQR.D260305.T1212121.xlsx",
            "Shadow Bundles Data Files",
            patterns,
        )
        == 244
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.BNMRK.D269999.T0000000.zip",
            "Reports",
            patterns,
        )
        == 115
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.QQR.D239999.T0100001.zip",
            "Reports",
            patterns,
        )
        == 133
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.PATB.D249999.T0000000.xlsx",
            "Reports",
            patterns,
        )
        == 134
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.QMCQM.D259999.T0400000.zip",
            "Reports",
            patterns,
        )
        == 306
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.AALR.Y2022.D229999.T0000000.zip",
            "Reports",
            patterns,
        )
        == 129
    )
    assert (
        _match_schema_acoms_file_type_code(
            "P.A2671.ACO.QALR.2024Q1.D249999.T0100000.zip",
            "Reports",
            patterns,
        )
        == 131
    )
