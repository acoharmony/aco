# © 2025 HarmonyCares
# All rights reserved.

"""
Real schema data for testing.

IMPORTANT: When you add a new schema with fourIcli.fileTypeCode to src/acoharmony/_schemas/,
you MUST add a corresponding entry here to ensure comprehensive test coverage.

This file contains REAL data from actual schemas, not fake test data.
"""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock
import importlib

import pytest
import openpyxl

from acoharmony._4icli.inventory import FileInventoryEntry

# REAL SCHEMA FILE TYPES
# ⚠  ADD ONE FOR EVERY NEW SCHEMA WITH fourIcli.fileTypeCode ⚠
#
# To regenerate this list, run:
#   uv run python3 scripts/generate_schema_test_data.py
#
REAL_SCHEMA_FILE_TYPES = [
    # aco_financial_guarantee_amount - Reports
    {
        "code": 267,
        "schema": "aco_financial_guarantee_amount",
        "category": "Reports",
        "pattern": "D????.FGL.PY????.D??????.T*.pdf",
    },
    # alternative_payment_arrangement_report - Reports
    {
        "code": 216,
        "schema": "alternative_payment_arrangement_report",
        "category": "Reports",
        "pattern": "REACH.D????.ALTPR.PY????.D??????.T*.xlsx",
    },
    # annual_beneficiary_level_quality_report - Reports
    {
        "code": 269,
        "schema": "annual_beneficiary_level_quality_report",
        "category": "Reports",
        "pattern": "D????.BLAQR.PY????.D??????.T*.zip",
    },
    # annual_quality_report - Reports
    {
        "code": 217,
        "schema": "annual_quality_report",
        "category": "Reports",
        "pattern": "P.D????.ANLQR.D??????.T*.xlsx",
    },
    # bar - Beneficiary List
    {
        "code": 159,
        "schema": "bar",
        "category": "Beneficiary List",
        "pattern": "P.D????.ALG???.RP.D??????.T*.xlsx",
    },
    # beneficiary_data_sharing_exclusion_file - Reports
    {
        "code": 114,
        "schema": "beneficiary_data_sharing_exclusion_file",
        "category": "Reports",
        "pattern": "",
    },
    # beneficiary_hedr_transparency_files - Reports
    {
        "code": 272,
        "schema": "beneficiary_hedr_transparency_files",
        "category": "Reports",
        "pattern": "P.D????.BDTF.A?.D??????.T*.zip",
    },
    # cclf0 - Claim and Claim Line Feed (CCLF) Files (All CCLF files share code 113)
    {
        "code": 113,
        "schema": "cclf8",
        "category": "Claim and Claim Line Feed (CCLF) Files",
        "pattern": "P.D????.ACO.ZCY??.D??????.T*.zip",
    },
    # cclf_management_report - Reports
    {
        "code": 198,
        "schema": "cclf_management_report",
        "category": "Reports",
        "pattern": "P.D????.MCMXP.RP.D??????.T*.xlsx",
    },
    # estimated_cisep_change_threshold_report - Reports
    {
        "code": 265,
        "schema": "estimated_cisep_change_threshold_report",
        "category": "Reports",
        "pattern": "D????.ECCTR.D??????.T*.xlsx",
    },
    # mexpr - Reports
    {
        "code": 214,
        "schema": "mexpr",
        "category": "Reports",
        "pattern": "REACH.D????.MEXPR.??.PY????.D??????.T*.xlsx",
    },
    # palmr - Beneficiary List
    {
        "code": 165,
        "schema": "palmr",
        "category": "Beneficiary List",
        "pattern": "P.D????.PALMR.D??????.T*.csv",
    },
    # pbvar - Beneficiary List
    {
        "code": 175,
        "schema": "pbvar",
        "category": "Beneficiary List",
        "pattern": "P.D????.PBVAR.D??????.T0112000.xlsx",
    },
    # plaru - Reports
    {
        "code": 220,
        "schema": "plaru",
        "category": "Reports",
        "pattern": "REACH.D????.PLARU.PY????.D??????.T*.xlsx",
    },
    # pecos_terminations_monthly_report - Reports
    {
        "code": 298,
        "schema": "pecos_terminations_monthly_report",
        "category": "Reports",
        "pattern": "P.D????.PECOSTRMN.RP.D??????.T*.xlsx",
    },
    # preliminary_alignment_estimate - Reports
    {
        "code": 221,
        "schema": "preliminary_alignment_estimate",
        "category": "Reports",
        "pattern": "REACH.D????.PAER.PY????.D??????.T*.xlsx",
    },
    # preliminary_alternative_payment_arrangement_report_156 - Reports
    {
        "code": 156,
        "schema": "preliminary_alternative_payment_arrangement_report_156",
        "category": "Reports",
        "pattern": "P.D????.ALPAR.D??????.T*.xlsx",
    },
    # preliminary_benchmark_report_for_dc - Reports
    {
        "code": 212,
        "schema": "preliminary_benchmark_report_for_dc",
        "category": "Reports",
        "pattern": "REACH.D????.PRLBR.PY????.D??????.T*.xlsx",
    },
    # preliminary_benchmark_report_unredacted - Reports
    {
        "code": 219,
        "schema": "preliminary_benchmark_report_unredacted",
        "category": "Reports",
        "pattern": "REACH.D????.PRBRU.PY????.D??????.T*.xlsx",
    },
    # prospective_plus_opportunity_report - Beneficiary List
    {
        "code": 170,
        "schema": "prospective_plus_opportunity_report",
        "category": "Beneficiary List",
        "pattern": "P.D????.PPOPR.Q?.D??????.T*.xlsx",
    },
    # quarterly_beneficiary_level_quality_report - Reports
    {
        "code": 268,
        "schema": "quarterly_beneficiary_level_quality_report",
        "category": "Reports",
        "pattern": "D????.BLQQR.Q?.PY????.D??????.T*.zip",
    },
    # quarterly_quality_report - Reports
    {
        "code": 176,
        "schema": "quarterly_quality_report",
        "category": "Reports",
        "pattern": "P.D????.QTLQR.Q?.D??????.T*.xlsx",
    },
    # reach_bnmr - Reports
    {"code": 215, "schema": "reach_bnmr", "category": "Reports", "pattern": "REACH.D*.BNMR.*.xlsx"},
    # risk_adjustment_data - Reports
    {
        "code": 140,
        "schema": "risk_adjustment_data",
        "category": "Reports",
        "pattern": "P.D????.RAP?V?.D??????.T*.csv",
    },
    # sbnabp - Reports
    {
        "code": 243,
        "schema": "sbnabp",
        "category": "Reports",
        "pattern": "D????.PY????.SBNABP.D??????.T*.xlsx",
    },
    # sbqr - Reports
    {
        "code": 243,
        "schema": "sbqr",
        "category": "Reports",
        "pattern": "D????.PY????.Q?.SBQR.D??????.T*.xlsx",
    },
    # shadow_bundle_reach - Reports
    {
        "code": 243,
        "schema": "shadow_bundle_reach",
        "category": "Reports",
        "pattern": "D????.PY????.??.SBM*.D??????.T*.*",
    },
    # tparc - Reports
    {
        "code": 157,
        "schema": "tparc",
        "category": "Reports",
        "pattern": "P.D????.TPARC.RP.D??????.T*.txt",
    },
]

# Total unique file type codes
UNIQUE_FILE_TYPE_CODES = sorted({ft["code"] for ft in REAL_SCHEMA_FILE_TYPES})


def create_sample_inventory_entries():
    """
    Create sample FileInventoryEntry objects for ALL schema file types.

    ⚠  WHEN YOU ADD A NEW SCHEMA: Add a corresponding FileInventoryEntry here! ⚠

    Returns:
        List of FileInventoryEntry objects covering all file types
    """
    # Use APM ID D0259 (real HarmonyCares ID) and recent years
    entries = [
        # CCLF8 (code 113) - Claim and Claim Line Feed
        FileInventoryEntry(
            filename="P.D0259.ACO.ZCY25.D250115.T1550060.zip",
            category="Claim and Claim Line Feed (CCLF) Files",
            file_type_code=113,
            year=2025,
            size_bytes=64660000,  # ~64 MB
            last_updated="2025-01-15T21:47:21.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # PALMR (code 165) - Provider Alignment Master Record
        FileInventoryEntry(
            filename="P.D0259.PALMR.D250110.T1022070.csv",
            category="Beneficiary List",
            file_type_code=165,
            year=2025,
            size_bytes=243700,  # ~237 KB
            last_updated="2025-01-10T15:22:07.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # TPARC (code 157) - Tie Plus Alignment Report Certification
        FileInventoryEntry(
            filename="P.D0259.TPARC.RP.D250125.T2141050.txt",
            category="Reports",
            file_type_code=157,
            year=2025,
            size_bytes=14450,  # ~14 KB
            last_updated="2025-01-26T02:02:38.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # REACH Preliminary Alignment Estimate Report (code 221)
        FileInventoryEntry(
            filename="REACH.D0259.PAER.PY2025.D241209.T1345390.xlsx",
            category="Reports",
            file_type_code=221,
            year=2025,
            size_bytes=6770,  # ~6.6 KB
            last_updated="2024-12-12T18:59:03.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # REACH Preliminary Benchmark Report (code 212)
        FileInventoryEntry(
            filename="REACH.D0259.PRLBR.PY2025.D241211.T1022070.xlsx",
            category="Reports",
            file_type_code=212,
            year=2025,
            size_bytes=243700,  # ~237 KB
            last_updated="2024-12-17T02:34:53.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # Financial Guarantee Letter (code 267)
        FileInventoryEntry(
            filename="D0259.FGL.PY2025.D250106.T1200000.pdf",
            category="Reports",
            file_type_code=267,
            year=2025,
            size_bytes=181600,  # ~177 KB
            last_updated="2025-01-06T19:29:11.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # BAR - Beneficiary Alignment Report (code 159)
        FileInventoryEntry(
            filename="P.D0259.ALG001.RP.D240115.T0950120.xlsx",
            category="Beneficiary List",
            file_type_code=159,
            year=2024,
            size_bytes=512000,  # ~500 KB
            last_updated="2024-01-15T09:50:12.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # Risk Adjustment Data (code 140)
        FileInventoryEntry(
            filename="P.D0259.RAP4V1.D250201.T0800000.csv",
            category="Reports",
            file_type_code=140,
            year=2025,
            size_bytes=8500000,  # ~8.5 MB
            last_updated="2025-02-01T08:00:00.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # Quarterly Quality Report (code 176)
        FileInventoryEntry(
            filename="P.D0259.QTLQR.Q1.D250401.T1500000.xlsx",
            category="Reports",
            file_type_code=176,
            year=2025,
            size_bytes=128000,  # ~125 KB
            last_updated="2025-04-01T15:00:00.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # PBVAR - Provider Beneficiary Visit Alignment Report (code 175)
        FileInventoryEntry(
            filename="P.D0259.PBVAR.D250115.T0112000.xlsx",
            category="Beneficiary List",
            file_type_code=175,
            year=2025,
            size_bytes=750000,  # ~732 KB
            last_updated="2025-01-15T01:12:00.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # CCLF Management Report (code 198)
        FileInventoryEntry(
            filename="P.D0259.MCMXP.RP.D250120.T1000000.xlsx",
            category="Reports",
            file_type_code=198,
            year=2025,
            size_bytes=256000,  # ~250 KB
            last_updated="2025-01-20T10:00:00.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # PLARU - Preliminary Alternative Payment Arrangement Report Unredacted (code 220)
        FileInventoryEntry(
            filename="REACH.D0259.PLARU.PY2025.D241209.T1200000.xlsx",
            category="Reports",
            file_type_code=220,
            year=2025,
            size_bytes=102400,  # ~100 KB
            last_updated="2024-12-09T12:00:00.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # SBQR - Shadow Bundles Quarterly Report (code 243)
        FileInventoryEntry(
            filename="D0259.PY2025.Q2.SBQR.D250929.T1212121.xlsx",
            category="Reports",
            file_type_code=243,
            year=2025,
            size_bytes=1500000,  # ~1.5 MB
            last_updated="2025-09-29T12:12:12.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # SBNABP - Shadow Bundles National Adjusted Benchmarks (code 243)
        FileInventoryEntry(
            filename="D0259.PY2025.SBNABP.D250421.T1520214.xlsx",
            category="Reports",
            file_type_code=243,
            year=2025,
            size_bytes=450000,  # ~450 KB
            last_updated="2025-04-21T15:20:21.000Z",
            discovered_at="2025-01-01T00:00:00",
        ),
        # ⚠  ADD MORE ENTRIES HERE WHEN NEW SCHEMAS ARE ADDED ⚠
        # Template:
        # FileInventoryEntry(
        #     filename="<REAL_FILENAME_MATCHING_PATTERN>",
        #     category="<CATEGORY_FROM_SCHEMA>",
        #     file_type_code=<CODE_FROM_SCHEMA>,
        #     year=2025,
        #     size_bytes=<REALISTIC_SIZE>,
        #     last_updated="2025-XX-XXT00:00:00.000Z",
        #     discovered_at="2025-01-01T00:00:00"
        # ),
    ]

    return entries


def get_file_types_by_category(category: str) -> list[dict]:
    """Get all file types for a specific category."""
    return [ft for ft in REAL_SCHEMA_FILE_TYPES if ft["category"] == category]


def get_all_categories() -> list[str]:
    """Get all unique categories."""
    return sorted({ft["category"] for ft in REAL_SCHEMA_FILE_TYPES})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_config(tmp_path):
    """Build a FourICLIConfig rooted in tmp_path."""

    working = tmp_path / "working"
    working.mkdir(parents=True, exist_ok=True)
    (working / "config.txt").write_text("dummy")

    data = tmp_path / "data"
    bronze = data / "bronze"
    bronze.mkdir(parents=True, exist_ok=True)
    archive = data / "archive"
    archive.mkdir(parents=True, exist_ok=True)
    logs = data / "logs"
    logs.mkdir(parents=True, exist_ok=True)
    tracking = logs / "tracking"
    tracking.mkdir(parents=True, exist_ok=True)

    return FourICLIConfig(
        binary_path=tmp_path / "4icli",
        working_dir=working,
        data_path=data,
        bronze_dir=bronze,
        archive_dir=archive,
        silver_dir=data / "silver",
        gold_dir=data / "gold",
        log_dir=logs,
        tracking_dir=tracking,
        default_year=2025,
        default_apm_id="D0259",
        request_delay=0.0,  # no delay in tests
    )


def _mock_log_writer():
    lw = MagicMock()
    lw.info = MagicMock()
    lw.warning = MagicMock()
    lw.error = MagicMock()
    lw.debug = MagicMock()
    return lw


"""
Comprehensive tests for optional-dependency parsers in the _parsers package.

These parsers require third-party dependencies (beautifulsoup4, pypdf,
pylatexenc, bibtexparser, markdown, python-frontmatter, pydantic) that
are NOT guaranteed to be available in a skinny install. Tests mock
imports or test directly when the deps exist.
"""


def _has(module_name: str) -> bool:
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


HAS_BS4 = _has("bs4")

HAS_PYPDF = _has("pypdf")

HAS_PYLATEXENC = _has("pylatexenc")

HAS_BIBTEXPARSER = _has("bibtexparser")

HAS_MARKDOWN = _has("markdown")

HAS_FRONTMATTER = _has("frontmatter")

HAS_PYDANTIC = _has("pydantic")

HAS_OPENPYXL = _has("openpyxl")


"""
Comprehensive coverage tests for the _4icli package.

Covers gaps in: cli.py, client.py, processor.py, _shared_drive_mapping.py,
config.py, inventory.py, comparison.py, models.py, state.py, registry.py
"""


@pytest.fixture
def tmp_bronze(tmp_path):
    d = tmp_path / "bronze"
    d.mkdir()
    return d


@pytest.fixture
def tmp_working(tmp_path):
    d = tmp_path / "working"
    d.mkdir()
    (d / "config.txt").write_text("dummy")
    return d


@pytest.fixture
def make_config(tmp_path, tmp_bronze, tmp_working):
    return FourICLIConfig(
        binary_path=tmp_path / "4icli",
        working_dir=tmp_working,
        data_path=tmp_path,
        bronze_dir=tmp_bronze,
        archive_dir=tmp_path / "archive",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        log_dir=tmp_path / "logs",
        tracking_dir=tmp_path / "tracking",
        default_year=2025,
        default_apm_id="D0259",
    )


@pytest.fixture
def mock_lw():
    lw = MagicMock()
    lw.info = MagicMock()
    lw.warning = MagicMock()
    lw.error = MagicMock()
    lw.debug = MagicMock()
    return lw
