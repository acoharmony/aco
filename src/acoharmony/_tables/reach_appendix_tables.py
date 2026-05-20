# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for the REACH Financial Operating Guide Appendix
Tables workbook.

CMS publishes a yearly ``FinOverview_REACH_PY{YYYY}_Financial Operating
Guide Appendix Tables.xlsx`` workbook containing three reference tables
that feed the High-Needs eligibility logic:

    - Table B.6.1 — Mobility Impairment ICD-10 Codes (per
      ACO REACH Participation Agreement, Appendix A, Section IV.B.1(a)).
    - Table B.6.2 — Frailty HCPCS Codes (per Section IV.B.1(d); the
      workbook's column header "ICD-10 Code" is misnamed — these are
      HCPCS codes for hospital-bed and transfer-equipment claims).
    - Table B.6.3 — Evaluation & Management Services (PQEM category
      catalogue used by Claims-Based Alignment and the Primary Care
      Capitation calculation).

Per Section IV.B.1(a) and IV.B.1(d) of the Participation Agreement, the
code lists "will be specified by CMS prior to the start of the relevant
Performance Year" — so the workbook is versioned per PY. The file_pattern
wildcards the PY so each year's workbook lands via the same bronze stage.

This is a multi-output table: the parser emits one silver parquet per
sheet so downstream criterion expressions can ``scan_parquet`` the one
they need without unpacking a workbook at query time.

B.6.1 stores one row per category with all codes in a single
comma-separated cell; downstream normalization (in the
``_high_needs_seed_loader`` expression module) explodes that into one
row per code. B.6.2 and B.6.3 are already one-row-per-code.
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_sheets,
    with_storage,
)

_FILE_PATTERN = "FinOverview_REACH_PY????_Financial Operating Guide Appendix Tables.xlsx"


@register_schema(
    name="reach_appendix_tables",
    version=1,
    tier="bronze",
    description=(
        "REACH Financial Operating Guide Appendix Tables — B.6.1 mobility "
        "impairment ICD-10, B.6.2 frailty HCPCS, B.6.3 PQEM/E&M services"
    ),
    file_patterns={"reach": [_FILE_PATTERN]},
)
@with_parser(
    type="excel_multi_sheet",
    parser="excel_multi_sheet",
    multi_output=True,
    encoding="utf-8",
    has_header=False,
    embedded_transforms=False,
    sheet_config={
        # Row 1 is a title banner ("Table B.6.1: Mobility Impairment…");
        # row 2 carries the column headers; data starts at row 3.
        "header_row": 1,
        "data_start_row": 2,
        "column_mapping_strategy": "header_match",
        "end_marker_column": 0,
        "end_marker_value": "__NO_MARKER__",
    },
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": [_FILE_PATTERN]},
    silver={
        "output_name": "reach_appendix_tables.parquet",
        "refresh_frequency": "annually",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_name": "Table B.6.1",
            "sheet_index": 0,
            "sheet_type": "mobility_impairment_icd10",
            "description": (
                "Mobility Impairment ICD-10 Codes used to evaluate criterion "
                "IV.B.1(a) of the Participation Agreement. One row per "
                "category, codes stored as a comma-separated string."
            ),
            "columns": [
                {
                    "name": "category",
                    "position": 0,
                    "data_type": "string",
                    "description": "Clinical category grouping the codes",
                },
                {
                    "name": "icd10_codes",
                    "position": 1,
                    "data_type": "string",
                    "description": "Comma-separated list of ICD-10 codes",
                },
            ],
        },
        {
            "sheet_name": "Table B.6.2",
            "sheet_index": 1,
            "sheet_type": "frailty_hcpcs",
            "description": (
                "Frailty HCPCS codes used to evaluate criterion IV.B.1(d) "
                "of the Participation Agreement. Workbook's 'ICD-10 Code' "
                "column header is misnamed — these are HCPCS codes."
            ),
            "columns": [
                {
                    "name": "category",
                    "position": 0,
                    "data_type": "string",
                    "description": "Transfer Equipment, Hospital Bed, etc.",
                },
                {
                    "name": "hcpcs_code",
                    "position": 1,
                    "data_type": "string",
                    "description": "HCPCS code (stored as string to preserve leading zeros)",
                },
                {
                    "name": "long_descriptor",
                    "position": 2,
                    "data_type": "string",
                    "description": "Full HCPCS long descriptor",
                },
            ],
        },
        {
            "sheet_name": "Table B.6.3",
            "sheet_index": 2,
            "sheet_type": "pqem_em_services",
            "description": (
                "PQEM / Evaluation & Management services catalogue. Some "
                "rows carry multi-line Effective Date / Used-for-Claims "
                "Based Alignment strings encoding version history; the "
                "raw parse keeps the newline-delimited values verbatim "
                "and the downstream normalizer splits them."
            ),
            "columns": [
                {
                    "name": "pqem_category",
                    "position": 0,
                    "data_type": "string",
                    "description": "PQEM category grouping",
                },
                {
                    "name": "hcpcs_code",
                    "position": 1,
                    "data_type": "string",
                    "description": "CPT / HCPCS code",
                },
                {
                    "name": "long_descriptor",
                    "position": 2,
                    "data_type": "string",
                    "description": "Full CPT/HCPCS long descriptor",
                },
                {
                    "name": "effective_date",
                    "position": 3,
                    "data_type": "string",
                    "description": (
                        "Effective date. May be a single date or a newline-"
                        "delimited list of dates for codes whose "
                        "claims-based-alignment flag changed mid-year."
                    ),
                },
                {
                    "name": "used_for_claims_based_alignment",
                    "position": 4,
                    "data_type": "string",
                    "description": (
                        "'Y' or 'N'. May be newline-delimited when paired "
                        "with a multi-line effective_date."
                    ),
                },
                {
                    "name": "used_for_pcc",
                    "position": 5,
                    "data_type": "string",
                    "description": "'Y' or 'N' — used for Primary Care Capitation calculation",
                },
                {
                    "name": "source",
                    "position": 6,
                    "data_type": "string",
                    "description": "Federal Register URL citing the code's inclusion",
                },
            ],
        },
    ]
)
@dataclass
class ReachAppendixTables:
    """
    REACH Financial Operating Guide Appendix Tables workbook — multi-sheet
    container.

    Instances carry the sheet identifier plus the union of all three
    sheets' columns (optional). Downstream consumers read the silver
    parquet for the specific sheet they care about rather than
    instantiating this class directly.
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    # Union of columns across all three sheets — every field is optional
    # since each row only populates one sheet's worth of columns.
    category: str | None = Field(default=None, description="Category label")
    icd10_codes: str | None = Field(
        default=None, description="Comma-separated ICD-10 codes (B.6.1)"
    )
    hcpcs_code: str | None = Field(default=None, description="HCPCS code (B.6.2, B.6.3)")
    long_descriptor: str | None = Field(default=None, description="Long descriptor")
    pqem_category: str | None = Field(default=None, description="PQEM category (B.6.3)")
    effective_date: str | None = Field(
        default=None, description="Effective date, possibly newline-delimited (B.6.3)"
    )
    used_for_claims_based_alignment: str | None = Field(
        default=None, description="Y/N for CBA eligibility (B.6.3)"
    )
    used_for_pcc: str | None = Field(default=None, description="Y/N for PCC (B.6.3)")
    source: str | None = Field(default=None, description="Federal Register source URL (B.6.3)")

    def to_dict(self) -> dict:
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ReachAppendixTables":
        return cls(**data)
