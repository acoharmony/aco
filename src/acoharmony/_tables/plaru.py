# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for plaru schema.

 a type-safe Pydantic dataclass for the schema with:
- Runtime type validation
- Field-level validators for known patterns (MBI, NPI, ICD codes, etc.)
- Decorator-based schema registration (parser-aware, transform-aware)
- Dynamic metadata access via class methods (no hardcoded globals)
- IDE autocomplete and type checking support
"""

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_sheets,
    with_storage,
)


@register_schema(
    name="plaru",
    version=2,
    tier="bronze",
    description="Preliminary Alternative Payment Arrangement Report Unredacted",
    file_patterns={"reach": ["REACH.D????.PLARU.PY????.D??????.T*.xlsx"]},
)
@with_parser(type="excel_multi_sheet", encoding="utf-8", has_header=False, embedded_transforms=True)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["REACH.D????.PLARU.PY????.D??????.T*.xlsx"]},
    silver={"output_name": "plaru.parquet", "refresh_frequency": "annually"},
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "report_parameters",
            "description": "Report Parameters metadata (key-value pairs from row 5 onwards)",
            "sheet_config": {
                "header_row": 3,
                "data_start_row": 4,
                "end_marker_column": 0,
                "end_marker_value": "version",
            },
            "transform": {
                "type": "key_value_pivot",
                "key_column": "alternative_payment_arrangement_election",
                "value_column": "__unnamed__1",
                "skip_empty_values": True,
                "sanitize_keys": True,
            },
        },
        {
            "sheet_index": 1,
            "sheet_type": "payment_history",
            "description": "Payment History data (standard table with header at row 4)",
            "sheet_config": {"header_row": 3, "data_start_row": 4},
            "columns": [
                {"name": "payment_date", "data_type": "string", "source_name": "Payment Date"},
                {
                    "name": "base_pcc_total",
                    "data_type": "string",
                    "source_name": "Base PCC Total",
                },
                {
                    "name": "enhanced_pcc_total",
                    "data_type": "string",
                    "source_name": "Enhanced PCC Total",
                },
                {"name": "apo_total", "data_type": "string", "source_name": "APO Total"},
            ],
        },
        {
            "sheet_index": 2,
            "sheet_type": "base_pcc_pmt_detailed",
            "description": "Base PCC Payment Detailed - matrix extraction with hierarchical sections",
            "sheet_config": {"header_row": 4, "data_start_row": 6},
            "transform": {
                "type": "matrix_extractor",
                "detection": {
                    "min_rows": 2,
                    "min_cols": 1,
                    "break_rows": 0,
                    "break_cols": 1,
                    "section_detection": {
                        "section_column": 1,
                        "aggregation_markers": ["Total", "Projected"],
                        "hierarchy_depth": 2,
                        "detect_bold": False,
                    },
                },
                "label_offsets": {
                    "column_labels": [
                        {"offset": -2, "forward_fill": True},
                        {"offset": -1, "forward_fill": False},
                    ],
                    "row_labels": [],
                },
                "naming": {
                    "separator": "_",
                    "skip_empty": True,
                    "sanitize": True,
                    "order": ["row", "col"],
                },
            },
        },
        {
            "sheet_index": 3,
            "sheet_type": "enhanced_pcc_pmt_detailed",
            "description": "Enhanced PCC Payment Detailed - matrix extraction with hierarchical sections",
            "sheet_config": {"header_row": 4, "data_start_row": 6},
            "transform": {
                "type": "matrix_extractor",
                "detection": {
                    "min_rows": 2,
                    "min_cols": 1,
                    "break_rows": 0,
                    "break_cols": 1,
                    "section_detection": {
                        "section_column": 1,
                        "aggregation_markers": ["Total", "Projected"],
                        "hierarchy_depth": 2,
                        "detect_bold": False,
                    },
                },
                "label_offsets": {
                    "column_labels": [
                        {"offset": -2, "forward_fill": True},
                        {"offset": -1, "forward_fill": False},
                    ],
                    "row_labels": [],
                },
                "naming": {
                    "separator": "_",
                    "skip_empty": True,
                    "sanitize": True,
                    "order": ["row", "col"],
                },
            },
        },
        {
            "sheet_index": 4,
            "sheet_type": "base_pcc_pct",
            "description": "Base PCC PCT - matrix extraction with provider category groupings",
            "sheet_config": {"header_row": 5, "data_start_row": 7},
            "transform": {
                "type": "matrix_extractor",
                "detection": {
                    "min_rows": 2,
                    "min_cols": 1,
                    "break_rows": 0,
                    "break_cols": 1,
                    "section_detection": {
                        "section_column": 1,
                        "aggregation_markers": ["Total", "Payment"],
                        "hierarchy_depth": 2,
                        "detect_bold": False,
                    },
                },
                "label_offsets": {
                    "column_labels": [
                        {"offset": -3, "forward_fill": True},
                        {"offset": -2, "forward_fill": False},
                    ],
                    "row_labels": [],
                },
                "naming": {
                    "separator": "_",
                    "skip_empty": True,
                    "sanitize": True,
                    "order": ["row", "col"],
                },
            },
        },
        {
            "sheet_index": 5,
            "sheet_type": "enhanced_pcc_pct_ceil",
            "description": "Enhanced PCC PCT Ceiling - matrix extraction with provider category groupings",
            "sheet_config": {"header_row": 5, "data_start_row": 7},
            "transform": {
                "type": "matrix_extractor",
                "detection": {
                    "min_rows": 2,
                    "min_cols": 1,
                    "break_rows": 0,
                    "break_cols": 1,
                    "section_detection": {
                        "section_column": 1,
                        "aggregation_markers": ["Total", "Payment"],
                        "hierarchy_depth": 2,
                        "detect_bold": False,
                    },
                },
                "label_offsets": {
                    "column_labels": [
                        {"offset": -3, "forward_fill": True},
                        {"offset": -2, "forward_fill": False},
                    ],
                    "row_labels": [],
                },
                "naming": {
                    "separator": "_",
                    "skip_empty": True,
                    "sanitize": True,
                    "order": ["row", "col"],
                },
            },
        },
        {
            "sheet_index": 6,
            "sheet_type": "apo_pmt_detailed",
            "description": "APO Payment Detailed - matrix extraction with hierarchical sections",
            "sheet_config": {"header_row": 4, "data_start_row": 6},
            "transform": {
                "type": "matrix_extractor",
                "detection": {
                    "min_rows": 2,
                    "min_cols": 1,
                    "break_rows": 0,
                    "break_cols": 1,
                    "section_detection": {
                        "section_column": 1,
                        "aggregation_markers": ["Total", "Projected"],
                        "hierarchy_depth": 2,
                        "detect_bold": False,
                    },
                },
                "label_offsets": {
                    "column_labels": [
                        {"offset": -2, "forward_fill": True},
                        {"offset": -1, "forward_fill": False},
                    ],
                    "row_labels": [],
                },
                "naming": {
                    "separator": "_",
                    "skip_empty": True,
                    "sanitize": True,
                    "order": ["row", "col"],
                },
            },
        },
        {
            "sheet_index": 7,
            "sheet_type": "apo_pbpm",
            "description": "APO PBPM - matrix extraction with provider category groupings",
            "sheet_config": {"header_row": 5, "data_start_row": 7},
            "transform": {
                "type": "matrix_extractor",
                "detection": {
                    "min_rows": 2,
                    "min_cols": 1,
                    "break_rows": 0,
                    "break_cols": 1,
                    "section_detection": {
                        "section_column": 1,
                        "aggregation_markers": ["Total", "Payment"],
                        "hierarchy_depth": 2,
                        "detect_bold": False,
                    },
                },
                "label_offsets": {
                    "column_labels": [
                        {"offset": -3, "forward_fill": True},
                        {"offset": -2, "forward_fill": False},
                    ],
                    "row_labels": [],
                },
                "naming": {
                    "separator": "_",
                    "skip_empty": True,
                    "sanitize": True,
                    "order": ["row", "col"],
                },
            },
        },
        {
            "sheet_index": 8,
            "sheet_type": "data_claims_prvdr",
            "description": "Data Claims Provider (standard wide table with single header row)",
            "sheet_config": {"header_row": 0, "data_start_row": 1},
            "columns": [
                {"name": "perf_yr", "data_type": "string", "source_name": "PERF_YR"},
                {"name": "clndr_yr", "data_type": "string", "source_name": "CLNDR_YR"},
                {"name": "clndr_mnth", "data_type": "string", "source_name": "CLNDR_MNTH"},
                {"name": "aco_id", "data_type": "string", "source_name": "ACO_ID"},
                {"name": "aco_tcc_ind", "data_type": "string", "source_name": "ACO_TCC_IND"},
                {"name": "aco_pcc_ind", "data_type": "string", "source_name": "ACO_PCC_IND"},
                {"name": "aco_apo_ind", "data_type": "string", "source_name": "ACO_APO_IND"},
                {"name": "bill_npi", "data_type": "string", "source_name": "BILL_NPI"},
                {"name": "ccn", "data_type": "string", "source_name": "CCN"},
                {"name": "bill_tin", "data_type": "string", "source_name": "BILL_TIN"},
                {"name": "clm_line_tin", "data_type": "string", "source_name": "CLM_LINE_TIN"},
                {"name": "ind_npi", "data_type": "string", "source_name": "IND_NPI"},
                {"name": "clm_type_cd", "data_type": "string", "source_name": "CLM_TYPE_CD"},
                {"name": "prvdr_class", "data_type": "string", "source_name": "PRVDR_CLASS"},
                {
                    "name": "tcc_elect_pct",
                    "data_type": "string",
                    "source_name": "TCC_ELECT_PCT",
                },
                {
                    "name": "pcc_elect_pct",
                    "data_type": "string",
                    "source_name": "PCC_ELECT_PCT",
                },
                {
                    "name": "apo_elect_pct",
                    "data_type": "string",
                    "source_name": "APO_ELECT_PCT",
                },
                {"name": "fqhc_ind", "data_type": "string", "source_name": "FQHC_IND"},
                {"name": "rhc_ind", "data_type": "string", "source_name": "RHC_IND"},
                {"name": "cah2_ind", "data_type": "string", "source_name": "CAH2_IND"},
                {
                    "name": "blend_trend_factor",
                    "data_type": "string",
                    "source_name": "BLEND_TREND_FACTOR",
                },
                {"name": "pcc_phys_ind", "data_type": "string", "source_name": "PCC_PHYS_IND "},
                {"name": "hcpcs_cd", "data_type": "string", "source_name": "HCPCS_CD"},
                {
                    "name": "ind_npi_spclty_cd",
                    "data_type": "string",
                    "source_name": "IND_NPI_SPCLTY_CD  ",
                },
                {"name": "apa_cd", "data_type": "string", "source_name": "APA_CD  "},
                {"name": "clm_pos_cd", "data_type": "string", "source_name": "CLM_POS_CD "},
                {"name": "fac_ind", "data_type": "string", "source_name": "FAC_IND"},
                {
                    "name": "sub_abuse_ind",
                    "data_type": "string",
                    "source_name": "SUB_ABUSE_IND",
                },
                {"name": "optout_ind", "data_type": "string", "source_name": "OPTOUT_IND"},
                {"name": "clm_pmt_amt", "data_type": "string", "source_name": "CLM_PMT_AMT"},
                {"name": "ucc_amt", "data_type": "string", "source_name": "UCC_AMT"},
                {
                    "name": "apa_rdctn_amt",
                    "data_type": "string",
                    "source_name": "APA_RDCTN_AMT",
                },
                {"name": "sqstr_amt", "data_type": "string", "source_name": "SQSTR_AMT"},
                {"name": "op_dsh_amt", "data_type": "string", "source_name": "OP_DSH_AMT"},
                {"name": "op_ime_amt", "data_type": "string", "source_name": "OP_IME_AMT"},
                {"name": "outlier_amt", "data_type": "string", "source_name": "OUTLIER_AMT "},
                {"name": "newtech_amt", "data_type": "string", "source_name": "NEWTECH_AMT "},
                {"name": "islet_amt", "data_type": "string", "source_name": "ISLET_AMT"},
                {"name": "pip_amt", "data_type": "string", "source_name": "PIP_AMT"},
                {
                    "name": "total_cbp_amt",
                    "data_type": "string",
                    "source_name": "TOTAL_CBP_AMT",
                },
                {
                    "name": "total_cbp_amt_adjust",
                    "data_type": "string",
                    "source_name": "TOTAL_CBP_AMT_ADJUST",
                },
                {
                    "name": "total_exp_amt",
                    "data_type": "string",
                    "source_name": "TOTAL_EXP_AMT",
                },
                {"name": "tcc_rdct_amt", "data_type": "string", "source_name": "TCC_RDCT_AMT "},
                {"name": "wh_amt", "data_type": "string", "source_name": "WH_AMT "},
                {"name": "pcc_rdct_amt", "data_type": "string", "source_name": "PCC_RDCT_AMT "},
                {
                    "name": "part100_pcc_rdct_amt",
                    "data_type": "string",
                    "source_name": "PART100_PCC_RDCT_AMT ",
                },
                {"name": "apo_rdct_amt", "data_type": "string", "source_name": "APO_RDCT_AMT "},
                {
                    "name": "apo_rdct_amt_trended",
                    "data_type": "string",
                    "source_name": "APO_RDCT_AMT_TRENDED ",
                },
                {"name": "lookback_ind", "data_type": "string", "source_name": "LOOKBACK_IND"},
            ],
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=220,
    file_pattern="REACH.D????.PLARU.PY????.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="annually",
)
@dataclass
class Plaru:
    """
    Preliminary Alternative Payment Arrangement Report Unredacted

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - Plaru.schema_name() -> str
        - Plaru.schema_metadata() -> dict
        - Plaru.parser_config() -> dict
        - Plaru.transform_config() -> dict
        - Plaru.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "Plaru":
        """Create instance from dictionary."""
        return cls(**data)
