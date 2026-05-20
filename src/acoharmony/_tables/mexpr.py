# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for mexpr schema.

Generated from: _schemas/mexpr.yml
"""

from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_four_icli,
    with_parser,
    with_sheets,
    with_storage,
)

# Column definitions using header_text matching (column counts vary across PY files)
_SHARED_DIMS = [
    {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string", "description": "Performance year"},
    {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string", "description": "Calendar year"},
    {"name": "clndr_mnth", "header_text": "CLNDR_MNTH", "data_type": "string", "description": "Calendar month"},
    {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string", "description": "Benchmark type (AD/PR)"},
    {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string", "description": "Alignment type (C=Claims, V=Voluntary)"},
    {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string", "description": "Benchmark type (RATEBOOK/BLEND)"},
    {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string", "description": "ACO identifier"},
]

_CLAIMS_COLS = _SHARED_DIMS + [
    {"name": "clm_type_cd", "header_text": "CLM_TYPE_CD", "data_type": "string", "description": "Claim type code (10=HHA, 20=SNF, 40=Outpatient, 50=Hospice, 60=Inpatient, 71=Physician, 82=DME)"},
    {"name": "clm_pmt_amt_agg", "header_text": "CLM_PMT_AMT_AGG", "data_type": "decimal", "description": "Claim payment amount aggregate"},
    {"name": "sqstr_amt_agg", "header_text": "SQSTR_AMT_AGG", "data_type": "decimal", "description": "Sequestration amount aggregate"},
    {"name": "apa_rdctn_amt_agg", "header_text": "APA_RDCTN_AMT_AGG", "data_type": "decimal", "description": "APA reduction amount aggregate"},
    {"name": "pcc_rdctn_amt_agg", "header_text": "PCC_RDCTN_AMT_AGG", "data_type": "decimal", "description": "PCC reduction amount aggregate"},
    {"name": "tcc_rdctn_amt_agg", "header_text": "TCC_RDCTN_AMT_AGG", "data_type": "decimal", "description": "TCC reduction amount aggregate"},
    {"name": "apo_rdctn_amt_agg", "header_text": "APO_RDCTN_AMT_AGG", "data_type": "decimal", "description": "APO reduction amount aggregate"},
    {"name": "ucc_amt_agg", "header_text": "UCC_AMT_AGG", "data_type": "decimal", "description": "Uncompensated care amount aggregate"},
    {"name": "nonpbp_rdct_amt_agg", "header_text": "NONPBP_RDCT_AMT_AGG", "data_type": "decimal", "description": "Non-PBP reduction amount aggregate"},
    {"name": "op_dsh_amt_agg", "header_text": "OP_DSH_AMT_AGG", "data_type": "decimal", "description": "Operational DSH amount aggregate"},
    {"name": "cp_dsh_amt_agg", "header_text": "CP_DSH_AMT_AGG", "data_type": "decimal", "description": "Capital DSH amount aggregate"},
    {"name": "op_ime_amt_agg", "header_text": "OP_IME_AMT_AGG", "data_type": "decimal", "description": "Operational IME amount aggregate"},
    {"name": "cp_ime_amt_agg", "header_text": "CP_IME_AMT_AGG", "data_type": "decimal", "description": "Capital IME amount aggregate"},
    {"name": "dc_amt_agg_apa", "header_text": "DC_AMT_AGG_APA", "data_type": "decimal", "description": "Direct contracting amount aggregate APA"},
    {"name": "total_exp_amt_agg", "header_text": "TOTAL_EXP_AMT_AGG", "data_type": "decimal", "description": "Total expenditure amount aggregate"},
    {"name": "srvc_month", "header_text": "SRVC_MONTH", "data_type": "string", "description": "Service month (YYYYMM)"},
    {"name": "efctv_month", "header_text": "EFCTV_MONTH", "data_type": "string", "description": "Effective month (YYYYMM)"},
]

_ENROLL_COLS = _SHARED_DIMS + [
    {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer", "description": "Beneficiary distinct count"},
    {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer", "description": "Eligible months"},
]


@register_schema(
    name="mexpr",
    version=2,
    tier="bronze",
    description="Monthly Expenditure Report",
    file_patterns={"reach": ["*MEXPR*"]},
)
@with_parser(
    type="excel_multi_sheet",
    parser="excel_multi_sheet",
    multi_output=True,
    encoding="utf-8",
    has_header=False,
    embedded_transforms=False,
    sheet_config={
        "header_row": 0,
        "data_start_row": 1,
        "column_mapping_strategy": "header_match",
        "end_marker_column": 0,
        "end_marker_value": "",
    },
)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*MEXPR*"]},
    silver={
        "output_name": "mexpr.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 3,
            "sheet_type": "data_claims",
            "description": "Claims expenditure data by month, claim type, alignment, and benchmark",
            "header_row": 0,
            "data_start_row": 1,
            "columns": _CLAIMS_COLS,
        },
        {
            "sheet_index": 4,
            "sheet_type": "data_enroll",
            "description": "Enrollment counts by month, alignment, and benchmark",
            "header_row": 0,
            "data_start_row": 1,
            "columns": _ENROLL_COLS,
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=214,
    file_pattern="REACH.D????.MEXPR.??.PY????.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class Mexpr:
    """Monthly Expenditure Report."""

    pass  # Multi-sheet Excel — columns defined per-sheet above
