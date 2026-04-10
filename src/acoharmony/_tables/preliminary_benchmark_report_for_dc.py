# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for preliminary_benchmark_report_for_dc schema.

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
    name="preliminary_benchmark_report_for_dc",
    version=2,
    tier="bronze",
    description="Preliminary Benchmark Report for DC",
    file_patterns={"reach": ["*PRLBR.PY*"]},
)
@with_parser(type="unknown", encoding="utf-8", has_header=False, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["*PRLBR.PY*"]},
    silver={
        "output_name": "preliminary_benchmark_report_for_dc.parquet",
        "refresh_frequency": "monthly",
    },
)
@with_sheets(
    sheets=[
        {
            "sheet_index": 0,
            "sheet_type": "report_parameters",
            "description": "ACO configuration and performance year parameters - 60 rows with metadata",
            "columns": [
                {
                    "name": "parameter_name",
                    "position": 0,
                    "data_type": "string",
                    "description": "Parameter name/label",
                },
                {
                    "name": "value_primary",
                    "position": 1,
                    "data_type": "string",
                    "description": "Primary parameter value",
                },
                {
                    "name": "value_2017",
                    "position": 2,
                    "data_type": "string",
                    "description": "2017 value (for incurred/paid parameters)",
                },
                {
                    "name": "value_2018",
                    "position": 3,
                    "data_type": "string",
                    "description": "2018 value (for incurred/paid parameters)",
                },
                {
                    "name": "value_2019",
                    "position": 4,
                    "data_type": "string",
                    "description": "2019 value (for incurred/paid parameters)",
                },
                {
                    "name": "value_2024",
                    "position": 5,
                    "data_type": "string",
                    "description": "2024 value (for incurred/paid parameters)",
                },
            ],
            "sections": [
                {
                    "name": "aco_parameters",
                    "start_row": 1,
                    "end_row": 14,
                    "description": "ACO-specific configuration (PY, ID, Type, Risk Arrangement, etc.)",
                },
                {
                    "name": "model_wide_parameters",
                    "start_row": 15,
                    "end_row": 22,
                    "description": "Model-wide parameters (blend %, ceiling, floor, trends, completion factors)",
                },
                {
                    "name": "incurred_paid_parameters",
                    "start_row": 25,
                    "end_row": 35,
                    "description": "Performance and reporting period date ranges by year (row 26 has year headers)",
                },
            ],
        },
        {
            "sheet_index": 1,
            "sheet_type": "financial_settlement",
            "description": "Financial settlement calculation flow - 75 rows, 5 cols (AD, ESRD, TOTAL)",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number in calculation flow",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of calculation line",
                },
                {
                    "name": "ad_value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Aged & Disabled value",
                },
                {
                    "name": "esrd_value",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "ESRD value",
                },
                {
                    "name": "total_value",
                    "position": 4,
                    "data_type": "decimal",
                    "description": "Total combined value",
                },
            ],
            "sections": [
                {
                    "name": "benchmark_expenditure",
                    "start_row": 7,
                    "end_row": 21,
                    "description": "Lines 1-14: Benchmark calculation for all aligned beneficiaries",
                },
                {
                    "name": "performance_expenditure",
                    "start_row": 22,
                    "end_row": 28,
                    "description": "Lines 15-21: ACO performance period expenditure with HEBA",
                },
                {
                    "name": "savings_losses",
                    "start_row": 29,
                    "end_row": 40,
                    "description": "Lines 22-33: Cost of care and gross savings/losses",
                },
                {
                    "name": "monies_owed",
                    "start_row": 41,
                    "end_row": 55,
                    "description": "Lines 34-48: Calculation of total monies owed",
                },
                {
                    "name": "risk_corridors",
                    "start_row": 56,
                    "end_row": 75,
                    "description": "Lines 49-68: Application of risk corridors",
                },
            ],
        },
        {
            "sheet_index": 2,
            "sheet_type": "riskscore_ad",
            "description": "A&D Risk Score calculations - 40 rows, 4 cols (RY2022 and PY values)",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number or section label",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of risk score component",
                },
                {
                    "name": "reference_year_value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Reference Year 2022 value",
                },
                {
                    "name": "py_value",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Performance Year value",
                },
            ],
            "sections": [
                {
                    "name": "claims_aligned",
                    "start_row": 6,
                    "end_row": 15,
                    "description": "Lines 1-9: Claims aligned risk score calculation (raw, normalized, capped, benchmark)",
                },
                {
                    "name": "voluntary_aligned_new",
                    "start_row": 17,
                    "end_row": 21,
                    "description": "Lines 10-12: Newly voluntary-aligned risk score",
                },
                {
                    "name": "voluntary_aligned_continuous",
                    "start_row": 22,
                    "end_row": 33,
                    "description": "Lines 13-21: Continuously voluntary-aligned risk score",
                },
                {
                    "name": "weighted_average",
                    "start_row": 34,
                    "end_row": 40,
                    "description": "Lines 22-26: Weighted average risk scores",
                },
            ],
        },
        {
            "sheet_index": 3,
            "sheet_type": "riskscore_esrd",
            "description": "ESRD Risk Score calculations - 39 rows, 4 cols (same structure as AD)",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number or section label",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of risk score component",
                },
                {
                    "name": "reference_year_value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Reference Year 2022 value",
                },
                {
                    "name": "py_value",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Performance Year value",
                },
            ],
            "sections": [
                {
                    "name": "claims_aligned",
                    "start_row": 6,
                    "end_row": 14,
                    "description": "Lines 1-8: Claims aligned risk score calculation",
                },
                {
                    "name": "voluntary_aligned_new",
                    "start_row": 16,
                    "end_row": 20,
                    "description": "Lines 9-11: Newly voluntary-aligned risk score",
                },
                {
                    "name": "voluntary_aligned_continuous",
                    "start_row": 21,
                    "end_row": 32,
                    "description": "Lines 12-19: Continuously voluntary-aligned risk score",
                },
                {
                    "name": "weighted_average",
                    "start_row": 33,
                    "end_row": 39,
                    "description": "Lines 20-24: Weighted average risk scores",
                },
            ],
        },
        {
            "sheet_index": 4,
            "sheet_type": "stop_loss_charge",
            "description": "Stop loss charge calculations - 35 rows, 6 cols (RY 2020-2022 and PY charge)",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of charge component",
                },
                {
                    "name": "reference_year_1",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Reference Year 1 (2020) value",
                },
                {
                    "name": "reference_year_2",
                    "position": 3,
                    "data_type": "decimal",
                    "description": "Reference Year 2 (2021) value",
                },
                {
                    "name": "reference_year_3",
                    "position": 4,
                    "data_type": "decimal",
                    "description": "Reference Year 3 (2022) value",
                },
                {
                    "name": "charge_value",
                    "position": 5,
                    "data_type": "decimal",
                    "description": "Performance Period charge value",
                },
            ],
            "sections": [
                {
                    "name": "ad_experience",
                    "start_row": 7,
                    "end_row": 14,
                    "description": "Lines 1-7: A&D beneficiaries, months, expenditure, PBPM",
                },
                {
                    "name": "esrd_experience",
                    "start_row": 15,
                    "end_row": 22,
                    "description": "Lines 8-14: ESRD beneficiaries, months, expenditure, PBPM",
                },
                {
                    "name": "stop_loss_experience",
                    "start_row": 23,
                    "end_row": 35,
                    "description": "Lines 15-27: Stop loss bands, payouts, and charges",
                },
            ],
        },
        {
            "sheet_index": 5,
            "sheet_type": "stop_loss_payout",
            "description": "Stop loss payout calculations - 27 rows, 3 cols",
            "columns": [
                {
                    "name": "line_number",
                    "position": 0,
                    "data_type": "string",
                    "description": "Line number",
                },
                {
                    "name": "line_description",
                    "position": 1,
                    "data_type": "string",
                    "description": "Description of payout component",
                },
                {
                    "name": "value",
                    "position": 2,
                    "data_type": "decimal",
                    "description": "Performance Period 2024 payout value",
                },
            ],
            "sections": [
                {
                    "name": "beneficiary_counts",
                    "start_row": 7,
                    "end_row": 11,
                    "description": "Lines 1-4: Beneficiaries by stop-loss band",
                },
                {
                    "name": "expenditures",
                    "start_row": 12,
                    "end_row": 16,
                    "description": "Lines 5-8: Expenditures by stop-loss band",
                },
                {
                    "name": "payouts",
                    "start_row": 17,
                    "end_row": 21,
                    "description": "Lines 9-12: Stop-loss payouts by band",
                },
                {
                    "name": "payout_rates",
                    "start_row": 22,
                    "end_row": 26,
                    "description": "Lines 13-16: Payout rates as percentage",
                },
            ],
        },
        {
            "sheet_index": 6,
            "sheet_type": "claims",
            "description": "Claims data by calendar year/month, benchmark type, and claim type",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "clndr_mnth", "header_text": "CLNDR_MNTH", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "clm_type_cd", "header_text": "CLM_TYPE_CD", "data_type": "string"},
                {
                    "name": "clm_pmt_amt_agg",
                    "header_text": "CLM_PMT_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "sqstr_amt_agg",
                    "header_text": "SQSTR_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "apa_rdctn_amt_agg",
                    "header_text": "APA_RDCTN_AMT_AGG",
                    "data_type": "decimal",
                },
                {"name": "ucc_amt_agg", "header_text": "UCC_AMT_AGG", "data_type": "decimal"},
                {
                    "name": "op_dsh_amt_agg",
                    "header_text": "OP_DSH_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "cp_dsh_amt_agg",
                    "header_text": "CP_DSH_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "op_ime_amt_agg",
                    "header_text": "OP_IME_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "cp_ime_amt_agg",
                    "header_text": "CP_IME_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "nonpbp_rdct_amt_agg",
                    "header_text": "NONPBP_RDCT_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_amt_agg_apa",
                    "header_text": "ACO_AMT_AGG_APA",
                    "data_type": "decimal",
                },
                {"name": "srvc_month", "header_text": "SRVC_MONTH", "data_type": "string"},
                {"name": "efctv_month", "header_text": "EFCTV_MONTH", "data_type": "string"},
            ],
        },
        {
            "sheet_index": 7,
            "sheet_type": "risk",
            "description": "Risk score and beneficiary count data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "clndr_mnth", "header_text": "CLNDR_MNTH", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {"name": "va_cat", "header_text": "VA_CAT", "data_type": "string"},
                {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer"},
                {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer"},
                {
                    "name": "raw_risk_score",
                    "header_text": "RAW_RISK_SCORE",
                    "data_type": "decimal",
                },
                {
                    "name": "norm_risk_score",
                    "header_text": "NORM_RISK_SCORE",
                    "data_type": "decimal",
                },
                {"name": "risk_denom", "header_text": "RISK_DENOM", "data_type": "decimal"},
                {"name": "score_type", "header_text": "SCORE_TYPE", "data_type": "string"},
                {
                    "name": "bene_dcnt_annual",
                    "header_text": "BENE_DCNT_ANNUAL",
                    "data_type": "integer",
                },
            ],
        },
        {
            "sheet_index": 8,
            "sheet_type": "county",
            "description": "County-level beneficiary and payment data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {"name": "bnmrk_type", "header_text": "BNMRK_TYPE", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "cty_accrl_cd", "header_text": "CTY_ACCRL_CD", "data_type": "string"},
                {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer"},
                {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer"},
                {"name": "cty_rate", "header_text": "CTY_RATE", "data_type": "decimal"},
                {"name": "adj_cty_pmt", "header_text": "ADJ_CTY_PMT", "data_type": "decimal"},
                {"name": "gaf_trend", "header_text": "GAF_TREND", "data_type": "decimal"},
                {
                    "name": "adj_gaf_trend",
                    "header_text": "ADJ_GAF_TREND",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_index": 9,
            "sheet_type": "uspcc",
            "description": "US Per Capita Cost data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "uspcc", "header_text": "USPCC", "data_type": "decimal"},
                {"name": "ucc_hosp_adj", "header_text": "UCC_HOSP_ADJ", "data_type": "decimal"},
                {
                    "name": "adj_ffs_uspcc",
                    "header_text": "ADJ_FFS_USPCC",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_index": 10,
            "sheet_type": "heba",
            "description": "Health Equity Benchmark Adjustment data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {
                    "name": "heba_up_mnths",
                    "header_text": "HEBA_UP_MNTHS",
                    "data_type": "integer",
                },
                {
                    "name": "heba_down_mnths",
                    "header_text": "HEBA_DOWN_MNTHS",
                    "data_type": "integer",
                },
                {"name": "heba_up_amt", "header_text": "HEBA_UP_AMT", "data_type": "decimal"},
                {
                    "name": "heba_down_amt",
                    "header_text": "HEBA_DOWN_AMT",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_index": 11,
            "sheet_type": "stop_loss_county",
            "description": "Stop Loss County-level data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "cty_accrl_cd", "header_text": "CTY_ACCRL_CD", "data_type": "string"},
                {"name": "bene_dcnt", "header_text": "BENE_DCNT", "data_type": "integer"},
                {"name": "elig_mnths", "header_text": "ELIG_MNTHS", "data_type": "integer"},
                {"name": "gaf_trend", "header_text": "GAF_TREND", "data_type": "decimal"},
                {
                    "name": "adj_gaf_trend",
                    "header_text": "ADJ_GAF_TREND",
                    "data_type": "decimal",
                },
                {
                    "name": "avg_payout_pct",
                    "header_text": "AVG_PAYOUT_PCT",
                    "data_type": "decimal",
                },
                {
                    "name": "ad_ry_avg_pbpm",
                    "header_text": "AD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
                {
                    "name": "esrd_ry_avg_pbpm",
                    "header_text": "ESRD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
                {
                    "name": "adj_avg_payout_pct",
                    "header_text": "ADJ_AVG_PAYOUT_PCT",
                    "data_type": "decimal",
                },
                {
                    "name": "adj_ad_ry_avg_pbpm",
                    "header_text": "ADJ_AD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
                {
                    "name": "adj_esrd_ry_avg_pbpm",
                    "header_text": "ADJ_ESRD_RY_AVG_PBPM",
                    "data_type": "decimal",
                },
            ],
        },
        {
            "sheet_index": 12,
            "sheet_type": "stop_loss_payout_data",
            "description": "Stop Loss Payout data (DATA sheet)",
            "columns": [
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {
                    "name": "algn_aco_amt_agg",
                    "header_text": "ALGN_ACO_AMT_AGG",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp",
                    "header_text": "ACO_STOPLOSS_EXP",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp_b0",
                    "header_text": "ACO_STOPLOSS_EXP_B0",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp_b1",
                    "header_text": "ACO_STOPLOSS_EXP_B1",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_exp_b2",
                    "header_text": "ACO_STOPLOSS_EXP_B2",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_b0",
                    "header_text": "ACO_STOPLOSS_PAYOUT_B0",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_b1",
                    "header_text": "ACO_STOPLOSS_PAYOUT_B1",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_b2",
                    "header_text": "ACO_STOPLOSS_PAYOUT_B2",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_stoploss_payout_total",
                    "header_text": "ACO_STOPLOSS_PAYOUT_TOTAL",
                    "data_type": "decimal",
                },
                {"name": "bene_cnt_b0", "header_text": "BENE_CNT_B0", "data_type": "integer"},
                {"name": "bene_cnt_b1", "header_text": "BENE_CNT_B1", "data_type": "integer"},
                {"name": "bene_cnt_b2", "header_text": "BENE_CNT_B2", "data_type": "integer"},
            ],
        },
        {
            "sheet_index": 13,
            "sheet_type": "stop_loss_claims",
            "description": "Stop Loss Claims data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "clndr_yr", "header_text": "CLNDR_YR", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "aco_amt_agg", "header_text": "ACO_AMT_AGG", "data_type": "decimal"},
            ],
        },
        {
            "sheet_index": 14,
            "sheet_type": "cap",
            "description": "Capitation payment data",
            "columns": [
                {"name": "perf_yr", "header_text": "PERF_YR", "data_type": "string"},
                {"name": "aco_id", "header_text": "ACO_ID", "data_type": "string"},
                {"name": "bnmrk", "header_text": "BNMRK", "data_type": "string"},
                {"name": "pmt_mnth", "header_text": "PMT_MNTH", "data_type": "string"},
                {"name": "align_type", "header_text": "ALIGN_TYPE", "data_type": "string"},
                {
                    "name": "aco_tcc_amt_total",
                    "header_text": "ACO_TCC_AMT_TOTAL",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_bpcc_amt_total",
                    "header_text": "ACO_BPCC_AMT_TOTAL",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_epcc_amt_total_seq",
                    "header_text": "ACO_EPCC_AMT_TOTAL_SEQ",
                    "data_type": "decimal",
                },
                {
                    "name": "aco_apo_amt_total_seq",
                    "header_text": "ACO_APO_AMT_TOTAL_SEQ",
                    "data_type": "decimal",
                },
            ],
        },
    ],
)
@with_four_icli(
    category="Reports",
    file_type_code=212,
    file_pattern="REACH.D????.PRLBR.PY????.D??????.T*.xlsx",
    extract_zip=False,
    refresh_frequency="monthly",
)
@dataclass
class PreliminaryBenchmarkReportForDc:
    """
    Preliminary Benchmark Report for DC

        This dataclass represents a single row of data from the schema.
        All fields are validated at runtime using Pydantic.

        Validation includes:
        - Type checking (str, int, float, date, etc.)
        - Pattern matching for structured fields (MBI, NPI, ICD codes)
        - Range validation for numeric fields
        - Required vs optional field enforcement

        Metadata access:
        - PreliminaryBenchmarkReportForDc.schema_name() -> str
        - PreliminaryBenchmarkReportForDc.schema_metadata() -> dict
        - PreliminaryBenchmarkReportForDc.parser_config() -> dict
        - PreliminaryBenchmarkReportForDc.transform_config() -> dict
        - PreliminaryBenchmarkReportForDc.lineage_config() -> dict
    """

    sheet_type: str | None = Field(default=None, description="Sheet type identifier")

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "PreliminaryBenchmarkReportForDc":
        """Create instance from dictionary."""
        return cls(**data)
