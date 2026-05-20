# © 2025 HarmonyCares
# All rights reserved.

"""
REACH BNMR Multi-Table Expression.

This expression transforms REACH BNMR from long format into multiple silver tables:
1. reach_bnmr_metadata.parquet - Pivoted metadata (1 row, ~250 columns)
2. reach_bnmr_claims.parquet - Claims with pivoted metadata
3. reach_bnmr_risk.parquet - Risk data with pivoted metadata
4. reach_bnmr_county.parquet - County data with pivoted metadata
5. reach_bnmr_uspcc.parquet - USPCC data with pivoted metadata
6. reach_bnmr_heba.parquet - HEBA data with pivoted metadata
7. reach_bnmr_cap.parquet - Capitation data with pivoted metadata
8. reach_bnmr_stop_loss_county.parquet - Stop loss county with pivoted metadata
9. reach_bnmr_stop_loss_claims.parquet - Stop loss claims with pivoted metadata
10. reach_bnmr_stop_loss_payout.parquet - Stop loss payout with pivoted metadata

Strategy:

1. Separate metadata sheets (0-7) from DATA sheets (8-16)
2. Pivot each metadata sheet to extract meaningful columns
3. Combine all pivoted metadata into single row (~250 columns total)
4. For each DATA sheet, cross join with pivoted metadata
5. Write each result as separate parquet file

Pivot Details:

- REPORT_PARAMETERS: Extract all parameter sections with proper column names
- FINANCIAL_SETTLEMENT: Flatten AD/ESRD/TOTAL structure
- BENCHMARK_HISTORICAL_AD/ESRD: Flatten year × metric grids
- RISKSCORE_AD/ESRD: Full decile distribution (all years)
- STOP_LOSS_CHARGE/PAYOUT: Extract configuration parameters

The pivoted metadata provides complete context without redundancy.
"""


from ._registry import register_expression


@register_expression(
    "reach_bnmr_multi_table",
    schemas=["silver"],
    dataset_types=["reports"],
    callable=False,
    description="Create separate REACH BNMR tables with pivoted metadata",
)
class ReachBnmrMultiTableExpression:
    """
    Transform REACH BNMR into multiple silver tables with pivoted metadata.

        This expression processes the long-format REACH BNMR output and creates:
        - One metadata table with all pivoted values (1 row)
        - Nine DATA tables, each with their data + pivoted metadata

        All tables can be appended to as new files arrive without reprocessing.
    """

    # Metadata sheet types (sheets 0-7)
    METADATA_SHEET_TYPES = {
        "report_parameters": 0,
        "financial_settlement": 1,
        "benchmark_historical_ad": 2,
        "benchmark_historical_esrd": 3,
        "riskscore_ad": 4,
        "riskscore_esrd": 5,
        "stop_loss_charge": 6,
        "stop_loss_payout": 7,
    }

    # DATA sheet types (sheets 8-16) mapped to output table names
    DATA_SHEET_MAPPING = {
        "claims": "reach_bnmr_claims",
        "risk": "reach_bnmr_risk",
        "county": "reach_bnmr_county",
        "uspcc": "reach_bnmr_uspcc",
        "heba": "reach_bnmr_heba",
        "cap": "reach_bnmr_cap",
        "stop_loss_county": "reach_bnmr_stop_loss_county",
        "stop_loss_claims": "reach_bnmr_stop_loss_claims",
        "stop_loss_payout": "reach_bnmr_stop_loss_payout",
    }

    # Natural key fields that identify a unique report
    # source_filename is the primary key - it uniquely identifies each BNMR file
    # Other fields provide context but source_filename is sufficient for joins
    NATURAL_KEY_FIELDS = [
        "source_filename",
        "performance_year",
        "aco_id",
    ]

