# © 2025 HarmonyCares
# All rights reserved.

"""
REACH BNMR multi-table expression.

This expression splits the parsed REACH BNMR data by sheet_type
and creates separate silver tables for each sheet type with metadata.
"""

import polars as pl

from .._decor8 import explain, timeit, traced
from ._registry import register_expression


@register_expression(
    "reach_bnmr_multi_table",
    schemas=["silver"],
    description="Split REACH BNMR parsed data into multiple tables by sheet type",
)
class ReachBNMRMultiTableExpression:
    """
    Transform REACH BNMR parsed data into multiple silver tables.

        The parser creates a single LazyFrame with all sheets concatenated vertically,
        each row tagged with a sheet_type. This expression:
        1. Splits the data by sheet_type
        2. Creates metadata table from report_parameters sheet
        3. Creates data tables for each sheet type (claims, risk, etc.)
        4. Adds natural keys for joining metadata to data tables

        Returns a dict of LazyFrames where keys are table names:
        - reach_bnmr_metadata: Pivoted metadata (one row per source file)
        - reach_bnmr_claims: Claims-aligned beneficiary data
        - reach_bnmr_risk: Risk score data by category
        - reach_bnmr_county: County-level data
        - reach_bnmr_uspcc: US Per Capita Cost data
        - reach_bnmr_heba: Health Equity Benchmark Adjustment data
        - reach_bnmr_cap: Capitation payment data
        - reach_bnmr_stop_loss_county: Stop loss data by county
        - reach_bnmr_stop_loss_claims: Stop loss claims data
        - reach_bnmr_stop_loss_payout: Stop loss payout calculation
    """

    # Define sheet type to output table mapping
    SHEET_TO_TABLE = {
        "financial_settlement": "reach_bnmr_claims",
        "riskscore_ad": "reach_bnmr_risk",
        "riskscore_esrd": "reach_bnmr_risk",
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

    # Metadata fields to extract (ACO parameters from sheet 0)
    METADATA_FIELDS = [
        "performance_year",
        "aco_id",
        "aco_type",
        "risk_arrangement",
        "payment_mechanism",
        "discount",
        "shared_savings_rate",
        "advanced_payment_option",
        "stop_loss_elected",
        "stop_loss_type",
        "quality_withhold",
        "quality_score",
        "voluntary_aligned_benchmark",
        "blend_percentage",
        "blend_ceiling",
        "blend_floor",
        "ad_retrospective_trend",
        "esrd_retrospective_trend",
        "ad_completion_factor",
        "esrd_completion_factor",
        "stop_loss_payout_neutrality_factor",
    ]

    # Source tracking fields
    TRACKING_FIELDS = ["source_filename", "source_file", "processed_at"]

    @classmethod
    @traced()
    @explain(
        why="Build failed",
        how="Check configuration and input data are valid",
    )
    @timeit("reach_bnmr_multi_table")
    def build(cls, df: pl.LazyFrame) -> dict[str, pl.LazyFrame]:
        """Split REACH BNMR parsed data into multiple silver tables by sheet type."""
        results: dict[str, pl.LazyFrame] = {}

        # Extract metadata from report_parameters sheet
        metadata_lf = df.filter(pl.col("sheet_type") == "report_parameters")
        if metadata_lf is not None:
            results["reach_bnmr_metadata"] = metadata_lf

        # Split data by sheet type
        for sheet_type, table_name in cls.SHEET_TO_TABLE.items():
            sheet_lf = df.filter(pl.col("sheet_type") == sheet_type)
            if table_name in results:
                # Concatenate with existing data for this table
                results[table_name] = pl.concat([results[table_name], sheet_lf])
            else:
                results[table_name] = sheet_lf

        return results
