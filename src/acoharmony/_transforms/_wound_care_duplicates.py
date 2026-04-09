# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care duplicate claims transform.

Identifies multiple claims with the same date of service and same product.
This may represent legitimate multiple applications or potential billing errors.

Output includes:
- Duplicate instance details by NPI-patient-date-HCPCS
- NPI-level summaries with aggregate metrics
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.wound_care_duplicates")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> dict[str, pl.LazyFrame]:
    """
    Identify same-day duplicate claim patterns.

    Args:
        executor: Transform executor with access to storage config

    Returns:
        Dictionary with two LazyFrames:
            - 'duplicate_details': Duplicate instances with counts and totals
            - 'npi_summary': Aggregated metrics by NPI

    Notes:
        - Idempotent: can be run multiple times
        - Reads from gold/skin_substitute_claims.parquet
        - Uses WoundCarePatternExpression.duplicate_filter()
    """
    from acoharmony._expressions._wound_care_patterns import WoundCarePatternExpression
    from acoharmony.medallion import MedallionLayer

    logger.info("Starting duplicate claims analysis")

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load skin substitute claims
    claims = pl.scan_parquet(gold_path / "skin_substitute_claims.parquet")
    logger.debug(
        f"Loaded skin substitute claims from {gold_path / 'skin_substitute_claims.parquet'}"
    )

    # Find same-day duplicates by grouping on natural key
    duplicate_details = (
        claims.group_by(
            ["rendering_npi", "member_id", "claim_end_date", "hcpcs_code"]
        )
        .agg(
            [
                pl.len().alias("claim_count"),
                pl.col("paid_amount").sum().alias("total_paid"),
                pl.col("paid_amount").mean().alias("avg_paid_per_claim"),
            ]
        )
        .filter(WoundCarePatternExpression.duplicate_filter())
        .sort("claim_count", descending=True)
    )

    # Summarize by NPI
    npi_summary = (
        duplicate_details.group_by("rendering_npi")
        .agg(
            [
                pl.len().alias("duplicate_instances"),
                pl.col("claim_count").sum().alias("total_duplicate_claims"),
                pl.col("total_paid").sum().alias("total_paid_duplicates"),
                pl.col("member_id").n_unique().alias("unique_patients_affected"),
            ]
        )
        .sort("duplicate_instances", descending=True)
    )

    logger.info("Duplicate claims analysis complete")
    return {"duplicate_details": duplicate_details, "npi_summary": npi_summary}
