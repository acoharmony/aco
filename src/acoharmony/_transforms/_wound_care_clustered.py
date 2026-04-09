# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care clustered claims transform.

Identifies patterns with multiple (3+) claim dates of service within 1 week periods.
This may indicate legitimate intensive treatment phases or billing timing patterns.

Output includes:
- Cluster-level details with date ranges and totals
- NPI-level summaries with aggregate metrics
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.wound_care_clustered")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor, min_claims_in_week: int = 3) -> dict[str, pl.LazyFrame]:
    """
    Identify temporal clustering patterns in wound care claims.

    Args:
        executor: Transform executor with access to storage config
        min_claims_in_week: Minimum claims within 7-day window (default 3)

    Returns:
        Dictionary with two LazyFrames:
            - 'cluster_details': Individual clusters with metrics
            - 'npi_summary': Aggregated metrics by NPI

    Notes:
        - Idempotent: can be run multiple times
        - Reads from gold/skin_substitute_claims.parquet
        - Uses self-join to detect 7-day windows
        - Uses WoundCarePatternExpression.cluster_filter()
    """
    from acoharmony._expressions._wound_care_patterns import WoundCarePatternExpression
    from acoharmony.medallion import MedallionLayer

    logger.info(
        f"Starting clustered claims analysis (min_claims_in_week={min_claims_in_week})"
    )

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load skin substitute claims
    claims = pl.scan_parquet(gold_path / "skin_substitute_claims.parquet")
    logger.debug(
        f"Loaded skin substitute claims from {gold_path / 'skin_substitute_claims.parquet'}"
    )

    # Select relevant columns and sort
    patient_dates = (
        claims.select(
            ["rendering_npi", "member_id", "claim_end_date", "paid_amount"]
        ).sort(["rendering_npi", "member_id", "claim_end_date"])
    )

    # Self-join to find dates within 7 days of each other
    cluster_details = (
        patient_dates.join(
            patient_dates, on=["rendering_npi", "member_id"], suffix="_next"
        )
        .filter(
            (pl.col("claim_end_date_next") >= pl.col("claim_end_date"))
            & (
                pl.col("claim_end_date_next")
                <= WoundCarePatternExpression.week_window_end("claim_end_date")
            )
        )
        .group_by(["rendering_npi", "member_id", "claim_end_date"])
        .agg(
            [
                pl.col("claim_end_date_next").n_unique().alias("claims_in_week"),
                pl.col("claim_end_date_next").min().alias("week_start"),
                pl.col("claim_end_date_next").max().alias("week_end"),
                pl.col("paid_amount_next").sum().alias("week_total_paid"),
            ]
        )
        .filter(WoundCarePatternExpression.cluster_filter(min_claims_in_week))
        .sort("claims_in_week", descending=True)
    )

    # Summarize by NPI
    npi_summary = (
        cluster_details.group_by("rendering_npi")
        .agg(
            [
                pl.col("member_id").n_unique().alias("patients_with_clusters"),
                pl.col("claims_in_week").sum().alias("total_clustered_claims"),
                pl.col("claims_in_week").max().alias("max_claims_in_week"),
                pl.col("week_total_paid").sum().alias("total_paid_in_clusters"),
            ]
        )
        .sort("patients_with_clusters", descending=True)
    )

    logger.info("Clustered claims analysis complete")
    return {"cluster_details": cluster_details, "npi_summary": npi_summary}
