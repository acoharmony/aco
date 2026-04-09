# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care high cost patients transform.

Identifies individual patients with more than $1M in wound care product costs.
These cases represent exceptional clinical complexity and financial impact.

Output includes patient-level metrics with treatment details.
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.wound_care_high_cost")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor, min_cost: float = 1_000_000.0) -> pl.LazyFrame:
    """
    Identify patients with exceptionally high wound care costs.

    Args:
        executor: Transform executor with access to storage config
        min_cost: Minimum total cost to flag (default $1,000,000)

    Returns:
        LazyFrame with high-cost patient details and metrics

    Notes:
        - Idempotent: can be run multiple times
        - Reads from gold/skin_substitute_claims.parquet
        - Uses WoundCarePatternExpression.high_cost_filter()
    """
    from acoharmony._expressions._wound_care_patterns import WoundCarePatternExpression
    from acoharmony.medallion import MedallionLayer

    logger.info(f"Starting high cost patients analysis (min_cost=${min_cost:,.0f})")

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load skin substitute claims
    claims = pl.scan_parquet(gold_path / "skin_substitute_claims.parquet")
    logger.debug(
        f"Loaded skin substitute claims from {gold_path / 'skin_substitute_claims.parquet'}"
    )

    # Calculate total cost per patient
    high_cost_patients = (
        claims.group_by("member_id")
        .agg(
            [
                pl.col("paid_amount").sum().alias("total_cost"),
                pl.len().alias("claim_count"),
                pl.col("rendering_npi").n_unique().alias("unique_providers"),
                pl.col("hcpcs_code").n_unique().alias("unique_products"),
                pl.col("claim_end_date").min().alias("first_claim"),
                pl.col("claim_end_date").max().alias("last_claim"),
            ]
        )
        .with_columns(
            [
                WoundCarePatternExpression.calculate_time_span(
                    "first_claim", "last_claim"
                ).alias("treatment_span_days")
            ]
        )
        .filter(WoundCarePatternExpression.high_cost_filter(min_cost))
        .sort("total_cost", descending=True)
    )

    logger.info("High cost patients analysis complete")
    return high_cost_patients
