# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care identical billing patterns transform.

Identifies NPIs with identical code/unit count/payment amounts across multiple applications.
This may indicate standardized care protocols or mechanical billing practices.

Unit of analysis: By NPI (identifies providers with consistent billing patterns)

Output includes:
- Pattern details by NPI-HCPCS-Amount combinations
- NPI-level summaries with aggregate metrics
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.wound_care_identical_patterns")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(
    executor, min_identical_claims: int = 10, min_patients: int = 3
) -> dict[str, pl.LazyFrame]:
    """
    Identify identical billing patterns across patients and dates.

    Args:
        executor: Transform executor with access to storage config
        min_identical_claims: Minimum identical claims to flag (default 10)
        min_patients: Minimum unique patients to flag (default 3)

    Returns:
        Dictionary with two LazyFrames:
            - 'pattern_details': Billing patterns by NPI-HCPCS-Amount
            - 'npi_summary': Aggregated metrics by NPI

    Notes:
        - Idempotent: can be run multiple times
        - Reads from gold/skin_substitute_claims.parquet
        - Uses WoundCarePatternExpression.identical_pattern_filter()
        - Unit of analysis: By NPI
    """
    from acoharmony._expressions._wound_care_patterns import WoundCarePatternExpression
    from acoharmony.medallion import MedallionLayer

    logger.info(
        f"Starting identical billing patterns analysis "
        f"(min_claims={min_identical_claims}, min_patients={min_patients})"
    )

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load skin substitute claims
    claims = pl.scan_parquet(gold_path / "skin_substitute_claims.parquet")
    logger.debug(
        f"Loaded skin substitute claims from {gold_path / 'skin_substitute_claims.parquet'}"
    )

    # Find identical billing patterns (same NPI + HCPCS + Amount)
    pattern_details = (
        claims.group_by(["rendering_npi", "hcpcs_code", "paid_amount"])
        .agg(
            [
                pl.len().alias("claim_count"),
                pl.col("member_id").n_unique().alias("unique_patients"),
                pl.col("claim_end_date").n_unique().alias("unique_dates"),
                pl.col("claim_end_date").min().alias("first_claim_date"),
                pl.col("claim_end_date").max().alias("last_claim_date"),
            ]
        )
        .with_columns(
            [
                WoundCarePatternExpression.calculate_time_span(
                    "first_claim_date", "last_claim_date"
                ).alias("pattern_span_days")
            ]
        )
        .filter(
            WoundCarePatternExpression.identical_pattern_filter(
                min_identical_claims, min_patients
            )
        )
        .sort(["rendering_npi", "claim_count"], descending=[False, True])
    )

    # Summarize by NPI
    npi_summary = (
        pattern_details.group_by("rendering_npi")
        .agg(
            [
                pl.len().alias("unique_billing_patterns"),
                pl.col("claim_count").sum().alias("total_identical_claims"),
                pl.col("unique_patients").sum().alias("total_patients_affected"),
                pl.col("claim_count").max().alias("max_identical_claims"),
                pl.col("paid_amount").n_unique().alias("unique_payment_amounts"),
            ]
        )
        .sort("total_identical_claims", descending=True)
    )

    logger.info("Identical billing patterns analysis complete")
    return {"pattern_details": pattern_details, "npi_summary": npi_summary}
