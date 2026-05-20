# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care high frequency providers transform.

Identifies providers (NPIs) with 15+ skin substitute applications per patient within a year.
This pattern may indicate complex wound cases or potential over-utilization.

Output includes:
- Patient-level details with application counts
- NPI-level summaries with aggregate metrics
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.wound_care_high_frequency")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor, min_applications: int = 15) -> dict[str, pl.LazyFrame]:
    """
    Identify providers with high frequency application patterns.

    Args:
        executor: Transform executor with access to storage config
        min_applications: Minimum applications per patient to flag (default 15)

    Returns:
        Dictionary with two LazyFrames:
            - 'patient_level': NPI-patient combinations with application counts
            - 'npi_summary': Aggregated metrics by NPI

    Notes:
        - Idempotent: can be run multiple times
        - Reads from gold/skin_substitute_claims.parquet
        - Uses WoundCarePatternExpression.high_frequency_filter()
    """
    from acoharmony._expressions._wound_care_patterns import WoundCarePatternExpression
    from acoharmony.medallion import MedallionLayer

    logger.info(
        f"Starting high frequency providers analysis (min_applications={min_applications})"
    )

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load skin substitute claims
    claims = pl.scan_parquet(gold_path / "skin_substitute_claims.parquet")
    logger.debug(
        f"Loaded skin substitute claims from {gold_path / 'skin_substitute_claims.parquet'}"
    )

    # Group by NPI and patient, count applications
    patient_level = (
        claims.group_by(["rendering_npi", "member_id"])
        .agg(
            [
                pl.len().alias("application_count"),
                pl.col("claim_end_date").min().alias("first_application"),
                pl.col("claim_end_date").max().alias("last_application"),
                pl.col("hcpcs_code").n_unique().alias("unique_products"),
            ]
        )
        .with_columns(
            [
                WoundCarePatternExpression.calculate_time_span(
                    "first_application", "last_application"
                ).alias("span_days")
            ]
        )
        .filter(WoundCarePatternExpression.high_frequency_filter(min_applications))
        .sort("application_count", descending=True)
    )

    # Aggregate by NPI
    npi_summary = (
        patient_level.group_by("rendering_npi")
        .agg(
            [
                pl.len().alias("patients_with_high_frequency"),
                pl.col("application_count").sum().alias("total_applications"),
                pl.col("application_count").max().alias("max_apps_single_patient"),
                pl.col("application_count").mean().alias("avg_apps_per_patient"),
            ]
        )
        .sort("patients_with_high_frequency", descending=True)
    )

    logger.info("High frequency providers analysis complete")
    return {"patient_level": patient_level, "npi_summary": npi_summary}
