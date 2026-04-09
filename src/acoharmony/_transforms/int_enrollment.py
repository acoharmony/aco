# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_enrollment.

Converts enrollment data from member-month grain to enrollment date spans,
applying MBI crosswalk to get current MBI.
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced


@composable
@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Create enrollment spans from enrollment data.

        Logic:
        1. Get staging enrollment data
        2. Apply MBI crosswalk (beneficiary_xref)
        3. If member_months_enrollment=False: pass through
        4. If member_months_enrollment=True: convert member months to date spans
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    enrollment = pl.scan_parquet(silver_path / "enrollment.parquet")
    beneficiary_xref = pl.scan_parquet(silver_path / "int_beneficiary_xref_deduped.parquet")

    # Check if we need to convert member months to spans
    # For now, assume False (simple pass-through with crosswalk)
    # TODO: Implement member_months logic if needed

    result = (
        enrollment.join(
            beneficiary_xref.select(["prvs_num", "crnt_num"]),
            left_on="current_bene_mbi_id",
            right_on="prvs_num",
            how="left",
        )
        .with_columns(pl.coalesce(["crnt_num", "current_bene_mbi_id"]).alias("current_bene_mbi_id"))
        .select(["current_bene_mbi_id", "enrollment_start_date", "enrollment_end_date"])
    )

    return result
