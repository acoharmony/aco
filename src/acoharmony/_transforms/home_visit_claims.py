# © 2025 HarmonyCares
# All rights reserved.

"""
Gold layer transform for home visit physician claims.

Filters physician claims to home visit HCPCS codes and returns
TIN/NPI combinations with claim details.
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method
from .._expressions._hcpcs_filter import HCPCSFilterExpression


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute home visit claims filtering logic.

    Algorithm:
        1. Load physician claims from int_physician_claim_deduped (silver)
        2. Filter to home visit HCPCS codes
        3. Select TIN/NPI combinations with claim details

    Returns:
        LazyFrame with home visit claims showing TIN/NPI combinations
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    # Load physician claims
    physician_claims = pl.scan_parquet(silver_path / "int_physician_claim_deduped.parquet")

    # Filter to home visit HCPCS codes
    home_visit_filter = HCPCSFilterExpression.filter_home_visit_hcpcs()

    result = physician_claims.filter(home_visit_filter).select(
        [
            pl.col("rendering_tin").alias("tin"),
            pl.col("rendering_npi").alias("npi"),
            pl.col("person_id"),
            pl.col("member_id"),
            pl.col("claim_id"),
            pl.col("claim_line_number"),
            pl.col("hcpcs_code"),
            pl.col("hcpcs_modifier_1"),
            pl.col("hcpcs_modifier_2"),
            pl.col("hcpcs_modifier_3"),
            pl.col("hcpcs_modifier_4"),
            pl.col("hcpcs_modifier_5"),
            pl.col("claim_start_date"),
            pl.col("claim_end_date"),
            pl.col("claim_line_start_date"),
            pl.col("claim_line_end_date"),
            pl.col("place_of_service_code"),
            pl.col("service_unit_quantity"),
            pl.col("paid_amount"),
            pl.col("allowed_amount"),
            pl.col("charge_amount"),
            pl.col("diagnosis_code_type"),
            pl.col("diagnosis_code_1"),
            pl.col("diagnosis_code_2"),
            pl.col("diagnosis_code_3"),
            pl.col("diagnosis_code_4"),
            pl.col("diagnosis_code_5"),
            pl.col("data_source"),
            pl.col("source_filename"),
            pl.col("ingest_datetime"),
        ]
    )

    return result
