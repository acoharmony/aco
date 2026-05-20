# © 2025 HarmonyCares
# All rights reserved.

"""
Member Medical Claims with Claim ID Match Transform.

Enriches member_medical_claims_results with claim_id_match flag that indicates
whether claim IDs are present in HDAI, CCLF, or both sources.

This transform is designed to support claim validation workflows where claims
are submitted via external sources (e.g., Excel) and need to be reconciled
against internal systems (HDAI) and CMS claims feed (CCLF).
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced, transform


@composable
@transform(name="member_medical_claims_with_match", tier=["gold"], sql_enabled=False)
@traced()
@timeit(log_level="debug")
@measure_dataframe_size
def execute(executor) -> pl.LazyFrame:
    """
    Enrich member medical claims with claim ID match flags.

    Takes member_medical_claims_results and adds claim_id_match column
    that categorizes each claim as:
    - "yes": Claim ID exists in both HDAI and CCLF
    - "missing_hdai": Claim ID only in CCLF (not in HDAI)
    - "missing_cclf": Claim ID only in HDAI (not in CCLF)

    Args:
        executor: Transform executor with access to storage config

    Returns:
        LazyFrame with added claim_id_match column (Categorical)

    Notes:
        - Assumes input has hdai_claim_id and cclf_claim_id columns
        - If columns don't exist, creates them as null before applying expression
        - Idempotent: can be run multiple times on same data
    """
    from acoharmony._expressions import ClaimIdMatchExpression
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load member medical claims results
    # This could come from medical_claim.parquet or a specific subset
    claims = pl.scan_parquet(gold_path / "member_medical_claims_results.parquet")

    # Ensure the required columns exist (they may be null if not joined yet)
    schema = claims.collect_schema()

    # Add claim ID columns if they don't exist
    if "hdai_claim_id" not in schema:
        claims = claims.with_columns(pl.lit(None).cast(pl.Utf8).alias("hdai_claim_id"))

    if "cclf_claim_id" not in schema:
        claims = claims.with_columns(pl.lit(None).cast(pl.Utf8).alias("cclf_claim_id"))

    # Apply claim ID match expression
    result = claims.with_columns(
        ClaimIdMatchExpression.claim_id_match_flag(
            hdai_claim_id_col="hdai_claim_id", cclf_claim_id_col="cclf_claim_id"
        ).alias("claim_id_match")
    )

    return result
