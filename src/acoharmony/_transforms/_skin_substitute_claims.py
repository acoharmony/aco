# © 2025 HarmonyCares
# All rights reserved.

"""
Skin substitute claims transform - filters medical claims for skin substitute procedures.

Includes Q4xxx HCPCS codes for cellular and tissue-based products for skin wounds.
This is a subset of wound care claims focusing specifically on skin substitutes.
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.skin_substitute_claims")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Filter medical claims for skin substitute procedures.

    Uses the skin_substitutes HCPCS filter expression to identify Q4xxx code
    claims from the medical_claim dataset.

    Args:
        executor: Transform executor with access to storage config

    Returns:
        LazyFrame with skin substitute claims only

    Notes:
        - Idempotent: can be run multiple times
        - Filters from gold/medical_claim.parquet
        - Uses HCPCSFilterExpression.skin_substitutes()
        - Subset of wound_care_claims (only Q4xxx codes)
    """
    from acoharmony._expressions._hcpcs_filter import HCPCSFilterExpression
    from acoharmony.medallion import MedallionLayer

    logger.info("Starting skin substitute claims transform")

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load medical claims
    medical_claims = pl.scan_parquet(gold_path / "medical_claim.parquet")
    logger.debug(f"Loaded medical claims from {gold_path / 'medical_claim.parquet'}")

    # Filter for skin substitute HCPCS codes
    skin_substitute_claims = medical_claims.filter(HCPCSFilterExpression.skin_substitutes())
    logger.info(
        f"Filtered for {len(HCPCSFilterExpression.skin_substitute_codes)} "
        "skin substitute HCPCS codes"
    )

    logger.info("Skin substitute claims transform complete")
    return skin_substitute_claims
