# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care claims transform - filters medical claims for HDAI wound care procedures.

Uses HDAI wound care codes from A2671_D0259_WoundClaim_Ids_20251117.xlsx (137 distinct codes).

Includes comprehensive wound care HCPCS codes:
- Wound debridement (11042-11047, 97597-97598)
- Skin grafts and substitutes (15271-15278, Q4xxx)
- Vascular procedures (37xxx)
- Negative pressure wound therapy (97605-97608)
- Hyperbaric oxygen therapy (99183, G0277)
- Foot/lower extremity procedures (27xxx, 28xxx)
- Vascular imaging (93970-93986)
"""

import polars as pl

from .._decor8 import measure_dataframe_size, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.wound_care_claims")


@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Filter medical claims for wound care procedures.

    Uses the wound_care HCPCS filter expression to identify all wound care
    related claims from the medical_claim dataset.

    Args:
        executor: Transform executor with access to storage config

    Returns:
        LazyFrame with wound care claims only

    Notes:
        - Idempotent: can be run multiple times
        - Filters from gold/medical_claim.parquet
        - Uses HCPCSFilterExpression.wound_care()
    """
    from acoharmony._expressions._hcpcs_filter import HCPCSFilterExpression
    from acoharmony.medallion import MedallionLayer

    logger.info("Starting wound care claims transform")

    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load medical claims
    medical_claims = pl.scan_parquet(gold_path / "medical_claim.parquet")
    logger.debug(f"Loaded medical claims from {gold_path / 'medical_claim.parquet'}")

    # Filter for wound care HCPCS codes
    wound_care_claims = medical_claims.filter(HCPCSFilterExpression.wound_care())
    logger.info(f"Filtered for {len(HCPCSFilterExpression.wound_care_codes)} wound care HCPCS codes")

    logger.info("Wound care claims transform complete")
    return wound_care_claims
