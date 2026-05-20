# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of medical_claim final model.

Unions DME, Physician, and Institutional deduped claims into single medical_claim table.
"""

import polars as pl

from .._decor8 import measure_dataframe_size


@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute medical_claim union logic.

        Unions the three claim types:
        - int_dme_claim_deduped
        - int_physician_claim_deduped
        - int_institutional_claim_deduped (optional - may not have injector yet)

        All three have identical schemas (normalized medical_claim schema).
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    dme = pl.scan_parquet(silver_path / "int_dme_claim_deduped.parquet")
    physician = pl.scan_parquet(silver_path / "int_physician_claim_deduped.parquet")

    institutional = pl.scan_parquet(silver_path / "int_institutional_claim_deduped.parquet")
    frames = [dme, physician, institutional]

    result = pl.concat(frames, how="vertical")

    return result
