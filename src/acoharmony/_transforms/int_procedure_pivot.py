# © 2025 HarmonyCares
# All rights reserved.

"""Pure Polars implementation of int_procedure_pivot."""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced


@composable
@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Pivot procedure codes using native Polars.

        Replaces SQL pivot with native Polars operations to avoid CTE alias issues.

        NOTE: Reads from pre-processed int_procedure_deduped.parquet, not re-execute logic.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    deduped = pl.scan_parquet(silver_path / "int_procedure_deduped.parquet")

    procedure_pivot = deduped.group_by(
        ["cur_clm_uniq_id", "current_bene_mbi_id", "dgns_prcdr_icd_ind"]
    ).agg(
        [
            pl.when(pl.col("clm_val_sqnc_num") == f"{i:02d}")
            .then(pl.col("clm_prcdr_cd"))
            .otherwise(None)
            .max()
            .alias(f"procedure_code_{i}")
            for i in range(1, 26)
        ]
    )

    date_pivot = deduped.group_by(
        ["cur_clm_uniq_id", "current_bene_mbi_id", "dgns_prcdr_icd_ind"]
    ).agg(
        [
            pl.when(pl.col("clm_val_sqnc_num") == f"{i:02d}")
            .then(pl.col("clm_prcdr_prfrm_dt"))
            .otherwise(None)
            .max()
            .alias(f"procedure_date_{i}")
            for i in range(1, 26)
        ]
    )

    result = procedure_pivot.join(
        date_pivot,
        on=["cur_clm_uniq_id", "current_bene_mbi_id", "dgns_prcdr_icd_ind"],
        how="inner",
    )

    return result
