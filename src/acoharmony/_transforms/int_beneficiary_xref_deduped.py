# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_beneficiary_xref_deduped.

This replaces the SQL window function approach with native Polars operations.
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute beneficiary xref deduplication using native Polars.

        Deduplicates the MBI crosswalk and resolves chained MBI mappings.

        Args:
            executor: TuvaSQLExecutor instance with access to staging models

        Returns:
            LazyFrame with deduplicated MBI crosswalk
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    stg_xref = pl.scan_parquet(silver_path / "cclf9.parquet")

    add_row_num = stg_xref.with_columns(
        pl.col("file_date")
        .rank(method="ordinal", descending=True)
        .over("prvs_num")
        .alias("row_num")
    )

    latest = add_row_num.filter(pl.col("row_num") == 1)

    check_crnt_num = latest.join(
        latest.select(["file_date", "prvs_num", "crnt_num"]),
        left_on="crnt_num",
        right_on="prvs_num",
        how="left",
        suffix="_b",
    ).with_columns(
        pl.when(
            (pl.col("crnt_num_b").is_not_null()) & (pl.col("file_date_b") > pl.col("file_date"))
        )
        .then(pl.col("crnt_num_b"))
        .otherwise(pl.col("crnt_num"))
        .alias("final_mbi")
    )

    result = check_crnt_num.select([
        pl.col("prvs_num"),
        pl.col("final_mbi").alias("crnt_num"),
        pl.col("prvs_id_efctv_dt"),
        pl.col("prvs_id_obslt_dt"),
        pl.col("source_filename"),
        pl.col("file_date"),
    ])

    return result
