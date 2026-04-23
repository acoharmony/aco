# © 2025 HarmonyCares
# All rights reserved.

"""Pure Polars implementation of int_diagnosis_deduped."""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced


@composable
@traced()
@timeit(log_level="debug")
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """Deduplicate diagnosis codes using native Polars."""
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    stg = pl.scan_parquet(silver_path / "cclf4.parquet")
    from ._identity_timeline import current_mbi_lookup_lazy
    xref = current_mbi_lookup_lazy(silver_path)

    stg = stg.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("bene_hic_num"),
            pl.lit(None).cast(pl.Utf8).alias("bene_eqtbl_bic_hicn_num"),
        ]
    )

    partition_cols = [
        "cur_clm_uniq_id",
        "current_bene_mbi_id",
        "bene_hic_num",
        "clm_type_cd",
        "clm_prod_type_cd",
        "clm_val_sqnc_num",
        "clm_dgns_cd",
        "bene_eqtbl_bic_hicn_num",
        "prvdr_oscar_num",
        "clm_from_dt",
        "clm_thru_dt",
        "clm_poa_ind",
        "dgns_prcdr_icd_ind",
    ]

    result = (
        stg.filter(pl.col("bene_mbi_id").is_not_null())
        .join(
            xref.select(["prvs_num", "crnt_num"]),
            left_on="bene_mbi_id",
            right_on="prvs_num",
            how="left",
        )
        .with_columns(pl.coalesce(["crnt_num", "bene_mbi_id"]).alias("current_bene_mbi_id"))
        .with_columns(
            pl.col("file_date")
            .rank(method="ordinal", descending=True)
            .over(partition_cols)
            .alias("row_num")
        )
        .filter(pl.col("row_num") == 1)
        .select(
            [
                pl.col("cur_clm_uniq_id").cast(pl.Utf8),
                pl.col("current_bene_mbi_id").cast(pl.Utf8),
                pl.col("bene_hic_num").cast(pl.Utf8),
                pl.col("clm_type_cd").cast(pl.Utf8),
                pl.col("clm_prod_type_cd").cast(pl.Utf8),
                pl.col("clm_val_sqnc_num").cast(pl.Utf8),
                pl.col("clm_dgns_cd").cast(pl.Utf8),
                pl.col("bene_eqtbl_bic_hicn_num").cast(pl.Utf8),
                pl.col("prvdr_oscar_num").cast(pl.Utf8),
                pl.col("clm_from_dt").cast(pl.Utf8),
                pl.col("clm_thru_dt").cast(pl.Utf8),
                pl.col("clm_poa_ind").cast(pl.Utf8),
                pl.col("dgns_prcdr_icd_ind").cast(pl.Utf8),
                pl.col("source_filename").cast(pl.Utf8),
                pl.col("file_date").cast(pl.Utf8),
            ]
        )
    )

    return result
