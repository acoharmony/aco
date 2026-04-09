# © 2025 HarmonyCares
# All rights reserved.

"""Pure Polars implementation of int_revenue_center_deduped."""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """Deduplicate revenue center detail using native Polars."""
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    staged_data = pl.scan_parquet(silver_path / "cclf2.parquet")

    staged_data = staged_data.with_columns(
        [
            pl.lit(None).cast(pl.Utf8).alias("bene_hic_num"),
            pl.lit(None).cast(pl.Utf8).alias("bene_eqtbl_bic_hicn_num"),
        ]
    )

    beneficiary_xref = pl.scan_parquet(silver_path / "int_beneficiary_xref_deduped.parquet")

    partition_cols = [
        "cur_clm_uniq_id",
        "clm_line_num",
        "current_bene_mbi_id",
        "bene_hic_num",
        "clm_type_cd",
        "clm_line_from_dt",
        "clm_line_thru_dt",
        "clm_line_prod_rev_ctr_cd",
        "clm_line_instnl_rev_ctr_dt",
        "clm_line_hcpcs_cd",
        "bene_eqtbl_bic_hicn_num",
        "prvdr_oscar_num",
        "clm_from_dt",
        "clm_thru_dt",
        "clm_line_srvc_unit_qty",
        "clm_line_cvrd_pd_amt",
        "hcpcs_1_mdfr_cd",
        "hcpcs_2_mdfr_cd",
        "hcpcs_3_mdfr_cd",
        "hcpcs_4_mdfr_cd",
        "hcpcs_5_mdfr_cd",
        "clm_rev_apc_hipps_cd",
    ]

    result = (
        staged_data.join(
            beneficiary_xref.select(["prvs_num", "crnt_num"]),
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
                "cur_clm_uniq_id",
                "clm_line_num",
                "current_bene_mbi_id",
                "bene_hic_num",
                "clm_type_cd",
                "clm_line_from_dt",
                "clm_line_thru_dt",
                "clm_line_prod_rev_ctr_cd",
                "clm_line_instnl_rev_ctr_dt",
                "clm_line_hcpcs_cd",
                "bene_eqtbl_bic_hicn_num",
                "prvdr_oscar_num",
                "clm_from_dt",
                "clm_thru_dt",
                "clm_line_srvc_unit_qty",
                "clm_line_cvrd_pd_amt",
                "hcpcs_1_mdfr_cd",
                "hcpcs_2_mdfr_cd",
                "hcpcs_3_mdfr_cd",
                "hcpcs_4_mdfr_cd",
                "hcpcs_5_mdfr_cd",
                "clm_rev_apc_hipps_cd",
                "source_filename",
                "file_date",
            ]
        )
        .unique()
        # FINAL DEDUPLICATION: Keep only one row per natural key (claim_id + line)
        # Take the most recent file_date if duplicates remain
        .with_columns(
            pl.col("file_date")
            .rank(method="ordinal", descending=True)
            .over(["cur_clm_uniq_id", "clm_line_num"])
            .alias("final_row_num")
        )
        .filter(pl.col("final_row_num") == 1)
        .drop("final_row_num")
    )

    return result
