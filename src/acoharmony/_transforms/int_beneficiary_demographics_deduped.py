# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_beneficiary_demographics_deduped.

This replaces the SQL window function approach with native Polars operations,
avoiding the need for DuckDB fallback.
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute beneficiary demographics deduplication using native Polars.

        This uses Polars' .rank().over() instead of SQL row_number() OVER(),
        which isn't supported in Polars SQL yet.

        Args:
            executor: TuvaSQLExecutor instance with access to staging models

        Returns:
            LazyFrame with deduplicated beneficiary demographics
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    stg_demographics = pl.scan_parquet(silver_path / "cclf8.parquet")

    stg_demographics = stg_demographics.with_columns(
        pl.lit(None).cast(pl.Utf8).alias("bene_hic_num")
    )

    partition_cols = [
        "bene_mbi_id",
        "bene_hic_num",
        "bene_fips_state_cd",
        "bene_fips_cnty_cd",
        "bene_zip_cd",
        "bene_dob",
        "bene_sex_cd",
        "bene_race_cd",
        "bene_mdcr_stus_cd",
        "bene_dual_stus_cd",
        "bene_death_dt",
        "bene_rng_bgn_dt",
        "bene_rng_end_dt",
        "bene_fst_name",
        "bene_mdl_name",
        "bene_lst_name",
        "bene_orgnl_entlmt_rsn_cd",
        "bene_entlmt_buyin_ind",
        "bene_part_a_enrlmt_bgn_dt",
        "bene_part_b_enrlmt_bgn_dt",
        "bene_line_1_adr",
        "bene_line_2_adr",
        "bene_line_3_adr",
        "bene_line_4_adr",
        "bene_line_5_adr",
        "bene_line_6_adr",
        "bene_city",
        "bene_state",
        "bene_zip",
        "bene_zip_ext",
    ]

    deduped = stg_demographics.with_columns(
        pl.col("file_date")
        .rank(method="ordinal", descending=True)
        .over(partition_cols)
        .alias("row_num")
    ).filter(pl.col("row_num") == 1)

    try:
        xref = pl.scan_parquet(silver_path / "int_beneficiary_xref_deduped.parquet")

        result = deduped.join(
            xref.select(["crnt_num", "prvs_num"]),
            left_on="bene_mbi_id",
            right_on="prvs_num",
            how="left",
        ).with_columns(pl.coalesce(["crnt_num", "bene_mbi_id"]).alias("current_bene_mbi_id"))
    except Exception:  # ALLOWED: Join fallback - use original MBI if xref dedup fails
        # If xref dedup fails, just use original MBI
        result = deduped.with_columns(pl.col("bene_mbi_id").alias("current_bene_mbi_id"))

    final = (
        result.with_columns(
            pl.col("file_date")
            .rank(method="ordinal", descending=True)
            .over("current_bene_mbi_id")
            .alias("row_num")
        )
        .filter(pl.col("row_num") == 1)
        .select(
            [
                pl.col("current_bene_mbi_id"),
                pl.col("bene_hic_num"),
                pl.col("bene_fips_state_cd"),
                pl.col("bene_fips_cnty_cd"),
                pl.col("bene_zip_cd"),
                pl.col("bene_dob"),
                pl.col("bene_sex_cd"),
                pl.col("bene_race_cd"),
                pl.col("bene_mdcr_stus_cd"),
                pl.col("bene_dual_stus_cd"),
                pl.col("bene_death_dt"),
                pl.col("bene_rng_bgn_dt"),
                pl.col("bene_rng_end_dt"),
                pl.col("bene_fst_name"),
                pl.col("bene_mdl_name"),
                pl.col("bene_lst_name"),
                pl.col("bene_orgnl_entlmt_rsn_cd"),
                pl.col("bene_entlmt_buyin_ind"),
                pl.col("bene_part_a_enrlmt_bgn_dt"),
                pl.col("bene_part_b_enrlmt_bgn_dt"),
                pl.col("bene_line_1_adr"),
                pl.col("bene_line_2_adr"),
                pl.col("bene_line_3_adr"),
                pl.col("bene_line_4_adr"),
                pl.col("bene_line_5_adr"),
                pl.col("bene_line_6_adr"),
                pl.col("bene_city").alias("geo_zip_plc_name"),
                pl.col("bene_state").alias("geo_usps_state_cd"),
                pl.col("bene_zip").alias("geo_zip5_cd"),
                pl.col("bene_zip_ext").alias("geo_zip4_cd"),
                pl.col("source_filename"),
                pl.col("file_date"),
            ]
        )
    )

    return final
