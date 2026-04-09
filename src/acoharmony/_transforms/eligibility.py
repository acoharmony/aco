# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of eligibility final model.

Joins demographics with enrollment and maps to Tuva eligibility schema.
"""

from datetime import date

import polars as pl

from .._decor8 import measure_dataframe_size


@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute eligibility mapping logic.

        Joins demographics with enrollment periods and maps to Tuva schema.

        NOTE: Final models read from pre-processed int_* parquet files, not re-execute logic.
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    demographics = pl.scan_parquet(silver_path / "int_beneficiary_demographics_deduped.parquet")
    enrollment = pl.scan_parquet(silver_path / "int_enrollment.parquet")
    demographics_prep = demographics.select(
        [
            pl.col("current_bene_mbi_id"),
            pl.col("bene_sex_cd"),
            pl.col("bene_race_cd"),
            pl.col("bene_dob"),
            pl.col("bene_death_dt"),
            pl.col("bene_orgnl_entlmt_rsn_cd"),
            pl.col("bene_dual_stus_cd"),
            pl.col("bene_mdcr_stus_cd"),
            pl.col("bene_fst_name"),
            pl.col("bene_lst_name"),
            pl.col("bene_line_1_adr"),
            pl.when(pl.col("bene_line_2_adr").is_null())
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit(", "), pl.col("bene_line_2_adr")]))
            .alias("bene_line_2_adr"),
            pl.when(pl.col("bene_line_3_adr").is_null())
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit(", "), pl.col("bene_line_3_adr")]))
            .alias("bene_line_3_adr"),
            pl.when(pl.col("bene_line_4_adr").is_null())
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit(", "), pl.col("bene_line_4_adr")]))
            .alias("bene_line_4_adr"),
            pl.when(pl.col("bene_line_5_adr").is_null())
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit(", "), pl.col("bene_line_5_adr")]))
            .alias("bene_line_5_adr"),
            pl.when(pl.col("bene_line_6_adr").is_null())
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit(", "), pl.col("bene_line_6_adr")]))
            .alias("bene_line_6_adr"),
            pl.col("geo_zip_plc_name"),
            pl.col("geo_usps_state_cd"),
            pl.col("geo_zip5_cd"),
            pl.when(pl.col("geo_zip4_cd").is_null())
            .then(pl.lit(""))
            .otherwise(pl.concat_str([pl.lit("-"), pl.col("geo_zip4_cd")]))
            .alias("geo_zip4_cd"),
        ]
    )

    joined = demographics_prep.join(
        enrollment.select(["current_bene_mbi_id", "enrollment_start_date", "enrollment_end_date"]),
        on="current_bene_mbi_id",
        how="left",
    )

    result = joined.select(
        [
            pl.col("current_bene_mbi_id").alias("person_id"),
            pl.col("current_bene_mbi_id").alias("member_id"),
            pl.lit(None).cast(pl.String).alias("subscriber_id"),
            pl.when(pl.col("bene_sex_cd") == "0")
            .then(pl.lit("unknown"))
            .when(pl.col("bene_sex_cd") == "1")
            .then(pl.lit("male"))
            .when(pl.col("bene_sex_cd") == "2")
            .then(pl.lit("female"))
            .otherwise(pl.lit("unknown"))
            .alias("gender"),
            pl.when(pl.col("bene_race_cd") == "0")
            .then(pl.lit("unknown"))
            .when(pl.col("bene_race_cd") == "1")
            .then(pl.lit("white"))
            .when(pl.col("bene_race_cd") == "2")
            .then(pl.lit("black"))
            .when(pl.col("bene_race_cd") == "3")
            .then(pl.lit("other"))
            .when(pl.col("bene_race_cd") == "4")
            .then(pl.lit("asian"))
            .when(pl.col("bene_race_cd") == "5")
            .then(pl.lit("hispanic"))
            .when(pl.col("bene_race_cd") == "6")
            .then(pl.lit("north american native"))
            .otherwise(pl.lit("unknown"))
            .alias("race"),
            pl.col("bene_dob").alias("birth_date"),
            pl.col("bene_death_dt").alias("death_date"),
            pl.when(pl.col("bene_death_dt").is_null())
            .then(pl.lit(0))
            .otherwise(pl.lit(1))
            .alias("death_flag"),
            pl.col("enrollment_start_date"),
            pl.when(
                pl.col("enrollment_end_date").is_null()
                | (pl.col("enrollment_end_date") >= pl.lit(date.today()))
            )
            .then(
                pl.lit(date.today()).dt.month_end()
            )
            .otherwise(pl.col("enrollment_end_date"))
            .alias("enrollment_end_date"),
            pl.lit("medicare").alias("payer"),
            pl.lit("medicare").alias("payer_type"),
            pl.lit("medicare").alias("plan"),
            pl.col("bene_orgnl_entlmt_rsn_cd").alias("original_reason_entitlement_code"),
            pl.col("bene_dual_stus_cd").alias("dual_status_code"),
            pl.col("bene_mdcr_stus_cd").alias("medicare_status_code"),
            pl.col("bene_fst_name").alias("first_name"),
            pl.col("bene_lst_name").alias("last_name"),
            pl.lit(None).cast(pl.String).alias("social_security_number"),
            pl.lit("self").alias("subscriber_relation"),
            pl.concat_str(
                [
                    pl.col("bene_line_1_adr"),
                    pl.col("bene_line_2_adr"),
                    pl.col("bene_line_3_adr"),
                    pl.col("bene_line_4_adr"),
                    pl.col("bene_line_5_adr"),
                    pl.col("bene_line_6_adr"),
                ]
            ).alias("address"),
            pl.col("geo_zip_plc_name").alias("city"),
            pl.col("geo_usps_state_cd").alias("state"),
            pl.concat_str([pl.col("geo_zip5_cd"), pl.col("geo_zip4_cd")]).alias("zip_code"),
            pl.lit(None).cast(pl.String).alias("phone"),
            pl.lit("medicare cclf").alias("data_source"),
        ]
    )

    return result
