# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_physician_claim_deduped.

Aggregates adjusted claim amounts, filters to latest version,
and maps to Tuva normalized medical_claim schema with diagnosis codes.

References:
    CCLF Implementation Guide v40.0 Section 5.3.1 Steps 2-3
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method
from .._expressions import CclfClaimFilterExpression


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute Physician claim deduplication and aggregation logic.

        Algorithm:
        1. Load adjusted claims from int_physician_claim_adr
        2. Sum adjusted line amounts by claim control number
        3. Filter to latest version (row_num = 1) and exclude canceled
        4. Remove remaining duplicates
        5. Map to Tuva normalized medical_claim schema
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)

    claims = pl.scan_parquet(silver_path / "int_physician_claim_adr.parquet")

    # CRITICAL: Filter to latest version (row_num=1) and non-canceled BEFORE aggregation
    # to avoid including historical claim versions in the sum
    # Per CCLF Guide Section 5.3.1: Use latest, non-canceled claims for expenditures
    filtered_claims = claims.filter(CclfClaimFilterExpression.latest_non_canceled_filter())

    line_totals = filtered_claims.group_by(
        ["clm_cntl_num", "clm_line_num", "current_bene_mbi_id"]
    ).agg(
        [
            pl.col("clm_line_cvrd_pd_amt").sum().alias("sum_clm_line_cvrd_pd_amt"),
            pl.col("clm_line_alowd_chrg_amt").sum().alias("sum_clm_line_alowd_chrg_amt"),
        ]
    )

    filtered_with_totals = (
        filtered_claims.join(
            line_totals, on=["clm_cntl_num", "clm_line_num", "current_bene_mbi_id"], how="left"
        )
        # Filter out fully canceled claims (sum <= 0 after netting cancellations)
        # Per CCLF debit/credit methodology: negative sums indicate data errors or fully canceled claims
        # Also filter negative charge amounts which indicate data inconsistencies (e.g., cancellations exceeding originals)
        .filter(
            (pl.col("sum_clm_line_cvrd_pd_amt") > 0)
            & (pl.col("sum_clm_line_alowd_chrg_amt") > 0)
        )
    )

    claim_dupes = (
        filtered_with_totals.group_by(["cur_clm_uniq_id", "clm_line_num"])
        .agg(pl.len().alias("count"))
        .filter(pl.col("count") > 1)
        .select(["cur_clm_uniq_id", "clm_line_num"])
    )

    deduped = filtered_with_totals.join(
        claim_dupes, on=["cur_clm_uniq_id", "clm_line_num"], how="anti"
    )

    result = deduped.select(
        [
            pl.col("cur_clm_uniq_id").alias("claim_id"),
            pl.col("clm_line_num").cast(pl.Int64).alias("claim_line_number"),
            pl.lit("professional").alias("claim_type"),
            pl.col("current_bene_mbi_id").alias("person_id"),
            pl.col("current_bene_mbi_id").alias("member_id"),
            pl.lit("medicare").alias("payer"),
            pl.lit("medicare").alias("plan"),
            pl.when(pl.col("clm_from_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_from_dt"))
            .alias("claim_start_date"),
            pl.when(pl.col("clm_thru_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_thru_dt"))
            .alias("claim_end_date"),
            pl.when(pl.col("clm_line_from_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_line_from_dt"))
            .alias("claim_line_start_date"),
            pl.when(pl.col("clm_line_thru_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_line_thru_dt"))
            .alias("claim_line_end_date"),
            # Always-null in physician claims (no inpatient concept).
            # Typed as Date (not String) so the union with institutional
            # in gold/medical_claim aligns; the prior String typing forced
            # admission_date/discharge_date to String across all consumers.
            pl.lit(None).cast(pl.Date).alias("admission_date"),
            pl.lit(None).cast(pl.Date).alias("discharge_date"),
            pl.lit(None).cast(pl.String).alias("admit_source_code"),
            pl.lit(None).cast(pl.String).alias("admit_type_code"),
            pl.lit(None).cast(pl.String).alias("discharge_disposition_code"),
            pl.col("clm_pos_cd").alias("place_of_service_code"),
            pl.lit(None).cast(pl.String).alias("bill_type_code"),
            pl.lit(None).cast(pl.String).alias("ms_drg_code"),
            pl.lit(None).cast(pl.String).alias("apr_drg_code"),
            pl.lit(None).cast(pl.String).alias("revenue_center_code"),
            pl.col("clm_line_srvc_unit_qty").alias("service_unit_quantity"),
            pl.col("clm_line_hcpcs_cd").alias("hcpcs_code"),
            pl.col("hcpcs_1_mdfr_cd").alias("hcpcs_modifier_1"),
            pl.col("hcpcs_2_mdfr_cd").alias("hcpcs_modifier_2"),
            pl.col("hcpcs_3_mdfr_cd").alias("hcpcs_modifier_3"),
            pl.col("hcpcs_4_mdfr_cd").alias("hcpcs_modifier_4"),
            pl.col("hcpcs_5_mdfr_cd").alias("hcpcs_modifier_5"),
            pl.col("rndrg_prvdr_npi_num").alias("rendering_npi"),
            pl.col("clm_rndrg_prvdr_tax_num").alias("rendering_tin"),
            pl.lit(None).cast(pl.String).alias("billing_npi"),
            pl.lit(None).cast(pl.String).alias("billing_tin"),
            pl.lit(None).cast(pl.String).alias("facility_npi"),
            pl.when(pl.col("clm_efctv_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_efctv_dt"))
            .alias("paid_date"),
            pl.col("sum_clm_line_cvrd_pd_amt").cast(pl.Decimal(None, 2)).alias("paid_amount"),
            pl.col("sum_clm_line_alowd_chrg_amt").cast(pl.Decimal(None, 2)).alias("allowed_amount"),
            pl.col("sum_clm_line_alowd_chrg_amt").cast(pl.Decimal(None, 2)).alias("charge_amount"),
            pl.lit(None).cast(pl.String).alias("coinsurance_amount"),
            pl.lit(None).cast(pl.String).alias("copayment_amount"),
            pl.lit(None).cast(pl.String).alias("deductible_amount"),
            pl.lit(None).cast(pl.String).alias("total_cost_amount"),
            pl.col("dgns_prcdr_icd_ind").alias("diagnosis_code_type"),
            pl.col("clm_dgns_1_cd").alias("diagnosis_code_1"),
            pl.col("clm_dgns_2_cd").alias("diagnosis_code_2"),
            pl.col("clm_dgns_3_cd").alias("diagnosis_code_3"),
            pl.col("clm_dgns_4_cd").alias("diagnosis_code_4"),
            pl.col("clm_dgns_5_cd").alias("diagnosis_code_5"),
            pl.col("clm_dgns_6_cd").alias("diagnosis_code_6"),
            pl.col("clm_dgns_7_cd").alias("diagnosis_code_7"),
            pl.col("clm_dgns_8_cd").alias("diagnosis_code_8"),
            pl.col("clm_dgns_9_cd").alias("diagnosis_code_9"),
            pl.col("clm_dgns_10_cd").alias("diagnosis_code_10"),
            pl.col("clm_dgns_11_cd").alias("diagnosis_code_11"),
            pl.col("clm_dgns_12_cd").alias("diagnosis_code_12"),
            *[pl.lit(None).cast(pl.String).alias(f"diagnosis_code_{i}") for i in range(13, 26)],
            *[pl.lit(None).cast(pl.String).alias(f"diagnosis_poa_{i}") for i in range(1, 26)],
            pl.lit(None).cast(pl.String).alias("procedure_code_type"),
            *[pl.lit(None).cast(pl.String).alias(f"procedure_code_{i}") for i in range(1, 26)],
            *[pl.lit(None).cast(pl.String).alias(f"procedure_date_{i}") for i in range(1, 26)],
            pl.lit(1).alias("in_network_flag"),
            pl.lit("medicare cclf").alias("data_source"),
            pl.col("source_filename"),
            pl.col("file_date").alias("ingest_datetime"),
        ]
    )

    return result
