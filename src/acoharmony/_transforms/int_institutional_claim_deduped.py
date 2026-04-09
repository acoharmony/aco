# © 2025 HarmonyCares
# All rights reserved.

"""
Pure Polars implementation of int_institutional_claim_deduped.

Complex model that:
1. Aggregates header-level amounts
2. Joins with revenue center line details
3. Joins with diagnosis and procedure pivots
4. Maps to Tuva normalized medical_claim schema

References:
    CCLF Implementation Guide v40.0:
    - Section 3.5: Part A Header Expenditures vs Revenue Center Expenditures
    - Section 5.3.1 Step 1: Part A claim expenditure calculation
"""

import polars as pl

from .._decor8 import measure_dataframe_size, transform_method
from .._expressions import CclfClaimFilterExpression, CclfRevenueCenterValidationExpression


@transform_method(enable_composition=False, threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Execute institutional claim deduplication with line details.

        Algorithm:
        1. Load adjusted claims from int_institutional_claim_adr
        2. Sum header amounts by natural keys
        3. Filter to latest version (row_num = 1) and exclude canceled
        4. Join with revenue center line details
        5. Join with diagnosis and procedure pivots
        6. Map to Tuva medical_claim schema
    """
    from acoharmony.medallion import MedallionLayer

    storage = executor.storage_config
    silver_path = storage.get_path(MedallionLayer.SILVER)
    claims = pl.scan_parquet(silver_path / "int_institutional_claim_adr.parquet")
    diagnosis_pivot = pl.scan_parquet(silver_path / "int_diagnosis_pivot.parquet")
    procedure_pivot = pl.scan_parquet(silver_path / "int_procedure_pivot.parquet")
    revenue_center = pl.scan_parquet(silver_path / "int_revenue_center_deduped.parquet")

    # CRITICAL FIX: Use window functions instead of group_by + join
    # This eliminates CACHE and enables true streaming
    # group_by + join breaks streaming because Polars caches the left table
    # Window functions compute aggregations in a single streaming pass
    # Per CCLF Guide Section 5.3.1: Filter to latest, non-canceled claims
    filtered_claims = (
        claims.filter(CclfClaimFilterExpression.latest_non_canceled_filter())
        .with_columns(
            [
                # Compute header totals as window functions over partition keys
                pl.col("clm_pmt_amt")
                .sum()
                .over([
                    "clm_blg_prvdr_oscar_num",
                    "clm_from_dt",
                    "clm_thru_dt",
                    "current_bene_mbi_id",
                ])
                .alias("sum_clm_pmt_amt"),
                pl.col("clm_mdcr_instnl_tot_chrg_amt")
                .sum()
                .over([
                    "clm_blg_prvdr_oscar_num",
                    "clm_from_dt",
                    "clm_thru_dt",
                    "current_bene_mbi_id",
                ])
                .alias("sum_clm_mdcr_instnl_tot_chrg_amt"),
            ]
        )
        # Filter out fully canceled claims (sum <= 0 after netting cancellations)
        # Per CCLF debit/credit methodology: negative sums indicate data errors or fully canceled claims
        # Also filter negative charge amounts which indicate data inconsistencies (e.g., cancellations exceeding originals)
        .filter(
            (pl.col("sum_clm_pmt_amt") > 0)
            & (pl.col("sum_clm_mdcr_instnl_tot_chrg_amt") > 0)
        )
    )

    # Per CCLF Guide Section 3.5: Revenue center payments should only be used if they sum
    # to header payment. Otherwise, ignore line-level payments and use header only.
    # Calculate sum of revenue center payments per claim to validate
    revenue_center_totals = revenue_center.group_by("cur_clm_uniq_id").agg([
        pl.col("clm_line_cvrd_pd_amt").sum().alias("revenue_center_pmt_sum")
    ])

    with_lines = (
        filtered_claims.join(revenue_center, on="cur_clm_uniq_id", how="left")
        .join(revenue_center_totals, on="cur_clm_uniq_id", how="left")
        .with_columns([
            # CCLF Guide Section 3.5: Validate revenue center payments match header
            CclfRevenueCenterValidationExpression.revenue_center_matches_header(
                tolerance=0.01
            ).alias("use_line_payments")
        ])
    )

    diagnosis_renamed = diagnosis_pivot.rename({"dgns_prcdr_icd_ind": "diagnosis_icd_ind"})
    with_diagnosis = with_lines.join(
        diagnosis_renamed, on=["cur_clm_uniq_id", "current_bene_mbi_id"], how="left"
    )
    procedure_renamed = procedure_pivot.rename({"dgns_prcdr_icd_ind": "procedure_icd_ind"})
    with_procedures = with_diagnosis.join(
        procedure_renamed, on=["cur_clm_uniq_id", "current_bene_mbi_id"], how="left"
    )

    result = with_procedures.select(
        [
            pl.col("cur_clm_uniq_id").alias("claim_id"),
            pl.col("clm_line_num").cast(pl.Int64).alias("claim_line_number"),
            pl.when(pl.col("clm_bill_fac_type_cd").is_in(["1", "2"]))
            .then(pl.lit("institutional"))
            .otherwise(pl.lit("institutional"))
            .alias("claim_type"),
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
            pl.when(pl.col("clm_from_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_from_dt").cast(pl.Utf8))
            .alias("admission_date"),
            pl.when(pl.col("clm_thru_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_thru_dt").cast(pl.Utf8))
            .alias("discharge_date"),
            pl.col("clm_admsn_src_cd").alias("admit_source_code"),
            pl.col("clm_admsn_type_cd").alias("admit_type_code"),
            pl.col("bene_ptnt_stus_cd").alias("discharge_disposition_code"),
            pl.lit(None).cast(pl.String).alias("place_of_service_code"),
            pl.concat_str(
                [
                    pl.col("clm_bill_fac_type_cd"),
                    pl.col("clm_bill_clsfctn_cd"),
                    pl.col("clm_bill_freq_cd"),
                ]
            ).alias("bill_type_code"),
            pl.col("dgns_drg_cd").alias("ms_drg_code"),
            pl.lit(None).cast(pl.String).alias("apr_drg_code"),
            pl.col("clm_line_prod_rev_ctr_cd").alias("revenue_center_code"),
            pl.col("clm_line_srvc_unit_qty").alias("service_unit_quantity"),
            pl.col("clm_line_hcpcs_cd").alias("hcpcs_code"),
            pl.col("hcpcs_1_mdfr_cd").alias("hcpcs_modifier_1"),
            pl.col("hcpcs_2_mdfr_cd").alias("hcpcs_modifier_2"),
            pl.col("hcpcs_3_mdfr_cd").alias("hcpcs_modifier_3"),
            pl.col("hcpcs_4_mdfr_cd").alias("hcpcs_modifier_4"),
            pl.col("hcpcs_5_mdfr_cd").alias("hcpcs_modifier_5"),
            pl.lit(None).cast(pl.String).alias("rendering_npi"),
            pl.lit(None).cast(pl.String).alias("rendering_tin"),
            pl.lit(None).cast(pl.String).alias("billing_npi"),
            pl.lit(None).cast(pl.String).alias("billing_tin"),
            pl.col("fac_prvdr_npi_num").alias("facility_npi"),
            pl.when(pl.col("clm_efctv_dt").cast(pl.Utf8).is_in(["1000-01-01", "9999-12-31"]))
            .then(None)
            .otherwise(pl.col("clm_efctv_dt"))
            .alias("paid_date"),
            # CCLF Guide Section 3.5: Use line-level payments ONLY if they sum to header.
            # Otherwise, allocate header payment to first line only to avoid double-counting.
            CclfRevenueCenterValidationExpression.allocate_header_payment_to_first_line()
            .cast(pl.Decimal(None, 2))
            .alias("paid_amount"),
            pl.col("sum_clm_mdcr_instnl_tot_chrg_amt")
            .cast(pl.Decimal(None, 2))
            .alias("allowed_amount"),
            pl.col("sum_clm_mdcr_instnl_tot_chrg_amt")
            .cast(pl.Decimal(None, 2))
            .alias("charge_amount"),
            pl.lit(None).cast(pl.String).alias("coinsurance_amount"),
            pl.lit(None).cast(pl.String).alias("copayment_amount"),
            pl.lit(None).cast(pl.String).alias("deductible_amount"),
            pl.lit(None).cast(pl.String).alias("total_cost_amount"),
            pl.col("diagnosis_icd_ind").alias("diagnosis_code_type"),
            *[pl.col(f"diagnosis_code_{i}") for i in range(1, 26)],
            *[pl.col(f"diagnosis_poa_{i}") for i in range(1, 26)],
            pl.col("procedure_icd_ind").alias("procedure_code_type"),
            *[pl.col(f"procedure_code_{i}") for i in range(1, 26)],
            *[pl.col(f"procedure_date_{i}") for i in range(1, 26)],
            pl.lit(1).alias("in_network_flag"),
            pl.lit("medicare cclf").alias("data_source"),
            pl.col("source_filename"),
            pl.col("file_date").alias("ingest_datetime"),
        ]
    )

    return result
