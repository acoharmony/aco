# © 2025 HarmonyCares
# All rights reserved.

"""
Beneficiary-Level Metrics Transform.

Calculates comprehensive beneficiary-level metrics from medical and pharmacy claims:
- Spend by category (inpatient, outpatient, SNF, hospice, home health, Part B)
- Utilization (inpatient admits, ER visits, E&M visits)
- Clinical indicators (hospice, SNF, IRF, home health admissions)
- Care gaps (most recent AWV, last E&M visit)

Inputs (gold):
    - medical_claim.parquet - Medical claims with service categories
    - pharmacy_claim.parquet - Pharmacy claims

Outputs (gold):
    - beneficiary_metrics.parquet - Beneficiary-level metrics by year
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced
from .._expressions._clinical_indicators import ClinicalIndicatorExpression
from .._expressions._spend_category import SpendCategoryExpression
from .._expressions._utilization import UtilizationExpression
from ..medallion import MedallionLayer


@composable
@traced()
@timeit(log_level="info", threshold=5.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Calculate beneficiary-level metrics from claims.

    Logic:
        1. Load medical_claim and pharmacy_claim
        2. Apply spend category expressions to calculate spend by type
        3. Apply utilization expressions to count admissions and visits
        4. Apply clinical indicator expressions to identify care transitions
        5. Aggregate to beneficiary-year level
        6. Calculate YTD and 90-day rolling metrics
    """
    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)

    # Load inputs
    medical_claim = pl.scan_parquet(gold_path / "medical_claim.parquet")
    pharmacy_claim = pl.scan_parquet(gold_path / "pharmacy_claim.parquet")

    # Extract year from claim dates for grouping
    medical_with_year = medical_claim.with_columns([
        pl.col("claim_start_date").dt.year().alias("year")
    ])

    pharmacy_with_year = pharmacy_claim.with_columns([
        pl.col("dispensing_date").dt.year().alias("year")
    ])

    # Apply spend category expressions to medical claims
    medical_spend = medical_with_year.with_columns([
        SpendCategoryExpression.is_inpatient_spend().alias("inpatient_spend"),
        SpendCategoryExpression.is_outpatient_spend().alias("outpatient_spend"),
        SpendCategoryExpression.is_snf_spend().alias("snf_spend"),
        SpendCategoryExpression.is_hospice_spend().alias("hospice_spend"),
        SpendCategoryExpression.is_home_health_spend().alias("home_health_spend"),
        SpendCategoryExpression.is_part_b_carrier_spend().alias("part_b_carrier_spend"),
    ])

    # Apply utilization expressions
    medical_utilization = medical_spend.with_columns([
        UtilizationExpression.is_inpatient_admission().alias("inpatient_admission"),
        UtilizationExpression.is_er_visit().alias("er_visit"),
        UtilizationExpression.is_em_visit().alias("em_visit"),
        UtilizationExpression.is_awv().alias("awv"),
    ])

    # Apply clinical indicator expressions
    medical_clinical = medical_utilization.with_columns([
        ClinicalIndicatorExpression.is_hospice_admission().alias("hospice_admission"),
        ClinicalIndicatorExpression.is_snf_admission().alias("snf_admission"),
        ClinicalIndicatorExpression.is_irf_admission().alias("irf_admission"),
        ClinicalIndicatorExpression.is_home_health_episode().alias("home_health_episode"),
    ])

    # Aggregate to beneficiary-year level for medical claims
    beneficiary_medical = medical_clinical.group_by(["person_id", "year"]).agg([
        # Spend metrics (YTD)
        pl.col("inpatient_spend").sum().alias("inpatient_spend_ytd"),
        pl.col("outpatient_spend").sum().alias("outpatient_spend_ytd"),
        pl.col("snf_spend").sum().alias("snf_spend_ytd"),
        pl.col("hospice_spend").sum().alias("hospice_spend_ytd"),
        pl.col("home_health_spend").sum().alias("home_health_spend_ytd"),
        pl.col("part_b_carrier_spend").sum().alias("part_b_carrier_spend_ytd"),

        # Utilization counts (YTD) - count unique claims, not sum of flags
        # For inpatient: count unique claims where flag=1
        pl.when(pl.col("inpatient_admission") == 1)
        .then(pl.col("claim_id"))
        .otherwise(None)
        .n_unique()
        .alias("inpatient_admits_ytd"),

        # For ER: count unique visit dates where flag=1 (not claims, as one ER visit = multiple claims)
        pl.when(pl.col("er_visit") == 1)
        .then(pl.col("claim_start_date"))
        .otherwise(None)
        .n_unique()
        .alias("er_admits_ytd"),

        # For E&M: count unique claims where flag=1
        pl.when(pl.col("em_visit") == 1)
        .then(pl.col("claim_id"))
        .otherwise(None)
        .n_unique()
        .alias("em_visits_ytd"),

        # Clinical indicators (any occurrence YTD)
        pl.col("hospice_admission").max().alias("hospice_admission_ytd"),
        pl.col("snf_admission").max().alias("snf_admission_ytd"),
        pl.col("irf_admission").max().alias("irf_admission_ytd"),
        pl.col("home_health_episode").max().alias("home_health_episode_ytd"),

        # Care gap tracking - most recent dates
        pl.when(pl.col("awv") == 1)
        .then(pl.col("claim_start_date"))
        .otherwise(None)
        .max()
        .alias("most_recent_awv_date"),

        pl.when(pl.col("em_visit") == 1)
        .then(pl.col("claim_start_date"))
        .otherwise(None)
        .max()
        .alias("last_em_visit_date"),
    ])

    # Aggregate pharmacy claims to beneficiary-year level
    beneficiary_pharmacy = pharmacy_with_year.group_by(["person_id", "year"]).agg([
        pl.col("paid_amount").sum().alias("dme_spend_ytd"),
    ])

    # Join medical and pharmacy metrics
    result = beneficiary_medical.join(
        beneficiary_pharmacy,
        on=["person_id", "year"],
        how="outer",
    )

    # Fill nulls with 0 for spend/counts, keep null for dates
    spend_count_cols = [
        "inpatient_spend_ytd",
        "outpatient_spend_ytd",
        "snf_spend_ytd",
        "hospice_spend_ytd",
        "home_health_spend_ytd",
        "part_b_carrier_spend_ytd",
        "dme_spend_ytd",
        "inpatient_admits_ytd",
        "er_admits_ytd",
        "em_visits_ytd",
        "hospice_admission_ytd",
        "snf_admission_ytd",
        "irf_admission_ytd",
        "home_health_episode_ytd",
    ]

    result = result.with_columns([
        pl.col(c).fill_null(0.0) for c in spend_count_cols
    ])

    # Add total spend column
    result = result.with_columns([
        pl.sum_horizontal([
            "inpatient_spend_ytd",
            "outpatient_spend_ytd",
            "snf_spend_ytd",
            "hospice_spend_ytd",
            "home_health_spend_ytd",
            "part_b_carrier_spend_ytd",
            "dme_spend_ytd",
        ]).alias("total_spend_ytd")
    ])

    # Select final columns in logical order
    result = result.select([
        "person_id",
        "year",
        # Spend metrics
        "total_spend_ytd",
        "inpatient_spend_ytd",
        "outpatient_spend_ytd",
        "snf_spend_ytd",
        "hospice_spend_ytd",
        "home_health_spend_ytd",
        "part_b_carrier_spend_ytd",
        "dme_spend_ytd",
        # Utilization metrics
        "inpatient_admits_ytd",
        "er_admits_ytd",
        "em_visits_ytd",
        # Clinical indicators
        "hospice_admission_ytd",
        "snf_admission_ytd",
        "irf_admission_ytd",
        "home_health_episode_ytd",
        # Care gaps
        "most_recent_awv_date",
        "last_em_visit_date",
    ])

    return result
