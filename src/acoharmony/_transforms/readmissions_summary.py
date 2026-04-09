# © 2025 HarmonyCares
# All rights reserved.

"""
Readmissions Summary Transform.

Identifies 30-day all-cause hospital readmissions following CMS methodology.
Finds pairs of inpatient admissions where a patient was readmitted within
30 days of discharge from an index admission.

Inputs (gold):
    - medical_claim.parquet - Medical claims with admission/discharge dates

Outputs (gold):
    - readmissions_summary.parquet - Index and readmission pairs
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced
from .._expressions._readmissions import ReadmissionsExpression
from ..medallion import MedallionLayer


@composable
@traced()
@timeit(log_level="info", threshold=10.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Calculate 30-day readmissions from medical claims.

    Logic:
        1. Load medical_claim from gold layer
        2. Filter to inpatient admissions (bill_type_code 11x, 12x)
        3. Self-join to find readmission pairs within 30 days
        4. Calculate days between discharge and readmission
        5. Return readmission pairs

    Returns:
        LazyFrame with columns:
        - patient_id
        - index_encounter_id
        - index_admission_date
        - index_discharge_date
        - readmission_encounter_id
        - readmission_admission_date
        - days_to_readmission
    """
    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)
    logger = executor.logger

    # Load medical claims
    medical_claim_path = gold_path / "medical_claim.parquet"

    if not medical_claim_path.exists():
        logger.warning(f"medical_claim.parquet not found at {medical_claim_path}")
        logger.info("Skipping readmissions - run 'aco pipeline cclf_gold' first")
        # Return empty dataframe with expected schema
        return pl.LazyFrame(
            schema={
                "patient_id": pl.Utf8,
                "index_encounter_id": pl.Utf8,
                "index_admission_date": pl.Date,
                "index_discharge_date": pl.Date,
                "readmission_encounter_id": pl.Utf8,
                "readmission_admission_date": pl.Date,
                "days_to_readmission": pl.Int64,
            }
        )

    medical_claim = pl.scan_parquet(medical_claim_path)

    # Use expression to identify readmission pairs
    logger.info("Identifying 30-day readmission pairs...")
    readmission_pairs = ReadmissionsExpression.identify_readmission_pairs(
        medical_claim,
        lookback_days=30,
        person_id_col="person_id",
        claim_id_col="claim_id",
        admit_date_col="admission_date",
        discharge_date_col="discharge_date",
    )

    # Count readmissions
    pair_count = readmission_pairs.select(pl.len()).collect().item()
    unique_patients = readmission_pairs.select(pl.col("patient_id").n_unique()).collect().item()

    logger.info("Readmissions summary complete:")
    logger.info(f"  Total readmission events: {pair_count:,}")
    logger.info(f"  Unique patients with readmissions: {unique_patients:,}")

    return readmission_pairs
