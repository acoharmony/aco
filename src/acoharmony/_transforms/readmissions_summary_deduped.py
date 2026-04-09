# © 2025 HarmonyCares
# All rights reserved.

"""
Readmissions Summary Deduplication Transform.

Removes duplicate rows from readmissions_summary output.
The readmissions detection logic sometimes generates exact duplicate rows
for the same readmission event due to claim duplicates or data quality issues.

Inputs (gold):
    - readmissions_summary.parquet

Outputs (gold):
    - readmissions_summary_deduped.parquet
"""

import polars as pl

from .._decor8 import composable, measure_dataframe_size, timeit, traced
from .._expressions._readmissions import ReadmissionsExpression
from ..medallion import MedallionLayer


@composable
@traced()
@timeit(log_level="info", threshold=2.0)
@measure_dataframe_size()
def execute(executor) -> pl.LazyFrame:
    """
    Deduplicate readmissions summary.

    Logic:
        1. Load readmissions_summary.parquet from gold layer
        2. Remove exact duplicate rows using unique()
        3. Return deduplicated LazyFrame
    """
    storage = executor.storage_config
    gold_path = storage.get_path(MedallionLayer.GOLD)
    logger = executor.logger

    # Load readmissions output
    readmissions_path = gold_path / "readmissions_summary.parquet"

    if not readmissions_path.exists():
        logger.warning(f"readmissions_summary.parquet not found at {readmissions_path}")
        logger.info("Skipping readmissions deduplication - run readmissions_summary transform first")
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

    readmissions = pl.scan_parquet(readmissions_path)

    # Count before deduplication
    orig_count = readmissions.select(pl.len()).collect().item()

    # Deduplicate using expression
    deduped = ReadmissionsExpression.deduplicate_readmissions(readmissions)

    # Count after deduplication
    deduped_count = deduped.select(pl.len()).collect().item()
    removed_count = orig_count - deduped_count

    logger.info("Readmissions deduplication complete:")
    logger.info(f"  Original rows: {orig_count:,}")
    logger.info(f"  Deduped rows: {deduped_count:,}")
    logger.info(f"  Removed: {removed_count:,} duplicate rows")

    return deduped
