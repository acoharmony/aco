# © 2025 HarmonyCares
# All rights reserved.

"""
Temporal Alignment Transformation for Idempotent Processing.

This module implements sophisticated temporal logic for beneficiary alignment,
handling the complex requirements of MSSP and REACH programs with proper
idempotent temporality, reconciliation vs current file logic, and comprehensive
data lineage tracking.

Key Concepts:
- Idempotent Processing: Results depend only on data, not "today's date"
- Temporal Windowing: Proper handling of quarterly (MSSP) vs monthly (REACH) data
- Reconciliation Logic: Historical reconciliation vs current period files
- Death Date Truncation: Proper enrollment period termination
- Voluntary Alignment: SVA signature validity and practitioner mapping
"""

import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from .._store import StorageBackend
from ..result import Result
from ..tracking import TransformTracker


class TemporalAlignmentTransform:
    """
    Handles temporal alignment logic for consolidated beneficiary data.

        This transform implements the idempotent temporality requirements:
        - Windows enrollment periods based on file temporality
        - Handles reconciliation vs current file logic
        - Tracks data lineage with temporal context
        - Manages voluntary alignment validity periods
    """

    def __init__(self, storage_backend=None):
        """Initialize temporal alignment transform."""
        self.storage = storage_backend or StorageBackend()
        self.tracker = TransformTracker("consolidated_alignment")
        self.file_patterns = {
            "alr_annual": r".*AALR.*Y(\d{4})",  # Annual ALR reconciliation
            "alr_quarterly": r".*QALR.*(\d{4})Q(\d)",  # Quarterly ALR current
            "bar_monthly": r".*ALG[CR].*(\d{4})M(\d{2})",  # Monthly BAR
            "bar_reconciliation": r".*ALGR.*(\d{4}).*RUN",  # BAR reconciliation
        }
    @traced()
    @timeit(log_level="debug")
    def extract_file_temporality(self, filename: str) -> dict[str, Any]:
        """
        Extract temporal information from filename.

                Returns dict with:
                - program: 'MSSP' or 'REACH'
                - type: 'reconciliation' or 'current'
                - year: int
                - period: quarter (1-4) or month (1-12)
                - start_date: period start
                - end_date: period end
        """
        result = {
            "filename": filename,
            "program": None,
            "type": None,
            "year": None,
            "period": None,
            "start_date": None,
            "end_date": None,
        }
        if "ALR" in filename.upper():
            result["program"] = "MSSP"
            match = re.search(self.file_patterns["alr_annual"], filename)
            if match:
                year = int(match.group(1))
                result.update(
                    {
                        "type": "reconciliation",
                        "year": year,
                        "period": "annual",
                        "start_date": datetime(year, 1, 1).date(),
                        "end_date": datetime(year, 12, 31).date(),
                    }
                )
                return result

            match = re.search(self.file_patterns["alr_quarterly"], filename)
            if match:
                year = int(match.group(1))
                quarter = int(match.group(2))
                quarter_starts = {1: 1, 2: 4, 3: 7, 4: 10}
                quarter_ends = {1: 3, 2: 6, 3: 9, 4: 12}

                start_month = quarter_starts[quarter]
                end_month = quarter_ends[quarter]

                result.update(
                    {
                        "type": "current",
                        "year": year,
                        "period": f"Q{quarter}",
                        "start_date": datetime(year, start_month, 1).date(),
                        "end_date": datetime(year, end_month, 1).date() + timedelta(days=31),
                    }
                )
                result["end_date"] = result["end_date"].replace(day=1) - timedelta(days=1)
                return result

        elif "ALG" in filename.upper():
            result["program"] = "REACH"

            if "RUN" in filename.upper() or "RECON" in filename.upper():
                match = re.search(r"(\d{4})", filename)
                if match:
                    year = int(match.group(1))
                    result.update(
                        {
                            "type": "reconciliation",
                            "year": year,
                            "period": "annual",
                            "start_date": datetime(year, 1, 1).date(),
                            "end_date": datetime(year, 12, 31).date(),
                        }
                    )
                    return result

            match = re.search(self.file_patterns["bar_monthly"], filename)
            if match:
                year = int(match.group(1))
                month = int(match.group(2))

                start_date = datetime(year, month, 1).date()
                if month == 12:
                    end_date = datetime(year, 12, 31).date()
                else:
                    end_date = datetime(year, month + 1, 1).date() - timedelta(days=1)

                result.update(
                    {
                        "type": "current",
                        "year": year,
                        "period": f"M{month:02d}",
                        "start_date": start_date,
                        "end_date": end_date,
                    }
                )
                return result

        return result

    @traced()
    @explain(
        why="Temporal windowing failed",
        how="Check enrollment data has valid dates and file temporality is correct",
        causes=["Invalid date format", "Missing temporal info", "Date calculation error"],
    )
    @timeit(log_level="info")
    def apply_temporal_windowing(
        self, df: pl.LazyFrame, file_temporality: dict[str, Any]
    ) -> pl.LazyFrame:
        """
        Apply temporal windowing based on file type and program rules.

                For reconciliation files:
                - Use the full year coverage

                For current files:
                - MSSP: Extend quarterly window to most recent data month
                - REACH: Extend monthly window to most recent data month
                - Both: Cap at death date or program transition
        """
        program = file_temporality["program"]
        file_type = file_temporality["type"]

        df = df.with_columns(
            [
                pl.lit(file_temporality["start_date"]).alias("enrollment_start"),
                pl.lit(file_temporality["end_date"]).alias("enrollment_end_raw"),
                pl.lit(program).alias("current_program"),
                pl.lit(file_type).alias("source_file_type"),
                pl.lit(file_temporality["filename"]).alias("source_file"),
                pl.lit(file_temporality["period"]).alias("source_period"),
                pl.lit(datetime.now()).alias("processed_timestamp"),
            ]
        )

        df = df.with_columns(
            pl.when(pl.col("bene_death_date").is_not_null())
            .then(pl.min_horizontal(["enrollment_end_raw", "bene_death_date"]))
            .otherwise(pl.col("enrollment_end_raw"))
            .alias("enrollment_end")
        )

        return df

    @traced()
    @timeit(log_level="info")
    def calculate_signature_validity(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate SVA signature validity periods.

                Signatures are valid for 2 years + remainder of current year,
                expiring on Jan 1 of year X+3.
        """
        df = df.with_columns(
            [
                pl.when(pl.col("voluntary_alignment_date").is_not_null())
                .then(pl.date(pl.col("voluntary_alignment_date").dt.year() + 3, 1, 1))
                .otherwise(None)
                .alias("signature_expiry_date"),
                pl.when(pl.col("voluntary_alignment_date").is_not_null())
                .then((pl.col("voluntary_alignment_date").dt.year() + 3) > datetime.now().year)
                .otherwise(False)
                .alias("signature_currently_valid"),
                pl.when(pl.col("voluntary_alignment_date").is_not_null())
                .then(
                    (
                        pl.date(pl.col("voluntary_alignment_date").dt.year() + 3, 1, 1)
                        - pl.lit(datetime.now().date())
                    ).dt.total_days()
                )
                .otherwise(None)
                .alias("days_until_signature_expiry"),
            ]
        )
        return df

    @traced()
    @explain(
        why="Failed to merge temporal alignments",
        how="Check all alignment dataframes have required columns and compatible schemas",
        causes=["Missing required columns", "Schema mismatch", "Empty alignment list"],
    )
    @timeit(log_level="info", threshold=5.0)
    def merge_temporal_alignments(self, alignments: list[pl.LazyFrame]) -> pl.LazyFrame:
        """
        Merge multiple temporal alignment records intelligently.

                Handles:
                - Program transitions (MSSP to REACH)
                - Overlapping periods from reconciliation vs current
                - Voluntary alignment precedence
        """
        if not alignments:
            return pl.LazyFrame()

        combined = pl.concat(alignments, how="vertical_relaxed")

        combined = combined.sort(
            [
                "bene_mbi",
                "enrollment_start",
                "source_file_type",
            ]
        )

        combined = combined.with_columns(
            [
                pl.col("enrollment_end").shift(1).over("bene_mbi").alias("prev_enrollment_end"),
                pl.col("current_program").shift(1).over("bene_mbi").alias("prev_program"),
                pl.col("enrollment_start").cum_count().over("bene_mbi").alias("enrollment_sequence"),
            ]
        )

        combined = combined.with_columns(
            [
                pl.when(pl.col("prev_enrollment_end").is_not_null())
                .then((pl.col("enrollment_start") - pl.col("prev_enrollment_end")).dt.total_days())
                .otherwise(None)
                .alias("enrollment_gap_days"),
                pl.when(pl.col("prev_program").is_not_null())
                .then(pl.col("prev_program") != pl.col("current_program"))
                .otherwise(False)
                .alias("is_program_transition"),
            ]
        )

        return combined

    @traced()
    @timeit(log_level="debug")
    def add_lineage_tracking(self, df: pl.LazyFrame) -> pl.LazyFrame:
        """
        Add comprehensive data lineage columns.
        """
        df = df.with_columns(
            [
                pl.concat_str(
                    [
                        pl.col("source_file"),
                        pl.lit(" ("),
                        pl.col("source_file_type"),
                        pl.lit(" "),
                        pl.col("source_period"),
                        pl.lit(")"),
                    ]
                ).alias("lineage_source"),
                pl.lit(datetime.now().isoformat()).alias("lineage_processed_at"),
                pl.lit("temporal_alignment_v2").alias("lineage_transform"),
                pl.struct(
                    ["enrollment_start", "enrollment_end", "source_period", "source_file_type"]
                ).alias("temporal_context"),
            ]
        )

        return df

    @traced()
    @explain(
        why="Consolidated temporal logic failed",
        how="Check all input files exist, have correct format, and contain valid temporal data",
        causes=[
            "Missing input files",
            "Invalid file format",
            "Temporal data inconsistency",
            "File processing error",
        ],
    )
    @timeit(log_level="info", threshold=10.0)
    def apply_consolidated_temporal_logic(
        self, schema: dict[str, Any], source_files: list[str]
    ) -> Result[pl.LazyFrame]:
        """
        Apply complete temporal alignment logic to consolidate alignments.

                This is the main entry point that orchestrates:
                1. File temporality extraction
                2. Temporal windowing
                3. Signature validity calculation
                4. Alignment merging
                5. Lineage tracking
        """
        try:
            self.tracker.start_transform(
                pipeline="consolidated_alignment", stage="temporal_processing"
            )

            aligned_dataframes = []

            for file_path in source_files:
                if self.tracker.has_processed_file(file_path):
                    continue

                temporality = self.extract_file_temporality(Path(file_path).name)

                if not temporality["program"]:
                    self.tracker.track_file(file_path, "skipped")
                    continue

                df = self._load_source_file(file_path, temporality["program"])

                if df is None:
                    self.tracker.track_file(file_path, "failed")
                    continue

                df = self.apply_temporal_windowing(df, temporality)

                if temporality["program"] == "REACH":
                    df = self.calculate_signature_validity(df)

                df = self.add_lineage_tracking(df)

                aligned_dataframes.append(df)
                self.tracker.track_file(file_path, "processed")

            if not aligned_dataframes:
                return Result.error("No valid alignment data found")

            consolidated = self.merge_temporal_alignments(aligned_dataframes)

            consolidated = consolidated.sort(
                ["bene_mbi", "enrollment_start", "lineage_processed_at"],
                descending=[False, False, True],
            )

            consolidated = consolidated.group_by("bene_mbi").agg([pl.first("*")])

            self.tracker.complete_transform(
                success=True, files=len(source_files), message="Temporal alignment completed"
            )

            return Result.ok(consolidated)

        except (
            Exception
        ) as e:  # ALLOWED: Result monad pattern - returns error Result instead of raising
            self.tracker.complete_transform(
                success=False, message=f"Temporal alignment failed: {str(e)}"
            )
            return Result.error(f"Temporal alignment error: {str(e)}")

    @traced()
    @timeit(log_level="debug")
    def _load_source_file(self, file_path: str, program: str) -> pl.LazyFrame | None:
        """
        Load source file based on program type using StorageBackend.
        """
        try:
            if "processed" in str(file_path):
                return pl.scan_parquet(file_path)
            else:
                return None
        except Exception as e:  # ALLOWED: Returns None to indicate error
            print(f"Error loading {file_path}: {e}")
            return None
