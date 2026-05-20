# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline checkpoint/resume utilities.

Provides stateful tracking of pipeline execution across runs to enable
resuming from failure points without re-running completed stages.

Created: 2025-11-24
"""

import json
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl


class PipelineCheckpoint:
    """
    Manages checkpoint/resume state for a pipeline.

    Only skips stages if:
    1. Previous run was incomplete (crashed/failed)
    2. Stage completed in that previous run
    3. Output parquet file is still valid

    When a pipeline completes successfully, the next run starts fresh.
    """

    def __init__(self, pipeline_name: str, force: bool = False):
        """
        Initialize checkpoint manager for a pipeline.

        Args:
            pipeline_name: Unique name for this pipeline (e.g., "cclf_silver")
            force: If True, ignore checkpoint and re-run all stages
        """
        self.pipeline_name = pipeline_name
        self.force = force
        self.tracking_dir = Path("/opt/s3/data/workspace/logs/tracking")
        self.tracking_dir.mkdir(parents=True, exist_ok=True)
        self.tracking_file = self.tracking_dir / f"{pipeline_name}_checkpoint.json"

        self.completed_stages: list[str] = []
        self.previous_completed: set[str] = set()
        self.previous_run_complete = True

        self._load_state()

    def _load_state(self) -> None:
        """Load checkpoint state from previous run."""
        if not self.tracking_file.exists() or self.force:
            return

        try:
            with open(self.tracking_file) as f:
                tracking_data = json.load(f)
                self.previous_run_complete = tracking_data.get("pipeline_complete", True)
                self.previous_completed = set(tracking_data.get("completed_stages", []))
        except Exception:
            # If we can't read the file, start fresh
            self.previous_completed = set()
            self.previous_run_complete = True

    def log_resume_info(self, logger: Any, total_stages: int) -> None:
        """
        Log checkpoint/resume information at pipeline start.

        Args:
            logger: Logger instance
            total_stages: Total number of stages in pipeline
        """
        if not self.previous_run_complete and self.previous_completed:
            logger.info(
                f"Found incomplete previous run with {len(self.previous_completed)} "
                f"completed stages"
            )
            logger.info(f"Resuming from stage {len(self.previous_completed) + 1}")
        elif self.previous_completed and not self.force:
            logger.info("Previous run completed successfully - starting fresh")

    def should_skip_stage(
        self, stage_name: str, output_file: Path, logger: Any
    ) -> tuple[bool, int]:
        """
        Determine if a stage should be skipped based on checkpoint state.

        Args:
            stage_name: Name of the stage
            output_file: Path to output parquet file
            logger: Logger instance for warnings

        Returns:
            tuple[bool, int]: (should_skip, row_count)
                - should_skip: True if stage should be skipped
                - row_count: Number of rows in existing file (0 if not skipping)
        """
        # Never skip if force flag set
        if self.force:
            return False, 0

        # Only skip if previous run was incomplete AND this stage completed
        if self.previous_run_complete or stage_name not in self.previous_completed:
            return False, 0

        # Validate the parquet file is still valid
        if not output_file.exists() or output_file.stat().st_size == 0:
            logger.warning(
                f"  ⚠ {stage_name} → {output_file.name} missing or empty, re-running"
            )
            return False, 0

        try:
            # Verify parquet file is complete and readable
            row_count = pl.scan_parquet(output_file).select(pl.len()).collect().item()
            if row_count > 0:
                logger.info(
                    f"  ↻ {stage_name} → {output_file.name} "
                    f"(completed in previous run, valid parquet with {row_count:,} rows, skipping)"
                )
                return True, row_count
            else:
                logger.warning(
                    f"  ⚠ {stage_name} → {output_file.name} exists but has 0 rows, re-running"
                )
                return False, 0
        except Exception as e:
            logger.warning(
                f"  ⚠ {stage_name} → {output_file.name} exists but is invalid/corrupted "
                f"({type(e).__name__}), re-running"
            )
            return False, 0

    def mark_stage_complete(self, stage_name: str) -> None:
        """
        Mark a stage as completed and update tracking file.

        Args:
            stage_name: Name of the completed stage
        """
        self.completed_stages.append(stage_name)

        # Update tracking file with incomplete state
        with open(self.tracking_file, "w") as f:
            json.dump(
                {
                    "pipeline_complete": False,
                    "completed_stages": self.completed_stages,
                    "last_updated": datetime.now().isoformat(),
                    "pipeline_name": self.pipeline_name,
                },
                f,
                indent=2,
            )

    def mark_pipeline_complete(self, total_rows: int, elapsed_seconds: float) -> None:
        """
        Mark entire pipeline as complete.

        Next run will start fresh rather than resuming.

        Args:
            total_rows: Total rows processed across all stages
            elapsed_seconds: Total pipeline execution time
        """
        with open(self.tracking_file, "w") as f:
            json.dump(
                {
                    "pipeline_complete": True,
                    "completed_stages": self.completed_stages,
                    "last_updated": datetime.now().isoformat(),
                    "pipeline_name": self.pipeline_name,
                    "total_rows": total_rows,
                    "elapsed_seconds": elapsed_seconds,
                },
                f,
                indent=2,
            )

    def get_tracking_file_path(self) -> Path:
        """Get path to tracking file for logging."""
        return self.tracking_file
