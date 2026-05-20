# © 2025 HarmonyCares
# All rights reserved.

"""
Wound care pattern analysis pipeline.

Analyzes wound care claims for concerning billing and utilization patterns:
- High frequency applications (15+ per patient per year)
- High cost patients (>$1M in costs)
- Clustered claims (3+ within 1 week)
- Same-day duplicates
- Identical billing patterns (by NPI)

Outputs:
- wound_care_high_frequency_patient.parquet: Patient-level high frequency data
- wound_care_high_frequency_npi.parquet: NPI-level high frequency summaries
- wound_care_high_cost.parquet: High cost patients
- wound_care_clustered_details.parquet: Cluster details
- wound_care_clustered_npi.parquet: NPI-level cluster summaries
- wound_care_duplicates_details.parquet: Duplicate instance details
- wound_care_duplicates_npi.parquet: NPI-level duplicate summaries
- wound_care_identical_patterns_details.parquet: Billing pattern details
- wound_care_identical_patterns_npi.parquet: NPI-level pattern summaries
"""

from typing import Any

import polars as pl

from .._decor8 import transform_method
from .._log import LogWriter
from ._registry import register_pipeline
from ._stage import PipelineStage

logger = LogWriter("pipes.wound_care_analysis")


class AnalysisStage(PipelineStage):
    """
    Extended pipeline stage for analysis outputs.

    Handles transforms that return dictionaries with multiple outputs.
    """

    def __init__(
        self,
        name: str,
        module: Any,
        group: str,
        order: int,
        depends_on: list[str],
        output_keys: list[str],
    ):
        super().__init__(name, module, group, order, depends_on)
        self.output_keys = output_keys  # Keys in the returned dict


@register_pipeline(name="wound_care_analysis")
@transform_method(
    enable_composition=False,
    threshold=10.0,
)
def apply_wound_care_analysis_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, pl.LazyFrame]:
    """
    Apply wound care pattern analysis pipeline.

    This pipeline analyzes skin substitute claims for 5 pattern types:
    1. High frequency providers (15+ applications per patient)
    2. High cost patients (>$1M total)
    3. Clustered claims (3+ within 1 week)
    4. Same-day duplicates
    5. Identical billing patterns (by NPI)

    Pipeline Order:
        Stage 1: high_frequency - Identifies providers with intensive usage
        Stage 2: high_cost - Identifies exceptionally costly patients
        Stage 3: clustered - Identifies temporal clustering patterns
        Stage 4: duplicates - Identifies same-day duplicate claims
        Stage 5: identical_patterns - Identifies standardized billing patterns

    Args:
        executor: TransformRunner instance with storage_config and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, pl.LazyFrame]: Dictionary mapping output names to LazyFrames

    Notes:
        - Depends on skin_substitute_claims.parquet from wound_care pipeline
        - All analysis outputs saved to gold layer
        - Uses checkpoint/resume for fault tolerance
    """
    from ..medallion import MedallionLayer
    from ._checkpoint import PipelineCheckpoint

    gold_path = executor.storage_config.get_path(MedallionLayer.GOLD)

    # Import transforms
    from .._transforms import (
        wound_care_clustered,
        wound_care_duplicates,
        wound_care_high_cost,
        wound_care_high_frequency,
        wound_care_identical_patterns,
    )

    # Define analysis stages
    high_frequency_stage = AnalysisStage(
        name="high_frequency",
        module=wound_care_high_frequency,
        group="wound_care_analysis",
        order=1,
        depends_on=["skin_substitute_claims"],
        output_keys=["patient_level", "npi_summary"],
    )

    high_cost_stage = AnalysisStage(
        name="high_cost",
        module=wound_care_high_cost,
        group="wound_care_analysis",
        order=2,
        depends_on=["skin_substitute_claims"],
        output_keys=["high_cost_patients"],
    )

    clustered_stage = AnalysisStage(
        name="clustered",
        module=wound_care_clustered,
        group="wound_care_analysis",
        order=3,
        depends_on=["skin_substitute_claims"],
        output_keys=["cluster_details", "npi_summary"],
    )

    duplicates_stage = AnalysisStage(
        name="duplicates",
        module=wound_care_duplicates,
        group="wound_care_analysis",
        order=4,
        depends_on=["skin_substitute_claims"],
        output_keys=["duplicate_details", "npi_summary"],
    )

    identical_patterns_stage = AnalysisStage(
        name="identical_patterns",
        module=wound_care_identical_patterns,
        group="wound_care_analysis",
        order=5,
        depends_on=["skin_substitute_claims"],
        output_keys=["pattern_details", "npi_summary"],
    )

    stages = [
        high_frequency_stage,
        high_cost_stage,
        clustered_stage,
        duplicates_stage,
        identical_patterns_stage,
    ]

    checkpoint = PipelineCheckpoint("wound_care_analysis", force=force)

    logger.info(f"Starting Wound Care Analysis Pipeline: {len(stages)} stages")
    logger.info("=" * 80)
    checkpoint.log_resume_info(logger, len(stages))

    output_files = {}

    # Execute stages with checkpoint/resume
    for stage in sorted(stages, key=lambda s: s.order):
        # Determine output file names based on stage type
        if hasattr(stage, "output_keys"):
            # Multi-output stage
            stage_outputs = {}
            for key in stage.output_keys:
                if key == "patient_level":
                    output_name = f"wound_care_{stage.name}_patient"
                elif key == "npi_summary":
                    output_name = f"wound_care_{stage.name}_npi"
                elif key == "high_cost_patients":
                    output_name = f"wound_care_{stage.name}"
                elif key in ["cluster_details", "duplicate_details", "pattern_details"]:
                    output_name = f"wound_care_{stage.name}_details"
                else:
                    output_name = f"wound_care_{stage.name}_{key}"
                stage_outputs[key] = gold_path / f"{output_name}.parquet"
        else:
            # Single-output stage
            stage_outputs = {stage.name: gold_path / f"wound_care_{stage.name}.parquet"}

        # Check if all outputs exist for skip logic
        all_outputs_exist = all(path.exists() for path in stage_outputs.values())
        primary_output = list(stage_outputs.values())[0]

        should_skip, row_count = checkpoint.should_skip_stage(
            stage.name, primary_output, logger
        )

        if should_skip and all_outputs_exist:
            logger.info(
                f"[{stage.group.upper()}] Stage {stage.order}: {stage.name} (skipped)"
            )
            checkpoint.completed_stages.append(stage.name)
            output_files.update(stage_outputs)
            continue

        try:
            logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
            logger.info(f"  Executing {stage.module.__name__}.execute()")

            # Execute transform
            result = stage.module.execute(executor)

            # Handle result based on type
            if isinstance(result, dict):
                # Multi-output transform
                for key, lazyframe in result.items():
                    output_path = stage_outputs[key]
                    logger.info(f"  Writing {key} to {output_path.name}")
                    lazyframe.collect().write_parquet(output_path)
                    output_files[output_path.stem] = output_path
            else:
                # Single-output transform
                output_path = stage_outputs[list(stage_outputs.keys())[0]]
                logger.info(f"  Writing to {output_path.name}")
                result.collect().write_parquet(output_path)
                output_files[output_path.stem] = output_path

            checkpoint.mark_stage_complete(stage.name)
            logger.info(f"  [OK] {stage.name} complete")

        except Exception as e:
            logger.error(f"  [ERROR] {stage.name} failed: {e}")
            logger.info(f"\n{'=' * 80}")
            logger.info(
                f"Pipeline STOPPED at stage {stage.order}/{len(stages)}: {stage.name}"
            )
            logger.info(
                f"Completed stages saved to: {checkpoint.get_tracking_file_path()}"
            )
            logger.info(
                "To resume from this stage, run again (completed stages will be skipped)"
            )
            logger.info("To force re-run all stages, use --force flag")
            logger.info(f"{'=' * 80}\n")
            raise

    # Count rows from written parquet files
    logger.info("=" * 80)
    logger.info("Counting final row counts...")
    total_rows = 0
    for file_path in output_files.values():
        if file_path.exists():
            row_count = pl.scan_parquet(file_path).select(pl.len()).collect().item()
            total_rows += row_count
            logger.info(f"  {file_path.stem}: {row_count:,} rows")

    logger.info("=" * 80)
    logger.info(
        f"[OK] Wound Care Analysis Pipeline Complete: {len(output_files)} tables generated"
    )
    logger.info(f"  Total rows: {total_rows:,}")
    logger.info("=" * 80)

    # Mark pipeline as complete
    checkpoint.mark_pipeline_complete(total_rows, 0)

    return output_files
