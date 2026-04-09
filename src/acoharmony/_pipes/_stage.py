# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline stage utilities for executing transform stages.

This module provides the PipelineStage class for declarative stage definitions
and the execute_stage function for running stages with logging and optimization.

Extracted from _pipeline.py during tech debt cleanup (2025-11-10).
"""

import gc
from datetime import datetime
from typing import Any

import polars as pl

from .._decor8 import transform_method


class PipelineStage:
    """Declarative pipeline stage definition."""

    def __init__(
        self, name: str, module: Any, group: str, order: int, depends_on: list[str] | None = None
    ):
        """
        Define a pipeline stage.

        Args:
            name: Output name for the transform
            module: Module containing execute() function
            group: Logical grouping (crosswalk, claims, supporting, enrollment)
            order: Execution order within pipeline
            depends_on: Optional list of stage names this depends on
        """
        self.name = name
        self.module = module
        self.group = group
        self.order = order
        self.depends_on = depends_on or []

    def __repr__(self) -> str:
        deps = f" → depends on {self.depends_on}" if self.depends_on else ""
        return f"Stage({self.order}: {self.name} [{self.group}]{deps})"


@transform_method(enable_composition=False, threshold=5.0)  # Returns tuple, not LazyFrame
def execute_stage(
    stage: PipelineStage, executor: Any, logger: Any, output_path: Any
) -> tuple[None, int]:
    """
    Execute a single pipeline stage with FORCED STREAMING to prevent memory exhaustion.

    STREAMING GUARANTEES:
        - All operations use streaming engine (no full materialization)
        - sink_parquet uses streaming writes with small row groups
        - Memory is released immediately after each stage
        - No LazyFrame references retained

    Returns:
        tuple[None, int]: (None, 0) - LazyFrame is explicitly cleared after sink
    """
    import os

    from ..config import get_config

    logger.info(f"[{stage.group.upper()}] Stage {stage.order}: {stage.name}")
    start_time = datetime.now()
    result = stage.module.execute(executor)
    parquet_path = output_path / f"{stage.name}.parquet"
    config = get_config()
    compression = config.transform.compression
    row_group_size = config.transform.row_group_size

    # CRITICAL: Force streaming for all sink operations
    # This prevents memory exhaustion on large joins and aggregations
    force_streaming = os.environ.get("ACO_FORCE_STREAMING", "0") == "1"

    # Sink to parquet with VERY SMALL row groups to prevent wslhost.exe fragmentation
    # After multiple stages, wslhost.exe address space becomes fragmented
    # Smaller row groups = more frequent writes = smaller allocations = less fragmentation
    # 10k rows is ~2-5MB per chunk depending on schema - aggressive but should prevent crashes
    effective_row_group_size = 10000 if force_streaming else row_group_size

    # CRITICAL: Explicitly force streaming engine
    # Some transforms may break streaming (like group_by + join), but we've fixed stage 8
    # Forcing engine='streaming' ensures Polars uses streaming for the entire pipeline
    result.sink_parquet(
        str(parquet_path),
        compression=compression,
        row_group_size=effective_row_group_size,
        engine='streaming',  # Force streaming engine - never collect in memory
    )

    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"  [OK] {stage.name} → {parquet_path.name} in {elapsed:.2f}s")

    # CRITICAL: Explicitly delete LazyFrame to break any circular references
    del result

    # CRITICAL: Clear Polars global string cache to prevent memory accumulation
    # The string cache accumulates across stages and can consume significant memory
    # DON'T re-enable it - let each stage start fresh without accumulated string mappings
    pl.disable_string_cache()

    # Run garbage collection TWICE to handle cyclic references
    # First pass breaks cycles, second pass collects unreachable objects
    gc.collect()
    gc.collect()

    # Return None - no references to LazyFrame should remain
    return None, 0
