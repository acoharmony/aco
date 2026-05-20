# © 2025 HarmonyCares
# All rights reserved.

"""SVA log processing pipeline."""

from typing import Any

import polars as pl

from .._decor8 import transform_method
from .._parsers._mabel_log import parse_mabel_log
from .._transforms import _sva_log as sva_log_transforms
from ..result import ResultStatus, TransformResult
from ._registry import register_pipeline


@register_pipeline(name="sva_log")
@transform_method(enable_composition=False, threshold=30.0)
def apply_sva_log_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, TransformResult]:
    """
    Process Mabel SVA log files through parse → transform → output.

    Pipeline Stages:
        1. Parse raw Mabel log file into structured event rows
        2. Build session-level summary (connections, durations, file counts)
        3. Build upload detail view (patient names, SVA form classification)
        4. Build daily summary (aggregate upload activity by date)

    Args:
        executor: Executor instance (TransformRunner) with storage and catalog access
        logger: Logger instance for recording operations
        force: Force reprocessing regardless of tracking state

    Returns:
        dict[str, TransformResult]: Results for each pipeline stage
    """
    from pathlib import Path

    results: dict[str, TransformResult] = {}

    log_path = Path("/opt/s3/data/workspace/logs/sva/LogMabel.log")

    # Stage 1: Parse raw log
    logger.info("[SVA_LOG] Stage 1: Parsing Mabel log file")
    try:
        lf = parse_mabel_log(log_path)
        row_count = lf.select(pl.len()).collect().item()
        logger.info(f"  [OK] Parsed {row_count:,} log events")
        results["sva_log_parse"] = TransformResult(
            status=ResultStatus.SUCCESS,
            message=f"Parsed {row_count:,} events from {log_path.name}",
        )
    except Exception as e:
        logger.error(f"  [ERROR] Parse failed: {e}")
        results["sva_log_parse"] = TransformResult(
            status=ResultStatus.ERROR,
            message=f"Parse failed: {e}",
        )
        return results

    # Stage 2: Session summary
    logger.info("[SVA_LOG] Stage 2: Building session summary")
    try:
        sessions = sva_log_transforms.build_session_summary(lf)
        session_count = sessions.select(pl.len()).collect().item()
        logger.info(f"  [OK] {session_count:,} sessions summarized")
        results["sva_log_sessions"] = TransformResult(
            status=ResultStatus.SUCCESS,
            message=f"{session_count:,} sessions",
        )
    except Exception as e:
        logger.error(f"  [ERROR] Session summary failed: {e}")
        results["sva_log_sessions"] = TransformResult(
            status=ResultStatus.ERROR,
            message=str(e),
        )

    # Stage 3: Upload detail
    logger.info("[SVA_LOG] Stage 3: Building upload detail")
    try:
        uploads = sva_log_transforms.build_upload_detail(lf)
        upload_count = uploads.select(pl.len()).collect().item()
        logger.info(f"  [OK] {upload_count:,} file uploads extracted")
        results["sva_log_uploads"] = TransformResult(
            status=ResultStatus.SUCCESS,
            message=f"{upload_count:,} uploads",
        )
    except Exception as e:
        logger.error(f"  [ERROR] Upload detail failed: {e}")
        results["sva_log_uploads"] = TransformResult(
            status=ResultStatus.ERROR,
            message=str(e),
        )

    # Stage 4: Daily summary
    logger.info("[SVA_LOG] Stage 4: Building daily summary")
    try:
        daily = sva_log_transforms.build_daily_summary(lf)
        day_count = daily.select(pl.len()).collect().item()
        logger.info(f"  [OK] {day_count:,} days summarized")
        results["sva_log_daily"] = TransformResult(
            status=ResultStatus.SUCCESS,
            message=f"{day_count:,} days",
        )
    except Exception as e:
        logger.error(f"  [ERROR] Daily summary failed: {e}")
        results["sva_log_daily"] = TransformResult(
            status=ResultStatus.ERROR,
            message=str(e),
        )

    successful = sum(1 for r in results.values() if r.success)
    logger.info(f"[OK] SVA Log Pipeline Complete: {successful}/{len(results)} stages succeeded")

    return results
