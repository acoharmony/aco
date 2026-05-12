# © 2025 HarmonyCares
# All rights reserved.

"""Meta-pipeline that runs every registered pipeline in dependency order.

Tier order:
    1. bronze_all              - parse all raw inputs (CCLF + non-CCLF) to bronze parquet
    2. reference_data          - Tuva seed reference tables (used by gold)
    3. cclf_silver             - dedupe/normalize bronze CCLF into silver
    4. identity_timeline       - beneficiary identity resolution off silver
    5. alignment               - assignment/alignment derivations off silver
    6. cclf_gold               - gold CCLF outputs
    7. home_visit_gold         - home visit claims gold
    8. preferred_providers     - preferred-provider claims attribution
                                 (joins silver/participant_list × gold/medical_claim)
    9. analytics_gold          - post-Tuva analytics (consumes other gold + reference)
   10. high_needs              - domain analysis on gold
   11. wound_care              - domain analysis on gold
   12. wound_care_analysis     - downstream of wound_care
   13. sva_log                 - SVA log processing (independent)

cclf_bronze is intentionally skipped: bronze_all already includes its stages.
"""

from typing import Any

from ..result import PipelineResult, ResultStatus, TransformResult
from ._registry import register_pipeline

PIPELINE_ORDER = [
    "bronze_all",
    "reference_data",
    "cclf_silver",
    "identity_timeline",
    "alignment",
    "cclf_gold",
    "home_visit_gold",
    "preferred_providers",
    "analytics_gold",
    "high_needs",
    "wound_care",
    "wound_care_analysis",
    "sva_log",
]


@register_pipeline(name="all")
def apply_all_pipeline(
    executor: Any, logger: Any, force: bool = False
) -> dict[str, TransformResult]:
    """Run every registered pipeline in dependency order.

    Returns a flat dict of "{pipeline}:{stage}" -> TransformResult so the
    standard pipeline summary surfaces every child stage with provenance.
    A pipeline that fails is logged and reported as a single SKIPPED entry,
    but does not abort the meta-pipeline — downstream pipelines still run.
    """
    results: dict[str, TransformResult] = {}

    logger.info(f"Starting full pipeline run: {len(PIPELINE_ORDER)} pipelines")
    if force:
        logger.info("⚠ FORCE MODE: Reprocessing all stages regardless of tracking state")
    logger.info("=" * 80)

    for pipeline_name in PIPELINE_ORDER:
        logger.info(f"━━━ {pipeline_name} ━━━")
        try:
            child: PipelineResult = executor.run_pipeline(pipeline_name, force=force)
        except Exception as e:  # ALLOWED: child failure must not abort the meta-pipeline
            logger.error(f"{pipeline_name} crashed: {e}")
            results[f"{pipeline_name}:<error>"] = TransformResult(
                status=ResultStatus.FAILURE,
                message=f"{pipeline_name} crashed: {e}",
                errors=[str(e)],
            )
            continue

        if child.data:
            for i, stage_result in enumerate(child.data, 1):
                key = f"{pipeline_name}:stage_{i:02d}"
                results[key] = stage_result
        else:
            results[f"{pipeline_name}:<empty>"] = TransformResult(
                status=ResultStatus.SKIPPED,
                message=f"{pipeline_name} returned no stages",
            )

    logger.info("=" * 80)
    successful = sum(1 for r in results.values() if r.success)
    skipped = sum(1 for r in results.values() if r.status == ResultStatus.SKIPPED)
    failed = sum(
        1 for r in results.values()
        if not r.success and r.status != ResultStatus.SKIPPED
    )
    logger.info(
        f"[OK] All Pipelines Complete: {successful} ok, {skipped} skipped, {failed} failed"
    )

    return results
