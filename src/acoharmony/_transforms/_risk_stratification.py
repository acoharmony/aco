# © 2025 HarmonyCares
# All rights reserved.

"""
Member Risk Stratification Transform.

Provides comprehensive member risk stratification:
- Clinical risk (RAF, chronic conditions, age)
- Utilization risk (admissions, ED, readmissions)
- Cost risk (PMPM, high-cost tiers)
- Composite risk tiers for care management prioritization
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, ClassVar

from .._decor8 import explain, profile_memory, timeit, traced, validate_args
from .._expressions import RiskStratificationExpression
from .._log import LogWriter
from ._base import HealthcareTransformBase, TransformConfig

logger = LogWriter("transforms.risk_stratification")


class RiskStratificationTransform(HealthcareTransformBase):
    """
    Comprehensive member risk stratification and prioritization.

        Stratifies members across multiple dimensions:
        - Clinical: RAF scores, chronic conditions, age/gender
        - Utilization: Admissions, ED visits, readmissions
        - Cost: PMPM, high-cost tiers
        - Composite: Weighted risk score and tiering

        Risk Tiers:
        - Critical: Composite score >= 4.5 (highest priority)
        - High: Score >= 3.5
        - Medium: Score >= 2.5
        - Low: Score >= 1.5
        - Minimal: Score < 1.5

        Inputs (gold):
            - eligibility.parquet (required)
            - hcc_raf_scores.parquet (optional)
            - chronic_conditions_member.parquet (optional)
            - admissions_all.parquet (optional)
            - readmissions_pairs.parquet (optional)
            - tcoc_member_level.parquet (optional)

        Outputs (gold):
            - risk_member_scores.parquet: All risk dimensions by member
            - risk_tier_summary.parquet: Summary stats by risk tier
            - risk_high_risk_members.parquet: Critical + high tier members
            - risk_composite.parquet: Composite scores and tiers only
    """

    transform_name: ClassVar[str] = "risk_stratification"
    required_inputs: ClassVar[list[str]] = ["eligibility.parquet"]
    required_seeds: ClassVar[list[str]] = []
    output_names: ClassVar[list[str]] = [
        "risk_member_scores",
        "risk_tier_summary",
        "risk_high_risk_members",
        "risk_composite",
    ]

    @traced()
    @explain(
        why="Risk stratification transform failed",
        how="Check eligibility data is available; other inputs are optional",
        causes=["Missing eligibility data", "Data format issues", "Risk calculation error"],
    )
    @timeit(log_level="info", threshold=60.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute member risk stratification.

                Args:
                    config: Optional configuration dict

                Returns:
                    Dictionary mapping output names to file paths
        """
        cfg = self.get_config(config)

        logger.info("Starting member risk stratification")

        # Load required data
        eligibility = self.load_parquet("eligibility.parquet")

        # Load optional inputs (lazy — only reads metadata, not full data)
        hcc_raf = self.load_optional_parquet("hcc_raf_scores.parquet")
        if hcc_raf.collect_schema().len() == 0:
            hcc_raf = None
            logger.info("HCC RAF scores not available, using age-based risk only")
        else:
            logger.info("Loaded HCC RAF scores")

        chronic_conditions = self.load_optional_parquet("chronic_conditions_member.parquet")
        if chronic_conditions.collect_schema().len() == 0:
            chronic_conditions = None
            logger.info("Chronic conditions not available")
        else:
            logger.info("Loaded chronic conditions")

        admissions = self.load_optional_parquet("admissions_all.parquet")
        if admissions.collect_schema().len() == 0:
            admissions = None
            logger.info("Admissions data not available")
        else:
            logger.info("Loaded admissions data")

        readmissions = self.load_optional_parquet("readmissions_pairs.parquet")
        if readmissions.collect_schema().len() == 0:
            readmissions = None
            logger.info("Readmissions data not available")
        else:
            logger.info("Loaded readmissions data")

        tcoc = self.load_optional_parquet("tcoc_member_level.parquet")
        if tcoc.collect_schema().len() == 0:
            tcoc = None
            logger.info("TCOC data not available")
        else:
            logger.info("Loaded total cost of care data")

        member_risk_scores, risk_tier_summary, high_risk_members, composite_risk = (
            RiskStratificationExpression.stratify_member_risk(
                eligibility, hcc_raf, chronic_conditions, admissions, readmissions, tcoc, cfg
            )
        )

        output_paths = {}
        output_paths["risk_member_scores"] = self.write_output(
            member_risk_scores, "risk_member_scores"
        )
        output_paths["risk_tier_summary"] = self.write_output(
            risk_tier_summary, "risk_tier_summary"
        )
        output_paths["risk_high_risk_members"] = self.write_output(
            high_risk_members, "risk_high_risk_members"
        )
        output_paths["risk_composite"] = self.write_output(composite_risk, "risk_composite")

        tier_summary = risk_tier_summary.collect()
        if tier_summary.height > 0:
            logger.info("Risk Stratification Summary:")
            total_members = tier_summary["member_count"].sum()
            logger.info(f"  Total members stratified: {total_members:,}")

            for row in tier_summary.iter_rows(named=True):
                tier = row["risk_tier"]
                count = row["member_count"]
                pct = (count / total_members * 100) if total_members > 0 else 0
                avg_score = row["avg_composite_score"]
                logger.info(
                    f"  {tier.upper()}: {count:,} members ({pct:.1f}%), avg score: {avg_score:.2f}"
                )

        high_risk_stats = high_risk_members.collect()
        if high_risk_stats.height > 0:
            logger.info(f"High-risk members (critical + high): {high_risk_stats.height:,}")

        logger.info("Risk stratification complete")
        return output_paths


def create_risk_stratification_transform(
    bronze_path: Path, silver_path: Path, gold_path: Path
) -> RiskStratificationTransform:
    """Create RiskStratificationTransform instance."""
    config = TransformConfig.create(bronze_path, silver_path, gold_path)
    return RiskStratificationTransform(config)


logger.info("Risk Stratification Transform initialized")
