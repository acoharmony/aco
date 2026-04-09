# © 2025 HarmonyCares
# All rights reserved.

"""
Base classes for healthcare analytics transforms.

Provides common infrastructure for healthcare analytics transforms,
allowing them to integrate with the acoharmony transform pipeline and
medallion architecture.

Each transform:
- Loads required inputs from gold layer
- Loads required seed/reference data from silver layer
- Executes the corresponding expression logic
- Writes outputs back to gold layer

Enhanced with _decor8 for validation, performance, and tracing.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

import polars as pl

from .._decor8 import (
    explain,
    profile_memory,
    timeit,
    traced,
    validate_args,
    validate_path_exists,
)
from .._expressions import (
    ChronicConditionsExpression,
    CmsHccExpression,
    QualityMeasuresExpression,
    ReadmissionsExpression,
)
from .._log import LogWriter
from .._store import StorageBackend
from ..medallion import MedallionLayer

logger = LogWriter("transforms.base")


@dataclass
class TransformConfig:
    """Base configuration for all healthcare transforms with fluent API."""

    storage: StorageBackend | None = None
    force_refresh: bool = False
    validate_outputs: bool = True
    write_compression: str = "zstd"
    extra_config: dict[str, Any] = field(default_factory=dict)

    def with_storage(self, storage: StorageBackend) -> TransformConfig:
        """Fluent API: Set storage backend."""
        self.storage = storage
        return self

    def with_force_refresh(self, force: bool = True) -> TransformConfig:
        """Fluent API: Enable force refresh."""
        self.force_refresh = force
        return self

    def with_validation(self, validate: bool = True) -> TransformConfig:
        """Fluent API: Enable output validation."""
        self.validate_outputs = validate
        return self

    def with_compression(self, compression: str) -> TransformConfig:
        """Fluent API: Set output compression."""
        self.write_compression = compression
        return self

    def merge(self, **kwargs) -> TransformConfig:
        """Fluent API: Merge additional config."""
        self.extra_config.update(kwargs)
        return self


class HealthcareTransformBase(ABC):
    """
    Base class for all healthcare analytics transforms.

        Provides common functionality and reduces boilerplate:
        - Storage management
        - Path resolution
        - Output writing
        - Validation
        - Performance monitoring
    """

    # Class variables for metadata
    transform_name: ClassVar[str] = "base"
    required_inputs: ClassVar[list[str]] = []
    required_seeds: ClassVar[list[str]] = []
    output_names: ClassVar[list[str]] = []

    def __init__(
        self, storage: StorageBackend | None = None, config: TransformConfig | None = None
    ):
        """
        Initialize transform with optional storage and configuration.

                Args:
                    storage: Storage backend (uses default if not provided)
                    config: Transform configuration (uses default if not provided)
        """
        self.storage = storage or StorageBackend()
        self.config = config or TransformConfig(storage=self.storage)
        self.name = self.transform_name

    @classmethod
    def create(cls, **kwargs) -> HealthcareTransformBase:
        """
        Factory method for creating transforms with fluent config.
        """
        config = TransformConfig(**kwargs)
        return cls(storage=config.storage, config=config)

    @classmethod
    def with_defaults(cls) -> HealthcareTransformBase:
        """Create transform with default settings."""
        return cls()

    def get_gold_path(self) -> Path:
        """Get gold layer path."""
        return self.storage.get_path(MedallionLayer.GOLD)

    def get_silver_path(self) -> Path:
        """Get silver layer path."""
        return self.storage.get_path(MedallionLayer.SILVER)

    def get_input_path(self, filename: str, layer: MedallionLayer = MedallionLayer.GOLD) -> Path:
        """
        Get full path for an input file.

                Args:
                    filename: Name of the file (with extension)
                    layer: Medallion layer (default: GOLD)

                Returns:
                    Full path to the file
        """
        return self.storage.get_path(layer) / filename

    def get_output_path(self, filename: str) -> Path:
        """Get full path for an output file (always in gold)."""
        return self.get_gold_path() / filename

    @validate_args(filename=str)
    @validate_path_exists(param_name="file_path")
    def load_parquet(
        self, filename: str, layer: MedallionLayer = MedallionLayer.GOLD
    ) -> pl.LazyFrame:
        """
        Load a parquet file as LazyFrame with path validation.

                Args:
                    filename: Name of the parquet file
                    layer: Medallion layer to load from

                Returns:
                    LazyFrame loaded from parquet
        """
        file_path = self.get_input_path(filename, layer)
        logger.debug(f"Loading {filename} from {layer.value}", file=str(file_path))
        return pl.scan_parquet(file_path)

    def load_optional_parquet(
        self,
        filename: str,
        layer: MedallionLayer = MedallionLayer.GOLD,
        default_schema: dict | None = None,
    ) -> pl.LazyFrame:
        """
        Load a parquet file, returning empty DataFrame if not found.

                Args:
                    filename: Name of the parquet file
                    layer: Medallion layer to load from
                    default_schema: Schema for empty DataFrame if file not found

                Returns:
                    LazyFrame (empty if file doesn't exist)
        """
        file_path = self.get_input_path(filename, layer)
        if file_path.exists():
            logger.debug(f"Loading {filename} from {layer.value}")
            return pl.scan_parquet(file_path)
        else:
            logger.warning(f"{filename} not found, using empty DataFrame")
            if default_schema is None:
                default_schema = {}
            return pl.DataFrame(schema=default_schema).lazy()

    @timeit(log_level="debug")
    def write_output(self, data: pl.LazyFrame, filename: str) -> Path:
        """
        Write output data to parquet with performance monitoring.

                Args:
                    data: LazyFrame to write
                    filename: Output filename

                Returns:
                    Path to written file
        """
        output_path = self.get_output_path(filename)
        logger.info(f"Writing {filename} to {output_path}")
        data.sink_parquet(output_path, compression=self.config.write_compression)
        return output_path

    def write_outputs(self, outputs: dict[str, pl.LazyFrame]) -> dict[str, Path]:
        """
        Write multiple outputs at once.

                Args:
                    outputs: Dictionary mapping output names to LazyFrames

                Returns:
                    Dictionary mapping output names to file paths
        """
        result = {}
        for name, data in outputs.items():
            filename = f"{name}.parquet"
            result[name] = self.write_output(data, filename)
        return result

    @abstractmethod
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute the transform.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        pass

    def __call__(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """Allow transform to be called directly."""
        return self.execute(config)

    def __repr__(self) -> str:
        """String representation."""
        return f"{self.__class__.__name__}(name='{self.name}', storage={self.storage})"


class CmsHccTransform(HealthcareTransformBase):
    """
    Transform for CMS HCC risk adjustment calculation.

        Inputs (gold):
        - medical_claim.parquet
        - eligibility.parquet

        Seeds (silver):
        - value_sets_cms_hcc_icd_10_cm_mappings.parquet
        - value_sets_cms_hcc_disease_factors.parquet
        - value_sets_cms_hcc_disease_hierarchy.parquet

        Outputs (gold):
        - cms_hcc_patient_risk_factors.parquet
        - cms_hcc_patient_risk_scores.parquet

    """

    transform_name: ClassVar[str] = "cms_hcc"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet", "eligibility.parquet"]
    required_seeds: ClassVar[list[str]] = [
        "value_sets_cms_hcc_icd_10_cm_mappings.parquet",
        "value_sets_cms_hcc_disease_factors.parquet",
        "value_sets_cms_hcc_disease_hierarchy.parquet",
    ]
    output_names: ClassVar[list[str]] = ["risk_factors", "risk_scores"]

    @traced()
    @explain(
        why="CMS HCC transform failed",
        how="Check medical_claim and eligibility data exist, seed data is available, and HCC logic is valid",
        causes=[
            "Missing input data",
            "Missing seed data",
            "HCC calculation error",
            "Invalid config",
        ],
    )
    @timeit(log_level="info", threshold=30.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute CMS HCC risk adjustment.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting CMS HCC risk adjustment transform", transform=self.name)

        logger.info("Loading input data...")
        medical_claims = self.load_parquet("medical_claim.parquet")
        eligibility = self.load_parquet("eligibility.parquet")

        logger.info("Loading seed data...")
        hcc_mapping = self.load_parquet(
            "value_sets_cms_hcc_icd_10_cm_mappings.parquet", MedallionLayer.SILVER
        )
        disease_factors = self.load_parquet(
            "value_sets_cms_hcc_disease_factors.parquet", MedallionLayer.SILVER
        )
        disease_hierarchy = self.load_parquet(
            "value_sets_cms_hcc_disease_hierarchy.parquet", MedallionLayer.SILVER
        )

        expr_config = {
            "patient_id_column": "person_id",
            "diagnosis_column": "diagnosis_code_1",
            "claim_through_date_column": "claim_end_date",
            "age_column": "age",
            "gender_column": "gender",
            "hcc_version": "v28",
            **config,
        }

        logger.info("Calculating patient risk factors...")
        risk_factors = CmsHccExpression.transform_patient_risk_factors(
            medical_claims=medical_claims,
            eligibility=eligibility,
            hcc_mapping=hcc_mapping,
            disease_factors=disease_factors,
            disease_hierarchy=disease_hierarchy,
            config=expr_config,
        )

        logger.info("Calculating patient risk scores...")
        risk_scores = CmsHccExpression.transform_patient_risk_scores(
            patient_risk_factors=risk_factors,
            eligibility=eligibility,
            config=expr_config,
        )

        logger.info("Writing outputs...")
        results = self.write_outputs(
            {
                "cms_hcc_patient_risk_factors": risk_factors,
                "cms_hcc_patient_risk_scores": risk_scores,
            }
        )

        logger.info("CMS HCC transform complete", transform=self.name)
        return results


class ReadmissionsTransform(HealthcareTransformBase):
    """
    Transform for 30-day hospital readmission analysis.

        Inputs (gold):
        - medical_claim.parquet

        Seeds (silver):
        - value_sets_readmissions_acute_diagnosis_icd_10_cm.parquet
        - value_sets_readmissions_planned_readmissions.parquet

        Outputs (gold):
        - readmissions_summary.parquet

    """

    transform_name: ClassVar[str] = "readmissions"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet"]
    required_seeds: ClassVar[list[str]] = [
        "value_sets_readmissions_acute_diagnosis_icd_10_cm.parquet",
        "value_sets_readmissions_planned_readmissions.parquet",
    ]
    output_names: ClassVar[list[str]] = ["readmissions_summary"]

    @traced()
    @explain(
        why="Readmissions transform failed",
        how="Check medical_claim data exists and readmissions logic is valid",
        causes=["Missing input data", "Readmissions calculation error", "Invalid config"],
    )
    @timeit(log_level="info", threshold=20.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute readmissions analysis.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting readmissions analysis transform", transform=self.name)
        logger.info("Loading medical claims...")
        medical_claims = self.load_parquet("medical_claim.parquet")
        encounters = medical_claims.filter(
            pl.col("claim_type").is_in(["institutional"])
            & pl.col("bill_type_code").is_in(["110", "111", "112", "113", "114"])
        ).select(
            [
                pl.col("person_id").alias("patient_id"),
                pl.col("claim_id").alias("encounter_id"),
                pl.lit("inpatient").alias("encounter_type"),
                pl.col("admission_date"),
                pl.col("discharge_date"),
                pl.col("diagnosis_code_1").alias("principal_diagnosis_code"),
            ]
        )
        logger.info("Loading seed data...")
        acute_diagnoses = self.load_optional_parquet(
            "value_sets_readmissions_acute_diagnosis_icd_10_cm.parquet",
            MedallionLayer.SILVER,
            default_schema={"icd_10_cm_code": pl.Utf8, "is_acute": pl.Boolean},
        )
        planned_procedures = self.load_optional_parquet(
            "value_sets_readmissions_planned_readmissions.parquet",
            MedallionLayer.SILVER,
            default_schema={"procedure_code": pl.Utf8, "is_planned": pl.Boolean},
        )
        expr_config = {
            "patient_id_column": "patient_id",
            "admission_date_column": "admission_date",
            "discharge_date_column": "discharge_date",
            "lookback_days": 30,
            **config,
        }
        logger.info("Identifying readmission pairs...")
        readmission_pairs = ReadmissionsExpression.transform_readmission_pairs(
            encounters=encounters,
            acute_diagnoses=acute_diagnoses,
            planned_procedures=planned_procedures,
            config=expr_config,
        )
        results = self.write_outputs({"readmissions_summary": readmission_pairs})
        logger.info("Readmissions transform complete", transform=self.name)
        return results


class ChronicConditionsTransform(HealthcareTransformBase):
    """
    Transform for chronic condition identification.

        Inputs (gold):
        - medical_claim.parquet

        Seeds (silver):
        - value_sets_chronic_conditions_cms_chronic_conditions_hierarchy.parquet

        Outputs (gold):
        - chronic_conditions_long.parquet
        - chronic_conditions_wide.parquet
    """

    transform_name: ClassVar[str] = "chronic_conditions"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet"]
    required_seeds: ClassVar[list[str]] = [
        "value_sets_chronic_conditions_cms_chronic_conditions_hierarchy.parquet"
    ]
    output_names: ClassVar[list[str]] = ["conditions_long", "conditions_wide"]

    @traced()
    @explain(
        why="Chronic conditions transform failed",
        how="Check medical_claim data exists, seed data is available, and chronic conditions logic is valid",
        causes=[
            "Missing input data",
            "Missing seed data",
            "Chronic conditions calculation error",
            "Invalid config",
        ],
    )
    @timeit(log_level="info", threshold=20.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute chronic conditions identification.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting chronic conditions transform", transform=self.name)
        logger.info("Loading medical claims...")
        medical_claims = self.load_parquet("medical_claim.parquet")
        logger.info("Loading condition mapping...")
        condition_mapping = self.load_parquet(
            "value_sets_chronic_conditions_cms_chronic_conditions_hierarchy.parquet",
            MedallionLayer.SILVER,
        )
        expr_config = {
            "patient_id_column": "person_id",
            "diagnosis_column": "diagnosis_code_1",
            "service_date_column": "claim_end_date",
            "min_claims_outpatient": 2,
            **config,
        }
        logger.info("Identifying patient conditions...")
        conditions_long = ChronicConditionsExpression.transform_patient_conditions_long(
            medical_claims=medical_claims,
            condition_mapping=condition_mapping,
            config=expr_config,
        )
        logger.info("Pivoting to wide format...")
        conditions_wide = ChronicConditionsExpression.transform_patient_conditions_wide(
            patient_conditions_long=conditions_long,
            config=expr_config,
        )
        results = self.write_outputs(
            {
                "chronic_conditions_long": conditions_long,
                "chronic_conditions_wide": conditions_wide,
            }
        )
        logger.info("Chronic conditions transform complete", transform=self.name)
        return results


class FinancialPmpmTransform(HealthcareTransformBase):
    """
    Transform for financial PMPM (Per Member Per Month) analysis.

        Enhanced version with granular service category breakdowns and trend analysis.

        Inputs (gold):
        - medical_claim.parquet
        - eligibility.parquet
        - pharmacy_claim.parquet (optional)
        - service_category.parquet (required for enhanced analysis)

        Outputs (gold):
        - financial_pmpm_by_service_category.parquet
        - financial_pmpm_by_service_category_time.parquet
        - financial_pmpm_summary.parquet
    """

    transform_name: ClassVar[str] = "financial_pmpm"
    required_inputs: ClassVar[list[str]] = [
        "medical_claim.parquet",
        "eligibility.parquet",
        "service_category.parquet",
    ]
    output_names: ClassVar[list[str]] = [
        "pmpm_by_service_category",
        "pmpm_by_service_category_time",
        "pmpm_summary",
    ]

    @traced()
    @explain(
        why="Financial PMPM transform failed",
        how="Check medical_claim data exists and PMPM calculation logic is valid",
        causes=["Missing input data", "PMPM calculation error", "Invalid config"],
    )
    @timeit(log_level="info", threshold=20.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute financial PMPM analysis.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting enhanced financial PMPM transform", transform=self.name)
        logger.info("Loading claims and eligibility...")
        service_category = self.load_parquet("service_category.parquet")
        eligibility = self.load_parquet("eligibility.parquet")
        logger.info("Calculating member months...")
        member_months = (
            eligibility.with_columns(
                [
                    pl.col("enrollment_start_date").dt.strftime("%Y-%m").alias("year_month"),
                ]
            )
            .group_by(["person_id", "year_month"])
            .agg([pl.len().alias("member_months")])
        )

        total_member_months = member_months.select(pl.sum("member_months")).collect().item()
        logger.info(f"Total member months: {total_member_months:,}")

        logger.info("Calculating spend by service category...")
        service_spend = service_category.with_columns(
            [pl.col("claim_end_date").dt.strftime("%Y-%m").alias("year_month")]
        )

        pmpm_by_category = (
            service_spend.group_by(["service_category_1", "service_category_2"])
            .agg(
                [
                    pl.sum("paid").alias("total_paid"),
                    pl.len().alias("claim_count"),
                    pl.n_unique("person_id").alias("member_count"),
                ]
            )
            .with_columns([(pl.col("total_paid") / pl.lit(total_member_months)).alias("pmpm")])
            .sort("total_paid", descending=True)
        )

        pmpm_by_category_time = (
            service_spend.group_by(["year_month", "service_category_1", "service_category_2"])
            .agg(
                [
                    pl.sum("paid").alias("total_paid"),
                    pl.len().alias("claim_count"),
                    pl.n_unique("person_id").alias("member_count"),
                ]
            )
            .join(
                member_months.group_by("year_month").agg(pl.sum("member_months")),
                on="year_month",
                how="left",
            )
            .with_columns([(pl.col("total_paid") / pl.col("member_months")).alias("pmpm")])
            .sort(["year_month", "total_paid"], descending=[False, True])
        )

        pmpm_summary = (
            service_spend.group_by("year_month")
            .agg(
                [
                    pl.sum("paid").alias("total_paid"),
                    pl.len().alias("total_claims"),
                    pl.n_unique("person_id").alias("unique_members"),
                ]
            )
            .join(
                member_months.group_by("year_month").agg(pl.sum("member_months")),
                on="year_month",
                how="left",
            )
            .with_columns(
                [
                    (pl.col("total_paid") / pl.col("member_months")).alias("pmpm"),
                    (pl.col("total_claims") / pl.col("member_months")).alias(
                        "claims_per_member_per_month"
                    ),
                ]
            )
            .sort("year_month")
        )

        logger.info("Writing PMPM outputs...")
        results = self.write_outputs(
            {
                "financial_pmpm_by_service_category": pmpm_by_category,
                "financial_pmpm_by_service_category_time": pmpm_by_category_time,
                "financial_pmpm_summary": pmpm_summary,
            }
        )

        logger.info("Enhanced financial PMPM transform complete", transform=self.name)
        return results


class QualityMeasuresTransform(HealthcareTransformBase):
    """
    Transform for quality measures calculation.

        Inputs (gold):
        - medical_claim.parquet
        - eligibility.parquet
        - pharmacy_claim.parquet (optional)

        Seeds (silver):
        - value_sets_clinical_concepts_*.parquet (various quality measure value sets)

        Outputs (gold):
        - quality_measures_summary.parquet
    """

    transform_name: ClassVar[str] = "quality_measures"
    required_inputs: ClassVar[list[str]] = ["medical_claim.parquet", "eligibility.parquet"]
    output_names: ClassVar[list[str]] = ["quality_measures_summary"]

    @traced()
    @explain(
        why="Quality measures transform failed",
        how="Check medical_claim data exists and quality measures logic is valid",
        causes=["Missing input data", "Quality measures calculation error", "Invalid config"],
    )
    @timeit(log_level="info", threshold=20.0)
    @profile_memory(log_result=True)
    @validate_args(config=(dict, type(None)))
    def execute(self, config: dict[str, Any] | None = None) -> dict[str, Path]:
        """
        Execute quality measures calculation.

                Args:
                    config: Optional configuration overrides

                Returns:
                    Dictionary mapping output names to file paths
        """
        if config is None:
            config = {}

        logger.info("Starting quality measures transform", transform=self.name)
        logger.info("Loading medical claims and eligibility...")
        medical_claims = self.load_parquet("medical_claim.parquet")
        eligibility = self.load_parquet("eligibility.parquet")
        pharmacy_claims = self.load_optional_parquet(
            "pharmacy_claim.parquet",
            default_schema={"person_id": pl.Utf8, "claim_end_date": pl.Date},
        )
        if pharmacy_claims.collect().height > 0:
            pharmacy_claims = pharmacy_claims.with_columns(
                [pl.col("dispensing_date").alias("claim_end_date")]
            )
        value_sets = {}
        expr_config = {
            "patient_id_column": "person_id",
            "diagnosis_column": "diagnosis_code_1",
            "procedure_column": "procedure_code_1",
            "service_date_column": "claim_end_date",
            "measurement_year": config.get("measurement_year", 2024),
            "measure_name": config.get("measure_name", "All Measures"),
            **config,
        }
        logger.info("Calculating quality measure summary...")
        measure_summary = QualityMeasuresExpression.transform_measure_summary(
            medical_claims=medical_claims,
            pharmacy_claims=pharmacy_claims,
            eligibility=eligibility,
            value_sets=value_sets,
            config=expr_config,
        )
        results = self.write_outputs({"quality_measures_summary": measure_summary})

        logger.info("Quality measures transform complete", transform=self.name)
        return results


def run_transform(
    transform_class: type[HealthcareTransformBase], config: dict[str, Any] | None = None, **kwargs
) -> dict[str, Path]:
    """
    Convenience function to create and run a transform in one call.

        Args:
            transform_class: The transform class to run
            config: Optional configuration for execute()
            **kwargs: Keyword arguments passed to transform constructor

        Returns:
            Dictionary mapping output names to file paths

    """
    transform = transform_class.create(**kwargs)
    return transform.execute(config)


def run_all_healthcare_transforms(
    storage: StorageBackend | None = None, **config
) -> dict[str, dict[str, Path]]:
    """
    Run all healthcare transforms in sequence.

        Args:
            storage: Optional storage backend
            **config: Configuration passed to all transforms

        Returns:
            Dictionary mapping transform names to their output paths

    """
    transforms = [
        CmsHccTransform,
        ReadmissionsTransform,
        ChronicConditionsTransform,
        FinancialPmpmTransform,
        QualityMeasuresTransform,
    ]

    results = {}
    for transform_class in transforms:
        transform = transform_class(storage=storage)
        logger.info(f"Running {transform.name} transform...")
        results[transform.name] = transform.execute(config)

    return results


class HealthcareTransformContext:
    """
    Context manager for running transforms with automatic cleanup and error handling.

    """

    def __init__(self, transform_class: type[HealthcareTransformBase], **kwargs):
        """
        Initialize context manager.

                Args:
                    transform_class: The transform class to use
                    **kwargs: Keyword arguments passed to transform constructor
        """
        self.transform_class = transform_class
        self.kwargs = kwargs
        self.transform = None

    def __enter__(self) -> HealthcareTransformBase:
        """Enter context and create transform."""
        self.transform = self.transform_class.create(**self.kwargs)
        logger.info(f"Entering transform context: {self.transform.name}")
        return self.transform

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Exit context with cleanup."""
        if exc_type is not None:
            logger.error(f"Transform {self.transform.name} failed: {exc_val}")
        else:
            logger.info(f"Transform {self.transform.name} completed successfully")
        # Could add cleanup logic here if needed
        return False  # Don't suppress exceptions


__all__ = [
    "TransformConfig",
    "HealthcareTransformBase",
    "CmsHccTransform",
    "ReadmissionsTransform",
    "ChronicConditionsTransform",
    "FinancialPmpmTransform",
    "QualityMeasuresTransform",
    "run_transform",
    "run_all_healthcare_transforms",
    "HealthcareTransformContext",
]
