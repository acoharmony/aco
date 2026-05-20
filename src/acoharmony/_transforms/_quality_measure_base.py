# © 2025 HarmonyCares
# All rights reserved.

"""
Base classes and framework for quality measure calculation.

 the foundational infrastructure for implementing
healthcare quality measures. All quality measures inherit from these base
classes to ensure consistent calculation patterns.

The framework supports:
- Numerator/denominator calculation
- Exclusions and exceptions
- Stratification (age, gender, etc.)
- Multi-year measurement periods
- HEDIS, NQF, PQA, and MIPS measures
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

import polars as pl

from .._decor8 import explain, timeit, traced
from .._log import LogWriter

logger = LogWriter("transforms.quality_measure_base")


@dataclass
class MeasureMetadata:
    """Metadata for a quality measure."""

    measure_id: str
    measure_name: str
    measure_steward: str  # NCQA, NQF, PQA, MIPS
    measure_version: str
    description: str
    numerator_description: str
    denominator_description: str
    exclusions_description: str | None = None


class QualityMeasureBase(ABC):
    """
    Base class for all quality measures.

        All quality measures follow this pattern:
        1. Define eligible population (denominator)
        2. Identify those meeting the quality criteria (numerator)
        3. Apply exclusions/exceptions
        4. Calculate rate = numerator / (denominator - exclusions)

        Subclasses must implement:
        - get_metadata(): Return measure metadata
        - calculate_denominator(): Identify eligible population
        - calculate_numerator(): Identify those meeting criteria
        - calculate_exclusions(): Identify exclusions (optional)
    """

    def __init__(self, config: dict[str, Any] | None = None):
        """
        Initialize quality measure.

                Args:
                    config: Configuration dict with measurement parameters
        """
        self.config = config or {}
        self.metadata = self.get_metadata()

    @abstractmethod
    def get_metadata(self) -> MeasureMetadata:
        """
        Get measure metadata.

                Returns:
                    MeasureMetadata instance with measure information
        """
        pass

    @abstractmethod
    def calculate_denominator(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate measure denominator (eligible population).

                Args:
                    claims: Medical/pharmacy claims data
                    eligibility: Member eligibility/enrollment data
                    value_sets: Dictionary of value sets for this measure

                Returns:
                    LazyFrame with person_id and denominator_flag=True
        """
        pass

    @abstractmethod
    def calculate_numerator(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate measure numerator (those meeting quality criteria).

                Args:
                    denominator: LazyFrame from calculate_denominator()
                    claims: Medical/pharmacy claims data
                    value_sets: Dictionary of value sets for this measure

                Returns:
                    LazyFrame with person_id and numerator_flag=True
        """
        pass

    def calculate_exclusions(
        self,
        denominator: pl.LazyFrame,
        claims: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate measure exclusions (optional).

                Override this method if measure has exclusions.

                Args:
                    denominator: LazyFrame from calculate_denominator()
                    claims: Medical/pharmacy claims data
                    value_sets: Dictionary of value sets for this measure

                Returns:
                    LazyFrame with person_id and exclusion_flag=True
        """
        # Default: no exclusions
        return denominator.select(pl.col("person_id")).with_columns(
            [pl.lit(False).alias("exclusion_flag")]
        )

    @traced()
    @explain(
        why="Quality measure calculation failed",
        how="Check claims data and value sets are available",
        causes=["Missing data", "Invalid value sets", "Calculation error"],
    )
    @timeit(log_level="info", threshold=30.0)
    def calculate(
        self,
        claims: pl.LazyFrame,
        eligibility: pl.LazyFrame,
        value_sets: dict[str, pl.LazyFrame],
    ) -> pl.LazyFrame:
        """
        Calculate the complete quality measure.

                This orchestrates the full measure calculation:
                1. Calculate denominator
                2. Calculate numerator
                3. Calculate exclusions
                4. Compute final rate

                Args:
                    claims: Medical/pharmacy claims data
                    eligibility: Member eligibility data
                    value_sets: Dictionary of value sets for this measure

                Returns:
                    LazyFrame with measure results including:
                    - person_id
                    - measure_id
                    - measure_name
                    - denominator_flag
                    - numerator_flag
                    - exclusion_flag
                    - performance_met (numerator after exclusions)
        """
        logger.info(
            f"Calculating measure: {self.metadata.measure_id} - {self.metadata.measure_name}"
        )

        # Calculate denominator
        logger.debug("Calculating denominator...")
        denominator = self.calculate_denominator(claims, eligibility, value_sets)

        # Calculate numerator
        logger.debug("Calculating numerator...")
        numerator = self.calculate_numerator(denominator, claims, value_sets)

        # Calculate exclusions
        logger.debug("Calculating exclusions...")
        exclusions = self.calculate_exclusions(denominator, claims, value_sets)

        # Combine results
        results = (
            denominator.join(numerator, on="person_id", how="left")
            .join(exclusions, on="person_id", how="left")
            .with_columns(
                [
                    pl.lit(self.metadata.measure_id).alias("measure_id"),
                    pl.lit(self.metadata.measure_name).alias("measure_name"),
                    pl.col("numerator_flag").fill_null(False),
                    pl.col("exclusion_flag").fill_null(False),
                ]
            )
        )

        # Calculate performance_met (numerator among non-excluded)
        results = results.with_columns(
            [(pl.col("numerator_flag") & ~pl.col("exclusion_flag")).alias("performance_met")]
        )

        logger.info(f"Measure {self.metadata.measure_id} calculation complete")

        return results

    @staticmethod
    def calculate_summary(measure_results: pl.LazyFrame) -> pl.LazyFrame:
        """
        Calculate summary statistics for a measure.

                Args:
                    measure_results: LazyFrame from calculate()

                Returns:
                    LazyFrame with summary statistics:
                    - measure_id
                    - measure_name
                    - denominator_count
                    - numerator_count
                    - exclusion_count
                    - performance_count
                    - performance_rate
        """
        summary = measure_results.group_by(["measure_id", "measure_name"]).agg(
            [
                pl.sum("denominator_flag").alias("denominator_count"),
                pl.sum("numerator_flag").alias("numerator_count"),
                pl.sum("exclusion_flag").alias("exclusion_count"),
                pl.sum("performance_met").alias("performance_count"),
            ]
        )

        # Calculate rate
        summary = summary.with_columns(
            [
                (
                    pl.col("performance_count").cast(pl.Float64)
                    / (pl.col("denominator_count") - pl.col("exclusion_count")).cast(pl.Float64)
                )
                .alias("performance_rate")
                .fill_nan(0.0)
            ]
        )

        return summary


class MeasureFactory:
    """
    Factory for creating quality measure instances.

        Maintains a registry of available measures and creates instances on demand.
    """

    _registry: dict[str, type[QualityMeasureBase]] = {}

    @classmethod
    def register(cls, measure_id: str, measure_class: type[QualityMeasureBase]):
        """
        Register a quality measure class.

                Args:
                    measure_id: Unique measure identifier (e.g., 'NQF0059')
                    measure_class: Class that implements QualityMeasureBase
        """
        cls._registry[measure_id] = measure_class
        logger.debug(f"Registered quality measure: {measure_id}")

    @classmethod
    def create(cls, measure_id: str, config: dict[str, Any] | None = None) -> QualityMeasureBase:
        """
        Create a quality measure instance.

                Args:
                    measure_id: Measure identifier
                    config: Configuration dict

                Returns:
                    Instance of the requested measure

                Raises:
                    KeyError: If measure_id not registered
        """
        if measure_id not in cls._registry:
            available = list(cls._registry.keys())
            raise KeyError(f"Measure '{measure_id}' not registered. Available: {available}")

        measure_class = cls._registry[measure_id]
        return measure_class(config)

    @classmethod
    def list_measures(cls) -> list[str]:
        """Get list of registered measure IDs."""
        return list(cls._registry.keys())
