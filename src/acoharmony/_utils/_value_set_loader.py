# © 2025 HarmonyCares
# All rights reserved.

"""
Value Set Loader for quality measures.

 utilities for loading and managing value sets used in
quality measure calculation. Value sets map clinical concepts (like "Diabetes Diagnosis")
to actual codes (ICD-10, CPT, HCPCS, NDC, etc.).
"""

from __future__ import annotations

from pathlib import Path

import polars as pl

from .._decor8 import timeit, traced
from .._log import LogWriter

logger = LogWriter("utils.value_set_loader")


class ValueSetLoader:
    """
    Loader for quality measure value sets.

        Manages loading and caching of value sets from silver layer.
        Value sets map concepts to codes across multiple code systems.
    """

    def __init__(self, silver_path: Path):
        """
        Initialize value set loader.

                Args:
                    silver_path: Path to silver layer containing value sets
        """
        self.silver_path = silver_path
        self._cache: dict[str, pl.LazyFrame] = {}
        self._value_sets_df: pl.LazyFrame | None = None
        self._concepts_df: pl.LazyFrame | None = None
        self._measures_df: pl.LazyFrame | None = None

    @traced()
    @timeit(log_level="debug")
    def load_value_sets_catalog(self) -> pl.LazyFrame:
        """
        Load the value sets catalog (52,101 value set codes).

                Returns:
                    LazyFrame with columns:
                    - concept_name: Clinical concept (e.g., "Diabetes Diagnosis")
                    - concept_oid: OID for the concept
                    - code: The actual code (ICD-10, CPT, etc.)
                    - code_system: Code system (ICD-10-CM, CPT, HCPCS, etc.)
        """
        if self._value_sets_df is None:
            file_path = self.silver_path / "value_sets_quality_measures_value_sets.parquet"
            logger.info(f"Loading value sets catalog from {file_path}")
            self._value_sets_df = pl.scan_parquet(file_path)
            logger.info("Value sets catalog loaded (52,101 codes)")

        return self._value_sets_df

    @traced()
    @timeit(log_level="debug")
    def load_concepts_catalog(self) -> pl.LazyFrame:
        """
        Load the concepts catalog (372 clinical concepts).

                Returns:
                    LazyFrame with columns:
                    - concept_name: Clinical concept name
                    - concept_oid: OID for the concept
                    - measure_id: Measure this concept belongs to
                    - measure_name: Measure name
        """
        if self._concepts_df is None:
            file_path = self.silver_path / "value_sets_quality_measures_concepts.parquet"
            logger.info(f"Loading concepts catalog from {file_path}")
            self._concepts_df = pl.scan_parquet(file_path)
            logger.info("Concepts catalog loaded (372 concepts)")

        return self._concepts_df

    @traced()
    @timeit(log_level="debug")
    def load_measures_catalog(self) -> pl.LazyFrame:
        """
        Load the measures catalog (180 measures).

                Returns:
                    LazyFrame with columns:
                    - id: Measure ID
                    - name: Measure name
                    - description: Measure description
                    - version: Measure version
                    - steward: Steward (NCQA, NQF, PQA, MIPS)
        """
        if self._measures_df is None:
            file_path = self.silver_path / "value_sets_quality_measures_measures.parquet"
            logger.info(f"Loading measures catalog from {file_path}")
            self._measures_df = pl.scan_parquet(file_path)
            logger.info("Measures catalog loaded (180 measures)")

        return self._measures_df

    @traced()
    @timeit(log_level="debug")
    def get_value_set_for_concept(
        self, concept_name: str, code_system: str | None = None
    ) -> pl.LazyFrame:
        """
        Get all codes for a specific clinical concept.

                Args:
                    concept_name: Name of the concept (e.g., "Diabetes Diagnosis")
                    code_system: Optional filter by code system (e.g., "ICD-10-CM")

                Returns:
                    LazyFrame with codes for this concept
        """
        cache_key = f"{concept_name}_{code_system or 'all'}"

        if cache_key not in self._cache:
            logger.debug(f"Loading value set for concept: {concept_name}")

            value_sets = self.load_value_sets_catalog()
            codes = value_sets.filter(pl.col("concept_name") == concept_name)

            if code_system:
                codes = codes.filter(pl.col("code_system") == code_system)

            self._cache[cache_key] = codes

        return self._cache[cache_key]

    @traced()
    @timeit(log_level="debug")
    def get_concepts_for_measure(self, measure_id: str) -> list[str]:
        """
        Get all concept names for a specific measure.

                Args:
                    measure_id: Measure identifier (e.g., "NQF0059")

                Returns:
                    List of concept names for this measure
        """
        concepts = self.load_concepts_catalog()
        measure_concepts = (
            concepts.filter(pl.col("measure_id") == measure_id)
            .select("concept_name")
            .unique()
            .collect()
        )

        return measure_concepts["concept_name"].to_list()

    @traced()
    @timeit(log_level="debug")
    def load_value_sets_for_measure(self, measure_id: str) -> dict[str, pl.LazyFrame]:
        """
        Load all value sets needed for a specific measure.

                This is the main method used by quality measures to get all their value sets.

                Args:
                    measure_id: Measure identifier (e.g., "NQF0059")

                Returns:
                    Dictionary mapping concept names to their value sets
        """
        logger.info(f"Loading value sets for measure: {measure_id}")

        # Get all concepts for this measure
        concept_names = self.get_concepts_for_measure(measure_id)
        logger.debug(f"Found {len(concept_names)} concepts for {measure_id}")

        # Load value sets for each concept
        value_sets = {}
        for concept_name in concept_names:
            value_sets[concept_name] = self.get_value_set_for_concept(concept_name)

        logger.info(f"Loaded {len(value_sets)} value sets for measure {measure_id}")

        return value_sets

    def clear_cache(self):
        """Clear the value set cache."""
        self._cache.clear()
        logger.debug("Value set cache cleared")
