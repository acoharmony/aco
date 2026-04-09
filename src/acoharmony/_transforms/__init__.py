# © 2025 HarmonyCares
# All rights reserved.

"""
Data transformation system for ACOHarmony.

Provides a decorator-based framework for transforming healthcare data
through various stages of processing. All transforms are registered via
decorators and orchestrate LazyFrame operations.

Key Components:
    - TransformRegistry: Registry of all transforms
    - @transform: Decorator for registering transforms
    - @register_crosswalk: Specialized decorator for crosswalk operations
    - @register_pipeline: Decorator for pipeline orchestration

Transform Architecture:
    - All transforms take pl.LazyFrame and return pl.LazyFrame
    - Transforms handle data orchestration (joins, filters, aggregations)
    - Registered via decorators at module import time
    - No class-based routing - direct decorator registration

Available Transforms:
    - Crosswalk operations (MBI mapping, enterprise crosswalk)
    - Intermediate tables (deduplication, standardization)
    - Quality measures (HEDIS, NQF, PQA, MIPS)
    - Analytics (admissions, utilization, financial, risk stratification)

Note:
    The module uses Polars LazyFrames for efficient lazy evaluation
    of large datasets. All transforms follow medallion architecture
    (Bronze → Silver → Gold).
"""

# Import core components for public API
# Import transforms to register them
from . import (  # noqa: F401 (imported for registration side effects)
    _bnex,
    _crosswalk,
    _enterprise_xwalk,
    _quality_acr,
    _quality_cardiovascular,
    _quality_diabetes,
    _quality_medication_adherence,
    _quality_preventive,
    _quality_uamcc,
    _skin_substitute_claims,
    _standardization,
    _sva_log,
    _voluntary_alignment,
    _wound_care_claims,
    _wound_care_clustered,
    _wound_care_duplicates,
    _wound_care_high_cost,
    _wound_care_high_frequency,
    _wound_care_identical_patterns,
)

# Import with aliases for pipeline use
from . import _skin_substitute_claims as skin_substitute_claims
from . import _wound_care_claims as wound_care_claims
from . import _wound_care_clustered as wound_care_clustered
from . import _wound_care_duplicates as wound_care_duplicates
from . import _wound_care_high_cost as wound_care_high_cost
from . import _wound_care_high_frequency as wound_care_high_frequency
from . import _wound_care_identical_patterns as wound_care_identical_patterns

# Import quality measure framework
from ._quality_measure_base import MeasureFactory, MeasureMetadata, QualityMeasureBase

# Import convenience decorators
from ._registry import (
    TransformRegistry,
    register_crosswalk,
    register_pipeline,
)

# Public API
__all__ = [
    # Transform modules (imported for side effects)
    "_bnex",
    "_crosswalk",
    "_enterprise_xwalk",
    "_quality_acr",
    "_quality_uamcc",
    "_quality_cardiovascular",
    "_quality_diabetes",
    "_quality_medication_adherence",
    "_quality_preventive",
    "_standardization",
    "_sva_log",
    "skin_substitute_claims",
    "wound_care_claims",
    "wound_care_high_frequency",
    "wound_care_high_cost",
    "wound_care_clustered",
    "wound_care_duplicates",
    "wound_care_identical_patterns",
    # Core classes
    "TransformRegistry",
    # Quality measure framework
    "QualityMeasureBase",
    "MeasureMetadata",
    "MeasureFactory",
    # Registration decorators
    "register_crosswalk",
    "register_pipeline",
]
