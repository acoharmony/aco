# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline orchestration module.

Provides pipeline registry and stage definitions for composing multi-stage
transformation pipelines. All pipelines are executed via the CLI entry point.

Key Components
==============

PipelineRegistry
----------------
Centralized registry for discovering and invoking pipelines by name.
Uses decorator-based registration for automatic discovery.

PipelineStage
-------------
Declarative stage definition referencing transform modules directly.

BronzeStage
-----------
Declarative bronze parsing stage definition.
"""

# Import all pipeline definitions to trigger registration
from . import (  # noqa: F401 (imported for registration side effects)
    _alignment,
    _all,
    _analytics_gold,
    _bronze_all,
    _cclf_bronze,
    _cclf_gold,
    _cclf_silver,
    _high_needs,
    _home_visit_gold,
    _identity_timeline,
    _mx_validate,
    _reference_data,
    _sva_log,
    _wound_care,
    _wound_care_analysis,
)
from ._builder import BronzeStage, PipelineStage
from ._registry import PipelineRegistry, register_pipeline

__all__ = [
    # Registry
    "PipelineRegistry",
    "register_pipeline",
    # Stages
    "PipelineStage",
    "BronzeStage",
]
