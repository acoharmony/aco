# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline-related exceptions.

Exceptions for pipeline execution, DAG processing,
and workflow management.
"""

from __future__ import annotations

from ._base import ACOHarmonyException
from ._registry import register_exception


@register_exception(
    error_code="PIPELINE_001",
    category="pipeline",
    why_template="Pipeline execution failed",
    how_template="Check pipeline configuration and dependencies",
)
class PipelineError(ACOHarmonyException):
    """Base class for pipeline errors."""

    pass


@register_exception(
    error_code="PIPELINE_002",
    category="pipeline",
    why_template="Pipeline stage failed",
    how_template="Check stage configuration and input data",
)
class StageError(ACOHarmonyException):
    """Raised when a pipeline stage fails."""

    pass


@register_exception(
    error_code="PIPELINE_003",
    category="pipeline",
    why_template="Pipeline dependency not met",
    how_template="Ensure all required upstream stages completed successfully",
)
class DependencyError(ACOHarmonyException):
    """Raised when pipeline dependencies are not met."""

    pass
