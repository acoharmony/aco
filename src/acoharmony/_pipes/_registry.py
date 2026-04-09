# © 2025 HarmonyCares
# All rights reserved.

"""
Pipeline registry for managing and discovering pipeline definitions.

Provides a decorator-based registry pattern for registering pipeline functions,
making them discoverable and maintainable while keeping implementation details
in private modules.

What is the Pipeline Registry?
================================

The Pipeline Registry is a **centralized catalog of pipeline definitions** that
enables dynamic discovery, lookup, and invocation of multi-stage transformation
pipelines without tight coupling between modules. It uses Python decorators to
register pipelines at module import time, creating a plugin-like architecture.

Key Concepts
============

Pipeline vs Transform
---------------------

- **Transform**: Single operation on a LazyFrame (dedupe, standardize, etc.)
- **Pipeline**: Orchestrated sequence of transforms with dependencies

The PipelineRegistry manages pipelines, while TransformRegistry manages transforms.
Pipelines reference transforms by name, enabling loose coupling.

Registry Pattern
----------------

The registry pattern provides:
- **Centralized catalog** - Single source of truth for all pipelines
- **Loose coupling** - Pipelines reference transforms by name, not directly
- **Dynamic discovery** - Find pipelines at runtime
- **Extensibility** - Add new pipelines without modifying core code
- **Metadata support** - Attach descriptive information

Decorator-Based Registration
----------------------------

Pipelines are registered using decorators at module import time:

.. code-block:: python

    @register_pipeline(name="physician_claim")
    def physician_claim_pipeline():
        return [
            PipelineStage("xref_mapping", "crosswalk", order=1),
            PipelineStage("dedup_claims", "deduplication", order=2),
            PipelineStage("apply_adr", "adr", order=3),
        ]

Pipeline Metadata
-----------------

Each pipeline can have associated metadata:
- Description of what the pipeline does
- Transform dependencies
- Execution order
- Performance characteristics
- Version and author information

Common Use Cases
================

1. **Dynamic Pipeline Lookup** - Find and invoke pipelines by name
2. **CLI Discovery** - List available pipelines for user selection
3. **Test Enumeration** - Iterate over all pipelines for testing
4. **Dependency Analysis** - Analyze transform dependencies
5. **Documentation Generation** - Auto-generate pipeline docs

How It Works
============

Registration Flow
-----------------

1. **Module Import** - Python imports pipeline module
2. **Decorator Execution** - @register_pipeline runs at function definition
3. **Registry Update** - Pipeline added to PipelineRegistry._pipelines dict
4. **Metadata Storage** - Optional metadata saved
5. **Function Return** - Original function returned unmodified

Lookup Flow
-----------

1. **Request Pipeline** - Caller requests pipeline by name
2. **Registry Lookup** - PipelineRegistry.get_pipeline() searches dict
3. **Function Return** - Return pipeline definition or None
4. **Stage Execution** - Caller executes stages in order
"""

from collections.abc import Callable
from typing import Any


class PipelineRegistry:
    """
    Registry for pipeline definitions.

    Maintains a central registry of all available pipeline definitions,
    organized by pipeline name. Pipelines are registered using decorators,
    allowing for automatic discovery and dynamic invocation.

    Attributes:
        _pipelines: Dict mapping pipeline names to pipeline functions
        _metadata: Dict mapping pipeline names to metadata dicts
    """

    _pipelines: dict[str, Callable] = {}
    _metadata: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(
        cls,
        name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Decorator to register a pipeline definition.

        Args:
            name: Optional name for the pipeline (defaults to function name)
            metadata: Optional metadata about the pipeline

        Returns:
            Decorated function that's registered in the registry
        """

        def decorator(func: Callable) -> Callable:
            pipeline_name = name or func.__name__

            # Register the function
            cls._pipelines[pipeline_name] = func

            # Store metadata if provided
            if metadata:
                cls._metadata[pipeline_name] = metadata

            # Return the original function unmodified
            return func

        return decorator

    @classmethod
    def get_pipeline(cls, name: str) -> Callable | None:
        """
        Retrieve a pipeline function from the registry.

        Args:
            name: The name of the pipeline

        Returns:
            The pipeline function, or None if not found
        """
        return cls._pipelines.get(name)

    @classmethod
    def list_pipelines(cls) -> list[str]:
        """
        List all registered pipeline names.

        Returns:
            List of registered pipeline names
        """
        return list(cls._pipelines.keys())

    @classmethod
    def get_metadata(cls, name: str) -> dict[str, Any] | None:
        """
        Get metadata for a specific pipeline.

        Args:
            name: The name of the pipeline

        Returns:
            Metadata dictionary, or None if not found
        """
        return cls._metadata.get(name)

    @classmethod
    def clear(cls):
        """Clear all registered pipelines (mainly for testing)."""
        cls._pipelines.clear()
        cls._metadata.clear()


def register_pipeline(name: str | None = None, **metadata):
    """
    Register a pipeline definition.

    Convenience decorator that registers a pipeline in the PipelineRegistry.

    Args:
        name: Optional name for the pipeline (defaults to function name)
        **metadata: Additional metadata about the pipeline

    """
    return PipelineRegistry.register(name, metadata)
