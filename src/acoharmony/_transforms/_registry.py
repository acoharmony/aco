# © 2025 HarmonyCares
# All rights reserved.

"""
Transform registry for managing and discovering transformation functions.

 a decorator-based registry pattern for registering
transformation functions, making them discoverable and maintainable while
keeping implementation details in private modules.

What is the Transform Registry?
================================

The Transform Registry is a **centralized catalog of transformation functions**
that enables dynamic discovery, lookup, and invocation of transforms without
tight coupling between modules. It uses Python decorators to register functions
at module import time, creating a plugin-like architecture for extensibility.

Key Concepts
============

Registry Pattern

The registry pattern provides:
- **Centralized catalog** - Single source of truth for all transforms
- **Loose coupling** - Transforms don't need to know about each other
- **Dynamic discovery** - Find transforms at runtime by name/type
- **Extensibility** - Add new transforms without modifying core code
- **Metadata support** - Attach descriptive information to transforms

Decorator-Based Registration

Functions are registered using decorators at module import time:

.. code-block:: python

    @register_deduplication(name="standard")
    def deduplicate_claims(df):
        # Implementation here
        return df

This approach provides:
- Clean, declarative syntax
- Automatic registration on import
- No manual registry maintenance
- Type-specific namespacing

Transform Types

Transforms are organized by type for logical grouping:
- **deduplication** - Remove duplicate records
- **enrichment** - Add computed columns and metadata
- **adr** - Adjustment, Denial, Reprocess logic
- **standardization** - Normalize columns and data types
- **pivot** - Reshape long to wide format
- **aggregation** - Group and summarize data
- **crosswalk** - Map identifiers (MBI, TIN, NPI)
- **pipeline** - Multi-stage transformation sequences

Metadata Storage

Each transform can have associated metadata:
- Description of what the transform does
- Performance characteristics (memory, speed)
- Dependencies (required columns, other transforms)
- Version and author information
- Usage examples and documentation links

Common Use Cases
================

1. **Dynamic Transform Lookup** - Find and invoke transforms by name
   - Pipeline executors lookup transforms from config
   - CLI tools discover available transforms
   - Test frameworks enumerate all transforms

2. **Plugin Architecture** - Add new transforms without core changes
   - Custom project-specific transforms
   - Client-specific business logic
   - Experimental transforms during development

3. **Transform Discovery** - List available transforms and capabilities
   - API endpoints expose available transforms
   - Documentation generation from registry
   - Admin UIs show transform options

4. **Metadata-Driven Execution** - Use metadata for smart execution
   - Check dependencies before execution
   - Estimate memory/time requirements
   - Generate execution plans

5. **Testing and Validation** - Comprehensive transform testing
   - Enumerate all transforms for test coverage
   - Validate transform signatures
   - Ensure consistent behavior

How It Works
============

Registration Flow

1. **Module Import** - Python imports transform module
2. **Decorator Execution** - @register_* decorator runs at class/function definition
3. **Registry Update** - Transform added to TransformRegistry._transforms dict
4. **Metadata Storage** - Optional metadata saved to TransformRegistry._metadata dict
5. **Function Return** - Original function returned unmodified

Lookup Flow

1. **Request Transform** - Caller requests transform by type and name
2. **Registry Lookup** - TransformRegistry.get_transform() searches _transforms dict
3. **Function Return** - Return callable function or None if not found
4. **Invocation** - Caller invokes function with appropriate arguments

Design Benefits
===============

**Separation of Concerns**
- Transform logic separate from registry management
- Private modules for implementation, public API for discovery

**Testability**
- Mock registry for unit tests
- Clear() method for test isolation
- Easy to verify registration

**Maintainability**
- Self-documenting through decorators
- No manual registration code
- Type-safe with dict[str, dict[str, Callable]]

**Extensibility**
- Add transforms without modifying registry code
- Client projects can add custom transforms
- Versioning through metadata

Performance Considerations
==========================

1. **Import-Time Registration** - One-time cost at module load
2. **Dict Lookup** - O(1) lookup by type and name
3. **No Function Wrapping** - Decorators return original function (zero overhead)
4. **Memory** - Small footprint (references to functions, not copies)
5. **Thread Safety** - Class-level dicts are thread-safe for reads

"""

from collections.abc import Callable
from typing import Any


class TransformRegistry:
    """
    Registry for transformation functions.

        This class maintains a central registry of all available transformation
        functions, organized by transformation type. Functions are registered
        using decorators, allowing for .
        True

    """

    _transforms: dict[str, dict[str, Callable]] = {}
    _metadata: dict[str, dict[str, dict[str, Any]]] = {}

    @classmethod
    def register(
        cls,
        transform_type: str,
        name: str | None = None,
        metadata: dict[str, Any] | None = None,
    ):
        """
        Decorator to register a transformation function.

                Args:
                    transform_type: The type of transformation (e.g., 'deduplication')
                    name: Optional name for the transform (defaults to function name)
                    metadata: Optional metadata about the transformation

                Returns:
                    Decorated function that's registered in the registry
        """

        def decorator(func: Callable) -> Callable:
            func_name = name or func.__name__

            # Initialize category if needed
            if transform_type not in cls._transforms:
                cls._transforms[transform_type] = {}
                cls._metadata[transform_type] = {}

            # Register the function
            cls._transforms[transform_type][func_name] = func

            # Store metadata if provided
            if metadata:
                cls._metadata[transform_type][func_name] = metadata

            # Return the original function unmodified (no wrapper needed - @wraps preserves metadata)
            return func

        return decorator

    @classmethod
    def get_transform(cls, transform_type: str, name: str) -> Callable | None:
        """
        Retrieve a transformation function from the registry.

                Args:
                    transform_type: The type of transformation
                    name: The name of the specific transform

                Returns:
                    The transformation function, or None if not found
        """
        if transform_type in cls._transforms:
            return cls._transforms[transform_type].get(name)
        return None

    @classmethod
    def list_transforms(cls, transform_type: str | None = None) -> dict[str, list]:
        """
        List all registered transformations.

                Args:
                    transform_type: Optional filter by transformation type

                Returns:
                    Dictionary of transform types and their registered functions
        """
        if transform_type:
            return {transform_type: list(cls._transforms.get(transform_type, {}).keys())}

        return {t_type: list(funcs.keys()) for t_type, funcs in cls._transforms.items()}

    @classmethod
    def get_metadata(cls, transform_type: str, name: str) -> dict[str, Any] | None:
        """
        Get metadata for a specific transformation.

                Args:
                    transform_type: The type of transformation
                    name: The name of the specific transform

                Returns:
                    Metadata dictionary, or None if not found
        """
        if transform_type in cls._metadata:
            return cls._metadata[transform_type].get(name)
        return None

    @classmethod
    def clear(cls):
        """Clear all registered transforms (mainly for testing)."""
        cls._transforms.clear()
        cls._metadata.clear()


def register_crosswalk(name: str | None = None, **metadata):
    """Register a crosswalk transformation."""
    return TransformRegistry.register("crosswalk", name, metadata)


def register_pipeline(name: str | None = None, **metadata):
    """Register a pipeline transformation."""
    return TransformRegistry.register("pipeline", name, metadata)
