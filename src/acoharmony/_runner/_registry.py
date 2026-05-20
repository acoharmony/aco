# © 2025 HarmonyCares
# All rights reserved.

"""
Runner operation registry for managing transformation execution strategies.

 a decorator-based registry pattern for registering
different types of transformation runners and execution strategies.
"""

from collections.abc import Callable
from functools import wraps
from typing import Any


class RunnerRegistry:
    """
    Registry for runner operations and strategies.

        This class maintains a central registry of all available runner
        operations, organized by operation type.
    """

    _operations: dict[str, Callable] = {}
    _processors: dict[str, type] = {}
    _metadata: dict[str, dict[str, Any]] = {}

    @classmethod
    def register_operation(cls, operation_type: str, metadata: dict[str, Any] | None = None):
        """
        Decorator to register a runner operation.

                Args:
                    operation_type: The type of operation (e.g., 'schema_transform', 'pipeline', 'pattern')
                    metadata: Optional metadata about the operation

                Returns:
                    Decorated function that's registered in the registry

        """

        def decorator(func: Callable) -> Callable:
            # Register the function
            cls._operations[operation_type] = func

            # Store metadata if provided
            if metadata:
                cls._metadata[operation_type] = metadata

            # Return the original function unmodified
            @wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper

        return decorator

    @classmethod
    def register_processor(cls, processor_type: str, metadata: dict[str, Any] | None = None):
        """
        Decorator to register a data processor class.

                Args:
                    processor_type: The type of processor (e.g., 'chunked', 'streaming', 'memory')
                    metadata: Optional metadata about the processor

                Returns:
                    Decorated class that's registered in the registry
        """

        def decorator(processor_class: type) -> type:
            # Register the class
            cls._processors[processor_type] = processor_class

            # Store metadata if provided
            if metadata:
                cls._metadata[processor_type] = metadata

            # Add a registry attribute to the class
            processor_class._processor_type = processor_type

            return processor_class

        return decorator

    @classmethod
    def get_operation(cls, operation_type: str) -> Callable | None:
        """
        Retrieve an operation from the registry.

                Args:
                    operation_type: The type of operation

                Returns:
                    The operation function, or None if not found
        """
        return cls._operations.get(operation_type)

    @classmethod
    def get_processor(cls, processor_type: str) -> type | None:
        """
        Retrieve a processor class from the registry.

                Args:
                    processor_type: The type of processor

                Returns:
                    The processor class, or None if not found
        """
        return cls._processors.get(processor_type)

    @classmethod
    def list_operations(cls) -> list[str]:
        """
        List all registered operations.

                Returns:
                    List of registered operation types
        """
        return list(cls._operations.keys())

    @classmethod
    def list_processors(cls) -> list[str]:
        """
        List all registered processors.

                Returns:
                    List of registered processor types
        """
        return list(cls._processors.keys())

    @classmethod
    def get_metadata(cls, item_type: str) -> dict[str, Any] | None:
        """
        Get metadata for a specific operation or processor.

                Args:
                    item_type: The type of operation or processor

                Returns:
                    Metadata dictionary, or None if not found
        """
        return cls._metadata.get(item_type)

    @classmethod
    def clear(cls):
        """Clear all registered items (mainly for testing)."""
        cls._operations.clear()
        cls._processors.clear()
        cls._metadata.clear()


# Convenience decorator shortcuts
def register_operation(operation_type: str, **metadata):
    """Register a runner operation."""
    return RunnerRegistry.register_operation(operation_type, metadata)


def register_processor(processor_type: str, **metadata):
    """Register a data processor."""
    return RunnerRegistry.register_processor(processor_type, metadata)
