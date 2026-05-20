# © 2025 HarmonyCares
# All rights reserved.

"""
Parser registry for managing and discovering file parser functions.

 a decorator-based registry pattern for registering
parser functions, making them discoverable and maintainable while
keeping implementation details in private modules.
"""

from collections.abc import Callable
from typing import Any


class ParserRegistry:
    """
    Registry for file parser functions.

        This class maintains a central registry of all available parser
        functions, organized by file format. Functions are registered
        using decorators, allowing for .
    """

    _parsers: dict[str, Callable] = {}
    _metadata: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(cls, format_type: str, metadata: dict[str, Any] | None = None):
        """
        Decorator to register a parser function.

                Args:
                    format_type: The type of file format (e.g., 'csv', 'parquet', 'fixed_width')
                    metadata: Optional metadata about the parser

                Returns:
                    Decorated function that's registered in the registry

        """

        def decorator(func: Callable) -> Callable:
            # Register the function
            cls._parsers[format_type] = func

            # Store metadata if provided
            if metadata:
                cls._metadata[format_type] = metadata

            # Return the original function unmodified (no wrapper needed - @wraps preserves metadata)
            return func

        return decorator

    @classmethod
    def get_parser(cls, format_type: str) -> Callable | None:
        """
        Retrieve a parser function from the registry.

                Args:
                    format_type: The type of file format

                Returns:
                    The parser function, or None if not found
        """
        return cls._parsers.get(format_type)

    @classmethod
    def list_parsers(cls) -> list:
        """
        List all registered parser formats.

                Returns:
                    List of registered format types
        """
        return list(cls._parsers.keys())

    @classmethod
    def get_metadata(cls, format_type: str) -> dict[str, Any] | None:
        """
        Get metadata for a specific parser.

                Args:
                    format_type: The type of file format

                Returns:
                    Metadata dictionary, or None if not found
        """
        return cls._metadata.get(format_type)

    @classmethod
    def clear(cls):
        """Clear all registered parsers (mainly for testing)."""
        cls._parsers.clear()
        cls._metadata.clear()


# Convenience decorator shortcuts
def register_parser(format_type: str, **metadata):
    """Register a file parser."""
    return ParserRegistry.register(format_type, metadata)
