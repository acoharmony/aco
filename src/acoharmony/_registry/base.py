# © 2025 HarmonyCares
# All rights reserved.

"""
Generic registry base class for reducing code duplication.

Provides a flexible, type-safe registry pattern that can be specialized
for different use cases (classes, functions, transforms, etc.).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class Registry[T]:
    """
    Generic registry for types or callables.

    Provides a standard pattern for registering items with metadata.
    Subclasses can specialize for specific types (classes, functions, etc.).

    The registry uses a two-level structure: category -> name -> item.
    This allows organizing different types of items while maintaining
    a single registry instance.

    Parameters
    ----------
    T : TypeVar
        Type of items being registered

    Examples
    --------
    Creating a custom registry:

    >>> class TransformRegistry(Registry[Callable]):
    ...     pass
    >>>
    >>> @TransformRegistry.register("enrichment", "add_demographics")
    ... def add_demographics(df):
    ...     return df.with_columns(pl.lit("demo_added"))

    Using with metadata:

    >>> @TransformRegistry.register(
    ...     "validation",
    ...     "check_required",
    ...     metadata={"version": 1, "author": "team"},
    ... )
    ... def check_required(df):
    ...     return df

    Retrieving items:

    >>> transform = TransformRegistry.get("enrichment", "add_demographics")
    >>> metadata = TransformRegistry.get_metadata("enrichment", "add_demographics")

    Listing items:

    >>> categories = TransformRegistry.list_categories()
    >>> items = TransformRegistry.list_items("enrichment")

    Notes
    -----
    - Thread-safe for reads, but not for concurrent registrations
    - Items are stored as class attributes, shared across instances
    - Metadata is optional but recommended for complex registries
    - Registry info is attached to items as attributes when possible

    See Also
    --------
    TypeRegistry : Specialized registry for classes/types
    CallableRegistry : Specialized registry for functions/callables
    """

    _items: dict[str, dict[str, T]] = {}
    _metadata: dict[str, dict[str, dict[str, Any]]] = {}

    @classmethod
    def register(
        cls,
        category: str,
        name: str,
        metadata: dict[str, Any] | None = None,
    ) -> Callable[[T], T]:
        """
        Register an item in the registry.

        This decorator registers an item and returns it unmodified,
        allowing it to be used normally while being tracked in the registry.

        Parameters
        ----------
        category : str
            Category (e.g., "processor", "transform", "expression")
        name : str
            Unique name within category
        metadata : dict[str, Any], optional
            Optional metadata dict (version, description, tags, etc.)

        Returns
        -------
        Callable[[T], T]
            Decorator that registers item and returns it unmodified

        Examples
        --------
        >>> @Registry.register("transforms", "dedupe", metadata={"version": 1})
        ... def dedupe_records(df):
        ...     return df.unique()

        >>> @Registry.register("validators", "schema_check")
        ... class SchemaValidator:
        ...     def validate(self, df):
        ...         pass
        """

        def decorator(item: T) -> T:
            # Initialize category if needed
            if category not in cls._items:
                cls._items[category] = {}
                cls._metadata[category] = {}

            # Register item
            cls._items[category][name] = item

            # Store metadata
            if metadata:
                cls._metadata[category][name] = metadata

            # Add registry info as attributes (if possible)
            if hasattr(item, "__dict__"):
                try:
                    item._registry_category = category  # type: ignore
                    item._registry_name = name  # type: ignore
                except AttributeError:
                    # Some objects don't allow attribute assignment
                    pass

            return item

        return decorator

    @classmethod
    def get(cls, category: str, name: str) -> T | None:
        """
        Get item from registry.

        Parameters
        ----------
        category : str
            Category name
        name : str
            Item name within category

        Returns
        -------
        T | None
            Registered item or None if not found

        Examples
        --------
        >>> item = Registry.get("transforms", "dedupe")
        >>> if item:
        ...     result = item(df)
        """
        return cls._items.get(category, {}).get(name)

    @classmethod
    def get_metadata(cls, category: str, name: str) -> dict[str, Any] | None:
        """
        Get metadata for item.

        Parameters
        ----------
        category : str
            Category name
        name : str
            Item name within category

        Returns
        -------
        dict[str, Any] | None
            Metadata dictionary or None if not found

        Examples
        --------
        >>> meta = Registry.get_metadata("transforms", "dedupe")
        >>> if meta:
        ...     print(f"Version: {meta.get('version')}")
        """
        return cls._metadata.get(category, {}).get(name)

    @classmethod
    def list_items(cls, category: str | None = None) -> dict[str, dict[str, T]]:
        """
        List all items, optionally filtered by category.

        Parameters
        ----------
        category : str, optional
            If provided, only return items in this category

        Returns
        -------
        dict[str, dict[str, T]]
            Dictionary mapping categories to their items

        Examples
        --------
        >>> # All items
        >>> all_items = Registry.list_items()
        >>>
        >>> # Only transforms
        >>> transforms = Registry.list_items("transforms")
        """
        if category:
            return {category: cls._items.get(category, {})}
        return cls._items.copy()

    @classmethod
    def list_categories(cls) -> list[str]:
        """
        List all registered categories.

        Returns
        -------
        list[str]
            List of category names

        Examples
        --------
        >>> categories = Registry.list_categories()
        >>> print(f"Available categories: {', '.join(categories)}")
        """
        return list(cls._items.keys())

    @classmethod
    def list_names(cls, category: str) -> list[str]:
        """
        List all item names in a category.

        Parameters
        ----------
        category : str
            Category name

        Returns
        -------
        list[str]
            List of item names in the category

        Examples
        --------
        >>> names = Registry.list_names("transforms")
        >>> print(f"Available transforms: {', '.join(names)}")
        """
        return list(cls._items.get(category, {}).keys())

    @classmethod
    def count(cls, category: str | None = None) -> int:
        """
        Count registered items.

        Parameters
        ----------
        category : str, optional
            If provided, count only items in this category

        Returns
        -------
        int
            Number of registered items

        Examples
        --------
        >>> total = Registry.count()
        >>> transforms = Registry.count("transforms")
        """
        if category:
            return len(cls._items.get(category, {}))
        return sum(len(items) for items in cls._items.values())

    @classmethod
    def clear(cls, category: str | None = None) -> None:
        """
        Clear registered items.

        Parameters
        ----------
        category : str, optional
            If provided, clear only this category. Otherwise clear all.

        Examples
        --------
        >>> # Clear one category
        >>> Registry.clear("transforms")
        >>>
        >>> # Clear everything (useful for testing)
        >>> Registry.clear()
        """
        if category:
            cls._items.pop(category, None)
            cls._metadata.pop(category, None)
        else:
            cls._items.clear()
            cls._metadata.clear()


class TypeRegistry(Registry):
    """
    Registry for classes/types.

    Specialized version of Registry for registering class types.
    Useful for plugin systems, factory patterns, and dynamic class loading.

    Examples
    --------
    Registering classes:

    >>> @TypeRegistry.register("parsers", "csv")
    ... class CSVParser:
    ...     def parse(self, file):
    ...         pass
    >>>
    >>> @TypeRegistry.register("parsers", "json")
    ... class JSONParser:
    ...     def parse(self, file):
    ...         pass

    Using registered classes:

    >>> parser_cls = TypeRegistry.get("parsers", "csv")
    >>> if parser_cls:
    ...     parser = parser_cls()
    ...     parser.parse(file)

    Factory pattern:

    >>> def create_parser(parser_type: str):
    ...     parser_cls = TypeRegistry.get("parsers", parser_type)
    ...     if parser_cls:
    ...         return parser_cls()
    ...     raise ValueError(f"Unknown parser: {parser_type}")

    Notes
    -----
    - Stores class objects, not instances
    - Use for factory patterns and plugin systems
    - Compatible with dataclasses and Pydantic models

    See Also
    --------
    Registry : Base registry class
    CallableRegistry : Registry for functions
    """

    pass


class CallableRegistry(Registry):
    """
    Registry for functions/callables.

    Specialized version of Registry for registering functions and callables.
    Useful for command registries, transform libraries, and plugin systems.

    Examples
    --------
    Registering functions:

    >>> @CallableRegistry.register("transforms", "normalize")
    ... def normalize(df):
    ...     return df.with_columns(pl.all().str.upper())
    >>>
    >>> @CallableRegistry.register("transforms", "filter_active")
    ... def filter_active(df):
    ...     return df.filter(pl.col("status") == "A")

    Using registered functions:

    >>> transform = CallableRegistry.get("transforms", "normalize")
    >>> if transform:
    ...     result = transform(df)

    Command registry pattern:

    >>> @CallableRegistry.register(
    ...     "commands",
    ...     "process",
    ...     metadata={"description": "Process data", "version": 1},
    ... )
    ... def process_command(args):
    ...     pass
    >>>
    >>> def execute_command(cmd_name: str, args):
    ...     command = CallableRegistry.get("commands", cmd_name)
    ...     if command:
    ...         return command(args)
    ...     raise ValueError(f"Unknown command: {cmd_name}")

    Notes
    -----
    - Stores callable objects (functions, lambdas, etc.)
    - Use for transform libraries and command registries
    - Can store both regular functions and decorated functions

    See Also
    --------
    Registry : Base registry class
    TypeRegistry : Registry for classes
    """

    pass


__all__ = ["Registry", "TypeRegistry", "CallableRegistry"]
