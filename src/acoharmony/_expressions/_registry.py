# © 2025 HarmonyCares
# All rights reserved.

"""
Expression registry for managing and discovering expression builders.

 a decorator-based registry pattern for registering
expression builder classes, making them discoverable and maintainable while
keeping implementation details in private modules.
"""

from typing import Any


class ExpressionRegistry:
    """
    Registry for expression builder classes.

        This class maintains a central registry of all available expression
        builders, organized by expression type. Builders are registered
        using decorators, allowing for .
    """

    _builders: dict[str, type] = {}
    _metadata: dict[str, dict[str, Any]] = {}

    @classmethod
    def register(
        cls,
        expression_type: str,
        metadata: dict[str, Any] | None = None,
        schemas: list[str] | None = None,
        callable: bool = True,
        dataset_types: list[str] | None = None,
        description: str | None = None,
    ):
        """
        Decorator to register an expression builder class with selective applicability.

                Args:
                    expression_type: The type of expression (e.g., 'deduplication', 'adr', 'pivot')
                    metadata: Optional metadata about the expression builder
                    schemas: List of schemas where this expression is applicable
                            (e.g., ['bronze', 'silver', 'gold']). None means all schemas.
                    callable: Whether this expression can be called as a standalone function
                    dataset_types: Optional list of dataset types this applies to
                                  (e.g., ['claims', 'eligibility', 'provider'])
                    description: Human-readable description of what this expression does

                Returns:
                    Decorated class that's registered in the registry

        """

        def decorator(builder_class: type) -> type:
            # Register the class
            cls._builders[expression_type] = builder_class

            # Build comprehensive metadata
            meta = metadata or {}
            meta.update(
                {
                    "schemas": schemas or ["bronze", "silver", "gold"],  # Default: all schemas
                    "callable": callable,
                    "dataset_types": dataset_types or [],  # Empty means all types
                    "description": description or builder_class.__doc__,
                    "class": builder_class.__name__,
                }
            )

            cls._metadata[expression_type] = meta

            # Add registry attributes to the class
            builder_class._expression_type = expression_type
            builder_class._schemas = meta["schemas"]
            builder_class._callable = meta["callable"]
            builder_class._dataset_types = meta["dataset_types"]

            return builder_class

        return decorator

    @classmethod
    def get_builder(cls, expression_type: str) -> type | None:
        """
        Retrieve an expression builder class from the registry.

                Args:
                    expression_type: The type of expression

                Returns:
                    The builder class, or None if not found
        """
        return cls._builders.get(expression_type)

    @classmethod
    def list_builders(cls) -> list[str]:
        """
        List all registered expression types.

                Returns:
                    List of registered expression types
        """
        return list(cls._builders.keys())

    @classmethod
    def get_metadata(cls, expression_type: str) -> dict[str, Any] | None:
        """
        Get metadata for a specific expression builder.

                Args:
                    expression_type: The type of expression

                Returns:
                    Metadata dictionary, or None if not found
        """
        return cls._metadata.get(expression_type)

    @classmethod
    def clear(cls):
        """Clear all registered builders (mainly for testing)."""
        cls._builders.clear()
        cls._metadata.clear()

    @classmethod
    def is_applicable(
        cls, expression_type: str, schema: str, dataset_type: str | None = None
    ) -> bool:
        """
        Check if an expression is applicable for a given schema and dataset type.

                Args:
                    expression_type: The type of expression
                    schema: The schema (bronze, silver, gold)
                    dataset_type: Optional dataset type (e.g., 'claims', 'eligibility')

                Returns:
                    True if the expression is applicable, False otherwise

        """
        metadata = cls.get_metadata(expression_type)
        if not metadata:
            return False

        # Check schema applicability
        applicable_schemas = metadata.get("schemas", [])
        if applicable_schemas and schema not in applicable_schemas:
            return False

        # Check dataset type applicability
        applicable_types = metadata.get("dataset_types", [])
        if applicable_types and dataset_type and dataset_type not in applicable_types:
            return False

        return True

    @classmethod
    def is_callable(cls, expression_type: str) -> bool:
        """
        Check if an expression can be called as a standalone function.

                Args:
                    expression_type: The type of expression

                Returns:
                    True if callable, False otherwise
        """
        metadata = cls.get_metadata(expression_type)
        if not metadata:
            return False
        return metadata.get("callable", True)

    @classmethod
    def list_for_schema(cls, schema: str, dataset_type: str | None = None) -> list[str]:
        """
        List all expressions applicable for a given schema.

                Args:
                    schema: The schema (bronze, silver, gold)
                    dataset_type: Optional dataset type filter

                Returns:
                    List of applicable expression types

        """
        applicable = []
        for expr_type in cls._builders.keys():
            if cls.is_applicable(expr_type, schema, dataset_type):
                applicable.append(expr_type)
        return applicable

    @classmethod
    def build_expression(
        cls, expression_type: str, config: dict[str, Any], schema: str | None = None
    ) -> Any:
        """
        Build an expression using the registered builder.

                Args:
                    expression_type: The type of expression to build
                    config: Configuration for the expression
                    schema: Optional schema for applicability checking

                Returns:
                    Built expression object

                Raises:
                    ValueError: If no builder is registered or expression not applicable
        """
        builder_class = cls.get_builder(expression_type)
        if not builder_class:
            raise ValueError(f"No builder registered for expression type: {expression_type}")

        # Check schema applicability if provided
        if schema and not cls.is_applicable(expression_type, schema):
            raise ValueError(
                f"Expression '{expression_type}' is not applicable for schema '{schema}'. "
                f"Applicable schemas: {cls.get_metadata(expression_type).get('schemas', [])}"
            )

        # Instantiate and build
        return builder_class.build(config)


# Convenience decorator shortcuts
def register_expression(
    expression_type: str,
    schemas: list[str] | None = None,
    callable: bool = True,
    dataset_types: list[str] | None = None,
    description: str | None = None,
    **metadata,
):
    """
    Register an expression builder with selective applicability.

        Args:
            expression_type: Unique identifier for the expression type
            schemas: List of applicable schemas (bronze, silver, gold). None = all.
            callable: Whether this expression can be called as a function
            dataset_types: List of applicable dataset types. Empty = all.
            description: Human-readable description
            **metadata: Additional metadata

        Returns:
            Decorator function

    """
    return ExpressionRegistry.register(
        expression_type,
        metadata=metadata,
        schemas=schemas,
        callable=callable,
        dataset_types=dataset_types,
        description=description,
    )
