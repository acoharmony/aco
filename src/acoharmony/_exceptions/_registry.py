# © 2025 HarmonyCares
# All rights reserved.

"""
Exception registry for tracking and documenting all exceptions.

Provides a central registry of all exception types with their
error codes, categories, and documentation.
"""

from __future__ import annotations

from ._base import ACOHarmonyException


class ExceptionRegistry:
    """
    Central registry for all ACO Harmony exceptions.

        Tracks exception types, their error codes, and provides
        lookup and documentation generation capabilities.
    """

    _registry: dict[str, type[ACOHarmonyException]] = {}
    _by_category: dict[str, list[type[ACOHarmonyException]]] = {}

    @classmethod
    def register(
        cls,
        error_code: str,
        category: str = "general",
        why_template: str = "",
        how_template: str = "",
        default_causes: list[str] | None = None,
        default_remediation: list[str] | None = None,
    ):
        """
        Decorator to register an exception class.

                Parameters

                error_code : str
                    Unique error code
                category : str, default="general"
                    Error category
                why_template : str, optional
                    Default WHY explanation template
                how_template : str, optional
                    Default HOW explanation template
                default_causes : list[str], optional
                    Default list of causes
                default_remediation : list[str], optional
                    Default remediation steps
        """

        def decorator(exception_class: type[ACOHarmonyException]) -> type[ACOHarmonyException]:
            # Set class attributes
            exception_class.error_code = error_code
            exception_class.category = category

            # Store templates as class attributes
            if why_template:
                exception_class._why_template = why_template
            if how_template:
                exception_class._how_template = how_template
            if default_causes:
                exception_class._default_causes = default_causes
            if default_remediation:
                exception_class._default_remediation = default_remediation

            # Register in registry
            cls._registry[error_code] = exception_class

            # Register by category
            if category not in cls._by_category:
                cls._by_category[category] = []
            cls._by_category[category].append(exception_class)

            return exception_class

        return decorator

    @classmethod
    def get(cls, error_code: str) -> type[ACOHarmonyException] | None:
        """
        Get exception class by error code.

                Parameters

                error_code : str
                    Error code to lookup

                Returns

                Type[ACOHarmonyException] or None
                    Exception class if found
        """
        return cls._registry.get(error_code)

    @classmethod
    def get_by_category(cls, category: str) -> list[type[ACOHarmonyException]]:
        """
        Get all exceptions in a category.

                Parameters

                category : str
                    Category name

                Returns

                list[Type[ACOHarmonyException]]
                    List of exception classes in category
        """
        return cls._by_category.get(category, [])

    @classmethod
    def all_codes(cls) -> list[str]:
        """
        Get all registered error codes.

                Returns

                list[str]
                    List of error codes
        """
        return sorted(cls._registry.keys())

    @classmethod
    def all_categories(cls) -> list[str]:
        """
        Get all exception categories.

                Returns

                list[str]
                    List of categories
        """
        return sorted(cls._by_category.keys())

    @classmethod
    def generate_docs(cls, category: str | None = None) -> str:
        """
        Generate markdown documentation for exceptions.

                Parameters

                category : str, optional
                    Only generate docs for specific category

                Returns

                str
                    Markdown documentation
        """
        lines = ["# ACO Harmony Exception Reference", ""]

        if category:
            exceptions = cls.get_by_category(category)
            lines.append(f"## Category: {category}")
            lines.append("")
        else:
            lines.append("## All Exceptions")
            lines.append("")
            exceptions = list(cls._registry.values())

        for exc_class in exceptions:
            lines.append(f"### {exc_class.__name__}")
            lines.append("")
            lines.append(f"**Error Code:** `{exc_class.error_code}`")
            lines.append(f"**Category:** {exc_class.category}")
            lines.append("")

            if hasattr(exc_class, "__doc__") and exc_class.__doc__:
                lines.append(exc_class.__doc__.strip())
                lines.append("")

            if hasattr(exc_class, "_why_template"):
                lines.append("**Why This Happens:**")
                lines.append(f"{exc_class._why_template}")
                lines.append("")

            if hasattr(exc_class, "_how_template"):
                lines.append("**How To Fix:**")
                lines.append(f"{exc_class._how_template}")
                lines.append("")

            if hasattr(exc_class, "_default_causes"):
                lines.append("**Common Causes:**")
                for cause in exc_class._default_causes:
                    lines.append(f"- {cause}")
                lines.append("")

            if hasattr(exc_class, "_default_remediation"):
                lines.append("**Remediation Steps:**")
                for i, step in enumerate(exc_class._default_remediation, 1):
                    lines.append(f"{i}. {step}")
                lines.append("")

            lines.append("---")
            lines.append("")

        return "\n".join(lines)

    @classmethod
    def print_summary(cls):
        """Print summary of registered exceptions."""
        print(f"Registered Exceptions: {len(cls._registry)}")
        print(f"Categories: {len(cls._by_category)}")
        print("\nBy Category:")
        for category in sorted(cls._by_category.keys()):
            count = len(cls._by_category[category])
            print(f"  {category}: {count}")


def register_exception(
    error_code: str,
    category: str = "general",
    **kwargs,
):
    """
    Convenience function to register exception.

        Alias for ExceptionRegistry.register for easier imports.

        Parameters

        error_code : str
            Unique error code
        category : str, default="general"
            Error category
        **kwargs
            Additional registration parameters
    """
    return ExceptionRegistry.register(error_code, category, **kwargs)
