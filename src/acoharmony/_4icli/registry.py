# © 2025 HarmonyCares
# All rights reserved.

"""
Schema registry for 4icli file type codes.

 a centralized, extensible registry of file type codes
from schema definitions. It uses a decorator pattern to automatically discover
and register schemas, making the system naturally DRY and extensible.

"""

from dataclasses import dataclass

from .._log import get_logger
from .._registry import SchemaRegistry as CentralRegistry


@dataclass
class RegisteredFileType:
    """Information about a registered file type from a schema."""

    file_type_code: int
    schema_name: str
    file_pattern: str
    category: str | None = None
    description: str | None = None


class SchemaRegistry:
    """
    Registry of file type codes from schema definitions.

        This registry is automatically populated by scanning the _schemas directory
        and extracting fourIcli configuration from each schema file.

        The registry is a singleton - there's only one instance per process.
    """

    _instance = None
    _initialized = False

    def __new__(cls):
        """Singleton pattern - only one registry instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize registry (only runs once due to singleton)."""
        if not self._initialized:
            self._logger = get_logger("4icli-registry")
            self._file_types: dict[int, RegisteredFileType] = {}
            self._by_schema: dict[str, list[RegisteredFileType]] = {}
            self._by_category: dict[str, list[RegisteredFileType]] = {}
            self._discover_schemas()
            SchemaRegistry._initialized = True

    def _discover_schemas(self) -> None:
        """
        Discover and register file types from the central SchemaRegistry.

                Reads fourIcli configuration registered via _tables Pydantic model
                decorators instead of scanning YAML files.
        """
        # Ensure _tables models are imported so CentralRegistry is populated
        from .. import _tables as _  # noqa: F401

        schema_count = 0
        file_type_count = 0

        for schema_name in sorted(CentralRegistry.list_schemas()):
            fouricli_block = CentralRegistry.get_four_icli_config(schema_name)
            if not fouricli_block:
                continue

            file_type_code = fouricli_block.get("fileTypeCode")
            file_pattern = fouricli_block.get("filePattern")

            if file_type_code is None or not file_pattern:
                continue

            # Extract category and description
            category = fouricli_block.get("category")
            meta = CentralRegistry.get_metadata(schema_name)
            description = meta.get("description")

            # Handle multiple patterns (comma-separated)
            patterns = [p.strip() for p in file_pattern.split(",")]

            for pattern in patterns:
                registered = RegisteredFileType(
                    file_type_code=file_type_code,
                    schema_name=schema_name,
                    file_pattern=pattern,
                    category=category,
                    description=description,
                )

                # Register in main dict (keyed by file type code)
                if file_type_code not in self._file_types:
                    self._file_types[file_type_code] = registered
                    file_type_count += 1

                # Register by schema name
                if schema_name not in self._by_schema:
                    self._by_schema[schema_name] = []
                self._by_schema[schema_name].append(registered)

                # Register by category
                if category:
                    if category not in self._by_category:
                        self._by_category[category] = []
                    self._by_category[category].append(registered)

            schema_count += 1
            self._logger.debug(
                f"Registered schema: {schema_name} (file_type_code={file_type_code})"
            )

        self._logger.info(
            f"Schema registry initialized: {schema_count} schemas, {file_type_count} file type codes"
        )

    def get_file_type_codes(self) -> list[int]:
        """
        Get all registered file type codes.

                Returns:
                    List of file type codes from registered schemas
        """
        return sorted(self._file_types.keys())

    def get_by_code(self, file_type_code: int) -> RegisteredFileType | None:
        """
        Get registered file type by code.

                Args:
                    file_type_code: File type code to lookup

                Returns:
                    RegisteredFileType if found, None otherwise
        """
        return self._file_types.get(file_type_code)

    def get_by_schema(self, schema_name: str) -> list[RegisteredFileType]:
        """
        Get all file types for a schema.

                Args:
                    schema_name: Schema name (without .yml extension)

                Returns:
                    List of registered file types for this schema
        """
        return self._by_schema.get(schema_name, [])

    def get_by_category(self, category: str) -> list[RegisteredFileType]:
        """
        Get all file types for a category.

                Args:
                    category: Category name (e.g., "Reports", "CCLF")

                Returns:
                    List of registered file types for this category
        """
        return self._by_category.get(category, [])

    def get_categories(self) -> list[str]:
        """
        Get all registered categories.

                Returns:
                    List of category names
        """
        return sorted(self._by_category.keys())

    def get_all(self) -> list[RegisteredFileType]:
        """
        Get all registered file types.

                Returns:
                    List of all registered file types
        """
        return list(self._file_types.values())

    def reload(self) -> None:
        """
        Reload the registry from schemas directory.

                Useful for development or when schemas are updated at runtime.
        """
        self._file_types.clear()
        self._by_schema.clear()
        self._by_category.clear()
        self._discover_schemas()


# Global registry instance
_registry = SchemaRegistry()


# Public API functions
def get_file_type_codes() -> list[int]:
    """
    Get all registered file type codes from schemas.

        Returns:
            List of file type codes

    """
    return _registry.get_file_type_codes()


def get_file_type(file_type_code: int) -> RegisteredFileType | None:
    """
    Get registered file type information by code.

        Args:
            file_type_code: File type code to lookup

        Returns:
            RegisteredFileType if registered, None otherwise
    """
    return _registry.get_by_code(file_type_code)


def get_file_types_by_category(category: str) -> list[RegisteredFileType]:
    """
    Get all registered file types for a category.

        Args:
            category: Category name

        Returns:
            List of registered file types
    """
    return _registry.get_by_category(category)


def get_categories() -> list[str]:
    """
    Get all registered categories.

        Returns:
            List of category names
    """
    return _registry.get_categories()


def get_all_file_types() -> list[RegisteredFileType]:
    """
    Get all registered file types.

        Returns:
            List of all registered file types
    """
    return _registry.get_all()


def reload_registry() -> None:
    """
    Reload the registry from schemas.

        Useful for development when schemas are modified.
    """
    _registry.reload()


def match_filename_to_file_type(filename: str) -> RegisteredFileType | None:
    """
    Match a filename against registered file patterns to determine file type code.

        IMPORTANT: Checks ALL schemas, not just unique file_type_codes, since multiple
        schemas may share the same file_type_code but have different patterns.

        Args:
            filename: Name of the file to match

        Returns:
            RegisteredFileType if a match is found, None otherwise

    """
    from fnmatch import fnmatch

    # Get all registered file types from ALL schemas (not just unique codes)
    # This handles cases where multiple schemas share the same file_type_code
    all_types = []
    for schema_types in _registry._by_schema.values():
        all_types.extend(schema_types)

    # Try to match filename against each pattern
    for file_type in all_types:
        if fnmatch(filename, file_type.file_pattern):
            return file_type

    return None
