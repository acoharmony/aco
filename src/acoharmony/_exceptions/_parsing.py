# © 2025 HarmonyCares
# All rights reserved.

"""
Parser-related exceptions.

Exceptions for file parsing, schema validation, and data format errors.
"""

from __future__ import annotations

from pathlib import Path

from ._base import ACOHarmonyException
from ._registry import register_exception


@register_exception(
    error_code="PARSE_001",
    category="parsing",
    why_template="File format does not match expected schema",
    how_template="Verify file format matches the Pydantic dataclass in src/acoharmony/_tables/",
)
class ParseError(ACOHarmonyException):
    """Base class for parsing errors."""

    pass


@register_exception(
    error_code="PARSE_002",
    category="parsing",
    why_template="Schema definition is missing or invalid",
    how_template="Check that the table dataclass exists in src/acoharmony/_tables/",
)
class SchemaNotFoundError(ACOHarmonyException):
    """Raised when schema definition cannot be found."""

    @classmethod
    def for_schema(cls, schema_name: str) -> SchemaNotFoundError:
        """Create error for missing schema."""
        return cls(
            f"Schema '{schema_name}' not found",
            why=f"No schema definition exists for '{schema_name}'",
            how=f"Create the table dataclass at: src/acoharmony/_tables/{schema_name}.py",
            causes=[
                "Table file was deleted or moved",
                "Typo in schema name",
                "Schema not yet implemented",
            ],
            remediation_steps=[
                f"Check if file exists: ls src/acoharmony/_tables/{schema_name}.py",
                "Verify schema name spelling",
                "Register the dataclass with @register_schema",
            ],
            metadata={"schema_name": schema_name},
        )


@register_exception(
    error_code="PARSE_003",
    category="parsing",
    why_template="Data file format is invalid or corrupted",
    how_template="Verify file is valid and not corrupted",
)
class InvalidFileFormatError(ACOHarmonyException):
    """Raised when file format is invalid."""

    @classmethod
    def for_file(cls, file_path: Path | str, expected_format: str) -> InvalidFileFormatError:
        """Create error for invalid file format."""
        return cls(
            f"Invalid {expected_format} file: {file_path}",
            why=f"File does not appear to be a valid {expected_format} file",
            how="Verify file integrity and format:\n"
            f"  - Check file is not empty: ls -lh {file_path}\n"
            f"  - Verify file type: file {file_path}\n"
            f"  - Try opening with appropriate tool",
            metadata={
                "file_path": str(file_path),
                "expected_format": expected_format,
            },
        )


@register_exception(
    error_code="PARSE_004",
    category="parsing",
    why_template="Required column is missing from data file",
    how_template="Verify data file has all required columns per schema",
)
class MissingColumnError(ACOHarmonyException):
    """Raised when required column is missing."""

    @classmethod
    def for_columns(
        cls,
        missing_columns: list[str],
        file_path: Path | str,
        schema_name: str,
    ) -> MissingColumnError:
        """Create error for missing columns."""
        cols = ", ".join(missing_columns)
        return cls(
            f"Missing required columns: {cols}",
            why=f"Schema '{schema_name}' requires these columns but they are missing from file",
            how="Add missing columns to data file or update schema definition",
            metadata={
                "missing_columns": missing_columns,
                "file_path": str(file_path),
                "schema_name": schema_name,
            },
        )


@register_exception(
    error_code="PARSE_005",
    category="parsing",
    why_template="Column data type does not match schema definition",
    how_template="Verify data types match schema or update schema to match data",
)
class DataTypeMismatchError(ACOHarmonyException):
    """Raised when column data type doesn't match schema."""

    pass


@register_exception(
    error_code="PARSE_006",
    category="parsing",
    why_template="Fixed-width file format specification is incorrect",
    how_template="Verify column positions and widths in schema definition",
)
class FixedWidthParseError(ACOHarmonyException):
    """Raised when fixed-width file parsing fails."""

    pass
