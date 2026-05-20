# © 2025 HarmonyCares
# All rights reserved.

"""
ACO Harmony Exception Framework.

Comprehensive exception handling with automatic logging, tracing, and
explanatory messages (WHY and HOW) for every error.

Usage

Basic usage with auto-logging and tracing:

Using decorators for automatic exception handling:

Registering custom exceptions:

Context managers:

Domain-Specific Exceptions

Storage exceptions:

Parsing exceptions:

Registry and Documentation

Generate documentation:

Error Codes

- STORAGE_001: StorageBackend initialization failed
- STORAGE_002: Storage configuration error
- STORAGE_003: Storage access error
- STORAGE_004: Storage path not found
- STORAGE_005: Invalid medallion tier
- PARSE_001: General parsing error
- PARSE_002: Schema not found
- PARSE_003: Invalid file format
- PARSE_004: Missing required column
- PARSE_005: Data type mismatch
- PARSE_006: Fixed-width parsing error
"""

# Base classes
from ._base import (
    ACOHarmonyException,
    ACOHarmonyWarning,
    ExceptionContext,
)
from ._catalog import (
    CatalogError,
    SchemaRegistrationError,
    TableNotFoundError,
)

# Decorators and context managers
from ._decorators import (
    catch_and_explain,
    explain,
    explain_on_error,
    log_errors,
    retry_with_explanation,
    suppress_and_log,
    trace_errors,
)
from ._parsing import (
    DataTypeMismatchError,
    FixedWidthParseError,
    InvalidFileFormatError,
    MissingColumnError,
    ParseError,
    SchemaNotFoundError,
)
from ._pipeline import (
    DependencyError,
    PipelineError,
    StageError,
)

# Registry
from ._registry import (
    ExceptionRegistry,
    register_exception,
)

# Domain-specific exceptions
from ._storage import (
    InvalidTierError,
    StorageAccessError,
    StorageBackendError,
    StorageConfigurationError,
    StoragePathError,
)
from ._transform import (
    TransformError,
    TransformOutputError,
    TransformSchemaError,
    TransformSourceError,
)
from ._validation import (
    EmptyDataError,
    FileFormatValidationError,
    MissingColumnsError,
    PathValidationError,
    TypeValidationError,
    ValidationError,
)

__all__ = [
    # Base classes
    "ACOHarmonyException",
    "ACOHarmonyWarning",
    "ExceptionContext",
    # Decorators
    "explain",
    "trace_errors",
    "log_errors",
    "catch_and_explain",
    "explain_on_error",
    "suppress_and_log",
    "retry_with_explanation",
    # Registry
    "ExceptionRegistry",
    "register_exception",
    # Storage exceptions
    "StorageBackendError",
    "StorageConfigurationError",
    "StorageAccessError",
    "StoragePathError",
    "InvalidTierError",
    # Parsing exceptions
    "ParseError",
    "SchemaNotFoundError",
    "InvalidFileFormatError",
    "MissingColumnError",
    "DataTypeMismatchError",
    "FixedWidthParseError",
    # Transform exceptions
    "TransformError",
    "TransformSchemaError",
    "TransformSourceError",
    "TransformOutputError",
    # Catalog exceptions
    "CatalogError",
    "TableNotFoundError",
    "SchemaRegistrationError",
    # Pipeline exceptions
    "PipelineError",
    "StageError",
    "DependencyError",
    # Validation exceptions
    "ValidationError",
    "MissingColumnsError",
    "TypeValidationError",
    "EmptyDataError",
    "PathValidationError",
    "FileFormatValidationError",
]
