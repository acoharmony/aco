# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive result type system for robust error handling and operation tracking.

 a unified result type system inspired by functional programming
languages like Rust and Haskell. It replaces traditional exception-based error
handling with explicit result types, making error cases visible in function
signatures and enabling better error recovery and reporting.

Core Philosophy:
    - **Explicit Error Handling**: Errors are values, not exceptions
    - **Type Safety**: Generic types preserve data type information
    - **Composability**: Results can be chained and transformed
    - **Rich Context**: Results carry metadata, warnings, and detailed errors
    - **Consistency**: All operations use the same result pattern

Result Types:
    1. **Result[T]**: Generic result for any operation
    2. **TransformResult**: Specialized for data transformations
    3. **PipelineResult**: Aggregates results from pipeline stages

Status Types:
    - SUCCESS: Operation completed successfully
    - FAILURE: Operation failed completely
    - PARTIAL: Operation partially succeeded with warnings
    - SKIPPED: Operation was intentionally skipped

Key Benefits:
    - **No Unexpected Exceptions**: All errors are explicit in return types
    - **Better Error Recovery**: Can handle partial failures gracefully
    - **Detailed Diagnostics**: Rich error context for debugging
    - **Functional Composition**: Chain operations with map/and_then
    - **Consistent API**: All operations follow the same pattern

Usage Patterns:
    # Basic success/failure

    # Error handling

    # Chaining operations

    # Pipeline tracking

Design Principles:
    1. **Fail Fast**: Invalid operations fail immediately with clear messages
    2. **Recover Gracefully**: Partial failures don't stop the entire pipeline
    3. **Track Everything**: All operations leave audit trails
    4. **Type Preservation**: Generic types maintain data type information
    5. **Zero Cost**: No runtime overhead compared to exceptions

Integration with ACOHarmony:
    Results are used throughout the system:
    - Transformations return TransformResult
    - Pipelines aggregate into PipelineResult
    - File operations return Result[DataFrame]
    - Validation returns Result[ValidationReport]

Best Practices:
    1. Always check result status before accessing data
    2. Use unwrap_or() for safe defaults
    3. Chain operations with map() and and_then()
    4. Include detailed error messages and metadata
    5. Use specialized result types for domain operations

Note:
    This module is foundational to ACOHarmony's reliability,
    enabling robust error handling and detailed operation tracking
    throughout the data processing pipeline.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import TypeVar


class ResultStatus(Enum):
    """
    Enumeration of possible result statuses.

        Defines the complete set of states that an operation result can have.
        Each status has specific semantics that guide error handling and
        recovery strategies.

        Members:
            SUCCESS: Operation completed successfully without issues.
                    Data is valid and can be used safely.

            FAILURE: Operation failed completely and cannot proceed.
                    No valid data is available. Errors list contains details.

            PARTIAL: Operation partially succeeded with some issues.
                    Data may be incomplete but usable. Warnings list contains issues.

            SKIPPED: Operation was intentionally skipped based on conditions.
                    No processing occurred. Often used for filtered operations.

        Status Semantics:
            - SUCCESS: Safe to use data, continue processing
            - FAILURE: Must handle error, cannot continue normally
            - PARTIAL: Can continue with degraded functionality
            - SKIPPED: Normal flow, but no action taken

        Note:
            Status values are strings for JSON serialization compatibility.
    """

    SUCCESS = "success"
    FAILURE = "failure"
    PARTIAL = "partial"
    SKIPPED = "skipped"


T = TypeVar("T")


@dataclass
class Result[T]:
    """
    Generic result type providing robust error handling for any operation.

        Result[T] is a container type that explicitly represents the success or
        failure of an operation, along with associated data, errors, warnings,
        and metadata. It enables functional error handling patterns and makes
        error cases explicit in function signatures.

        Type Parameter:
            T: The type of data contained in a successful result.
               Can be any type including primitives, collections, or custom objects.

        Attributes:
            status: The result status (SUCCESS, FAILURE, PARTIAL, SKIPPED)
            message: Human-readable description of the result
            data: The actual data payload (None if failed)
            errors: List of error messages for failed operations
            warnings: List of warning messages for partial successes
            metadata: Dictionary of additional context information

            # Checking status

            # Safe unwrapping

            # Functional composition

            # Error chaining

        Design Patterns:
            - **Railway Oriented Programming**: Operations on track or derailed
            - **Maybe/Option Pattern**: Explicit presence/absence of values
            - **Error as Values**: Errors are first-class citizens

        Thread Safety:
            Result objects are immutable after creation, making them
            safe to share across threads.

        Note:
            Prefer using factory methods (ok, error, partial, skipped)
            over direct construction for better readability.
    """

    status: ResultStatus
    message: str = ""
    data: T | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    metadata: dict = field(default_factory=dict)

    @property
    def success(self) -> bool:
        """
        Check if the result represents a successful operation.

                Returns:
                    bool: True if status is SUCCESS, False otherwise

        """
        return self.status == ResultStatus.SUCCESS

    @property
    def failed(self) -> bool:
        """
        Check if the result represents a failed operation.

                Returns:
                    bool: True if status is FAILURE, False otherwise

        """
        return self.status == ResultStatus.FAILURE

    @classmethod
    def ok(cls, data: T | None = None, message: str = "", **metadata) -> "Result[T]":
        """
        Create a successful result with optional data and metadata.

                Factory method for creating results representing successful operations.
                This is the primary way to create success results.

                Args:
                    data: The successful operation's output data
                    message: Optional success message for logging/display
                    **metadata: Additional key-value pairs stored in metadata dict

                Returns:
                    Result[T]: A new Result with SUCCESS status

        """
        return cls(status=ResultStatus.SUCCESS, message=message, data=data, metadata=metadata)

    @classmethod
    def error(cls, message: str, errors: list[str] | None = None, **metadata) -> "Result[T]":
        """
        Create a failed result with error details.

                Factory method for creating results representing failed operations.
                Captures detailed error information for debugging and recovery.

                Args:
                    message: Primary error message describing the failure
                    errors: Optional list of detailed error messages
                    **metadata: Additional context about the failure

                Returns:
                    Result[T]: A new Result with FAILURE status and no data

                Note:
                    If errors list is not provided, the message is used as the single error.
        """
        return cls(
            status=ResultStatus.FAILURE,
            message=message,
            errors=errors or [message],
            metadata=metadata,
        )

    @classmethod
    def partial(
        cls, data: T | None, message: str, warnings: list[str] | None = None, **metadata
    ) -> "Result[T]":
        """
        Create a partial success result with warnings.

                Factory method for operations that succeeded but encountered
                non-fatal issues. Data is present but may be incomplete or degraded.

                Args:
                    data: The partially successful operation's output
                    message: Description of what succeeded and what didn't
                    warnings: List of warning messages about issues encountered
                    **metadata: Additional context about the partial success

                Returns:
                    Result[T]: A new Result with PARTIAL status

                Use Cases:
                    - Data import with some invalid records
                    - API calls with partial responses
                    - Batch operations with some failures
                    - Transformations with data loss
        """
        return cls(
            status=ResultStatus.PARTIAL,
            message=message,
            data=data,
            warnings=warnings or [],
            metadata=metadata,
        )

    @classmethod
    def skipped(cls, message: str, **metadata) -> "Result[T]":
        """
        Create a result for intentionally skipped operations.

                Factory method for operations that were not executed due to
                preconditions, filters, or business logic. Not an error condition.

                Args:
                    message: Explanation of why the operation was skipped
                    **metadata: Additional context about the skip condition

                Returns:
                    Result[T]: A new Result with SKIPPED status and no data

                Use Cases:
                    - Incremental processing (already done)
                    - Feature flags (disabled features)
                    - Conditional execution (prerequisites not met)
                    - Filtering (doesn't match criteria)
        """
        return cls(status=ResultStatus.SKIPPED, message=message, metadata=metadata)

    def unwrap(self) -> T:
        """
        Extract the data from a successful result or raise an exception.

                Forcefully unwraps the result data. Should only be used when you're
                certain the result is successful or when you want to fail fast.

                Returns:
                    T: The contained data if successful

                Raises:
                    ValueError: If result is not successful, with the error message

                Warning:
                    Prefer unwrap_or() or checking success property first.
                    Use unwrap() only when failure is truly exceptional.
        """
        if not self.success:
            raise ValueError(f"Cannot unwrap failed result: {self.message}")
        return self.data

    def unwrap_or(self, default: T) -> T:
        """
        Extract data from result or return a default value if failed.

                Safe unwrapping method that never raises exceptions. Returns
                the contained data if successful, otherwise returns the default.

                Args:
                    default: Value to return if result is not successful

                Returns:
                    T: The contained data if successful, otherwise default

                Note:
                    This is the preferred way to handle optional results.
        """
        return self.data if self.success else default

    def map(self, func) -> "Result":
        """
        Transform successful result data using a function.

                Applies a transformation function to the contained data if the
                result is successful. Failed results are passed through unchanged.
                If the function raises an exception, returns an error result.

                Args:
                    func: Function to apply to the data (data: T) -> U

                Returns:
                    Result: New result with transformed data or original if failed

                Functional Composition:
                    Map allows chaining transformations:
                    ```python
                        load_data()
                        .map(clean_data)
                        .map(transform_data)
                        .map(validate_data)
                    )
                    ```
        """
        if self.success and self.data is not None:
            try:
                new_data = func(self.data)
                return Result.ok(new_data, self.message, **self.metadata)
            except (
                Exception
            ) as e:  # ALLOWED: Result monad pattern - returns error Result instead of raising
                return Result.error(str(e))
        return self

    def and_then(self, func) -> "Result":
        """
        Chain operations that return Results (flatmap/bind operation).

                Applies a function that returns a Result to the contained data
                if successful. This allows chaining operations that might fail.
                Failed results are passed through unchanged.

                Args:
                    func: Function that returns a Result (data: T) -> Result[U]

                Returns:
                    Result: The result from func if successful, original if failed

                Railway Pattern:
                    and_then implements the railway pattern where operations
                    stay on the success track until one fails:
                    ```python
                        load_file(path)
                        .and_then(parse_json)
                        .and_then(validate_schema)
                        .and_then(transform_data)
                    )
                    ```

                Difference from map():
                    - map: (T -> U) transforms data
                    - and_then: (T -> Result[U]) chains fallible operations
        """
        if self.success:
            return func(self.data)
        return self

    def __bool__(self) -> bool:
        """
        Enable Result to be used in boolean conditions.

                Makes Result truthy if successful, falsy otherwise.
                This allows idiomatic Python conditionals.

                Returns:
                    bool: True if successful, False otherwise

        """
        return self.success

    def __str__(self) -> str:
        """
        Generate human-readable string representation of the result.

                Creates a formatted string showing status, message, errors,
                and warnings using status-appropriate symbols.

                Returns:
                    str: Formatted string representation

                Format:
                    {symbol} {message}
                      Errors: {errors}    (if present)
                      Warnings: {warnings} (if present)

                Symbols:
                    - [OK] SUCCESS
                    - [ERROR] FAILURE
                    - ⚠ PARTIAL
                    - ⊘ SKIPPED

        """
        status_symbol = {
            ResultStatus.SUCCESS: "[OK]",
            ResultStatus.FAILURE: "[ERROR]",
            ResultStatus.PARTIAL: "⚠",
            ResultStatus.SKIPPED: "⊘",
        }[self.status]

        s = f"{status_symbol} {self.message}"

        if self.errors:
            s += f"\n  Errors: {', '.join(self.errors)}"

        if self.warnings:
            s += f"\n  Warnings: {', '.join(self.warnings)}"

        return s


@dataclass
class TransformResult(Result[dict]):
    """
    Specialized result type for data transformation operations.

        Extends the base Result class with transformation-specific metrics
        and tracking. Used throughout ACOHarmony for reporting the outcome
        of data processing operations.

        Additional Attributes:
            records_processed: Number of records successfully processed
            files_processed: Number of files processed in the transformation
            output_path: Path where transformation results were written

        Inherited Attributes:
            All attributes from Result[dict] including status, message,
            data, errors, warnings, and metadata.

        Data Format:
            The data dictionary typically contains:
            - records: Number of records processed
            - files: Number of files processed
            - output: Output file path
            - Additional transformation-specific metrics

        Note:
            Always use factory methods (transform_ok, transform_error)
            rather than direct construction.
    """

    records_processed: int = 0
    files_processed: int = 0
    output_path: str | None = None

    @classmethod
    def transform_ok(
        cls, records: int, files: int, output: str, message: str = ""
    ) -> "TransformResult":
        """
        Create a successful transformation result with metrics.

                Factory method for creating results from successful data
                transformations. Automatically generates a descriptive message
                if not provided.

                Args:
                    records: Number of records successfully processed
                    files: Number of files processed
                    output: Path where results were written
                    message: Optional custom success message.
                            If empty, auto-generates from metrics.

                Returns:
                    TransformResult: Success result with transformation metrics

                Message Generation:
                    If no message provided, generates:
                    "Processed {records:,} records from {files} files"
                    with proper number formatting.
        """
        return cls(
            status=ResultStatus.SUCCESS,
            message=message or f"Processed {records:,} records from {files} files",
            records_processed=records,
            files_processed=files,
            output_path=output,
            data={"records": records, "files": files, "output": output},
        )

    @classmethod
    def transform_error(cls, message: str, errors: list[str] | None = None) -> "TransformResult":
        """
        Create a failed transformation result with error details.

                Factory method for creating results from failed transformations.
                Captures detailed error information for debugging and recovery.

                Args:
                    message: Primary error message describing the failure
                    errors: Optional list of detailed error messages

                Returns:
                    TransformResult: Failed result with error information

                Common Error Patterns:
                    - File I/O errors
                    - Schema validation failures
                    - Data type mismatches
                    - Memory/resource constraints
                    - Configuration errors

                Note:
                    Error details are preserved for logging and debugging.
                    Consider including actionable error messages.
        """
        return cls(status=ResultStatus.FAILURE, message=message, errors=errors or [message])


@dataclass
class PipelineResult(Result[list[TransformResult]]):
    """
    Specialized result type for multi-stage pipeline operations.

        Aggregates results from multiple transformation stages into a single
        result object. Provides pipeline-level metrics and summary reporting
        for complex multi-step data processing workflows.

        Additional Attributes:
            stages_completed: Number of stages that completed successfully
            stages_total: Total number of stages in the pipeline

        Inherited Attributes:
            All attributes from Result[List[TransformResult]] including
            status, message, data (list of stage results), errors, warnings,
            and metadata.

        Data Structure:
            The data attribute contains a list of TransformResult objects,
            one for each pipeline stage, in execution order.

        Status Determination:
            - SUCCESS: All stages completed successfully
            - PARTIAL: Some stages succeeded, some failed
            - FAILURE: All stages failed or pipeline couldn't start

            Pipeline Result: Completed 3/3 stages
            Completion: 100.0%
            [OK] Stage 1: Processed 1,000 records from 1 files
            [OK] Stage 2: Processed 950 records from 1 files
            [OK] Stage 3: Processed 900 records from 1 files

        Pipeline Tracking:
            - Tracks individual stage outcomes
            - Calculates overall completion rate
            - Provides detailed summary reports
            - Enables partial failure recovery

        Use Cases:
            - ETL pipelines with multiple stages
            - Data validation workflows
            - Complex transformation chains
            - Batch processing pipelines

        Note:
            PipelineResult helps identify which stages failed in
            complex pipelines, enabling targeted recovery.
    """

    stages_completed: int = 0
    stages_total: int = 0

    @property
    def completion_rate(self) -> float:
        """
        Calculate the pipeline completion percentage.

                Computes the percentage of stages that completed successfully
                relative to the total number of stages.

                Returns:
                    float: Completion percentage (0.0 to 100.0)
                          Returns 0.0 if no stages exist

                Note:
                    Useful for progress bars and monitoring dashboards.
        """
        if self.stages_total == 0:
            return 0.0
        return (self.stages_completed / self.stages_total) * 100

    @classmethod
    def pipeline_ok(cls, results: list[TransformResult]) -> "PipelineResult":
        """
        Create a pipeline result from stage results.

                Factory method that analyzes stage results to determine overall
                pipeline status. Sets status to SUCCESS if all stages succeeded,
                PARTIAL if some succeeded.

                Args:
                    results: List of TransformResult objects from pipeline stages

                Returns:
                    PipelineResult: Aggregated pipeline result with metrics

                Status Logic:
                    - All stages successful -> SUCCESS
                    - Some stages successful -> PARTIAL
                    - No stages successful -> PARTIAL (with 0 completed)

                Message Format:
                    "Completed {completed}/{total} stages"

                Note:
                    Empty results list creates a PARTIAL result with 0/0 stages.
        """
        # Count both successful and skipped stages as completed
        completed = sum(1 for r in results if r.success or r.status == ResultStatus.SKIPPED)
        total = len(results)

        return cls(
            status=ResultStatus.SUCCESS if completed == total else ResultStatus.PARTIAL,
            message=f"Completed {completed}/{total} stages",
            data=results,
            stages_completed=completed,
            stages_total=total,
        )

    def get_summary(self) -> str:
        """
        Generate a detailed summary of pipeline execution.

                Creates a formatted multi-line summary showing overall pipeline
                status, completion rate, and individual stage outcomes with
                visual indicators.

                Returns:
                    str: Formatted summary string with:
                        - Overall pipeline result and message
                        - Completion percentage
                        - Individual stage statuses with symbols
                        - Stage messages

                Format:
                    Pipeline Result: {message}
                    Completion: {rate}%

                      {symbol} Stage {n}: {stage_message}

                Symbols:
                    - [OK] Successful stage
                    - [ERROR] Failed stage
                    - ⚠ Partial/warning stage
                    - ⊘ Skipped stage

                Use Cases:
                    - Console output for CLI tools
                    - Log file entries
                    - Status reports
                    - Debugging pipeline failures

                Note:
                    Summary includes all stages regardless of status,
                    making it easy to identify failure points.
        """
        lines = [f"Pipeline Result: {self.message}", f"Completion: {self.completion_rate:.1f}%", ""]

        if self.data:
            for i, result in enumerate(self.data, 1):
                if result.success:
                    symbol = "[OK]"
                elif result.status == ResultStatus.SKIPPED:
                    symbol = "⊘"
                elif result.status == ResultStatus.PARTIAL:
                    symbol = "⚠"
                else:
                    symbol = "[ERROR]"
                lines.append(f"  {symbol} Stage {i}: {result.message}")

        return "\n".join(lines)
