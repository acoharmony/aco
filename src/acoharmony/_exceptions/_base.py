# © 2025 HarmonyCares
# All rights reserved.

"""
Base exception classes with automatic logging and tracing.

Provides foundation for all ACO Harmony exceptions with built-in:
- Structured logging
- OpenTelemetry tracing
- Explanatory messages (WHY and HOW)
- Context capture
"""

from __future__ import annotations

import traceback
from dataclasses import dataclass, field
from typing import Any, ClassVar


@dataclass
class ExceptionContext:
    """
    Context information captured when an exception occurs.

        Attributes

        original_error : Exception | None
            The original exception that was caught
        why : str
            Explanation of why this error occurred
        how : str
            Instructions on how to fix this error
        causes : list[str]
            Possible causes of the error
        remediation_steps : list[str]
            Step-by-step remediation instructions
        metadata : dict
            Additional context-specific metadata
        stack_trace : str | None
            Stack trace at time of exception
    """

    original_error: Exception | None = None
    why: str = ""
    how: str = ""
    causes: list[str] = field(default_factory=list)
    remediation_steps: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)
    stack_trace: str | None = None

    def __post_init__(self):
        """Capture stack trace if not provided."""
        if self.stack_trace is None and self.original_error:
            self.stack_trace = "".join(
                traceback.format_exception(
                    type(self.original_error),
                    self.original_error,
                    self.original_error.__traceback__,
                )
            )


class ACOHarmonyException(Exception):
    """
    Base exception for all ACO Harmony errors.

        Automatically logs and traces exceptions with full context.

        Class Attributes

        error_code : str
            Unique error code for this exception type
        category : str
            Error category (storage, parsing, transform, etc.)

        Attributes

        message : str
            Primary error message
        context : ExceptionContext
            Full exception context with WHY/HOW
    """

    error_code: ClassVar[str] = "ACO_UNKNOWN"
    category: ClassVar[str] = "general"

    def __init__(
        self,
        message: str,
        *,
        original_error: Exception | None = None,
        why: str = "",
        how: str = "",
        causes: list[str] | None = None,
        remediation_steps: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        auto_log: bool = True,
        auto_trace: bool = True,
    ):
        """
        Initialize exception with full context.

                Parameters

                message : str
                    Primary error message
                original_error : Exception, optional
                    Original exception that was caught
                why : str, optional
                    Explanation of why this error occurred
                how : str, optional
                    Instructions on how to fix
                causes : list[str], optional
                    Possible causes
                remediation_steps : list[str], optional
                    Step-by-step fix instructions
                metadata : dict, optional
                    Additional context
                auto_log : bool, default=True
                    Automatically log this exception
                auto_trace : bool, default=True
                    Automatically trace this exception
        """
        super().__init__(message)

        self.message = message
        self.context = ExceptionContext(
            original_error=original_error,
            why=why,
            how=how,
            causes=causes or [],
            remediation_steps=remediation_steps or [],
            metadata=metadata or {},
        )

        # Automatic logging
        if auto_log:
            self._log_error()

        # Automatic tracing
        if auto_trace:
            self._trace_error()

    def _log_error(self):
        """Log this exception with full context."""
        try:
            from .._log import get_logger

            logger = get_logger(f"exceptions.{self.category}")

            # Build log context
            log_context = {
                "error_code": self.error_code,
                "category": self.category,
                "message": self.message,
                **self.context.metadata,
            }

            if self.context.original_error:
                log_context["original_error"] = str(self.context.original_error)
                log_context["original_type"] = type(self.context.original_error).__name__

            logger.error(
                f"[{self.error_code}] {self.message}",
                extra=log_context,
                exc_info=self.context.original_error,
            )

        except Exception:  # ALLOWED: Don't let logging errors break exception handling
            # Don't let logging errors break exception handling
            pass

    def _trace_error(self):
        """Create trace span for this exception."""
        try:
            from opentelemetry.trace import Status, StatusCode

            from .._trace import get_tracer

            tracer = get_tracer(f"exceptions.{self.category}")

            with tracer.start_as_current_span(
                f"exception.{self.error_code}",
                attributes={
                    "error.code": self.error_code,
                    "error.category": self.category,
                    "error.message": self.message,
                    **{f"metadata.{k}": str(v) for k, v in self.context.metadata.items()},
                },
            ) as span:
                span.set_status(Status(StatusCode.ERROR, self.message))
                if self.context.original_error:
                    span.record_exception(self.context.original_error)

        except Exception:  # ALLOWED: Don't let tracing errors break exception handling
            # Don't let tracing errors break exception handling
            pass

    def format_message(self, *, detailed: bool = True) -> str:
        """
        Format exception message with full context.

                Parameters

                detailed : bool, default=True
                    Include full WHY/HOW details

                Returns

                str
                    Formatted error message
        """
        if not detailed:
            return self.message

        lines = [
            "╔" + "=" * 78 + "╗",
            f"║ {self.error_code}: {self.category.upper()} ERROR".ljust(79) + "║",
            "╚" + "=" * 78 + "╝",
            "",
            self.message,
            "",
        ]

        if self.context.original_error:
            lines.extend(
                [
                    "ORIGINAL ERROR:",
                    f"  {type(self.context.original_error).__name__}: {self.context.original_error}",
                    "",
                ]
            )

        if self.context.why:
            lines.extend(
                [
                    "WHY THIS HAPPENED:",
                    *[f"  {line}" for line in self.context.why.split("\n")],
                    "",
                ]
            )

        if self.context.causes:
            lines.extend(
                [
                    "POSSIBLE CAUSES:",
                    *[f"  {i + 1}. {cause}" for i, cause in enumerate(self.context.causes)],
                    "",
                ]
            )

        if self.context.how:
            lines.extend(
                [
                    "HOW TO FIX:",
                    *[f"  {line}" for line in self.context.how.split("\n")],
                    "",
                ]
            )

        if self.context.remediation_steps:
            lines.extend(
                [
                    "REMEDIATION STEPS:",
                    *[
                        f"  {i + 1}. {step}"
                        for i, step in enumerate(self.context.remediation_steps)
                    ],
                    "",
                ]
            )

        if self.context.metadata:
            lines.extend(
                [
                    "ADDITIONAL CONTEXT:",
                    *[f"  {k}: {v}" for k, v in self.context.metadata.items()],
                    "",
                ]
            )

        lines.append("╚" + "=" * 78 + "╝")

        return "\n".join(lines)

    def __str__(self) -> str:
        """Return detailed error message."""
        return self.format_message(detailed=True)

    def __repr__(self) -> str:
        """Return concise representation."""
        return f"{self.__class__.__name__}('{self.message}', code={self.error_code})"


class ACOHarmonyWarning(UserWarning):
    """
    Base warning for ACO Harmony.

        Similar to ACOHarmonyException but for warnings that should
        be logged but not necessarily raised.
    """

    def __init__(self, message: str, *, category: str = "general", metadata: dict | None = None):
        super().__init__(message)
        self.message = message
        self.category = category
        self.metadata = metadata or {}

        # Log warning
        try:
            from .._log import get_logger

            logger = get_logger(f"warnings.{category}")
            logger.warning(message, extra=self.metadata)
        except Exception:  # ALLOWED: Optional operation, errors are non-critical
            pass
