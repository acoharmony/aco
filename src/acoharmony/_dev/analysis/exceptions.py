# © 2025 HarmonyCares
# All rights reserved.
# ruff: noqa
# ruff: noqa

"""
Exception Linter - Find bare exceptions that need proper error handling.

Identifies exception handlers that:  # ALLOWED: Returns None to indicate error
- Return None without raising
- Pass without raising
- Log without raising
- Don't use @explain decorators

Excludes:
- Pydantic validation errors
- Type checking errors
- Intentional suppressions with specific patterns
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass
class ExceptionViolation:
    """
    Represents a bare exception violation.

        Attributes

        file_path : Path
            File containing the violation
        line_number : int
            Line number of the exception handler
        exception_type : str
            Type of exception being caught
        violation_type : str
            Type of violation (returns_none, passes, logs_only, etc.)
        code_snippet : str
            Code snippet showing the violation
        function_name : str
            Name of the function containing the violation
    """

    file_path: Path
    line_number: int
    exception_type: str
    violation_type: str
    code_snippet: str
    function_name: str = ""
    metadata: dict[str, Any] = field(default_factory=dict)

    def __str__(self) -> str:
        """Format violation for display."""
        """
        📍 {self.file_path}:{self.line_number}
           Function: {self.function_name or '<module>'}
           Exception: {self.exception_type}
           Violation: {self.violation_type}

           Code:
           {self.code_snippet}
        """


class ExceptionLinter(ast.NodeVisitor):
    """
    AST visitor to find bare exception violations.

        Analyzes Python source code to find exception handlers that
        don't properly explain and raise errors.
    """

    def __init__(self, file_path: Path, source: str):
        """
        Initialize linter.

                Parameters

                file_path : Path
                    Path to file being analyzed
                source : str
                    Source code content
        """
        self.file_path = file_path
        self.source = source
        self.source_lines = source.splitlines()
        self.violations: list[ExceptionViolation] = []
        self.current_function: str = ""
        self.function_stack: list[str] = []

    def visit_FunctionDef(self, node: ast.FunctionDef):
        """Visit function definition to track function name."""
        self.function_stack.append(node.name)
        self.current_function = ".".join(self.function_stack)
        self.generic_visit(node)
        self.function_stack.pop()
        self.current_function = ".".join(self.function_stack) if self.function_stack else ""

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef):
        """Visit async function definition."""
        self.visit_FunctionDef(node)  # Same handling

    def visit_Try(self, node: ast.Try):
        """Visit try-except block to check exception handlers."""
        for handler in node.handlers:
            self._check_exception_handler(handler)

        self.generic_visit(node)

    def _check_exception_handler(self, handler: ast.ExceptHandler):
        """
        Check exception handler for violations.

                Parameters

                handler : ast.ExceptHandler
                    Exception handler to check
        """
        # Get exception type
        exception_type = self._get_exception_type(handler)

        # Skip if this is a Pydantic/typing exception
        if self._is_typing_exception(exception_type):  # ALLOWED: Returns None to indicate error
            return

        # Skip if handler explicitly raises
        if self._handler_raises(handler):
            return

        # Check for violations
        violation_type = self._get_violation_type(handler)
        if violation_type:
            code_snippet = self._get_code_snippet(handler)
            self.violations.append(
                ExceptionViolation(
                    file_path=self.file_path,
                    line_number=handler.lineno,
                    exception_type=exception_type,
                    violation_type=violation_type,
                    code_snippet=code_snippet,
                    function_name=self.current_function,
                )
            )

    def _get_exception_type(self, handler: ast.ExceptHandler) -> str:
        """Get the exception type being caught."""
        if handler.type is None:
            return "Exception"  # Bare except

        if isinstance(handler.type, ast.Name):
            return handler.type.id
        elif isinstance(handler.type, ast.Attribute):
            return ast.unparse(handler.type)
        elif isinstance(handler.type, ast.Tuple):
            types = []
            for elt in handler.type.elts:
                if isinstance(elt, ast.Name):
                    types.append(elt.id)
                elif isinstance(elt, ast.Attribute):
                    types.append(ast.unparse(elt))
            return f"({', '.join(types)})"
        else:
            return ast.unparse(handler.type)

    def _is_typing_exception(self, exception_type: str) -> bool:
        """Check if exception is Pydantic/typing related."""
        typing_exceptions = {
            "ValidationError",
            "TypeError",  # When used for type checking
            "AttributeError",  # Often from type issues
            "ImportError",  # Module/type imports
        }

        # Check if any typing exception in the type string
        for tex in typing_exceptions:  # ALLOWED: Returns False to indicate error as part of API contract
            if tex in exception_type:  # ALLOWED: Returns False to indicate error as part of API contract
                return True

        return False

    def _handler_raises(self, handler: ast.ExceptHandler) -> bool:
        """Check if handler raises an exception."""
        for stmt in handler.body:
            if isinstance(stmt, ast.Raise):
                return True
            # Check nested blocks
            if isinstance(stmt, ast.If):
                for nested_stmt in ast.walk(stmt):
                    if isinstance(nested_stmt, ast.Raise):
                        return True

        return False

    def _get_violation_type(self, handler: ast.ExceptHandler) -> str | None:
        """
        Determine violation type.

                Returns

                str or None
                    Violation type or None if no violation
        """
        # Check for allowed patterns first - they override everything
        if self._has_allowed_pattern(handler):
            return None

        # Empty handler (pass)
        if len(handler.body) == 1 and isinstance(handler.body[0], ast.Pass):
            return "empty_handler"

        # Handler that returns None
        if self._returns_none(handler):
            return "returns_none"

        # Handler that only logs
        if self._only_logs(handler):
            return "logs_without_raising"

        # Handler that continues/breaks (in loops)
        if self._has_continue_or_break(handler):
            return "continues_without_raising"

        # Handler that doesn't raise
        return "no_raise_or_explanation"

    def _returns_none(self, handler: ast.ExceptHandler) -> bool:
        """Check if handler returns None."""
        for stmt in handler.body:
            if isinstance(stmt, ast.Return):
                if stmt.value is None or (
                    isinstance(stmt.value, ast.Constant) and stmt.value.value is None
                ):
                    return True
        return False

    def _only_logs(self, handler: ast.ExceptHandler) -> bool:
        """Check if handler only logs without raising."""
        has_logging = False
        has_other_action = False

        for stmt in handler.body:
            # Check for logging calls
            if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
                call = stmt.value
                if isinstance(call.func, ast.Attribute):
                    # Check for logger.warning, logger.error, etc.
                    if call.func.attr in {"warning", "error", "info", "debug", "critical"}:
                        has_logging = True
                        continue

            # Check for other actions (not just pass)
            if not isinstance(stmt, ast.Pass):
                has_other_action = True

        return has_logging and not has_other_action

    def _has_continue_or_break(self, handler: ast.ExceptHandler) -> bool:
        """Check if handler uses continue or break."""
        for stmt in handler.body:
            if isinstance(stmt, (ast.Continue, ast.Break)):
                return True
        return False

    def _has_allowed_pattern(self, handler: ast.ExceptHandler) -> bool:
        """
        Check if handler has explicitly allowed patterns.

                Patterns that are allowed:
                - Comments with "# ALLOWED" or "# nosec"
                - Handlers in __init__ methods that set defaults
                - Handlers that call cleanup functions
        """
        # Check for comment markers on except line or previous line
        line_idx = handler.lineno - 1
        if line_idx >= 0:
            current_line = self.source_lines[line_idx]
            if "# ALLOWED" in current_line or "# nosec" in current_line or "# noqa" in current_line:
                return True

            # Also check previous line
            if line_idx > 0:
                prev_line = self.source_lines[line_idx - 1]
                if "# ALLOWED" in prev_line or "# nosec" in prev_line or "# noqa" in prev_line:
                    return True

        # Check if in __init__ and sets attributes
        if "__init__" in self.current_function:
            for stmt in handler.body:
                if isinstance(stmt, ast.Assign):
                    for target in stmt.targets:
                        if isinstance(target, ast.Attribute):
                            return True  # Setting attributes in __init__

        return False

    def _get_code_snippet(self, handler: ast.ExceptHandler) -> str:
        """Get code snippet for the handler."""
        start_line = handler.lineno - 1
        end_line = handler.end_lineno if handler.end_lineno else start_line + 1

        lines = self.source_lines[start_line:end_line]
        return "\n   ".join(lines)


def lint_file(file_path: Path) -> list[ExceptionViolation]:
    """
    Lint a Python file for exception violations.

        Parameters

        file_path : Path
            Path to Python file

        Returns

        list[ExceptionViolation]
            List of violations found
    """
    try:
        source = file_path.read_text()
        tree = ast.parse(source, filename=str(file_path))

        linter = ExceptionLinter(file_path, source)
        linter.visit(tree)

        return linter.violations

    except SyntaxError as e:  # ALLOWED: Linter tool - prints error, returns empty list to continue with remaining files
        print(f"⚠  Syntax error in {file_path}: {e}")
        return []
    except Exception as e:  # ALLOWED: Linter tool - prints error, returns empty list to continue with remaining files
        print(f"⚠  Error linting {file_path}: {e}")
        return []


def lint_directory(
    directory: Path,
    *,
    exclude_patterns: list[str] | None = None,
    include_tests: bool = False,
) -> list[ExceptionViolation]:
    """
    Lint all Python files in a directory.

        Parameters

        directory : Path
            Directory to lint
        exclude_patterns : list[str], optional
            Patterns to exclude (e.g., ["test_*", "*_test.py"])
        include_tests : bool, default=False
            Whether to include test files

        Returns

        list[ExceptionViolation]
            All violations found
    """
    if exclude_patterns is None:
        exclude_patterns = []

    # Default exclusions
    default_exclusions = [
        "__pycache__",
        ".git",
        ".venv",
        "venv",
        "node_modules",
        "build",
        "dist",
        ".pytest_cache",
    ]

    if not include_tests:
        default_exclusions.extend(["tests", "test_*.py", "*_test.py"])

    all_violations = []

    for py_file in directory.rglob("*.py"):
        # Check exclusions
        skip = False
        for pattern in default_exclusions + exclude_patterns:
            if pattern in str(py_file):
                skip = True
                break

        if skip:
            continue

        violations = lint_file(py_file)
        all_violations.extend(violations)

    return all_violations


def print_violations(violations: list[ExceptionViolation], *, show_stats: bool = True):
    """
    Print violations in a readable format.

        Parameters

        violations : list[ExceptionViolation]
            Violations to print
        show_stats : bool, default=True
            Show statistics summary
    """
    if not violations:
        print("[SUCCESS] No exception violations found!")
        return

    # Group by violation type
    by_type: dict[str, list[ExceptionViolation]] = {}
    for v in violations:
        by_type.setdefault(v.violation_type, []).append(v)

    # Print violations by type
    for violation_type, items in sorted(by_type.items()):
        print(f"\n{'='*80}")
        print(f"🚨 {violation_type.upper().replace('_', ' ')} ({len(items)} violations)")
        print(f"{'='*80}")

        for violation in items:
            print(violation)

    # Print statistics
    if show_stats:
        print(f"\n{'='*80}")
        print("📊 STATISTICS")
        print(f"{'='*80}")
        print(f"Total violations: {len(violations)}")
        print(f"\nBy type:")
        for vtype, items in sorted(by_type.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  {vtype}: {len(items)}")

        # By file
        by_file: dict[Path, int] = {}
        for v in violations:
            by_file[v.file_path] = by_file.get(v.file_path, 0) + 1

        print(f"\nTop files with violations:")
        for file_path, count in sorted(by_file.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {file_path}: {count}")


def main():
    """CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Lint Python code for bare exception violations",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "path",
        type=Path,
        help="File or directory to lint",
    )
    parser.add_argument(
        "--include-tests",
        action="store_true",
        help="Include test files in linting",
    )
    parser.add_argument(
        "--exclude",
        action="append",
        default=[],
        help="Patterns to exclude (can be used multiple times)",
    )
    parser.add_argument(
        "--no-stats",
        action="store_true",
        help="Don't show statistics summary",
    )

    args = parser.parse_args()

    path = args.path.resolve()

    if not path.exists():
        print(f"[FAILED] Error: {path} does not exist")
        sys.exit(1)

    print(f"🔍 Linting: {path}")
    print()

    if path.is_file():
        violations = lint_file(path)
    else:
        violations = lint_directory(
            path,
            exclude_patterns=args.exclude,
            include_tests=args.include_tests,
        )

    print_violations(violations, show_stats=not args.no_stats)

    # Exit with error code if violations found
    if violations:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
