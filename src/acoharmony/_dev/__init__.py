# © 2025 HarmonyCares
# All rights reserved.

"""Development utilities for ACO Harmony."""

# Documentation generators (core functionality, imports must work)
from .docs.modules import generate_module_docs
from .docs.orchestrator import generate_all_documentation
from .docs.pipelines import generate_full_documentation as generate_pipeline_docs


def __getattr__(name):
    """Lazy import for dev utilities that have complex dependency chains."""
    import importlib

    _lazy = {
        # Setup
        "setup_storage": (".setup.storage", "setup_storage"),
        "verify_storage": (".setup.storage", "verify_storage"),
        "populate_test_duckdb": (".setup.database", "populate_test_duckdb"),
        "add_copyright": (".setup.copyright", "add_copyright"),
        # Generators
        "generate_aco_metadata": (".generators.metadata", "generate_aco_metadata"),
        # Test
        "MockDataGenerator": (".test.mocks", "MockDataGenerator"),
        "generate_test_mocks": (".test.mocks", "generate_test_mocks"),
        "TestCoverageManager": (".test.coverage", "TestCoverageManager"),
        "organize_test_fixtures": (".test.fixtures", "organize_fixtures"),
        # Analysis
        "audit_docstrings": (".analysis.docstrings", "audit_all_modules"),
        "analyze_import_chains": (".analysis.imports", "build_dependency_graph"),
        "lint_exceptions": (".analysis.exceptions", "lint_exceptions"),
        "introspect_schemas": (".analysis.schemas", "introspect_schemas"),
        "introspect_csv": (".analysis.schemas", "introspect_csv"),
        "generate_schema_template": (".analysis.schemas", "generate_schema_template"),
        # Excel
        "ExcelAnalyzer": (".excel.analyzer", "ExcelAnalyzer"),
        "compare_excel_files": (".excel.diffs", "compare_excel_files"),
    }
    if name in _lazy:
        module_path, attr = _lazy[name]
        mod = importlib.import_module(module_path, __name__)
        return getattr(mod, attr)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # Documentation
    "generate_all_documentation",
    "generate_module_docs",
    "generate_pipeline_docs",
    # Test
    "generate_test_mocks",
    "MockDataGenerator",
    "TestCoverageManager",
    "organize_test_fixtures",
    # Analysis
    "audit_docstrings",
    "analyze_import_chains",
    "lint_exceptions",
    "introspect_schemas",
    "introspect_csv",
    "generate_schema_template",
    # Excel
    "ExcelAnalyzer",
    "compare_excel_files",
    # Setup
    "setup_storage",
    "verify_storage",
    "populate_test_duckdb",
    "add_copyright",
    # Generators
    "generate_aco_metadata",
]
