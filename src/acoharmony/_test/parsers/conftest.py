"""Shared fixtures and helpers for parser tests."""

import importlib
from types import SimpleNamespace
from typing import Any

from acoharmony._catalog import TableMetadata
from acoharmony.medallion import MedallionLayer


def _has(module_name: str) -> bool:
    """Check if a module is available."""
    try:
        importlib.import_module(module_name)
        return True
    except ImportError:
        return False


# Optional dependency checks
HAS_BS4 = _has('bs4')
HAS_PYPDF = _has('pypdf')
HAS_PYLATEXENC = _has('pylatexenc')
HAS_BIBTEXPARSER = _has('bibtexparser')
HAS_MARKDOWN = _has('markdown')
HAS_FRONTMATTER = _has('frontmatter')
HAS_PYDANTIC = _has('pydantic')
HAS_OPENPYXL = _has('openpyxl')


def _schema(columns, **extras):
    """Build a lightweight schema-like object with a .columns attribute."""
    return SimpleNamespace(columns=columns, **extras)


def _schema_with_storage(columns, storage=None, **extras):
    """Build schema with storage config."""
    return SimpleNamespace(columns=columns, storage=storage or {}, **extras)


def _schema_with_file_format(columns, file_format=None, **extras):
    """Build schema with file_format config."""
    return SimpleNamespace(columns=columns, file_format=file_format or {}, **extras)


def create_mock_metadata(
    name: str,
    columns: list[dict[str, Any]],
    file_format: dict[str, Any],
    medallion_layer: MedallionLayer | None = MedallionLayer.BRONZE,
) -> TableMetadata:
    """Create a mock TableMetadata object for testing."""
    return TableMetadata(
        name=name,
        description=f"Test table {name}",
        columns=columns,
        storage={"tier": "raw"},
        file_format=file_format,
        medallion_layer=medallion_layer,
        unity_catalog="main",
    )
