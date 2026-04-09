"""Tests for acoharmony._transforms._key_value_pivot module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._key_value_pivot is not None


import polars as pl

from acoharmony._transforms._key_value_pivot import (
    KeyValuePivotConfig,
    KeyValuePivotExpression,
)


class TestKeyValuePivotBranches:
    """Cover uncovered branches in KeyValuePivotExpression.build."""

    @pytest.mark.unit
    def test_skip_empty_values_false(self):
        """Branch 213->220: skip_empty_values=False bypasses the filter."""
        df = pl.DataFrame({
            "column_1": ["key_a", "key_b"],
            "column_2": ["val_a", ""],
        })
        config = KeyValuePivotConfig(
            skip_empty_values=False,
            sanitize_keys=True,
            key_mapping=None,
        )
        result = KeyValuePivotExpression.build(df, config).collect()
        assert result.height == 1
        # Empty value should be kept because skip_empty_values is False
        assert result["key_b"][0] == ""

    @pytest.mark.unit
    def test_key_mapping_applied(self):
        """Branch 228->229: key_mapping matches a key, col_name is remapped."""
        df = pl.DataFrame({
            "column_1": ["Original Key"],
            "column_2": ["the_value"],
        })
        config = KeyValuePivotConfig(
            skip_empty_values=False,
            sanitize_keys=False,
            key_mapping={"Original Key": "mapped_name"},
        )
        result = KeyValuePivotExpression.build(df, config).collect()
        assert result.height == 1
        assert "mapped_name" in result.columns
        assert result["mapped_name"][0] == "the_value"

    @pytest.mark.unit
    def test_sanitize_keys_false(self):
        """Branch 234->238: sanitize_keys=False skips sanitization."""
        df = pl.DataFrame({
            "column_1": ["My Key"],
            "column_2": ["val"],
        })
        config = KeyValuePivotConfig(
            skip_empty_values=False,
            sanitize_keys=False,
            key_mapping=None,
        )
        result = KeyValuePivotExpression.build(df, config).collect()
        assert result.height == 1
        # Key should be unsanitized (original "My Key" preserved)
        assert "My Key" in result.columns
