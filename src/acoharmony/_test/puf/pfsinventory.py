"""Tests for acoharmony._puf.pfs_inventory module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from unittest.mock import patch, MagicMock

import acoharmony
from acoharmony._puf.pfs_inventory import get_files_by_category
from acoharmony._puf.models import FileCategory, FileMetadata, DatasetInventory


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._puf.pfs_inventory is not None


class TestGetFilesByCategoryEnumInput:
    """Cover branch 231->238: category passed as non-str type (not str)."""

    @pytest.mark.unit
    def test_category_as_non_str_skips_conversion(self):
        """When category is not a str (e.g., a mock enum), skip str conversion branch.
        Branch 231->238: isinstance(category, str) is False -> jump to line 238."""
        # Use a sentinel object that is NOT a str subclass
        sentinel_category = MagicMock()
        sentinel_category.__eq__ = lambda self, other: other is sentinel_category

        mock_file = MagicMock(spec=FileMetadata)
        mock_file.category = sentinel_category

        mock_rule = MagicMock()
        mock_rule.files = {"file1": mock_file}

        mock_year_inv = MagicMock()
        mock_year_inv.rules = {"Final": mock_rule}

        mock_inventory = MagicMock(spec=DatasetInventory)
        mock_inventory.list_available_years.return_value = ["2024"]
        mock_inventory.get_year.return_value = mock_year_inv

        with patch("acoharmony._puf.pfs_inventory.get_inventory", return_value=mock_inventory):
            results = get_files_by_category(sentinel_category)
        assert len(results) == 1
        assert results[0] == ("2024", "Final", mock_file)
