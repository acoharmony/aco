"""Unit tests for _store module."""
from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

from acoharmony._store import StorageBackend

if TYPE_CHECKING:
    pass

class TestStorageBackend:
    """Tests for StorageBackend."""

    @pytest.mark.unit
    def test_storagebackend_initialization(self) -> None:
        """StorageBackend can be initialized."""
        sb = StorageBackend()
        assert sb is not None
        assert sb.profile is not None
        assert sb.config is not None
        assert sb.project_root is not None

    @pytest.mark.unit
    def test_storagebackend_basic_functionality(self) -> None:
        """StorageBackend basic functionality works."""
        sb = StorageBackend()
        # get_data_path should return a valid path
        data_path = sb.get_data_path()
        assert data_path is not None
        # get_storage_type should return a string
        storage_type = sb.get_storage_type()
        assert isinstance(storage_type, str)
        # get_environment should return a string
        env = sb.get_environment()
        assert isinstance(env, str)

    @pytest.mark.unit
    def test_missing_profile_raises(self) -> None:
        with pytest.raises(ValueError, match='not found'):
            StorageBackend(profile='nonexistent_profile_xyz_99999')

    @pytest.mark.unit
    def test_pyproject_not_found_raises(self) -> None:
        """Branch 88->89: pyproject.toml does not exist, raises FileNotFoundError."""
        from unittest.mock import patch, PropertyMock

        with patch("acoharmony._store.Path.exists", return_value=False):
            with pytest.raises(FileNotFoundError, match="pyproject.toml not found"):
                StorageBackend(profile="local")
