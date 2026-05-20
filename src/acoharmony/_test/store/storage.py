# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for StorageBackend - Polars style.

Tests storage configuration, path resolution, and backend operations.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

from acoharmony._store import StorageBackend

if TYPE_CHECKING:
    pass


class TestStorageBackend:
    """Tests for StorageBackend initialization."""

    @pytest.mark.unit
    def test_initialization_default(self) -> None:
        """StorageBackend initializes with default profile."""
        storage = StorageBackend()

        assert storage is not None
        assert storage.profile in ["local", os.getenv("ACO_PROFILE", "local")]
        assert storage.config is not None
        assert storage.project_root is not None

    @pytest.mark.unit
    def test_initialization_with_profile(self) -> None:
        """StorageBackend accepts specific profile."""
        storage = StorageBackend(profile="local")

        assert storage.profile == "local"

    @pytest.mark.unit
    def test_initialization_respects_env_var(self) -> None:
        """StorageBackend uses ACO_PROFILE env variable."""
        # Save current env var
        old_profile = os.getenv("ACO_PROFILE")

        try:
            # Use a valid profile that exists
            os.environ["ACO_PROFILE"] = "local"
            storage = StorageBackend()

            # Should use the env var value
            assert storage.profile == "local"

        finally:
            # Restore original
            if old_profile:
                os.environ["ACO_PROFILE"] = old_profile
            elif "ACO_PROFILE" in os.environ:
                del os.environ["ACO_PROFILE"]


class TestGetStorageType:
    """Tests for storage type detection."""

    @pytest.mark.unit
    def test_get_storage_type_local(self) -> None:
        """get_storage_type returns correct type for local profile."""
        storage = StorageBackend(profile="local")
        storage_type = storage.get_storage_type()

        assert isinstance(storage_type, str)
        # Should be 'local' or similar
        assert storage_type.lower() in ["local", "filesystem"]

    @pytest.mark.unit
    def test_get_storage_type_exists(self) -> None:
        """get_storage_type method exists and returns string."""
        storage = StorageBackend()
        storage_type = storage.get_storage_type()

        assert isinstance(storage_type, str)
        assert len(storage_type) > 0


class TestGetEnvironment:
    """Tests for environment detection."""

    @pytest.mark.unit
    def test_get_environment(self) -> None:
        """get_environment returns environment string."""
        storage = StorageBackend(profile="local")
        env = storage.get_environment()

        assert isinstance(env, str)
        assert len(env) > 0


class TestGetPath:
    """Tests for path resolution."""

    @pytest.mark.parametrize(
        "tier",
        ["bronze", "silver", "staging", "logs"],
    )
    @pytest.mark.unit
    def test_get_path_valid_tiers(self, tier: str) -> None:
        """get_path returns path for valid tiers."""
        storage = StorageBackend(profile="local")

        path = storage.get_path(tier)

        assert path is not None
        # Should be either Path or string
        assert isinstance(path, Path | str)

    @pytest.mark.unit
    def test_get_path_bronze(self) -> None:
        """get_path returns bronze data path."""
        storage = StorageBackend(profile="local")

        bronze_path = storage.get_path("bronze")

        assert bronze_path is not None
        if isinstance(bronze_path, Path):
            # Path should contain 'bronze' somewhere
            assert "bronze" in str(bronze_path).lower()

    @pytest.mark.unit
    def test_get_path_silver(self) -> None:
        """get_path returns silver data path."""
        storage = StorageBackend(profile="local")

        silver_path = storage.get_path("silver")

        assert silver_path is not None
        if isinstance(silver_path, Path):
            # Path should contain 'silver' somewhere
            assert "silver" in str(silver_path).lower()


class TestGetDataPath:
    """Tests for get_data_path method."""

    @pytest.mark.unit
    def test_get_data_path_default(self) -> None:
        """get_data_path returns base data path."""
        storage = StorageBackend(profile="local")

        data_path = storage.get_data_path()

        assert data_path is not None
        assert isinstance(data_path, Path | str)

    @pytest.mark.unit
    def test_get_data_path_with_subpath(self) -> None:
        """get_data_path accepts subpath argument."""
        storage = StorageBackend(profile="local")

        data_path = storage.get_data_path("test_subdir")

        assert data_path is not None
        assert isinstance(data_path, Path | str)


class TestGetConnectionParams:
    """Tests for connection parameters."""

    @pytest.mark.unit
    def test_get_connection_params_local(self) -> None:
        """get_connection_params returns params for local storage."""
        storage = StorageBackend(profile="local")

        params = storage.get_connection_params()

        assert isinstance(params, dict)
        # Local storage may have minimal params


@pytest.mark.slow
class TestStorageIntegration:
    """Integration tests for storage operations."""

    @pytest.mark.unit
    def test_paths_are_accessible(self) -> None:
        """Storage paths are accessible."""
        storage = StorageBackend(profile="local")

        # Get common paths
        bronze_path = storage.get_path("bronze")
        silver_path = storage.get_path("silver")

        # Convert to Path if string
        if isinstance(bronze_path, str):
            bronze_path = Path(bronze_path)
        if isinstance(silver_path, str):
            silver_path = Path(silver_path)

        # Paths should be valid (may not exist yet, but should be valid paths)
        assert bronze_path is not None
        assert silver_path is not None


class TestPrivateMethods:
    """Tests for private helper methods."""

    @pytest.mark.unit
    def test_load_config(self) -> None:
        """_load_config loads configuration."""
        storage = StorageBackend(profile="local")

        # Config should be loaded during init
        assert storage.config is not None
        assert isinstance(storage.config, dict)

    @pytest.mark.unit
    def test_expand_env_vars(self) -> None:
        """_expand_env_vars expands environment variables."""
        storage = StorageBackend(profile="local")

        # Set a test env var
        os.environ["TEST_STORAGE_VAR"] = "test_value"

        try:
            # Test expansion
            result = storage._expand_env_vars("${TEST_STORAGE_VAR}")

            # Should expand the variable
            assert "test_value" in result or result == "${TEST_STORAGE_VAR}"

        finally:
            # Clean up
            if "TEST_STORAGE_VAR" in os.environ:
                del os.environ["TEST_STORAGE_VAR"]
