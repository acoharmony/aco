from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import pytest

# © 2025 HarmonyCares
"""Tests for acoharmony/_exceptions/_storage.py."""



class TestStorage:
    """Test suite for _storage."""

    @pytest.mark.unit
    def test_from_initialization_error(self) -> None:
        """Test from_initialization_error function."""
        original = RuntimeError("connection failed")
        exc = StorageBackendError.from_initialization_error(original, profile="test_profile")
        assert "STORAGE BACKEND INITIALIZATION FAILED" in exc.message
        assert exc.context.original_error is original
        assert exc.context.metadata["profile"] == "test_profile"
        assert exc.context.metadata["original_error_type"] == "RuntimeError"
        assert len(exc.context.causes) > 0
        assert len(exc.context.remediation_steps) > 0

    @pytest.mark.unit
    def test_missing_profile(self) -> None:
        """Test missing_profile function."""
        exc = StorageConfigurationError.missing_profile("dev", ["local", "prod"])
        assert "dev" in exc.message
        assert exc.context.metadata["requested_profile"] == "dev"
        assert exc.context.metadata["available_profiles"] == ["local", "prod"]

    @pytest.mark.unit
    def test_path_not_found(self) -> None:
        """Test path_not_found function."""
        exc = StoragePathError.path_not_found("/data/missing", storage_type="s3")
        assert "/data/missing" in exc.message
        assert exc.context.metadata["path"] == "/data/missing"
        assert exc.context.metadata["storage_type"] == "s3"

    @pytest.mark.unit
    def test_invalid_tier(self) -> None:
        """Test invalid_tier function."""
        exc = InvalidTierError.invalid_tier("diamond")
        assert "diamond" in exc.message
        assert exc.context.metadata["invalid_tier"] == "diamond"
        assert "raw" in exc.context.metadata["valid_tiers"]
        assert len(exc.context.causes) > 0
        assert len(exc.context.remediation_steps) > 0

    @pytest.mark.unit
    def test_storagebackenderror_init(self) -> None:
        """Test StorageBackendError initialization."""
        exc = StorageBackendError("backend err", auto_log=False, auto_trace=False)
        assert exc.message == "backend err"
        assert exc.error_code == "STORAGE_001"
        assert exc.category == "storage"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_storageconfigurationerror_init(self) -> None:
        """Test StorageConfigurationError initialization."""
        exc = StorageConfigurationError("config err", auto_log=False, auto_trace=False)
        assert exc.message == "config err"
        assert exc.error_code == "STORAGE_002"
        assert exc.category == "storage"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_storageaccesserror_init(self) -> None:
        """Test StorageAccessError initialization."""
        exc = StorageAccessError("access err", auto_log=False, auto_trace=False)
        assert exc.message == "access err"
        assert exc.error_code == "STORAGE_003"
        assert exc.category == "storage"
        assert isinstance(exc, ACOHarmonyException)



if TYPE_CHECKING:
    pass


class TestStorageBackendError:
    """Tests for StorageBackendError."""

    @pytest.mark.unit
    def test_storagebackenderror_initialization(self) -> None:
        """StorageBackendError can be initialized."""
        exc = StorageBackendError("init fail", auto_log=False, auto_trace=False)
        assert exc.message == "init fail"
        assert isinstance(exc, ACOHarmonyException)

    @pytest.mark.unit
    def test_storagebackenderror_basic_functionality(self) -> None:
        """StorageBackendError basic functionality works."""
        exc = StorageBackendError.from_initialization_error(
            RuntimeError("fail"), profile="local"
        )
        assert isinstance(exc, StorageBackendError)
        assert exc.context.original_error is not None

class TestStorageConfigurationError:
    """Tests for StorageConfigurationError."""


    @pytest.mark.unit
    def test_storageconfigurationerror_initialization(self) -> None:
        """StorageConfigurationError can be initialized."""
        exc = StorageConfigurationError("cfg err", auto_log=False, auto_trace=False)
        assert exc.message == "cfg err"
        assert exc.error_code == "STORAGE_002"

    @pytest.mark.unit
    def test_storageconfigurationerror_basic_functionality(self) -> None:
        """StorageConfigurationError basic functionality works."""
        exc = StorageConfigurationError.missing_profile("dev", ["local"])
        assert "dev" in exc.message
        assert isinstance(exc, StorageConfigurationError)

class TestStorageAccessError:
    """Tests for StorageAccessError."""


    @pytest.mark.unit
    def test_storageaccesserror_initialization(self) -> None:
        """StorageAccessError can be initialized."""
        exc = StorageAccessError("access fail", auto_log=False, auto_trace=False)
        assert exc.message == "access fail"
        assert exc.error_code == "STORAGE_003"

    @pytest.mark.unit
    def test_storageaccesserror_basic_functionality(self) -> None:
        """StorageAccessError basic functionality works."""
        with pytest.raises(StorageAccessError):
            raise StorageAccessError("no access", auto_log=False, auto_trace=False)
        exc = StorageAccessError("a", auto_log=False, auto_trace=False)
        assert isinstance(exc, ACOHarmonyException)

class TestStoragePathError:
    """Tests for StoragePathError."""


    @pytest.mark.unit
    def test_storagepatherror_initialization(self) -> None:
        """StoragePathError can be initialized."""
        exc = StoragePathError("path err", auto_log=False, auto_trace=False)
        assert exc.message == "path err"
        assert exc.error_code == "STORAGE_004"

    @pytest.mark.unit
    def test_storagepatherror_basic_functionality(self) -> None:
        """StoragePathError basic functionality works."""
        exc = StoragePathError.path_not_found("/missing/path", "local")
        assert "/missing/path" in exc.message
        assert isinstance(exc, StoragePathError)

class TestInvalidTierError:
    """Tests for InvalidTierError."""


    @pytest.mark.unit
    def test_invalidtiererror_initialization(self) -> None:
        """InvalidTierError can be initialized."""
        exc = InvalidTierError("tier err", auto_log=False, auto_trace=False)
        assert exc.message == "tier err"
        assert exc.error_code == "STORAGE_005"

    @pytest.mark.unit
    def test_invalidtiererror_basic_functionality(self) -> None:
        """InvalidTierError basic functionality works."""
        exc = InvalidTierError.invalid_tier("diamond")
        assert "diamond" in exc.message
        assert isinstance(exc, InvalidTierError)
