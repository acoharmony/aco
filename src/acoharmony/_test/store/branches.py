# © 2025 HarmonyCares
# All rights reserved.

"""
Branch coverage tests for StorageBackend (_store.py).

Covers uncovered branches:
  72->73, 88->89, 98->99, 130->132, 135->137, 137->138, 137->139,
  143->146, 173->174, 207->209, 209->210, 209->211, 233->234,
  233->242, 242->243, 242->249, 249->250, 249->255
"""

from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from acoharmony._store import StorageBackend
from acoharmony.medallion import MedallionLayer


# ---------------------------------------------------------------------------
# _expand_env_vars branches
# ---------------------------------------------------------------------------


class TestExpandEnvVarsBranches:
    """Cover _expand_env_vars list branch (72->73)."""

    @pytest.mark.unit
    def test_expand_env_vars_list_branch(self):
        """Branch 72->73: value is a list, each item is expanded."""
        sb = StorageBackend.__new__(StorageBackend)
        with patch.dict(os.environ, {"LISTVAR": "resolved"}):
            result = sb._expand_env_vars(["${LISTVAR}", "literal", "${LISTVAR}"])
        assert result == ["resolved", "literal", "resolved"]


# ---------------------------------------------------------------------------
# _load_config branches
# ---------------------------------------------------------------------------


class TestLoadConfigBranches:
    """Cover _load_config error branches (88->89, 98->99)."""

    @pytest.mark.unit
    def test_pyproject_not_found_raises(self, tmp_path):
        """Branch 88->89: FileNotFoundError when pyproject.toml missing."""
        import acoharmony._store as store_module

        # Point Path(__file__) resolution to a dir without pyproject.toml
        fake_file = tmp_path / "a" / "b" / "c" / "fake.py"
        fake_file.parent.mkdir(parents=True, exist_ok=True)
        fake_file.touch()

        sb = StorageBackend.__new__(StorageBackend)
        sb.profile = "local"

        # Patch Path(__file__) inside _load_config so package_root -> tmp_path
        # The method does: Path(__file__).parent.parent.parent
        # We patch the module-level Path so that Path(anything) returns
        # a path whose .parent.parent.parent is tmp_path (no pyproject.toml).
        original_path = Path

        class FakePath(type(Path())):
            pass

        # Simpler: patch __file__ in the module
        old_file = store_module.__file__
        store_module.__file__ = str(fake_file)
        try:
            with pytest.raises(FileNotFoundError, match="pyproject.toml not found"):
                sb._load_config()
        finally:
            store_module.__file__ = old_file

    @pytest.mark.unit
    def test_invalid_profile_raises(self, tmp_path):
        """Branch 98->99: ValueError when profile not in config."""
        import acoharmony._store as store_module

        # _load_config does: Path(__file__).parent.parent.parent / "pyproject.toml"
        # So if __file__ = tmp_path/x/y/z/fake.py, package_root = tmp_path/x
        # We need pyproject.toml at tmp_path/x/pyproject.toml
        pkg_dir = tmp_path / "x" / "y" / "z"
        pkg_dir.mkdir(parents=True, exist_ok=True)
        fake_file = pkg_dir / "fake.py"
        fake_file.touch()

        package_root = tmp_path / "x"
        pyproject = package_root / "pyproject.toml"
        pyproject.write_text(
            '[tool.acoharmony.profiles.local]\nenvironment = "local"\n'
        )

        sb = StorageBackend.__new__(StorageBackend)
        sb.profile = "nonexistent_profile"

        old_file = store_module.__file__
        store_module.__file__ = str(fake_file)
        try:
            with pytest.raises(ValueError, match="nonexistent_profile"):
                sb._load_config()
        finally:
            store_module.__file__ = old_file


# ---------------------------------------------------------------------------
# get_data_path branches
# ---------------------------------------------------------------------------


class TestGetDataPathBranches:
    """Cover get_data_path branches (130->132, 135->137, 137->138, 137->139, 143->146)."""

    @pytest.mark.unit
    def test_no_data_path_fallback(self, tmp_path):
        """Branch 130->132: data_path missing, fallback to project_root/data."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {}}
        sb.project_root = tmp_path

        result = sb.get_data_path("")
        expected = tmp_path / "data"
        assert result == expected

    @pytest.mark.unit
    def test_cloud_s3_with_subpath(self):
        """Branch 135->137 and 137->138: s3:// path with subpath."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://mybucket/data"}}
        sb.project_root = Path("/dummy")

        result = sb.get_data_path("bronze")
        assert result == "s3://mybucket/data/bronze"

    @pytest.mark.unit
    def test_cloud_s3_no_subpath(self):
        """Branch 135->137 and 137->139: s3:// path without subpath."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://mybucket/data"}}
        sb.project_root = Path("/dummy")

        result = sb.get_data_path("")
        assert result == "s3://mybucket/data"

    @pytest.mark.unit
    def test_cloud_az_with_subpath(self):
        """Branch 135->137 and 137->138: az:// path with subpath."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "az://container/path"}}
        sb.project_root = Path("/dummy")

        result = sb.get_data_path("silver")
        assert result == "az://container/path/silver"

    @pytest.mark.unit
    def test_cloud_gs_no_subpath(self):
        """Branch 135->137 and 137->139: gs:// path without subpath."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "gs://mybucket"}}
        sb.project_root = Path("/dummy")

        result = sb.get_data_path("")
        assert result == "gs://mybucket"

    @pytest.mark.unit
    def test_local_with_subpath(self, tmp_path):
        """Branch 143->146 (true): local path with subpath."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": str(tmp_path)}}
        sb.project_root = Path("/dummy")

        result = sb.get_data_path("bronze")
        assert result == tmp_path / "bronze"
        assert result.exists()

    @pytest.mark.unit
    def test_local_no_subpath(self, tmp_path):
        """Branch 143->146 (false): local path without subpath."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": str(tmp_path)}}
        sb.project_root = Path("/dummy")

        result = sb.get_data_path("")
        assert result == tmp_path


# ---------------------------------------------------------------------------
# get_path branches
# ---------------------------------------------------------------------------


class TestGetPathBranches:
    """Cover get_path MedallionLayer branch (173->174)."""

    @pytest.mark.unit
    def test_get_path_with_medallion_layer_enum(self, tmp_path):
        """Branch 173->174: tier is a MedallionLayer instance."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": str(tmp_path)}}
        sb.project_root = Path("/dummy")

        result = sb.get_path(MedallionLayer.GOLD)
        assert isinstance(result, Path)
        assert "gold" in str(result)

    @pytest.mark.unit
    def test_get_path_with_string_tier(self, tmp_path):
        """Complement: tier is a plain string."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": str(tmp_path)}}
        sb.project_root = Path("/dummy")

        result = sb.get_path("silver")
        assert isinstance(result, Path)
        assert "silver" in str(result)


# ---------------------------------------------------------------------------
# get_storage_type branches
# ---------------------------------------------------------------------------


class TestGetStorageTypeBranches:
    """Cover get_storage_type branches (207->209, 209->210, 209->211)."""

    @pytest.mark.unit
    def test_s3_data_path_known_backend_s3api(self):
        """Branch 207->209 and 209->210: s3:// path + s3api backend."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "s3api", "data_path": "s3://bucket"}}

        assert sb.get_storage_type() == "s3api"

    @pytest.mark.unit
    def test_s3_data_path_known_backend_s3(self):
        """Branch 207->209 and 209->210: s3:// path + s3 backend."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "s3", "data_path": "s3://bucket"}}

        assert sb.get_storage_type() == "s3"

    @pytest.mark.unit
    def test_s3_data_path_known_backend_databricks(self):
        """Branch 207->209 and 209->210: s3:// path + databricks backend."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "databricks", "data_path": "s3://bucket"}}

        assert sb.get_storage_type() == "databricks"

    @pytest.mark.unit
    def test_s3_data_path_unknown_backend_fallback(self):
        """Branch 207->209 and 209->211: s3:// path + unknown backend -> 's3'."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "foobar", "data_path": "s3://bucket"}}

        assert sb.get_storage_type() == "s3"

    @pytest.mark.unit
    def test_local_data_path_returns_backend(self):
        """No s3:// prefix -> returns backend directly."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "local", "data_path": "/some/path"}}

        assert sb.get_storage_type() == "local"


# ---------------------------------------------------------------------------
# get_connection_params branches
# ---------------------------------------------------------------------------


class TestGetConnectionParamsBranches:
    """Cover get_connection_params branches
    (233->234, 233->242, 242->243, 242->249, 249->250, 249->255)."""

    @pytest.mark.unit
    def test_s3_params(self):
        """Branch 233->234: backend is s3."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {
            "storage": {
                "backend": "s3",
                "data_path": "s3://bucket",
                "endpoint": "http://localhost:9000",
                "access_key": "ak",
                "secret_key": "sk",
                "bucket": "mybucket",
                "region": "us-west-2",
                "use_ssl": False,
            }
        }

        result = sb.get_connection_params()
        assert result["endpoint"] == "http://localhost:9000"
        assert result["access_key"] == "ak"
        assert result["secret_key"] == "sk"
        assert result["bucket"] == "mybucket"
        assert result["region"] == "us-west-2"
        assert result["use_ssl"] is False

    @pytest.mark.unit
    def test_s3api_params(self):
        """Branch 233->234: backend is s3api."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {
            "storage": {
                "backend": "s3api",
                "data_path": "s3://aco",
                "endpoint": "http://s3api:10001",
                "access_key": "user",
                "secret_key": "pass",
                "bucket": "aco",
            }
        }

        result = sb.get_connection_params()
        assert result["endpoint"] == "http://s3api:10001"
        # Defaults
        assert result["region"] == "us-east-1"
        assert result["use_ssl"] is True

    @pytest.mark.unit
    def test_databricks_params(self):
        """Branch 233->242 and 242->243: backend is databricks."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {
            "storage": {
                "backend": "databricks",
                "data_path": "s3://bucket",
                "databricks_host": "https://db.cloud",
                "databricks_token": "tok123",
                "catalog": "prod",
                "schema": "myschema",
            }
        }

        result = sb.get_connection_params()
        assert result["host"] == "https://db.cloud"
        assert result["token"] == "tok123"
        assert result["catalog"] == "prod"
        assert result["schema"] == "myschema"

    @pytest.mark.unit
    def test_databricks_defaults(self):
        """Branch 242->243: databricks with defaults."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {
            "storage": {
                "backend": "databricks",
                "data_path": "s3://bucket",
                "databricks_host": "host",
                "databricks_token": "tok",
            }
        }

        result = sb.get_connection_params()
        assert result["catalog"] == "main"
        assert result["schema"] == "aco_harmony"

    @pytest.mark.unit
    def test_duckdb_params(self):
        """Branch 242->249 and 249->250: backend is duckdb."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {
            "storage": {
                "backend": "duckdb",
                "data_path": "/local",
                "database": "my.db",
                "read_only": True,
            }
        }

        result = sb.get_connection_params()
        assert result["database"] == "my.db"
        assert result["read_only"] is True

    @pytest.mark.unit
    def test_duckdb_defaults(self):
        """Branch 249->250: duckdb with defaults."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "duckdb", "data_path": "/local"}}

        result = sb.get_connection_params()
        assert result["database"] == "aco_harmony.db"
        assert result["read_only"] is False

    @pytest.mark.unit
    def test_local_fallback_params(self, tmp_path):
        """Branch 249->255: unknown backend falls through to else."""
        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"backend": "local", "data_path": str(tmp_path)}}
        sb.project_root = Path("/dummy")

        result = sb.get_connection_params()
        assert "base_path" in result
