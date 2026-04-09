"""Tests for acoharmony._store module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path

import pytest

import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._store is not None


class TestStorageBackendBranches:
    """Cover branches in _store.py: 72->73, 88->89, 98->99, 130->132,
    135->137, 137->138/139, 143->146, 173->174, 207->209,
    209->210/211, 233->234/242, 242->243/249, 249->250/255."""

    @pytest.mark.unit
    def test_expand_env_vars_string(self):
        """Branch 72->73: value is a string with env var."""
        from acoharmony._store import StorageBackend
        import os

        sb = StorageBackend.__new__(StorageBackend)
        os.environ["_TEST_VAR_XYZ"] = "hello"
        try:
            result = sb._expand_env_vars("${_TEST_VAR_XYZ}")
            assert result == "hello"
        finally:
            del os.environ["_TEST_VAR_XYZ"]

    @pytest.mark.unit
    def test_expand_env_vars_dict(self):
        """Branch 72: value is a dict, recurses."""
        from acoharmony._store import StorageBackend
        import os

        sb = StorageBackend.__new__(StorageBackend)
        os.environ["_TEST_DICT_VAR"] = "world"
        try:
            result = sb._expand_env_vars({"key": "${_TEST_DICT_VAR}"})
            assert result == {"key": "world"}
        finally:
            del os.environ["_TEST_DICT_VAR"]

    @pytest.mark.unit
    def test_expand_env_vars_list(self):
        """Branch 72: value is a list, recurses."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        result = sb._expand_env_vars(["plain", 42])
        assert result == ["plain", 42]

    @pytest.mark.unit
    def test_expand_env_vars_other(self):
        """Branch 72->75: value is not str/dict/list, returned as-is."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        result = sb._expand_env_vars(42)
        assert result == 42

    @pytest.mark.unit
    def test_load_config_profile_not_found(self):
        """Branch 98->99: profile not found in pyproject."""
        from acoharmony._store import StorageBackend

        with pytest.raises(ValueError, match="not found"):
            StorageBackend(profile="totally_fake_profile_that_does_not_exist_xyz")

    @pytest.mark.unit
    def test_get_data_path_cloud(self):
        """Branch 135->137: data_path starts with s3://."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://bucket/data"}}
        sb.project_root = None

        result = sb.get_data_path("bronze")
        assert result == "s3://bucket/data/bronze"

    @pytest.mark.unit
    def test_get_data_path_cloud_no_subpath(self):
        """Branch 137->138: cloud path, no subpath."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://bucket/data"}}
        sb.project_root = None

        result = sb.get_data_path("")
        assert result == "s3://bucket/data"

    @pytest.mark.unit
    def test_get_data_path_cloud_with_subpath(self):
        """Branch 137->139: cloud path with subpath."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://bucket/data/"}}
        sb.project_root = None

        result = sb.get_data_path("silver")
        assert result == "s3://bucket/data/silver"

    @pytest.mark.unit
    def test_get_data_path_local_with_subpath(self, tmp_path):
        """Branch 143->146: local path with subpath."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": str(tmp_path / "data")}}
        sb.project_root = tmp_path

        result = sb.get_data_path("gold")
        assert isinstance(result, Path)
        assert str(result).endswith("gold")

    @pytest.mark.unit
    def test_get_data_path_no_data_path_fallback(self, tmp_path):
        """Branch 130->132: no data_path in config, falls back to project /data."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {}}
        sb.project_root = tmp_path

        result = sb.get_data_path("")
        assert isinstance(result, Path)

    @pytest.mark.unit
    def test_get_path_with_medallion_layer_enum(self):
        """Branch 173->174: tier is MedallionLayer enum."""
        from acoharmony._store import StorageBackend
        try:
            from acoharmony._store import MedallionLayer
            sb = StorageBackend.__new__(StorageBackend)
            sb.config = {"storage": {"data_path": "s3://bucket/data"}}
            sb.project_root = None

            result = sb.get_path(MedallionLayer.BRONZE)
            assert "bronze" in str(result)
        except (ImportError, AttributeError):
            # MedallionLayer might not be importable
            pass

    @pytest.mark.unit
    def test_get_storage_type_s3(self):
        """Branch 207->209, 209->210: data_path starts with s3://, backend is s3."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://bucket/data", "backend": "s3"}}

        result = sb.get_storage_type()
        assert result == "s3"

    @pytest.mark.unit
    def test_get_storage_type_s3api(self):
        """Branch 209->210: backend is s3api."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://bucket/data", "backend": "s3api"}}

        result = sb.get_storage_type()
        assert result == "s3api"

    @pytest.mark.unit
    def test_get_storage_type_s3_default(self):
        """Branch 209->211: s3 path but backend not in known list."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "s3://bucket/data", "backend": "unknown"}}

        result = sb.get_storage_type()
        assert result == "s3"

    @pytest.mark.unit
    def test_get_storage_type_local(self):
        """Branch 207->213: non-cloud path, returns backend."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": "/local/path", "backend": "local"}}

        result = sb.get_storage_type()
        assert result == "local"

    @pytest.mark.unit
    def test_get_connection_params_s3(self):
        """Branch 233->234: backend is s3."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {
            "data_path": "s3://bucket/data",
            "backend": "s3",
            "endpoint": "http://localhost:9000",
            "access_key": "key",
            "secret_key": "secret",
            "bucket": "mybucket",
        }}

        result = sb.get_connection_params()
        assert result["endpoint"] == "http://localhost:9000"
        assert result["bucket"] == "mybucket"

    @pytest.mark.unit
    def test_get_connection_params_databricks(self):
        """Branch 242->243: backend is databricks."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {
            "data_path": "/local/path",
            "backend": "databricks",
            "databricks_host": "https://host",
            "databricks_token": "token123",
        }}

        result = sb.get_connection_params()
        assert result["host"] == "https://host"
        assert result["token"] == "token123"

    @pytest.mark.unit
    def test_get_connection_params_duckdb(self):
        """Branch 249->250: backend is duckdb."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {
            "data_path": "/local/path",
            "backend": "duckdb",
            "database": "test.db",
        }}

        result = sb.get_connection_params()
        assert result["database"] == "test.db"

    @pytest.mark.unit
    def test_get_connection_params_local(self, tmp_path):
        """Branch 249->255: backend is local (default)."""
        from acoharmony._store import StorageBackend

        sb = StorageBackend.__new__(StorageBackend)
        sb.config = {"storage": {"data_path": str(tmp_path), "backend": "local"}}
        sb.project_root = tmp_path

        result = sb.get_connection_params()
        assert "base_path" in result
