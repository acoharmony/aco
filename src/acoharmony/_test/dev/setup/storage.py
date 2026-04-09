"""Tests for acoharmony._dev.setup.storage module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._dev.setup.storage import create_local_structure, setup_storage


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.setup.storage is not None


class TestCreateLocalStructureDocSymlink:
    """Cover branch 74->78: elif on line 74 is False, falls to line 78."""

    @pytest.mark.unit
    def test_docs_symlink_created_when_target_does_not_exist(self, tmp_path):
        """When docs_target does not exist (not a symlink, not a file), it is created."""
        base_path = tmp_path / "data"
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        # Create the docs source that the function checks
        docs_source = tmp_path / "docs_src"
        docs_source.mkdir()

        # We need to patch the hard-coded paths inside the function
        docs_target = tmp_path / "docs_target"
        # docs_target does NOT exist initially -> line 74 elif is False -> line 78 is True

        with patch(
            "acoharmony._dev.setup.storage.Path",
            side_effect=lambda x: {
                "/home/care/acoharmony/docs": docs_source,
                "/home/care/docs": docs_target,
            }.get(x, Path(x)),
        ):
            create_local_structure(base_path, symlink_to=workspace)

        # The docs symlink should have been created
        assert docs_target.is_symlink()
        assert docs_target.resolve() == docs_source.resolve()

    @pytest.mark.unit
    def test_docs_target_is_non_dir_file_no_symlink_created(self, tmp_path):
        """When docs_target exists as a regular file (not dir), warn and do not create symlink."""
        base_path = tmp_path / "data"
        workspace = tmp_path / "workspace"
        workspace.mkdir()

        docs_source = tmp_path / "docs_src"
        docs_source.mkdir()

        # docs_target exists as a regular file (not dir, not symlink)
        docs_target = tmp_path / "docs_target"
        docs_target.write_text("I am a file")

        with patch(
            "acoharmony._dev.setup.storage.Path",
            side_effect=lambda x: {
                "/home/care/acoharmony/docs": docs_source,
                "/home/care/docs": docs_target,
            }.get(x, Path(x)),
        ):
            create_local_structure(base_path, symlink_to=workspace)

        # docs_target should still be a regular file (not replaced by symlink)
        assert docs_target.is_file()
        assert not docs_target.is_symlink()


class TestSetupStorageFallthrough:
    """Cover branch 300->304: backend is not local/s3api/databricks/s3+create_bucket."""

    @pytest.mark.unit
    def test_unknown_backend_falls_through_to_verify(self):
        """When backend is 'duckdb', none of the if/elif branches match; verify_storage is called."""
        mock_config = MagicMock()
        mock_config.get_storage_type.return_value = "duckdb"
        mock_config.get_environment.return_value = "test"
        mock_config.get_path.return_value = Path("/tmp/fake")
        mock_config.get_connection_params.return_value = {"database": "test.db"}

        with patch(
            "acoharmony._dev.setup.storage.StorageBackend",
            return_value=mock_config,
        ):
            with patch(
                "acoharmony._dev.setup.storage.verify_storage"
            ) as mock_verify:
                setup_storage(profile="local", create_bucket=False, dry_run=False)
                mock_verify.assert_called_once_with(mock_config)

    @pytest.mark.unit
    def test_s3_without_create_bucket_falls_through(self):
        """When backend is 's3' but create_bucket=False, no s3-specific branch runs."""
        mock_config = MagicMock()
        mock_config.get_storage_type.return_value = "s3"
        mock_config.get_environment.return_value = "test"
        mock_config.get_path.return_value = Path("/tmp/fake")
        mock_config.get_connection_params.return_value = {}

        with patch(
            "acoharmony._dev.setup.storage.StorageBackend",
            return_value=mock_config,
        ):
            with patch(
                "acoharmony._dev.setup.storage.verify_storage"
            ) as mock_verify:
                setup_storage(profile="staging", create_bucket=False, dry_run=False)
                mock_verify.assert_called_once_with(mock_config)
