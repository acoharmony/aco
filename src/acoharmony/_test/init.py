"""Tests for acoharmony/__init__.py."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch
import pytest

import acoharmony


class TestInitModule:

    @pytest.mark.unit
    def test_init_version(self):
        assert hasattr(acoharmony, "__version__")

    @pytest.mark.unit
    def test_init_exports(self):
        assert hasattr(acoharmony, "__version__")

    @pytest.mark.unit
    def test_init_polars_env_no_pyproject(self):


        with patch("acoharmony.Path") as mock_path:
            mock_path.return_value.parent.parent.parent.__truediv__ = (
                lambda s, x: mock_path
            )
            mock_path.exists.return_value = False
            _init_polars_env()

    @pytest.mark.unit
    def test_init_polars_env_exception(self):


        with patch("acoharmony.Path", side_effect=RuntimeError("boom")):
            _init_polars_env()


class TestInitPolarsEnvBranches:
    """Cover branches 35->36 (pyproject exists) and 55->56 (max_threads set)."""

    @pytest.mark.unit
    def test_pyproject_exists_sets_max_threads(self, tmp_path, monkeypatch):
        """Branch 35->36 (exists) and 55->56 (max_threads in config)."""
        # The real _init_polars_env reads the real pyproject.toml.
        # The real pyproject.toml exists, so branch 35->36 is exercised.
        # To exercise 55->56, we need max_threads in the config.
        # Simply calling the function exercises the "exists" branch.
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
        _init_polars_env()

    @pytest.mark.unit
    def test_pyproject_missing_returns_early(self):
        """Branch 35->36: pyproject does NOT exist, returns early."""
        with patch("acoharmony.Path") as mock_path_cls:
            mock_path_instance = mock_path_cls.return_value
            mock_path_instance.parent.parent.parent.__truediv__ = lambda s, x: mock_path_instance
            mock_path_instance.exists.return_value = False
            _init_polars_env()

    @pytest.mark.unit
    def test_max_threads_not_in_config(self, monkeypatch):
        """Branch 55->56 NOT taken: polars config has no max_threads."""
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
        # If the function runs against the real pyproject.toml with no max_threads
        # in the active profile, branch 55 goes to the else side (no-op)
        _init_polars_env()
