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
    def test_init_polars_env_no_aco_toml(self, monkeypatch):
        """When load_aco_config raises, _init_polars_env degrades silently."""
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
        with patch(
            "acoharmony._config_loader.load_aco_config",
            side_effect=FileNotFoundError("aco.toml missing"),
        ):
            _init_polars_env()  # must not raise
        assert "POLARS_MAX_THREADS" not in __import__("os").environ

    @pytest.mark.unit
    def test_init_polars_env_exception(self, monkeypatch):
        """Any exception from load_aco_config is swallowed."""
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
        with patch(
            "acoharmony._config_loader.load_aco_config",
            side_effect=RuntimeError("boom"),
        ):
            _init_polars_env()  # must not raise


class TestInitPolarsEnvBranches:
    """Cover the real aco.toml path + the max_threads setter."""

    @pytest.mark.unit
    def test_aco_toml_sets_max_threads(self, monkeypatch):
        """When the profile has max_threads, POLARS_MAX_THREADS is set."""
        import os
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
        fake_config = {
            "default_profile": "dev",
            "profiles": {"dev": {"polars": {"max_threads": 7}}},
        }
        with patch(
            "acoharmony._config_loader.load_aco_config",
            return_value=fake_config,
        ):
            _init_polars_env()
        assert os.environ.get("POLARS_MAX_THREADS") == "7"
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)

    @pytest.mark.unit
    def test_max_threads_already_set_is_preserved(self, monkeypatch):
        """If POLARS_MAX_THREADS is already in env, the loader does not clobber it."""
        import os
        monkeypatch.setenv("POLARS_MAX_THREADS", "42")
        fake_config = {
            "default_profile": "dev",
            "profiles": {"dev": {"polars": {"max_threads": 7}}},
        }
        with patch(
            "acoharmony._config_loader.load_aco_config",
            return_value=fake_config,
        ):
            _init_polars_env()
        assert os.environ["POLARS_MAX_THREADS"] == "42"

    @pytest.mark.unit
    def test_max_threads_not_in_config(self, monkeypatch):
        """When polars config has no max_threads, nothing is set."""
        import os
        monkeypatch.delenv("POLARS_MAX_THREADS", raising=False)
        fake_config = {
            "default_profile": "dev",
            "profiles": {"dev": {"polars": {}}},
        }
        with patch(
            "acoharmony._config_loader.load_aco_config",
            return_value=fake_config,
        ):
            _init_polars_env()
        assert "POLARS_MAX_THREADS" not in os.environ
