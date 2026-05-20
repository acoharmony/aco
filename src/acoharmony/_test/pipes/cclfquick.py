"""Tests for acoharmony._pipes._cclf_quick module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock

import pytest

import acoharmony
from acoharmony._test.pipes import _make_executor


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        import acoharmony._pipes._cclf_quick
        assert acoharmony._pipes._cclf_quick is not None


class TestCCLFQuickPipeline:
    @pytest.mark.unit
    def test_successful_run(self, tmp_path):
        import sys

        executor = _make_executor(tmp_path)
        executor.profile_config = {"polars": {"streaming_chunk_size": 1000, "max_threads": 4}}

        mock_ap = MagicMock()
        saved = sys.modules.get("acoharmony_polars")
        try:
            sys.modules["acoharmony_polars"] = mock_ap
            from acoharmony._pipes._cclf_quick import run_cclf_quick

            run_cclf_quick(executor)

            mock_ap.configure_streaming.assert_called_once_with(1000, 4)
            mock_ap.process_medical_claim.assert_called_once()
            mock_ap.process_pharmacy_claim.assert_called_once()
            mock_ap.process_eligibility.assert_called_once()
        finally:
            if saved is None:
                sys.modules.pop("acoharmony_polars", None)
            else:
                sys.modules["acoharmony_polars"] = saved

    @pytest.mark.unit
    def test_import_error(self, tmp_path):
        import importlib
        import sys

        executor = _make_executor(tmp_path)

        # Temporarily remove acoharmony_polars from sys.modules and force re-import
        saved = sys.modules.pop("acoharmony_polars", None)
        try:
            # Set sys.modules entry to None to force ImportError
            sys.modules["acoharmony_polars"] = None

            # Need to reimport the module since Python caches the import
            from acoharmony._pipes import _cclf_quick

            importlib.reload(_cclf_quick)

            with pytest.raises(ImportError, match="acoharmony-polars"):
                _cclf_quick.run_cclf_quick(executor)
        finally:
            sys.modules.pop("acoharmony_polars", None)
            if saved is not None:
                sys.modules["acoharmony_polars"] = saved

    @pytest.mark.unit
    def test_default_profile_config(self, tmp_path):
        import sys

        executor = _make_executor(tmp_path)
        executor.profile_config = {}  # No polars config

        mock_ap = MagicMock()
        saved = sys.modules.get("acoharmony_polars")
        try:
            sys.modules["acoharmony_polars"] = mock_ap
            from acoharmony._pipes._cclf_quick import run_cclf_quick

            run_cclf_quick(executor)

            # Should use defaults: 50000 chunk, 12 threads
            mock_ap.configure_streaming.assert_called_once_with(50000, 12)
        finally:
            if saved is None:
                sys.modules.pop("acoharmony_polars", None)
            else:
                sys.modules["acoharmony_polars"] = saved
