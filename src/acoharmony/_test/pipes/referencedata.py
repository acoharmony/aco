"""Tests for acoharmony._pipes._reference_data module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony._pipes._reference_data import apply_reference_data_pipeline
from acoharmony.result import ResultStatus, TransformResult


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._reference_data is not None


class TestReferenceDataPipeline:
    @patch("acoharmony._transforms._reference.transform_all_reference_data")
    @pytest.mark.unit
    def test_successful_run(self, mock_transform, logger):
        from acoharmony._pipes._reference_data import apply_reference_data_pipeline

        mock_transform.return_value = {
            "terminology_icd10": TransformResult(status=ResultStatus.SUCCESS, message="ok"),
            "terminology_hcpcs": TransformResult(status=ResultStatus.SKIPPED, message="exists"),
            "valueset_ccsr": TransformResult(status=ResultStatus.FAILURE, message="fail"),
        }

        results = apply_reference_data_pipeline(MagicMock(), logger, force=False)
        assert len(results) == 3
        assert results["terminology_icd10"].success

    @patch("acoharmony._transforms._reference.transform_all_reference_data")
    @pytest.mark.unit
    def test_force_mode(self, mock_transform, logger):
        mock_transform.return_value = {}
        apply_reference_data_pipeline(MagicMock(), logger, force=True)
        assert any("FORCE" in str(c) for c in logger.info.call_args_list)
        # Force sets overwrite=True
        mock_transform.assert_called_once()
        _, kwargs = mock_transform.call_args
        assert kwargs.get("overwrite") is True
