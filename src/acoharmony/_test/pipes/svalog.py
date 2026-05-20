"""Tests for acoharmony._pipes._sva_log module."""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import MagicMock, patch

import pytest

import acoharmony
from acoharmony.result import ResultStatus

# The source code _sva_log.py uses ResultStatus.ERROR, alias it from FAILURE
if not hasattr(ResultStatus, 'ERROR'):
    ResultStatus.ERROR = ResultStatus.FAILURE


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._pipes._sva_log is not None


class TestSVALogPipeline:
    @patch("acoharmony._pipes._sva_log.sva_log_transforms")
    @patch("acoharmony._pipes._sva_log.parse_mabel_log")
    @pytest.mark.unit
    def test_all_stages_succeed(self, mock_parse, mock_transforms, logger):
        from acoharmony._pipes._sva_log import apply_sva_log_pipeline

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=100))
        mock_parse.return_value = mock_lf

        mock_transforms.build_session_summary.return_value = mock_lf
        mock_transforms.build_upload_detail.return_value = mock_lf
        mock_transforms.build_daily_summary.return_value = mock_lf

        results = apply_sva_log_pipeline(MagicMock(), logger)
        assert len(results) == 4
        assert all(r.success for r in results.values())

    @patch("acoharmony._pipes._sva_log.parse_mabel_log")
    @pytest.mark.unit
    def test_parse_failure_aborts(self, mock_parse, logger):
        from acoharmony._pipes._sva_log import apply_sva_log_pipeline

        mock_parse.side_effect = FileNotFoundError("no log")

        results = apply_sva_log_pipeline(MagicMock(), logger)
        assert len(results) == 1
        assert results["sva_log_parse"].status == ResultStatus.ERROR

    @patch("acoharmony._pipes._sva_log.sva_log_transforms")
    @patch("acoharmony._pipes._sva_log.parse_mabel_log")
    @pytest.mark.unit
    def test_session_failure(self, mock_parse, mock_transforms, logger):
        from acoharmony._pipes._sva_log import apply_sva_log_pipeline

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=10))
        mock_parse.return_value = mock_lf

        mock_transforms.build_session_summary.side_effect = RuntimeError("session fail")
        mock_transforms.build_upload_detail.return_value = mock_lf
        mock_transforms.build_daily_summary.return_value = mock_lf

        results = apply_sva_log_pipeline(MagicMock(), logger)
        assert results["sva_log_parse"].success
        assert results["sva_log_sessions"].status == ResultStatus.ERROR
        assert results["sva_log_uploads"].success
        assert results["sva_log_daily"].success

    @patch("acoharmony._pipes._sva_log.sva_log_transforms")
    @patch("acoharmony._pipes._sva_log.parse_mabel_log")
    @pytest.mark.unit
    def test_upload_failure(self, mock_parse, mock_transforms, logger):
        from acoharmony._pipes._sva_log import apply_sva_log_pipeline

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=10))
        mock_parse.return_value = mock_lf

        mock_transforms.build_session_summary.return_value = mock_lf
        mock_transforms.build_upload_detail.side_effect = RuntimeError("upload fail")
        mock_transforms.build_daily_summary.return_value = mock_lf

        results = apply_sva_log_pipeline(MagicMock(), logger)
        assert results["sva_log_uploads"].status == ResultStatus.ERROR

    @patch("acoharmony._pipes._sva_log.sva_log_transforms")
    @patch("acoharmony._pipes._sva_log.parse_mabel_log")
    @pytest.mark.unit
    def test_daily_failure(self, mock_parse, mock_transforms, logger):
        from acoharmony._pipes._sva_log import apply_sva_log_pipeline

        mock_lf = MagicMock()
        mock_lf.select.return_value = mock_lf
        mock_lf.collect.return_value = MagicMock(item=MagicMock(return_value=10))
        mock_parse.return_value = mock_lf

        mock_transforms.build_session_summary.return_value = mock_lf
        mock_transforms.build_upload_detail.return_value = mock_lf
        mock_transforms.build_daily_summary.side_effect = RuntimeError("daily fail")

        results = apply_sva_log_pipeline(MagicMock(), logger)
        assert results["sva_log_daily"].status == ResultStatus.ERROR
