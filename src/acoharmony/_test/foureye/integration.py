"""Tests for foureye module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from unittest.mock import patch

import pytest


@pytest.mark.integration
class TestFourICLIIntegration:
    """Integration tests for full workflow."""

    @pytest.mark.unit
    @patch('subprocess.run')
    def test_download_and_track_workflow(self, mock_run, mock_config, mock_log_writer, temp_bronze_dir) -> None:
        """Full download and state tracking workflow."""
        mock_run.return_value.returncode = 0

        def create_file(*args, **kwargs):
            file = temp_bronze_dir / 'CCLF8.D240101.T1234567.zip'
            file.write_text('mock content')
            return mock_run.return_value
        mock_run.side_effect = create_file
        cli = FourICLI(config=mock_config, log_writer=mock_log_writer)
        result1 = cli.download(category=DataHubCategory.CCLF)
        assert result1.success
        assert len(result1.files_downloaded) > 0
        result2 = cli.download(category=DataHubCategory.CCLF)
        assert result2.success
        assert len(result2.files_downloaded) == 0
