"""Unit tests for 4icli file processor."""
from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock, patch
from zipfile import ZipFile

import pytest
from acoharmony._test.foureye.conftest import _make_config  # noqa: F401


@pytest.fixture
def processor_config(mock_config, tmp_path):
    """Create a config for FileProcessor tests with logging disabled."""
    mock_config.enable_logging = False
    mock_config.download_dir = tmp_path / 'download'
    mock_config.bronze_dir = tmp_path / 'bronze'
    mock_config.archive_dir = tmp_path / 'archive'
    mock_config.silver_dir = tmp_path / 'silver'
    mock_config.gold_dir = tmp_path / 'gold'
    return mock_config

class TestFileProcessorInitialization:
    """Tests for FileProcessor initialization."""

    @pytest.mark.unit
    def test_init_with_config(self, mock_config, tmp_path) -> None:
        """FileProcessor initializes with config."""
        log_dir = tmp_path / 'logs'
        log_dir.mkdir(parents=True, exist_ok=True)
        mock_config.log_dir = log_dir
        mock_config.enable_logging = True
        processor = FileProcessor(config=mock_config)
        assert processor.config is mock_config

    @pytest.mark.unit
    def test_init_without_config_uses_default(self, tmp_path) -> None:
        """FileProcessor creates default config when none provided."""
        with patch('acoharmony._4icli.processor.FourICLIConfig.from_profile') as mock_from_profile:
            mock_config = MagicMock()
            mock_config.enable_logging = False
            mock_config.log_dir = tmp_path / 'logs'
            mock_from_profile.return_value = mock_config
            processor = FileProcessor(config=None)
            mock_from_profile.assert_called_once()
            assert processor.config is not None

class TestFileProcessorCCLFZipExtraction:
    """Tests for CCLF ZIP file extraction."""

    @pytest.mark.unit
    def test_process_cclf_zip_extracts_files(self, processor_config, tmp_path) -> None:
        """CCLF ZIP files are extracted to appropriate directory."""
        zip_path = tmp_path / 'download' / 'P.D0259.ACO.ZCY25.D250210.T1550060.zip'
        zip_path.parent.mkdir(parents=True, exist_ok=True)
        temp_files = tmp_path / 'temp'
        temp_files.mkdir()
        (temp_files / 'CCLF8.D250210.T1550060').write_text('CCLF8 data')
        (temp_files / 'CCLF9.D250210.T1550060').write_text('CCLF9 data')
        with ZipFile(zip_path, 'w') as zf:
            zf.write(temp_files / 'CCLF8.D250210.T1550060', 'CCLF8.D250210.T1550060')
            zf.write(temp_files / 'CCLF9.D250210.T1550060', 'CCLF9.D250210.T1550060')
        processor = FileProcessor(config=processor_config)
        result = processor._process_cclf_zip(zip_path)
        assert len(result) == 2
        assert any('CCLF8' in str(f) for f in result)
        assert any('CCLF9' in str(f) for f in result)

    @pytest.mark.unit
    def test_process_cclf_zip_bad_zip_raises(self, processor_config, tmp_path) -> None:
        """Invalid ZIP files raise error."""
        bad_zip = tmp_path / 'bad.zip'
        bad_zip.write_text('not a zip file')
        processor = FileProcessor(config=processor_config)
        with pytest.raises(Exception, match='.*'):
            processor._process_cclf_zip(bad_zip)

class TestFileProcessorAlignmentFiles:
    """Tests for alignment file processing (PALMR, PBVAR, TPARC)."""

    @pytest.mark.integration
    def test_process_alignment_file_palmr(self, processor_config, tmp_path) -> None:
        """PALMR files are moved to correct directory."""
        source_file = tmp_path / 'download' / 'P.D0259.PALMR.D250123.T1741300.csv'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text('PALMR data')
        # Use separate dirs to avoid SameFileError (get_alignment_dir and get_report_dir both return bronze_dir)
        report_dir = tmp_path / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        processor_config.get_report_dir = lambda report_type: report_dir
        processor = FileProcessor(config=processor_config)
        result = processor._process_alignment_file(source_file, 'PALMR')
        assert len(result) == 1

    @pytest.mark.integration
    def test_process_alignment_file_pbvar(self, processor_config, tmp_path) -> None:
        """PBVAR files are moved to correct directory."""
        source_file = tmp_path / 'download' / 'P.D0259.PBVAR.D250122.T0112000.xlsx'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text('PBVAR data')
        report_dir = tmp_path / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        processor_config.get_report_dir = lambda report_type: report_dir
        processor = FileProcessor(config=processor_config)
        result = processor._process_alignment_file(source_file, 'PBVAR')
        assert len(result) == 1

    @pytest.mark.integration
    def test_process_alignment_file_tparc(self, processor_config, tmp_path) -> None:
        """TPARC files are moved to correct directory."""
        source_file = tmp_path / 'download' / 'P.D0259.TPARC.RP.D250103.T1111012.txt'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text('TPARC data')
        report_dir = tmp_path / 'reports'
        report_dir.mkdir(parents=True, exist_ok=True)
        processor_config.get_report_dir = lambda report_type: report_dir
        processor = FileProcessor(config=processor_config)
        result = processor._process_alignment_file(source_file, 'TPARC')
        assert len(result) == 1

class TestFileProcessorRouting:
    """Tests for file type routing logic."""

    @pytest.mark.unit
    def test_process_file_routes_palmr(self, processor_config, tmp_path) -> None:
        """PALMR files are routed to alignment processor."""
        source_file = tmp_path / 'download' / 'P.D0259.PALMR.D250123.T1741300.csv'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text('PALMR data')
        processor = FileProcessor(config=processor_config)
        with patch.object(processor, '_process_alignment_file') as mock_process:
            mock_process.return_value = {}
            processor.process_file(source_file)
            mock_process.assert_called_once()
            call_args = mock_process.call_args
            assert call_args[0][1] == 'PALMR'

    @pytest.mark.unit
    def test_process_file_routes_pbvar(self, processor_config, tmp_path) -> None:
        """PBVAR files are routed to alignment processor."""
        source_file = tmp_path / 'download' / 'P.D0259.PBVAR.D250122.T0112000.xlsx'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text('PBVAR data')
        processor = FileProcessor(config=processor_config)
        with patch.object(processor, '_process_alignment_file') as mock_process:
            mock_process.return_value = {}
            processor.process_file(source_file)
            mock_process.assert_called_once()
            call_args = mock_process.call_args
            assert call_args[0][1] == 'PBVAR'

    @pytest.mark.unit
    def test_process_file_routes_cclf_zip(self, processor_config, tmp_path) -> None:
        """CCLF ZIP files are routed to ZIP processor."""
        source_file = tmp_path / 'download' / 'CCLF8.D250210.T1550060.zip'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        with ZipFile(source_file, 'w') as zf:
            zf.writestr('test.txt', 'data')
        processor = FileProcessor(config=processor_config)
        with patch.object(processor, '_process_cclf_zip') as mock_process:
            mock_process.return_value = []
            processor.process_file(source_file)
            mock_process.assert_called_once()

    @pytest.mark.unit
    def test_process_file_routes_unknown_file(self, processor_config, tmp_path) -> None:
        """Unknown files are routed to other processor."""
        source_file = tmp_path / 'download' / 'unknown_file.xyz'
        source_file.parent.mkdir(parents=True, exist_ok=True)
        source_file.write_text('unknown data')
        processor = FileProcessor(config=processor_config)
        with patch.object(processor, '_process_other_file') as mock_process:
            mock_process.return_value = {}
            processor.process_file(source_file)
            mock_process.assert_called_once()

class TestFileProcessorBatchProcessing:
    """Tests for batch file processing."""

    @pytest.mark.unit
    def test_process_all_processes_multiple_files(self, processor_config, tmp_path) -> None:
        """process_all handles multiple files."""
        download_dir = tmp_path / 'download'
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / 'file1.csv').write_text('data1')
        (download_dir / 'file2.txt').write_text('data2')
        (download_dir / 'file3.xlsx').write_text('data3')
        processor = FileProcessor(config=processor_config)
        with patch.object(processor, 'process_file') as mock_process:
            mock_process.return_value = {'moved': {}, 'extracted': []}
            result = processor.process_all()
            assert mock_process.call_count == 3
            assert result.file_count == 3
            assert result.success

    @pytest.mark.unit
    def test_process_all_handles_errors(self, processor_config, tmp_path) -> None:
        """process_all captures processing errors."""
        download_dir = tmp_path / 'download'
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / 'bad_file.txt').write_text('data')
        processor = FileProcessor(config=processor_config)
        with patch.object(processor, 'process_file') as mock_process:
            mock_process.side_effect = Exception('Processing failed')
            result = processor.process_all()
            assert len(result.errors) == 1
            assert not result.success
            assert 'Processing failed' in result.errors[0]

class TestFileProcessorCleanup:
    """Tests for cleanup operations."""

    @pytest.mark.unit
    def test_cleanup_download_dir_removes_files(self, processor_config, tmp_path) -> None:
        """cleanup_download_dir removes files when keep_files=False."""
        download_dir = tmp_path / 'download'
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / 'file1.txt').write_text('data1')
        (download_dir / 'file2.txt').write_text('data2')
        processor = FileProcessor(config=processor_config)
        count = processor.cleanup_download_dir(keep_files=False)
        assert count == 2
        assert len(list(download_dir.glob('*'))) == 0

    @pytest.mark.unit
    def test_cleanup_download_dir_keeps_files(self, processor_config, tmp_path) -> None:
        """cleanup_download_dir keeps files when keep_files=True."""
        download_dir = tmp_path / 'download'
        download_dir.mkdir(parents=True, exist_ok=True)
        (download_dir / 'file1.txt').write_text('data1')
        processor = FileProcessor(config=processor_config)
        count = processor.cleanup_download_dir(keep_files=True)
        assert count == 1
        assert (download_dir / 'file1.txt').exists()

class TestProcessingResult2:
    """Tests for ProcessingResult dataclass."""

    @pytest.mark.unit
    def test_processing_result_success(self) -> None:
        """ProcessingResult.success returns True when no errors."""
        from datetime import datetime
        result = ProcessingResult(files_processed=[Path('file1.txt')], files_moved={}, files_extracted=[], errors=[], started_at=datetime.now())
        assert result.success
        assert result.file_count == 1

    @pytest.mark.unit
    def test_processing_result_failure(self) -> None:
        """ProcessingResult.success returns False when errors present."""
        from datetime import datetime
        result = ProcessingResult(files_processed=[Path('file1.txt')], files_moved={}, files_extracted=[], errors=['Error 1', 'Error 2'], started_at=datetime.now())
        assert not result.success
        assert len(result.errors) == 2


class TestFileProcessor:
    @pytest.fixture
    def processor(self, tmp_path):
        config = _make_config(tmp_path)
        config.download_dir = config.bronze_dir  # processor uses download_dir
        config.raw_data_dir = tmp_path / "raw"
        config.raw_data_dir.mkdir(parents=True, exist_ok=True)

        with patch("acoharmony._4icli.processor.FourICLIConfig.from_profile", return_value=config):
            from acoharmony._4icli.processor import FileProcessor
            return FileProcessor(config=config)

    @pytest.mark.unit
    def test_process_alignment_palmr(self, processor, tmp_path):
        f = processor.config.bronze_dir / "P.D259.PALMR.D250101.T123456.txt"
        f.write_text("palmr data")

        with patch("acoharmony._4icli.processor.shutil.move") as mock_move, \
             patch("acoharmony._4icli.processor.shutil.copy2") as mock_copy:
            result = processor.process_file(f)
            assert "moved" in result
            mock_move.assert_called_once()
            mock_copy.assert_called_once()

    @pytest.mark.unit
    def test_process_alignment_pbvar(self, processor, tmp_path):
        f = processor.config.bronze_dir / "P.D259.PBVAR.D250101.T123456.txt"
        f.write_text("pbvar data")

        with patch("acoharmony._4icli.processor.shutil.move"), \
             patch("acoharmony._4icli.processor.shutil.copy2"):
            result = processor.process_file(f)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_alignment_bar(self, processor, tmp_path):
        """Beneficiary alignment (ALG pattern)."""
        f = processor.config.bronze_dir / "P.D259.ALGC.D250101.T123456.txt"
        f.write_text("alg data")

        with patch("acoharmony._4icli.processor.shutil.move"), \
             patch("acoharmony._4icli.processor.shutil.copy2"):
            result = processor.process_file(f)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_cclf_non_zip(self, processor, tmp_path):
        f = processor.config.bronze_dir / "CCLF8.D250101.T123456.txt"
        f.write_text("cclf data")

        with patch("acoharmony._4icli.processor.shutil.move") as mock_move:
            processor.process_file(f)
            mock_move.assert_called_once()

    @pytest.mark.unit
    def test_process_cclf_zip(self, processor, tmp_path):
        # Create a real ZIP file
        zip_path = processor.config.bronze_dir / "CCLF8.D250101.T123456.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("inner_file.txt", "inner content")

        with patch("acoharmony._4icli.processor.shutil.copy2"):
            result = processor.process_file(zip_path)
            assert len(result["extracted"]) > 0

    @pytest.mark.unit
    def test_process_risk_adjustment(self, processor, tmp_path):
        f = processor.config.bronze_dir / "some.RAP.report.xlsx"
        f.write_text("rap data")

        with patch("acoharmony._4icli.processor.shutil.move"), \
             patch("acoharmony._4icli.processor.shutil.copy2"):
            result = processor.process_file(f)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_quality_report(self, processor, tmp_path):
        f = processor.config.bronze_dir / "some.BLQQR.report.xlsx"
        f.write_text("blqqr data")

        with patch("acoharmony._4icli.processor.shutil.move"):
            result = processor.process_file(f)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_other_file(self, processor, tmp_path):
        f = processor.config.bronze_dir / "random_file.txt"
        f.write_text("other data")

        with patch("acoharmony._4icli.processor.shutil.move"):
            result = processor.process_file(f)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_tparc(self, processor, tmp_path):
        """TPARC detected as WEEKLY_CLAIMS_REDUCTION, not alignment."""
        f = processor.config.bronze_dir / "P.D259.TPARC.D250101.T123456.txt"
        f.write_text("tparc data")
        # TPARC -> WEEKLY_CLAIMS_REDUCTION -> process_other_file (none of the alignment branches)
        # Actually from models.py, TPARC -> FileType.WEEKLY_CLAIMS_REDUCTION
        # In processor.py there's no branch for WEEKLY_CLAIMS_REDUCTION, so falls to _process_other_file
        with patch("acoharmony._4icli.processor.shutil.move"):
            result = processor.process_file(f)
            assert "moved" in result

    @pytest.mark.unit
    def test_cleanup_download_dir_remove(self, processor, tmp_path):
        f = processor.config.download_dir / "leftover.txt"
        f.write_text("leftover")

        count = processor.cleanup_download_dir(keep_files=False)
        assert count == 1
        assert not f.exists()

    @pytest.mark.unit
    def test_cleanup_download_dir_keep(self, processor, tmp_path):
        f = processor.config.download_dir / "keep_me.txt"
        f.write_text("keep")

        count = processor.cleanup_download_dir(keep_files=True)
        assert count == 1
        assert f.exists()

    @pytest.mark.unit
    def test_process_all(self, processor, tmp_path):
        # Create files in download_dir
        f1 = processor.config.download_dir / "random_file.txt"
        f1.write_text("data")

        with patch("acoharmony._4icli.processor.shutil.move"):
            result = processor.process_all()
            assert result.file_count >= 1
            assert result.success

    @pytest.mark.unit
    def test_process_all_with_error(self, processor, tmp_path):
        f1 = processor.config.download_dir / "bad_file.txt"
        f1.write_text("data")

        with patch.object(processor, "process_file", side_effect=RuntimeError("boom")):
            result = processor.process_all()
            assert len(result.errors) == 1

    @pytest.mark.unit
    def test_processing_result_properties(self):
        from acoharmony._4icli.processor import ProcessingResult
        r = ProcessingResult(
            files_processed=[Path("a"), Path("b")],
            files_moved={},
            files_extracted=[],
            errors=[],
            started_at=datetime.now(),
            completed_at=datetime.now(),
        )
        assert r.success is True
        assert r.file_count == 2

    @pytest.mark.unit
    def test_processing_result_with_errors(self):
        from acoharmony._4icli.processor import ProcessingResult
        r = ProcessingResult(
            files_processed=[],
            files_moved={},
            files_extracted=[],
            errors=["err"],
            started_at=datetime.now(),
        )
        assert r.success is False

class TestProcessorBadZip:
    """Cover processor.py lines 219-223: BadZipFile exception."""

    @pytest.mark.unit
    def test_process_cclf_bad_zip(self, tmp_path):
        config = _make_config(tmp_path)
        config.download_dir = config.bronze_dir
        config.raw_data_dir = tmp_path / "raw"
        config.raw_data_dir.mkdir(parents=True, exist_ok=True)

        with patch("acoharmony._4icli.processor.FourICLIConfig.from_profile", return_value=config):
            from acoharmony._4icli.processor import FileProcessor
            processor = FileProcessor(config=config)

        # Create an invalid ZIP file
        bad_zip = processor.config.bronze_dir / "CCLF8.D250101.T123456.zip"
        bad_zip.write_text("not a zip file")

        with pytest.raises(zipfile.BadZipFile):
            processor.process_file(bad_zip)


class TestProcessingResult:
    @pytest.mark.unit
    def test_success_property(self):
        from acoharmony._4icli.processor import ProcessingResult
        r = ProcessingResult(
            files_processed=[], files_moved={}, files_extracted=[], errors=[],
            started_at=datetime.now()
        )
        assert r.success is True
        assert r.file_count == 0

    @pytest.mark.unit
    def test_failure_property(self):
        from acoharmony._4icli.processor import ProcessingResult
        r = ProcessingResult(
            files_processed=[], files_moved={}, files_extracted=[],
            errors=["error1"],
            started_at=datetime.now()
        )
        assert r.success is False


class TestFileProcessor:  # noqa: F811
    def _ensure_dirs(self, make_config):
        for d in [make_config.bronze_dir, make_config.archive_dir, make_config.silver_dir,
                  make_config.gold_dir, make_config.tracking_dir, make_config.log_dir]:
            d.mkdir(parents=True, exist_ok=True)

    @pytest.mark.unit
    def test_init_with_config(self, make_config):
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            assert processor.config is make_config

    @pytest.mark.unit
    def test_process_file_palmr(self, make_config):
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        palmr_file = make_config.working_dir / "P.D0259.PALMR.D250101.T1234567.csv"
        palmr_file.write_text("palmr data")

        # get_alignment_dir and get_report_dir both return bronze_dir by default,
        # which causes SameFileError when copy2 copies a file onto itself.
        # Use a separate report directory to avoid this.
        report_dir = make_config.bronze_dir.parent / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"), \
             patch.object(make_config, "get_report_dir", return_value=report_dir):
            processor = FileProcessor(config=make_config)
            result = processor.process_file(palmr_file)
            assert "moved" in result
            # File should have been moved
            assert len(result["moved"]) >= 1

    @pytest.mark.unit
    def test_process_file_pbvar(self, make_config):
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        pbvar_file = make_config.working_dir / "P.D0259.PBVAR.D250101.T1234567.csv"
        pbvar_file.write_text("pbvar data")

        # get_alignment_dir and get_report_dir both return bronze_dir by default,
        # causing SameFileError. Use separate report_dir.
        report_dir = make_config.working_dir / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"), \
             patch.object(make_config, "get_report_dir", return_value=report_dir):
            processor = FileProcessor(config=make_config)
            result = processor.process_file(pbvar_file)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_file_cclf_non_zip(self, make_config):
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        cclf_file = make_config.bronze_dir / "CCLF8.D250101.T1234567.txt"
        cclf_file.write_text("cclf data")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            result = processor.process_file(cclf_file)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_file_cclf_zip(self, make_config):
        import zipfile

        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        zip_path = make_config.bronze_dir / "CCLF8.D250101.T1234567.ZIP"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("inner_file.txt", "inner content")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            result = processor.process_file(zip_path)
            assert "extracted" in result
            assert len(result["extracted"]) == 1

    @pytest.mark.unit
    def test_process_file_unknown_type(self, make_config):
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        unknown = make_config.bronze_dir / "unknown_file.dat"
        unknown.write_text("data")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            # processor._process_other_file uses raw_data_dir - add it
            processor.config.raw_data_dir = make_config.bronze_dir
            result = processor.process_file(unknown)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_file_tparc(self, make_config):
        """TPARC triggers WEEKLY_CLAIMS_REDUCTION which isn't handled by processor - goes to else."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        tparc = make_config.bronze_dir / "P.D0259.TPARC.RP.D251025.T2136026.txt"
        tparc.write_text("tparc data")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            processor.config.raw_data_dir = make_config.bronze_dir
            result = processor.process_file(tparc)
            assert "moved" in result or "extracted" in result

    @pytest.mark.unit
    def test_process_file_alg(self, make_config):
        """ALG file triggers BENEFICIARY_ALIGNMENT."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        alg = make_config.working_dir / "P.D0259.ALGC24.RP.D240119.xlsx"
        alg.write_text("alg data")

        report_dir = make_config.working_dir / "reports"
        report_dir.mkdir(parents=True, exist_ok=True)
        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"), \
             patch.object(make_config, "get_report_dir", return_value=report_dir):
            processor = FileProcessor(config=make_config)
            result = processor.process_file(alg)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_file_rap(self, make_config):
        """RAP file triggers RISK_ADJUSTMENT."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        rap = make_config.bronze_dir / "P.D0259.RAPV01.D250101.T1234567.txt"
        rap.write_text("rap data")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            processor.config.raw_data_dir = make_config.bronze_dir
            result = processor.process_file(rap)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_file_blqqr(self, make_config):
        """BLQQR file triggers QUALITY_REPORT."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        blqqr = make_config.bronze_dir / "P.D0259.BLQQR.D250101.xlsx"
        blqqr.write_text("quality data")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            processor.config.raw_data_dir = make_config.bronze_dir
            result = processor.process_file(blqqr)
            assert "moved" in result

    @pytest.mark.unit
    def test_process_all(self, make_config):
        """process_all processes all files in download directory."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            # Set download_dir to bronze_dir for testing
            processor.config.download_dir = make_config.bronze_dir

            # Create some files
            (make_config.bronze_dir / "CCLF8.D250101.txt").write_text("data")

            result = processor.process_all()
            assert result.started_at is not None
            assert result.completed_at is not None

    @pytest.mark.unit
    def test_cleanup_download_dir_remove(self, make_config):
        """cleanup_download_dir removes files."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            processor.config.download_dir = make_config.bronze_dir

            f = make_config.bronze_dir / "temp.txt"
            f.write_text("temp")

            count = processor.cleanup_download_dir(keep_files=False)
            assert count >= 1
            assert not f.exists()

    @pytest.mark.unit
    def test_cleanup_download_dir_keep(self, make_config):
        """cleanup_download_dir keeps files when keep_files=True."""
        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            processor.config.download_dir = make_config.bronze_dir

            f = make_config.bronze_dir / "keep.txt"
            f.write_text("keep me")

            count = processor.cleanup_download_dir(keep_files=True)
            assert count >= 1
            assert f.exists()

    @pytest.mark.unit
    def test_process_cclf_zip_bad_zipfile(self, make_config):
        """process_file with invalid ZIP raises BadZipFile."""
        import zipfile

        from acoharmony._4icli.processor import FileProcessor

        self._ensure_dirs(make_config)
        bad_zip = make_config.bronze_dir / "CCLF8.D250101.T1234567.ZIP"
        bad_zip.write_text("not a zip file")

        with patch("acoharmony._4icli.processor.FileProcessor._setup_logging"):
            processor = FileProcessor(config=make_config)
            with pytest.raises(zipfile.BadZipFile):
                processor.process_file(bad_zip)


class TestProcessorProcessAllWithZip:
    """Cover processor.py lines 95-98: extracted files in process_all."""

    @pytest.mark.unit
    def test_process_all_with_extracted(self, tmp_path):
        config = _make_config(tmp_path)
        config.download_dir = config.bronze_dir
        config.raw_data_dir = tmp_path / "raw"
        config.raw_data_dir.mkdir(parents=True, exist_ok=True)

        with patch("acoharmony._4icli.processor.FourICLIConfig.from_profile", return_value=config):
            from acoharmony._4icli.processor import FileProcessor

            processor = FileProcessor(config=config)

        # Create a real ZIP file
        zip_path = processor.config.download_dir / "CCLF8.D250101.T123456.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("inner_file.txt", "inner content")

        with patch("acoharmony._4icli.processor.shutil.copy2"):
            result = processor.process_all()
            assert len(result.files_extracted) > 0
            assert result.success


class TestProcessorSkipsDirectories:
    """Cover processor.py 91->90 and 306->305: directories in download_dir are skipped."""

    @pytest.mark.unit
    def test_process_all_skips_directories(self, tmp_path):
        """Cover processor.py 91->90: directory in download_dir is skipped by is_file() check."""
        config = _make_config(tmp_path)
        config.download_dir = config.bronze_dir
        config.raw_data_dir = tmp_path / "raw"
        config.raw_data_dir.mkdir(parents=True, exist_ok=True)

        with patch("acoharmony._4icli.processor.FourICLIConfig.from_profile", return_value=config):
            from acoharmony._4icli.processor import FileProcessor
            processor = FileProcessor(config=config)

        # Create a subdirectory (not a file) inside download_dir
        subdir = processor.config.download_dir / "some_subdir"
        subdir.mkdir(parents=True, exist_ok=True)

        # Also create a real file so we can verify processing continues
        real_file = processor.config.download_dir / "real_file.txt"
        real_file.write_text("data")

        with patch("acoharmony._4icli.processor.shutil.move"):
            result = processor.process_all()
            # Only the file should be processed, not the directory
            assert result.file_count == 1
            assert result.success

    @pytest.mark.unit
    def test_cleanup_download_dir_skips_directories(self, tmp_path):
        """Cover processor.py 306->305: directory in download_dir is skipped by is_file() check."""
        config = _make_config(tmp_path)
        config.download_dir = config.bronze_dir
        config.raw_data_dir = tmp_path / "raw"
        config.raw_data_dir.mkdir(parents=True, exist_ok=True)

        with patch("acoharmony._4icli.processor.FourICLIConfig.from_profile", return_value=config):
            from acoharmony._4icli.processor import FileProcessor
            processor = FileProcessor(config=config)

        # Create a subdirectory and a file
        subdir = processor.config.download_dir / "some_subdir"
        subdir.mkdir(parents=True, exist_ok=True)
        real_file = processor.config.download_dir / "file_to_clean.txt"
        real_file.write_text("data")

        count = processor.cleanup_download_dir(keep_files=False)
        # Only the file counts, not the directory
        assert count == 1
        assert not real_file.exists()
        # Directory should still exist (not counted, not removed)
        assert subdir.exists()
