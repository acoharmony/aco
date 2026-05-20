# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for 4icli models - Polars style."""

from __future__ import annotations
from dataclasses import dataclass

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import datetime, timedelta
from pathlib import Path

import pytest

from acoharmony._4icli.models import (
    DataHubCategory,
    DataHubQuery,
    DateFilter,
    DownloadResult,
    FileInfo,
    FileType,
    FileTypeCode,
)


class TestDataHubCategory:
    """Tests for DataHubCategory enum."""

    @pytest.mark.unit
    def test_beneficiary_list_category(self) -> None:
        """Beneficiary List category has correct value."""
        assert DataHubCategory.BENEFICIARY_LIST == "Beneficiary List"

    @pytest.mark.unit
    def test_cclf_category(self) -> None:
        """CCLF category has correct value."""
        assert DataHubCategory.CCLF == "CCLF"

    @pytest.mark.unit
    def test_reports_category(self) -> None:
        """Reports category has correct value."""
        assert DataHubCategory.REPORTS == "Reports"



class TestFileTypeCode:
    """Tests for FileTypeCode enum."""

    @pytest.mark.unit
    def test_cclf_code(self) -> None:
        """CCLF has code 113."""
        assert FileTypeCode.CCLF == 113

    @pytest.mark.unit
    def test_provider_alignment_code(self) -> None:
        """Provider Alignment Report has code 165."""
        assert FileTypeCode.PROVIDER_ALIGNMENT_REPORT == 165

    @pytest.mark.unit
    def test_beneficiary_alignment_code(self) -> None:
        """Beneficiary Alignment Report has code 159."""
        assert FileTypeCode.BENEFICIARY_ALIGNMENT_REPORT == 159

    @pytest.mark.unit
    def test_risk_score_report_code(self) -> None:
        """Risk Score Report has code 211."""
        assert FileTypeCode.RISK_SCORE_REPORT == 211

    @pytest.mark.unit
    def test_all_codes_are_unique(self) -> None:
        """All file type codes are unique."""
        codes = [code.value for code in FileTypeCode]
        assert len(codes) == len(set(codes))


class TestFileType:
    """Tests for FileType enum."""

    @pytest.mark.unit
    def test_provider_alignment_type(self) -> None:
        """Provider alignment has correct value."""
        assert FileType.PROVIDER_ALIGNMENT == "PALMR"

    @pytest.mark.unit
    def test_voluntary_alignment_type(self) -> None:
        """Voluntary alignment has correct value."""
        assert FileType.VOLUNTARY_ALIGNMENT == "PBVAR"

    @pytest.mark.unit
    def test_beneficiary_alignment_type(self) -> None:
        """Beneficiary alignment has correct value."""
        assert FileType.BENEFICIARY_ALIGNMENT == "ALG"

    @pytest.mark.unit
    def test_cclf_type(self) -> None:
        """CCLF has correct value."""
        assert FileType.CCLF == "CCLF"


class TestFileInfo:
    """Tests for FileInfo dataclass."""

    @pytest.mark.unit
    def test_from_filename_cclf(self) -> None:
        """from_filename detects CCLF files."""
        file_info = FileInfo.from_filename("CCLF8.D240101.T1234567.zip")

        assert file_info.name == "CCLF8.D240101.T1234567.zip"
        assert file_info.file_type == FileType.CCLF

    @pytest.mark.unit
    def test_from_filename_palmr(self) -> None:
        """from_filename detects PALMR files."""
        file_info = FileInfo.from_filename("P.D259999.PALMR.D240101.T1234567")

        assert file_info.file_type == FileType.PROVIDER_ALIGNMENT

    @pytest.mark.unit
    def test_from_filename_pbvar(self) -> None:
        """from_filename detects PBVAR files."""
        file_info = FileInfo.from_filename("P.D259999.PBVAR.D240101.T1234567")

        assert file_info.file_type == FileType.VOLUNTARY_ALIGNMENT

    @pytest.mark.unit
    def test_from_filename_tparc(self) -> None:
        """from_filename detects TPARC files."""
        file_info = FileInfo.from_filename("P.D259999.TPARC.D240101.T1234567")

        assert file_info.file_type == FileType.WEEKLY_CLAIMS_REDUCTION

    @pytest.mark.unit
    def test_from_filename_rap(self) -> None:
        """from_filename detects RAP files."""
        file_info = FileInfo.from_filename("P.D259999.RAPV01.D240101.T1234567")

        assert file_info.file_type == FileType.RISK_ADJUSTMENT

    @pytest.mark.unit
    def test_from_filename_blqqr(self) -> None:
        """from_filename detects BLQQR files."""
        file_info = FileInfo.from_filename("P.D259999.BLQQR.D240101.T1234567")

        assert file_info.file_type == FileType.QUALITY_REPORT

    @pytest.mark.unit
    def test_from_filename_unknown(self) -> None:
        """from_filename handles unknown file types."""
        file_info = FileInfo.from_filename("unknown_file.txt")

        assert file_info.name == "unknown_file.txt"
        assert file_info.file_type is None


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    @pytest.mark.unit
    def test_download_result_success(self, temp_bronze_dir: Path) -> None:
        """DownloadResult tracks successful downloads."""
        file1 = temp_bronze_dir / "file1.txt"
        file1.write_text("content")

        started = datetime.now()
        completed = datetime.now()

        result = DownloadResult(
            success=True,
            files_downloaded=[file1],
            errors=[],
            download_path=temp_bronze_dir,
            started_at=started,
            completed_at=completed,
        )

        assert result.success
        assert result.file_count == 1
        assert result.duration is not None

    @pytest.mark.unit
    def test_download_result_failure(self, temp_bronze_dir: Path) -> None:
        """DownloadResult tracks failed downloads."""
        started = datetime.now()
        completed = datetime.now()

        result = DownloadResult(
            success=False,
            files_downloaded=[],
            errors=["Connection failed"],
            download_path=temp_bronze_dir,
            started_at=started,
            completed_at=completed,
        )

        assert not result.success
        assert result.file_count == 0
        assert len(result.errors) == 1

    @pytest.mark.unit
    def test_download_result_duration(self, temp_bronze_dir: Path) -> None:
        """DownloadResult calculates duration correctly."""
        started = datetime(2025, 1, 1, 12, 0, 0)
        completed = datetime(2025, 1, 1, 12, 1, 30)  # 90 seconds later

        result = DownloadResult(
            success=True,
            files_downloaded=[],
            errors=[],
            download_path=temp_bronze_dir,
            started_at=started,
            completed_at=completed,
        )

        assert result.duration == 90.0

    @pytest.mark.unit
    def test_download_result_no_completion(self, temp_bronze_dir: Path) -> None:
        """DownloadResult handles missing completion time."""
        result = DownloadResult(
            success=True,
            files_downloaded=[],
            errors=[],
            download_path=temp_bronze_dir,
            started_at=datetime.now(),
        )

        assert result.duration is None


class TestDateFilter:
    """Tests for DateFilter dataclass."""

    @pytest.mark.unit
    def test_date_filter_created_after(self) -> None:
        """DateFilter converts created_after to CLI args."""
        filter = DateFilter(created_after="2024-01-01")

        args = filter.to_cli_args()

        assert "--createdAfter" in args
        assert "2024-01-01" in args

    @pytest.mark.unit
    def test_date_filter_created_between(self) -> None:
        """DateFilter converts created_between to CLI args."""
        filter = DateFilter(created_between=("2024-01-01", "2024-12-31"))

        args = filter.to_cli_args()

        assert "--createdBetween" in args
        assert "2024-01-01,2024-12-31" in args

    @pytest.mark.unit
    def test_date_filter_created_within_last_week(self) -> None:
        """DateFilter converts created_within_last_week to CLI args."""
        filter = DateFilter(created_within_last_week=True)

        args = filter.to_cli_args()

        assert "--createdWithinLastWeek" in args

    @pytest.mark.unit
    def test_date_filter_multiple_options(self) -> None:
        """DateFilter handles multiple filter options."""
        filter = DateFilter(
            created_after="2024-01-01",
            updated_after="2024-06-01",
        )

        args = filter.to_cli_args()

        assert "--createdAfter" in args
        assert "--updatedAfter" in args


class TestDataHubQuery:
    """Tests for DataHubQuery dataclass."""

    @pytest.mark.unit
    def test_query_to_cli_args_basic(self) -> None:
        """DataHubQuery converts to CLI args."""
        query = DataHubQuery(
            category=DataHubCategory.CCLF,
            year=2025,
        )

        args = query.to_cli_args()

        assert "-y" in args
        assert "2025" in args
        assert "-c" in args
        assert "CCLF" in args

    @pytest.mark.unit
    def test_query_to_cli_args_with_apm(self) -> None:
        """DataHubQuery includes APM ID."""
        query = DataHubQuery(
            apm_id="D0259",
            year=2025,
        )

        args = query.to_cli_args()

        assert "-a" in args
        assert "D0259" in args

    @pytest.mark.unit
    def test_query_to_cli_args_with_file_type(self) -> None:
        """DataHubQuery includes file type code."""
        query = DataHubQuery(
            file_type_code=FileTypeCode.CCLF,
            year=2025,
        )

        args = query.to_cli_args()

        assert "-f" in args
        assert "113" in args

    @pytest.mark.unit
    def test_query_to_cli_args_with_date_filter(self) -> None:
        """DataHubQuery includes date filter."""
        date_filter = DateFilter(created_within_last_week=True)
        query = DataHubQuery(
            year=2025,
            date_filter=date_filter,
        )

        args = query.to_cli_args()

        assert "--createdWithinLastWeek" in args

    @pytest.mark.unit
    def test_query_to_cli_args_complete(self) -> None:
        """DataHubQuery with all options."""
        date_filter = DateFilter(created_after="2024-01-01")
        query = DataHubQuery(
            apm_id="D0259",
            year=2025,
            category=DataHubCategory.BENEFICIARY_LIST,
            file_type_code=FileTypeCode.PROVIDER_ALIGNMENT_REPORT,
            date_filter=date_filter,
        )

        args = query.to_cli_args()

        assert "-a" in args
        assert "-y" in args
        assert "-c" in args
        assert "-f" in args
        assert "--createdAfter" in args


class TestModels:
    @pytest.mark.unit
    def test_file_info_from_filename_all_types(self):
        from acoharmony._4icli.models import FileInfo, FileType

        assert FileInfo.from_filename("PALMR_file.txt").file_type == FileType.PROVIDER_ALIGNMENT
        assert FileInfo.from_filename("PBVAR_file.txt").file_type == FileType.VOLUNTARY_ALIGNMENT
        assert FileInfo.from_filename("ALGC_file.txt").file_type == FileType.BENEFICIARY_ALIGNMENT
        assert FileInfo.from_filename("TPARC_file.txt").file_type == FileType.WEEKLY_CLAIMS_REDUCTION
        assert FileInfo.from_filename("CCLF8.zip").file_type == FileType.CCLF
        assert FileInfo.from_filename("RAP_file.xlsx").file_type == FileType.RISK_ADJUSTMENT
        assert FileInfo.from_filename("BLQQR_file.xlsx").file_type == FileType.QUALITY_REPORT
        assert FileInfo.from_filename("unknown.txt").file_type is None

    @pytest.mark.unit
    def test_download_result_properties(self):
        from acoharmony._4icli.models import DownloadResult

        now = datetime.now()
        r = DownloadResult(
            success=True,
            files_downloaded=[Path("a.zip")],
            errors=[],
            download_path=Path("/tmp"),
            started_at=now,
            completed_at=now + timedelta(seconds=5),
        )
        assert r.duration == 5.0
        assert r.file_count == 1

    @pytest.mark.unit
    def test_download_result_no_completed(self):
        from acoharmony._4icli.models import DownloadResult

        r = DownloadResult(
            success=True,
            files_downloaded=[],
            errors=[],
            download_path=Path("/tmp"),
            started_at=datetime.now(),
        )
        assert r.duration is None

    @pytest.mark.unit
    def test_date_filter_to_cli_args(self):
        from acoharmony._4icli.models import DateFilter

        df = DateFilter(
            created_after="2025-01-01",
            created_between=("2025-01-01", "2025-06-01"),
            created_within_last_month=True,
            created_within_last_week=True,
            updated_after="2025-01-01",
            updated_between=("2025-01-01", "2025-06-01"),
        )
        args = df.to_cli_args()
        assert "--createdAfter" in args
        assert "--createdBetween" in args
        assert "--createdWithinLastMonth" in args
        assert "--createdWithinLastWeek" in args
        assert "--updatedAfter" in args
        assert "--updatedBetween" in args

    @pytest.mark.unit
    def test_datahub_query_to_cli_args(self):
        from acoharmony._4icli.models import DataHubCategory, DataHubQuery, DateFilter, FileTypeCode

        q = DataHubQuery(
            apm_id="D0259",
            year=2025,
            category=DataHubCategory.CCLF,
            file_type_code=FileTypeCode.CCLF,
            date_filter=DateFilter(created_after="2025-01-01"),
        )
        args = q.to_cli_args()
        assert "-a" in args
        assert "-y" in args
        assert "-c" in args
        assert "-f" in args
        assert "--createdAfter" in args



class TestDateFilterExtended:
    @pytest.mark.unit
    def test_empty_filter_produces_no_args(self):
        from acoharmony._4icli.models import DateFilter
        f = DateFilter()
        assert f.to_cli_args() == []

    @pytest.mark.unit
    def test_updated_between(self):
        from acoharmony._4icli.models import DateFilter
        f = DateFilter(updated_between=("2025-01-01", "2025-06-30"))
        args = f.to_cli_args()
        assert "--updatedBetween" in args
        assert "2025-01-01,2025-06-30" in args


class TestDataHubQueryExtended:
    @pytest.mark.unit
    def test_query_no_optional_fields(self):
        from acoharmony._4icli.models import DataHubQuery
        q = DataHubQuery(year=2024)
        args = q.to_cli_args()
        assert "-y" in args
        assert "2024" in args
        assert "-a" not in args
        assert "-c" not in args
        assert "-f" not in args


class TestFileInfoExtended:
    @pytest.mark.unit
    def test_from_filename_alg(self):
        from acoharmony._4icli.models import FileInfo, FileType
        info = FileInfo.from_filename("P.D0259.ALGC24.RP.D240119.xlsx")
        assert info.file_type == FileType.BENEFICIARY_ALIGNMENT

    @pytest.mark.unit
    def test_from_filename_blqqr(self):
        from acoharmony._4icli.models import FileInfo, FileType
        info = FileInfo.from_filename("P.D0259.BLQQR.D250101.T0000000.xlsx")
        assert info.file_type == FileType.QUALITY_REPORT


class TestDownloadResultExtended:
    @pytest.mark.unit
    def test_file_count_multiple(self):
        from acoharmony._4icli.models import DownloadResult
        r = DownloadResult(
            success=True,
            files_downloaded=[Path("a"), Path("b"), Path("c")],
            errors=[],
            download_path=Path("/tmp"),
            started_at=datetime.now(),
        )
        assert r.file_count == 3
        assert r.duration is None
