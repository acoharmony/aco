"""Tests for acoharmony._dev.generators.metadata."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# extract_aco_metadata tests
# ---------------------------------------------------------------------------


class TestExtractAcoMetadata:

    @pytest.mark.unit
    def test_valid_filename(self):
        from acoharmony._dev.generators.metadata import extract_aco_metadata

        result = extract_aco_metadata("P.D0259.ACO.ZC1Y23.D240115.T1234567")
        assert result is not None
        assert result["aco_id"] == "D0259"
        assert result["cclf_type"] == "1"
        assert result["is_weekly"] is False
        assert result["program"] == "Y"
        assert result["year"] == "23"
        assert result["date"] == "240115"
        assert result["time"] == "1234567"
        assert result["program_full"] == "Y23"

    @pytest.mark.unit
    def test_weekly_filename(self):
        from acoharmony._dev.generators.metadata import extract_aco_metadata

        result = extract_aco_metadata("P.A2671.ACO.ZC5WR24.D240301.T0000001")
        assert result is not None
        assert result["is_weekly"] is True
        assert result["program"] == "R"

    @pytest.mark.unit
    def test_invalid_filename(self):
        from acoharmony._dev.generators.metadata import extract_aco_metadata

        assert extract_aco_metadata("invalid_file.txt") is None
        assert extract_aco_metadata("") is None
        assert extract_aco_metadata("P.ACO.ZC1Y23") is None

    @pytest.mark.unit
    def test_runout_program(self):
        from acoharmony._dev.generators.metadata import extract_aco_metadata

        result = extract_aco_metadata("P.D0259.ACO.ZC8R24.D240201.T9999999")
        assert result["program"] == "R"
        assert result["program_full"] == "R24"

    @pytest.mark.unit
    def test_alphanumeric_cclf_type(self):
        from acoharmony._dev.generators.metadata import extract_aco_metadata

        result = extract_aco_metadata("P.D0259.ACO.ZCAY23.D240115.T1234567")
        assert result is not None
        assert result["cclf_type"] == "A"


# ---------------------------------------------------------------------------
# load_schema_file_patterns tests (now reads SchemaRegistry, not yml files)
# ---------------------------------------------------------------------------


class TestLoadSchemaFilePatterns:

    @pytest.mark.unit
    def test_returns_dict(self):
        """load_schema_file_patterns reads from SchemaRegistry and returns a dict."""
        from acoharmony._dev.generators.metadata import load_schema_file_patterns

        result = load_schema_file_patterns()
        assert isinstance(result, dict)

    @pytest.mark.unit
    def test_includes_schemas_with_file_patterns(self):
        """Schemas registered with @with_storage(file_patterns=...) appear in the result."""
        from acoharmony._dev.generators.metadata import load_schema_file_patterns

        result = load_schema_file_patterns()
        # cclf1 has file_patterns defined via @with_storage
        assert "cclf1" in result


# ---------------------------------------------------------------------------
# generate_aco_metadata tests
# ---------------------------------------------------------------------------


class TestGenerateAcoMetadata:

    @pytest.mark.unit
    def test_generate_returns_bool(self, tmp_path):
        from acoharmony._dev.generators.metadata import generate_aco_metadata

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = tmp_path / "raw"
        (tmp_path / "raw").mkdir()

        with patch("acoharmony._dev.generators.metadata.StorageBackend", return_value=mock_storage):
            with patch("acoharmony._dev.generators.metadata.load_schema_file_patterns", return_value={}):
                old_cwd = os.getcwd()
                try:
                    os.chdir(tmp_path)
                    result = generate_aco_metadata()
                    assert result is True
                finally:
                    os.chdir(old_cwd)

    @pytest.mark.unit
    def test_generate_with_files(self, tmp_path):
        from acoharmony._dev.generators.metadata import generate_aco_metadata

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()
        (raw_dir / "P.D0259.ACO.ZC1Y23.D240115.T1234567").touch()

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = raw_dir

        patterns = {"cclf1": {"glob": "P.*.ACO.ZC1*"}}

        with patch("acoharmony._dev.generators.metadata.StorageBackend", return_value=mock_storage):
            with patch("acoharmony._dev.generators.metadata.load_schema_file_patterns", return_value=patterns):
                old_cwd = os.getcwd()
                try:
                    os.chdir(tmp_path)
                    result = generate_aco_metadata()
                    assert result is True
                    assert (tmp_path / "docs" / "ACO_METADATA.md").exists()
                    content = (tmp_path / "docs" / "ACO_METADATA.md").read_text()
                    assert "D0259" in content
                finally:
                    os.chdir(old_cwd)

    @pytest.mark.unit
    def test_generate_storage_error(self):
        from acoharmony._dev.generators.metadata import generate_aco_metadata
        from acoharmony._exceptions import StorageBackendError

        with patch("acoharmony._dev.generators.metadata.StorageBackend", side_effect=Exception("no storage")):
            with pytest.raises(StorageBackendError):
                generate_aco_metadata()

    @pytest.mark.unit
    def test_generate_write_error(self, tmp_path):
        from acoharmony._dev.generators.metadata import generate_aco_metadata

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = tmp_path / "raw"
        (tmp_path / "raw").mkdir()

        with patch("acoharmony._dev.generators.metadata.StorageBackend", return_value=mock_storage):
            with patch("acoharmony._dev.generators.metadata.load_schema_file_patterns", return_value={}):
                with patch("builtins.open", side_effect=PermissionError("denied")):
                    old_cwd = os.getcwd()
                    try:
                        os.chdir(tmp_path)
                        result = generate_aco_metadata()
                        assert result is False
                    finally:
                        os.chdir(old_cwd)

    @pytest.mark.unit
    def test_generate_with_empty_patterns_skip(self, tmp_path):
        """Cover line 100: skip schemas with empty patterns."""
        from acoharmony._dev.generators.metadata import generate_aco_metadata

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = raw_dir

        # Schema with empty patterns should be skipped via line 100
        patterns = {"empty_schema": {}}

        with patch("acoharmony._dev.generators.metadata.StorageBackend", return_value=mock_storage):
            with patch(
                "acoharmony._dev.generators.metadata.load_schema_file_patterns",
                return_value=patterns,
            ):
                old_cwd = os.getcwd()
                try:
                    os.chdir(tmp_path)
                    result = generate_aco_metadata()
                    assert result is True
                finally:
                    os.chdir(old_cwd)

    @pytest.mark.unit
    def test_generate_with_dict_pattern_value_skip(self, tmp_path):
        """Cover line 104: skip dict pattern values (e.g. report_year_extraction)."""
        from acoharmony._dev.generators.metadata import generate_aco_metadata

        raw_dir = tmp_path / "raw"
        raw_dir.mkdir()

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = raw_dir

        patterns = {
            "schema_with_dict": {
                "glob": "*.csv",
                "report_year_extraction": {"pattern": "x", "group": 1},
            }
        }

        with patch("acoharmony._dev.generators.metadata.StorageBackend", return_value=mock_storage):
            with patch(
                "acoharmony._dev.generators.metadata.load_schema_file_patterns",
                return_value=patterns,
            ):
                old_cwd = os.getcwd()
                try:
                    os.chdir(tmp_path)
                    result = generate_aco_metadata()
                    assert result is True
                finally:
                    os.chdir(old_cwd)
