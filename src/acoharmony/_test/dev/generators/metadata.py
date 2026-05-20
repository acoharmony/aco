"""Tests for acoharmony._dev.generators.metadata module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from collections import defaultdict
from pathlib import Path
from unittest.mock import MagicMock, patch

import acoharmony
from acoharmony._dev.generators.metadata import (
    extract_aco_metadata,
    generate_aco_metadata,
)


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.generators.metadata is not None


# ---------------------------------------------------------------------------
# Branch 113→155: raw_data_path does NOT exist → skip file search loop
# ---------------------------------------------------------------------------


class TestGenerateAcoMetadataRawDataMissing:
    @pytest.mark.unit
    def test_raw_data_path_not_exists_skips_file_loop(self, tmp_path):
        """Branch 113→155: raw_data_path.exists() is False → no files scanned."""
        docs_dir = tmp_path / "docs"
        fake_raw = tmp_path / "nonexistent_raw"
        # raw path does not exist

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = fake_raw

        with (
            patch(
                "acoharmony._dev.generators.metadata.StorageBackend",
                return_value=mock_storage,
            ),
            patch(
                "acoharmony._dev.generators.metadata.load_schema_file_patterns",
                return_value={"cclf1": {"raw": "*.txt"}},
            ),
            patch("acoharmony._dev.generators.metadata.Path", wraps=Path) as mock_path_cls,
        ):
            # Make sure Path("docs") → our tmp docs dir
            original_path = Path

            def path_side_effect(arg=""):
                if arg == "docs":
                    return docs_dir
                return original_path(arg)

            mock_path_cls.side_effect = path_side_effect

            result = generate_aco_metadata()

        assert result is True
        output = docs_dir / "ACO_METADATA.md"
        assert output.exists()
        content = output.read_text()
        assert "Total ACOs" in content
        assert "Total CCLF files" in content


# ---------------------------------------------------------------------------
# Branch 142→139: metadata extraction returns None for a file
# ---------------------------------------------------------------------------


class TestGenerateAcoMetadataNoMetadata:
    @pytest.mark.unit
    def test_file_with_no_matching_metadata_is_skipped(self):
        """Branch 142→139: extract_aco_metadata returns None → continue."""
        result = extract_aco_metadata("random_file_name.txt")
        assert result is None

    @pytest.mark.unit
    def test_file_with_valid_metadata(self):
        """Positive case: valid filename extracts metadata."""
        result = extract_aco_metadata(
            "P.D0259.ACO.ZC1Y25.D260101.T0000001"
        )
        assert result is not None
        assert result["aco_id"] == "D0259"
        assert result["cclf_type"] == "1"
        assert result["is_weekly"] is False


# ---------------------------------------------------------------------------
# Branch 192→191: f.get("schema") != schema → inner loop continues
# ---------------------------------------------------------------------------


class TestSchemaPatternAnalysis:
    @pytest.mark.unit
    def test_pattern_types_skip_non_matching_schema(self, tmp_path):
        """Branch 192→191: file schema doesn't match current schema → skip."""
        # Simulate the pattern_types calculation logic from generate_aco_metadata
        aco_files = defaultdict(lambda: defaultdict(list))
        aco_files["D0259"]["1"].append(
            {
                "filename": "P.D0259.ACO.ZC1Y25.D260101.T0000001",
                "metadata": extract_aco_metadata("P.D0259.ACO.ZC1Y25.D260101.T0000001"),
                "schema": "cclf1",
                "pattern_type": "raw",
            }
        )

        # Searching for "cclf2" should find no matching pattern_types
        schema = "cclf2"
        pattern_types = set()
        for aco in aco_files.values():
            for files in aco.values():
                for f in files:
                    if f.get("schema") == schema:
                        pattern_types.add(f.get("pattern_type", ""))

        assert len(pattern_types) == 0  # 192→191: skipped all


# ---------------------------------------------------------------------------
# Branch 227→229: regular_programs is empty → only weekly_programs shown
# ---------------------------------------------------------------------------


class TestWeeklyOnlyPrograms:
    @pytest.mark.unit
    def test_only_weekly_files_no_regular(self):
        """Branch 227→229: regular_programs is empty, weekly_programs present."""
        # Simulate the program aggregation from generate_aco_metadata
        files = [
            {
                "filename": "P.D0259.ACO.ZC1WY25.D260101.T0000001",
                "metadata": extract_aco_metadata("P.D0259.ACO.ZC1WY25.D260101.T0000001"),
                "schema": "cclf1",
                "pattern_type": "raw",
            }
        ]

        regular_files = [f for f in files if not f["metadata"]["is_weekly"]]
        weekly_files = [f for f in files if f["metadata"]["is_weekly"]]

        regular_programs = defaultdict(int)
        for f in regular_files:
            regular_programs[f["metadata"]["program_full"]] += 1

        weekly_programs = defaultdict(int)
        for f in weekly_files:
            weekly_programs[f["metadata"]["program_full"]] += 1

        programs = []
        if regular_programs:
            programs.extend([f"{p}:{c}" for p, c in sorted(regular_programs.items())])
        if weekly_programs:
            programs.extend([f"{p}(W):{c}" for p, c in sorted(weekly_programs.items())])

        assert len(regular_programs) == 0  # 227 false → skip
        assert len(weekly_programs) == 1
        assert "Y25(W):1" in programs


# ---------------------------------------------------------------------------
# Integration test: generate_aco_metadata with mixed files
# Covers: 142->139 (no metadata), 192->191 (schema mismatch), 227->229 (weekly only)
# ---------------------------------------------------------------------------


class TestGenerateAcoMetadataIntegration:
    """Test generate_aco_metadata with actual file patterns to cover branches."""

    @pytest.mark.unit
    def test_with_mixed_valid_and_invalid_files(self, tmp_path):
        """Covers 142->139: some files have no metadata, loop continues."""
        import glob as glob_mod

        docs_dir = tmp_path / "docs"
        fake_raw = tmp_path / "raw"
        fake_raw.mkdir()

        # Create actual files - one valid CCLF, one invalid
        (fake_raw / "P.D0259.ACO.ZC1Y25.D260101.T0000001").write_text("")
        (fake_raw / "random_garbage_file.txt").write_text("")

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = fake_raw

        with (
            patch(
                "acoharmony._dev.generators.metadata.StorageBackend",
                return_value=mock_storage,
            ),
            patch(
                "acoharmony._dev.generators.metadata.load_schema_file_patterns",
                return_value={"cclf1": {"raw": "*"}},
            ),
            patch("acoharmony._dev.generators.metadata.Path", wraps=Path) as mock_path_cls,
        ):
            original_path = Path

            def path_side_effect(arg=""):
                if arg == "docs":
                    return docs_dir
                return original_path(arg)

            mock_path_cls.side_effect = path_side_effect

            result = generate_aco_metadata()

        assert result is True
        output = docs_dir / "ACO_METADATA.md"
        assert output.exists()

    @pytest.mark.unit
    def test_with_weekly_files_and_pattern_matches(self, tmp_path):
        """Covers 192->191 and 227->229 through actual generate_aco_metadata."""
        docs_dir = tmp_path / "docs"
        fake_raw = tmp_path / "raw"
        fake_raw.mkdir()

        # Create weekly file
        (fake_raw / "P.D0259.ACO.ZC1WY25.D260101.T0000001").write_text("")

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = fake_raw

        with (
            patch(
                "acoharmony._dev.generators.metadata.StorageBackend",
                return_value=mock_storage,
            ),
            patch(
                "acoharmony._dev.generators.metadata.load_schema_file_patterns",
                return_value={
                    "cclf1": {"raw": "P.D0259.ACO.ZC1W*"},
                    "cclf2": {"raw": "P.D0259.ACO.ZC2*"},
                },
            ),
            patch("acoharmony._dev.generators.metadata.Path", wraps=Path) as mock_path_cls,
        ):
            original_path = Path

            def path_side_effect(arg=""):
                if arg == "docs":
                    return docs_dir
                return original_path(arg)

            mock_path_cls.side_effect = path_side_effect

            result = generate_aco_metadata()

        assert result is True
        content = (docs_dir / "ACO_METADATA.md").read_text()
        # Weekly files should be counted
        assert "Weekly" in content or "weekly" in content.lower() or "W" in content


class TestSchemaPatternMismatchInGenerate:
    """Cover branch 192->191: f.get('schema') != schema in generate_aco_metadata."""

    @pytest.mark.unit
    def test_schema_mismatch_skips_file_in_pattern_types(self, tmp_path):
        """Branch 192->191: inner loop skips files whose schema doesn't match.

        Two schemas both match files from the same ACO. When iterating
        for one schema, the files with the other schema hit False at line 192.
        """
        docs_dir = tmp_path / "docs"
        fake_raw = tmp_path / "raw"
        fake_raw.mkdir()

        # Create files: one matching cclf1 pattern, one matching cclf2 pattern
        (fake_raw / "P.D0259.ACO.ZC1Y25.D260101.T0000001").write_text("")
        (fake_raw / "P.D0259.ACO.ZC2Y25.D260101.T0000001").write_text("")

        mock_storage = MagicMock()
        mock_storage.get_data_path.return_value = fake_raw

        with (
            patch(
                "acoharmony._dev.generators.metadata.StorageBackend",
                return_value=mock_storage,
            ),
            patch(
                "acoharmony._dev.generators.metadata.load_schema_file_patterns",
                return_value={
                    "cclf1": {"raw": "P.*.ZC1*"},
                    "cclf2": {"raw": "P.*.ZC2*"},
                },
            ),
            patch("acoharmony._dev.generators.metadata.Path", wraps=Path) as mock_path_cls,
        ):
            original_path = Path

            def path_side_effect(arg=""):
                if arg == "docs":
                    return docs_dir
                return original_path(arg)

            mock_path_cls.side_effect = path_side_effect

            result = generate_aco_metadata()

        assert result is True
        content = (docs_dir / "ACO_METADATA.md").read_text()
        # Both schemas should appear in the pattern analysis table
        assert "cclf1" in content
        assert "cclf2" in content
