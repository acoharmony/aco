# © 2025 HarmonyCares
# All rights reserved.

"""Unit tests for parser module - Polars style."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


import pytest
import acoharmony


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._aco_id is not None


class TestParserModule:
    """Tests for parser functionality."""

    @pytest.mark.unit
    def test_basic_placeholder(self) -> None:
        """Placeholder test."""
        assert True


class TestAcoId:
    """Tests for acoharmony._parsers._aco_id."""

    @pytest.mark.unit
    def test_extract_aco_id_d_prefix(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("CCLF1_D0259_claims.csv") == "D0259"

    @pytest.mark.unit
    def test_extract_aco_id_a_prefix(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("report_A2671_2024.txt") == "A2671"

    @pytest.mark.unit
    def test_extract_aco_id_lowercase(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("file_d1234_data") == "D1234"

    @pytest.mark.unit
    def test_extract_aco_id_aco_dash_format(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("aco-1234_report") == "ACO-1234"

    @pytest.mark.unit
    def test_extract_aco_id_aco_no_dash(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("ACO5678_data") == "O5678"

    @pytest.mark.unit
    def test_extract_aco_id_legacy_pd(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("P.D0259.claims.csv") == "D0259"

    @pytest.mark.unit
    def test_extract_aco_id_legacy_pd_only(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("file_P.XX9.dat") == "P.XX9"

    @pytest.mark.unit
    def test_extract_aco_id_legacy_v(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("data.V.36.file") == "V.36"

    @pytest.mark.unit
    def test_extract_aco_id_none(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("no_id_here.csv") is None

    @pytest.mark.unit
    def test_extract_aco_id_empty(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id("") is None

    @pytest.mark.unit
    def test_extract_aco_id_none_input(self):
        from acoharmony._parsers._aco_id import extract_aco_id

        assert extract_aco_id(None) is None

    @pytest.mark.unit
    def test_program_reach(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("D0259") == "REACH"

    @pytest.mark.unit
    def test_program_mssp(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("A2671") == "MSSP"

    @pytest.mark.unit
    def test_program_legacy_reach(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("P.D0259") == "REACH"

    @pytest.mark.unit
    def test_program_legacy_mssp(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("V.36") == "MSSP"

    @pytest.mark.unit
    def test_program_unknown(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("X9999") is None

    @pytest.mark.unit
    def test_program_empty(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("") is None

    @pytest.mark.unit
    def test_program_none(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id(None) is None

    @pytest.mark.unit
    def test_program_a_single_char(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("A") is None

    @pytest.mark.unit
    def test_program_d_no_digits(self):
        from acoharmony._parsers._aco_id import extract_program_from_aco_id

        assert extract_program_from_aco_id("Dabc") is None

    @pytest.mark.unit
    def test_filename_reach_keyword(self):
        from acoharmony._parsers._aco_id import extract_program_from_filename

        assert extract_program_from_filename("REACH_enrollment.csv") == "REACH"

    @pytest.mark.unit
    def test_filename_mssp_keyword(self):
        from acoharmony._parsers._aco_id import extract_program_from_filename

        assert extract_program_from_filename("MSSP_report.csv") == "MSSP"

    @pytest.mark.unit
    def test_filename_fallback_aco_id(self):
        from acoharmony._parsers._aco_id import extract_program_from_filename

        assert extract_program_from_filename("D0259_claims.csv") == "REACH"

    @pytest.mark.unit
    def test_filename_fallback_none(self):
        from acoharmony._parsers._aco_id import extract_program_from_filename

        assert extract_program_from_filename("unknown_file.csv") is None

    @pytest.mark.unit
    def test_filename_empty(self):
        from acoharmony._parsers._aco_id import extract_program_from_filename

        assert extract_program_from_filename("") is None

    @pytest.mark.unit
    def test_filename_none(self):
        from acoharmony._parsers._aco_id import extract_program_from_filename

        assert extract_program_from_filename(None) is None
