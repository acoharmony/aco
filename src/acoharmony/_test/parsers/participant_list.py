"""Tests for acoharmony._parsers._participant_list module."""

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from pathlib import Path
import pytest
import acoharmony

from .conftest import _schema


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._parsers._participant_list is not None


class TestParticipantListCoverageGaps:
    """Additional tests for _participant_list coverage gaps."""

    @pytest.mark.unit
    def test_d0259_column_already_exists(self):
        """Cover branch where a column from missing_columns already exists."""
        from acoharmony._parsers._participant_list import normalize_participant_list

        df = pl.DataFrame(
            {"Billing TIN": ["123"], "TIN Legal Bus Name": ["Org"], "Entity ID": ["existing_value"]}
        )
        result = normalize_participant_list(df, "d0259.xlsx")
        assert result["Entity ID"][0] == "existing_value"


class TestParticipantListExcelCoverageGaps:
    """Additional tests for _participant_list_excel coverage gaps."""

    @pytest.mark.unit
    def test_harmonycares_entity_columns_exist(self, tmp_path):
        """Cover branches where entity columns already exist."""
        from acoharmony._parsers._participant_list_excel import _normalize_harmonycares_format

        data = {f"col_{i}": ["val"] for i in range(27)}
        df = pl.DataFrame(data)
        output_names = [f"output_{i}" for i in range(51)]
        schema = _schema([{"output_name": name} for name in output_names])
        df = df.with_columns(
            [
                pl.lit("EXISTING", dtype=pl.Utf8).alias("entity_id"),
                pl.lit("EXISTING_TIN", dtype=pl.Utf8).alias("entity_tin"),
                pl.lit("EXISTING_NAME", dtype=pl.Utf8).alias("entity_legal_business_name"),
            ]
        )
        result = _normalize_harmonycares_format(
            df, schema, Path("D0259 Provider List - 1-30-2026 15.27.44.xlsx")
        )
        assert result["entity_id"][0] == "EXISTING"

class TestParticipantList:
    """Tests for acoharmony._parsers._participant_list."""

    @pytest.mark.unit
    def test_normalize_reach_format(self):
        from acoharmony._parsers._participant_list import normalize_participant_list

        df = pl.DataFrame(
            {"Entity ID": ["D0259"], "Entity TIN": ["123456789"], "Provider Type": ["Group"]}
        )
        result = normalize_participant_list(df, "reach_file.xlsx")
        assert "Entity ID" in result.columns

    @pytest.mark.unit
    def test_normalize_d0259_format(self):
        from acoharmony._parsers._participant_list import normalize_participant_list

        df = pl.DataFrame(
            {
                "Billing TIN": ["123456789"],
                "TIN Legal Bus Name": ["Test Org"],
                "Provider Type": ["Group"],
            }
        )
        result = normalize_participant_list(df, "d0259_file.xlsx")
        assert "Base Provider TIN" in result.columns
        assert "Entity ID" in result.columns

    @pytest.mark.unit
    def test_normalize_unknown_format_raises(self):
        from acoharmony._parsers._participant_list import normalize_participant_list

        df = pl.DataFrame({"Unknown Column": ["val"]})
        with pytest.raises(ValueError, match="Unknown participant list format"):
            normalize_participant_list(df, "unknown_file.xlsx")

    @pytest.mark.unit
    def test_normalize_d0259_missing_columns_added(self):
        from acoharmony._parsers._participant_list import normalize_participant_list

        df = pl.DataFrame({"Billing TIN": ["123"], "TIN Legal Bus Name": ["Org"]})
        result = normalize_participant_list(df, "d0259.xlsx")
        assert "Entity ID" in result.columns
        assert "Performance Year" in result.columns
        assert "Telehealth" in result.columns

    @pytest.mark.unit
    def test_normalize_d0259_column_mapping(self):
        from acoharmony._parsers._participant_list import normalize_participant_list

        df = pl.DataFrame(
            {
                "Billing TIN": ["123"],
                "TIN Legal Bus Name": ["Org"],
                "Organization NPI": ["NPI1"],
                "CCN": ["CCN1"],
                "Individual NPI(s)": ["INPI1"],
                "Last Name": ["Smith"],
                "First Name": ["John"],
                "Email": ["test@test.com"],
                "Other": ["other"],
                "CEHRT ID": ["C1"],
            }
        )
        result = normalize_participant_list(df, "d0259.xlsx")
        assert "Base Provider TIN" in result.columns
        assert "Provider Legal Business Name" in result.columns
        assert "Individual NPI" in result.columns
        assert "Individual Last Name" in result.columns
        assert "Individual First Name" in result.columns
