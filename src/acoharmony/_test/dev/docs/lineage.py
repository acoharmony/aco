"""Tests for acoharmony._dev.docs.lineage module."""



# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest
from unittest.mock import patch, mock_open

import acoharmony
from acoharmony._dev.docs.lineage import load_all_schemas, generate_data_lineage


class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._dev.docs.lineage is not None


class TestLoadAllSchemasBranches:
    """Tests for uncovered branches in load_all_schemas."""

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_config_none_skips_schema(self, mock_registry):
        """Branch 37->38: config is falsy (None), schema is skipped."""
        mock_registry.list_schemas.return_value = ["schema_a"]
        mock_registry.get_full_table_config.return_value = None
        result = load_all_schemas()
        assert "schema_a" not in result

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_config_missing_name_skips_schema(self, mock_registry):
        """Branch 37->38: config present but missing 'name' key, skipped."""
        mock_registry.list_schemas.return_value = ["schema_b"]
        mock_registry.get_full_table_config.return_value = {"foo": "bar"}
        result = load_all_schemas()
        assert "schema_b" not in result

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_staging_as_list(self, mock_registry):
        """Branch 46->48, 48->49: staging is a list, extends depends."""
        mock_registry.list_schemas.return_value = ["test_schema"]
        mock_registry.get_full_table_config.return_value = {
            "name": "test_schema",
            "staging": ["dep_a", "dep_b"],
        }
        result = load_all_schemas()
        assert "dep_a" in result["test_schema"]["depends"]
        assert "dep_b" in result["test_schema"]["depends"]

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_staging_neither_str_nor_list(self, mock_registry):
        """Branch 48->52: staging is neither str nor list (dict), no deps added."""
        mock_registry.list_schemas.return_value = ["test_schema"]
        mock_registry.get_full_table_config.return_value = {
            "name": "test_schema",
            "staging": {"key": "value"},
        }
        result = load_all_schemas()
        assert result["test_schema"]["depends"] == []

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_union_sources_as_list(self, mock_registry):
        """Branch 52->53, 54->55: union with list sources extends depends."""
        mock_registry.list_schemas.return_value = ["test_schema"]
        mock_registry.get_full_table_config.return_value = {
            "name": "test_schema",
            "union": {"sources": ["src_a", "src_b"]},
        }
        result = load_all_schemas()
        assert "src_a" in result["test_schema"]["depends"]
        assert "src_b" in result["test_schema"]["depends"]

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_union_sources_not_list(self, mock_registry):
        """Branch 54->58: union sources is a string, not list, so not extended."""
        mock_registry.list_schemas.return_value = ["test_schema"]
        mock_registry.get_full_table_config.return_value = {
            "name": "test_schema",
            "union": {"sources": "single_source"},
        }
        result = load_all_schemas()
        assert result["test_schema"]["depends"] == []

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_pivot_sources_as_list(self, mock_registry):
        """Branch 58->59, 60->61: pivot with list sources extends depends."""
        mock_registry.list_schemas.return_value = ["test_schema"]
        mock_registry.get_full_table_config.return_value = {
            "name": "test_schema",
            "pivot": {"sources": ["pivot_a", "pivot_b"]},
        }
        result = load_all_schemas()
        assert "pivot_a" in result["test_schema"]["depends"]
        assert "pivot_b" in result["test_schema"]["depends"]

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.SchemaRegistry")
    def test_pivot_sources_not_list(self, mock_registry):
        """Branch 60->64: pivot sources is a string, not list, so not extended."""
        mock_registry.list_schemas.return_value = ["test_schema"]
        mock_registry.get_full_table_config.return_value = {
            "name": "test_schema",
            "pivot": {"sources": "single_pivot_source"},
        }
        result = load_all_schemas()
        assert result["test_schema"]["depends"] == []


class TestGenerateDataLineageMermaidBranches:
    """Tests for Mermaid diagram node classification branches in generate_data_lineage."""

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_source_classified_as_consolidated(self, mock_load):
        """Branch 417->423, 423->424: source not raw, contains 'consolidated'.

        Setup: cclf1 depends on institutional_claim (reversed flow so
        institutional_claim is first seen as source). Then eligibility
        depends on consolidated_alignment, making consolidated_alignment
        a source first. consolidated_alignment has deps -> not raw.
        """
        mock_load.return_value = {
            "cclf8": {"depends": []},
            "beneficiary_demographics": {"depends": ["cclf8"]},
            "enrollment": {"depends": ["beneficiary_demographics"]},
            # eligibility depends on consolidated_alignment: creates
            # (consolidated_alignment, eligibility) relationship.
            # consolidated_alignment is first seen as source.
            "eligibility": {"depends": ["consolidated_alignment"]},
            "consolidated_alignment": {"depends": ["enrollment"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_source_classified_as_processed(self, mock_load):
        """Branch 423->425, 425->428: source not raw, not consolidated,
        not report/engagement -> processed.

        Setup: cclf1 depends on institutional_claim (reversed dependency
        so institutional_claim appears as source before it appears as
        a target). institutional_claim has deps on a non-key-chains schema
        so it is not raw.
        """
        mock_load.return_value = {
            # institutional_claim has deps -> not raw
            "institutional_claim": {"depends": ["some_internal_dep"]},
            # cclf1 depends on institutional_claim (reversed flow)
            # -> relationship (institutional_claim, cclf1)
            # institutional_claim is first seen as source: not raw,
            # not "cclf", not in raw list, not "consolidated",
            # not "report"/"engagement" -> processed
            "cclf1": {"depends": ["institutional_claim"]},
            "some_internal_dep": {"depends": []},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_target_classified_as_raw(self, mock_load):
        """Branch 433->438: target is raw.

        Setup: bar depends on alr, both in key_chains.
        Relationship: (alr, bar). Target=bar has no deps -> raw.
        """
        mock_load.return_value = {
            "alr": {"depends": []},
            "bar": {"depends": ["alr"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_consolidated_as_target(self, mock_load):
        """Target classified as consolidated_alignment.

        Setup: consolidated_alignment depends on enrollment.
        Relationship: (enrollment, consolidated_alignment).
        Target=consolidated_alignment contains 'consolidated'.
        """
        mock_load.return_value = {
            "cclf8": {"depends": []},
            "beneficiary_demographics": {"depends": ["cclf8"]},
            "enrollment": {"depends": ["beneficiary_demographics"]},
            "consolidated_alignment": {"depends": ["enrollment"]},
            "eligibility": {"depends": ["enrollment"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True


class TestSourceClassifiedAsReport:
    """Cover branches 425->426 (source with 'report'/'engagement') and 441->442 (target)."""

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_source_with_report_in_name(self, mock_load):
        """Branch 425->426: source contains 'report', classified as report_nodes."""
        mock_load.return_value = {
            "cclf8": {"depends": []},
            "some_report_summary": {"depends": ["cclf8"]},
            # some_report_summary appears as a source for another node
            "final_output": {"depends": ["some_report_summary"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_source_with_engagement_in_name(self, mock_load):
        """Branch 425->426: source contains 'engagement', classified as report_nodes."""
        mock_load.return_value = {
            "cclf8": {"depends": []},
            "beneficiary_engagement": {"depends": ["cclf8"]},
            "engagement_summary": {"depends": ["beneficiary_engagement"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_target_with_report_in_name(self, mock_load):
        """Branch 441->442: target contains 'report', classified as report_nodes."""
        mock_load.return_value = {
            "cclf8": {"depends": []},
            # processed node that depends on cclf8
            "beneficiary_demographics": {"depends": ["cclf8"]},
            # report target that depends on processed node
            "quality_report": {"depends": ["beneficiary_demographics"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_target_with_engagement_in_name(self, mock_load):
        """Branch 441->442: target contains 'engagement', classified as report_nodes."""
        mock_load.return_value = {
            "cclf8": {"depends": []},
            "beneficiary_demographics": {"depends": ["cclf8"]},
            "member_engagement": {"depends": ["beneficiary_demographics"]},
        }
        with patch("builtins.open", mock_open()):
            result = generate_data_lineage()
        assert result is True

    @pytest.mark.unit
    @patch("acoharmony._dev.docs.lineage.load_all_schemas")
    def test_report_nodes_produces_class_statement(self, mock_load):
        """Line 454->455: report_nodes is non-empty, class statement written."""
        mock_load.return_value = {
            "cclf8": {"depends": []},
            "beneficiary_demographics": {"depends": ["cclf8"]},
            "quality_report": {"depends": ["beneficiary_demographics"]},
        }
        m = mock_open()
        with patch("builtins.open", m):
            result = generate_data_lineage()
        assert result is True
        # Check that the "class ... report" line was written
        written = "".join(call.args[0] for call in m().write.call_args_list)
        assert "report" in written
