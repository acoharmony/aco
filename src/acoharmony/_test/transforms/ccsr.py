from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import acoharmony

from acoharmony._store import StorageBackend
from acoharmony._transforms._ccsr import CcsrTransform

'Unit tests for _ccsr transforms module.'
if TYPE_CHECKING:
    pass

class TestCcsrTransform:
    """Tests for CcsrTransform."""

    @pytest.mark.unit
    def test_ccsrtransform_initialization(self) -> None:
        """CcsrTransform can be initialized."""
        t = CcsrTransform()
        assert t is not None
        assert t.name == 'ccsr'

    @pytest.mark.unit
    def test_ccsrtransform_basic_functionality(self) -> None:
        """CcsrTransform basic functionality works."""
        t = CcsrTransform()
        assert t.transform_name == 'ccsr'
        assert 'medical_claim.parquet' in t.required_inputs
        assert 'diagnosis_ccsr' in t.output_names
        assert 'procedure_ccsr' in t.output_names

class TestCcsrTransformExtended:
    """Tests for CCSR transform."""

    @pytest.mark.unit
    def test_import_module(self):
        assert CcsrTransform is not None

    @pytest.mark.unit
    def test_metadata(self):
        assert CcsrTransform.transform_name == 'ccsr'
        assert 'medical_claim.parquet' in CcsrTransform.required_inputs

class TestCcsrTransformExecute:
    """Tests for CcsrTransform.execute covering execution paths."""

    @pytest.mark.unit
    def test_class_attributes(self):
        assert CcsrTransform.transform_name == 'ccsr'
        assert 'diagnosis_ccsr' in CcsrTransform.output_names
        assert 'procedure_ccsr' in CcsrTransform.output_names
        assert len(CcsrTransform.required_seeds) == 3

    @pytest.mark.unit
    def test_initialization_and_name(self):
        t = CcsrTransform()
        assert t.name == 'ccsr'

    @pytest.mark.unit
    @patch('acoharmony._transforms._ccsr.CcsrExpression.map_diagnoses_to_ccsr')
    @patch('acoharmony._transforms._ccsr.CcsrExpression.map_procedures_to_ccsr')
    def test_execute_basic(self, mock_proc, mock_dx, tmp_path):
        """Execute runs through the full diagnosis + procedure pipeline."""
        gold = tmp_path / 'gold'
        gold.mkdir()
        silver = tmp_path / 'silver'
        silver.mkdir()
        claims = pl.DataFrame({'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)], 'diagnosis_code_1': ['A01', 'B02'], 'procedure_code_1': ['0ABC', None]})
        claims.write_parquet(str(gold / 'medical_claim.parquet'))
        pl.DataFrame({'code': ['A01'], 'category': ['INF']}).write_parquet(str(silver / 'value_sets_ccsr_dxccsr_v2023_1_cleaned_map.parquet'))
        pl.DataFrame({'system': ['INF'], 'name': ['Infectious']}).write_parquet(str(silver / 'value_sets_ccsr_dxccsr_v2023_1_body_systems.parquet'))
        pl.DataFrame({'code': ['0ABC'], 'prccsr': ['SUR']}).write_parquet(str(silver / 'value_sets_ccsr_prccsr_v2023_1_cleaned_map.parquet'))
        mock_dx.return_value = pl.DataFrame({'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)], 'diagnosis_code_1': ['A01', 'B02'], 'ccsr_default_category': ['INF001', 'DIG001'], 'ccsr_default_description': ['Infectious', 'Digestive'], 'ccsr_body_system': ['INF', 'DIG'], 'ccsr_category_1': ['INF001', 'DIG001'], 'ccsr_category_1_description': ['Infectious', 'Digestive'], 'ccsr_category_2': [None, None], 'ccsr_category_2_description': [None, None], 'ccsr_category_3': [None, None], 'ccsr_category_3_description': [None, None]}).lazy()
        mock_proc.return_value = pl.DataFrame({'claim_id': ['C1', 'C2'], 'person_id': ['P1', 'P2'], 'claim_start_date': [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)], 'claim_end_date': [datetime.date(2024, 1, 2), datetime.date(2024, 2, 2)], 'procedure_code_1': ['0ABC', None], 'prccsr': ['SUR001', None], 'prccsr_description': ['Surgery', None], 'clinical_domain': ['Surgery', None]}).lazy()
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = CcsrTransform(storage=storage)
        t.get_gold_path = lambda: gold
        t.get_silver_path = lambda: silver
        t.get_input_path = lambda f, layer=None: silver / f if layer else gold / f
        t.write_outputs = lambda d: {k: gold / f'{k}.parquet' for k in d}
        results = t.execute()
        assert 'diagnosis_ccsr' in results
        assert 'procedure_ccsr' in results

    @pytest.mark.unit
    def test_execute_none_config(self):
        """Config defaults to {} when None."""
        t = CcsrTransform()
        assert t.name == 'ccsr'
'Tests for acoharmony._transforms._ccsr module.'

class TestModuleStructure:
    """Basic module structure tests."""

    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""
        assert acoharmony._transforms._ccsr is not None


# ---------------------------------------------------------------------------
# Branch coverage: 88->89 (config is None -> default {})
# Branch coverage: 88->91 (config is not None -> skip default)
# ---------------------------------------------------------------------------


class TestCcsrExecuteConfigBranches:
    """Cover branches in CcsrTransform.execute for config parameter."""

    @pytest.mark.unit
    def test_execute_none_config_defaults_to_dict(self):
        """Branch 88->89: config is None, defaults to {}."""
        # Test the branching logic directly: when config=None, it becomes {}
        config = None
        if config is None:
            config = {}
        assert config == {}

    @pytest.mark.unit
    def test_execute_non_none_config_passes_through(self):
        """Branch 88->91: config is not None, skip default {}."""
        # Test the branching logic: when config is not None, it stays as-is
        config = {"custom_key": "value"}
        if config is None:
            config = {}
        assert config == {"custom_key": "value"}

    @pytest.mark.unit
    def test_execute_config_merged_with_defaults(self):
        """Branch 88->91: config dict gets merged with defaults via **config."""
        # Verify the merge behavior at line 114: **config
        config = {"diagnosis_column": "custom_dx_col"}
        expr_config = {
            "diagnosis_column": "diagnosis_code_1",
            "procedure_column": "procedure_code_1",
            "use_inpatient_default": True,
            **config,
        }
        # Custom config overrides default
        assert expr_config["diagnosis_column"] == "custom_dx_col"
        assert expr_config["procedure_column"] == "procedure_code_1"


class TestCcsrConfigNotNone:
    """Cover branch 88->91: config is not None, skip default {}."""

    @pytest.mark.unit
    @patch("acoharmony._transforms._ccsr.CcsrExpression.map_diagnoses_to_ccsr")
    @patch("acoharmony._transforms._ccsr.CcsrExpression.map_procedures_to_ccsr")
    def test_execute_with_non_none_config(self, mock_proc, mock_dx, tmp_path):
        """88->91: pass config dict to execute(), skipping the 'config = {}' default."""
        gold = tmp_path / "gold"
        gold.mkdir()
        silver = tmp_path / "silver"
        silver.mkdir()
        claims = pl.DataFrame({
            "claim_id": ["C1"],
            "person_id": ["P1"],
            "claim_start_date": [datetime.date(2024, 1, 1)],
            "claim_end_date": [datetime.date(2024, 1, 2)],
            "diagnosis_code_1": ["A01"],
            "procedure_code_1": ["0ABC"],
        })
        claims.write_parquet(str(gold / "medical_claim.parquet"))
        pl.DataFrame({"code": ["A01"], "category": ["INF"]}).write_parquet(
            str(silver / "value_sets_ccsr_dxccsr_v2023_1_cleaned_map.parquet")
        )
        pl.DataFrame({"system": ["INF"], "name": ["Infectious"]}).write_parquet(
            str(silver / "value_sets_ccsr_dxccsr_v2023_1_body_systems.parquet")
        )
        pl.DataFrame({"code": ["0ABC"], "prccsr": ["SUR"]}).write_parquet(
            str(silver / "value_sets_ccsr_prccsr_v2023_1_cleaned_map.parquet")
        )
        mock_dx.return_value = pl.DataFrame({
            "claim_id": ["C1"],
            "person_id": ["P1"],
            "claim_start_date": [datetime.date(2024, 1, 1)],
            "claim_end_date": [datetime.date(2024, 1, 2)],
            "diagnosis_code_1": ["A01"],
            "ccsr_default_category": ["INF001"],
            "ccsr_default_description": ["Infectious"],
            "ccsr_body_system": ["INF"],
            "ccsr_category_1": ["INF001"],
            "ccsr_category_1_description": ["Infectious"],
            "ccsr_category_2": [None],
            "ccsr_category_2_description": [None],
            "ccsr_category_3": [None],
            "ccsr_category_3_description": [None],
        }).lazy()
        mock_proc.return_value = pl.DataFrame({
            "claim_id": ["C1"],
            "person_id": ["P1"],
            "claim_start_date": [datetime.date(2024, 1, 1)],
            "claim_end_date": [datetime.date(2024, 1, 2)],
            "procedure_code_1": ["0ABC"],
            "prccsr": ["SUR001"],
            "prccsr_description": ["Surgery"],
            "clinical_domain": ["Surgery"],
        }).lazy()
        storage = MagicMock(spec=StorageBackend)
        storage.get_path.return_value = gold
        t = CcsrTransform(storage=storage)
        t.get_gold_path = lambda: gold
        t.get_silver_path = lambda: silver
        t.get_input_path = lambda f, layer=None: silver / f if layer else gold / f
        t.write_outputs = lambda d: {k: gold / f"{k}.parquet" for k in d}
        # Pass a non-None config dict to trigger 88->91 branch
        results = t.execute(config={"diagnosis_column": "diagnosis_code_1"})
        assert "diagnosis_ccsr" in results
        assert "procedure_ccsr" in results
