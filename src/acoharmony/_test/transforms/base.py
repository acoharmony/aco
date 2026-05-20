





# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from pathlib import Path
from unittest.mock import MagicMock
import datetime
from unittest.mock import patch

import pytest
import acoharmony

from acoharmony._transforms._base import (
    ChronicConditionsTransform,
    CmsHccTransform,
    FinancialPmpmTransform,
    QualityMeasuresTransform,
    ReadmissionsTransform,
)
from acoharmony._expressions import (
    ChronicConditionsExpression,
    ReadmissionsExpression,
)

# © 2025 HarmonyCares
# All rights reserved.


"""Unit tests for _base transforms module."""









class TestTransformConfig:
    """Tests for TransformConfig."""


    @pytest.mark.unit
    def test_default_initialization(self):
        config = TransformConfig()
        assert config.storage is None
        assert config.force_refresh is False
        assert config.validate_outputs is True
        assert config.write_compression == "zstd"
        assert config.extra_config == {}

    @pytest.mark.unit
    def test_with_storage(self):
        mock_storage = MagicMock()
        config = TransformConfig()
        result = config.with_storage(mock_storage)
        assert result is config  # fluent API
        assert config.storage is mock_storage

    @pytest.mark.unit
    def test_with_force_refresh(self):
        config = TransformConfig()
        result = config.with_force_refresh(True)
        assert result is config
        assert config.force_refresh is True

    @pytest.mark.unit
    def test_with_force_refresh_default(self):
        config = TransformConfig()
        config.with_force_refresh()
        assert config.force_refresh is True

    @pytest.mark.unit
    def test_with_validation(self):
        config = TransformConfig()
        config.with_validation(False)
        assert config.validate_outputs is False

    @pytest.mark.unit
    def test_with_validation_default(self):
        config = TransformConfig()
        config.with_validation()
        assert config.validate_outputs is True

    @pytest.mark.unit
    def test_with_compression(self):
        config = TransformConfig()
        config.with_compression("snappy")
        assert config.write_compression == "snappy"

    @pytest.mark.unit
    def test_merge(self):
        config = TransformConfig()
        result = config.merge(key1="value1", key2=42)
        assert result is config
        assert config.extra_config == {"key1": "value1", "key2": 42}

    @pytest.mark.unit
    def test_merge_updates(self):
        config = TransformConfig()
        config.merge(a=1)
        config.merge(b=2)
        assert config.extra_config == {"a": 1, "b": 2}

    @pytest.mark.unit
    def test_fluent_chaining(self):
        mock_storage = MagicMock()
        config = (
            TransformConfig()
            .with_storage(mock_storage)
            .with_force_refresh()
            .with_compression("lz4")
            .merge(custom_key="custom_val")
        )
        assert config.storage is mock_storage
        assert config.force_refresh is True
        assert config.write_compression == "lz4"
        assert config.extra_config["custom_key"] == "custom_val"


class TestHealthcareTransformBase:
    """Tests for HealthcareTransformBase."""


    def _make_concrete(self):
        """Create a concrete subclass of the abstract base class."""


        class ConcreteTransform(HealthcareTransformBase):
            transform_name = "test_transform"
            required_inputs = ["input.parquet"]
            required_seeds = ["seed.parquet"]
            output_names = ["output"]

            def execute(self, config=None):
                return {"output": Path("/tmp/output.parquet")}

        return ConcreteTransform

    @pytest.mark.unit
    def test_initialization_default(self):
        cls = self._make_concrete()
        t = cls()
        assert t.name == "test_transform"
        assert t.storage is not None
        assert t.config is not None

    @pytest.mark.unit
    def test_initialization_with_storage(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        t = cls(storage=mock_storage)
        assert t.storage is mock_storage

    @pytest.mark.unit
    def test_initialization_with_config(self):
        cls = self._make_concrete()
        config = TransformConfig(force_refresh=True)
        t = cls(config=config)
        assert t.config.force_refresh is True

    @pytest.mark.unit
    def test_create_factory(self):
        cls = self._make_concrete()
        t = cls.create()
        assert isinstance(t, HealthcareTransformBase)
        assert t.name == "test_transform"

    @pytest.mark.unit
    def test_with_defaults(self):
        cls = self._make_concrete()
        t = cls.with_defaults()
        assert isinstance(t, HealthcareTransformBase)

    @pytest.mark.unit
    def test_get_gold_path(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/data/gold")
        t = cls(storage=mock_storage)
        assert t.get_gold_path() == Path("/data/gold")

    @pytest.mark.unit
    def test_get_silver_path(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/data/silver")
        t = cls(storage=mock_storage)
        assert t.get_silver_path() == Path("/data/silver")

    @pytest.mark.unit
    def test_get_input_path(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/data/gold")
        t = cls(storage=mock_storage)
        result = t.get_input_path("test.parquet")
        assert result == Path("/data/gold/test.parquet")

    @pytest.mark.unit
    def test_get_output_path(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/data/gold")
        t = cls(storage=mock_storage)
        result = t.get_output_path("result.parquet")
        assert result == Path("/data/gold/result.parquet")

    @pytest.mark.unit
    def test_load_optional_parquet_missing(self, tmp_path):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)
        result = t.load_optional_parquet("nonexistent.parquet")
        df = result.collect()
        assert df.height == 0

    @pytest.mark.unit
    def test_load_optional_parquet_missing_with_schema(self, tmp_path):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)
        result = t.load_optional_parquet(
            "nonexistent.parquet",
            default_schema={"col1": pl.Utf8, "col2": pl.Int64},
        )
        df = result.collect()
        assert df.height == 0
        assert "col1" in df.columns
        assert "col2" in df.columns

    @pytest.mark.unit
    def test_load_optional_parquet_exists(self, tmp_path):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)

        # Create parquet file
        pl.DataFrame({"a": [1, 2], "b": ["x", "y"]}).write_parquet(
            tmp_path / "exists.parquet"
        )

        result = t.load_optional_parquet("exists.parquet")
        df = result.collect()
        assert df.height == 2

    @pytest.mark.unit
    def test_write_output(self, tmp_path):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)

        data = pl.DataFrame({"a": [1, 2]}).lazy()
        path = t.write_output(data, "output.parquet")
        assert path.exists()

    @pytest.mark.unit
    def test_write_outputs(self, tmp_path):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)

        outputs = {
            "result1": pl.DataFrame({"a": [1]}).lazy(),
            "result2": pl.DataFrame({"b": [2]}).lazy(),
        }
        paths = t.write_outputs(outputs)
        assert "result1" in paths
        assert "result2" in paths
        assert paths["result1"].exists()
        assert paths["result2"].exists()

    @pytest.mark.unit
    def test_callable(self):
        cls = self._make_concrete()
        t = cls()
        result = t()
        assert isinstance(result, dict)
        assert "output" in result

    @pytest.mark.unit
    def test_repr(self):
        cls = self._make_concrete()
        t = cls()
        r = repr(t)
        assert "test_transform" in r

    @pytest.mark.unit
    def test_class_variables(self):
        cls = self._make_concrete()
        assert cls.transform_name == "test_transform"
        assert cls.required_inputs == ["input.parquet"]
        assert cls.required_seeds == ["seed.parquet"]
        assert cls.output_names == ["output"]


class TestCmsHccTransform:
    """Tests for CmsHccTransform."""


    @pytest.mark.unit
    def test_class_metadata(self):
        assert CmsHccTransform.transform_name == "cms_hcc"
        assert "medical_claim.parquet" in CmsHccTransform.required_inputs
        assert "eligibility.parquet" in CmsHccTransform.required_inputs
        assert len(CmsHccTransform.required_seeds) == 3
        assert len(CmsHccTransform.output_names) == 2

    @pytest.mark.unit
    def test_initialization(self):
        t = CmsHccTransform()
        assert t.name == "cms_hcc"


class TestReadmissionsTransform:
    """Tests for ReadmissionsTransform."""


    @pytest.mark.unit
    def test_class_metadata(self):
        assert ReadmissionsTransform.transform_name == "readmissions"
        assert "medical_claim.parquet" in ReadmissionsTransform.required_inputs
        assert len(ReadmissionsTransform.required_seeds) == 2

    @pytest.mark.unit
    def test_initialization(self):
        t = ReadmissionsTransform()
        assert t.name == "readmissions"


class TestChronicConditionsTransform:
    """Tests for ChronicConditionsTransform."""


    @pytest.mark.unit
    def test_class_metadata(self):
        assert ChronicConditionsTransform.transform_name == "chronic_conditions"
        assert "medical_claim.parquet" in ChronicConditionsTransform.required_inputs
        assert len(ChronicConditionsTransform.output_names) == 2

    @pytest.mark.unit
    def test_initialization(self):
        t = ChronicConditionsTransform()
        assert t.name == "chronic_conditions"


class TestFinancialPmpmTransform:
    """Tests for FinancialPmpmTransform."""


    @pytest.mark.unit
    def test_class_metadata(self):
        assert FinancialPmpmTransform.transform_name == "financial_pmpm"
        assert "service_category.parquet" in FinancialPmpmTransform.required_inputs
        assert len(FinancialPmpmTransform.output_names) == 3

    @pytest.mark.unit
    def test_initialization(self):
        t = FinancialPmpmTransform()
        assert t.name == "financial_pmpm"


class TestQualityMeasuresTransform:
    """Tests for QualityMeasuresTransform."""


    @pytest.mark.unit
    def test_class_metadata(self):
        assert QualityMeasuresTransform.transform_name == "quality_measures"
        assert "eligibility.parquet" in QualityMeasuresTransform.required_inputs
        assert len(QualityMeasuresTransform.output_names) == 1

    @pytest.mark.unit
    def test_initialization(self):
        t = QualityMeasuresTransform()
        assert t.name == "quality_measures"


class TestReadmissionsTransformExecuteExplicitConfig:
    """Tests for ReadmissionsTransform.execute with explicit config."""

    @pytest.mark.unit
    def test_execute_with_explicit_config(self, tmp_path):
        """Cover branch 411->414: config is not None so we skip the default assignment."""
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = ReadmissionsTransform(storage=mock_storage)

        # Create minimal medical_claim parquet
        medical_claims = pl.DataFrame({
            "person_id": ["p1", "p2"],
            "claim_id": ["c1", "c2"],
            "claim_type": ["institutional", "institutional"],
            "bill_type_code": ["111", "112"],
            "admission_date": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
            "discharge_date": [datetime.date(2024, 1, 5), datetime.date(2024, 2, 5)],
            "diagnosis_code_1": ["A01", "B02"],
        })
        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")

        mock_readmissions_result = pl.DataFrame({
            "patient_id": ["p1"],
            "readmission": [True],
        }).lazy()

        with patch.object(
            ReadmissionsExpression,
            "transform_readmission_pairs",
            return_value=mock_readmissions_result,
        ):
            result = t.execute(config={"lookback_days": 45})

        assert isinstance(result, dict)
        assert "readmissions_summary" in result


class TestChronicConditionsTransformExecuteExplicitConfig:
    """Tests for ChronicConditionsTransform.execute with explicit config."""

    @pytest.mark.unit
    def test_execute_with_explicit_config(self, tmp_path):
        """Cover branch 506->509: config is not None so we skip the default assignment."""
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = ChronicConditionsTransform(storage=mock_storage)

        # Create minimal medical_claim parquet
        medical_claims = pl.DataFrame({
            "person_id": ["p1"],
            "diagnosis_code_1": ["E11"],
            "claim_end_date": [datetime.date(2024, 1, 1)],
        })
        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")

        # Create seed parquet
        condition_mapping = pl.DataFrame({
            "condition": ["diabetes"],
            "icd_10_cm_code": ["E11"],
        })
        condition_mapping.write_parquet(
            tmp_path / "value_sets_chronic_conditions_cms_chronic_conditions_hierarchy.parquet"
        )

        mock_long = pl.DataFrame({"patient_id": ["p1"], "condition": ["diabetes"]}).lazy()
        mock_wide = pl.DataFrame({"patient_id": ["p1"], "diabetes": [True]}).lazy()

        with patch.object(
            ChronicConditionsExpression,
            "transform_patient_conditions_long",
            return_value=mock_long,
        ), patch.object(
            ChronicConditionsExpression,
            "transform_patient_conditions_wide",
            return_value=mock_wide,
        ):
            result = t.execute(config={"min_claims_outpatient": 3})

        assert isinstance(result, dict)
        assert "chronic_conditions_long" in result
        assert "chronic_conditions_wide" in result


class TestFinancialPmpmTransformExecuteExplicitConfig:
    """Tests for FinancialPmpmTransform.execute with explicit config."""

    @pytest.mark.unit
    def test_execute_with_explicit_config(self, tmp_path):
        """Cover branch 594->597: config is not None so we skip the default assignment."""
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = FinancialPmpmTransform(storage=mock_storage)

        # Create service_category parquet
        service_category = pl.DataFrame({
            "person_id": ["p1", "p2"],
            "service_category_1": ["inpatient", "outpatient"],
            "service_category_2": ["medical", "surgical"],
            "paid": [1000.0, 500.0],
            "claim_end_date": [datetime.date(2024, 1, 15), datetime.date(2024, 1, 20)],
        })
        service_category.write_parquet(tmp_path / "service_category.parquet")

        # Create eligibility parquet
        eligibility = pl.DataFrame({
            "person_id": ["p1", "p2"],
            "enrollment_start_date": [datetime.date(2024, 1, 1), datetime.date(2024, 1, 1)],
        })
        eligibility.write_parquet(tmp_path / "eligibility.parquet")

        result = t.execute(config={})

        assert isinstance(result, dict)
        assert "financial_pmpm_by_service_category" in result
        assert "financial_pmpm_by_service_category_time" in result
        assert "financial_pmpm_summary" in result


class TestHealthcareTransformContext:
    """Tests for HealthcareTransformContext."""


    def _make_concrete(self):
        class ConcreteTransform(HealthcareTransformBase):
            transform_name = "ctx_test"
            def execute(self, config=None):
                return {"ok": Path("/tmp/ok")}
        return ConcreteTransform

    @pytest.mark.unit
    def test_enter_creates_transform(self):
        cls = self._make_concrete()
        ctx = HealthcareTransformContext(cls)
        with ctx as t:
            assert isinstance(t, HealthcareTransformBase)
            assert t.name == "ctx_test"

    @pytest.mark.unit
    def test_exit_on_success(self):
        cls = self._make_concrete()
        ctx = HealthcareTransformContext(cls)
        with ctx:
            pass  # No exception
        # Should not raise

    @pytest.mark.unit
    def test_exit_on_exception(self):
        cls = self._make_concrete()
        ctx = HealthcareTransformContext(cls)
        with pytest.raises(RuntimeError):
            with ctx:
                raise RuntimeError("test error")

    @pytest.mark.unit
    def test_exit_returns_false(self):
        cls = self._make_concrete()
        ctx = HealthcareTransformContext(cls)
        ctx.__enter__()
        result = ctx.__exit__(None, None, None)
        assert result is False


class TestRunTransform:
    """Tests for run_transform function."""


    @pytest.mark.unit
    def test_basic(self):
        class SimpleTransform(HealthcareTransformBase):
            transform_name = "simple"
            def execute(self, config=None):
                return {"out": Path("/tmp/out")}

        result = run_transform(SimpleTransform)
        assert "out" in result

    @pytest.mark.unit
    def test_with_config(self):
        class ConfigTransform(HealthcareTransformBase):
            transform_name = "cfg"
            def execute(self, config=None):
                return {"cfg": Path("/tmp/cfg")}

        result = run_transform(ConfigTransform, config={"key": "val"})
        assert "cfg" in result


class TestRunAllHealthcareTransforms:
    """Tests for run_all_healthcare_transforms function."""


    @patch.object(CmsHccTransform, "execute")
    @patch.object(ReadmissionsTransform, "execute")
    @patch.object(ChronicConditionsTransform, "execute")
    @patch.object(FinancialPmpmTransform, "execute")
    @patch.object(QualityMeasuresTransform, "execute")
    @pytest.mark.unit
    def test_runs_all(self, mock_qm, mock_fp, mock_cc, mock_r, mock_hcc):
        mock_hcc.return_value = {"hcc": Path("/tmp/hcc")}
        mock_r.return_value = {"read": Path("/tmp/read")}
        mock_cc.return_value = {"cc": Path("/tmp/cc")}
        mock_fp.return_value = {"fp": Path("/tmp/fp")}
        mock_qm.return_value = {"qm": Path("/tmp/qm")}

        results = run_all_healthcare_transforms()
        assert "cms_hcc" in results
        assert "readmissions" in results
        assert "chronic_conditions" in results
        assert "financial_pmpm" in results
        assert "quality_measures" in results

    @patch.object(CmsHccTransform, "execute")
    @patch.object(ReadmissionsTransform, "execute")
    @patch.object(ChronicConditionsTransform, "execute")
    @patch.object(FinancialPmpmTransform, "execute")
    @patch.object(QualityMeasuresTransform, "execute")
    @pytest.mark.unit
    def test_runs_all_with_storage(self, mock_qm, mock_fp, mock_cc, mock_r, mock_hcc):
        mock_hcc.return_value = {"hcc": Path("/tmp/hcc")}
        mock_r.return_value = {"read": Path("/tmp/read")}
        mock_cc.return_value = {"cc": Path("/tmp/cc")}
        mock_fp.return_value = {"fp": Path("/tmp/fp")}
        mock_qm.return_value = {"qm": Path("/tmp/qm")}

        mock_storage = MagicMock()
        results = run_all_healthcare_transforms(storage=mock_storage)
        assert len(results) == 5


class TestTransformConfigAdditional:
    """Additional TransformConfig tests for coverage."""


    @pytest.mark.unit
    def test_with_validation_false(self):
        config = TransformConfig()
        result = config.with_validation(False)
        assert result is config
        assert config.validate_outputs is False

    @pytest.mark.unit
    def test_with_compression_returns_self(self):
        config = TransformConfig()
        result = config.with_compression("lz4")
        assert result is config
        assert config.write_compression == "lz4"

    @pytest.mark.unit
    def test_merge_overwrites_existing_key(self):
        config = TransformConfig()
        config.merge(key="old")
        config.merge(key="new")
        assert config.extra_config["key"] == "new"

    @pytest.mark.unit
    def test_initialization_with_all_params(self):
        mock_storage = MagicMock()
        config = TransformConfig(
            storage=mock_storage,
            force_refresh=True,
            validate_outputs=False,
            write_compression="snappy",
            extra_config={"custom": "val"},
        )
        assert config.storage is mock_storage
        assert config.force_refresh is True
        assert config.validate_outputs is False
        assert config.write_compression == "snappy"
        assert config.extra_config == {"custom": "val"}


class TestHealthcareTransformBaseAdditional:
    """Additional HealthcareTransformBase tests for coverage."""


    def _make_concrete(self):
        class ConcreteTransform(HealthcareTransformBase):
            transform_name = "test_additional"
            required_inputs = ["input.parquet"]
            required_seeds = ["seed.parquet"]
            output_names = ["output"]

            def execute(self, config=None):
                return {"output": Path("/tmp/output.parquet")}

        return ConcreteTransform

    @pytest.mark.unit
    def test_get_input_path_silver(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = Path("/data/silver")
        t = cls(storage=mock_storage)
        result = t.get_input_path("seed.parquet", MedallionLayer.SILVER)
        assert result == Path("/data/silver/seed.parquet")

    @pytest.mark.unit
    def test_load_optional_parquet_default_schema_none(self, tmp_path):
        """Test load_optional_parquet with default_schema=None."""


        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)
        result = t.load_optional_parquet("nonexistent.parquet", default_schema=None)
        df = result.collect()
        assert df.height == 0
        assert len(df.columns) == 0

    @pytest.mark.unit
    def test_create_with_kwargs(self):
        cls = self._make_concrete()
        t = cls.create(force_refresh=True)
        assert isinstance(t, HealthcareTransformBase)

    @pytest.mark.unit
    def test_callable_with_config(self):
        cls = self._make_concrete()
        t = cls()
        result = t(config={"key": "val"})
        assert "output" in result

    @pytest.mark.unit
    def test_repr_contains_class_name(self):
        cls = self._make_concrete()
        t = cls()
        r = repr(t)
        assert "ConcreteTransform" in r
        assert "test_additional" in r


class TestCmsHccTransformExecute:
    """Tests for CmsHccTransform.execute method."""


    @pytest.mark.unit
    def test_class_metadata_detailed(self):
        assert "risk_factors" in CmsHccTransform.output_names
        assert "risk_scores" in CmsHccTransform.output_names
        assert "value_sets_cms_hcc_icd_10_cm_mappings.parquet" in CmsHccTransform.required_seeds


class TestReadmissionsTransformExecute:
    """Tests for ReadmissionsTransform.execute."""


    @pytest.mark.unit
    def test_class_metadata_detailed(self):
        assert ReadmissionsTransform.transform_name == "readmissions"
        assert "readmissions_summary" in ReadmissionsTransform.output_names
        assert len(ReadmissionsTransform.required_seeds) == 2


class TestChronicConditionsTransformMetadata:
    """Tests for ChronicConditionsTransform metadata."""


    @pytest.mark.unit
    def test_output_names(self):
        assert "conditions_long" in ChronicConditionsTransform.output_names
        assert "conditions_wide" in ChronicConditionsTransform.output_names

    @pytest.mark.unit
    def test_required_seeds(self):
        assert len(ChronicConditionsTransform.required_seeds) == 1
        assert "chronic_conditions" in ChronicConditionsTransform.required_seeds[0]


class TestFinancialPmpmTransformMetadata:
    """Tests for FinancialPmpmTransform metadata."""


    @pytest.mark.unit
    def test_required_inputs(self):
        assert "medical_claim.parquet" in FinancialPmpmTransform.required_inputs
        assert "eligibility.parquet" in FinancialPmpmTransform.required_inputs
        assert "service_category.parquet" in FinancialPmpmTransform.required_inputs

    @pytest.mark.unit
    def test_output_names(self):
        assert "pmpm_by_service_category" in FinancialPmpmTransform.output_names
        assert "pmpm_by_service_category_time" in FinancialPmpmTransform.output_names
        assert "pmpm_summary" in FinancialPmpmTransform.output_names


class TestQualityMeasuresTransformMetadata:
    """Tests for QualityMeasuresTransform metadata."""


    @pytest.mark.unit
    def test_required_inputs(self):
        assert "medical_claim.parquet" in QualityMeasuresTransform.required_inputs
        assert "eligibility.parquet" in QualityMeasuresTransform.required_inputs

    @pytest.mark.unit
    def test_no_required_seeds(self):
        # Quality measures don't specify required_seeds at class level
        assert QualityMeasuresTransform.required_seeds == []


class TestHealthcareTransformContextAdditional:
    """Additional HealthcareTransformContext tests."""


    def _make_concrete(self):
        class ConcreteTransform(HealthcareTransformBase):
            transform_name = "ctx_test2"
            def execute(self, config=None):
                return {"ok": Path("/tmp/ok")}
        return ConcreteTransform

    @pytest.mark.unit
    def test_context_with_kwargs(self):
        cls = self._make_concrete()
        mock_storage = MagicMock()
        ctx = HealthcareTransformContext(cls, storage=mock_storage)
        with ctx as t:
            assert t.storage is mock_storage

    @pytest.mark.unit
    def test_exit_does_not_suppress(self):
        """__exit__ returns False so exceptions propagate."""


        cls = self._make_concrete()
        ctx = HealthcareTransformContext(cls)
        ctx.__enter__()
        result = ctx.__exit__(RuntimeError, RuntimeError("test"), None)
        assert result is False

    @pytest.mark.unit
    def test_exit_logs_error(self):
        cls = self._make_concrete()
        ctx = HealthcareTransformContext(cls)
        ctx.__enter__()
        # Passing exception info
        ctx.__exit__(ValueError, ValueError("oops"), None)
        # Just verifies no crash when exc_type is provided


class TestRunTransformAdditional:
    """Additional tests for run_transform."""


    @pytest.mark.unit
    def test_with_storage_kwarg(self):
        class StorageTransform(HealthcareTransformBase):
            transform_name = "storage_test"
            def execute(self, config=None):
                return {"path": Path("/tmp/out")}

        mock_storage = MagicMock()
        result = run_transform(StorageTransform, storage=mock_storage)
        assert "path" in result


class TestLoadParquet:
    """Tests for load_parquet method."""


    def _make_concrete(self):
        class ConcreteTransform(HealthcareTransformBase):
            transform_name = "load_test"
            def execute(self, config=None):
                return {}
        return ConcreteTransform

    @pytest.mark.unit
    def test_load_parquet_existing_file(self, tmp_path):
        """load_parquet reads an existing parquet file."""


        cls = self._make_concrete()
        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path
        t = cls(storage=mock_storage)

        # Create a parquet file
        df = pl.DataFrame({"a": [1, 2, 3]})
        df.write_parquet(tmp_path / "test.parquet")

        # load_parquet has decorators, access the underlying via __wrapped__ or .func
        result = t.load_parquet.__wrapped__(t, "test.parquet")
        assert isinstance(result, pl.LazyFrame)
        assert result.collect().height == 3


class TestCmsHccTransformExecuteMethod:
    """Tests for CmsHccTransform.execute (lines 309-365)."""


    @patch("acoharmony._transforms._base.CmsHccExpression")
    @pytest.mark.unit
    def test_execute_full_pipeline(self, mock_expr_cls, tmp_path):
        """Test CmsHccTransform.execute runs full pipeline."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        # Create required input parquet files
        medical_claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E1100"],
            "claim_end_date": ["2024-01-01"],
        })
        eligibility = pl.DataFrame({
            "person_id": ["P1"],
            "age": [70],
            "gender": ["M"],
        })
        seed1 = pl.DataFrame({"code": ["E1100"], "hcc": ["19"]})
        seed2 = pl.DataFrame({"hcc": ["19"], "factor": [0.5]})
        seed3 = pl.DataFrame({"hcc": ["19"], "parent": ["NA"]})

        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")
        eligibility.write_parquet(tmp_path / "eligibility.parquet")
        seed1.write_parquet(tmp_path / "value_sets_cms_hcc_icd_10_cm_mappings.parquet")
        seed2.write_parquet(tmp_path / "value_sets_cms_hcc_disease_factors.parquet")
        seed3.write_parquet(tmp_path / "value_sets_cms_hcc_disease_hierarchy.parquet")

        # Mock expression results
        risk_factors_lf = pl.DataFrame({"person_id": ["P1"], "hcc": ["19"]}).lazy()
        risk_scores_lf = pl.DataFrame({"person_id": ["P1"], "score": [1.5]}).lazy()
        mock_expr_cls.transform_patient_risk_factors.return_value = risk_factors_lf
        mock_expr_cls.transform_patient_risk_scores.return_value = risk_scores_lf

        t = CmsHccTransform(storage=mock_storage)
        # Use __wrapped__ to bypass decorators
        execute_fn = t.execute
        # Try accessing the underlying function
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config=None)
        assert "cms_hcc_patient_risk_factors" in result
        assert "cms_hcc_patient_risk_scores" in result

    @patch("acoharmony._transforms._base.CmsHccExpression")
    @pytest.mark.unit
    def test_execute_with_config_overrides(self, mock_expr_cls, tmp_path):
        """Test CmsHccTransform.execute with config overrides."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        for fname in [
            "medical_claim.parquet", "eligibility.parquet",
            "value_sets_cms_hcc_icd_10_cm_mappings.parquet",
            "value_sets_cms_hcc_disease_factors.parquet",
            "value_sets_cms_hcc_disease_hierarchy.parquet",
        ]:
            pl.DataFrame({"col": [1]}).write_parquet(tmp_path / fname)

        mock_expr_cls.transform_patient_risk_factors.return_value = pl.DataFrame({"x": [1]}).lazy()
        mock_expr_cls.transform_patient_risk_scores.return_value = pl.DataFrame({"y": [1]}).lazy()

        t = CmsHccTransform(storage=mock_storage)
        execute_fn = t.execute
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config={"hcc_version": "v24"})
        assert isinstance(result, dict)


class TestReadmissionsTransformExecuteMethod:
    """Tests for ReadmissionsTransform.execute (lines 411-457)."""


    @patch("acoharmony._transforms._base.ReadmissionsExpression")
    @pytest.mark.unit
    def test_execute_full_pipeline(self, mock_expr_cls, tmp_path):
        """Test ReadmissionsTransform.execute runs full pipeline."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        medical_claims = pl.DataFrame({
            "person_id": ["P1"],
            "claim_id": ["C1"],
            "claim_type": ["institutional"],
            "bill_type_code": ["111"],
            "admission_date": ["2024-01-01"],
            "discharge_date": ["2024-01-05"],
            "diagnosis_code_1": ["J18.9"],
        })
        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")

        # Seed files don't exist - load_optional_parquet handles missing
        mock_expr_cls.transform_readmission_pairs.return_value = (
            pl.DataFrame({"patient_id": ["P1"], "readmit": [False]}).lazy()
        )

        t = ReadmissionsTransform(storage=mock_storage)
        execute_fn = t.execute
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config=None)
        assert "readmissions_summary" in result


class TestChronicConditionsTransformExecuteMethod:
    """Tests for ChronicConditionsTransform.execute (lines 506-542)."""


    @patch("acoharmony._transforms._base.ChronicConditionsExpression")
    @pytest.mark.unit
    def test_execute_full_pipeline(self, mock_expr_cls, tmp_path):
        """Test ChronicConditionsTransform.execute runs full pipeline."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        medical_claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E1100"],
            "claim_end_date": ["2024-01-01"],
        })
        condition_mapping = pl.DataFrame({
            "icd_10_cm_code": ["E1100"],
            "condition": ["Diabetes"],
        })
        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")
        condition_mapping.write_parquet(
            tmp_path / "value_sets_chronic_conditions_cms_chronic_conditions_hierarchy.parquet"
        )

        mock_expr_cls.transform_patient_conditions_long.return_value = (
            pl.DataFrame({"person_id": ["P1"], "condition": ["Diabetes"]}).lazy()
        )
        mock_expr_cls.transform_patient_conditions_wide.return_value = (
            pl.DataFrame({"person_id": ["P1"], "diabetes": [True]}).lazy()
        )

        t = ChronicConditionsTransform(storage=mock_storage)
        execute_fn = t.execute
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config=None)
        assert "chronic_conditions_long" in result
        assert "chronic_conditions_wide" in result


class TestFinancialPmpmTransformExecuteMethod:
    """Tests for FinancialPmpmTransform.execute (lines 594-686)."""


    @pytest.mark.unit
    def test_execute_full_pipeline(self, tmp_path):
        """Test FinancialPmpmTransform.execute runs full pipeline."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path


        service_category = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "claim_end_date": [datetime.date(2024, 1, 15), datetime.date(2024, 2, 15)],
            "paid": [100.0, 200.0],
            "service_category_1": ["inpatient", "outpatient"],
            "service_category_2": ["medical", "surgical"],
        })
        eligibility = pl.DataFrame({
            "person_id": ["P1", "P2"],
            "enrollment_start_date": [datetime.date(2024, 1, 1), datetime.date(2024, 2, 1)],
        })

        service_category.write_parquet(tmp_path / "service_category.parquet")
        eligibility.write_parquet(tmp_path / "eligibility.parquet")

        t = FinancialPmpmTransform(storage=mock_storage)
        execute_fn = t.execute
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config=None)
        assert "financial_pmpm_by_service_category" in result
        assert "financial_pmpm_by_service_category_time" in result
        assert "financial_pmpm_summary" in result


class TestQualityMeasuresTransformExecuteMethod:
    """Tests for QualityMeasuresTransform.execute (lines 728-764)."""


    @patch("acoharmony._transforms._base.QualityMeasuresExpression")
    @pytest.mark.unit
    def test_execute_full_pipeline(self, mock_expr_cls, tmp_path):
        """Test QualityMeasuresTransform.execute runs full pipeline."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path

        medical_claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E1100"],
            "procedure_code_1": ["99213"],
            "claim_end_date": ["2024-01-01"],
        })
        eligibility = pl.DataFrame({
            "person_id": ["P1"],
        })

        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")
        eligibility.write_parquet(tmp_path / "eligibility.parquet")
        # pharmacy_claim.parquet doesn't exist - load_optional_parquet handles it

        mock_expr_cls.transform_measure_summary.return_value = (
            pl.DataFrame({"measure": ["A1C"], "rate": [0.85]}).lazy()
        )

        t = QualityMeasuresTransform(storage=mock_storage)
        execute_fn = t.execute
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config=None)
        assert "quality_measures_summary" in result

    @patch("acoharmony._transforms._base.QualityMeasuresExpression")
    @pytest.mark.unit
    def test_execute_with_pharmacy_claims(self, mock_expr_cls, tmp_path):
        """Test QualityMeasuresTransform.execute with pharmacy claims present."""


        mock_storage = MagicMock()
        mock_storage.get_path.return_value = tmp_path


        medical_claims = pl.DataFrame({
            "person_id": ["P1"],
            "diagnosis_code_1": ["E1100"],
            "procedure_code_1": ["99213"],
            "claim_end_date": [datetime.date(2024, 1, 1)],
        })
        eligibility = pl.DataFrame({"person_id": ["P1"]})
        pharmacy_claims = pl.DataFrame({
            "person_id": ["P1"],
            "claim_end_date": [datetime.date(2024, 1, 15)],
            "dispensing_date": [datetime.date(2024, 1, 15)],
        })

        medical_claims.write_parquet(tmp_path / "medical_claim.parquet")
        eligibility.write_parquet(tmp_path / "eligibility.parquet")
        pharmacy_claims.write_parquet(tmp_path / "pharmacy_claim.parquet")

        mock_expr_cls.transform_measure_summary.return_value = (
            pl.DataFrame({"measure": ["A1C"], "rate": [0.85]}).lazy()
        )

        t = QualityMeasuresTransform(storage=mock_storage)
        execute_fn = t.execute
        while hasattr(execute_fn, '__wrapped__'):
            execute_fn = execute_fn.__wrapped__
        while hasattr(execute_fn, 'func'):
            execute_fn = execute_fn.func

        result = execute_fn(t, config={"measurement_year": 2024})
        assert "quality_measures_summary" in result



# © 2025 HarmonyCares
# All rights reserved.
"""Tests for acoharmony._transforms._base module."""






class TestModuleStructure:
    """Basic module structure tests."""


    @pytest.mark.unit
    def test_module_imports(self):
        """Module can be imported."""


        assert acoharmony._transforms._base is not None
