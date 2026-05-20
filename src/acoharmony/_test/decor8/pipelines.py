# © 2025 HarmonyCares
# All rights reserved.

"""
Tests for @pipeline decorator.

Tests multi-transform chaining, error handling, Result monad, and checkpointing.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._decor8.pipelines import pipeline
from acoharmony._decor8.transforms import transform
from acoharmony.result import Result


class TestPipelineBasics:
    """Test basic pipeline decorator functionality."""

    @pytest.mark.unit
    def test_pipeline_decorator_basic(self):
        """Test basic pipeline wrapping."""

        @pipeline("claims_processing")
        def process_claims(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        # Should have metadata
        assert hasattr(process_claims, "_pipeline_name")
        assert process_claims._pipeline_name == "claims_processing"

    @pytest.mark.unit
    def test_pipeline_with_parameters(self):
        """Test pipeline with custom parameters."""

        @pipeline(
            name="complex_pipeline",
            description="Multi-stage processing",
            sql_enabled=True,
            checkpoint_after=["dedupe", "enrich"],
        )
        def complex_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        # Check metadata
        assert complex_pipeline._pipeline_name == "complex_pipeline"
        assert complex_pipeline._pipeline_description == "Multi-stage processing"
        assert complex_pipeline._pipeline_sql_enabled is True
        assert complex_pipeline._pipeline_checkpoint_after == ["dedupe", "enrich"]

    @pytest.mark.unit
    def test_pipeline_returns_result(self):
        """Test pipeline returns Result monad."""

        @pipeline("simple_pipeline")
        def simple_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = simple_pipeline(df)
        assert isinstance(result, Result)
        assert result.success is True


class TestPipelineInputValidation:
    """Test input validation for pipelines."""

    @pytest.mark.unit
    def test_non_lazyframe_input(self):
        """Test error when input is not LazyFrame."""

        @pipeline("test_pipeline")
        @pytest.mark.unit
        def test_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        result = test_pipeline("not a dataframe")  # type: ignore
        assert result.success is False
        assert "expects pl.LazyFrame" in result.message or any(
            "expects pl.LazyFrame" in err for err in result.errors
        )

    @pytest.mark.unit
    def test_eager_dataframe_input(self):
        """Test error when passing eager DataFrame."""

        @pipeline("test_pipeline")
        @pytest.mark.unit
        def test_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]})
        result = test_pipeline(df)  # type: ignore
        assert result.success is False


class TestPipelineReturnValidation:
    """Test return type validation and wrapping."""

    @pytest.mark.unit
    def test_lazyframe_return_wrapped_in_result(self):
        """Test LazyFrame return is wrapped in Result."""

        @pipeline("return_lazyframe")
        def return_lazyframe(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = return_lazyframe(df)
        assert isinstance(result, Result)
        assert result.success is True
        assert isinstance(result.data, pl.LazyFrame)

    @pytest.mark.unit
    def test_result_return_preserved(self):
        """Test Result return is preserved (not double-wrapped)."""

        @pipeline("return_result")
        def return_result(df: pl.LazyFrame) -> pl.LazyFrame:
            # Return Result explicitly
            return Result.ok(df, "Already a Result")  # type: ignore

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = return_result(df)
        assert isinstance(result, Result)
        assert result.success is True

    @pytest.mark.unit
    def test_invalid_return_type(self):
        """Test error when returning invalid type."""

        @pipeline("invalid_return")
        def invalid_return(df: pl.LazyFrame) -> pl.LazyFrame:
            return "not a lazyframe"  # type: ignore

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = invalid_return(df)
        assert result.success is False
        assert "Invalid return type" in result.message


class TestPipelineAutoChaining:
    """Test auto-chaining of transforms."""

    @pytest.mark.unit
    def test_auto_chain_single_transform(self):
        """Test auto-chaining with single transform."""

        @transform("filter_active")
        def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        @pipeline("with_filter", transforms=[filter_active])
        def with_filter(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"status": ["A", "B", "A", "C"], "value": [1, 2, 3, 4]}).lazy()
        result = with_filter(df)
        assert result.success is True
        collected = result.data.collect()
        assert len(collected) == 2

    @pytest.mark.unit
    def test_auto_chain_multiple_transforms(self):
        """Test auto-chaining with multiple transforms."""

        @transform("add_one")
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @transform("multiply_two")
        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        @pipeline("chain_pipeline", transforms=[add_one, multiply_two])
        def chain_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = chain_pipeline(df)
        assert result.success is True
        collected = result.data.collect()
        assert collected["value"].to_list() == [4, 6, 8]  # (1+1)*2, (2+1)*2, (3+1)*2

    @pytest.mark.unit
    def test_transforms_plus_pipeline_logic(self):
        """Test transforms applied before pipeline logic."""

        @transform("double_value")
        def double_value(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        @pipeline("then_filter", transforms=[double_value])
        def then_filter(df: pl.LazyFrame) -> pl.LazyFrame:
            # Transform is already applied, now filter
            return df.filter(pl.col("value") > 5)

        df = pl.DataFrame({"value": [1, 2, 3, 4]}).lazy()
        result = then_filter(df)
        assert result.success is True
        collected = result.data.collect()
        # Values after doubling: [2, 4, 6, 8], filtered > 5: [6, 8]
        assert collected["value"].to_list() == [6, 8]


class TestPipelineErrorHandling:
    """Test error handling with Result monad."""

    @pytest.mark.unit
    def test_exception_in_pipeline_returns_failure(self):
        """Test exception in pipeline returns failure Result."""

        @pipeline("failing_pipeline")
        def failing_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            raise ValueError("Intentional error")

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = failing_pipeline(df)
        assert result.success is False
        assert any("Intentional error" in err for err in result.errors)

    @pytest.mark.unit
    def test_exception_in_transform_returns_failure(self):
        """Test exception in auto-chained transform returns failure."""

        @transform("failing_transform")
        def failing_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            raise RuntimeError("Transform failed")

        @pipeline("with_failing_transform", transforms=[failing_transform])
        def pipeline_with_fail(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = pipeline_with_fail(df)
        assert result.success is False
        assert any("Transform failed" in err for err in result.errors)

    @pytest.mark.unit
    def test_invalid_transform_return_type(self):
        """Test error when transform returns wrong type."""

        @transform("bad_transform")
        def bad_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            return "wrong type"  # type: ignore

        @pipeline("with_bad_transform", transforms=[bad_transform])
        def pipeline_with_bad(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = pipeline_with_bad(df)
        assert result.success is False


class TestPipelineCheckpointing:
    """Test checkpoint_after parameter."""

    @pytest.mark.unit
    def test_checkpoint_metadata(self):
        """Test checkpoint_after is stored in metadata."""

        @pipeline("with_checkpoints", checkpoint_after=["step1", "step3"])
        def with_checkpoints(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert with_checkpoints._pipeline_checkpoint_after == ["step1", "step3"]

    @pytest.mark.unit
    def test_checkpoint_markers_logged(self, caplog):
        """Test checkpoint markers are logged (not yet implemented)."""

        @transform("step1")
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @transform("step2")
        def step2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @pipeline(
            "with_checkpoint_logging",
            transforms=[step1, step2],
            checkpoint_after=["step1"],
        )
        def with_logging(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = with_logging(df)
        assert result.success is True
        # Checkpoint logging is documented but not yet fully implemented


class TestPipelineRealWorld:
    """Test realistic pipeline scenarios."""

    @pytest.mark.unit
    def test_claims_processing_pipeline(self):
        """Test realistic claims processing pipeline."""

        @transform("dedupe_claims")
        def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.unique(subset=["claim_id"], maintain_order=True, keep="last")

        @transform("filter_active")
        def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        @transform("standardize_amounts")
        def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(pl.col("amount").cast(pl.Float64))

        @pipeline(
            "claims_pipeline",
            transforms=[dedupe, filter_active, standardize],
            description="Complete claims processing pipeline",
        )
        def process_claims(df: pl.LazyFrame) -> pl.LazyFrame:
            # Add final aggregation
            return df.group_by("patient_id").agg(pl.col("amount").sum().alias("total"))

        df = pl.DataFrame(
            {
                "claim_id": [1, 1, 2, 3, 3],
                "patient_id": [101, 101, 102, 103, 103],
                "status": ["A", "A", "B", "A", "A"],
                "amount": [100, 150, 200, 300, 350],
            }
        ).lazy()

        result = process_claims(df)
        assert result.success is True
        collected = result.data.collect()
        # After dedupe: claim_id 1 (150), 2 (200), 3 (350)
        # After filter: status A: claim_id 1 (150), 3 (350)
        # After group_by patient: 101: 150, 103: 350
        assert len(collected) == 2

    @pytest.mark.unit
    def test_data_quality_pipeline(self):
        """Test data quality pipeline."""

        @transform("remove_nulls")
        def remove_nulls(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("value").is_not_null())

        @transform("remove_negatives")
        def remove_negatives(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("value") >= 0)

        @transform("cap_outliers")
        def cap_outliers(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(
                pl.when(pl.col("value") > 1000)
                .then(pl.lit(1000))
                .otherwise(pl.col("value"))
                .alias("value")
            )

        @pipeline("quality_pipeline", transforms=[remove_nulls, remove_negatives, cap_outliers])
        def quality_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"value": [None, -10, 50, 500, 2000]}).lazy()
        result = quality_pipeline(df)
        assert result.success is True
        collected = result.data.collect()
        # After remove_nulls: [-10, 50, 500, 2000]
        # After remove_negatives: [50, 500, 2000]
        # After cap_outliers: [50, 500, 1000]
        assert collected["value"].to_list() == [50, 500, 1000]


class TestPipelineMetadata:
    """Test metadata attachment and access."""

    @pytest.mark.unit
    def test_metadata_attributes(self):
        """Test all metadata attributes are attached."""

        @pipeline(
            name="test_pipeline",
            description="Test pipeline for validation",
            sql_enabled=False,
            checkpoint_after=["step1", "step2"],
        )
        @pytest.mark.unit
        def test_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert test_pipeline._pipeline_name == "test_pipeline"
        assert test_pipeline._pipeline_description == "Test pipeline for validation"
        assert test_pipeline._pipeline_sql_enabled is False
        assert test_pipeline._pipeline_checkpoint_after == ["step1", "step2"]

    @pytest.mark.unit
    def test_transforms_list_metadata(self):
        """Test transforms list is stored in metadata."""

        @transform("t1")
        def t1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @transform("t2")
        def t2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @pipeline("with_transforms", transforms=[t1, t2])
        def with_transforms(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert len(with_transforms._pipeline_transforms) == 2


class TestPipelineEdgeCases:
    """Test edge cases and error handling."""

    @pytest.mark.unit
    def test_empty_dataframe(self):
        """Test pipeline on empty dataframe."""

        @pipeline("process_empty")
        def process(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("doubled"))

        df = pl.DataFrame({"value": []}, schema={"value": pl.Int64}).lazy()
        result = process(df)
        assert result.success is True
        collected = result.data.collect()
        assert len(collected) == 0

    @pytest.mark.unit
    def test_pipeline_with_no_transforms(self):
        """Test pipeline with no auto-chained transforms."""

        @pipeline("passthrough")
        def passthrough(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = passthrough(df)
        assert result.success is True
        collected = result.data.collect()
        assert len(collected) == 3


class TestPipelineResultMonad:
    """Test Result monad usage patterns."""

    @pytest.mark.unit
    def test_success_result_access(self):
        """Test accessing data from successful result."""

        @pipeline("successful")
        def successful(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("incremented"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()
        result = successful(df)

        assert result.success is True
        assert result.data is not None
        collected = result.data.collect()
        assert "incremented" in collected.columns

    @pytest.mark.unit
    def test_failure_result_error(self):
        """Test accessing error from failed result."""

        @pipeline("failing")
        def failing(df: pl.LazyFrame) -> pl.LazyFrame:
            raise ValueError("Test error")

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result = failing(df)

        assert result.success is False
        assert any("Test error" in err for err in result.errors)
        assert result.data is None

    @pytest.mark.unit
    def test_result_chaining(self):
        """Test chaining operations on Result."""

        @pipeline("step1")
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"a": [1, 2, 3]}).lazy()
        result1 = step1(df)

        if result1.success:
            # Can chain another operation
            @pipeline("step2")
            def step2(df: pl.LazyFrame) -> pl.LazyFrame:
                return df

            result2 = step2(result1.data)
            assert result2.success is True


class TestPipelineDecorator:
    """Tests for the pipeline decorator."""

    @pytest.mark.unit
    def test_pipeline_basic(self):
        """Test basic pipeline decorator."""

        @pipeline("test_pipeline")
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("a") > 0)

        lf = pl.LazyFrame({"a": [1, -1, 2]})
        result = my_pipeline(lf)
        assert result.success
        assert result.data.collect().height == 2

    @pytest.mark.unit
    def test_pipeline_invalid_input(self):
        """Test pipeline with non-LazyFrame input."""

        @pipeline("bad_input")
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        result = my_pipeline("not_a_lazyframe")
        assert not result.success
        assert "expects pl.LazyFrame" in result.message

    @pytest.mark.unit
    def test_pipeline_with_transforms(self):
        """Test pipeline with auto-chained transforms."""

        def double(df):
            return df.with_columns((pl.col("a") * 2).alias("a"))

        @pipeline("with_transforms", transforms=[double])
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1, 2, 3]})
        result = my_pipeline(lf)
        assert result.success
        assert result.data.collect()["a"].to_list() == [2, 4, 6]

    @pytest.mark.unit
    def test_pipeline_transform_returns_non_lazyframe(self):
        """Test pipeline fails when transform returns wrong type."""

        def bad_transform(df):
            return "not_a_lazyframe"

        @pipeline("bad_transform", transforms=[bad_transform])
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_pipeline(lf)
        assert not result.success
        assert "instead of LazyFrame" in result.errors[0]

    @pytest.mark.unit
    def test_pipeline_transform_exception(self):
        """Test pipeline handles transform exceptions."""

        def failing_transform(df):
            raise ValueError("transform failed")

        @pipeline("failing_transform", transforms=[failing_transform])
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_pipeline(lf)
        assert not result.success
        assert "transform failed" in result.errors[0]

    @pytest.mark.unit
    def test_pipeline_returns_result(self):
        """Test pipeline when function already returns Result."""

        @pipeline("returns_result")
        def my_pipeline(df: pl.LazyFrame):
            return Result.ok(data=df, message="custom result")

        lf = pl.LazyFrame({"a": [1]})
        result = my_pipeline(lf)
        assert result.success
        assert result.message == "custom result"

    @pytest.mark.unit
    def test_pipeline_returns_invalid_type(self):
        """Test pipeline when function returns invalid type."""

        @pipeline("bad_return")
        def my_pipeline(df: pl.LazyFrame):
            return "not_valid"

        lf = pl.LazyFrame({"a": [1]})
        result = my_pipeline(lf)
        assert not result.success
        assert "Invalid return type" in result.message

    @pytest.mark.unit
    def test_pipeline_unexpected_exception(self):
        """Test pipeline handles unexpected exceptions."""

        @pipeline("failing")
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            raise RuntimeError("unexpected")

        lf = pl.LazyFrame({"a": [1]})
        result = my_pipeline(lf)
        assert not result.success
        assert "unexpected error" in result.message

    @pytest.mark.unit
    def test_pipeline_metadata(self):
        """Test pipeline metadata is attached."""

        @pipeline(
            "meta_pipeline",
            description="A test pipeline",
            sql_enabled=False,
            checkpoint_after=["step1"],
        )
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert my_pipeline._pipeline_name == "meta_pipeline"
        assert my_pipeline._pipeline_description == "A test pipeline"
        assert my_pipeline._pipeline_sql_enabled is False
        assert my_pipeline._pipeline_checkpoint_after == ["step1"]

    @pytest.mark.unit
    def test_pipeline_checkpoint_after(self):
        """Test pipeline with checkpoint_after."""

        def step1(df):
            df._transform_name = "step1"
            return df

        step1._transform_name = "step1"

        @pipeline("ckpt_pipeline", transforms=[step1], checkpoint_after=["step1"])
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        lf = pl.LazyFrame({"a": [1]})
        result = my_pipeline(lf)
        assert result.success

    @pytest.mark.unit
    def test_pipeline_description_from_docstring(self):
        """Test pipeline uses docstring when no description provided."""

        @pipeline("doc_pipeline")
        def my_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            """My pipeline docstring."""
            return df

        assert my_pipeline._pipeline_description == "My pipeline docstring."
