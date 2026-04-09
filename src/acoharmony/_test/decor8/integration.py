# © 2025 HarmonyCares
# All rights reserved.

"""
Integration tests for the complete decorator suite.

Tests expression → transform → pipeline flow, SQL generation, and composition.
"""


# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import datetime
import polars as pl
import pytest

from acoharmony._decor8.composition import composable, compose
from acoharmony._decor8.expressions import expression
from acoharmony._decor8.pipelines import pipeline
from acoharmony._decor8.transforms import transform


class TestExpressionToTransform:
    """Test expression → transform integration."""

    @pytest.mark.unit
    def test_expression_in_transform_manual(self):
        """Test manually using expression in transform."""

        @expression("status_upper")
        def status_upper() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status_upper")

        @transform("standardize")
        def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns(status_upper())

        df = pl.DataFrame({"status": ["a", "b", "c"]}).lazy()
        result = standardize(df).collect()

        assert "status_upper" in result.columns
        assert result["status_upper"].to_list() == ["A", "B", "C"]

    @pytest.mark.unit
    def test_expression_in_transform_auto_compose(self):
        """Test auto-composition of expressions in transform."""

        @expression("amount_doubled")
        def amount_doubled() -> pl.Expr:
            return (pl.col("amount") * 2).alias("amount_doubled")

        @expression("status_upper")
        def status_upper() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status_upper")

        @transform("process", expressions=[amount_doubled, status_upper])
        def process(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame({"amount": [100, 200], "status": ["a", "b"]}).lazy()
        result = process(df).collect()

        assert "amount_doubled" in result.columns
        assert "status_upper" in result.columns
        assert result["amount_doubled"].to_list() == [200, 400]

    @pytest.mark.unit
    def test_multiple_expressions_list_return(self):
        """Test expression returning list used in transform."""

        @expression("date_parts")
        def date_parts() -> list[pl.Expr]:
            return [
                pl.col("date").dt.year().alias("year"),
                pl.col("date").dt.month().alias("month"),
                pl.col("date").dt.day().alias("day"),
            ]

        @transform("extract_dates", expressions=[date_parts])
        def extract_dates(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        import datetime

        df = pl.DataFrame(
            {"date": [datetime.date(2025, 1, 15), datetime.date(2025, 2, 20)]},
            schema={"date": pl.Date},
        ).lazy()
        result = extract_dates(df).collect()

        assert all(col in result.columns for col in ["year", "month", "day"])


class TestTransformToPipeline:
    """Test transform → pipeline integration."""

    @pytest.mark.unit
    def test_transforms_in_pipeline_auto_chain(self):
        """Test auto-chaining transforms in pipeline."""

        @transform("filter_active")
        def filter_active(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        @transform("double_amount")
        def double_amount(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("amount") * 2).alias("amount"))

        @pipeline("process_pipeline", transforms=[filter_active, double_amount])
        def process_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        df = pl.DataFrame(
            {"status": ["A", "B", "A"], "amount": [100, 200, 300]}
        ).lazy()
        result = process_pipeline(df)

        assert result.success is True
        collected = result.data.collect()
        # Filtered to status A: rows 0,2 with amounts 100,300
        # Doubled: 200, 600
        assert collected["amount"].to_list() == [200, 600]

    @pytest.mark.unit
    def test_pipeline_with_additional_logic(self):
        """Test pipeline with transforms plus additional logic."""

        @transform("dedupe")
        def dedupe(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.unique(subset=["id"], maintain_order=True, keep="last")

        @transform("filter_positive")
        def filter_positive(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("amount") > 0)

        @pipeline("full_pipeline", transforms=[dedupe, filter_positive])
        def full_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            # Add final aggregation
            return df.group_by("category").agg(pl.col("amount").sum().alias("total"))

        df = pl.DataFrame(
            {
                "id": [1, 1, 2, 3],
                "category": ["A", "A", "B", "A"],
                "amount": [100, 150, -50, 200],
            }
        ).lazy()

        result = full_pipeline(df)
        assert result.success is True


class TestCompleteFlow:
    """Test complete expression → transform → pipeline flow."""

    @pytest.mark.unit
    def test_full_decorator_stack(self):
        """Test complete decorator stack with all layers."""

        # Layer 1: Expressions
        @expression("standardize_status")
        def standardize_status() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status")

        @expression("compute_total")
        def compute_total() -> pl.Expr:
            return (pl.col("qty") * pl.col("price")).alias("total")

        # Layer 2: Transforms using expressions
        @transform("standardize", expressions=[standardize_status, compute_total])
        def standardize(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @transform("filter_valid")
        def filter_valid(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("total") > 0)

        # Layer 3: Pipeline orchestrating transforms
        @pipeline("complete_pipeline", transforms=[standardize, filter_valid])
        def complete_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.group_by("category").agg(pl.col("total").sum().alias("grand_total"))

        # Execute
        df = pl.DataFrame(
            {
                "status": ["a", "b", "c"],
                "qty": [1, 2, 0],
                "price": [10.0, 20.0, 30.0],
                "category": ["X", "Y", "X"],
            }
        ).lazy()

        result = complete_pipeline(df)
        assert result.success is True
        collected = result.data.collect()

        # After standardize: total = qty*price: [10, 40, 0]
        # After filter: total > 0: [10, 40]
        # After group_by: X: 10, Y: 40
        assert len(collected) == 2


class TestCompositionIntegration:
    """Test composition with decorated functions."""

    @pytest.mark.unit
    def test_composition_of_transforms(self):
        """Test composition of transform-decorated functions."""

        @transform("add_one")
        def add_one(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @transform("multiply_two")
        def multiply_two(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [1, 2, 3]}).lazy()

        # Transforms already have __rshift__ from @transform decorator
        pipeline = add_one >> multiply_two
        result = pipeline(df).collect()

        assert result["value"].to_list() == [4, 6, 8]

    @pytest.mark.unit
    def test_composable_on_transform(self):
        """Test @composable on @transform."""

        @composable
        @transform("step1")
        def step1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("value") > 0)

        @composable
        @transform("step2")
        def step2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        df = pl.DataFrame({"value": [-5, 0, 5]}).lazy()
        pipeline = step1 >> step2
        result = pipeline(df).collect()

        assert result["value"].to_list() == [10]

    @pytest.mark.unit
    def test_compose_function_with_decorated(self):
        """Test compose() with decorated functions."""

        @transform("t1")
        def t1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("active"))  # type: ignore

        @transform("t2")
        def t2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") + 1).alias("value"))

        @transform("t3")
        def t3(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.with_columns((pl.col("value") * 2).alias("value"))

        pipeline = compose(t1, t2, t3)

        df = pl.DataFrame({"active": [True, False, True], "value": [1, 2, 3]}).lazy()
        result = pipeline(df).collect()

        # Active rows: 1, 3 with values 1, 3
        # Add 1: 2, 4
        # Multiply 2: 4, 8
        assert result["value"].to_list() == [4, 8]


class TestClaimsProcessingRealWorld:
    """Test realistic claims processing scenario with all decorators."""

    @pytest.mark.unit
    def test_complete_claims_pipeline(self):
        """Test complete claims processing with expression → transform → pipeline."""

        # Expressions
        @expression("standardize_status")
        def standardize_status() -> pl.Expr:
            return pl.col("status").str.to_uppercase().alias("status")

        @expression("compute_allowed")
        def compute_allowed() -> pl.Expr:
            return (pl.col("billed") * 0.8).alias("allowed")

        # Transforms
        @transform("standardize_claims", expressions=[standardize_status, compute_allowed])
        def standardize_claims(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @transform("dedupe_claims")
        def dedupe_claims(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.unique(subset=["claim_id"], maintain_order=True, keep="last")

        @transform("filter_approved")
        def filter_approved(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.filter(pl.col("status") == "A")

        # Pipeline
        @pipeline(
            "claims_pipeline",
            transforms=[standardize_claims, dedupe_claims, filter_approved],
            description="Complete claims processing pipeline",
        )
        def claims_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            return df.group_by("provider_id").agg(pl.col("allowed").sum().alias("total_allowed"))

        # Execute
        df = pl.DataFrame(
            {
                "claim_id": [1, 1, 2, 3, 4],
                "provider_id": [101, 101, 102, 101, 103],
                "status": ["a", "a", "d", "a", "a"],
                "billed": [100.0, 120.0, 200.0, 150.0, 180.0],
            }
        ).lazy()

        result = claims_pipeline(df)
        assert result.success is True
        collected = result.data.collect()

        # After standardize: status uppercase, allowed = billed*0.8
        # After dedupe: keep last for claim_id 1 (120), others unique
        # After filter: status A: claim_id 1 (120), 3 (150), 4 (180)
        # Allowed: 96, 120, 144
        # After group_by provider: 101: 96+120=216, 103: 144
        assert len(collected) == 2


class TestErrorHandlingAcrossLayers:
    """Test error handling across all decorator layers."""

    @pytest.mark.unit
    def test_error_in_expression_caught(self):
        """Test error in expression is caught."""

        @expression("bad_expr")
        def bad_expr() -> pl.Expr:
            return "not an expr"  # type: ignore

        with pytest.raises(TypeError):
            bad_expr()

    @pytest.mark.unit
    def test_error_in_transform_caught(self):
        """Test error in transform is caught."""

        @transform("bad_transform")
        def bad_transform(df: pl.LazyFrame) -> pl.LazyFrame:
            raise ValueError("Transform error")

        df = pl.DataFrame({"a": [1]}).lazy()
        with pytest.raises(ValueError, match=r".*"):
            bad_transform(df)

    @pytest.mark.unit
    def test_error_in_pipeline_returns_result_failure(self):
        """Test error in pipeline returns Result.failure."""

        @pipeline("bad_pipeline")
        def bad_pipeline(df: pl.LazyFrame) -> pl.LazyFrame:
            raise RuntimeError("Pipeline error")

        df = pl.DataFrame({"a": [1]}).lazy()
        result = bad_pipeline(df)

        assert result.success is False
        assert any("Pipeline error" in err for err in result.errors)


class TestMetadataPreservation:
    """Test metadata is preserved across decorator stacking."""

    @pytest.mark.unit
    def test_metadata_in_pipeline_with_transforms(self):
        """Test metadata accessible in pipeline with transforms."""

        @transform("t1", description="First transform")
        def t1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @transform("t2", description="Second transform")
        def t2(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        @pipeline("p1", transforms=[t1, t2], description="Test pipeline")
        def p1(df: pl.LazyFrame) -> pl.LazyFrame:
            return df

        assert p1._pipeline_name == "p1"
        assert p1._pipeline_description == "Test pipeline"
        assert len(p1._pipeline_transforms) == 2
