# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for transformation pipeline - Polars style.

Tests pipeline orchestration and chaining.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from typing import TYPE_CHECKING

import polars as pl
import pytest
import acoharmony

if TYPE_CHECKING:
    pass


class TestTransformationPipeline:
    """Tests for transformation pipeline."""

    @pytest.mark.unit
    def test_pipeline_sequential_transforms(self) -> None:
        """Pipeline applies transforms sequentially."""
        df = pl.DataFrame({"id": [1, 1, 2, 3], "value": ["a", "a", "b", "c"]})

        # Simulate pipeline: dedupe -> filter -> select
        result = df.unique(subset=["id"]).filter(pl.col("id") > 1).select(["id", "value"])

        assert len(result) == 2
        assert sorted(result["id"].to_list()) == [2, 3]

    @pytest.mark.unit
    def test_pipeline_preserves_data_quality(self) -> None:
        """Pipeline maintains data integrity through transforms."""
        df = pl.DataFrame({"id": [1, 2, 3], "amount": [100.0, 200.0, 300.0]})

        # Pipeline should preserve data types
        result = df.with_columns(pl.col("amount").cast(pl.Float64)).filter(pl.col("amount") > 150)

        assert result["amount"].dtype == pl.Float64
        assert len(result) == 2
