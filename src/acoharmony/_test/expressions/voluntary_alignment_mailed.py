from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import polars as pl
import pytest

from acoharmony._expressions._voluntary_alignment_mailed import (
    build_mailed_aggregation_exprs,
    build_mailed_derived_exprs,
)


class TestVoluntaryAlignmentMailed:
    """Tests for _voluntary_alignment_mailed expression builders."""

    @pytest.mark.unit
    def test_build_mailed_aggregation_exprs(self):
        exprs = build_mailed_aggregation_exprs()
        assert len(exprs) == 5

    @pytest.mark.unit
    def test_build_mailed_derived_exprs(self):
        df = pl.DataFrame({'mailed_delivered': [8], 'mailed_campaigns_sent': [10]})
        result = df.select(build_mailed_derived_exprs())
        assert abs(result['mailed_delivery_rate'][0] - 80.0) < 0.01
