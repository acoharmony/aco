from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._expressions._voluntary_alignment_sva import (
    build_sva_aggregation_exprs,
    build_sva_derived_exprs,
)


class TestVoluntaryAlignmentSva:
    """Tests for _voluntary_alignment_sva expression builders."""

    @pytest.mark.unit
    def test_build_sva_aggregation_exprs(self):
        exprs = build_sva_aggregation_exprs()
        assert len(exprs) == 7

    @pytest.mark.unit
    def test_build_sva_derived_exprs(self):
        exprs = build_sva_derived_exprs()
        assert len(exprs) == 2
