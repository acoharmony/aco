from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._expressions._voluntary_alignment_pbvar import build_pbvar_aggregation_exprs


class TestVoluntaryAlignmentPbvar:
    """Tests for _voluntary_alignment_pbvar expression builders."""

    @pytest.mark.unit
    def test_build_pbvar_aggregation_exprs(self):
        exprs = build_pbvar_aggregation_exprs()
        assert len(exprs) == 4
