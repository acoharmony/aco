from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

from acoharmony._expressions._voluntary_alignment import VoluntaryAlignmentExpression


class TestVoluntaryAlignmentExpression:

    @pytest.mark.unit
    def test_class_exists(self):
        assert VoluntaryAlignmentExpression is not None
