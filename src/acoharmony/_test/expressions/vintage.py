from __future__ import annotations

import pytest

# Magic auto-import: brings in ALL exports from acoharmony._expressions._vintage
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

# Now build_office_vintage_distribution_derived_metrics,
# build_vintage_distribution_derived_metrics, and ALL other exports are available


class TestVintageExpressions:

    @pytest.mark.unit
    def test_build_vintage_distribution_positive_total(self):
        exprs = build_vintage_distribution_derived_metrics(100)
        assert len(exprs) == 4

    @pytest.mark.unit
    def test_build_vintage_distribution_zero_total(self):
        exprs = build_vintage_distribution_derived_metrics(0)
        assert len(exprs) == 4

    @pytest.mark.unit
    def test_build_office_vintage_distribution_derived_metrics(self):
        exprs = build_office_vintage_distribution_derived_metrics()
        assert len(exprs) == 5
