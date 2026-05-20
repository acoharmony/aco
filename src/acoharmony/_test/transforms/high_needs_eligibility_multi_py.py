# © 2025 HarmonyCares
# All rights reserved.

"""Tests for _transforms.high_needs_eligibility module — multi-PY mode.

Covers the PA Section IV.B.3 sticky-alignment rule across performance
years: once a beneficiary meets any criterion in any PY, they stay
eligible forever.
"""

from __future__ import annotations

from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date
from pathlib import Path
from types import SimpleNamespace

import polars as pl
import pytest

from acoharmony._transforms.high_needs_eligibility import (
    _apply_cross_py_sticky_alignment,
    _resolve_performance_years,
)


class _MockStorage:
    def __init__(self, root: Path):
        self._root = root

    def get_path(self, tier):
        from acoharmony.medallion import MedallionLayer
        tier_name = tier.data_tier if isinstance(tier, MedallionLayer) else str(tier).lower()
        p = self._root / tier_name
        p.mkdir(parents=True, exist_ok=True)
        return p


class TestResolvePerformanceYears:
    @pytest.mark.unit
    def test_explicit_list_wins(self):
        executor = SimpleNamespace(performance_years=[2023, 2024, 2025])
        assert _resolve_performance_years(executor) == [2023, 2024, 2025]

    @pytest.mark.unit
    def test_single_py_wrapped_as_list(self):
        executor = SimpleNamespace(performance_year=2026)
        assert _resolve_performance_years(executor) == [2026]

    @pytest.mark.unit
    def test_default_begins_at_py2023(self):
        """PY2022 is flagged in the PA (line 3897) as 'not relevant to
        this Agreement'; our default historical window starts at PY2023."""
        executor = SimpleNamespace()
        years = _resolve_performance_years(executor)
        assert years[0] == 2023

    @pytest.mark.unit
    def test_explicit_list_takes_precedence_over_single(self):
        executor = SimpleNamespace(
            performance_years=[2024, 2025], performance_year=2026
        )
        assert _resolve_performance_years(executor) == [2024, 2025]


class TestCrossPyStickyAlignment:
    """PA Section IV.B.3 extends beyond a single PY. Once a beneficiary
    is aligned via any criterion at any check date in any PY, they
    remain aligned forever. The cross-PY rollup implements this via
    cum_max over (performance_year, check_date) per MBI."""

    def _build(self, rows: list[dict]) -> pl.LazyFrame:
        return pl.LazyFrame(
            rows,
            schema={
                "mbi": pl.String,
                "performance_year": pl.Int64,
                "check_date": pl.Date,
                "criteria_any_met": pl.Boolean,
            },
        )

    @pytest.mark.unit
    def test_eligible_in_py2023_persists_to_py2026(self):
        """A beneficiary who qualifies at PY2023 Apr 1 should show as
        eligible-across-pys at every subsequent check date in
        PY2024/25/26, even when their per-check criteria_any_met is False."""
        rows = []
        # PY2023: eligible at Apr 1; Jan 1 was not.
        rows.append({"mbi": "X", "performance_year": 2023,
                     "check_date": date(2023, 1, 1), "criteria_any_met": False})
        rows.append({"mbi": "X", "performance_year": 2023,
                     "check_date": date(2023, 4, 1), "criteria_any_met": True})
        # Every subsequent PY: never re-qualifies per-criterion.
        for py in (2024, 2025, 2026):
            for cd in (date(py, 1, 1), date(py, 4, 1), date(py, 7, 1), date(py, 10, 1)):
                rows.append({"mbi": "X", "performance_year": py,
                             "check_date": cd, "criteria_any_met": False})

        out = _apply_cross_py_sticky_alignment(self._build(rows)).collect()

        # Every row from PY2023 Apr 1 onward should be eligible-sticky.
        sticky = out.sort(["performance_year", "check_date"]).select(
            "performance_year", "check_date", "criteria_any_met",
            "eligible_sticky_across_pys",
        )
        # Pre-Apr-1 PY2023: not sticky-eligible yet
        assert sticky.row(0, named=True)["eligible_sticky_across_pys"] is False
        # Apr 1 PY2023 onward: sticky-eligible
        for row in sticky.rows(named=True)[1:]:
            assert row["eligible_sticky_across_pys"] is True, row

    @pytest.mark.unit
    def test_first_ever_eligible_populated(self):
        rows = [
            {"mbi": "X", "performance_year": 2024,
             "check_date": date(2024, 7, 1), "criteria_any_met": True},
            {"mbi": "X", "performance_year": 2025,
             "check_date": date(2025, 1, 1), "criteria_any_met": False},
        ]
        out = _apply_cross_py_sticky_alignment(self._build(rows)).collect()
        for row in out.rows(named=True):
            assert row["first_ever_eligible_py"] == 2024
            assert row["first_ever_eligible_check_date"] == date(2024, 7, 1)

    @pytest.mark.unit
    def test_never_eligible_leaves_firsts_null(self):
        rows = [
            {"mbi": "Z", "performance_year": 2023,
             "check_date": date(2023, 1, 1), "criteria_any_met": False},
            {"mbi": "Z", "performance_year": 2024,
             "check_date": date(2024, 1, 1), "criteria_any_met": False},
        ]
        out = _apply_cross_py_sticky_alignment(self._build(rows)).collect()
        for row in out.rows(named=True):
            assert row["eligible_sticky_across_pys"] is False
            assert row["first_ever_eligible_py"] is None
            assert row["first_ever_eligible_check_date"] is None

    @pytest.mark.unit
    def test_earliest_hit_wins_when_multiple_py_hits(self):
        """A beneficiary who qualifies in both PY2023 AND PY2025 should
        have first_ever_eligible_py=2023, not 2025."""
        rows = [
            {"mbi": "X", "performance_year": 2023,
             "check_date": date(2023, 1, 1), "criteria_any_met": True},
            {"mbi": "X", "performance_year": 2025,
             "check_date": date(2025, 7, 1), "criteria_any_met": True},
        ]
        out = _apply_cross_py_sticky_alignment(self._build(rows)).collect()
        for row in out.rows(named=True):
            assert row["first_ever_eligible_py"] == 2023
            assert row["first_ever_eligible_check_date"] == date(2023, 1, 1)

    @pytest.mark.unit
    def test_per_mbi_scope(self):
        """Two different MBIs don't share cross-PY sticky state."""
        rows = [
            {"mbi": "X", "performance_year": 2023,
             "check_date": date(2023, 1, 1), "criteria_any_met": True},
            {"mbi": "Y", "performance_year": 2023,
             "check_date": date(2023, 1, 1), "criteria_any_met": False},
        ]
        out = _apply_cross_py_sticky_alignment(self._build(rows)).collect()
        x_row = out.filter(pl.col("mbi") == "X").row(0, named=True)
        y_row = out.filter(pl.col("mbi") == "Y").row(0, named=True)
        assert x_row["eligible_sticky_across_pys"] is True
        assert y_row["eligible_sticky_across_pys"] is False
