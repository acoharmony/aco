# © 2025 HarmonyCares
# All rights reserved.

"""Tests for acoharmony._expressions._alignment_delivery.

Covers only the NEW helpers for point-in-time member-months reconciliation:

1. ``extract_performance_year_from_filename`` — parse PY from BAR/ALR filenames.
2. ``bar_active_at_month_end_filter`` — row-level "was this bene active in
   this month?" filter using start_date / end_date semantics.

ACO ID extraction lives in ``acoharmony._parsers._aco_id`` and already has
its own tests. Silver ``file_date`` parsing lives in the existing
``_aco_temporal_bar.build_bar_file_date_expr`` / ``_aco_temporal_alr``
helpers. We reuse those, not re-test them.
"""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

from datetime import date

import polars as pl
import pytest

from acoharmony._expressions._alignment_delivery import AlignmentDeliveryExpression


class TestExtractPerformanceYearFromFilename:
    """Parses the 4-digit performance year from BAR and ALR filenames."""

    @pytest.mark.unit
    def test_bar_algc_2digit_year(self):
        """BAR filenames encode PY in the ALG[CR]<YY> token: ALGC24 → 2024."""
        df = pl.DataFrame(
            {
                "source_filename": [
                    "P.D0259.ALGC23.RP.D230125.T1747381.xlsx",
                    "P.D0259.ALGC24.RP.D240119.T1735222.xlsx",
                    "P.D0259.ALGC25.RP.D250318.T1111111.xlsx",
                    "P.D0259.ALGC26.RP.D260313.T1525036.xlsx",
                ]
            }
        )
        result = df.with_columns(
            AlignmentDeliveryExpression.extract_performance_year_from_filename()
        )
        assert result["performance_year"].to_list() == [2023, 2024, 2025, 2026]

    @pytest.mark.unit
    def test_bar_algr_year(self):
        """ALGR is the year-end roster-based variant; same 2-digit encoding."""
        df = pl.DataFrame(
            {
                "source_filename": [
                    "P.D0259.ALGR23.RP.D240228.T1035222.xlsx",
                    "P.D0259.ALGR25.RP.D260220.T0957016.xlsx",
                ]
            }
        )
        result = df.with_columns(
            AlignmentDeliveryExpression.extract_performance_year_from_filename()
        )
        assert result["performance_year"].to_list() == [2023, 2025]

    @pytest.mark.unit
    def test_alr_quarterly_encodes_4digit_year(self):
        """MSSP quarterly: QALR.<YYYY>Q<N> → YYYY."""
        df = pl.DataFrame(
            {
                "source_filename": [
                    "P.A2671.ACO.QALR.2024Q1.D249999.T0100000_1-2.csv",
                    "P.A2671.ACO.QALR.2025Q4.D259999.T0400000_1-2.csv",
                ]
            }
        )
        result = df.with_columns(
            AlignmentDeliveryExpression.extract_performance_year_from_filename()
        )
        assert result["performance_year"].to_list() == [2024, 2025]

    @pytest.mark.unit
    def test_alr_annual_encodes_4digit_year(self):
        """MSSP annual: AALR.Y<YYYY> → YYYY."""
        df = pl.DataFrame(
            {
                "source_filename": [
                    "P.A2671.ACO.AALR.Y2022.D259999.T0000000_1-2.csv",
                ]
            }
        )
        result = df.with_columns(
            AlignmentDeliveryExpression.extract_performance_year_from_filename()
        )
        assert result["performance_year"].to_list() == [2022]

    @pytest.mark.unit
    def test_unparseable_filename_is_null(self):
        """Garbage filenames produce null rather than raising."""
        df = pl.DataFrame(
            {"source_filename": ["random.csv", "", None]},
            schema={"source_filename": pl.Utf8},
        )
        result = df.with_columns(
            AlignmentDeliveryExpression.extract_performance_year_from_filename()
        )
        assert result["performance_year"].to_list() == [None, None, None]


class TestBarActiveAtMonthEndFilter:
    """Row-level filter expressing 'bene was active on this month-end date'.

    Rule: active iff ``start_date <= month_end AND (end_date IS NULL OR
    end_date > month_end)``. The ``end_date`` is treated as the first day
    of non-coverage — a bene with ``end_date = 2024-05-31`` is NOT active
    in May (they churned out ON that day).
    """

    def _frame(self) -> pl.DataFrame:
        return pl.DataFrame(
            {
                "bene_mbi": ["A", "B", "C", "D", "E"],
                "start_date": [
                    date(2024, 1, 1),   # active from Jan
                    date(2024, 3, 1),   # active from Mar
                    date(2024, 6, 1),   # starts June (not yet active in May)
                    date(2024, 1, 1),   # active Jan, churns out May 31
                    date(2024, 1, 1),   # active Jan, ends June 1
                ],
                "end_date": [
                    None,
                    None,
                    None,
                    date(2024, 5, 31),  # NOT active May
                    date(2024, 6, 1),   # IS active May
                ],
            },
            schema={
                "bene_mbi": pl.Utf8,
                "start_date": pl.Date,
                "end_date": pl.Date,
            },
        )

    @pytest.mark.unit
    def test_month_end_may_31(self):
        """A, B, E are active on 2024-05-31; C starts later; D churns out."""
        df = self._frame()
        result = df.filter(
            AlignmentDeliveryExpression.bar_active_at_month_end_filter(
                date(2024, 5, 31)
            )
        )
        assert sorted(result["bene_mbi"].to_list()) == ["A", "B", "E"]

    @pytest.mark.unit
    def test_month_end_before_anyone_started(self):
        """2023-12-31 is before all start_dates in the fixture → empty."""
        df = self._frame()
        result = df.filter(
            AlignmentDeliveryExpression.bar_active_at_month_end_filter(
                date(2023, 12, 31)
            )
        )
        assert result.height == 0

    @pytest.mark.unit
    def test_month_end_after_churn_but_open_ended_bene_stays(self):
        """By 2024-12-31, D churned (end 5/31) and E ended (6/1), A and B stay."""
        df = self._frame()
        result = df.filter(
            AlignmentDeliveryExpression.bar_active_at_month_end_filter(
                date(2024, 12, 31)
            )
        )
        assert sorted(result["bene_mbi"].to_list()) == ["A", "B", "C"]

    @pytest.mark.unit
    def test_open_ended_alignment_covers_all_future_months(self):
        """A bene with end_date=NULL is active in every month from start onward."""
        df = pl.DataFrame(
            {
                "bene_mbi": ["X"],
                "start_date": [date(2022, 1, 1)],
                "end_date": [None],
            },
            schema={
                "bene_mbi": pl.Utf8,
                "start_date": pl.Date,
                "end_date": pl.Date,
            },
        )
        for month_end in [
            date(2022, 1, 31),
            date(2023, 6, 30),
            date(2025, 12, 31),
            date(2030, 4, 30),
        ]:
            result = df.filter(
                AlignmentDeliveryExpression.bar_active_at_month_end_filter(month_end)
            )
            assert result.height == 1, f"expected X active on {month_end}"
