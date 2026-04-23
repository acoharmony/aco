# © 2025 HarmonyCares
# All rights reserved.

"""
High-needs eligibility lookback window builder.

Resolves the per-check-date lookback intervals specified by the ACO REACH
Participation Agreement, Appendix A, Tables C and D. Each criterion
evaluated at a given check date (Jan 1 / Apr 1 / Jul 1 / Oct 1 of a
Performance Year) uses a window of claims ending two months prior to the
check date. Criteria (a), (b), (c), (e) use a 12-month window (Table C);
criterion (d) uses a five-year window (Table D).

Quoting the PA:

    {table_c}

    {table_d}

This module is pure and side-effect free. The intervals it emits drive
the filter predicates in the criterion-specific expression modules
(``_high_needs_criterion_a`` through ``_high_needs_criterion_e``) and the
eligibility rollup in ``_high_needs_eligibility``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date

from acoharmony._expressions._high_needs_citations import (
    APPENDIX_A_TABLE_C_LOOKBACK_ABCE,
    APPENDIX_A_TABLE_D_LOOKBACK_D,
)

__doc__ = __doc__.format(  # type: ignore[union-attr]
    table_c=APPENDIX_A_TABLE_C_LOOKBACK_ABCE.strip(),
    table_d=APPENDIX_A_TABLE_D_LOOKBACK_D.strip(),
)


# The four check dates per Performance Year, in calendar order. Phrased as
# (month, day) so a performance year integer resolves them into real dates.
CHECK_DATE_MONTH_DAY: tuple[tuple[int, int], ...] = (
    (1, 1),
    (4, 1),
    (7, 1),
    (10, 1),
)


@dataclass(frozen=True)
class LookbackWindow:
    """An inclusive [begin, end] interval used to filter claims for one
    criterion evaluated at one check date."""

    begin: date
    end: date

    def contains(self, d: date) -> bool:
        """True iff ``d`` falls within [begin, end] inclusive."""
        return self.begin <= d <= self.end


def check_dates_for_py(performance_year: int) -> list[date]:
    """
    Real calendar dates for the four eligibility check points of a PY.

    Returns ``[Jan 1, Apr 1, Jul 1, Oct 1]`` of the given performance
    year.
    """
    return [date(performance_year, month, day) for month, day in CHECK_DATE_MONTH_DAY]


def _shift_month(anchor_year: int, anchor_month: int, delta_months: int) -> tuple[int, int]:
    """Return (year, month) resulting from shifting (anchor_year, anchor_month)
    by ``delta_months`` (may be negative). Months are 1–12."""
    zero_based = anchor_month - 1 + delta_months
    year = anchor_year + zero_based // 12
    month = (zero_based % 12) + 1
    return year, month


def _last_day_of_month(year: int, month: int) -> date:
    """Last day of the given calendar month (handles Dec rollover and Feb leap)."""
    from datetime import timedelta

    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def _window_ending_three_months_before(check: date, months_spanned: int) -> LookbackWindow:
    """
    Build a lookback window whose ``end`` is the last day of the month
    three months before ``check`` (i.e. check.month − 3) and whose
    ``begin`` is the first day of the month ``months_spanned`` months
    earlier.

    Matches Tables C and D exactly. Verified against PA rows:
        Jan 1 2026 → end = Oct 31 2025; 12-month begin = Nov 1 2024;
                                       60-month begin = Nov 1 2020.
        Apr 1 2026 → end = Jan 31 2026; 12-month begin = Feb 1 2025;
                                       60-month begin = Feb 1 2021.
    """
    end_year, end_month = _shift_month(check.year, check.month, -3)
    end = _last_day_of_month(end_year, end_month)
    begin_year, begin_month = _shift_month(end_year, end_month, -(months_spanned - 1))
    begin = date(begin_year, begin_month, 1)
    return LookbackWindow(begin=begin, end=end)


def table_c_window(performance_year: int, check_date: date) -> LookbackWindow:
    """
    Lookback window for criteria (a), (b), (c), (e): 12 months whose end
    is the last day of the month three months before the check month.
    ``check_date`` must be one of Jan 1, Apr 1, Jul 1, or Oct 1 of
    ``performance_year``.

    Raises ``ValueError`` on an off-cycle check date — the PA specifies
    these four anchor dates and nothing else.
    """
    _ensure_valid_check_date(performance_year, check_date)
    return _window_ending_three_months_before(check_date, months_spanned=12)


def table_d_window(performance_year: int, check_date: date) -> LookbackWindow:
    """
    Lookback window for criterion (d): 60 months (five years) whose end
    is the last day of the month three months before the check month.
    Same check-date guarantees as ``table_c_window``.
    """
    _ensure_valid_check_date(performance_year, check_date)
    return _window_ending_three_months_before(check_date, months_spanned=60)


def _ensure_valid_check_date(performance_year: int, check_date: date) -> None:
    if check_date not in check_dates_for_py(performance_year):
        raise ValueError(
            f"{check_date.isoformat()} is not one of the four eligibility "
            f"check dates for PY{performance_year}: "
            f"{[d.isoformat() for d in check_dates_for_py(performance_year)]}"
        )


def all_windows_for_py(performance_year: int) -> dict[date, dict[str, LookbackWindow]]:
    """
    All lookback windows for every check date in a PY. Returns a nested
    mapping ``{check_date: {"table_c": window, "table_d": window}}``.

    Useful in transforms that evaluate every criterion at every check
    date without re-invoking ``table_c_window``/``table_d_window`` per
    row.
    """
    return {
        check: {
            "table_c": table_c_window(performance_year, check),
            "table_d": table_d_window(performance_year, check),
        }
        for check in check_dates_for_py(performance_year)
    }
