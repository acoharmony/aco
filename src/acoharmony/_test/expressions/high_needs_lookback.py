# © 2025 HarmonyCares
# All rights reserved.

"""Tests for expressions._high_needs_lookback module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701


from datetime import date

import pytest


# ---------------------------------------------------------------------------
# Table C: 12-month lookback for criteria (a), (b), (c), (e)
# Locked to every row of Appendix A Table C, PY2022 through PY2026.
# ---------------------------------------------------------------------------

TABLE_C_PA_ROWS = {
    # (PY, check_month_index 0..3) -> (expected_begin, expected_end)
    (2022, 0): ("2020-11-01", "2021-10-31"),
    (2022, 1): ("2021-02-01", "2022-01-31"),
    (2022, 2): ("2021-05-01", "2022-04-30"),
    (2022, 3): ("2021-08-01", "2022-07-31"),
    (2023, 0): ("2021-11-01", "2022-10-31"),
    (2023, 1): ("2022-02-01", "2023-01-31"),
    (2023, 2): ("2022-05-01", "2023-04-30"),
    (2023, 3): ("2022-08-01", "2023-07-31"),
    (2024, 0): ("2022-11-01", "2023-10-31"),
    (2024, 1): ("2023-02-01", "2024-01-31"),
    (2024, 2): ("2023-05-01", "2024-04-30"),
    (2024, 3): ("2023-08-01", "2024-07-31"),
    (2025, 0): ("2023-11-01", "2024-10-31"),
    (2025, 1): ("2024-02-01", "2025-01-31"),
    (2025, 2): ("2024-05-01", "2025-04-30"),
    (2025, 3): ("2024-08-01", "2025-07-31"),
    (2026, 0): ("2024-11-01", "2025-10-31"),
    (2026, 1): ("2025-02-01", "2026-01-31"),
    (2026, 2): ("2025-05-01", "2026-04-30"),
    (2026, 3): ("2025-08-01", "2026-07-31"),
}

TABLE_D_PA_ROWS = {
    (2022, 0): ("2016-11-01", "2021-10-31"),
    (2022, 1): ("2017-02-01", "2022-01-31"),
    (2022, 2): ("2017-05-01", "2022-04-30"),
    (2022, 3): ("2017-08-01", "2022-07-31"),
    (2023, 0): ("2017-11-01", "2022-10-31"),
    (2023, 1): ("2018-02-01", "2023-01-31"),
    (2023, 2): ("2018-05-01", "2023-04-30"),
    (2023, 3): ("2018-08-01", "2023-07-31"),
    (2024, 0): ("2018-11-01", "2023-10-31"),
    (2024, 1): ("2019-02-01", "2024-01-31"),
    (2024, 2): ("2019-05-01", "2024-04-30"),
    (2024, 3): ("2019-08-01", "2024-07-31"),
    (2025, 0): ("2019-11-01", "2024-10-31"),
    (2025, 1): ("2020-02-01", "2025-01-31"),
    (2025, 2): ("2020-05-01", "2025-04-30"),
    (2025, 3): ("2020-08-01", "2025-07-31"),
    (2026, 0): ("2020-11-01", "2025-10-31"),
    (2026, 1): ("2021-02-01", "2026-01-31"),
    (2026, 2): ("2021-05-01", "2026-04-30"),
    (2026, 3): ("2021-08-01", "2026-07-31"),
}


class TestCheckDatesForPy:
    @pytest.mark.unit
    def test_four_quarterly_anchors(self):
        assert check_dates_for_py(2026) == [
            date(2026, 1, 1),
            date(2026, 4, 1),
            date(2026, 7, 1),
            date(2026, 10, 1),
        ]

    @pytest.mark.unit
    def test_handles_leap_year_pys(self):
        """PY divisible by 4 must still produce Jan 1 and not crash on Feb."""
        dates = check_dates_for_py(2024)
        assert dates[0] == date(2024, 1, 1)


class TestTableCWindow:
    """Every row of Appendix A Table C must match the PA verbatim."""

    @pytest.mark.parametrize("py,idx", list(TABLE_C_PA_ROWS.keys()))
    @pytest.mark.unit
    def test_matches_pa_row(self, py, idx):
        check = check_dates_for_py(py)[idx]
        w = table_c_window(py, check)
        b_expected, e_expected = TABLE_C_PA_ROWS[(py, idx)]
        assert w.begin.isoformat() == b_expected
        assert w.end.isoformat() == e_expected

    @pytest.mark.unit
    def test_rejects_off_cycle_check_date(self):
        with pytest.raises(ValueError, match="not one of the four"):
            table_c_window(2026, date(2026, 2, 1))

    @pytest.mark.unit
    def test_window_contains_predicate(self):
        w = table_c_window(2026, date(2026, 1, 1))
        assert w.contains(date(2024, 11, 1))
        assert w.contains(date(2025, 10, 31))
        assert not w.contains(date(2024, 10, 31))
        assert not w.contains(date(2025, 11, 1))


class TestTableDWindow:
    """Every row of Appendix A Table D must match the PA verbatim."""

    @pytest.mark.parametrize("py,idx", list(TABLE_D_PA_ROWS.keys()))
    @pytest.mark.unit
    def test_matches_pa_row(self, py, idx):
        check = check_dates_for_py(py)[idx]
        w = table_d_window(py, check)
        b_expected, e_expected = TABLE_D_PA_ROWS[(py, idx)]
        assert w.begin.isoformat() == b_expected
        assert w.end.isoformat() == e_expected

    @pytest.mark.unit
    def test_window_spans_sixty_months(self):
        """5 years × 12 months = 60 months inclusive (first-of-begin to
        last-of-end spans exactly 60 month-starts)."""
        w = table_d_window(2026, date(2026, 1, 1))
        # begin 2020-11-01 → end 2025-10-31 is exactly 60 full months.
        assert w.begin == date(2020, 11, 1)
        assert w.end == date(2025, 10, 31)

    @pytest.mark.unit
    def test_rejects_off_cycle_check_date(self):
        with pytest.raises(ValueError):
            table_d_window(2026, date(2026, 6, 15))


class TestAllWindowsForPy:
    @pytest.mark.unit
    def test_returns_both_tables_for_each_check_date(self):
        all_ = all_windows_for_py(2026)
        assert list(all_) == check_dates_for_py(2026)
        for _, windows in all_.items():
            assert set(windows) == {"table_c", "table_d"}

    @pytest.mark.unit
    def test_windows_match_individual_builders(self):
        all_ = all_windows_for_py(2024)
        for cd, windows in all_.items():
            assert windows["table_c"] == table_c_window(2024, cd)
            assert windows["table_d"] == table_d_window(2024, cd)


class TestLookbackWindow:
    @pytest.mark.unit
    def test_frozen_dataclass(self):
        w = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))
        with pytest.raises((AttributeError, TypeError)):
            w.begin = date(2024, 2, 1)  # type: ignore[misc]

    @pytest.mark.unit
    def test_contains_end_inclusive(self):
        w = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))
        assert w.contains(date(2024, 12, 31))

    @pytest.mark.unit
    def test_contains_begin_inclusive(self):
        w = LookbackWindow(begin=date(2024, 1, 1), end=date(2024, 12, 31))
        assert w.contains(date(2024, 1, 1))
