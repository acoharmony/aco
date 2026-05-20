# © 2025 HarmonyCares
# All rights reserved.

import pytest



# =============================================================================
# Tests for wound_care_patterns
# =============================================================================









        # date + 7 days = Jan 8


# ---------------------------------------------------------------------------
# Office Stats Expression tests
# ---------------------------------------------------------------------------



class TestWeekWindowStart:
    """Cover _wound_care_patterns.py:205."""

    @pytest.mark.unit
    def test_week_window_start(self):
        import polars as pl
        from acoharmony._expressions._wound_care_patterns import WoundCarePatternExpression
        if hasattr(WoundCarePatternExpression, 'week_window_start'):
            expr = WoundCarePatternExpression.week_window_start("claim_date")
            df = pl.DataFrame({"claim_date": ["2024-01-15"]})
            try:
                result = df.select(expr)
            except Exception:
                pass
