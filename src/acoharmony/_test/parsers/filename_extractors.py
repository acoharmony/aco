# © 2025 HarmonyCares
# All rights reserved.

"""
Unit tests for the filename-extractors registry.

Covers registry mechanics (register/get/list), the three built-in
extractors (``aco_id``, ``program``, ``performance_year``) against real
CMS filename conventions, and the error surface when schemas misname
an extractor.
"""

from __future__ import annotations

import pytest

from acoharmony._parsers._filename_extractors import (
    get_filename_extractor,
    list_filename_extractors,
    register_filename_extractor,
)


class TestRegistry:
    @pytest.mark.unit
    def test_builtins_are_registered(self):
        available = list_filename_extractors()
        assert "aco_id" in available
        assert "performance_year" in available
        assert "program" in available

    @pytest.mark.unit
    def test_get_returns_callable(self):
        fn = get_filename_extractor("aco_id")
        assert callable(fn)

    @pytest.mark.unit
    def test_get_unknown_raises_with_available_list(self):
        with pytest.raises(ValueError) as exc_info:
            get_filename_extractor("does_not_exist")
        # Error message should list what IS registered so schema typos
        # surface loudly.
        assert "does_not_exist" in str(exc_info.value)
        assert "aco_id" in str(exc_info.value)

    @pytest.mark.unit
    def test_duplicate_registration_raises(self):
        @register_filename_extractor("test_duplicate_once")
        def _extractor_once(filename):
            return None

        with pytest.raises(ValueError, match="already registered"):
            @register_filename_extractor("test_duplicate_once")
            def _extractor_twice(filename):
                return None


class TestAcoIdExtractor:
    @pytest.fixture
    def extract(self):
        return get_filename_extractor("aco_id")

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "filename,expected",
        [
            # Standard BNMR filename
            ("REACH.D0259.BNMR.PY2024.D250212.T1037070.xlsx", "D0259"),
            # CCLF filename
            ("P.D0259.ACO.ZC1Y26.D260202.T1000000", "D0259"),
            # BAR filename
            ("P.D0259.ALGC24.RP.D240501.T1111111.xlsx", "D0259"),
            # MSSP-style
            ("MSSP.A2671.ACO.QALR.2024Q1.xlsx", "A2671"),
            # 5-digit ACO (rare but supported)
            ("REACH.D12345.something.xlsx", "D12345"),
            # Edge case: date comes before ACO ID (date should be ignored)
            ("D250212.REACH.D0259.xlsx", "D0259"),
        ],
    )
    def test_real_filename_patterns(self, extract, filename, expected):
        assert extract(filename) == expected

    @pytest.mark.unit
    def test_none_filename(self, extract):
        assert extract(None) is None

    @pytest.mark.unit
    def test_empty_filename(self, extract):
        assert extract("") is None

    @pytest.mark.unit
    def test_filename_with_no_aco_id(self, extract):
        # No letter+digits pattern at all
        assert extract("random.xlsx") is None

    @pytest.mark.unit
    def test_six_digit_date_is_not_captured_as_aco_id(self, extract):
        """Regression for issue #29: D250212 is a delivery date, not an ACO
        ID. The regex must cap at 5 digits to reject it."""
        # Filename that ONLY has a 6-digit D-prefix token — no valid ACO ID
        assert extract("D250212.txt") is None


class TestPerformanceYearExtractor:
    @pytest.fixture
    def extract(self):
        return get_filename_extractor("performance_year")

    @pytest.mark.unit
    @pytest.mark.parametrize(
        "filename,expected",
        [
            ("REACH.D0259.BNMR.PY2024.D250212.T1037070.xlsx", "2024"),
            ("P.D0259.ALGC24.RP.D240501.T1111111.xlsx", "2024"),
            ("P.D0259.ALGR25.RP.D250501.xlsx", "2025"),
            ("MSSP.A2671.ACO.QALR.2024Q1.xlsx", "2024"),
            ("REACH.A5678.ACO.AALR.Y2022.xlsx", "2022"),
        ],
    )
    def test_real_filename_patterns(self, extract, filename, expected):
        assert extract(filename) == expected

    @pytest.mark.unit
    def test_none_returns_none(self, extract):
        assert extract(None) is None

    @pytest.mark.unit
    def test_unrecognized_filename_returns_none(self, extract):
        assert extract("totally_unrelated.xlsx") is None


class TestProgramExtractor:
    @pytest.fixture
    def extract(self):
        return get_filename_extractor("program")

    @pytest.mark.unit
    def test_reach_detected(self, extract):
        assert extract("REACH.D0259.BNMR.PY2024.xlsx") == "REACH"

    @pytest.mark.unit
    def test_mssp_detected(self, extract):
        assert extract("MSSP.A2671.ALR.xlsx") == "MSSP"

    @pytest.mark.unit
    def test_none_returns_none(self, extract):
        assert extract(None) is None
