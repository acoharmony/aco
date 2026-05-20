"""Tests for _transforms.notebook_voluntary_alignment module."""

from __future__ import annotations

# Magic auto-import: brings in ALL exports from module under test
from acoharmony._test._import_magic import auto_import


@auto_import
class _:
    pass  # noqa: E701

import pytest

import acoharmony
import acoharmony._transforms._notebook_voluntary_alignment as _notebook_voluntary_alignment
from acoharmony._transforms._notebook_voluntary_alignment import (
    calculate_voluntary_alignment_stats,
)


class TestNotebookVoluntaryAlignment:
    """Tests for notebook voluntary alignment."""

    @pytest.mark.unit
    def test_import_module(self):
        assert acoharmony._transforms._notebook_voluntary_alignment is not None

    @pytest.mark.unit
    def test_calculate_voluntary_alignment_stats_exists(self):
        assert callable(calculate_voluntary_alignment_stats)
