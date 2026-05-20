# © 2025 HarmonyCares
# All rights reserved.

"""Fixtures shared across _xfr tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from acoharmony._xfr.profile import (
    DirectoryVerifier,
    LiteralPatternRule,
    TransferProfile,
    _reset_registry_for_tests,
)


@pytest.fixture(autouse=True)
def _isolated_xfr_registry():
    """Each test sees a clean profile registry."""
    _reset_registry_for_tests()
    yield
    _reset_registry_for_tests()


@pytest.fixture
def tmp_dirs(tmp_path: Path) -> dict[str, Path]:
    """Pre-built source/destination/state directories."""
    src = tmp_path / "src"
    dst = tmp_path / "dst"
    state = tmp_path / "state"
    src.mkdir()
    dst.mkdir()
    state.mkdir()
    return {"src": src, "dst": dst, "state": state, "root": tmp_path}


@pytest.fixture
def simple_profile(tmp_dirs: dict[str, Path]) -> TransferProfile:
    """Profile with literal pattern + DirectoryVerifier — no schema registry."""
    return TransferProfile(
        name="test",
        description="test profile",
        source_dirs=(tmp_dirs["src"],),
        destination=tmp_dirs["dst"],
        source_rule=LiteralPatternRule(patterns=("*.txt",), date_floor=None),
        verifier=DirectoryVerifier(destination=tmp_dirs["dst"]),
    )
