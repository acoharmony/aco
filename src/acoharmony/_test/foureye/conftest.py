# © 2025 HarmonyCares
# All rights reserved.

"""
Pytest configuration and fixtures for _4icli tests.
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, Mock

import pytest

if TYPE_CHECKING:
    pass


# Helper functions for creating mock objects (used across multiple test files)
def _make_config(tmp_path: Path):
    """Return a FourICLIConfig instance rooted under tmp_path."""
    from acoharmony._4icli.config import FourICLIConfig

    # Create all necessary directories
    working_dir = tmp_path / "working"
    working_dir.mkdir(parents=True, exist_ok=True)

    # Create config.txt in working directory
    config_file = working_dir / "config.txt"
    config_file.write_text("dummy_config=true\n")

    bronze_dir = tmp_path / "bronze"
    archive_dir = tmp_path / "archive"
    silver_dir = tmp_path / "silver"
    gold_dir = tmp_path / "gold"
    log_dir = tmp_path / "logs"
    tracking_dir = log_dir / "tracking"

    # Create the FourICLIConfig instance with actual Path objects
    cfg = FourICLIConfig(
        binary_path=tmp_path / "4icli",
        working_dir=working_dir,
        data_path=tmp_path,
        bronze_dir=bronze_dir,
        archive_dir=archive_dir,
        silver_dir=silver_dir,
        gold_dir=gold_dir,
        log_dir=log_dir,
        tracking_dir=tracking_dir,
        default_year=2025,
        default_apm_id="A9999",
    )
    return cfg


def _mock_log_writer():
    """Return a mock LogWriter instance."""
    lw = MagicMock()
    lw.info = MagicMock()
    lw.warning = MagicMock()
    lw.error = MagicMock()
    lw.debug = MagicMock()
    return lw


@pytest.fixture(scope="session")
def test_4icli_root() -> Path:
    """Root directory for 4icli test data."""
    return Path(__file__).parent / "data"


@pytest.fixture
def temp_bronze_dir(tmp_path: Path) -> Path:
    """Temporary bronze directory for testing."""
    bronze_dir = tmp_path / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    return bronze_dir


@pytest.fixture
def temp_working_dir(tmp_path: Path) -> Path:
    """Temporary working directory with config.txt."""
    working_dir = tmp_path / "working"
    working_dir.mkdir(parents=True, exist_ok=True)

    # Create dummy config.txt
    config_file = working_dir / "config.txt"
    config_file.write_text("dummy_config=true\n")

    return working_dir


@pytest.fixture
def tmp_bronze(tmp_path):
    """Temporary bronze directory (alias for compatibility)."""
    d = tmp_path / "bronze"
    d.mkdir()
    return d


@pytest.fixture
def tmp_working(tmp_path):
    """Temporary working directory (alias for compatibility)."""
    d = tmp_path / "working"
    d.mkdir()
    (d / "config.txt").write_text("dummy")
    return d


@pytest.fixture
def make_config(tmp_path, tmp_bronze, tmp_working):
    """Create a FourICLIConfig instance for testing."""
    from acoharmony._4icli.config import FourICLIConfig

    return FourICLIConfig(
        binary_path=tmp_path / "4icli",
        working_dir=tmp_working,
        data_path=tmp_path,
        bronze_dir=tmp_bronze,
        archive_dir=tmp_path / "archive",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        log_dir=tmp_path / "logs",
        tracking_dir=tmp_path / "tracking",
        default_year=2025,
        default_apm_id="D0259",
    )


@pytest.fixture
def mock_lw():
    """Mock LogWriter for testing."""
    lw = MagicMock()
    lw.info = MagicMock()
    lw.warning = MagicMock()
    lw.error = MagicMock()
    lw.debug = MagicMock()
    return lw


@pytest.fixture
def mock_4icli_binary(tmp_path: Path) -> Path:
    """Mock 4icli binary for testing."""
    binary_path = tmp_path / "4icli"

    # Create a simple shell script that mocks 4icli
    binary_path.write_text("""#!/bin/bash
# Mock 4icli binary for testing
echo "4icli mock execution"
exit 0
""")
    binary_path.chmod(0o755)

    return binary_path


@pytest.fixture
def mock_config(temp_bronze_dir: Path, temp_working_dir: Path, mock_4icli_binary: Path):
    """Mock FourICLIConfig for testing."""
    from acoharmony._4icli.config import FourICLIConfig

    return FourICLIConfig(
        binary_path=mock_4icli_binary,
        working_dir=temp_working_dir,
        data_path=temp_bronze_dir.parent,
        bronze_dir=temp_bronze_dir,
        archive_dir=temp_bronze_dir.parent / "archive",
        silver_dir=temp_bronze_dir.parent / "silver",
        gold_dir=temp_bronze_dir.parent / "gold",
        log_dir=temp_bronze_dir.parent / "logs",
        tracking_dir=temp_bronze_dir.parent / "tracking",
        default_year=2025,
        default_apm_id="D0259",
    )


@pytest.fixture
def mock_log_writer():
    """Mock LogWriter for testing."""
    mock = MagicMock()
    mock.info = MagicMock()
    mock.warning = MagicMock()
    mock.error = MagicMock()
    mock.debug = MagicMock()
    return mock


@pytest.fixture
def mock_state_tracker(mock_log_writer, tmp_path: Path):
    """Mock state tracker with temporary state file."""
    from acoharmony._4icli.state import FourICLIStateTracker

    state_file = tmp_path / "tracking" / "4icli_state.json"
    return FourICLIStateTracker(log_writer=mock_log_writer, state_file=state_file)


@pytest.fixture
def sample_cclf_file(temp_bronze_dir: Path) -> Path:
    """Sample CCLF file for testing."""
    file_path = temp_bronze_dir / "CCLF8.D240101.T1234567.zip"
    file_path.write_text("mock CCLF content")
    return file_path


@pytest.fixture
def sample_alignment_file(temp_bronze_dir: Path) -> Path:
    """Sample alignment file for testing."""
    file_path = temp_bronze_dir / "P.D259999.PALMR.D240101.T1234567"
    file_path.write_text("mock alignment content")
    return file_path


@pytest.fixture
def sample_download_files(temp_bronze_dir: Path) -> list[Path]:
    """Multiple sample downloaded files."""
    files = []

    # CCLF file
    cclf = temp_bronze_dir / "CCLF8.D240101.T1234567.zip"
    cclf.write_text("cclf content")
    files.append(cclf)

    # Provider alignment
    palmr = temp_bronze_dir / "P.D259999.PALMR.D240101.T1234567"
    palmr.write_text("palmr content")
    files.append(palmr)

    # Beneficiary alignment
    tparc = temp_bronze_dir / "P.D259999.TPARC.D240101.T1234567"
    tparc.write_text("tparc content")
    files.append(tparc)

    return files


@pytest.fixture
def mock_subprocess_run():
    """Mock subprocess.run for testing."""

    mock = Mock()
    mock.return_value.returncode = 0
    mock.return_value.stdout = "Success"
    mock.return_value.stderr = ""

    return mock


# Markers for 4icli tests
def pytest_configure(config):
    """Configure custom pytest markers for 4icli tests."""
    config.addinivalue_line(
        "markers", "requires_4icli_binary: marks tests that require actual 4icli binary"
    )
    config.addinivalue_line(
        "markers", "requires_credentials: marks tests that require 4icli credentials"
    )
