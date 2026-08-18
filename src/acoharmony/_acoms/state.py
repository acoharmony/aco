# © 2025 HarmonyCares
# All rights reserved.

"""State tracking for ACOMS downloads."""

from __future__ import annotations

from pathlib import Path

from acoharmony._4icli.state import FileDownloadState, FourICLIStateTracker

from .._log import LogWriter
from .config import AcomsConfig


class AcomsStateTracker(FourICLIStateTracker):
    """ACOMS-specific defaults around the existing file hash tracker."""

    def __init__(
        self,
        log_writer: LogWriter | None = None,
        state_file: Path | None = None,
        search_paths: list[Path] | None = None,
    ):
        config = AcomsConfig.from_profile()
        super().__init__(
            log_writer=log_writer or LogWriter(name="acoms"),
            state_file=state_file or config.tracking_dir / "acoms_state.json",
            search_paths=search_paths
            or [
                config.bronze_dir,
                config.archive_dir,
            ],
        )


__all__ = ["AcomsStateTracker", "FileDownloadState"]
