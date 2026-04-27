# © 2025 HarmonyCares
# All rights reserved.

"""
File copy + state recording.

Copies each source file directly to its destination path. We don't
have permission to write a tempfile alongside the destination on the
target filesystem, so we forgo the temp-file + ``os.replace`` dance.
Records each successful placement in the profile's state tracker.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass
from enum import Enum
from pathlib import Path

from .profile import TransferProfile
from .selector import SelectedFile, select_files
from .state import XfrStateTracker


class FileStatus(str, Enum):
    PLACED = "placed"
    SKIPPED_DUPLICATE = "skipped_duplicate"
    DRY_RUN = "dry_run"
    ERROR = "error"


@dataclass
class TransferRecord:
    source: SelectedFile
    status: FileStatus
    dest_path: Path
    error: str | None = None


def _copy(src: Path, dest: Path) -> None:
    """Copy ``src`` → ``dest`` directly (contents only, no metadata)."""
    # copyfile (not copy2): destination filesystem may reject chmod
    # (e.g. CIFS mount we don't own), and we don't need to preserve
    # mode/times on the recipient's outbound drop.
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(src, dest)


def send_pending(
    profile: TransferProfile,
    tracker: XfrStateTracker,
    dry_run: bool = False,
) -> list[TransferRecord]:
    """
    Copy every ``pending`` candidate to the profile's destination.

    Idempotent: a file we've already placed (per state tracker) is
    skipped without recopying. A file already at the destination but
    not in our state is treated as foreign and left alone — we record
    it but don't overwrite.
    """
    selected = select_files(profile, tracker)
    records: list[TransferRecord] = []

    for choice in selected:
        if choice.state != "pending":
            continue

        dest_path = profile.destination / choice.dest_filename

        if dry_run:
            records.append(
                TransferRecord(source=choice, status=FileStatus.DRY_RUN, dest_path=dest_path)
            )
            continue

        if dest_path.exists():
            # Foreign placement (someone or something else dropped this
            # file). Don't overwrite; record what we observed so the
            # next ``status`` shows it as already-there.
            tracker.record_placement(choice.dest_filename, choice.source_path)
            records.append(
                TransferRecord(
                    source=choice,
                    status=FileStatus.SKIPPED_DUPLICATE,
                    dest_path=dest_path,
                )
            )
            continue

        try:
            _copy(choice.source_path, dest_path)
        except Exception as exc:  # ALLOWED: per-file error, surface and continue
            records.append(
                TransferRecord(
                    source=choice,
                    status=FileStatus.ERROR,
                    dest_path=dest_path,
                    error=str(exc),
                )
            )
            continue

        tracker.record_placement(choice.dest_filename, choice.source_path)
        records.append(
            TransferRecord(source=choice, status=FileStatus.PLACED, dest_path=dest_path)
        )

    return records
