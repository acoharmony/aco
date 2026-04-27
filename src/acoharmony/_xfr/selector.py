# © 2025 HarmonyCares
# All rights reserved.

"""
Selector: combines a profile's source rule with its state tracker and
verifier to label every candidate file with one of the canonical
states (``pending``, ``in_flight``, ``placed``, ``sent``, ``archived``).

The selector itself does no I/O on the destination — it asks the
verifier. That keeps the rule-engine pure and the destination logic
swappable.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from .profile import TransferProfile
from .state import XfrStateTracker


@dataclass
class SelectedFile:
    source_filename: str
    dest_filename: str
    source_path: Path
    state: str  # "pending" | "in_flight" | "placed" | "sent" | "archived"


def select_files(profile: TransferProfile, tracker: XfrStateTracker) -> list[SelectedFile]:
    """
    Walk the profile's source directories and label every candidate.

    State precedence (most-progressed wins):
      1. ``archived`` — verifier says destination tool moved it on.
      2. ``sent`` — verifier confirms destination has it.
      3. ``placed`` — destination file exists but verifier doesn't (or
         can't) confirm a hand-off. Distinct from ``in_flight`` only
         when the verifier is silent — for log-based verifiers, a
         placed-but-not-yet-uploaded file shows as ``in_flight``.
      4. ``in_flight`` — we placed it (state tracker), verifier hasn't
         confirmed the hand-off yet.
      5. ``pending`` — source rule matches, nothing has been placed yet.
    """
    candidates = profile.source_rule.applicable_filenames(list(profile.source_dirs))
    out: list[SelectedFile] = []
    for src_name in candidates:
        dest_name = profile.dest_filename(src_name)
        src_path = profile.find_source_path(src_name)
        if src_path is None:
            # Race: file disappeared between listing and lookup. Skip.
            continue

        verifier_state = profile.verifier.state_for(dest_name) if profile.verifier else None
        if verifier_state == "archived":
            state = "archived"
        elif verifier_state == "sent":
            state = "sent"
        elif tracker.has_placed(dest_name):
            # We placed it; verifier either hasn't confirmed a hand-off
            # (in_flight) or only signals destination presence (placed).
            state = "placed" if verifier_state == "placed" else "in_flight"
        else:
            state = "pending"

        out.append(
            SelectedFile(
                source_filename=src_name,
                dest_filename=dest_name,
                source_path=src_path,
                state=state,
            )
        )
    return out


def pending_only(selected: list[SelectedFile]) -> list[SelectedFile]:
    """Filter to files that still need to be copied."""
    return [s for s in selected if s.state == "pending"]
