# © 2025 HarmonyCares
# All rights reserved.

"""CLI commands for the xfr (transfer) module."""

from __future__ import annotations

import argparse
from collections import Counter
from pathlib import Path

from .._log import LogWriter
from .._store import StorageBackend
from .profile import list_profiles, resolve_profile
from .selector import select_files
from .state import XfrStateTracker
from .transfer import FileStatus, send_pending


def _tracker(profile_name: str) -> XfrStateTracker:
    storage = StorageBackend()
    tracking_dir = Path(storage.get_path("logs")) / "tracking"
    return XfrStateTracker(profile_name=profile_name, state_dir=tracking_dir)


def cmd_xfr_list(args: argparse.Namespace) -> int:
    """Print available transfer profiles."""
    profiles = list_profiles()
    if not profiles:
        print("No xfr profiles registered.")
        return 0
    print(f"{'NAME':<16} DESCRIPTION")
    print("-" * 80)
    for p in profiles:
        print(f"{p.name:<16} {p.description}")
    return 0


def cmd_xfr_status(args: argparse.Namespace) -> int:
    """Summarize file states for a profile."""
    profile = resolve_profile(args.profile)
    tracker = _tracker(profile.name)
    selected = select_files(profile, tracker)

    counts = Counter(s.state for s in selected)
    print(f"Profile:     {profile.name}")
    print(f"Sources:     {', '.join(str(p) for p in profile.source_dirs)}")
    print(f"Destination: {profile.destination}")
    print(f"Total in scope: {len(selected)}")
    print()
    for state in ("pending", "in_flight", "placed", "sent", "archived"):
        if counts.get(state):
            print(f"  {state:<10} {counts[state]:>5}")
    print()

    limit = args.limit if hasattr(args, "limit") and args.limit else 20
    if args.show == "all":
        rows = selected
    else:
        rows = [s for s in selected if s.state == args.show]
    if not rows:
        return 0
    print(f"Files ({args.show}, showing {min(limit, len(rows))} of {len(rows)}):")
    print("-" * 80)
    for entry in rows[:limit]:
        print(f"  [{entry.state:<9}] {entry.source_filename}")
        if entry.dest_filename != entry.source_filename:
            print(f"               -> {entry.dest_filename}")
    return 0


def cmd_xfr_send(args: argparse.Namespace) -> int:
    """Copy pending files to the profile's destination."""
    profile = resolve_profile(args.profile)
    tracker = _tracker(profile.name)
    log_writer = LogWriter(name="xfr")
    log_writer.info(
        "xfr send",
        profile=profile.name,
        sources=[str(p) for p in profile.source_dirs],
        destination=str(profile.destination),
        dry_run=bool(args.dry_run),
    )
    records = send_pending(profile, tracker, dry_run=bool(args.dry_run))

    summary = Counter(r.status.value for r in records)
    if not records:
        print("No pending files to transfer.")
        return 0

    print(f"Profile:     {profile.name}")
    print(f"Destination: {profile.destination}")
    if args.dry_run:
        print("DRY RUN — no files copied.")
    print()
    for status in ("placed", "skipped_duplicate", "dry_run", "error"):
        if summary.get(status):
            print(f"  {status:<20} {summary[status]:>5}")
    print()

    for r in records:
        prefix = {
            FileStatus.PLACED: "[OK]   ",
            FileStatus.DRY_RUN: "[DRY]  ",
            FileStatus.SKIPPED_DUPLICATE: "[SKIP] ",
            FileStatus.ERROR: "[ERR]  ",
        }[r.status]
        print(f"{prefix}{r.source.source_filename} -> {r.dest_path}")
        if r.error:
            print(f"        {r.error}")

    return 1 if summary.get("error") else 0


def add_subparsers(parent: argparse._SubParsersAction) -> None:
    """Wire up `aco xfr ...` subcommands. Called from acoharmony.cli."""
    xfr_parser = parent.add_parser("xfr", help="Transfer files between locations")
    sub = xfr_parser.add_subparsers(dest="xfr_command", help="xfr commands")

    sub.add_parser("list", help="List registered transfer profiles")

    status = sub.add_parser("status", help="Show file states for a profile")
    status.add_argument("profile", help="Profile name (see `aco xfr list`)")
    status.add_argument(
        "--show",
        default="pending",
        choices=["pending", "in_flight", "placed", "sent", "archived", "all"],
        help="Which state to list (default: pending)",
    )
    status.add_argument("--limit", type=int, default=20, help="Max rows to print")

    send = sub.add_parser("send", help="Copy pending files to the profile's destination")
    send.add_argument("profile", help="Profile name (see `aco xfr list`)")
    send.add_argument("--dry-run", action="store_true", help="Show what would be copied")

    xfr_parser.set_defaults(_xfr_parser=xfr_parser)


def dispatch(args: argparse.Namespace) -> int:
    """Resolve the chosen xfr subcommand. Called from acoharmony.cli."""
    cmd = getattr(args, "xfr_command", None)
    if cmd == "list":
        return cmd_xfr_list(args)
    if cmd == "status":
        return cmd_xfr_status(args)
    if cmd == "send":
        return cmd_xfr_send(args)
    parser = getattr(args, "_xfr_parser", None)
    if parser is not None:
        parser.print_help()
    return 1
