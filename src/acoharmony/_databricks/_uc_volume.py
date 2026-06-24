# © 2025 HarmonyCares
# All rights reserved.

"""Copy local medallion files into configured Databricks UC volumes."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from time import perf_counter
from typing import Protocol

from .._log import LogWriter
from .._store import MedallionLayer, StorageBackend
from ._state import (
    DatabricksStateStore,
    SourceSnapshot,
    default_databricks_state_file,
    is_hidden_or_system_name,
    normalize_state_path,
    snapshot_local_path,
    snapshot_remote_path,
    snapshots_have_same_listing,
    uc_volume_copy_state_key,
)
from ._uc_tables import DatabricksCli, quote_command

MEDALLION_LAYERS = ("bronze", "silver", "gold")

logger = LogWriter("databricks.uc_volume")


@dataclass(frozen=True)
class UcVolumeCopyCommand:
    layer: str
    source: Path
    destination: str
    command: list[str]


@dataclass(frozen=True)
class LocalFileCopy:
    relative_path: str
    source: Path
    destination: str
    command: list[str]


class UcVolumeStorage(Protocol):
    def get_path(self, tier: str | MedallionLayer) -> Path | str: ...

    def get_uc_volume_path(self, tier: str | MedallionLayer) -> str: ...


def normalize_uc_path(path: str) -> str:
    """Return the Databricks CLI path for DBFS or UC volume paths."""
    if path.startswith("dbfs:/"):
        return path
    if path.startswith("/Volumes/"):
        return f"dbfs:{path}"
    return path


def base_databricks_cmd(
    *,
    databricks_bin: str,
    databricks_profile: str | None = None,
    target: str | None = None,
) -> list[str]:
    command = [databricks_bin]
    if databricks_profile:
        command.extend(["--profile", databricks_profile])
    if target:
        command.extend(["--target", target])
    return command


def selected_layers(layer: str | None) -> list[str]:
    if layer in (None, "all"):
        return list(MEDALLION_LAYERS)
    if layer not in MEDALLION_LAYERS:
        raise ValueError(f"Unknown layer: {layer}")
    return [layer]


def build_copy_commands(
    *,
    storage: UcVolumeStorage,
    layer: str | None,
    source: Path | None,
    destination: str | None,
    databricks_bin: str,
    databricks_profile: str | None,
    target: str | None,
    concurrency: int,
    overwrite: bool,
) -> list[UcVolumeCopyCommand]:
    commands: list[UcVolumeCopyCommand] = []
    for layer_name in selected_layers(layer):
        source_path = source or Path(storage.get_path(layer_name))
        destination_path = destination or storage.get_uc_volume_path(layer_name)
        command = base_databricks_cmd(
            databricks_bin=databricks_bin,
            databricks_profile=databricks_profile,
            target=target,
        ) + [
            "fs",
            "cp",
            str(source_path),
            normalize_uc_path(destination_path),
            "--recursive",
            "--concurrency",
            str(concurrency),
        ]
        if overwrite:
            command.append("--overwrite")
        commands.append(
            UcVolumeCopyCommand(
                layer=layer_name,
                source=source_path,
                destination=destination_path,
                command=command,
            )
        )
    return commands


def uc_volume_file_state_key(
    *,
    layer: str,
    source: Path,
    destination: str,
    relative_path: str,
) -> str:
    source_path = str(source.expanduser().resolve())
    destination_path = f"{destination.rstrip('/')}/{relative_path}"
    return f"uc-volume-file-copy:{layer}:{source_path}->{normalize_state_path(destination_path)}"


def iter_local_files(root: Path) -> list[tuple[str, Path, int]]:
    if root.is_file():
        return [(root.name, root, root.stat().st_size)]

    files: list[tuple[str, Path, int]] = []
    for child in root.rglob("*"):
        relative_path = child.relative_to(root)
        if any(is_hidden_or_system_name(part) for part in relative_path.parts):
            continue
        if child.is_file():
            files.append((relative_path.as_posix(), child, child.stat().st_size))
    return sorted(files)


def relative_remote_path(path: str, *, root: str, fallback_name: str) -> str:
    normalized_path = normalize_state_path(path).rstrip("/")
    normalized_root = normalize_state_path(root).rstrip("/")
    prefix = f"{normalized_root}/"
    if normalized_path.startswith(prefix):
        return normalized_path.removeprefix(prefix)
    return fallback_name.rstrip("/")


def destination_file_manifest(
    destination: str,
    *,
    databricks_cli: DatabricksCli,
) -> dict[str, int | None]:
    root = destination.rstrip("/")
    files: dict[str, int | None] = {}
    stack = [root]
    seen_dirs: set[str] = set()

    while stack:
        current = stack.pop()
        current_key = normalize_state_path(current).rstrip("/")
        if current_key in seen_dirs:
            continue
        seen_dirs.add(current_key)

        try:
            entries = databricks_cli.fs_ls(current)
        except (FileNotFoundError, RuntimeError):
            if current == root:
                return {}
            raise

        for entry in entries:
            entry_path = str(entry.path).rstrip("/")
            entry_name = str(entry.name or entry_path.rsplit("/", 1)[-1]).rstrip("/")
            if is_hidden_or_system_name(entry_name):
                continue

            if entry.is_dir:
                stack.append(entry_path)
                continue

            files[
                relative_remote_path(
                    entry_path,
                    root=root,
                    fallback_name=entry_name,
                )
            ] = entry.size

    return files


def build_file_copy_command(
    *,
    source: Path,
    destination: str,
    relative_path: str,
    databricks_bin: str,
    databricks_profile: str | None,
    target: str | None,
    overwrite: bool,
) -> LocalFileCopy:
    destination_file = f"{destination.rstrip('/')}/{relative_path}"
    command = base_databricks_cmd(
        databricks_bin=databricks_bin,
        databricks_profile=databricks_profile,
        target=target,
    ) + ["fs", "cp", str(source), normalize_uc_path(destination_file)]
    if overwrite:
        command.append("--overwrite")
    return LocalFileCopy(
        relative_path=relative_path,
        source=source,
        destination=destination_file,
        command=command,
    )


def manifests_match_source(
    local_files: list[tuple[str, Path, int]],
    destination_files: dict[str, int | None] | None,
) -> bool:
    if destination_files is None:
        return False
    local_sizes = {relative_path: size for relative_path, _, size in local_files}
    return local_sizes == destination_files


def log_writer_from_args(args: argparse.Namespace) -> LogWriter:
    return getattr(args, "log_writer", None) or logger


def log_event(args: argparse.Namespace, level: str, message: str, **kwargs) -> None:
    log_writer = log_writer_from_args(args)
    log_writer.log(
        level,
        message,
        pipeline=getattr(args, "pipeline_name", None),
        step="copy-volume",
        **kwargs,
    )


def run_command(command: list[str], *, dry_run: bool) -> None:
    print(quote_command(command))
    if dry_run:
        return
    subprocess.run(command, check=True)


def destination_volume_matches_source(
    *,
    source_snapshot: SourceSnapshot,
    destination: str,
    databricks_cli: DatabricksCli,
) -> bool:
    """Best-effort bootstrap check against files already present in a UC volume."""
    try:
        destination_snapshot = snapshot_remote_path(
            destination,
            list_entries=databricks_cli.fs_ls,
            recursive=True,
        )
    except (FileNotFoundError, RuntimeError):
        return False

    return snapshots_have_same_listing(source_snapshot, destination_snapshot)


def copy_to_uc_volumes(args: argparse.Namespace) -> int:
    """Copy local medallion files into UC volumes using storage config defaults."""
    started = perf_counter()
    storage = StorageBackend(profile=getattr(args, "aco_profile", None))
    databricks_bin = args.databricks_bin
    state_store = DatabricksStateStore(
        default_databricks_state_file(
            storage=storage,
            state_file=getattr(args, "state_file", None),
        )
    )
    force = bool(getattr(args, "force", False))
    databricks_cli = DatabricksCli(
        databricks_bin=databricks_bin,
        profile=args.profile,
        target=args.target,
        warehouse_id=None,
        wait_timeout_seconds=30,
        poll_interval_seconds=2.0,
    )

    log_event(
        args,
        "INFO",
        "Starting UC volume sync",
        action="start_step",
        aco_profile=getattr(args, "aco_profile", None),
        databricks_profile=args.profile,
        target=args.target,
        layer=args.layer,
        overwrite=args.overwrite,
        force=force,
        dry_run=args.dry_run,
        databricks_bin=databricks_bin,
        state_file=str(state_store.state_file),
    )

    if not args.dry_run and shutil.which(databricks_bin) is None:
        log_event(
            args,
            "ERROR",
            "Databricks CLI not found",
            action="validate_databricks_cli",
            databricks_bin=databricks_bin,
        )
        print(f"Databricks CLI not found: {databricks_bin}", file=sys.stderr)
        return 127

    source = Path(args.source).expanduser().resolve() if args.source else None
    commands = build_copy_commands(
        storage=storage,
        layer=args.layer,
        source=source,
        destination=args.destination,
        databricks_bin=databricks_bin,
        databricks_profile=args.profile,
        target=args.target,
        concurrency=args.concurrency,
        overwrite=args.overwrite,
    )
    log_event(
        args,
        "INFO",
        "Built UC volume copy plan",
        action="build_copy_plan",
        command_count=len(commands),
        layers=[item.layer for item in commands],
        destinations=[item.destination for item in commands],
    )

    for item in commands:
        layer_started = perf_counter()
        log_event(
            args,
            "INFO",
            "Inspecting UC volume layer",
            action="inspect_layer",
            layer=item.layer,
            source=str(item.source),
            destination=item.destination,
        )
        if not item.source.exists():
            log_event(
                args,
                "ERROR",
                "Source path does not exist for UC volume layer",
                action="validate_source",
                layer=item.layer,
                source=str(item.source),
                destination=item.destination,
            )
            print(f"Source path does not exist for {item.layer}: {item.source}", file=sys.stderr)
            return 2
        if not item.source.is_dir():
            log_event(
                args,
                "ERROR",
                "Source path is not a directory for UC volume layer",
                action="validate_source",
                layer=item.layer,
                source=str(item.source),
                destination=item.destination,
            )
            print(
                f"Source path is not a directory for {item.layer}: {item.source}", file=sys.stderr
            )
            return 2

        log_event(
            args,
            "INFO",
            "Snapshotting local UC volume source",
            action="snapshot_source",
            layer=item.layer,
            source=str(item.source),
        )
        source_snapshot = snapshot_local_path(item.source, recursive=True)
        local_files = iter_local_files(item.source)
        log_event(
            args,
            "INFO",
            "Snapshot complete for UC volume source",
            action="snapshot_source_complete",
            layer=item.layer,
            source=str(item.source),
            source_entries=source_snapshot.entry_count,
            source_total_bytes=source_snapshot.total_size,
            source_max_mtime_ns=source_snapshot.max_mtime_ns,
            local_file_count=len(local_files),
        )
        destination_files: dict[str, int | None] | None = None
        if databricks_cli.available:
            try:
                log_event(
                    args,
                    "INFO",
                    "Listing destination UC volume",
                    action="list_destination",
                    layer=item.layer,
                    destination=item.destination,
                )
                destination_files = destination_file_manifest(
                    item.destination,
                    databricks_cli=databricks_cli,
                )
                log_event(
                    args,
                    "INFO",
                    "Destination UC volume listing complete",
                    action="list_destination_complete",
                    layer=item.layer,
                    destination=item.destination,
                    destination_file_count=len(destination_files),
                )
            except (FileNotFoundError, RuntimeError) as exc:
                log_event(
                    args,
                    "WARNING",
                    "Could not list destination UC volume",
                    action="list_destination_failed",
                    layer=item.layer,
                    destination=item.destination,
                    error=str(exc),
                )
                print(
                    f"WARNING: Could not list {item.destination}; changed local files "
                    f"will still be copied: {exc}",
                    file=sys.stderr,
                )

        state_key = uc_volume_copy_state_key(
            layer=item.layer,
            source=item.source,
            destination=item.destination,
        )
        previous_snapshot = state_store.previous_snapshot(state_key)
        if (
            not force
            and previous_snapshot is not None
            and (previous_snapshot.digest == source_snapshot.digest)
            and manifests_match_source(local_files, destination_files)
        ):
            log_event(
                args,
                "INFO",
                "Skipping UC volume layer because source and destination are unchanged",
                action="skip_layer",
                reason="source_unchanged_destination_matches",
                layer=item.layer,
                source=str(item.source),
                destination=item.destination,
                source_entries=source_snapshot.entry_count,
                destination_file_count=len(destination_files or {}),
                duration_seconds=round(perf_counter() - layer_started, 3),
            )
            print(f"Skipping {item.layer}: source unchanged since last successful copy.")
            continue
        if (
            not force
            and previous_snapshot is None
            and databricks_cli.available
            and destination_volume_matches_source(
                source_snapshot=source_snapshot,
                destination=item.destination,
                databricks_cli=databricks_cli,
            )
        ):
            log_event(
                args,
                "INFO",
                "Bootstrapping UC volume layer state from matching destination",
                action="bootstrap_layer",
                reason="destination_volume_matches_source",
                layer=item.layer,
                source=str(item.source),
                destination=item.destination,
                source_entries=source_snapshot.entry_count,
                duration_seconds=round(perf_counter() - layer_started, 3),
            )
            print(
                f"Skipping {item.layer}: destination volume already matches source; "
                "seeding local Databricks state."
            )
            if not args.dry_run:
                state_store.mark_success(
                    state_key,
                    source_snapshot,
                    operation="uc-volume-copy-bootstrap",
                    metadata={
                        "layer": item.layer,
                        "source": str(item.source),
                        "destination": item.destination,
                        "bootstrap_source": "destination-volume-listing",
                    },
                )
            continue

        if not local_files:
            log_event(
                args,
                "INFO",
                "Skipping UC volume layer because source contains no files",
                action="skip_layer",
                reason="source_empty",
                layer=item.layer,
                source=str(item.source),
                destination=item.destination,
                duration_seconds=round(perf_counter() - layer_started, 3),
            )
            print(f"Skipping {item.layer}: source contains no files to copy.")
            continue

        log_event(
            args,
            "INFO",
            "Planning per-file UC volume copy work",
            action="plan_file_copies",
            layer=item.layer,
            source=str(item.source),
            destination=item.destination,
            local_file_count=len(local_files),
            has_destination_manifest=destination_files is not None,
        )
        planned_copies: list[tuple[LocalFileCopy, SourceSnapshot, str]] = []
        seeded_count = 0
        skipped_count = 0
        can_bootstrap_file_state = (
            previous_snapshot is None or previous_snapshot.digest == source_snapshot.digest
        )
        for relative_path, source_file, source_size in local_files:
            file_snapshot = snapshot_local_path(source_file, recursive=False)
            file_state_key = uc_volume_file_state_key(
                layer=item.layer,
                source=source_file,
                destination=item.destination,
                relative_path=relative_path,
            )
            destination_size = (
                destination_files.get(relative_path) if destination_files is not None else None
            )
            destination_matches = destination_files is not None and destination_size == source_size
            if not force and state_store.is_unchanged(file_state_key, file_snapshot):
                if destination_files is None or destination_matches:
                    skipped_count += 1
                    continue

            if (
                not force
                and can_bootstrap_file_state
                and state_store.previous_snapshot(file_state_key) is None
                and destination_matches
            ):
                seeded_count += 1
                if not args.dry_run:
                    state_store.mark_success(
                        file_state_key,
                        file_snapshot,
                        operation="uc-volume-file-copy-bootstrap",
                        metadata={
                            "layer": item.layer,
                            "source": str(source_file),
                            "destination": f"{item.destination.rstrip('/')}/{relative_path}",
                            "bootstrap_source": "destination-volume-listing",
                        },
                    )
                continue

            planned_copies.append(
                (
                    build_file_copy_command(
                        source=source_file,
                        destination=item.destination,
                        relative_path=relative_path,
                        databricks_bin=databricks_bin,
                        databricks_profile=args.profile,
                        target=args.target,
                        overwrite=args.overwrite,
                    ),
                    file_snapshot,
                    file_state_key,
                )
            )

        log_event(
            args,
            "INFO",
            "UC volume file copy plan complete",
            action="file_copy_plan_complete",
            layer=item.layer,
            source=str(item.source),
            destination=item.destination,
            local_file_count=len(local_files),
            files_to_copy=len(planned_copies),
            skipped_files=skipped_count,
            bootstrapped_files=seeded_count,
        )
        if planned_copies:
            if not args.skip_mkdir:
                mkdir_command = base_databricks_cmd(
                    databricks_bin=databricks_bin,
                    databricks_profile=args.profile,
                    target=args.target,
                ) + ["fs", "mkdir", normalize_uc_path(item.destination)]
                run_command(mkdir_command, dry_run=args.dry_run)

            created_parents: set[str] = {normalize_state_path(item.destination).rstrip("/")}
            for file_copy, file_snapshot, file_state_key in planned_copies:
                parent = file_copy.destination.rsplit("/", 1)[0]
                parent_key = normalize_state_path(parent).rstrip("/")
                if not args.skip_mkdir and parent_key not in created_parents:
                    parent_command = base_databricks_cmd(
                        databricks_bin=databricks_bin,
                        databricks_profile=args.profile,
                        target=args.target,
                    ) + ["fs", "mkdir", normalize_uc_path(parent)]
                    run_command(parent_command, dry_run=args.dry_run)
                    created_parents.add(parent_key)

                run_command(file_copy.command, dry_run=args.dry_run)
                if not args.dry_run:
                    state_store.mark_success(
                        file_state_key,
                        file_snapshot,
                        operation="uc-volume-file-copy",
                        metadata={
                            "layer": item.layer,
                            "source": str(file_copy.source),
                            "destination": file_copy.destination,
                            "overwrite": args.overwrite,
                        },
                    )
                log_event(
                    args,
                    "INFO",
                    "Copied UC volume file",
                    action="copy_file",
                    layer=item.layer,
                    source=str(file_copy.source),
                    destination=file_copy.destination,
                    dry_run=args.dry_run,
                )
        else:
            log_event(
                args,
                "INFO",
                "Skipping UC volume layer because files already match destination state",
                action="skip_layer",
                reason="files_match_destination_state",
                layer=item.layer,
                source=str(item.source),
                destination=item.destination,
                skipped_files=skipped_count,
                bootstrapped_files=seeded_count,
                duration_seconds=round(perf_counter() - layer_started, 3),
            )
            print(
                f"Skipping {item.layer}: {skipped_count + seeded_count} files already "
                "match Databricks volume state."
            )

        if not args.dry_run:
            destination_matches_source = manifests_match_source(local_files, destination_files)
            if destination_files is None or destination_matches_source or planned_copies:
                state_store.mark_success(
                    state_key,
                    source_snapshot,
                    operation="uc-volume-copy",
                    metadata={
                        "layer": item.layer,
                        "source": str(item.source),
                        "destination": item.destination,
                        "overwrite": args.overwrite,
                        "copied_files": len(planned_copies),
                        "skipped_files": skipped_count,
                        "bootstrapped_files": seeded_count,
                    },
                )
        log_event(
            args,
            "INFO",
            "Completed UC volume layer sync",
            action="complete_layer",
            layer=item.layer,
            source=str(item.source),
            destination=item.destination,
            source_entries=source_snapshot.entry_count,
            copied_files=len(planned_copies),
            skipped_files=skipped_count,
            bootstrapped_files=seeded_count,
            duration_seconds=round(perf_counter() - layer_started, 3),
        )

    log_event(
        args,
        "INFO",
        "Completed UC volume sync",
        action="complete_step",
        layer=args.layer,
        success=True,
        duration_seconds=round(perf_counter() - started, 3),
    )
    return 0


def cmd_copy_volume(args: argparse.Namespace) -> int:
    """Entry point used by ``aco databricks copy-volume``."""
    return copy_to_uc_volumes(args)
