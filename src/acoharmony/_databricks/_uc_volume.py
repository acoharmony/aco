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
from typing import Protocol

from .._store import MedallionLayer, StorageBackend
from ._uc_tables import quote_command

MEDALLION_LAYERS = ("bronze", "silver", "gold")


@dataclass(frozen=True)
class UcVolumeCopyCommand:
    layer: str
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


def run_command(command: list[str], *, dry_run: bool) -> None:
    print(quote_command(command))
    if dry_run:
        return
    subprocess.run(command, check=True)


def copy_to_uc_volumes(args: argparse.Namespace) -> int:
    """Copy local medallion files into UC volumes using storage config defaults."""
    storage = StorageBackend(profile=getattr(args, "aco_profile", None))
    databricks_bin = args.databricks_bin

    if not args.dry_run and shutil.which(databricks_bin) is None:
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

    for item in commands:
        if not item.source.exists():
            print(f"Source path does not exist for {item.layer}: {item.source}", file=sys.stderr)
            return 2
        if not item.source.is_dir():
            print(
                f"Source path is not a directory for {item.layer}: {item.source}", file=sys.stderr
            )
            return 2

        if not args.skip_mkdir:
            mkdir_command = base_databricks_cmd(
                databricks_bin=databricks_bin,
                databricks_profile=args.profile,
                target=args.target,
            ) + ["fs", "mkdir", normalize_uc_path(item.destination)]
            run_command(mkdir_command, dry_run=args.dry_run)

        run_command(item.command, dry_run=args.dry_run)

    return 0


def cmd_copy_volume(args: argparse.Namespace) -> int:
    """Entry point used by ``aco databricks copy-volume``."""
    return copy_to_uc_volumes(args)
