# © 2025 HarmonyCares
# All rights reserved.

"""Shared state tracking helpers for Databricks transfer workflows."""

from __future__ import annotations

import hashlib
import json
import logging
import tempfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

logger = logging.getLogger(__name__)

DEFAULT_STATE_FILENAME = "databricks_state.json"
STATE_VERSION = 1
HIDDEN_OR_SYSTEM_PREFIXES = ("_", ".")


class RemoteEntry(Protocol):
    path: str
    name: str
    is_dir: bool


@dataclass(frozen=True)
class SourceSnapshot:
    """Compact source manifest used to decide whether Databricks work is needed."""

    path: str
    kind: str
    digest: str
    fingerprint: str
    entry_count: int
    total_size: int
    max_mtime_ns: int | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "path": self.path,
            "kind": self.kind,
            "digest": self.digest,
            "fingerprint": self.fingerprint,
            "entry_count": self.entry_count,
            "total_size": self.total_size,
            "max_mtime_ns": self.max_mtime_ns,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SourceSnapshot:
        return cls(
            path=str(data.get("path", "")),
            kind=str(data.get("kind", "")),
            digest=str(data.get("digest", "")),
            fingerprint=str(data.get("fingerprint") or data.get("digest") or ""),
            entry_count=int(data.get("entry_count") or 0),
            total_size=int(data.get("total_size") or 0),
            max_mtime_ns=_optional_int(data.get("max_mtime_ns")),
        )


class DatabricksStateStore:
    """JSON-backed state store keyed by Databricks operation target."""

    def __init__(self, state_file: Path | str) -> None:
        self.state_file = Path(state_file).expanduser()
        self.state = self._load()

    def _load(self) -> dict[str, Any]:
        if not self.state_file.exists():
            return {"version": STATE_VERSION, "operations": {}}

        try:
            with open(self.state_file, encoding="utf-8") as handle:
                state = json.load(handle)
        except Exception as exc:  # noqa: BLE001 - corrupt state should not block a run.
            logger.warning("Failed to load Databricks state file %s: %s", self.state_file, exc)
            return {"version": STATE_VERSION, "operations": {}}

        if not isinstance(state, dict):
            return {"version": STATE_VERSION, "operations": {}}

        operations = state.get("operations")
        if not isinstance(operations, dict):
            state["operations"] = {}
        state.setdefault("version", STATE_VERSION)
        return state

    def save(self) -> None:
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with tempfile.NamedTemporaryFile(
            "w",
            encoding="utf-8",
            dir=self.state_file.parent,
            prefix=f".{self.state_file.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(self.state, handle, indent=2, sort_keys=True)
            handle.write("\n")
            temp_path = Path(handle.name)

        temp_path.replace(self.state_file)

    def previous_snapshot(self, key: str) -> SourceSnapshot | None:
        operation = self.state.get("operations", {}).get(key)
        if not isinstance(operation, dict):
            return None

        snapshot_data = operation.get("source_snapshot")
        if not isinstance(snapshot_data, dict):
            return None

        return SourceSnapshot.from_dict(snapshot_data)

    def is_unchanged(self, key: str, snapshot: SourceSnapshot) -> bool:
        previous = self.previous_snapshot(key)
        return previous is not None and previous.digest == snapshot.digest

    def mark_success(
        self,
        key: str,
        snapshot: SourceSnapshot,
        *,
        operation: str,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        operations = self.state.setdefault("operations", {})
        operations[key] = {
            "operation": operation,
            "source_snapshot": snapshot.to_dict(),
            "metadata": metadata or {},
            "updated_at": datetime.now(UTC).isoformat(),
        }
        self.save()


def default_databricks_state_file(
    *,
    storage: Any | None = None,
    state_file: str | Path | None = None,
) -> Path:
    """Return the default Databricks state file under the configured logs path."""
    if state_file:
        return Path(state_file).expanduser()

    logs_path: str | Path = Path("/opt/s3/data/workspace/logs")
    if storage is not None:
        try:
            logs_path = storage.get_path("logs")
        except Exception as exc:  # noqa: BLE001 - fall back to the historical logs root.
            logger.warning("Could not resolve configured logs path for Databricks state: %s", exc)

    logs_path_text = str(logs_path)
    if "://" in logs_path_text:
        logs_path = Path(tempfile.gettempdir()) / "acoharmony" / "logs"

    return Path(logs_path).expanduser() / "databricks" / DEFAULT_STATE_FILENAME


def uc_volume_copy_state_key(
    *,
    layer: str,
    source: Path,
    destination: str,
) -> str:
    source_path = str(source.expanduser().resolve())
    return f"uc-volume-copy:{layer}:{source_path}->{normalize_state_path(destination)}"


def uc_table_state_key(
    *,
    catalog: str,
    schema: str,
    table: str,
    table_mode: str,
    source_path: str,
) -> str:
    target = f"{catalog}.{schema}.{table}"
    return f"uc-table:{table_mode}:{target}:{normalize_state_path(source_path)}"


def snapshot_local_path(
    path: Path | str,
    *,
    recursive: bool = True,
    ignore_hidden: bool = True,
) -> SourceSnapshot:
    root = Path(path).expanduser()
    if not root.exists():
        raise FileNotFoundError(f"Local source path does not exist: {root}")

    if root.is_file():
        records = [_local_record(root, root, root.name)]
        kind = "file"
    else:
        iterator = root.rglob("*") if recursive else root.iterdir()
        records = []
        for child in iterator:
            if ignore_hidden and _has_hidden_part(child.relative_to(root)):
                continue
            if child.is_file():
                records.append(_local_record(child, root, _relative_posix(child, root)))
        kind = "directory"

    digest, fingerprint, total_size, max_mtime_ns = _digest_records(records)
    return SourceSnapshot(
        path=str(root),
        kind=kind,
        digest=digest,
        fingerprint=fingerprint,
        entry_count=len(records),
        total_size=total_size,
        max_mtime_ns=max_mtime_ns,
    )


def snapshot_remote_path(
    root: str,
    *,
    list_entries: Callable[[str], list[Any]],
    recursive: bool = True,
    ignore_hidden: bool = True,
) -> SourceSnapshot:
    """Snapshot a DBFS/UC path using a dbutils- or CLI-backed listing callback."""
    normalized_root = normalize_state_path(root).rstrip("/")
    records: list[tuple[str, str, int, int | None]] = []
    stack = [root.rstrip("/")]
    root_kind = "directory"
    seen_dirs: set[str] = set()

    while stack:
        current = stack.pop()
        current_key = normalize_state_path(current).rstrip("/")
        if current_key in seen_dirs:
            continue
        seen_dirs.add(current_key)

        try:
            entries = list_entries(current)
        except FileNotFoundError:
            entry = _find_remote_file_entry(current, list_entries=list_entries)
            if entry is None:
                raise
            records.append(_remote_record(entry, normalized_root))
            root_kind = "file"
            continue
        except RuntimeError:
            entry = _find_remote_file_entry(current, list_entries=list_entries)
            if entry is None:
                raise
            records.append(_remote_record(entry, normalized_root))
            root_kind = "file"
            continue

        if _entries_are_single_file(entries, current):
            records.append(_remote_record(entries[0], normalized_root))
            root_kind = "file"
            continue

        for entry in entries:
            entry_path = normalize_state_path(str(getattr(entry, "path", ""))).rstrip("/")
            entry_name = str(getattr(entry, "name", "") or entry_path.rsplit("/", 1)[-1])
            entry_name = entry_name.rstrip("/")
            if not entry_path:
                continue
            if ignore_hidden and is_hidden_or_system_name(entry_name):
                continue

            if bool(getattr(entry, "is_dir", False)):
                if recursive:
                    stack.append(str(entry.path))
                continue

            records.append(_remote_record(entry, normalized_root))

    digest, fingerprint, total_size, max_mtime_ns = _digest_records(records)
    return SourceSnapshot(
        path=normalized_root,
        kind=root_kind,
        digest=digest,
        fingerprint=fingerprint,
        entry_count=len(records),
        total_size=total_size,
        max_mtime_ns=max_mtime_ns,
    )


def normalize_state_path(path: str) -> str:
    return path.removeprefix("dbfs:")


def is_hidden_or_system_name(name: str) -> bool:
    return name.startswith(HIDDEN_OR_SYSTEM_PREFIXES)


def snapshots_have_same_listing(left: SourceSnapshot, right: SourceSnapshot) -> bool:
    """Return True when two roots have the same relative files and sizes."""
    return left.fingerprint == right.fingerprint


def _digest_records(
    records: list[tuple[str, str, int, int | None]],
) -> tuple[str, str, int, int | None]:
    digest = hashlib.sha256()
    fingerprint = hashlib.sha256()
    total_size = 0
    max_mtime_ns: int | None = None

    for relative_path, kind, size, mtime_ns in sorted(records):
        total_size += size
        if mtime_ns is not None:
            max_mtime_ns = mtime_ns if max_mtime_ns is None else max(max_mtime_ns, mtime_ns)
        digest.update(relative_path.encode("utf-8", errors="surrogateescape"))
        digest.update(b"\0")
        digest.update(kind.encode("ascii"))
        digest.update(b"\0")
        digest.update(str(size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(mtime_ns or "").encode("ascii"))
        digest.update(b"\n")

        fingerprint.update(relative_path.encode("utf-8", errors="surrogateescape"))
        fingerprint.update(b"\0")
        fingerprint.update(kind.encode("ascii"))
        fingerprint.update(b"\0")
        fingerprint.update(str(size).encode("ascii"))
        fingerprint.update(b"\n")

    return digest.hexdigest(), fingerprint.hexdigest(), total_size, max_mtime_ns


def _local_record(path: Path, root: Path, relative_path: str) -> tuple[str, str, int, int | None]:
    stat = path.stat()
    return relative_path, "file", stat.st_size, stat.st_mtime_ns


def _remote_record(entry: Any, root: str) -> tuple[str, str, int, int | None]:
    path = normalize_state_path(str(getattr(entry, "path", ""))).rstrip("/")
    relative_path = _remote_relative_path(path, root)
    size = _optional_int(
        getattr(entry, "size", None)
        or getattr(entry, "file_size", None)
        or getattr(entry, "length", None)
    )
    mtime = _optional_int(
        getattr(entry, "modification_time", None)
        or getattr(entry, "modificationTime", None)
        or getattr(entry, "mtime", None)
    )
    return relative_path, "file", size or 0, _mtime_to_ns(mtime)


def _find_remote_file_entry(
    path: str,
    *,
    list_entries: Callable[[str], list[Any]],
) -> Any | None:
    normalized_path = normalize_state_path(path).rstrip("/")
    parent, _, name = path.rstrip("/").rpartition("/")
    if not parent or not name:
        return None

    try:
        entries = list_entries(parent)
    except Exception:  # noqa: BLE001 - original listing error should surface.
        return None

    for entry in entries:
        entry_path = normalize_state_path(str(getattr(entry, "path", ""))).rstrip("/")
        entry_name = str(getattr(entry, "name", "") or entry_path.rsplit("/", 1)[-1]).rstrip("/")
        if entry_path == normalized_path or entry_name == name:
            return entry
    return None


def _entries_are_single_file(entries: list[Any], path: str) -> bool:
    if len(entries) != 1:
        return False

    entry = entries[0]
    if bool(getattr(entry, "is_dir", False)):
        return False

    entry_path = normalize_state_path(str(getattr(entry, "path", ""))).rstrip("/")
    requested_path = normalize_state_path(path).rstrip("/")
    return entry_path == requested_path


def _optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _mtime_to_ns(value: int | None) -> int | None:
    if value is None:
        return None
    if value > 10_000_000_000_000_000:
        return value
    if value > 10_000_000_000_000:
        return value * 1_000
    if value > 10_000_000_000:
        return value * 1_000_000
    return value * 1_000_000_000


def _relative_posix(path: Path, root: Path) -> str:
    return path.relative_to(root).as_posix()


def _remote_relative_path(path: str, root: str) -> str:
    root = normalize_state_path(root).rstrip("/")
    path = normalize_state_path(path).rstrip("/")
    if path == root:
        return path.rsplit("/", 1)[-1]
    prefix = f"{root}/"
    if path.startswith(prefix):
        return path[len(prefix) :]
    return path


def _has_hidden_part(path: Path) -> bool:
    return any(is_hidden_or_system_name(part) for part in path.parts)
