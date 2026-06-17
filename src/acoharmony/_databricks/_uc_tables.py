#!/usr/bin/env python3
"""Create Unity Catalog Parquet tables for ACOHarmony UC volume files.

The default roots come from :class:`acoharmony._store.StorageBackend`, so the
same behavior is available through the ``aco databricks create-tables`` command
without maintaining a separate script.

Examples:
    aco databricks create-tables --dry-run
    aco databricks create-tables
    aco databricks create-tables --source-volume uat_sandbox.gov_programs.gold
    aco databricks create-tables --warehouse-id abc123
"""

from __future__ import annotations

import argparse
import hashlib
import importlib
import json
import re
import shutil
import subprocess
import sys
import tempfile
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from .._store import StorageBackend

DEFAULT_CATALOG = "uat_sandbox"
DEFAULT_SCHEMA = "gov_programs"
DEFAULT_LAYERS = ("bronze", "silver", "gold")

HIDDEN_OR_SYSTEM_PREFIXES = ("_", ".")
PARQUET_SUFFIX = ".parquet"
UC_MAX_COLUMN_NAME_LENGTH = 255
COLUMN_ALIAS_HASH_LENGTH = 12
DELTA_INVALID_COLUMN_NAME_CHARS = set(" ,;{}()\n\t=")
READ_FILES_SOURCE_READER = "read_files"
PARQUET_SOURCE_READER = "parquet"


@dataclass(frozen=True)
class VolumeSpec:
    catalog: str
    schema: str
    volume: str

    @property
    def root_path(self) -> str:
        return f"/Volumes/{self.catalog}/{self.schema}/{self.volume}"


@dataclass(frozen=True)
class VolumePath:
    catalog: str
    schema: str
    volume: str
    relative_path: str

    @property
    def full_name(self) -> str:
        return f"{self.catalog}.{self.schema}.{self.volume}"


@dataclass(frozen=True)
class TableRegistration:
    table_name: str
    source_path: str

    @property
    def source_location(self) -> str:
        """Return the canonical SQL location for UC volume paths."""
        if self.source_path.startswith("dbfs:/Volumes/"):
            return self.source_path.removeprefix("dbfs:")
        return self.source_path


@dataclass(frozen=True)
class FsEntry:
    path: str
    name: str
    is_dir: bool


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Create one Unity Catalog table for every parquet file found in "
            "the configured UC volume roots."
        )
    )
    parser.add_argument(
        "--catalog",
        default=None,
        help=(
            f"Target Unity Catalog catalog. Default: storage uc_catalog, {DEFAULT_CATALOG}, "
            "or the source volume catalog when --source-volume is supplied."
        ),
    )
    parser.add_argument(
        "--schema",
        default=None,
        help=(
            f"Target Unity Catalog schema. Default: storage uc_schema, {DEFAULT_SCHEMA}, "
            "or the source volume schema when --source-volume is supplied."
        ),
    )
    parser.add_argument(
        "--aco-profile",
        default=None,
        help="ACO storage profile used for default UC roots. Default: ACO_PROFILE/local.",
    )
    parser.add_argument(
        "--layer",
        action="append",
        choices=DEFAULT_LAYERS,
        dest="layers",
        help="Medallion layer to scan from storage config. May be passed more than once.",
    )
    parser.add_argument(
        "--source-volume",
        "--volume",
        action="append",
        dest="volume_fqns",
        help=(
            "UC volume FQN to scan, e.g. uat_sandbox.gov_programs.gold. "
            "May be passed more than once."
        ),
    )
    parser.add_argument(
        "--volume-root",
        action="append",
        dest="volume_roots",
        help=(
            "UC volume path to scan. May be passed more than once. "
            "Default: storage-configured bronze/silver/gold UC roots."
        ),
    )
    parser.add_argument(
        "--table-prefix",
        default="",
        help="Optional prefix to prepend to every generated table name.",
    )
    parser.add_argument(
        "--include-volume-prefix",
        action="store_true",
        help=("Prefix every table name with the UC volume name, e.g. gold_eligibility."),
    )
    parser.add_argument(
        "--duplicate-strategy",
        choices=("prefix-volume", "error"),
        default="prefix-volume",
        help=(
            "How to handle duplicate parquet stems across volumes. Default: "
            "prefix-volume, which keeps unique stems unchanged and prefixes only "
            "duplicates with the volume/layer name."
        ),
    )
    parser.add_argument(
        "--table-mode",
        choices=("managed-delta", "external-parquet"),
        default="managed-delta",
        help=(
            "How tables are created. Default: managed-delta, which creates "
            "managed Delta tables with CTAS from read_files(..., format => 'parquet') "
            "and Delta column mapping enabled so original parquet column names are "
            "preserved. external-parquet emits USING PARQUET LOCATION and is not "
            "valid for managed UC volume storage."
        ),
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Drop each target table before creating it so metadata is refreshed.",
    )
    parser.add_argument(
        "--no-recurse",
        action="store_true",
        help="Only scan parquet files directly under each volume root.",
    )
    parser.add_argument(
        "--skip-missing-roots",
        action="store_true",
        help="Warn and continue when a configured volume root does not exist.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print the SQL that would run without creating tables.",
    )
    parser.add_argument(
        "--warehouse-id",
        help=(
            "Databricks SQL warehouse ID for local execution through the "
            "Databricks CLI. Not needed when running on a Databricks cluster."
        ),
    )
    parser.add_argument(
        "--profile",
        help="Optional ~/.databrickscfg profile to pass to databricks CLI.",
    )
    parser.add_argument(
        "--target",
        help="Optional bundle target to pass to databricks CLI.",
    )
    parser.add_argument(
        "--databricks-bin",
        default="databricks",
        help="Databricks CLI executable name or path. Default: databricks",
    )
    parser.add_argument(
        "--wait-timeout-seconds",
        type=int,
        default=30,
        help="Initial SQL statement wait timeout for Databricks CLI execution. Default: 30",
    )
    parser.add_argument(
        "--poll-interval-seconds",
        type=float,
        default=2.0,
        help="Polling interval while waiting for Databricks SQL statements. Default: 2.0",
    )
    return parser.parse_args(argv)


def parse_volume_fqn(value: str) -> VolumeSpec:
    parts = [part.strip("` ") for part in value.split(".")]
    if len(parts) != 3 or any(not part for part in parts):
        raise ValueError(
            f"Volume FQN must have exactly three parts: catalog.schema.volume (got {value!r})"
        )
    return VolumeSpec(catalog=parts[0], schema=parts[1], volume=parts[2])


class DatabricksCli:
    def __init__(
        self,
        *,
        databricks_bin: str,
        profile: str | None,
        target: str | None,
        warehouse_id: str | None,
        wait_timeout_seconds: int,
        poll_interval_seconds: float,
    ) -> None:
        self.databricks_bin = databricks_bin
        self.profile = profile
        self.target = target
        self.warehouse_id = warehouse_id
        self.wait_timeout_seconds = wait_timeout_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self._volume_storage_locations: dict[str, str] = {}

    @property
    def available(self) -> bool:
        return shutil.which(self.databricks_bin) is not None

    @property
    def base_command(self) -> list[str]:
        command = [self.databricks_bin]
        if self.profile:
            command.extend(["--profile", self.profile])
        if self.target:
            command.extend(["--target", self.target])
        return command

    def run(self, command: list[str], *, json_output: bool) -> Any:
        full_command = self.base_command + command
        if json_output:
            full_command.extend(["-o", "json"])

        result = subprocess.run(
            full_command,
            check=False,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            message = result.stderr.strip() or result.stdout.strip()
            raise RuntimeError(f"{quote_command(full_command)} failed: {message}")

        if not json_output:
            return result.stdout

        output = result.stdout.strip()
        if not output:
            return None

        try:
            return json.loads(output)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"{quote_command(full_command)} did not return valid JSON: {output}"
            ) from exc

    def api_post(self, path: str, payload: dict[str, Any]) -> dict[str, Any]:
        with tempfile.NamedTemporaryFile("w", suffix=".json", delete=False) as payload_file:
            json.dump(payload, payload_file)
            payload_path = payload_file.name

        try:
            data = self.run(
                ["api", "post", path, "--json", f"@{payload_path}"],
                json_output=True,
            )
        finally:
            Path(payload_path).unlink(missing_ok=True)

        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Databricks API response: {data!r}")
        return data

    def api_get(self, path: str) -> dict[str, Any]:
        data = self.run(["api", "get", path], json_output=True)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Databricks API response: {data!r}")
        return data

    def fs_ls(self, path: str) -> list[FsEntry]:
        data = self.run(
            ["fs", "ls", normalize_cli_fs_path(path), "--absolute", "--long"],
            json_output=True,
        )
        return parse_cli_fs_entries(data)

    def volume_storage_location(self, volume_path: VolumePath) -> str:
        cached_location = self._volume_storage_locations.get(volume_path.full_name)
        if cached_location:
            return cached_location

        data = self.run(["volumes", "read", volume_path.full_name], json_output=True)
        if not isinstance(data, dict):
            raise RuntimeError(f"Unexpected Databricks volume response: {data!r}")

        storage_location = data.get("storage_location")
        if not isinstance(storage_location, str) or not storage_location:
            raise RuntimeError(
                f"Volume {volume_path.full_name} does not expose a storage_location."
            )

        self._volume_storage_locations[volume_path.full_name] = storage_location
        return storage_location

    def submit_sql(self, sql: str) -> dict[str, Any]:
        if not self.warehouse_id:
            raise RuntimeError(
                "No Spark session is available locally. Pass --warehouse-id to execute "
                "through Databricks SQL, add --dry-run to print SQL only, or run this "
                "script on a Databricks cluster."
            )

        payload = {
            "statement": sql,
            "warehouse_id": self.warehouse_id,
            "wait_timeout": f"{self.wait_timeout_seconds}s",
            "on_wait_timeout": "CONTINUE",
        }
        return self.api_post("/api/2.0/sql/statements", payload)

    def execute_sql(self, sql: str) -> None:
        response = self.submit_sql(sql)
        self.wait_for_statement(response)

    def execute_query(self, sql: str) -> list[list[Any]]:
        response = self.submit_sql(sql)
        response = self.wait_for_statement(response)
        return self.statement_rows(response)

    def wait_for_statement(self, response: dict[str, Any]) -> dict[str, Any]:
        statement_id = response.get("statement_id")
        if not statement_id:
            raise RuntimeError(f"Databricks statement response has no statement_id: {response}")

        while True:
            status = response.get("status") or {}
            state = status.get("state")

            if state == "SUCCEEDED":
                return response

            if state in {"FAILED", "CANCELED", "CLOSED"}:
                error = status.get("error") or response.get("manifest", {}).get("truncated")
                raise RuntimeError(
                    f"Databricks SQL statement {statement_id} ended with {state}: {error}"
                )

            time.sleep(self.poll_interval_seconds)
            response = self.api_get(f"/api/2.0/sql/statements/{statement_id}")

    def statement_rows(self, response: dict[str, Any]) -> list[list[Any]]:
        statement_id = response.get("statement_id")
        if not statement_id:
            raise RuntimeError(f"Databricks statement response has no statement_id: {response}")

        rows: list[list[Any]] = []
        seen_chunk_indexes: set[int] = set()
        result = response.get("result") or {}

        if isinstance(result, dict):
            first_chunk_index = result.get("chunk_index")
            if isinstance(first_chunk_index, int):
                seen_chunk_indexes.add(first_chunk_index)
            rows.extend(result.get("data_array") or [])

        manifest = response.get("manifest") or {}
        chunks = manifest.get("chunks") or []
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue

            chunk_index = chunk.get("chunk_index")
            if not isinstance(chunk_index, int) or chunk_index in seen_chunk_indexes:
                continue

            chunk_response = self.api_get(
                f"/api/2.0/sql/statements/{statement_id}/result/chunks/{chunk_index}"
            )
            chunk_result = chunk_response.get("result") or chunk_response
            if isinstance(chunk_result, dict):
                rows.extend(chunk_result.get("data_array") or [])
            seen_chunk_indexes.add(chunk_index)

        return rows


def normalize_cli_fs_path(path: str) -> str:
    if path.startswith("dbfs:/"):
        return path
    if path.startswith("/Volumes/"):
        return f"dbfs:{path}"
    return path


def parse_volume_path(path: str) -> VolumePath | None:
    normalized = path.removeprefix("dbfs:")
    parts = normalized.split("/")
    if len(parts) < 5 or parts[1] != "Volumes":
        return None

    relative_path = "/".join(parts[5:])
    return VolumePath(
        catalog=parts[2],
        schema=parts[3],
        volume=parts[4],
        relative_path=relative_path,
    )


def cloud_location_for_volume_path(path: str, databricks_cli: DatabricksCli) -> str:
    volume_path = parse_volume_path(path)
    if volume_path is None:
        return path

    storage_location = databricks_cli.volume_storage_location(volume_path).rstrip("/")
    if not volume_path.relative_path:
        return storage_location
    return f"{storage_location}/{volume_path.relative_path}"


def parse_cli_fs_entries(data: Any) -> list[FsEntry]:
    if data is None:
        return []

    entries: Any
    if isinstance(data, list):
        entries = data
    elif isinstance(data, dict):
        entries = (
            data.get("files")
            or data.get("contents")
            or data.get("entries")
            or data.get("resources")
            or []
        )
    else:
        raise RuntimeError(f"Unexpected Databricks fs ls response: {data!r}")

    parsed_entries: list[FsEntry] = []
    for entry in entries:
        if not isinstance(entry, dict):
            continue

        raw_path = str(entry.get("path") or entry.get("name") or "")
        path = raw_path.rstrip("/")
        if not path:
            continue

        raw_name = str(entry.get("name") or path.rsplit("/", 1)[-1])
        name = raw_name.rstrip("/").rsplit("/", 1)[-1]
        object_type = str(
            entry.get("type") or entry.get("object_type") or entry.get("file_type") or ""
        ).upper()
        is_dir = bool(
            entry.get("is_dir")
            or entry.get("is_directory")
            or object_type in {"DIRECTORY", "DIR"}
            or raw_path.endswith("/")
            or raw_name.endswith("/")
        )
        parsed_entries.append(FsEntry(path=path, name=name, is_dir=is_dir))

    return parsed_entries


def quote_command(command: list[str]) -> str:
    return " ".join(shlex_quote(part) for part in command)


def shlex_quote(value: str) -> str:
    if not value:
        return "''"
    safe_chars = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_@%+=:,./-")
    if all(char in safe_chars for char in value):
        return value
    return "'" + value.replace("'", "'\"'\"'") + "'"


def get_spark_session(required: bool) -> Any | None:
    existing_spark = globals().get("spark")
    if existing_spark is not None:
        return existing_spark

    if not required:
        return None

    try:
        pyspark_sql = cast(Any, importlib.import_module("pyspark.sql"))
        SparkSession = pyspark_sql.SparkSession
    except ImportError as exc:
        raise RuntimeError(
            "A Spark session is required. Run this script inside a Databricks "
            "Python context, pass --warehouse-id to execute locally through "
            "Databricks SQL, or use --dry-run to print SQL only."
        ) from exc

    return SparkSession.builder.getOrCreate()


def get_dbutils(spark_session: Any | None) -> Any | None:
    existing_dbutils = globals().get("dbutils")
    if existing_dbutils is not None:
        return existing_dbutils

    if spark_session is None:
        return None

    try:
        pyspark_dbutils = cast(Any, importlib.import_module("pyspark.dbutils"))
        DBUtils = pyspark_dbutils.DBUtils
    except ImportError:
        return None

    return DBUtils(spark_session)


def quote_identifier(identifier: str) -> str:
    return f"`{identifier.replace('`', '``')}`"


def quote_string_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def schema_identifier(catalog: str, schema: str) -> str:
    return f"{quote_identifier(catalog)}.{quote_identifier(schema)}"


def table_identifier(catalog: str, schema: str, table: str) -> str:
    return f"{schema_identifier(catalog, schema)}.{quote_identifier(table)}"


def is_hidden_or_system_name(name: str) -> bool:
    return name.startswith(HIDDEN_OR_SYSTEM_PREFIXES)


def is_parquet_path(path: str) -> bool:
    return path.rstrip("/").lower().endswith(PARQUET_SUFFIX)


def normalize_table_name(value: str) -> str:
    table_name = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip().lower())
    table_name = re.sub(r"_+", "_", table_name).strip("_")

    if not table_name:
        raise ValueError(f"Could not derive a table name from {value!r}")

    if table_name[0].isdigit():
        table_name = f"_{table_name}"

    return table_name


def normalize_column_name(value: str, *, fallback: str) -> str:
    column_name = re.sub(r"[^0-9A-Za-z_]+", "_", value.strip().lower())
    column_name = re.sub(r"_+", "_", column_name).strip("_")

    if not column_name:
        column_name = fallback

    if column_name[0].isdigit():
        column_name = f"_{column_name}"

    return column_name


def stable_hash(value: str) -> str:
    return hashlib.sha1(value.encode("utf-8")).hexdigest()[:COLUMN_ALIAS_HASH_LENGTH]


def truncate_column_name(value: str, *, source_name: str) -> str:
    if len(value) <= UC_MAX_COLUMN_NAME_LENGTH:
        return value

    suffix = f"_{stable_hash(source_name)}"
    prefix_length = UC_MAX_COLUMN_NAME_LENGTH - len(suffix)
    prefix = value[:prefix_length].rstrip("_")
    if not prefix:
        prefix = "col"[:prefix_length]
    return f"{prefix}{suffix}"


def unique_column_name(
    value: str,
    *,
    used_names: set[str],
    source_name: str,
    position: int,
) -> str:
    candidate = value
    candidate_key = candidate.lower()
    if candidate_key not in used_names:
        used_names.add(candidate_key)
        return candidate

    unique_suffix = f"_{position}_{stable_hash(f'{position}:{source_name}')}"
    prefix_length = UC_MAX_COLUMN_NAME_LENGTH - len(unique_suffix)
    prefix = candidate[:prefix_length].rstrip("_") or "col"
    candidate = f"{prefix}{unique_suffix}"
    counter = 2

    while candidate.lower() in used_names:
        counter_suffix = f"_{position}_{counter}_{stable_hash(f'{counter}:{source_name}')}"
        prefix_length = UC_MAX_COLUMN_NAME_LENGTH - len(counter_suffix)
        prefix = value[:prefix_length].rstrip("_") or "col"
        candidate = f"{prefix}{counter_suffix}"
        counter += 1

    used_names.add(candidate.lower())
    return candidate


def column_name_needs_alias(name: str) -> bool:
    return (
        not name
        or len(name) > UC_MAX_COLUMN_NAME_LENGTH
        or any(char in DELTA_INVALID_COLUMN_NAME_CHARS for char in name)
    )


def safe_uc_column_names(source_column_names: list[str]) -> list[str]:
    used_names: set[str] = set()
    output_names: list[str] = []

    for position, source_name in enumerate(source_column_names, start=1):
        if column_name_needs_alias(source_name):
            fallback = f"col_{position}"
            candidate = normalize_column_name(source_name, fallback=fallback)
        else:
            candidate = source_name

        candidate = truncate_column_name(candidate, source_name=source_name)
        candidate = unique_column_name(
            candidate,
            used_names=used_names,
            source_name=source_name,
            position=position,
        )
        output_names.append(candidate)

    return output_names


def relation_with_column_aliases(
    base_relation: str,
    *,
    output_column_names: list[str],
) -> list[str]:
    lines = [f"{base_relation} AS source ("]
    for index, column_name in enumerate(output_column_names):
        suffix = "," if index < len(output_column_names) - 1 else ""
        lines.append(f"  {quote_identifier(column_name)}{suffix}")
    lines.append(")")
    return lines


def source_relation_lines(
    source_location: str,
    *,
    source_reader: str,
    output_column_names: list[str] | None = None,
) -> list[str]:
    if source_reader == READ_FILES_SOURCE_READER:
        lines = [
            "read_files(",
            f"  {quote_string_literal(source_location)},",
            "  format => 'parquet'",
            ")",
        ]
        if output_column_names:
            lines[-1] = ") AS source ("
            for index, column_name in enumerate(output_column_names):
                suffix = "," if index < len(output_column_names) - 1 else ""
                lines.append(f"  {quote_identifier(column_name)}{suffix}")
            lines.append(")")
        return lines

    if source_reader == PARQUET_SOURCE_READER:
        relation = f"parquet.{quote_identifier(source_location)}"
        if output_column_names:
            return relation_with_column_aliases(
                relation,
                output_column_names=output_column_names,
            )
        return [relation]

    raise ValueError(f"Unsupported source reader: {source_reader}")


def describe_read_files_query_sql(source_location: str) -> str:
    relation_lines = source_relation_lines(
        source_location,
        source_reader=READ_FILES_SOURCE_READER,
    )
    return "\n".join(["DESCRIBE QUERY SELECT *", f"FROM {relation_lines[0]}", *relation_lines[1:]])


def parquet_column_names_from_rows(rows: list[list[Any]]) -> list[str]:
    column_names: list[str] = []
    for row in rows:
        if not row:
            continue

        column_name = row[0]
        if column_name is None:
            continue

        column_names.append(str(column_name))

    return column_names


def parquet_column_names(
    source_location: str,
    *,
    spark_session: Any | None,
    databricks_cli: DatabricksCli | None,
) -> list[str]:
    if spark_session is not None:
        schema = spark_session.read.parquet(source_location).schema
        return [field.name for field in schema.fields]

    if databricks_cli is not None and databricks_cli.warehouse_id:
        rows = databricks_cli.execute_query(describe_read_files_query_sql(source_location))
        return parquet_column_names_from_rows(rows)

    raise RuntimeError(
        "Column aliases are required, but no Spark session or Databricks SQL "
        "warehouse is available to inspect the parquet schema."
    )


def should_retry_with_column_aliases(exc: RuntimeError) -> bool:
    message = str(exc)
    return (
        "DELTA_INVALID_CHARACTERS_IN_COLUMN_NAMES" in message
        or "ColumnInfo.name" in message
        or "Maximum length is 255" in message
        or "toolong" in message
    )


def should_retry_with_parquet_reader(exc: RuntimeError) -> bool:
    return "CF_FAILED_TO_INFER_SCHEMA" in str(exc)


def should_skip_unreadable_parquet(exc: RuntimeError) -> bool:
    message = str(exc)
    return (
        "CF_FAILED_TO_INFER_SCHEMA" in message
        or "UNABLE_TO_INFER_SCHEMA" in message
        or "DELTA_EMPTY_DATA" in message
        or "EMPTY_SCHEMA" in message
        or "empty schema" in message.lower()
    )


def storage_uc_catalog_schema(storage: StorageBackend) -> tuple[str, str]:
    storage_config = storage.config.get("storage", {})
    catalog = storage_config.get("uc_catalog") or storage_config.get("catalog", DEFAULT_CATALOG)
    schema = storage_config.get("uc_schema") or storage_config.get("schema", DEFAULT_SCHEMA)
    return str(catalog), str(schema)


def parquet_stem(path: str) -> str:
    name = path.rstrip("/").rsplit("/", 1)[-1]
    if name.lower().endswith(PARQUET_SUFFIX):
        return name[: -len(PARQUET_SUFFIX)]
    return name


def volume_name(path: str) -> str | None:
    normalized = path.removeprefix("dbfs:")
    parts = normalized.split("/")
    if len(parts) >= 5 and parts[1] == "Volumes":
        return parts[4]
    return None


def source_group_name(path: str) -> str | None:
    volume = volume_name(path)
    if volume:
        return volume

    parent = path.rstrip("/").rsplit("/", 1)[0]
    if not parent or parent == path.rstrip("/"):
        return None
    return parent.rsplit("/", 1)[-1] or None


def table_name_for_path(
    path: str,
    *,
    table_prefix: str,
    include_volume_prefix: bool,
) -> str:
    parts: list[str] = []
    if table_prefix:
        parts.append(table_prefix)
    if include_volume_prefix:
        source_group = source_group_name(path)
        if source_group:
            parts.append(source_group)
    parts.append(parquet_stem(path))
    return normalize_table_name("_".join(parts))


def item_is_directory(file_info: Any) -> bool:
    is_dir = getattr(file_info, "isDir", None)
    if callable(is_dir):
        return bool(is_dir())
    if isinstance(is_dir, bool):
        return is_dir
    return str(file_info.path).endswith("/")


def iter_parquet_paths_dbutils(
    dbutils: Any,
    root: str,
    *,
    recursive: bool,
) -> list[str]:
    root = root.rstrip("/")
    if is_parquet_path(root):
        return [root]

    found: list[str] = []
    stack = [root]

    while stack:
        current = stack.pop()
        try:
            entries = dbutils.fs.ls(current)
        except Exception as exc:  # noqa: BLE001 - Databricks wraps FS errors.
            raise FileNotFoundError(f"Could not list {current}: {exc}") from exc

        for entry in entries:
            entry_name = str(entry.name).rstrip("/")
            entry_path = str(entry.path).rstrip("/")

            if is_hidden_or_system_name(entry_name):
                continue

            if item_is_directory(entry):
                if is_parquet_path(entry_path):
                    found.append(entry_path)
                elif recursive:
                    stack.append(entry_path)
                continue

            if is_parquet_path(entry_path):
                found.append(entry_path)

    return found


def iter_parquet_paths_cli(
    databricks_cli: DatabricksCli,
    root: str,
    *,
    recursive: bool,
) -> list[str]:
    root = root.rstrip("/")
    if is_parquet_path(root):
        return [root]

    found: list[str] = []
    stack = [root]

    while stack:
        current = stack.pop()
        try:
            entries = databricks_cli.fs_ls(current)
        except RuntimeError as exc:
            raise FileNotFoundError(f"Could not list {current}: {exc}") from exc

        for entry in entries:
            entry_name = entry.name.rstrip("/")
            entry_path = entry.path.rstrip("/")

            if is_hidden_or_system_name(entry_name):
                continue

            if entry.is_dir:
                if is_parquet_path(entry_path):
                    found.append(entry_path)
                elif recursive:
                    stack.append(entry_path)
                continue

            if is_parquet_path(entry_path):
                found.append(entry_path)

    return found


def local_path_can_be_listed(path: str) -> bool:
    return Path(path).expanduser().exists()


def iter_parquet_paths_local(root: str, *, recursive: bool) -> list[str]:
    root_path = Path(root).expanduser()

    if not root_path.exists():
        raise FileNotFoundError(f"Local path does not exist: {root}")

    if root_path.is_file():
        return [str(root_path)] if is_parquet_path(str(root_path)) else []

    iterator = root_path.rglob("*") if recursive else root_path.iterdir()
    found: list[str] = []

    for path in iterator:
        if is_hidden_or_system_name(path.name):
            continue
        if is_parquet_path(str(path)) and (path.is_file() or path.is_dir()):
            found.append(str(path))

    return found


def discover_parquet_paths(
    roots: list[str],
    *,
    dbutils: Any | None,
    databricks_cli: DatabricksCli | None,
    recursive: bool,
    skip_missing_roots: bool,
) -> list[str]:
    parquet_paths: list[str] = []

    for root in roots:
        try:
            if dbutils is not None:
                parquet_paths.extend(iter_parquet_paths_dbutils(dbutils, root, recursive=recursive))
            elif local_path_can_be_listed(root):
                parquet_paths.extend(iter_parquet_paths_local(root, recursive=recursive))
            elif databricks_cli is not None and databricks_cli.available:
                parquet_paths.extend(
                    iter_parquet_paths_cli(databricks_cli, root, recursive=recursive)
                )
            else:
                parquet_paths.extend(iter_parquet_paths_local(root, recursive=recursive))
        except FileNotFoundError as exc:
            if not skip_missing_roots:
                raise
            print(f"WARNING: {exc}", file=sys.stderr)

    return sorted(set(parquet_paths))


def build_registrations(
    parquet_paths: list[str],
    *,
    table_prefix: str,
    include_volume_prefix: bool,
    duplicate_strategy: str,
) -> list[TableRegistration]:
    registrations = [
        TableRegistration(
            table_name=table_name_for_path(
                path,
                table_prefix=table_prefix,
                include_volume_prefix=include_volume_prefix,
            ),
            source_path=path,
        )
        for path in parquet_paths
    ]

    registrations = resolve_duplicate_table_names(
        registrations,
        table_prefix=table_prefix,
        duplicate_strategy=duplicate_strategy,
    )

    return sorted(registrations, key=lambda item: item.table_name)


def resolve_duplicate_table_names(
    registrations: list[TableRegistration],
    *,
    table_prefix: str,
    duplicate_strategy: str,
) -> list[TableRegistration]:
    duplicate_tables = find_duplicate_table_names(registrations)

    if not duplicate_tables:
        return registrations

    if duplicate_strategy == "prefix-volume":
        duplicate_paths = {path for paths in duplicate_tables.values() for path in paths}
        resolved = [
            TableRegistration(
                table_name=table_name_for_path(
                    registration.source_path,
                    table_prefix=table_prefix,
                    include_volume_prefix=True,
                ),
                source_path=registration.source_path,
            )
            if registration.source_path in duplicate_paths
            else registration
            for registration in registrations
        ]

        duplicate_tables = find_duplicate_table_names(resolved)
        if not duplicate_tables:
            return resolved

    details = "\n".join(
        f"  {table}: {', '.join(paths)}" for table, paths in sorted(duplicate_tables.items())
    )
    raise ValueError(
        "Multiple parquet files map to the same target table name. "
        "Use --include-volume-prefix, --table-prefix, or scan a single volume root.\n"
        f"{details}"
    )


def find_duplicate_table_names(
    registrations: list[TableRegistration],
) -> dict[str, list[str]]:
    paths_by_table: dict[str, list[str]] = defaultdict(list)
    for registration in registrations:
        paths_by_table[registration.table_name].append(registration.source_path)

    return {table: paths for table, paths in paths_by_table.items() if len(paths) > 1}


def sql_statements_for_registration(
    registration: TableRegistration,
    *,
    catalog: str,
    schema: str,
    replace_existing: bool,
    table_mode: str,
    source_reader: str = READ_FILES_SOURCE_READER,
    source_location: str | None = None,
    source_column_names: list[str] | None = None,
) -> list[str]:
    table = table_identifier(catalog, schema, registration.table_name)
    location = source_location or registration.source_location
    statements: list[str] = []

    if replace_existing:
        statements.append(f"DROP TABLE IF EXISTS {table}")
        create_prefix = "CREATE TABLE"
    else:
        create_prefix = "CREATE TABLE IF NOT EXISTS"

    if table_mode == "managed-delta":
        output_column_names = (
            safe_uc_column_names(source_column_names) if source_column_names else None
        )
        if output_column_names == source_column_names:
            output_column_names = None
        relation_lines = source_relation_lines(
            registration.source_location,
            source_reader=source_reader,
            output_column_names=output_column_names,
        )
        statements.append(
            "\n".join(
                [
                    f"{create_prefix} {table}",
                    "USING DELTA",
                    "TBLPROPERTIES (",
                    "  'delta.columnMapping.mode' = 'name',",
                    "  'delta.minReaderVersion' = '2',",
                    "  'delta.minWriterVersion' = '5'",
                    ")",
                    "AS SELECT *",
                    f"FROM {relation_lines[0]}",
                    *relation_lines[1:],
                ]
            )
        )
    else:
        statements.append(
            "\n".join(
                [
                    f"{create_prefix} {table}",
                    "USING PARQUET",
                    f"LOCATION {quote_string_literal(location)}",
                ]
            )
        )

    return statements


def run_sql(
    spark_session: Any | None,
    databricks_cli: DatabricksCli | None,
    sql: str,
    *,
    dry_run: bool,
) -> None:
    print(f"{sql};")
    if dry_run:
        return

    if spark_session is not None:
        spark_session.sql(sql)
        return

    if databricks_cli is None:
        raise RuntimeError(
            "No Spark session is available locally. Pass --warehouse-id to execute "
            "through Databricks SQL, add --dry-run to print SQL only, or run this "
            "script on a Databricks cluster."
        )

    databricks_cli.execute_sql(sql)


def run_managed_delta_create_with_retries(
    *,
    registration: TableRegistration,
    catalog: str,
    schema: str,
    source_location: str,
    spark_session: Any | None,
    databricks_cli: DatabricksCli | None,
    dry_run: bool,
    sql: str,
) -> bool:
    try:
        run_sql(spark_session, databricks_cli, sql, dry_run=dry_run)
        return True
    except RuntimeError as exc:
        if should_retry_with_column_aliases(exc):
            print(
                "WARNING: Databricks rejected column names for "
                f"{registration.table_name}; retrying with safe UC aliases.",
                file=sys.stderr,
            )
            source_column_names = parquet_column_names(
                registration.source_location,
                spark_session=spark_session,
                databricks_cli=databricks_cli,
            )
            retry_sql = sql_statements_for_registration(
                registration,
                catalog=catalog,
                schema=schema,
                replace_existing=False,
                table_mode="managed-delta",
                source_location=source_location,
                source_column_names=source_column_names,
            )[-1]
            try:
                run_sql(spark_session, databricks_cli, retry_sql, dry_run=dry_run)
                return True
            except RuntimeError as retry_exc:
                if not should_retry_with_parquet_reader(retry_exc):
                    raise
                exc = retry_exc

        if not should_retry_with_parquet_reader(exc):
            raise

        print(
            "WARNING: Databricks could not infer schema with read_files for "
            f"{registration.table_name}; retrying with the parquet data source.",
            file=sys.stderr,
        )
        retry_sql = sql_statements_for_registration(
            registration,
            catalog=catalog,
            schema=schema,
            replace_existing=False,
            table_mode="managed-delta",
            source_reader=PARQUET_SOURCE_READER,
            source_location=source_location,
        )[-1]
        try:
            run_sql(spark_session, databricks_cli, retry_sql, dry_run=dry_run)
            return True
        except RuntimeError as retry_exc:
            if not should_skip_unreadable_parquet(retry_exc):
                raise

            print(
                "WARNING: Skipping "
                f"{registration.table_name}; {registration.source_location} has no "
                "schema Databricks can materialize as a Delta table.",
                file=sys.stderr,
            )
            return False


def create_tables(args: argparse.Namespace) -> int:
    """Create UC tables from parquet files using parsed CLI arguments."""
    storage = StorageBackend(profile=getattr(args, "aco_profile", None))
    volume_specs = [parse_volume_fqn(value) for value in args.volume_fqns or []]

    if volume_specs and (args.catalog is None or args.schema is None):
        catalog_schemas = {(spec.catalog, spec.schema) for spec in volume_specs}
        if len(catalog_schemas) > 1:
            raise ValueError(
                "Pass --catalog and --schema when scanning volumes from multiple "
                "catalog/schema pairs."
            )

    storage_catalog, storage_schema = storage_uc_catalog_schema(storage)
    catalog = args.catalog or (volume_specs[0].catalog if volume_specs else storage_catalog)
    schema = args.schema or (volume_specs[0].schema if volume_specs else storage_schema)

    volume_roots = list(args.volume_roots or [])
    volume_roots.extend(spec.root_path for spec in volume_specs)
    if not volume_roots:
        layers = cast(list[str], args.layers) if args.layers else list(DEFAULT_LAYERS)
        volume_roots = storage.get_uc_volume_roots(layers)

    recursive = not args.no_recurse

    spark_session = get_spark_session(required=not args.dry_run and not args.warehouse_id)
    dbutils = get_dbutils(spark_session)
    databricks_cli = DatabricksCli(
        databricks_bin=args.databricks_bin,
        profile=args.profile,
        target=args.target,
        warehouse_id=args.warehouse_id,
        wait_timeout_seconds=args.wait_timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds,
    )

    if spark_session is None and not args.dry_run:
        if not args.warehouse_id:
            raise RuntimeError(
                "No Spark session is available locally. Pass --warehouse-id to execute "
                "through Databricks SQL, add --dry-run to print SQL only, or run this "
                "script on a Databricks cluster."
            )
        if not databricks_cli.available:
            raise RuntimeError(f"Databricks CLI not found: {args.databricks_bin}")

    if dbutils is None and not databricks_cli.available:
        print(
            f"WARNING: Databricks CLI not found: {args.databricks_bin}. "
            "Remote UC volume paths cannot be listed locally.",
            file=sys.stderr,
        )

    parquet_paths = discover_parquet_paths(
        volume_roots,
        dbutils=dbutils,
        databricks_cli=databricks_cli if dbutils is None else None,
        recursive=recursive,
        skip_missing_roots=args.skip_missing_roots,
    )

    if not parquet_paths:
        print("No parquet files found.", file=sys.stderr)
        return 1

    registrations = build_registrations(
        parquet_paths,
        table_prefix=args.table_prefix,
        include_volume_prefix=args.include_volume_prefix,
        duplicate_strategy=args.duplicate_strategy,
    )

    run_sql(
        spark_session,
        databricks_cli,
        f"CREATE SCHEMA IF NOT EXISTS {schema_identifier(catalog, schema)}",
        dry_run=args.dry_run,
    )

    skipped_registrations: list[TableRegistration] = []

    for registration in registrations:
        source_location = registration.source_location
        if (
            args.table_mode == "external-parquet"
            and spark_session is None
            and databricks_cli.warehouse_id
        ):
            source_location = cloud_location_for_volume_path(
                source_location,
                databricks_cli,
            )

        skipped = False
        for sql in sql_statements_for_registration(
            registration,
            catalog=catalog,
            schema=schema,
            replace_existing=args.replace_existing,
            table_mode=args.table_mode,
            source_location=source_location,
        ):
            if args.table_mode == "managed-delta" and sql.lstrip().upper().startswith(
                "CREATE TABLE"
            ):
                created_or_existing = run_managed_delta_create_with_retries(
                    registration=registration,
                    catalog=catalog,
                    schema=schema,
                    source_location=source_location,
                    spark_session=spark_session,
                    databricks_cli=databricks_cli,
                    dry_run=args.dry_run,
                    sql=sql,
                )
                if not created_or_existing:
                    skipped = True
                    break
            else:
                run_sql(spark_session, databricks_cli, sql, dry_run=args.dry_run)

        if skipped:
            skipped_registrations.append(registration)

    action = "Would register" if args.dry_run else "Registered"
    processed_count = len(registrations) - len(skipped_registrations)
    print(f"{action} {processed_count} parquet-backed tables in {catalog}.{schema}.")
    if skipped_registrations:
        skipped_tables = ", ".join(item.table_name for item in skipped_registrations)
        print(
            f"Skipped {len(skipped_registrations)} parquet files with no usable schema: "
            f"{skipped_tables}.",
            file=sys.stderr,
        )
    return 0


def cmd_create_tables(args: argparse.Namespace) -> int:
    """Entry point used by ``aco databricks create-tables``."""
    return create_tables(args)


def main(argv: list[str] | None = None) -> int:
    """Standalone module entry point."""
    return create_tables(parse_args(argv))


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except (FileNotFoundError, RuntimeError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        raise SystemExit(1) from exc
