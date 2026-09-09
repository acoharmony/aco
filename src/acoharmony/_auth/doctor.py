"""Auth health checks for 4icli and ACOMS containers."""

from __future__ import annotations

import grp
import json
import os
import pwd
import stat
import subprocess
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from .env_file import find_deploy_dir, read_env_file
from .paths import AuthPaths, get_auth_paths
from .public_ip import ProbeResult, fetch_container_public_ip, fetch_public_ip, parse_ip_values
from .registry import AuthRegistry, TokenRecord, directory_fingerprint, file_fingerprint

SERVICES = ("4icli", "acoms")
NONSECRET_ENV_KEYS = {
    "FOURICLI_APM_ID",
    "FOURICLI_IP_ADDRESS",
    "FOURICLI_PUBLIC_IP",
    "FOURICLI_ALT_IP_ADDRESS",
    "FOURICLI_REGISTERED_IP",
    "ACOMS_API_ID",
    "ACOMS_IP_ADDRESS",
    "ACOMS_PUBLIC_IP",
    "ACOMS_ALT_IP_ADDRESS",
    "ACOMS_REGISTERED_IP",
}


@dataclass
class PathStatus:
    """Safe file/directory metadata."""

    path: str
    exists: bool
    kind: str | None = None
    mode: str | None = None
    owner: str | None = None
    group: str | None = None
    size_bytes: int | None = None
    mtime: str | None = None
    fingerprint: str | None = None
    warning: str | None = None


@dataclass
class LiveCheck:
    """Result from a live vendor auth smoke test."""

    status: str
    message: str
    command: list[str]


@dataclass
class ServiceReport:
    """Auth diagnostic report for one service."""

    service: str
    diagnosis: str
    entity_id: str | None
    host_public_ip: ProbeResult
    container_public_ip: ProbeResult
    registered_ips: list[str]
    registry_record: TokenRecord
    config: PathStatus
    live_check: LiveCheck

    def as_dict(self) -> dict[str, Any]:
        """Return JSON-serializable data."""
        data = asdict(self)
        return data


def _safe_owner(uid: int) -> str:
    try:
        return pwd.getpwuid(uid).pw_name
    except KeyError:
        return str(uid)


def _safe_group(gid: int) -> str:
    try:
        return grp.getgrgid(gid).gr_name
    except KeyError:
        return str(gid)


def path_status(path: Path, *, directory: bool = False) -> PathStatus:
    """Build non-secret metadata for a config path."""
    if not path.exists():
        return PathStatus(path=str(path), exists=False)

    st = path.stat()
    mode = stat.S_IMODE(st.st_mode)
    owner = _safe_owner(st.st_uid)
    group = _safe_group(st.st_gid)
    mtime = datetime.fromtimestamp(st.st_mtime).isoformat(timespec="seconds")
    if directory and path.is_dir() and not any(child.is_file() for child in path.rglob("*")):
        return PathStatus(
            path=str(path),
            exists=False,
            kind="directory",
            mode=f"{mode:04o}",
            owner=owner,
            group=group,
            mtime=mtime,
            warning="directory exists but contains no config files; run `aco acoms setup`",
        )

    fingerprint = directory_fingerprint(path) if path.is_dir() else file_fingerprint(path)
    warning = None
    if mode & 0o077:
        warning = f"permissions are too broad ({mode:04o}); run `aco auth harden`"

    return PathStatus(
        path=str(path),
        exists=True,
        kind="directory" if path.is_dir() else "file",
        mode=f"{mode:04o}",
        owner=owner,
        group=group,
        size_bytes=None if path.is_dir() else st.st_size,
        mtime=mtime,
        fingerprint=fingerprint,
        warning=warning,
    )


def _deploy_nonsecret_env() -> dict[str, str]:
    try:
        deploy_dir = find_deploy_dir()
    except FileNotFoundError:
        return {}
    values = read_env_file(deploy_dir / ".env", NONSECRET_ENV_KEYS)
    for key in NONSECRET_ENV_KEYS:
        if os.getenv(key):
            values[key] = os.environ[key]
    return values


def _env_registered_ips(service: str, env: dict[str, str]) -> list[str]:
    if service == "4icli":
        keys = [
            "FOURICLI_REGISTERED_IP",
            "FOURICLI_PUBLIC_IP",
            "FOURICLI_IP_ADDRESS",
            "FOURICLI_ALT_IP_ADDRESS",
        ]
    else:
        keys = [
            "ACOMS_REGISTERED_IP",
            "ACOMS_PUBLIC_IP",
            "ACOMS_IP_ADDRESS",
            "ACOMS_ALT_IP_ADDRESS",
        ]
    try:
        return parse_ip_values([env.get(key) for key in keys])
    except ValueError:
        return []


def _entity_id(service: str, record: TokenRecord, env: dict[str, str]) -> str | None:
    if record.entity_id:
        return record.entity_id
    if service == "4icli":
        return env.get("FOURICLI_APM_ID")
    return env.get("ACOMS_API_ID")


def _service_config_path(service: str, paths: AuthPaths) -> tuple[Path, bool]:
    if service == "4icli":
        if paths.fouricli_config.exists() or not paths.legacy_fouricli_config.exists():
            return paths.fouricli_config, False
        return paths.legacy_fouricli_config, False

    if paths.acoms_config_dir.exists() or not paths.legacy_acoms_config_dir.exists():
        return paths.acoms_config_dir, True
    return paths.legacy_acoms_config_dir, True


def _classify_output(output: str) -> str | None:
    text = output.lower()
    if any(token in text for token in ("certificate", "x509", "tls", "ssl", "zscaler")):
        return "TLS_OR_ZSCALER"
    if any(
        token in text for token in ("unauthorized", "forbidden", "401", "403", "invalid client")
    ):
        return "BAD_SECRET"
    return None


def live_check(service: str, entity_id: str | None, year: int, timeout: float) -> LiveCheck:
    """Run a small live auth check inside the service container."""
    if not entity_id:
        return LiveCheck("MISSING_ENTITY_ID", "No entity ID is configured.", [])

    if service == "4icli":
        command = [
            "docker",
            "exec",
            "4icli",
            "4icli",
            "datahub",
            "-v",
            "-a",
            entity_id,
            "-y",
            str(year),
        ]
    else:
        command = [
            "docker",
            "exec",
            "acoms",
            "acoms",
            "datahub",
            "--view",
            "--aco",
            entity_id,
            "--year",
            str(year),
        ]

    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        return LiveCheck("CONTAINER_DOWN", "docker command not found.", command)
    except subprocess.TimeoutExpired:
        return LiveCheck("UNKNOWN_ERROR", f"Timed out after {timeout:g}s.", command)

    combined = "\n".join(part for part in (result.stdout, result.stderr) if part).strip()
    classified = _classify_output(combined)
    if result.returncode != 0:
        if classified:
            return LiveCheck(classified, combined or f"Exit code {result.returncode}.", command)
        if "no such container" in combined.lower() or "is not running" in combined.lower():
            return LiveCheck("CONTAINER_DOWN", combined, command)
        return LiveCheck("UNKNOWN_ERROR", combined or f"Exit code {result.returncode}.", command)

    if classified:
        return LiveCheck(classified, combined, command)
    return LiveCheck("OK", "Live auth check completed.", command)


def _diagnose(
    *,
    config: PathStatus,
    current_ips: list[str],
    registered_ips: list[str],
    live: LiveCheck,
) -> str:
    if not config.exists:
        return "MISSING_CONFIG"
    if live.status == "CONTAINER_DOWN":
        return "CONTAINER_DOWN"
    if live.status == "TLS_OR_ZSCALER":
        return "TLS_OR_ZSCALER"
    if current_ips and registered_ips and not any(ip in registered_ips for ip in current_ips):
        return "IP_MISMATCH"
    if live.status == "BAD_SECRET":
        return "BAD_SECRET"
    if live.status == "OK":
        return "OK"
    if not registered_ips:
        return "REGISTERED_IP_UNKNOWN"
    if live.status == "SKIPPED":
        return "CHECK_SKIPPED"
    return live.status


def _observed_ips(*probes: ProbeResult) -> list[str]:
    """Return unique observed IPs from multiple probes."""
    values: list[str] = []
    for probe in probes:
        for ip in probe.observed_values:
            if ip not in values:
                values.append(ip)
    return values


def check_service(
    service: str,
    *,
    paths: AuthPaths | None = None,
    registry: AuthRegistry | None = None,
    host_public_ip: ProbeResult | None = None,
    skip_live: bool = False,
    timeout: float = 30.0,
    year: int | None = None,
) -> ServiceReport:
    """Check one service and return a structured report."""
    if service not in SERVICES:
        raise ValueError(f"Unknown auth service: {service}")

    paths = paths or get_auth_paths()
    registry = registry or AuthRegistry(paths.registry)
    env = _deploy_nonsecret_env()
    record = registry.get(service)
    registered_ips = [*record.registered_ips]
    for ip in _env_registered_ips(service, env):
        if ip not in registered_ips:
            registered_ips.append(ip)

    entity_id = _entity_id(service, record, env)
    host_ip = host_public_ip or fetch_public_ip()
    container_ip = fetch_container_public_ip(service, timeout=min(timeout, 8.0))
    config_path, is_dir = _service_config_path(service, paths)
    config = path_status(config_path, directory=is_dir)
    check_year = year or datetime.now().year
    if skip_live:
        live = LiveCheck("SKIPPED", "Live auth check skipped by request.", [])
    else:
        live = live_check(service, entity_id, check_year, timeout)

    current_ips = _observed_ips(container_ip, host_ip)
    diagnosis = _diagnose(
        config=config,
        current_ips=current_ips,
        registered_ips=registered_ips,
        live=live,
    )

    return ServiceReport(
        service=service,
        diagnosis=diagnosis,
        entity_id=entity_id,
        host_public_ip=host_ip,
        container_public_ip=container_ip,
        registered_ips=registered_ips,
        registry_record=record,
        config=config,
        live_check=live,
    )


def write_audit_log(paths: AuthPaths, reports: list[ServiceReport]) -> None:
    """Append a non-secret auth check audit record."""
    paths.logs_dir.mkdir(parents=True, exist_ok=True)
    record = {
        "checked_at": datetime.now().isoformat(timespec="seconds"),
        "reports": [report.as_dict() for report in reports],
    }
    with (paths.logs_dir / "auth-checks.jsonl").open("a") as handle:
        handle.write(json.dumps(record, sort_keys=True) + "\n")
