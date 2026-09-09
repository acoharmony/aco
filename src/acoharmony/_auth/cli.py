"""Command handlers for `aco auth`."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path

from .doctor import SERVICES, check_service, write_audit_log
from .env_file import find_deploy_dir, read_env_file
from .paths import get_auth_paths
from .public_ip import fetch_public_ip, parse_ip_values
from .registry import AuthRegistry


def add_auth_subparsers(subparsers) -> argparse.ArgumentParser:
    """Register the top-level auth parser."""
    parser = subparsers.add_parser(
        "auth",
        help="Check and maintain DataHub auth/IP registration state",
    )
    auth_subparsers = parser.add_subparsers(dest="auth_command", help="auth commands")

    doctor = auth_subparsers.add_parser(
        "doctor",
        help="Check public IP, registered IP, config, and live auth status",
    )
    doctor.add_argument(
        "--service",
        choices=[*SERVICES, "all"],
        default="all",
        help="Service to check (default: all)",
    )
    doctor.add_argument("--json", action="store_true", help="Emit JSON instead of text")
    doctor.add_argument(
        "--skip-live",
        action="store_true",
        help="Skip live vendor auth smoke tests",
    )
    doctor.add_argument("--timeout", type=float, default=30.0, help="Live check timeout seconds")
    doctor.add_argument("--year", type=int, help="Year to use for vendor view checks")
    doctor.add_argument(
        "--no-audit",
        action="store_true",
        help="Do not append a non-secret auth-check audit record",
    )

    register = auth_subparsers.add_parser(
        "register-ip",
        help="Record the public IP registered in a vendor portal",
    )
    register.add_argument("service", choices=SERVICES, help="Token service to update")
    register.add_argument(
        "--ip",
        action="append",
        help="Registered public IP. May be passed multiple times. Defaults to current host IP.",
    )
    register.add_argument("--entity-id", help="APM/ACO entity ID associated with this token")
    register.add_argument("--token-label", help="Human label for the token")
    register.add_argument("--replace", action="store_true", help="Replace existing IP list")
    register.add_argument("--notes", help="Operator note to store with the record")

    harden = auth_subparsers.add_parser(
        "harden",
        help="Create auth directories and tighten local credential permissions",
    )
    harden.add_argument("--dry-run", action="store_true", help="Show changes without applying")
    return parser


def _legacy_entity_id(service: str) -> str | None:
    keys = {"4icli": {"FOURICLI_APM_ID"}, "acoms": {"ACOMS_API_ID"}}[service]
    values = {key: os.getenv(key, "") for key in keys}
    try:
        deploy_dir = find_deploy_dir()
    except FileNotFoundError:
        deploy_dir = None
    if deploy_dir:
        values.update(read_env_file(deploy_dir / ".env", keys))
    for key in keys:
        if values.get(key):
            return values[key]
    return None


def _print_report(report) -> None:
    host = report.host_public_ip.value or f"unknown ({report.host_public_ip.error})"
    container = report.container_public_ip.value or f"unknown ({report.container_public_ip.error})"
    registered = ", ".join(report.registered_ips) if report.registered_ips else "(not recorded)"
    fingerprint = report.config.fingerprint or "(unavailable)"
    if len(fingerprint) > 24:
        fingerprint = fingerprint[:24] + "..."

    print(report.service)
    print(f"  Diagnosis:     {report.diagnosis}")
    print(f"  Entity ID:     {report.entity_id or '(not configured)'}")
    print(f"  Host IP:       {host}")
    print(f"  Container IP:  {container}")
    print(f"  Registered IP: {registered}")
    print(f"  Config:        {report.config.path}")
    print(
        "  Config state:  "
        + (
            f"{report.config.kind}, mode {report.config.mode}, owner "
            f"{report.config.owner}:{report.config.group}, mtime {report.config.mtime}"
            if report.config.exists
            else "missing"
        )
    )
    print(f"  Fingerprint:   {fingerprint}")
    if report.config.warning:
        print(f"  Warning:       {report.config.warning}")
    print(f"  Live check:    {report.live_check.status} - {report.live_check.message[:180]}")
    print()


def cmd_doctor(args) -> int:
    """Run auth diagnostics."""
    paths = get_auth_paths()
    registry = AuthRegistry(paths.registry)
    services = list(SERVICES) if args.service == "all" else [args.service]
    host_ip = fetch_public_ip(timeout=min(args.timeout, 8.0))
    reports = [
        check_service(
            service,
            paths=paths,
            registry=registry,
            host_public_ip=host_ip,
            skip_live=args.skip_live,
            timeout=args.timeout,
            year=args.year,
        )
        for service in services
    ]

    if not args.no_audit:
        write_audit_log(paths, reports)

    if args.json:
        print(json.dumps({"reports": [report.as_dict() for report in reports]}, indent=2))
    else:
        print("ACO Auth Doctor")
        print("=" * 80)
        print(f"Registry: {paths.registry}")
        print()
        for report in reports:
            _print_report(report)

    hard_failures = {
        "MISSING_CONFIG",
        "IP_MISMATCH",
        "BAD_SECRET",
        "TLS_OR_ZSCALER",
        "CONTAINER_DOWN",
    }
    return 1 if any(report.diagnosis in hard_failures for report in reports) else 0


def cmd_register_ip(args) -> int:
    """Record or update a service's registered public IP."""
    if args.ip:
        try:
            ips = parse_ip_values(args.ip)
        except ValueError as exc:
            print(f"[ERROR] Invalid IP address: {exc}")
            return 1
    else:
        probe = fetch_public_ip()
        if not probe.value:
            print(f"[ERROR] Could not determine current public IP: {probe.error}")
            return 1
        ips = [probe.value]

    paths = get_auth_paths()
    registry = AuthRegistry(paths.registry)
    entity_id = args.entity_id or _legacy_entity_id(args.service)
    record = registry.update(
        args.service,
        entity_id=entity_id,
        registered_ips=ips,
        replace_ips=args.replace,
        token_label=args.token_label,
        notes=args.notes,
    )

    print(f"[OK] Updated {paths.registry}")
    print(f"Service:        {args.service}")
    print(f"Entity ID:      {record.entity_id or '(not configured)'}")
    print(f"Registered IPs: {', '.join(record.registered_ips) or '(none)'}")
    return 0


def _chmod(path: Path, mode: int, *, dry_run: bool) -> None:
    if dry_run:
        print(f"would chmod {mode:04o} {path}")
    else:
        os.chmod(path, mode)
        print(f"chmod {mode:04o} {path}")


def cmd_harden(args) -> int:
    """Create auth directories and tighten credential permissions."""
    paths = get_auth_paths()
    directories = [
        paths.root,
        paths.secrets_dir,
        paths.secrets_dir / "4icli",
        paths.secrets_dir / "acoms",
        paths.acoms_config_dir,
        paths.logs_dir,
        paths.certs_dir,
    ]

    for directory in directories:
        if args.dry_run:
            print(f"would mkdir -p {directory}")
        else:
            directory.mkdir(parents=True, exist_ok=True)
        if directory.exists() or args.dry_run:
            _chmod(directory, 0o700, dry_run=args.dry_run)

    files = [
        paths.registry,
        paths.fouricli_config,
        paths.legacy_fouricli_config,
        paths.cert_bundle,
    ]
    try:
        deploy_dir = find_deploy_dir()
        files.append(deploy_dir / ".env")
    except FileNotFoundError:
        pass

    for file_path in files:
        if file_path.exists():
            _chmod(file_path, 0o600, dry_run=args.dry_run)

    if paths.secrets_dir.exists():
        for child in paths.secrets_dir.rglob("*"):
            _chmod(child, 0o700 if child.is_dir() else 0o600, dry_run=args.dry_run)

    print(
        "[OK] Auth filesystem hardening complete" if not args.dry_run else "[OK] Dry run complete"
    )
    return 0


def dispatch(args) -> int:
    """Dispatch parsed auth args."""
    if args.auth_command == "doctor":
        return cmd_doctor(args)
    if args.auth_command == "register-ip":
        return cmd_register_ip(args)
    if args.auth_command == "harden":
        return cmd_harden(args)
    return 1
