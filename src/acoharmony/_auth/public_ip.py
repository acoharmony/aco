"""Public IP probes for host and Docker containers."""

from __future__ import annotations

import ipaddress
import subprocess
import urllib.error
import urllib.request
from collections.abc import Iterable
from dataclasses import dataclass

PUBLIC_IP_URLS = (
    "https://api.ipify.org",
    "https://checkip.amazonaws.com",
    "https://ifconfig.me/ip",
    "https://icanhazip.com",
)


@dataclass(frozen=True)
class ProbeResult:
    """Result from a network probe."""

    value: str | None
    source: str | None = None
    error: str | None = None
    values: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        return self.value is not None and self.error is None

    @property
    def observed_values(self) -> tuple[str, ...]:
        """All unique IPs observed by the probe, preserving provider order."""
        if self.values:
            return self.values
        if self.value:
            return (self.value,)
        return ()


def normalize_ip(value: str) -> str:
    """Return a canonical IPv4/IPv6 string or raise ValueError."""
    return str(ipaddress.ip_address(value.strip()))


def parse_ip_values(values: Iterable[str | None] | str | None) -> list[str]:
    """Parse comma/space separated IP values, preserving unique valid entries."""
    if values is None:
        values = []
    elif isinstance(values, str):
        values = [values]

    parsed: list[str] = []
    for value in values:
        if not value:
            continue
        for token in value.replace(",", " ").split():
            ip = normalize_ip(token)
            if ip not in parsed:
                parsed.append(ip)
    return parsed


def fetch_public_ip(timeout: float = 5.0) -> ProbeResult:
    """Fetch observed host public IPs using multiple simple HTTPS services."""
    errors: list[str] = []
    values: list[str] = []
    first_source: str | None = None
    for url in PUBLIC_IP_URLS:
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                text = response.read().decode("utf-8", errors="replace").strip()
            ip = normalize_ip(text)
            if not first_source:
                first_source = url
            if ip not in values:
                values.append(ip)
        except (OSError, ValueError, urllib.error.URLError) as exc:
            errors.append(f"{url}: {exc}")
    if values:
        return ProbeResult(value=values[0], source=first_source, values=tuple(values))
    return ProbeResult(value=None, error="; ".join(errors))


def fetch_container_public_ip(container: str, timeout: float = 8.0) -> ProbeResult:
    """Fetch observed public IPs from inside a running container using curl."""
    urls = " ".join(PUBLIC_IP_URLS)
    script = (
        "set -eu; "
        "if ! command -v curl >/dev/null 2>&1; then "
        "echo 'curl not installed in container' >&2; exit 127; fi; "
        "found=0; "
        f"for url in {urls}; do "
        'ip=$(curl -fsS --max-time 5 "$url" 2>/dev/null || true); '
        'if [ -n "$ip" ]; then echo "$url $ip"; found=1; fi; '
        "done; "
        '[ "$found" -eq 1 ] && exit 0; '
        "echo 'all public IP probes failed' >&2; exit 1"
    )

    try:
        result = subprocess.run(
            ["docker", "exec", container, "sh", "-lc", script],
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except FileNotFoundError:
        return ProbeResult(value=None, error="docker command not found")
    except subprocess.TimeoutExpired:
        return ProbeResult(value=None, error=f"timed out after {timeout:g}s")

    if result.returncode != 0:
        message = (result.stderr or result.stdout or "docker exec failed").strip()
        return ProbeResult(value=None, error=message)

    values: list[str] = []
    first_source: str | None = None
    for line in result.stdout.splitlines():
        output = line.strip().split()
        if len(output) < 2:
            continue
        try:
            ip = normalize_ip(output[-1])
        except ValueError as exc:
            return ProbeResult(value=None, error=str(exc))
        if not first_source:
            first_source = output[0]
        if ip not in values:
            values.append(ip)

    if values:
        return ProbeResult(value=values[0], source=first_source, values=tuple(values))

    return ProbeResult(value=None, error="container did not return an IP address")
