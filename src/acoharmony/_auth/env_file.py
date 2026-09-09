"""Small helpers for deploy/.env files.

These helpers intentionally avoid shell-style expansion. The deploy env files
used by the local compose stack are flat KEY=VALUE files, and treating them as
plain data keeps secrets from being executed accidentally.
"""

from __future__ import annotations

from pathlib import Path


def find_deploy_dir(required_relative: str | None = None, start: Path | None = None) -> Path:
    """Find the repo's deploy/ directory by walking upward."""
    here = (start or Path(__file__).resolve()).resolve()
    for parent in here.parents:
        candidate = parent / "deploy"
        if not candidate.exists():
            continue
        if required_relative and not (candidate / required_relative).exists():
            continue
        if not required_relative and not (
            (candidate / ".env").exists() or (candidate / "docker-compose.yml").exists()
        ):
            continue
        return candidate
    raise FileNotFoundError("Could not locate deploy/ directory from this checkout.")


def parse_env_value(raw: str) -> str:
    """Parse a simple .env value, trimming optional matching quotes."""
    value = raw.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def read_env_file(env_path: Path, keys: set[str] | None = None) -> dict[str, str]:
    """Read simple KEY=VALUE pairs from an env file."""
    values: dict[str, str] = {}
    if not env_path.exists():
        return values

    for line in env_path.read_text().splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, value = stripped.partition("=")
        key = key.strip()
        if not key:
            continue
        if keys is not None and key not in keys:
            continue
        values[key] = parse_env_value(value)
    return values


def update_env_file(env_path: Path, updates: dict[str, str]) -> None:
    """Rewrite env_path with updates applied, preserving comments and order."""
    lines = env_path.read_text().splitlines() if env_path.exists() else []
    seen: set[str] = set()
    new_lines: list[str] = []
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0].strip()
            if key in updates:
                new_lines.append(f"{key}={updates[key]}")
                seen.add(key)
                continue
        new_lines.append(line)

    for key, value in updates.items():
        if key not in seen:
            new_lines.append(f"{key}={value}")

    env_path.write_text("\n".join(new_lines) + "\n")


def mask_secret(secret: str) -> str:
    """Render a secret as a short, non-sensitive display value."""
    if not secret:
        return "(unset)"
    if len(secret) <= 8:
        return "..."
    return f"{secret[:4]}...{secret[-4:]}"
