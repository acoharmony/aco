# © 2025 HarmonyCares
# All rights reserved.

"""
Lightweight loader for acoharmony's packaged TOML configuration.

The config file ships with the wheel at ``acoharmony/_config/aco.toml`` and
is resolved via ``Path(__file__).parent`` so the same code path works for
both editable and non-editable installs. This module deliberately avoids
``importlib.resources`` to sidestep the import-time chicken-and-egg that
occurs when callers are themselves inside the ``acoharmony`` package and
``acoharmony.__init__`` has not finished executing yet.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import tomllib
except ImportError:  # pragma: no cover – Python < 3.11 fallback
    import tomli as tomllib  # type: ignore[import-not-found, no-redef]


# The config file lives next to this module, at
# ``src/acoharmony/_config/aco.toml``. ``__file__`` here is
# ``.../acoharmony/_config_loader.py`` so ``parent`` is the package root
# and ``_config/aco.toml`` is the packaged file.
_CONFIG_PATH: Path = Path(__file__).parent / "_config" / "aco.toml"


def get_config_path() -> Path:
    """Return the absolute path to the packaged ``aco.toml`` file."""
    return _CONFIG_PATH


def load_aco_config() -> dict[str, Any]:
    """
    Load and return the full parsed ``aco.toml`` configuration.

    Raises
    ------
    FileNotFoundError
        If ``aco.toml`` is missing from the installed package. This should
        never happen with a properly built wheel; it indicates a corrupt
        install or a build that failed to include package data.
    """
    if not _CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"aco.toml not found at {_CONFIG_PATH}. "
            "This indicates a corrupt acoharmony install."
        )
    with _CONFIG_PATH.open("rb") as f:
        return tomllib.load(f)


def get_aco_identity(apm_id: str | None = None) -> dict[str, Any]:
    """
    Resolve the operating ACO entity identity from ``aco.toml``.

    Reads the ``[aco_identity]`` section. If ``apm_id`` is omitted the active
    ACO from ``aco_identity.apm_id`` is used. Returns the matching directory
    row (``apm_id``, ``tin``, ``legal_business_name``, ``program``).

    This is the *only* place ACO entity constants live. Code paths that need
    to stamp entity columns onto rows lacking them (e.g. internal exports
    that ship without entity TIN/name) should call this loader rather than
    hardcoding any of those values.

    Raises
    ------
    KeyError
        If no directory entry matches the requested ``apm_id``.
    """
    config = load_aco_config()
    identity = config.get("aco_identity", {})
    active_id = apm_id or identity.get("apm_id")
    directory = identity.get("directory", []) or []

    for row in directory:
        if row.get("apm_id") == active_id:
            return dict(row)

    raise KeyError(
        f"No aco_identity.directory entry found for apm_id={active_id!r}. "
        "Add a [[aco_identity.directory]] row in aco.toml."
    )
