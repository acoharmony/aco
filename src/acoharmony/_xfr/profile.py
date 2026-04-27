# © 2025 HarmonyCares
# All rights reserved.

"""
Transfer profile primitives: source rules, verifiers, and the registry.

A ``TransferProfile`` declares one transfer use case end-to-end:
which files to consider, where they go, what to call them when they
arrive, and how to confirm they got there. Profiles are defined as
classes under ``_xfr/_profiles/`` and discovered by the registry.

Source rules (``SourceRule``) decide which files in the source directory
are candidates. The default ``MonthlyMatchRule`` reuses the schema
registry: it picks any file matching a schema whose 4icli
``refreshFrequency`` is ``"monthly"``, optionally filtered by a date
floor parsed from the filename's ``D<YYMMDD>`` token. That covers HDAI
(monthly CCLF + BAR) without hardcoding either.

Verifiers answer "did the file actually arrive?" — pluggable so HDAI
can parse the SFTP tool's log while internal profiles just check the
destination directory.
"""

from __future__ import annotations

import fnmatch
import re
from dataclasses import dataclass, field
from datetime import date, datetime
from pathlib import Path
from typing import Callable, Protocol


# ---------------------------------------------------------------------------
# Source rules
# ---------------------------------------------------------------------------


class SourceRule(Protocol):
    """Decides which files in the source directory are candidates."""

    def matches(self, filename: str) -> bool: ...  # pragma: no cover

    def applicable_filenames(self, source_dir: Path) -> list[str]:  # pragma: no cover
        """List candidate filenames in ``source_dir`` matching this rule."""
        ...


@dataclass
class SchemaPatternRule:
    """
    Source rule: include files whose name matches a registered schema's
    ``metadata.file_patterns``, scoped to a caller-chosen set of schemas
    and (optionally) a per-schema set of pattern keys.

    The registry remains the single source of truth for the patterns
    themselves — this rule only decides which schemas count for this
    transfer, so adding files for a new program (e.g. MSSP CCLF) is
    one new entry here, not a new copy of any patterns.

    ``schemas`` accepts:
      * ``"name"`` — include every pattern key for that schema.
      * ``("name", ("key1", "key2"))`` — restrict to specific keys
        (e.g. ``("cclf0", ("reach_monthly", "mssp_monthly"))`` to drop
        the weekly variants).

    ``date_floor`` accepts ``None`` (no floor), ``"month_start"`` (1st
    of the current month at call time), or a ``date``. Filenames with
    no parseable ``D<YYMMDD>`` token are not rejected by the floor —
    we'd rather over-include than silently drop something.
    """

    schemas: tuple[str | tuple[str, tuple[str, ...]], ...] = ()
    date_floor: date | str | None = "month_start"

    _D_TOKEN = re.compile(r"\.D(\d{6})\.")

    def _resolve_floor(self) -> date | None:
        if self.date_floor is None:
            return None
        if isinstance(self.date_floor, date):
            return self.date_floor
        if self.date_floor == "month_start":
            today = datetime.now().date()
            return today.replace(day=1)
        raise ValueError(f"Unrecognized date_floor: {self.date_floor!r}")

    def _patterns(self) -> list[str]:
        """Collect the configured patterns from the schema registry."""
        # Force every schema module to register itself before we query.
        # ``_tables/__init__.py`` re-exports each schema dataclass, so
        # importing the package triggers all decorator registrations.
        import acoharmony._tables  # noqa: F401  (registers all schemas)
        from acoharmony._registry import SchemaRegistry

        registry = SchemaRegistry()
        patterns: list[str] = []
        for spec in self.schemas:
            if isinstance(spec, str):
                schema_name = spec
                key_filter: tuple[str, ...] | None = None
            else:
                schema_name, key_filter = spec
            metadata = registry.get_metadata(schema_name) or {}
            file_patterns = metadata.get("file_patterns") or {}
            if key_filter is not None and isinstance(file_patterns, dict):
                file_patterns = {k: v for k, v in file_patterns.items() if k in key_filter}
            patterns.extend(_flatten_patterns(file_patterns))
        return patterns

    def matches(self, filename: str) -> bool:
        # Convert CMS shell-style ``?`` to fnmatch's ``?`` (already same)
        # and strip any whitespace inside comma-separated pattern groups.
        all_patterns: list[str] = []
        for p in self._patterns():
            for piece in p.split(","):
                piece = piece.strip()
                if piece:
                    all_patterns.append(piece)
        if not any(fnmatch.fnmatchcase(filename, p) for p in all_patterns):
            return False
        floor = self._resolve_floor()
        if floor is None:
            return True
        token_date = _extract_d_token_date(filename)
        if token_date is None:
            return True
        return token_date >= floor

    def applicable_filenames(self, source_dir: Path | list[Path]) -> list[str]:
        """
        List candidate filenames across one or more source directories.

        Multiple roots are supported because the same file can live in
        either ``bronze`` (just-downloaded) or ``archive`` (post-unpack)
        depending on pipeline timing — both are legitimate sources for
        downstream transfer. If the same basename exists in more than
        one root, the first occurrence (in the order given) wins.
        """
        roots = [source_dir] if isinstance(source_dir, Path) else list(source_dir)
        seen: set[str] = set()
        out: list[str] = []
        for root in roots:
            if not root.exists():
                continue
            for p in root.iterdir():
                if not p.is_file():
                    continue
                name = p.name
                if name in seen or not self.matches(name):
                    continue
                seen.add(name)
                out.append(name)
        return sorted(out)


@dataclass
class LiteralPatternRule:
    """
    Source rule for files that aren't fronted by a schema — pure
    transport artifacts like the CCLF bundle zip.

    ``patterns`` are fnmatch-style strings applied directly to filenames.
    Same date-floor semantics as ``SchemaPatternRule``: filenames with
    no ``D<YYMMDD>`` token are not rejected by the floor.
    """

    patterns: tuple[str, ...] = ()
    date_floor: date | str | None = "month_start"

    def _resolve_floor(self) -> date | None:
        if self.date_floor is None:
            return None
        if isinstance(self.date_floor, date):
            return self.date_floor
        if self.date_floor == "month_start":
            today = datetime.now().date()
            return today.replace(day=1)
        raise ValueError(f"Unrecognized date_floor: {self.date_floor!r}")

    def matches(self, filename: str) -> bool:
        if not any(fnmatch.fnmatchcase(filename, p) for p in self.patterns):
            return False
        floor = self._resolve_floor()
        if floor is None:
            return True
        token_date = _extract_d_token_date(filename)
        if token_date is None:
            return True
        return token_date >= floor

    def applicable_filenames(self, source_dir: Path | list[Path]) -> list[str]:
        roots = [source_dir] if isinstance(source_dir, Path) else list(source_dir)
        seen: set[str] = set()
        out: list[str] = []
        for root in roots:
            if not root.exists():
                continue
            for p in root.iterdir():
                if not p.is_file():
                    continue
                name = p.name
                if name in seen or not self.matches(name):
                    continue
                seen.add(name)
                out.append(name)
        return sorted(out)


@dataclass
class CompositeRule:
    """OR-combine multiple rules. A filename passes if any rule accepts it."""

    rules: tuple[SourceRule, ...] = ()

    def matches(self, filename: str) -> bool:
        return any(r.matches(filename) for r in self.rules)

    def applicable_filenames(self, source_dir: Path | list[Path]) -> list[str]:
        seen: set[str] = set()
        out: list[str] = []
        for rule in self.rules:
            for name in rule.applicable_filenames(source_dir):
                if name not in seen:
                    seen.add(name)
                    out.append(name)
        return sorted(out)


# Back-compat alias: the original public name. ``MonthlyMatchRule`` was
# briefly exported as the default rule type; ``SchemaPatternRule`` is the
# accurate name now that schemas opt-in by listing.
MonthlyMatchRule = SchemaPatternRule


def _flatten_patterns(file_patterns: dict | list | str) -> list[str]:
    """Schemas store file_patterns as either a string, a list, or a dict-of-strings/lists."""
    if isinstance(file_patterns, str):
        return [file_patterns]
    if isinstance(file_patterns, list):
        return [p for p in file_patterns if isinstance(p, str)]
    if isinstance(file_patterns, dict):
        out: list[str] = []
        for v in file_patterns.values():
            out.extend(_flatten_patterns(v))
        return out
    return []


def _extract_d_token_date(filename: str) -> date | None:
    """Parse the ``D<YYMMDD>`` token CMS uses for file generation date."""
    match = MonthlyMatchRule._D_TOKEN.search(filename)
    if not match:
        return None
    raw = match.group(1)
    try:
        year = 2000 + int(raw[0:2])
        month = int(raw[2:4])
        day = int(raw[4:6])
        return date(year, month, day)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Verifiers
# ---------------------------------------------------------------------------


class Verifier(Protocol):
    """Resolves the destination-side state for a filename."""

    def state_for(self, filename: str) -> str | None:  # pragma: no cover
        """
        Return ``"sent"`` / ``"archived"`` / etc. if the verifier can
        confirm the file reached the destination, or ``None`` if the
        verifier has no signal for this file.
        """
        ...


@dataclass
class LogVerifier:
    """
    Verifier that parses an SFTP-style log (Mabel/HDAI format) and
    treats ``Upload file ... to <upload_dest_prefix><basename>.`` lines
    as authoritative "this filename left us."

    Reuses ``acoharmony._parsers._mabel_log.parse_mabel_log`` — same
    parser the SVA dashboard uses — so the log format is shared code.

    The set of uploaded basenames is cached on the instance so a
    selector pass that calls ``state_for`` once per candidate parses
    the log exactly once. Mount/IO errors on the log path don't crash
    the caller — they're treated as "no signal."
    """

    log_path: Path
    upload_dest_prefix: str = ""

    def __post_init__(self) -> None:
        self._cache: set[str] | None = None

    def _uploaded_basenames(self) -> set[str]:
        if self._cache is not None:
            return self._cache
        # Local import: keep _xfr import-light at package load.
        import polars as pl

        from acoharmony._parsers._mabel_log import parse_mabel_log

        try:
            if not self.log_path.exists():
                self._cache = set()
                return self._cache
            df = parse_mabel_log(self.log_path).collect()
        except OSError:
            # Mount flake / transient IO — don't fail the whole run; we
            # just lose the "sent" signal for this pass and everything
            # appears as in_flight or pending instead.
            self._cache = set()
            return self._cache
        if df.height == 0:
            self._cache = set()
            return self._cache
        uploads = df.filter(pl.col("event_type") == "upload")
        if uploads.height == 0:
            self._cache = set()
            return self._cache
        rows = uploads.select("filename", "destination_path").to_dicts()
        prefix = self.upload_dest_prefix
        out: set[str] = set()
        for row in rows:
            dest = row.get("destination_path") or ""
            if prefix and not dest.startswith(prefix):
                continue
            name = row.get("filename")
            if name:
                out.add(name)
        self._cache = out
        return out

    def state_for(self, filename: str) -> str | None:
        return "sent" if filename in self._uploaded_basenames() else None


@dataclass
class DirectoryVerifier:
    """
    Verifier that checks the destination directory directly. Suitable
    for internal-folder profiles where there's no upload log.
    """

    destination: Path

    def state_for(self, filename: str) -> str | None:
        if not self.destination.exists():
            return None
        return "placed" if (self.destination / filename).exists() else None


# ---------------------------------------------------------------------------
# Profile + registry
# ---------------------------------------------------------------------------


@dataclass
class TransferProfile:
    """
    Declarative spec for one transfer use case.

    Fields:
        name: Profile identifier used on the CLI (``aco xfr send hdai``).
        description: Human-facing explanation, surfaced by ``aco xfr list``.
        source_dirs: One or more directories to read files from, in
            priority order. The same file (by basename) can appear in
            bronze and/or archive depending on pipeline timing — both
            are valid sources. The first match wins.
        destination: Where to drop files. Always a literal Path —
            destinations are usually outside the workspace.
        source_rule: Decides which filenames are in scope.
        rename: Optional ``filename -> filename`` callable for profiles
            that need to rewrite names (internal human-readable layout).
            ``None`` preserves the original.
        verifier: Optional destination-side state lookup.
        archive_dir: Optional secondary directory the destination tool
            may move files to after a successful upload — used only by
            ``cmd_status`` to surface ``archived`` state.
    """

    name: str
    description: str
    source_dirs: tuple[Path, ...]
    destination: Path
    source_rule: SourceRule
    rename: Callable[[str], str] | None = None
    verifier: Verifier | None = None
    archive_dir: Path | None = None

    def dest_filename(self, filename: str) -> str:
        return self.rename(filename) if self.rename else filename

    def find_source_path(self, filename: str) -> Path | None:
        """First source directory that holds ``filename``, or None."""
        for root in self.source_dirs:
            candidate = root / filename
            if candidate.is_file():
                return candidate
        return None


_REGISTRY: dict[str, TransferProfile] = {}


def register_profile(profile: TransferProfile) -> TransferProfile:
    """
    Register a transfer profile under ``profile.name``. Re-registration
    with the same name overwrites silently — supports module reloads in
    notebooks/test runs.
    """
    _REGISTRY[profile.name] = profile
    return profile


def resolve_profile(name: str) -> TransferProfile:
    """Look up a registered profile by name. Raises ``KeyError`` if missing."""
    _ensure_profiles_imported()
    if name not in _REGISTRY:
        raise KeyError(
            f"Unknown xfr profile {name!r}. Known profiles: {sorted(_REGISTRY)}"
        )
    return _REGISTRY[name]


def list_profiles() -> list[TransferProfile]:
    """Return all registered profiles, sorted by name."""
    _ensure_profiles_imported()
    return [_REGISTRY[k] for k in sorted(_REGISTRY)]


def _ensure_profiles_imported() -> None:
    """Trigger discovery of every profile module under ``_xfr/_profiles/``."""
    import importlib
    import pkgutil

    from . import _profiles

    for module_info in pkgutil.iter_modules(_profiles.__path__):
        importlib.import_module(f"{_profiles.__name__}.{module_info.name}")


def _reset_registry_for_tests() -> None:
    """Clear the registry. Test-only — never call from production code."""
    _REGISTRY.clear()
