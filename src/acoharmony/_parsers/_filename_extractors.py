# © 2025 HarmonyCares
# All rights reserved.

"""
Registry of filename metadata extractors.

Parsers often need to stamp global metadata onto every row — fields
like ``aco_id``, ``performance_year``, ``program`` that live in the
filename itself rather than the workbook contents. Letting each parser
hardcode its own extraction calls produces duplicate code and makes the
set of "derivable filename fields" opaque.

This module exposes a small decorator-based registry. Parsers declare
what to extract in the schema:

    filename_fields=[
        {"name": "aco_id", "extractor": "aco_id"},
        {"name": "performance_year", "extractor": "performance_year_from_py"},
    ]

At parse time the generic excel_multi_sheet parser looks up each
extractor name, applies it to the source filename, and stamps the
result as a column on every row — overwriting any same-named column
from the workbook content. Filename is the authoritative source; any
workbook cell that claims to carry the same field is second-class.

Extractors are pure ``str | None -> str | None`` callables. They MUST
handle ``None`` input gracefully (return ``None``) and MUST be total
on any valid filename (return ``None`` rather than raise). Extractors
that cannot guarantee a value across all filenames should return
``None`` for unrecognized inputs rather than guess.
"""

from __future__ import annotations

import re
from typing import Callable

from ._aco_id import (
    extract_aco_id,
    extract_program_from_filename,
)


FilenameExtractor = Callable[[str | None], str | None]

_REGISTRY: dict[str, FilenameExtractor] = {}


def register_filename_extractor(
    name: str,
) -> Callable[[FilenameExtractor], FilenameExtractor]:
    """
    Register a filename extractor under ``name``.

    Usage::

        @register_filename_extractor("performance_year_from_py")
        def extract_performance_year_from_py(filename: str | None) -> str | None:
            ...
    """

    def decorator(fn: FilenameExtractor) -> FilenameExtractor:
        if name in _REGISTRY:
            raise ValueError(
                f"Filename extractor {name!r} already registered "
                f"(existing: {_REGISTRY[name]!r}, new: {fn!r})"
            )
        _REGISTRY[name] = fn
        return fn

    return decorator


def get_filename_extractor(name: str) -> FilenameExtractor:
    """
    Resolve an extractor by name. Raises ``ValueError`` if unknown so
    schema typos surface loudly at parse time.
    """
    if name not in _REGISTRY:
        available = sorted(_REGISTRY.keys())
        raise ValueError(
            f"Unknown filename extractor {name!r}. "
            f"Available: {available}"
        )
    return _REGISTRY[name]


def list_filename_extractors() -> list[str]:
    """Return all registered extractor names, sorted."""
    return sorted(_REGISTRY.keys())


# ---------------------------------------------------------------------------
# Built-in extractors
# ---------------------------------------------------------------------------


@register_filename_extractor("aco_id")
def _aco_id(filename: str | None) -> str | None:
    """ACO identifier (D0259, A2671, etc.) — delegates to extract_aco_id."""
    if not filename:
        return None
    return extract_aco_id(filename)


@register_filename_extractor("program")
def _program(filename: str | None) -> str | None:
    """Program type (REACH / MSSP) — delegates to extract_program_from_filename."""
    if not filename:
        return None
    return extract_program_from_filename(filename)


# Performance-year patterns across known CMS filename conventions.
# Order matters: most-specific first.
_PERFORMANCE_YEAR_PATTERNS: list[re.Pattern[str]] = [
    # BNMR: REACH.D0259.BNMR.PY2024....xlsx  → 2024
    re.compile(r"\.PY(\d{4})\.", re.IGNORECASE),
    # BAR (REACH): P.D0259.ALGC24.RP....      → 2024 (2-digit year + 2000)
    re.compile(r"\.ALG[CR](\d{2})\.", re.IGNORECASE),
    # MSSP quarterly: P.A1234.ACO.QALR.2024Q1...
    re.compile(r"\.QALR\.(\d{4})Q\d", re.IGNORECASE),
    # MSSP annual: P.A1234.ACO.AALR.Y2022...
    re.compile(r"\.AALR\.Y(\d{4})", re.IGNORECASE),
]


@register_filename_extractor("performance_year")
def _performance_year(filename: str | None) -> str | None:
    """
    Performance year as a 4-digit string.

    Tries several known CMS encodings: BNMR's ``PY2024`` marker, the BAR
    ``ALGC24`` two-digit year (expanded to 2000+YY), MSSP quarterly
    ``QALR.2024Q1``, and MSSP annual ``AALR.Y2022``. Returns ``None`` if
    no pattern matches.
    """
    if not filename:
        return None
    for pattern in _PERFORMANCE_YEAR_PATTERNS:
        m = pattern.search(filename)
        if not m:
            continue
        year_str = m.group(1)
        if len(year_str) == 2:
            # 2-digit year assumed 21st century
            return str(2000 + int(year_str))
        return year_str
    return None
