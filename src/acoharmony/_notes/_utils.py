# © 2025 HarmonyCares
# All rights reserved.

"""General utility functions for notebooks (formatting, parsing, exports)."""

from __future__ import annotations

from io import BytesIO

import polars as pl

from ._base import PluginRegistry


class UtilityPlugins(PluginRegistry):
    """Stateless helpers for notebooks."""

    @staticmethod
    def format_size(size_bytes: int | float) -> str:
        size = float(size_bytes)
        for unit in ("B", "KB", "MB", "GB", "TB"):
            if size < 1024.0:
                return f"{size:.1f} {unit}"
            size /= 1024.0
        return f"{size:.1f} PB"

    @staticmethod
    def parse_input_list(input_text: str | None, delimiter: str = ",") -> list[str]:
        """Parse a delimited string into a clean list of trimmed values."""
        if not input_text or not input_text.strip():
            return []
        return [item.strip() for item in input_text.split(delimiter) if item.strip()]

    @staticmethod
    def create_multi_sheet_excel(
        sheets: dict[str, pl.DataFrame],
        filename: str | None = None,  # noqa: ARG004 (interface compat)
    ) -> bytes:
        """First sheet only — polars hasn't shipped multi-sheet writes yet."""
        buf = BytesIO()
        first = next(iter(sheets))
        sheets[first].write_excel(buf, worksheet=first)
        buf.seek(0)
        return buf.getvalue()
