# © 2025 HarmonyCares
# All rights reserved.

"""
Citation corpus analytics for the citations notebook.

Reads ``CiteStateTracker.get_processed_files()`` rows + the master
``corpus.parquet`` and produces dashboard-ready frames: counts by type
and domain, recent rollup, tagged-only filter, and Federal Register
parent/children grouping by document_number.

Notebooks call these directly — no inline polars or list-comp logic
in cells.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any

import polars as pl

from ._base import PluginRegistry

CONTENT_COLS = (
    "html_content",
    "text_content",
    "links",
    "structured_data",
    "content",
    "abstract",
)


class CitePlugins(PluginRegistry):
    """Citation corpus DataFrame builders + Federal Register grouping."""

    # ---- inventory rollup ------------------------------------------------

    def state_summary(self, processed_files: list) -> dict[str, Any]:
        """Counts + per-row metadata lists from a state-tracker file list."""
        types: list[str] = []
        domains: list[str] = []
        tags: list[str] = []
        notes: list[str] = []
        for row in processed_files:
            md = row.metadata or {}
            types.append(md.get("citation_type", "unknown"))
            domains.append(md.get("url_domain", "unknown"))
            row_tags = md.get("tags") or []
            if row_tags:
                tags.extend(row_tags)
            note = md.get("note", "")
            if note:
                notes.append(note)
        return {
            "total": len(processed_files),
            "citation_types": types,
            "domains": domains,
            "tags": tags,
            "notes": notes,
            "unique_types": len(set(types)),
            "unique_domains": len(set(domains)),
            "tagged_count": len(tags),
            "note_count": len(notes),
        }

    def state_dataframe(self, processed_files: list) -> pl.DataFrame:
        """One row per processed file with the public-facing columns."""
        records = []
        for row in processed_files:
            md = row.metadata or {}
            records.append(
                {
                    "file_path": str(row.source_path),
                    "url": md.get("url", ""),
                    "url_domain": md.get("url_domain", ""),
                    "title": md.get("title", ""),
                    "citation_type": md.get("citation_type", ""),
                    "doi": md.get("doi", ""),
                    "source_type": row.source_type,
                    "record_count": row.record_count or 0,
                    "processed_at": (
                        row.process_timestamp.isoformat()
                        if row.process_timestamp
                        else ""
                    ),
                    "note": md.get("note", ""),
                    "tags": ", ".join(md.get("tags") or []),
                }
            )
        return pl.DataFrame(records)

    # ---- per-axis rollups -----------------------------------------------

    def by_type(self, citations_df: pl.DataFrame) -> pl.DataFrame:
        return (
            citations_df.group_by("citation_type")
            .agg(
                pl.len().alias("count"),
                pl.col("url_domain").n_unique().alias("unique_domains"),
            )
            .sort("count", descending=True)
        )

    def by_domain(self, citations_df: pl.DataFrame) -> pl.DataFrame:
        return (
            citations_df.group_by("url_domain")
            .agg(
                pl.len().alias("count"),
                pl.col("citation_type").n_unique().alias("unique_types"),
            )
            .sort("count", descending=True)
        )

    def recent(self, citations_df: pl.DataFrame, limit: int = 20) -> pl.DataFrame:
        return (
            citations_df.sort("processed_at", descending=True)
            .head(limit)
            .select(
                "processed_at",
                "title",
                "citation_type",
                "url_domain",
                "note",
                "tags",
            )
        )

    def tagged_only(self, citations_df: pl.DataFrame) -> pl.DataFrame:
        return (
            citations_df.filter(pl.col("tags") != "")
            .select("title", "citation_type", "tags", "note", "processed_at")
            .sort("processed_at", descending=True)
        )

    # ---- master corpus + Federal Register grouping ---------------------

    def load_master_corpus(
        self, corpus_path: Path
    ) -> tuple[pl.DataFrame, pl.DataFrame]:
        """
        Return ``(editor_view, full_view)``.

        ``editor_view`` strips heavy content columns so the data editor
        stays responsive; ``full_view`` retains them for content lookup.
        Both are empty DataFrames when the corpus parquet doesn't exist.
        """
        if not Path(corpus_path).exists():
            return pl.DataFrame(), pl.DataFrame()
        full = pl.read_parquet(str(corpus_path))
        edit_cols = [c for c in full.columns if c not in CONTENT_COLS]
        return full.select(edit_cols), full

    def federal_register_only(self, full_corpus: pl.DataFrame) -> pl.DataFrame:
        if full_corpus.is_empty() or "citation_type" not in full_corpus.columns:
            return pl.DataFrame()
        return full_corpus.filter(
            pl.col("citation_type").str.contains("federal_register")
        )

    def group_by_final_rule(
        self, fr_citations: pl.DataFrame
    ) -> dict[str, dict[str, Any]]:
        """``{document_number: {parent, children, total_count}}``."""
        if fr_citations.is_empty() or "document_number" not in fr_citations.columns:
            return {}
        out: dict[str, dict[str, Any]] = {}
        for doc_num in fr_citations["document_number"].drop_nulls().unique().to_list():
            doc = fr_citations.filter(pl.col("document_number") == doc_num)
            parent = doc.filter(pl.col("is_parent_citation") == True)  # noqa: E712
            children = doc.filter(pl.col("is_parent_citation") == False)  # noqa: E712
            if "paragraph_number" in children.columns:
                children = children.sort("paragraph_number")
            elif "source_url" in children.columns:
                children = children.sort("source_url")
            out[doc_num] = {
                "parent": parent,
                "children": children,
                "total_count": doc.height,
            }
        return out

    def lookup_full_record(
        self,
        full_corpus: pl.DataFrame,
        url_hash: str | None,
    ) -> pl.DataFrame:
        """Get the heavy-content row for an editor record (empty if not found)."""
        if (
            full_corpus.is_empty()
            or url_hash is None
            or "url_hash" not in full_corpus.columns
        ):
            return pl.DataFrame()
        return full_corpus.filter(pl.col("url_hash") == url_hash)

    def replace_record(
        self,
        full_corpus: pl.DataFrame,
        url_hash: str,
        edited_row: pl.DataFrame,
    ) -> pl.DataFrame:
        """
        Splice ``edited_row`` (metadata edits) back over the matching
        ``url_hash`` row, preserving the heavy content columns from the
        original. Returns the new full DataFrame ready to ``write_parquet``.
        """
        if "url_hash" not in full_corpus.columns:
            return full_corpus
        keep_content = [c for c in CONTENT_COLS if c in full_corpus.columns]
        original_content = full_corpus.filter(pl.col("url_hash") == url_hash).select(
            keep_content
        )
        updated_row = (
            pl.concat([edited_row, original_content], how="horizontal")
            if not original_content.is_empty()
            else edited_row
        )
        others = full_corpus.filter(pl.col("url_hash") != url_hash)
        return pl.concat([others, updated_row])
