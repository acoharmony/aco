# © 2025 HarmonyCares
# All rights reserved.

"""
SVA log analysis expressions.

Provides Polars expressions for analyzing parsed Mabel SFTP log data from
the REACH SVA (Specialist Verification of Attribution) document transfer
service. These expressions classify events, extract patient names from
uploaded filenames, and compute session-level flags.

Expressions
===========

Event Classification
--------------------
- ``is_upload``: True for file upload events
- ``is_connection_event``: True for connection/disconnect events
- ``has_uploads``: Session-level flag for sessions with file transfers

Upload Metadata Extraction
--------------------------
- ``patient_name``: Extracted patient name (title case) from SVA filename conventions
- ``patient_name_key``: Canonicalized name key for duplicate detection
- ``submission_date_str``: Extracted date string from SVA filename
- ``is_sva_form``: True if the uploaded file follows SVA naming conventions

Session Health
--------------
- ``auth_succeeded``: True if session authentication succeeded
- ``disconnected_cleanly``: True if session ended with clean disconnect

Duplicate Detection
-------------------
- ``find_duplicate_patients``: Pairwise bigram Jaccard similarity on patient names
"""

from itertools import combinations

import polars as pl

from ._registry import register_expression


def _token_similarity(a: str, b: str) -> float:
    """Jaccard similarity on character bigrams (0.0–1.0)."""
    if not a or not b:
        return 0.0
    bg_a = {a[i : i + 2] for i in range(len(a) - 1)} if len(a) > 1 else {a}
    bg_b = {b[i : i + 2] for i in range(len(b) - 1)} if len(b) > 1 else {b}
    # After the empty-string guard above, bg_a and bg_b are guaranteed non-empty:
    # len>=2 produces at least one bigram; len==1 produces {a} with one element.
    return len(bg_a & bg_b) / len(bg_a | bg_b)


@register_expression(
    "sva_log",
    schemas=["bronze", "silver"],
    callable=True,
    dataset_types=["log"],
    description="SVA Mabel SFTP log analysis expressions",
)
class SvaLogExpression:
    """Expression builders for SVA Mabel log analysis."""

    @staticmethod
    def is_upload() -> pl.Expr:
        """True for file upload event rows."""
        return pl.col("event_type") == "upload"

    @staticmethod
    def is_connection_event() -> pl.Expr:
        """True for connection or disconnect events."""
        return pl.col("event_type").is_in(["connection", "disconnect"])

    @staticmethod
    def auth_succeeded() -> pl.Expr:
        """True for rows where the message indicates authentication success."""
        return pl.col("message").str.contains("Authentication succeeded")

    @staticmethod
    def disconnected_cleanly() -> pl.Expr:
        """True for rows indicating a clean SFTP disconnect."""
        return pl.col("message").str.contains("SFTP connection closed")

    @staticmethod
    def is_sva_form() -> pl.Expr:
        """
        True if the uploaded filename follows SVA naming convention.

        SVA filenames typically follow: ``<Name> SVA MM.DDYYYY.pdf``
        """
        return (
            pl.col("filename").is_not_null()
            & pl.col("filename").str.to_lowercase().str.contains("sva")
        )

    @staticmethod
    def patient_name() -> pl.Expr:
        """
        Extract patient name from SVA upload filename, normalized to title case.

        Handles formats like:
        - ``Andrew Weigert Jr SVA 02.182026.pdf`` → ``Andrew Weigert Jr``
        - ``HELEN BILLINGS SVA 03.12.2026.pdf`` → ``Helen Billings``
        - ``Marion_Booker_SVA_)3.16.2026.pdf`` → ``Marion Booker``
        - ``Cabb.pdf`` → ``Cabb`` (no SVA marker, use full stem)
        """
        return (
            pl.when(pl.col("filename").is_not_null() & pl.col("filename").str.contains("(?i)SVA"))
            .then(
                pl.col("filename")
                .str.replace(r"(?i)[\s_]*SVA[\s_]*[).]?\d{0,2}\.?\d{1,2}\.?\d{0,6}(?:-\d+)?(?:\s*\(\d+\))?(?:pdf)?\.pdf$", "")
                .str.replace_all(r"_", " ")
                .str.replace_all(r"\s{2,}", " ")
                .str.strip_chars()
                .str.to_titlecase()
            )
            .when(pl.col("filename").is_not_null())
            .then(
                pl.col("filename")
                .str.replace(r"\.pdf$", "")
                .str.strip_chars()
                .str.to_titlecase()
            )
            .otherwise(pl.lit(None))
            .alias("patient_name")
        )

    @staticmethod
    def patient_name_key() -> pl.Expr:
        """
        Canonicalized patient name for duplicate detection.

        Normalizes by lowercasing, stripping punctuation, collapsing whitespace,
        removing common suffixes (Jr, Sr, II, III), and sorting name tokens
        alphabetically so ``Trevino Isabel`` and ``Isabel Trevino`` match.
        """
        return (
            pl.col("patient_name")
            .str.to_lowercase()
            .str.replace_all(r"[^a-z\s]", "")
            .str.replace_all(r"\b(jr|sr|ii|iii|iv)\b", "")
            .str.replace_all(r"\s{2,}", " ")
            .str.strip_chars()
            .alias("patient_name_key")
        )

    @staticmethod
    def submission_date_str() -> pl.Expr:
        """
        Extract submission date string from SVA filename (MM.DDYYYY portion).

        Returns None for non-SVA filenames.
        """
        return (
            pl.when(pl.col("filename").is_not_null() & pl.col("filename").str.contains(r"\d{2}\.\d{2,6}\.pdf$"))
            .then(
                pl.col("filename")
                .str.extract(r"(\d{2}\.\d{2,6})\.pdf$", 1)
            )
            .otherwise(pl.lit(None))
            .alias("submission_date_str")
        )

    @staticmethod
    def submission_date() -> pl.Expr:
        """
        Submission date extracted by the parser from SVA filenames.

        Handles multiple filename conventions (``MM.DDYYYY``, ``MM.DD.YYYY``,
        ``MM.DD.YY``) — parsing is done in the ``mabel_log`` parser for
        robustness. This expression simply selects the column.
        """
        return pl.col("submission_date")


def find_duplicate_patients(
    df: pl.DataFrame,
    similarity_threshold: float = 0.6,
) -> pl.DataFrame:
    """
    Detect likely duplicate patient names using bigram similarity.

    Compares all unique patient names pairwise using character-bigram
    Jaccard similarity on canonicalized keys (lowered, no punctuation,
    common suffixes stripped). Also flags exact sorted-token matches
    (catches first/last name swaps like ``Trevino Isabel`` ↔ ``Isabel Trevino``).

    Args:
        df: DataFrame with a ``patient_name`` column.
        similarity_threshold: Minimum bigram Jaccard similarity (0–1) to flag
            a pair as a potential duplicate. Default 0.55.

    Returns:
        DataFrame with columns: ``name_a``, ``name_b``, ``similarity``,
        ``match_type`` (``exact_tokens`` or ``fuzzy``), sorted by descending
        similarity.
    """
    names = (
        df.select("patient_name")
        .unique()
        .drop_nulls()
        .with_columns(SvaLogExpression.patient_name_key())
        .filter(pl.col("patient_name_key").str.len_chars() > 1)
        .collect()
        if isinstance(df, pl.LazyFrame)
        else df.select("patient_name")
        .unique()
        .drop_nulls()
        .with_columns(SvaLogExpression.patient_name_key())
        .filter(pl.col("patient_name_key").str.len_chars() > 1)
    )

    rows = names.to_dicts()
    pairs: list[dict] = []

    # Build sorted-token keys for swap detection
    for row in rows:
        row["_sorted"] = " ".join(sorted(row["patient_name_key"].split()))

    for a, b in combinations(rows, 2):
        # Exact sorted-token match (catches first/last swaps)
        if a["_sorted"] == b["_sorted"] and a["patient_name"] != b["patient_name"]:
            pairs.append(
                {
                    "name_a": a["patient_name"],
                    "name_b": b["patient_name"],
                    "similarity": 1.0,
                    "match_type": "exact_tokens",
                }
            )
            continue

        sim = _token_similarity(a["patient_name_key"], b["patient_name_key"])
        if sim >= similarity_threshold:
            pairs.append(
                {
                    "name_a": a["patient_name"],
                    "name_b": b["patient_name"],
                    "similarity": round(sim, 3),
                    "match_type": "fuzzy",
                }
            )

    if not pairs:
        return pl.DataFrame(
            schema={
                "name_a": pl.Utf8,
                "name_b": pl.Utf8,
                "similarity": pl.Float64,
                "match_type": pl.Utf8,
            }
        )

    return pl.DataFrame(pairs).sort("similarity", descending=True)
