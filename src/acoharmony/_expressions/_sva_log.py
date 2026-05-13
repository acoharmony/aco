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

import re
from itertools import combinations

import polars as pl

from ._registry import register_expression

# Trailing signature-date suffix in any of the observed forms
# (``05.11.2026``, ``05.112026``, ``05 11 26``, ``5-11-2026``, ``05.11026``...).
_TRAILING_DATE_RE = re.compile(
    r"[\s._\-]+\(?\d{1,2}[.\s\-_/]*\d{1,2}[.\s\-_/]*\d{2,6}\)?.*$"
)

# Trailing SVA/SA marker once underscores/periods have been normalized to
# spaces (so ``Ann Chovitz Sva`` becomes ``Ann Chovitz``). Also catches the
# ``SA`` typo. Must be word-bounded so it doesn't eat names like ``Sarlas``.
_TRAILING_SVA_TOKEN_RE = re.compile(r"\s+s[av]a?(\s.*)?$", re.IGNORECASE)

# Leading numeric chart/patient ID (e.g. "6128324 Kathryn Mitchell.pdf").
_LEADING_NUMERIC_ID_RE = re.compile(r"^\s*\d{4,}\s+")

_PDF_EXT_RE = re.compile(r"\.pdf$", re.IGNORECASE)
_MULTI_WHITESPACE_RE = re.compile(r"\s{2,}")

# Punctuation we want gone *between* name tokens (keep hyphens for
# double-barreled last names like ``Laguerre-Val``).
_NAME_PUNCT_RE = re.compile(r"[._]+")


def clean_filename_to_name(filename: str | None) -> str | None:
    """
    Clean a raw SVA upload filename down to a beneficiary name.

    Single source of truth: shared by the Polars expression and the PDF
    extractor. Strategy is order-sensitive — trailing date is stripped
    *before* underscores/periods become spaces (otherwise a date like
    ``05.11.2026`` separated by underscores would survive), and the SVA
    marker is stripped *after* (because underscored ``_SVA_`` becomes a
    proper word boundary only once underscores collapse to spaces).

    Pipeline:

    1. Drop trailing ``.pdf``.
    2. Strip trailing date suffix.
    3. Drop a leading 4+ digit chart/patient ID.
    4. Replace ``_`` / ``.`` with spaces (keeps hyphens).
    5. Strip trailing SVA / SA marker (now a real word boundary).
    6. Collapse whitespace; trim; title-case.
    """
    if filename is None:
        return None
    s = _PDF_EXT_RE.sub("", filename)
    s = _TRAILING_DATE_RE.sub("", s)
    s = _LEADING_NUMERIC_ID_RE.sub("", s)
    s = _NAME_PUNCT_RE.sub(" ", s)
    s = _MULTI_WHITESPACE_RE.sub(" ", s).strip()
    s = _TRAILING_SVA_TOKEN_RE.sub("", s)
    s = _MULTI_WHITESPACE_RE.sub(" ", s).strip()
    if not s:
        return None
    return s.title()


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

        Delegates to ``clean_filename_to_name`` so the Polars expression and
        the PDF extractor (acoharmony._notes._sva_pdf_extract) share one
        definition. Handles:

        - canonical: ``Andrew Weigert Jr SVA 02.182026.pdf`` → ``Andrew Weigert Jr``
        - underscores: ``Ann_Chovitz_SVA_05.11.2026.pdf`` → ``Ann Chovitz``
        - no-SVA-token: ``Christina Smith 4.15.2026.pdf`` → ``Christina Smith``
        - leading chart ID: ``6128324 Kathryn Mitchell.pdf`` → ``Kathryn Mitchell``
        - typo'd marker: ``Leland_Poole_SA_5.11.2026.pdf`` → ``Leland Poole``
        - punctuation between tokens: ``George.Beebe_SVA_05.11.2026.pdf`` → ``George Beebe``
        - hyphenated last name: ``Angelina Laguerre-Val SVA ...`` → ``Angelina Laguerre-Val``
        """
        return (
            pl.col("filename")
            .map_elements(clean_filename_to_name, return_dtype=pl.Utf8)
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
