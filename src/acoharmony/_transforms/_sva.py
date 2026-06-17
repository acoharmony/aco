# © 2025 HarmonyCares
# All rights reserved.

"""SVA source normalization transform."""

from typing import Any

import polars as pl

from .._decor8 import transform, transform_method

_RAW_TO_CANONICAL = {
    "aco_id": ("aco_id",),
    "bene_mbi": ("bene_mbi", "beneficiary_s_mbi"),
    "bene_first_name": ("bene_first_name", "beneficiary_s_first_name"),
    "bene_last_name": ("bene_last_name", "beneficiary_s_last_name"),
    "bene_street_address": ("bene_street_address", "beneficiary_s_street_address"),
    "city": ("city",),
    "state": ("state",),
    "zip": ("zip",),
    "provider_name": (
        "provider_name",
        "provider_name_primary_place_the_beneficiary_receives_care_as_it_appears_on_the_signed_sva_letter",
    ),
    "sva_provider_name": (
        "sva_provider_name",
        "name_of_individual_participant_provider_associated_w_attestation",
    ),
    "sva_npi": ("sva_npi", "i_npi_for_individual_participant_provider_column_j"),
    "sva_tin": ("sva_tin", "tin_for_individual_participant_provider_column_j"),
    "sva_response_code": ("sva_response_code", "response_code_cms_to_fill_out"),
}

_CANONICAL_COLUMNS = [
    "aco_id",
    "bene_mbi",
    "bene_first_name",
    "bene_last_name",
    "bene_street_address",
    "city",
    "state",
    "zip",
    "provider_name",
    "sva_provider_name",
    "sva_npi",
    "sva_tin",
    "sva_signature_date",
    "sva_response_code",
    "processed_at",
    "source_file",
    "source_filename",
    "file_date",
    "medallion_layer",
]


def _text_expr(existing: set[str], candidates: tuple[str, ...], output: str) -> pl.Expr:
    parts = [pl.col(name).cast(pl.Utf8, strict=False) for name in candidates if name in existing]
    expr = pl.coalesce(parts) if parts else pl.lit(None, dtype=pl.Utf8)
    cleaned = expr.str.strip_chars()
    return pl.when(cleaned == "").then(None).otherwise(cleaned).alias(output)


def _date_expr(existing: set[str], candidates: tuple[str, ...], output: str) -> pl.Expr:
    parts = [pl.col(name).cast(pl.Utf8, strict=False) for name in candidates if name in existing]
    raw = pl.coalesce(parts) if parts else pl.lit(None, dtype=pl.Utf8)
    text = raw.str.strip_chars()
    parsed = pl.coalesce(
        [
            text.str.to_date("%Y-%m-%d", strict=False),
            text.str.to_date("%m/%d/%Y", strict=False),
            text.str.to_date("%-m/%-d/%Y", strict=False),
            text.str.to_date("%m/%d/%y", strict=False),
            text.str.to_date("%-m/%-d/%y", strict=False),
        ]
    )
    return pl.when((text.is_null()) | (text == "")).then(None).otherwise(parsed).alias(output)


def _optional_expr(existing: set[str], name: str, dtype: Any) -> pl.Expr:
    if name in existing:
        return pl.col(name).cast(dtype, strict=False).alias(name)
    return pl.lit(None, dtype=dtype).alias(name)


@transform(name="sva", tier=["bronze"], sql_enabled=False)
@transform_method(enable_composition=False, threshold=5.0)
def apply_transform(
    df: pl.LazyFrame, schema: dict, catalog: Any, logger: Any, force: bool = False
) -> pl.LazyFrame:
    """
    Normalize CMS SVA workbooks to the canonical silver SVA columns.

    The raw CMS workbook fields are long, human-readable labels. Downstream
    voluntary alignment logic expects the historical canonical names, so this
    transform coalesces either layout into that stable shape and drops
    non-data rows such as imported instruction-sheet content.
    """
    logger.info("Starting transform: sva")
    existing = set(df.collect_schema().names())

    text_columns = [
        _text_expr(existing, candidates, output) for output, candidates in _RAW_TO_CANONICAL.items()
    ]

    normalized = df.select(
        [
            *text_columns,
            _date_expr(
                existing,
                ("sva_signature_date", "signature_date_on_sva_letter"),
                "sva_signature_date",
            ),
            _optional_expr(existing, "processed_at", pl.Datetime),
            _optional_expr(existing, "source_file", pl.Utf8),
            _optional_expr(existing, "source_filename", pl.Utf8),
            _date_expr(existing, ("file_date",), "file_date"),
            _optional_expr(existing, "medallion_layer", pl.Utf8),
        ]
    ).with_columns(
        [
            pl.col("bene_mbi").str.replace_all(r"\s+", "").str.to_uppercase(),
            pl.col("aco_id").str.to_uppercase(),
            pl.col("state").str.to_uppercase(),
            pl.col("sva_npi").str.replace(r"\.0$", ""),
            pl.col("sva_tin").str.replace(r"\.0$", ""),
            pl.col("zip").str.replace(r"\.0$", ""),
            pl.col("sva_response_code").str.to_uppercase(),
        ]
    )

    result = normalized.filter(
        pl.col("aco_id").str.contains(r"^D\d{4}$")
        & pl.col("bene_mbi").str.contains(r"^[A-Z0-9]{11}$")
    ).select(_CANONICAL_COLUMNS)

    logger.info("Completed transform: sva")
    return result
