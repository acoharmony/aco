# © 2025 HarmonyCares
# All rights reserved.

"""
Participant list entity-column expressions.

Pure expression builders that fill the participant_list entity columns
(``entity_id``, ``entity_tin``, ``entity_legal_business_name``,
``performance_year``) for source files that ship without them — typically
HarmonyCares-internal provider list exports whose rows describe providers
under the operating ACO but omit the entity rollup columns.

The operating ACO identity (apm_id, TIN, legal business name) is *not*
hardcoded here. It is loaded from ``aco.toml`` via
:func:`acoharmony._config_loader.get_aco_identity`, so deploying this
project for a different ACO requires only a config change.
"""

from typing import Any

import polars as pl

from .._config_loader import get_aco_identity


def build_fill_entity_columns_exprs(
    identity: dict[str, Any] | None = None,
) -> list[pl.Expr]:
    """
    Build expressions to fill entity columns when they are null.

    Used for source rows that lack entity-level columns (HarmonyCares
    internal provider list exports). Existing values are preserved — these
    are coalesce-style fills, not overwrites — so REACH rows that already
    carry their own entity columns pass through untouched.

    Parameters
    ----------
    identity : dict, optional
        Pre-loaded ACO identity row from ``aco.toml`` (used by tests to
        inject fixtures). When omitted, the active identity is loaded
        lazily via :func:`get_aco_identity`.

    Returns
    -------
    list[pl.Expr]
        Three expressions, one each for ``entity_id``, ``entity_tin``, and
        ``entity_legal_business_name``.
    """
    row = identity or get_aco_identity()
    return [
        pl.col("entity_id").fill_null(pl.lit(row["apm_id"])).alias("entity_id"),
        pl.col("entity_tin").fill_null(pl.lit(row["tin"])).alias("entity_tin"),
        pl.col("entity_legal_business_name")
        .fill_null(pl.lit(row["legal_business_name"]))
        .alias("entity_legal_business_name"),
    ]


def build_performance_year_from_file_date_expr() -> pl.Expr:
    """
    Build expression to derive ``performance_year`` from ``file_date`` when null.

    Performance year follows CMS convention of ``PYYYYY`` (e.g. ``PY2026``).
    The transform expects ``file_date`` to already be present and ISO-formatted
    (``YYYY-MM-DD``) — the standard runner adds this column from filename
    parsing before the transform stage.

    Source files that already include a ``performance_year`` column pass
    through untouched.

    Returns
    -------
    pl.Expr
        Expression producing the ``performance_year`` column.
    """
    return (
        pl.col("performance_year")
        .fill_null(pl.lit("PY") + pl.col("file_date").cast(pl.Utf8).str.slice(0, 4))
        .alias("performance_year")
    )
