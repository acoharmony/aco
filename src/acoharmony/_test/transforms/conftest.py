# © 2025 HarmonyCares
# All rights reserved.

"""Shared test fixtures for _transforms test modules.

This conftest provides two things:

1. A generic ``tmp_base`` directory for ad-hoc test data.
2. A session-scoped marimo notebook fixture bundle that lets the 7
   ``notebookalignment*functions.py`` test modules call
   ``consolidated_alignments.app.run()`` without needing /opt/s3 data.

The notebook's module-load cells expect to read:
- ``{gold}/consolidated_alignment.parquet``
- ``{gold}/medical_claim.parquet``
- ``catalog.scan_table("emails")``  (resolves to ``{silver}/emails.parquet``)
- ``catalog.scan_table("mailed")``  (resolves to ``{silver}/mailed.parquet``)

We write minimal-but-schema-complete parquet files into a session-scoped
temp directory, then monkey-patch ``StorageBackend.get_path`` to return
that directory for every tier the notebook asks about. ``Catalog`` uses
``storage_config.get_path(tier)`` internally, so patching at that level
redirects both the direct parquet reads and the ``scan_table`` reads.
"""

from __future__ import annotations

import sys
from datetime import date, datetime
from pathlib import Path

import polars as pl
import pytest


@pytest.fixture
def tmp_base(tmp_path: Path) -> Path:
    """Provide a base temp directory for test data."""
    return tmp_path / "data"


# ---------------------------------------------------------------------------
# Notebook fixture parquet builders
# ---------------------------------------------------------------------------

# Year-month columns the notebook references by literal. The notebook
# extracts year-months from schema names matching ``ym_YYYYMM_*``, so we
# include a few consecutive months plus the specific Dec-2025 → Jan-2026
# transition cells that line-4308 filters on.
_YEAR_MONTHS = [
    "202312",
    "202401",
    "202402",
    "202403",
    "202404",
    "202405",
    "202512",
    "202601",
]


def _build_consolidated_alignment_df() -> pl.DataFrame:
    """Build a 2-row consolidated_alignment DataFrame with every column the
    notebook cells touch. Both rows represent living beneficiaries in both
    programs so that filters reliably produce non-empty (but tiny) results.
    """
    # Per-ym_ booleans: two rows, both enrolled in both programs
    ym_cols: dict[str, list[bool]] = {}
    for ym in _YEAR_MONTHS:
        ym_cols[f"ym_{ym}_reach"] = [True, True]
        ym_cols[f"ym_{ym}_mssp"] = [True, True]
        ym_cols[f"ym_{ym}_ffs"] = [False, False]

    data: dict[str, list] = {
        # Identity
        "current_mbi": ["1A00A00AA00", "1B11B11BB11"],
        "bene_mbi": ["1A00A00AA00", "1B11B11BB11"],
        "previous_mbi_count": [0, 0],
        "mbi_stability": ["stable", "stable"],
        # Demographics
        "bene_first_name": ["Alice", "Bob"],
        "bene_last_name": ["Smith", "Jones"],
        "birth_date": [date(1950, 1, 1), date(1955, 6, 15)],
        "sex": ["F", "M"],
        "race": ["1", "2"],
        "bene_city": ["Dallas", "Houston"],
        "bene_state": ["TX", "TX"],
        "bene_zip_5": ["75201", "77001"],
        "bene_death_date": [None, None],
        "death_date": [None, None],
        # Program status
        "consolidated_program": ["REACH", "MSSP"],
        "current_program": ["REACH", "MSSP"],
        "ever_reach": [True, True],
        "ever_mssp": [True, True],
        # Temporal range
        "first_reach_date": [date(2024, 1, 1), date(2024, 1, 1)],
        "last_reach_date": [date(2026, 1, 1), date(2026, 1, 1)],
        "first_mssp_date": [date(2024, 1, 1), date(2024, 1, 1)],
        "last_mssp_date": [date(2026, 1, 1), date(2026, 1, 1)],
        "months_in_reach": [12, 12],
        "months_in_mssp": [12, 12],
        "total_aligned_months": [24, 24],
        "enrollment_gaps": [0, 0],
        "observable_start": [date(2023, 12, 1), date(2023, 12, 1)],
        "observable_end": [date(2026, 1, 31), date(2026, 1, 31)],
        # Enrollment continuity
        "has_continuous_enrollment": [True, True],
        "has_program_transition": [False, False],
        # Voluntary alignment / SVA
        "has_voluntary_alignment": [True, False],
        "has_valid_voluntary_alignment": [True, False],
        "sva_expiration_date": [date(2027, 1, 1), None],
        "first_sva_submission_date": [date(2024, 1, 1), None],
        "last_sva_submission_date": [date(2024, 6, 1), None],
        "last_signature_expiry_date": [date(2027, 1, 1), None],
        # Outreach (pre-enrichment — usually nulls because load cell enriches)
        "voluntary_email_count": [0, 0],
        "voluntary_letter_count": [0, 0],
        "voluntary_emails_opened": [0, 0],
        "voluntary_emails_clicked": [0, 0],
        "voluntary_outreach_attempts": [0, 0],
        "has_voluntary_outreach": [False, False],
        "voluntary_outreach_type": ["No Voluntary Outreach", "No Voluntary Outreach"],
        "voluntary_engagement_level": ["Not Contacted", "Not Contacted"],
        "email_campaign_periods": ["", ""],
        "letter_campaign_periods": ["", ""],
        "campaign_periods_contacted": ["", ""],
        "voluntary_email_campaigns": [0, 0],
        "voluntary_letter_campaigns": [0, 0],
        "last_voluntary_email_date": [None, None],
        "last_voluntary_letter_date": [None, None],
        # Office / provider
        "office_location": ["Dallas", "Houston"],
        "office_name": ["Dallas Clinic", "Houston Clinic"],
        "assigned_provider_npi": ["1234567890", "0987654321"],
        # Source attribution
        "newly_added_source_2025_to_2026": [None, None],
        # Unalignment reason flags
        "expired_sva_2025": [False, False],
        "lost_provider_2025": [False, False],
        "moved_ma_2025": [False, False],
        # Vintage
        "vintage_cohort": ["12-24 months", "12-24 months"],
        "primary_alignment_source": ["SVA", "Claims"],
        # SVA action categorization (referenced by analyze_sva_action_categories)
        "sva_action_needed": ["Valid", "Renewal"],
        **ym_cols,
    }

    # Explicit schema so None columns get the right dtype (rather than Object).
    schema = {
        "last_voluntary_email_date": pl.Datetime,
        "last_voluntary_letter_date": pl.Datetime,
        "bene_death_date": pl.Date,
        "death_date": pl.Date,
        "sva_expiration_date": pl.Date,
        "first_sva_submission_date": pl.Date,
        "last_sva_submission_date": pl.Date,
        "last_signature_expiry_date": pl.Date,
        "newly_added_source_2025_to_2026": pl.Utf8,
    }
    return pl.DataFrame(data, schema_overrides=schema)


def _build_medical_claim_df() -> pl.DataFrame:
    """Build a minimal medical_claim frame. The notebook filters to
    ``claim_start_date.dt.year() == 2025`` and then needs:

    - ``bill_type_code`` for hospice spend (starts with '81' or '82')
    - ``hcpcs_code`` for E&M visit detection via
      ``UtilizationExpression.is_em_visit()``
    - ``paid_amount`` for spend sum
    - ``member_id`` for joining back to ``current_mbi``
    """
    return pl.DataFrame(
        {
            "claim_start_date": [date(2025, 3, 1), date(2025, 6, 1)],
            "member_id": ["1A00A00AA00", "1B11B11BB11"],
            "bill_type_code": ["811", "131"],
            "hcpcs_code": ["99213", "G0438"],
            "paid_amount": [1500.00, 250.00],
        }
    )


def _build_emails_df() -> pl.DataFrame:
    """Build a minimal emails frame. Notebook only needs mbi, campaign,
    send_datetime, status, has_been_opened, has_been_clicked. The boolean
    flag columns are stored as strings ("true"/"false") per the live table.
    """
    return pl.DataFrame(
        {
            "mbi": ["1A00A00AA00"],
            "campaign": ["2024 Q2 ACO Voluntary Alignment"],
            "send_datetime": [datetime(2024, 5, 15, 10, 0, 0)],
            "status": ["Delivered"],
            "has_been_opened": ["true"],
            "has_been_clicked": ["false"],
        }
    )


def _build_mailed_df() -> pl.DataFrame:
    """Build a minimal mailed frame. Notebook only needs mbi, campaign_name,
    send_datetime, status.
    """
    return pl.DataFrame(
        {
            "mbi": ["1A00A00AA00"],
            "campaign_name": ["2024 Q3 ACO Voluntary Alignment"],
            "send_datetime": [datetime(2024, 8, 15, 9, 0, 0)],
            "status": ["Mailed"],
        }
    )


# ---------------------------------------------------------------------------
# Notebook fixture wiring
# ---------------------------------------------------------------------------

_NOTEBOOK_FIXTURES_DIR = (
    Path(__file__).resolve().parent.parent / "_fixtures" / "notebooks"
)

# Put the bundled notebook on sys.path at conftest import time so that test
# modules can do a plain ``import consolidated_alignments`` at their module
# level. Importing the notebook only defines the marimo ``app`` object —
# it does NOT execute any cells — so this is safe even without storage
# patching in place.
if str(_NOTEBOOK_FIXTURES_DIR) not in sys.path:
    sys.path.insert(0, str(_NOTEBOOK_FIXTURES_DIR))


@pytest.fixture(scope="session")
def notebook_fixture_data_dir(tmp_path_factory) -> Path:
    """Write the minimal parquet fixtures the notebook cells need.

    Returns the root of a tier-structured temp directory:
        <root>/
          silver/emails.parquet
          silver/mailed.parquet
          gold/consolidated_alignment.parquet
          gold/medical_claim.parquet
    """
    root = tmp_path_factory.mktemp("notebook_fixture_data")
    silver = root / "silver"
    gold = root / "gold"
    silver.mkdir()
    gold.mkdir()

    _build_consolidated_alignment_df().write_parquet(gold / "consolidated_alignment.parquet")
    _build_medical_claim_df().write_parquet(gold / "medical_claim.parquet")
    _build_emails_df().write_parquet(silver / "emails.parquet")
    _build_mailed_df().write_parquet(silver / "mailed.parquet")

    return root


@pytest.fixture(scope="session")
def notebook_defs(notebook_fixture_data_dir: Path) -> dict:
    """Load the bundled consolidated_alignments marimo notebook and return
    its definitions dict.

    The notebook's module-load cells expect to read real parquet files.
    We monkey-patch ``StorageBackend.get_path`` and
    ``StorageBackend.get_data_path`` to point every tier request at the
    session-scoped fixture dir created by ``notebook_fixture_data_dir``.
    The patch is applied for the duration of the session so cached
    ``Catalog`` instances keep seeing the fixture paths.
    """
    from unittest.mock import patch

    from acoharmony._store import StorageBackend

    tier_mapping = {
        "bronze": "bronze",
        "silver": "silver",
        "gold": "gold",
        "tmp": "tmp",
        "logs": "logs",
        "cites": "cites",
    }

    def _fake_get_path(self, tier):  # noqa: ARG001 - self unused
        subdir = tier_mapping.get(str(tier).lower(), str(tier))
        full = notebook_fixture_data_dir / subdir
        full.mkdir(parents=True, exist_ok=True)
        return full

    def _fake_get_data_path(self, subpath: str = "", tier: str | None = None):  # noqa: ARG001
        # Some callers use get_data_path("gold"), others use get_data_path() + tier kwarg.
        # We honor either shape.
        if tier is not None:
            target = notebook_fixture_data_dir / tier_mapping.get(str(tier).lower(), str(tier))
        elif subpath:
            target = notebook_fixture_data_dir / subpath
        else:
            target = notebook_fixture_data_dir
        target.mkdir(parents=True, exist_ok=True)
        return target

    # Ensure the bundled notebook is importable.
    if str(_NOTEBOOK_FIXTURES_DIR) not in sys.path:
        sys.path.insert(0, str(_NOTEBOOK_FIXTURES_DIR))

    with (
        patch.object(StorageBackend, "get_path", _fake_get_path),
        patch.object(StorageBackend, "get_data_path", _fake_get_data_path),
    ):
        try:
            import consolidated_alignments  # type: ignore[import-not-found]
        except ModuleNotFoundError as exc:  # pragma: no cover - fixture bug
            pytest.skip(f"bundled consolidated_alignments notebook not importable: {exc}")
        _, defs = consolidated_alignments.app.run()
    return defs
