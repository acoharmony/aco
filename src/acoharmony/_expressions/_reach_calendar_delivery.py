# © 2025 HarmonyCares
# All rights reserved.

"""
REACH calendar delivery provenance expressions.

Match the dates CMS said they'd deliver a report (from the ACO REACH Calendar,
bronze table reach_calendar) to the dates we actually received each
corresponding file in 4i (from the FourICLIStateTracker JSON), then compute
per-file delivery diffs.

The core challenge is bridging the two representations:

    - Calendar rows carry free-text like "Monthly Expenditure Report - April"
      or "Beneficiary Alignment Report - February" and a scheduled start_date.

    - 4i state entries carry a filename (with embedded delivery date), a
      file_type_code, a category, and a remote_metadata.created timestamp.

We classify each calendar description into (schema_name, period_marker), and
derive the same pair from each delivered filename. The two sides then join.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import polars as pl

from acoharmony._parsers._date_extraction import extract_file_date

# Canonical month names to their integer index.
_MONTHS: dict[str, int] = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5, "june": 6,
    "july": 7, "august": 8, "september": 9, "october": 10, "november": 11, "december": 12,
}

# Description-stem → schema_name. Stems are matched case-insensitively with
# word-boundary regexes; the first match wins, so order is load-bearing: most
# specific stems appear first (e.g. "Preliminary Benchmark" before "Benchmark").
_STEM_RULES: list[tuple[str, str]] = [
    # CCLF claim feeds (ftc 113). CCLF Management Report (ftc 198) is separate.
    (r"cclf\s*claims?\s*management", "cclf_management_report"),
    (r"cclfs?\b", "cclf0"),

    # Finance — monthly
    (r"monthly\s+expenditure\s+run-?out\s+report", "mexpr"),
    (r"monthly\s+expenditure\s+report", "mexpr"),
    (r"national\s+ref(?:erence)?\s+pop(?:ulation)?\s+(?:rta\s+)?data\s+report", "mexpr"),
    (r"provider\s+specific\s+(?:payment\s+)?reduction\s+report", "tparc"),

    # Alignment — monthly / quarterly
    (r"beneficiary\s+alignment\s+report", "bar"),
    (r"provider\s+alignment\s+report", "palmr"),
    (r"preliminary\s+alignment\s+estimate", "preliminary_alignment_estimate"),
    (r"provisional\s+alignment\s+estimate", "preliminary_alignment_estimate"),
    (r"prospective\s+plus\s+opportunity\s+report", "prospective_plus_opportunity_report"),
    (r"signed\s+attestation\s+based\s+voluntary\s+alignment", "pbvar"),
    (r"signed\s+voluntary\s+alignment\s+response", "pbvar"),

    # Finance — quarterly / settlement
    (r"alternative\s+payment\s+arrangement\s+report", "alternative_payment_arrangement_report"),
    (r"risk\s+score\s+report", "risk_adjustment_data"),
    (r"preliminary\s+benchmark\s+report", "preliminary_benchmark_report_unredacted"),
    (r"quarterly\s+benchmark\s+report", "reach_bnmr"),
    (r"benchmark\s+report", "reach_bnmr"),

    # Quality
    (r"annual\s+quality\s+report", "annual_quality_report"),
    (r"quarterly\s+quality\s+report", "quarterly_quality_report"),
    (r"beneficiary[-\s]level\s+report", "quarterly_beneficiary_level_quality_report"),
    (r"bene(?:ficiary)?[-\s]?level\s+report", "quarterly_beneficiary_level_quality_report"),
    (r"beneficiary\s+hedr\s+transparency", "beneficiary_hedr_transparency_files"),

    # Compliance
    (r"financial\s+guarantee", "aco_financial_guarantee_amount"),
    (r"estimated\s+ci/?sep\s+change\s+threshold", "estimated_cisep_change_threshold_report"),

    # Shadow Bundles: CMS delivers one zip per month containing all SBM*
    # CSVs, stamped with ``SBMON`` in the filename — that archive matches
    # the ``shadow_bundle_reach`` catch-all pattern. Calendar descriptions
    # refer to this zip as "Shadow Bundles Monthly Files" / "Monthly Files
    # (CYxxxx-CYyyyy)", so both resolve to the same aggregate schema for
    # the purpose of delivery matching.
    (r"shadow\s+bundles?\s+monthly", "shadow_bundle_reach"),
    (r"shadow\s+bundles?\s+quarterly|sbqr", "sbqr"),
    (r"monthly\s+files\s*\(", "shadow_bundle_reach"),
    (r"quarterly\s+report\s*\(", "sbqr"),
]

_COMPILED_STEM_RULES: list[tuple[re.Pattern, str]] = [
    (re.compile(rx, re.IGNORECASE), schema) for rx, schema in _STEM_RULES
]

_MONTH_RX = re.compile(
    r"\b(" + "|".join(_MONTHS) + r")\b", re.IGNORECASE
)
_QUARTER_RX = re.compile(r"\bQ\s*([1-4])\b", re.IGNORECASE)
_SEMI_RX = re.compile(r"\bS\s*([12])\b", re.IGNORECASE)
_PY_RX = re.compile(r"\bPY\s*(20\d{2})\b", re.IGNORECASE)


def classify_calendar_description(
    description: str | None, category: str | None = None
) -> tuple[str | None, str | None, int | None]:
    """
    Parse a calendar Report description into a (schema_name, period, py) triple.

    ``period`` is a coarse bucket that can match a delivered filename:
        - ``"M{1..12}"`` for a month-dated report,
        - ``"Q{1..4}"`` for a quarter-dated report,
        - ``"S{1..2}"`` for a semi-annual settlement report,
        - ``"A"`` for an annual / PY-wide report,
        - ``None`` if nothing recognisable.

    Calendars reuse the same description for every month, so the period comes
    primarily from the description text and, as a fallback, from the start_date
    the caller stamps on later (see ``build_calendar_reports_lf``).
    """
    if not description:
        return None, None, None

    text = description.strip()
    schema_name: str | None = None
    for pattern, name in _COMPILED_STEM_RULES:
        if pattern.search(text):
            schema_name = name
            break

    if schema_name is None:
        return None, None, None

    period: str | None = None
    q = _QUARTER_RX.search(text)
    if q:
        period = f"Q{q.group(1)}"
    elif _SEMI_RX.search(text):
        s = _SEMI_RX.search(text)
        assert s is not None
        period = f"S{s.group(1)}"
    else:
        m = _MONTH_RX.search(text)
        if m:
            period = f"M{_MONTHS[m.group(1).lower()]:02d}"
        elif re.search(r"\bannual\b|\bpy\s*20\d{2}\b|\bfinal\b", text, re.IGNORECASE):
            period = "A"

    py_match = _PY_RX.search(text)
    py = int(py_match.group(1)) if py_match else None

    return schema_name, period, py


def _period_from_date(d: date | None, *, quarterly: bool) -> str | None:
    """Derive a period bucket from start_date when the description was silent."""
    if d is None:
        return None
    if quarterly:
        q = (d.month - 1) // 3 + 1
        return f"Q{q}"
    return f"M{d.month:02d}"


def build_calendar_reports_lf(
    reach_calendar_path: Path, *, latest_only: bool = True
) -> pl.LazyFrame:
    """
    Load reach_calendar.parquet and project Report rows onto the expected-side
    schema used for provenance joins.

    Columns:
        schema_name, period, py, expected_date, category, description,
        calendar_file_date
    """
    lf = pl.scan_parquet(reach_calendar_path).filter(pl.col("type") == "Report")

    if latest_only:
        latest = lf.select(pl.col("file_date").max()).collect().item()
        lf = lf.filter(pl.col("file_date") == latest)

    df = lf.collect()

    schema_names: list[str | None] = []
    periods: list[str | None] = []
    pys: list[int | None] = []
    for desc, cat, sd in zip(
        df["description"].to_list(), df["category"].to_list(), df["start_date"].to_list()
    ):
        name, period, py = classify_calendar_description(desc, cat)
        if name is not None:
            # Annual schemas: structurally one-per-PY, period must be "A".
            if name in _ANNUAL_SCHEMAS:
                period = "A"
            # For monthly and quarterly schemas, the description is the
            # authoritative period source (a calendar row captioned
            # "February Monthly Files (CY2024-CY2025)" is the February
            # report even though CMS schedules its drop for end-of-March).
            # Only fall back to start_date when the description was silent.
            elif not period and name in _QUARTERLY_SCHEMAS:
                period = _period_from_date(sd, quarterly=True)
            elif not period and name in _MONTHLY_SCHEMAS:
                period = _period_from_date(sd, quarterly=False)
            # PAER is the one monthly schema whose descriptions aren't
            # consistent about the month ("Round 1 of 2", "#1", "PY2025
            # Round 2") — the classifier's generic annual fallback would
            # resolve some of them to "A". Force use of start_date month
            # instead, since PAER ships Nov + Dec each PY.
            if name == "preliminary_alignment_estimate":
                period = _period_from_date(sd, quarterly=False)
        schema_names.append(name)
        periods.append(period)
        pys.append(py)

    classified = df.with_columns(
        pl.Series("schema_name", schema_names, dtype=pl.String),
        pl.Series("period", periods, dtype=pl.String),
        pl.Series("inferred_py", pys, dtype=pl.Int64),
    ).rename({"start_date": "expected_date", "file_date": "calendar_file_date"})

    return (
        classified.lazy()
        .with_columns(
            pl.col("inferred_py")
            .fill_null(pl.col("py"))
            .alias("py")
        )
        .select(
            "schema_name",
            "period",
            "py",
            "expected_date",
            "category",
            "description",
            "calendar_file_date",
        )
    )


def _filename_date(filename: str) -> date | None:
    """
    Pull a delivery date out of the filename.

    CMS report filenames stamp the actual delivery date as ``.D{YYMMDD}.``
    immediately before the time token (``.T{HHMMSS}.``). That chunk is the
    specific delivery moment and always wins for provenance matching, even
    when the filename also carries a PY marker like ``PY2024`` (the shared
    extractor treats the PY marker as an annual-reconciliation hint and
    returns end-of-year, which would smear every MEXPR file in the PY into
    the same December-31 bucket — wrong for per-month tracking).

    Only fall back to the shared extractor when there is no delivery-date
    stamp, so purely-annual artifacts still resolve.
    """
    m = re.search(r"\.D(\d{2})(0[1-9]|1[0-2])(0[1-9]|[12]\d|3[01])\.T", filename)
    if m:
        yy, mm, dd = m.groups()
        return date(2000 + int(yy), int(mm), int(dd))

    iso = extract_file_date(filename, None)
    if iso:
        try:
            return date.fromisoformat(iso)
        except ValueError:  # ALLOWED: malformed ISO from extractor, fall through
            pass
    return None


# Schemas CMS publishes once per performance year — the calendar classifier
# resolves their descriptions to ``period="A"`` (annual). Delivery filenames
# don't carry a period token, so we bucket them here to keep join keys aligned.
_ANNUAL_SCHEMAS = frozenset(
    {
        "aco_financial_guarantee_amount",
        "annual_quality_report",
        "annual_beneficiary_level_quality_report",
        "beneficiary_hedr_transparency_files",
        "plaru",
        "estimated_cisep_change_threshold_report",
        "preliminary_benchmark_report_unredacted",
    }
)

# Schemas CMS publishes quarterly without encoding ``.Q#.`` in the filename —
# we synthesise the quarter from the delivery date.
_QUARTERLY_SCHEMAS = frozenset(
    {
        "reach_bnmr",
        "quarterly_quality_report",
        "quarterly_beneficiary_level_quality_report",
        "alternative_payment_arrangement_report",
        "risk_adjustment_data",
        "preliminary_benchmark_report_for_dc",
        "prospective_plus_opportunity_report",
        "sbqr",
        "palmr",
        "pbvar",
    }
)

# Schemas CMS publishes monthly — period is the month of the delivery date.
_MONTHLY_SCHEMAS = frozenset(
    {
        "mexpr",
        "tparc",
        "sbmdm",
        "shadow_bundle_reach",  # SBMON zip archive is monthly
        "cclf_management_report",
        "pecos_terminations_monthly_report",
        "preliminary_alignment_estimate",  # two per PY; treat each as its own month
    }
)


def _filename_period(filename: str, schema_name: str | None) -> str | None:
    """Parse the period marker out of a delivered filename.

    Falls back to the schema's expected cadence (annual / quarterly / monthly)
    when the filename doesn't encode one explicitly. Without the cadence
    fallback, annual and some quarterly deliveries would land with
    ``period=None`` and drop out of the join entirely.
    """
    if not filename:
        return None

    q = re.search(r"\.Q([1-4])\.", filename)
    if q:
        return f"Q{q.group(1)}"

    # ``D????.PY2024.02.SBMON.D*`` — the two-digit segment after PYyyyy is
    # month-of-performance-year for shadow bundles.
    sb = re.search(r"PY20\d{2}\.(\d{2})\.SBM", filename)
    if sb:
        return f"M{int(sb.group(1)):02d}"

    # BAR: ``ALGC23.RP.D230221`` → month from delivery date.
    if schema_name == "bar":
        d = _filename_date(filename)
        if d:
            return f"M{d.month:02d}"

    if schema_name in _QUARTERLY_SCHEMAS:
        d = _filename_date(filename)
        if d:
            q_num = (d.month - 1) // 3 + 1
            return f"Q{q_num}"

    if schema_name in _MONTHLY_SCHEMAS:
        d = _filename_date(filename)
        if d:
            return f"M{d.month:02d}"

    if schema_name in _ANNUAL_SCHEMAS:
        return "A"

    return None


def _filename_py(filename: str) -> int | None:
    """Extract PY from filenames like ``PY2024`` or ``PYRED25``."""
    m = re.search(r"PY(?:RED)?(\d{2,4})", filename)
    if not m:
        return None
    raw = m.group(1)
    if len(raw) == 2:
        return 2000 + int(raw)
    return int(raw)


def _schema_for_filename(filename: str, ftc: int | None, patterns: list[dict[str, Any]]) -> str | None:
    """
    Resolve the schema_name for a delivered file by pattern match, falling
    back to the first schema registered for the given file_type_code.
    """
    from fnmatch import fnmatch

    # Most-specific pattern wins — mirror _4icli.inventory._match_file_type_code.
    scored = sorted(
        patterns,
        key=lambda p: (len(p["pattern"]), sum(c not in "*?" for c in p["pattern"])),
        reverse=True,
    )
    for pat in scored:
        if fnmatch(filename, pat["pattern"]):
            return pat["schema_name"]

    if ftc is None:
        return None
    for pat in patterns:
        if pat.get("file_type_code") == ftc:
            return pat["schema_name"]
    return None


def _load_schema_patterns() -> list[dict[str, Any]]:
    """Registry-backed (pattern, file_type_code, schema_name) map."""
    from acoharmony._4icli.inventory import _load_schema_patterns as _loader

    return _loader()


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        # Trailing Z → +00:00 for fromisoformat on all supported versions.
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:  # ALLOWED: malformed timestamp in state → treat as missing
        return None


def build_deliveries_lf(
    state_file: Path, patterns: list[dict[str, Any]] | None = None
) -> pl.LazyFrame:
    """
    Materialise the 4icli_state.json into a LazyFrame of deliveries.

    ``actual_delivery_date`` is coalesced by priority:
        1. ``remote_metadata.created``  — DataHub's own "made available" stamp
        2. ``download_timestamp``        — first time we pulled it locally
        3. filename-embedded date        — CMS's own ``D{YYMMDD}`` convention

    Only the first of these that is present is used per file. The two fallbacks
    are also carried on the frame so downstream callers can see which source
    drove the final value.
    """
    patterns = patterns if patterns is not None else _load_schema_patterns()

    with state_file.open() as f:
        state = json.load(f)

    records: list[dict[str, Any]] = []
    for entry in state.values():
        filename = entry.get("filename")
        if not filename:
            continue
        ftc = entry.get("file_type_code")
        schema_name = _schema_for_filename(filename, ftc, patterns)
        rm = entry.get("remote_metadata") or {}
        remote_created_dt = _parse_iso(rm.get("created"))
        downloaded_dt = _parse_iso(entry.get("download_timestamp"))
        fname_date = _filename_date(filename)

        if remote_created_dt is not None:
            actual = remote_created_dt.astimezone(timezone.utc).date()
            source = "remote_created"
        elif fname_date is not None:
            # The CMS D{YYMMDD} stamp baked into the filename is the real
            # delivery date — far more accurate than our local download
            # timestamp, which only tells us when *we* pulled the file.
            actual = fname_date
            source = "filename"
        elif downloaded_dt is not None:
            actual = downloaded_dt.date()
            source = "downloaded"
        else:
            actual = None
            source = "unknown"

        records.append(
            {
                "filename": filename,
                "schema_name": schema_name,
                "file_type_code": ftc,
                "category": entry.get("category"),
                "period": _filename_period(filename, schema_name),
                "py": _filename_py(filename),
                "actual_delivery_date": actual,
                "actual_delivery_source": source,
                "remote_created_at": remote_created_dt,
                "downloaded_at": downloaded_dt,
                "filename_date": fname_date,
            }
        )

    df = pl.DataFrame(records, schema={
        "filename": pl.String,
        "schema_name": pl.String,
        "file_type_code": pl.Int64,
        "category": pl.String,
        "period": pl.String,
        "py": pl.Int64,
        "actual_delivery_date": pl.Date,
        "actual_delivery_source": pl.String,
        "remote_created_at": pl.Datetime,
        "downloaded_at": pl.Datetime,
        "filename_date": pl.Date,
    })
    return df.lazy()


def build_provenance_expr(expected: pl.Expr, actual: pl.Expr) -> pl.Expr:
    """
    Delivery-status classifier.

    Inputs are both Date. The caller guarantees ``actual`` is never null —
    provenance is delivery-centric, so rows without an actual delivery don't
    exist in the output at all. ``expected`` may be null when the calendar
    didn't schedule the delivery.
    """
    diff = (actual - expected).dt.total_days()
    return (
        pl.when(expected.is_null())
        .then(pl.lit("unscheduled"))
        .when(diff < -1)
        .then(pl.lit("early"))
        .when(diff.abs() <= 1)
        .then(pl.lit("on_time"))
        .otherwise(pl.lit("late"))
    )


def build_provenance_join(
    calendar_lf: pl.LazyFrame, deliveries_lf: pl.LazyFrame
) -> pl.LazyFrame:
    """
    Delivery-centric provenance join.

    The output carries exactly one row per *actual* delivery key
    ``(schema_name, period, year(actual_delivery_date))``. The calendar side
    is attached by a left-join so every delivery gets enriched with the
    matching scheduled date when one exists, and is flagged ``unscheduled``
    when it doesn't. Calendar entries that never produced a delivery are
    out of scope — we only track what we actually received.

    We key on the *year of the date*, not on the calendar's ``py`` field,
    because the calendar PY is the performance year the data is *about*
    (e.g. a January 2024 expenditure report for PY2024 is scheduled in
    February 2024) while the delivery PY in the filename is the coverage
    year, which can be the same number but isn't reliably so for run-outs
    and reconciliations. The delivery year and expected year are both real
    calendar dates and always align when a scheduled report actually lands.
    """
    deliveries_keyed = deliveries_lf.filter(
        pl.col("schema_name").is_not_null()
        & pl.col("actual_delivery_date").is_not_null()
    ).with_columns(
        pl.col("actual_delivery_date").dt.year().alias("_delivery_year")
    )

    # Collapse deliveries to one row per (schema, period, delivery_year). The
    # earliest delivery date is the one we diff against — corrections and
    # reissues stay visible via delivered_filenames / delivered_file_count.
    deliveries_agg = deliveries_keyed.group_by(
        ["schema_name", "period", "_delivery_year"]
    ).agg(
        pl.col("actual_delivery_date").min().alias("actual_delivery_date"),
        pl.col("actual_delivery_source").first().alias("actual_delivery_source"),
        pl.col("filename").alias("delivered_filenames"),
        pl.col("filename").n_unique().alias("delivered_file_count"),
        pl.col("py").first().alias("delivery_py"),
    )

    calendar_keyed = (
        calendar_lf.filter(pl.col("schema_name").is_not_null())
        .with_columns(pl.col("expected_date").dt.year().alias("_expected_year"))
        # A given (schema, period, year) may have multiple calendar rows if
        # the same report was rescheduled or reworded; take the earliest
        # scheduled date as the "planned" marker.
        .group_by(["schema_name", "period", "_expected_year"])
        .agg(
            pl.col("expected_date").min().alias("expected_date"),
            pl.col("category").first().alias("category"),
            pl.col("description").first().alias("description"),
            pl.col("calendar_file_date").first().alias("calendar_file_date"),
            pl.col("py").first().alias("calendar_py"),
        )
    )

    joined = deliveries_agg.join(
        calendar_keyed,
        left_on=["schema_name", "period", "_delivery_year"],
        right_on=["schema_name", "period", "_expected_year"],
        how="left",
    )

    return joined.with_columns(
        (pl.col("actual_delivery_date") - pl.col("expected_date"))
        .dt.total_days()
        .alias("delivery_diff_days"),
        build_provenance_expr(
            pl.col("expected_date"), pl.col("actual_delivery_date")
        ).alias("delivery_status"),
        pl.coalesce(["calendar_py", "delivery_py"]).alias("py"),
    ).select(
        "schema_name",
        "period",
        "py",
        "category",
        "description",
        "expected_date",
        "actual_delivery_date",
        "delivery_diff_days",
        "delivery_status",
        "actual_delivery_source",
        "delivered_filenames",
        "delivered_file_count",
        "calendar_file_date",
    )
