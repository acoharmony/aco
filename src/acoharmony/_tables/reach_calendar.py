# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for reach_calendar schema.

ACO REACH Calendar - CMS-published calendar of events, reports, and deadlines
for the ACO REACH program. Ingested from single-sheet Excel files named
ACO_REACH_Calendar_updated_*.xlsx.

Field order matches the column order of the source "ACO REACH Events" sheet
(position 0 through 11). The excel parser maps columns by position, so field
ordering here is load-bearing.
"""

from datetime import date, time

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_parser,
    with_storage,
)


@register_schema(
    name="reach_calendar",
    version=1,
    tier="bronze",
    description="ACO REACH Calendar of events, reports, and deadlines published by CMS",
    file_patterns={"reach": ["ACO_REACH_Calendar_updated_*.xlsx"]},
)
@with_parser(type="excel", encoding="utf-8", has_header=True, embedded_transforms=False)
@with_storage(
    tier="bronze",
    file_patterns={"reach": ["ACO_REACH_Calendar_updated_*.xlsx"]},
    silver={"output_name": "reach_calendar.parquet", "refresh_frequency": "ad_hoc"},
)
@dataclass
class ReachCalendar:
    """
    ACO REACH Calendar of events, reports, and deadlines published by CMS.

    Single-sheet ("ACO REACH Events") workbook with one row per calendar item.
    """

    update: str | None = Field(
        default=None, description="Update marker (NEW, REVISED, etc.) in the source calendar"
    )
    py: int | None = Field(default=None, description="Performance year")
    category: str | None = Field(
        default=None,
        description="Event category (e.g., Finance, Alignment, Compliance, Learning System)",
    )
    type: str | None = Field(default=None, description="Entry type (Event, Report, Deadline)")
    start_date: date | None = Field(
        default=None, description="Start date of the event/report/deadline"
    )
    start_time: time | None = Field(default=None, description="Start time (Eastern)")
    end_date: date | None = Field(default=None, description="End date, when applicable")
    end_time: time | None = Field(default=None, description="End time (Eastern), when applicable")
    description: str | None = Field(default=None, description="Description of the calendar entry")
    links: str | None = Field(default=None, description="Associated links or reference URLs")
    updated_as_of: date | None = Field(
        default=None,
        description="Date the entry was last updated in the source calendar",
    )
    notes: str | None = Field(
        default=None,
        description="Additional notes; for Type=Reports the date denotes 'Beneficiary Alignment as of'",
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "ReachCalendar":
        """Create instance from dictionary."""
        return cls(**data)
