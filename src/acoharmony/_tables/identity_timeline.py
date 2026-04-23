# © 2025 HarmonyCares
# All rights reserved.

"""
Pydantic dataclass model for identity_timeline schema.

Silver-tier append-only record of beneficiary identifier history. One row per
(mbi, file_date) observation — preserves effective/obsolete dates that CMS
provides in CCLF9 but that legacy crosswalk tables collapse away.
"""

from datetime import date

from pydantic import Field
from pydantic.dataclasses import dataclass

from acoharmony._registry import (
    register_schema,
    with_storage,
)


@register_schema(
    name="identity_timeline",
    version=1,
    tier="silver",
    description="Append-only MBI identifier timeline with temporal validity",
)
@with_storage(
    tier="silver",
    medallion_layer="silver",
    gold={"output_name": "identity_timeline.parquet"},
)
@dataclass
class IdentityTimeline:
    """
    Point-in-time record of MBI observations and remappings.

    Each row is one observation of a mapping as of a specific CCLF file_date.
    The same prvs_num can appear many times across file_dates; unions across
    file_dates are answered by the gold view (`identity_as_of`).

    Observation types:
        cclf9_remap   — CCLF9 XREF row: prvs_num was remapped to crnt_num
        cclf8_self    — CCLF8 demographics row: self-mapping (mbi == mbi)

    Chain semantics:
        chain_id     — deterministic hash of the sorted MBI set reachable via
                       union-find over (prvs_num, crnt_num) edges (stable
                       unless the chain grows to include a new MBI)
        hop_index    — 0 for the "current" leaf (no outgoing remap), 1+ for
                       historical MBIs ordered by earliest appearance

    Currency:
        is_current_as_of_file_date — True when this row was delivered in the
                       most recent CCLF9/8 file. CMS delivers cumulative-active
                       snapshots, so a row present in Jan but absent in Feb
                       is considered stale as of Feb's file_date.
    """

    mbi: str = Field(description="Observed Medicare Beneficiary Identifier")
    maps_to_mbi: str | None = Field(
        default=None,
        description="MBI this one maps to (null for self-mappings and terminal nodes)",
    )
    effective_date: date | None = Field(
        default=None,
        description="CMS-provided prvs_id_efctv_dt — when this mapping became valid",
    )
    obsolete_date: date | None = Field(
        default=None,
        description="CMS-provided prvs_id_obslt_dt — when this mapping stopped being valid",
    )
    file_date: date = Field(
        description="file_date of the CCLF delivery that produced this observation"
    )
    observation_type: str = Field(
        description="Source observation: cclf9_remap | cclf8_self"
    )
    source_file: str | None = Field(
        default=None,
        description="Source filename for audit trail",
    )
    hcmpi: str | None = Field(
        default=None,
        description="HarmonyCares Master Patient Index (joined from hcmpi_master when available)",
    )
    chain_id: str = Field(
        description="Deterministic hash of the connected-component MBI set",
    )
    hop_index: int = Field(
        description="Position in the chain (0 = current leaf, 1+ = historical)",
    )
    is_current_as_of_file_date: bool = Field(
        description="True if this observation was present in the most recent CCLF file",
    )

    def to_dict(self) -> dict:
        """Convert to dictionary."""
        from dataclasses import asdict

        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "IdentityTimeline":
        """Create instance from dictionary."""
        return cls(**data)
