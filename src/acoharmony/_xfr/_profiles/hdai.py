# © 2025 HarmonyCares
# All rights reserved.

"""
HDAI transfer profile.

HarmonyCares → HDAI hand-off: monthly CCLF bundle zips (REACH ``D????``
and MSSP ``A????``) plus the BAR (alignment) report xlsx. Files are
dropped into the HDAI Outbound directory, which a third-party SFTP
tool watches and pushes to ``sftp.a2671.prod.healthvision.ai``. The
same tool moves uploaded files into HDAI/Archive.

Source rule: a composite of two rules.

* ``LiteralPatternRule`` for the CCLF bundle zip
  (``P.?????.ACO.ZCY??.D??????.T*.zip``). The bundle has no schema —
  it's a transport artifact wrapping ``cclf0..cclf9``, so we encode
  its pattern directly here rather than registering a fake schema.
  Excludes weekly (``ZCWY``) and runout (``ZCYR``) variants by virtue
  of the literal ``ZCY`` glyph.
* ``SchemaPatternRule`` for ``bar`` (file_type 159), so changes to
  the schema's pattern flow through automatically.

A bundle zip can live in ``bronze`` (just-downloaded) or ``archive``
(post-unpack); both directories are listed as sources.

Verifier: ``LogVerifier`` against ``LogHDAI.log``. The log format is
shared with Mabel, so it reuses ``parse_mabel_log`` rather than
introducing a second parser.
"""

from __future__ import annotations

from pathlib import Path

from acoharmony._store import StorageBackend

from ..profile import (
    CompositeRule,
    LiteralPatternRule,
    LogVerifier,
    SchemaPatternRule,
    TransferProfile,
    register_profile,
)

_HDAI_ROOT = Path("/mnt/x/Documents/USMM/Shared Savings/HDAI")

_storage = StorageBackend()
_BRONZE = Path(_storage.get_path("bronze"))
_ARCHIVE = Path(_storage.get_path("archive"))


hdai_profile = TransferProfile(
    name="hdai",
    description="HarmonyCares → HDAI: monthly CCLF bundle + BAR; verify via LogHDAI.log",
    source_dirs=(_BRONZE, _ARCHIVE),
    destination=_HDAI_ROOT / "Outbound",
    archive_dir=_HDAI_ROOT / "Archive",
    source_rule=CompositeRule(
        rules=(
            LiteralPatternRule(
                patterns=("P.?????.ACO.ZCY??.D??????.T*.zip",),
                date_floor="month_start",
            ),
            SchemaPatternRule(
                schemas=("bar", "palmr", "pbvar"),
                date_floor="month_start",
            ),
        )
    ),
    rename=None,
    verifier=LogVerifier(
        log_path=_HDAI_ROOT / "Log" / "LogHDAI.log",
        upload_dest_prefix="/home/HarmonyCaresHDAI/FromHC/",
    ),
)

register_profile(hdai_profile)
