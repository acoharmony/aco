"""Tests for ACOMS inventory discovery."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pytest

from acoharmony._acoms.config import AcomsConfig
from acoharmony._acoms.inventory import FileInventoryEntry, InventoryDiscovery, InventoryResult
from acoharmony._acoms.models import AcomsCategory, FileTypeDefinition


def _test_config(tmp_path: Path) -> AcomsConfig:
    return AcomsConfig(
        binary_path=Path("acoms"),
        working_dir=tmp_path / "bronze",
        data_path=tmp_path,
        bronze_dir=tmp_path / "bronze",
        archive_dir=tmp_path / "archive",
        silver_dir=tmp_path / "silver",
        gold_dir=tmp_path / "gold",
        log_dir=tmp_path / "logs",
        tracking_dir=tmp_path / "logs" / "tracking",
        default_aco_id="aco-1",
        request_delay=0,
    )


@pytest.mark.unit
def test_discover_and_enrich_file_type_codes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery = InventoryDiscovery(config=_test_config(tmp_path), request_delay=0)

    def fake_view(aco_id, category=None, file_type_code=None, year=None):
        del aco_id
        assert category is not None
        assert year == 2026
        if file_type_code == 113:
            return [
                {
                    "filename": "P.A2671.ACO.ZCY26.D260211.T1435560.zip",
                    "size_bytes": 10,
                    "last_updated": "2026-02-12T12:34:44.000Z",
                }
            ]
        if file_type_code == 305:
            return [
                {
                    "filename": "P.A2671.ACO.ZCWY26.S260101.E260217.D260218.T1104240.zip",
                    "size_bytes": 20,
                    "last_updated": "2026-02-18T19:58:50.000Z",
                }
            ]
        return [
            {
                "filename": "P.A2671.ACO.ZCY26.D260211.T1435560.zip",
                "size_bytes": 10,
                "last_updated": "2026-02-12T12:34:44.000Z",
            },
            {
                "filename": "P.A2671.ACO.ZCWY26.S260101.E260217.D260218.T1104240.zip",
                "size_bytes": 20,
                "last_updated": "2026-02-18T19:58:50.000Z",
            },
        ]

    def fail_catalog():
        raise AssertionError("registered _tables ACOMS metadata should be used first")

    monkeypatch.setattr(discovery, "_load_file_type_catalog", fail_catalog)
    monkeypatch.setattr(discovery, "_run_view_command", fake_view)

    result = discovery.discover_years(
        aco_id="aco-1",
        start_year=2026,
        end_year=2026,
        categories=[AcomsCategory.CCLF],
    )
    result = discovery.enrich_with_file_type_codes(result)

    assert result.total_files == 2
    assert {file_entry.file_type_code for file_entry in result.files} == {113, 305}

    path = discovery.get_inventory_path()
    result.save_to_json(path)
    reloaded = InventoryResult.load_from_json(path)
    assert reloaded.total_files == 2
    assert reloaded.files_by_year == {2026: 2}


@pytest.mark.unit
def test_enrich_does_not_probe_ambiguous_acoms_file_types(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery = InventoryDiscovery(config=_test_config(tmp_path), request_delay=0)

    result = InventoryResult(
        aco_id="aco-1",
        categories=["Reports"],
        years=[2023],
        total_files=1,
        files_by_year={2023: 1},
        files_by_category={"Reports": 1},
        files=[
            FileInventoryEntry(
                filename="P.A2671.ACO.UNKNOWN.D239999.T0000000.zip",
                category="Reports",
                file_type_code=None,
                year=2023,
            )
        ],
        started_at=datetime.now(),
        completed_at=datetime.now(),
    )

    monkeypatch.setattr(
        discovery,
        "_load_file_type_catalog",
        lambda: [
            FileTypeDefinition("Reports", "Adhoc Report", 124),
            FileTypeDefinition("Reports", "Quality Reconciliation Report", 133),
        ],
    )

    def fail_view_probe(*args, **kwargs):
        raise AssertionError("ACOMS --view --file should not be used for enrichment")

    monkeypatch.setattr(discovery, "_run_view_command", fail_view_probe)

    enriched = discovery.enrich_with_file_type_codes(result)

    assert enriched.files[0].file_type_code is None
