"""Tests for Unity Catalog table creation helpers."""

from __future__ import annotations

from typing import cast

import pytest

from acoharmony._databricks._uc_tables import (
    TableRegistration,
    sql_statements_for_registration,
    storage_uc_catalog_schema,
)
from acoharmony._store import StorageBackend


class _Storage:
    config = {"storage": {"uc_catalog": "uat_sandbox", "uc_schema": "gov_programs"}}


class TestUcTables:
    @pytest.mark.unit
    def test_storage_uc_catalog_schema_uses_storage_config(self):
        assert storage_uc_catalog_schema(cast(StorageBackend, _Storage())) == (
            "uat_sandbox",
            "gov_programs",
        )

    @pytest.mark.unit
    def test_managed_delta_sql_uses_source_location(self):
        registration = TableRegistration(
            table_name="gold_alignment",
            source_path="/Volumes/uat_sandbox/gov_programs/gold/alignment.parquet",
        )

        sql = sql_statements_for_registration(
            registration,
            catalog="uat_sandbox",
            schema="gov_programs",
            replace_existing=False,
            table_mode="managed-delta",
        )[-1]

        assert "CREATE TABLE IF NOT EXISTS `uat_sandbox`.`gov_programs`.`gold_alignment`" in sql
        assert "read_files(" in sql
        assert "'/Volumes/uat_sandbox/gov_programs/gold/alignment.parquet'" in sql
