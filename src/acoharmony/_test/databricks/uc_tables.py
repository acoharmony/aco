"""Tests for Unity Catalog table creation helpers."""

from __future__ import annotations

import argparse
import time
from pathlib import Path
from typing import cast

import pytest

import acoharmony._databricks._uc_tables as uc_tables
from acoharmony._databricks._uc_tables import (
    TableRegistration,
    create_tables,
    sql_statements_for_registration,
    storage_uc_catalog_schema,
)
from acoharmony._store import StorageBackend


class _Storage:
    config = {"storage": {"uc_catalog": "uat_sandbox", "uc_schema": "gov_programs"}}

    def __init__(self, root: Path | None = None):
        self.root = root

    def get_path(self, tier: str) -> Path:
        assert self.root is not None
        return self.root / tier

    def get_uc_volume_roots(self, tiers=None) -> list[str]:
        assert self.root is not None
        return [str(self.root / tier) for tier in (tiers or ["bronze", "silver", "gold"])]


class _Spark:
    def __init__(self):
        self.queries: list[str] = []

    def sql(self, sql: str) -> None:
        self.queries.append(sql)


class _Rows:
    def __init__(self, rows):
        self.rows = rows

    def collect(self):
        return self.rows


class _SparkWithTableProperties:
    def __init__(self, properties: dict[str, str]):
        self.properties = properties
        self.queries: list[str] = []

    def sql(self, sql: str):
        self.queries.append(sql)
        if sql.startswith("SHOW TBLPROPERTIES"):
            return _Rows(list(self.properties.items()))
        return _Rows([])


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

    @pytest.mark.unit
    def test_create_tables_replace_existing_skips_unchanged_sources(
        self, tmp_path, monkeypatch, capsys
    ):
        parquet = tmp_path / "gold" / "alignment.parquet"
        parquet.parent.mkdir()
        parquet.write_text("fake parquet bytes", encoding="utf-8")

        spark = _Spark()
        storage = _Storage(tmp_path)
        monkeypatch.setattr(uc_tables, "StorageBackend", lambda profile=None: storage)
        monkeypatch.setattr(uc_tables, "get_spark_session", lambda required: spark)
        monkeypatch.setattr(uc_tables, "get_dbutils", lambda spark_session: None)

        args = argparse.Namespace(
            aco_profile=None,
            catalog="uat_sandbox",
            databricks_bin="databricks-that-is-not-installed",
            dry_run=False,
            duplicate_strategy="prefix-volume",
            force=False,
            include_volume_prefix=False,
            layers=None,
            no_recurse=False,
            poll_interval_seconds=0.01,
            profile=None,
            replace_existing=True,
            schema="gov_programs",
            skip_missing_roots=False,
            state_file=str(tmp_path / "databricks_state.json"),
            table_mode="managed-delta",
            table_prefix="",
            target=None,
            volume_fqns=[],
            volume_roots=[str(tmp_path / "gold")],
            wait_timeout_seconds=1,
            warehouse_id=None,
        )

        assert create_tables(args) == 0
        first_run_queries = list(spark.queries)
        assert any(query.startswith("DROP TABLE IF EXISTS") for query in first_run_queries)

        assert create_tables(args) == 0
        second_run_queries = spark.queries[len(first_run_queries) :]
        assert len(second_run_queries) == 1
        assert second_run_queries[0].startswith("CREATE SCHEMA IF NOT EXISTS")
        assert "Skipping alignment" in capsys.readouterr().out

        time.sleep(0.01)
        parquet.write_text("changed parquet bytes", encoding="utf-8")

        assert create_tables(args) == 0
        third_run_queries = spark.queries[len(first_run_queries) + len(second_run_queries) :]
        assert any(query.startswith("DROP TABLE IF EXISTS") for query in third_run_queries)

    @pytest.mark.unit
    def test_create_tables_bootstraps_from_catalog_table_properties(
        self, tmp_path, monkeypatch, capsys
    ):
        parquet = tmp_path / "gold" / "alignment.parquet"
        parquet.parent.mkdir()
        parquet.write_text("fake parquet bytes", encoding="utf-8")

        snapshot = uc_tables.source_snapshot_for_registration(
            TableRegistration(table_name="alignment", source_path=str(parquet)),
            dbutils=None,
            databricks_cli=None,
        )
        spark = _SparkWithTableProperties(uc_tables.source_snapshot_table_properties(snapshot))
        storage = _Storage(tmp_path)
        state_file = tmp_path / "databricks_state.json"

        monkeypatch.setattr(uc_tables, "StorageBackend", lambda profile=None: storage)
        monkeypatch.setattr(uc_tables, "get_spark_session", lambda required: spark)
        monkeypatch.setattr(uc_tables, "get_dbutils", lambda spark_session: None)

        args = argparse.Namespace(
            aco_profile=None,
            catalog="uat_sandbox",
            databricks_bin="databricks-that-is-not-installed",
            dry_run=False,
            duplicate_strategy="prefix-volume",
            force=False,
            include_volume_prefix=False,
            layers=None,
            no_recurse=False,
            poll_interval_seconds=0.01,
            profile=None,
            replace_existing=True,
            schema="gov_programs",
            skip_missing_roots=False,
            state_file=str(state_file),
            table_mode="managed-delta",
            table_prefix="",
            target=None,
            volume_fqns=[],
            volume_roots=[str(tmp_path / "gold")],
            wait_timeout_seconds=1,
            warehouse_id=None,
        )

        assert create_tables(args) == 0
        assert not any(query.startswith("DROP TABLE IF EXISTS") for query in spark.queries)
        assert not any(query.startswith("CREATE TABLE") for query in spark.queries)
        assert "catalog table state already matches source" in capsys.readouterr().out
        assert state_file.exists()
