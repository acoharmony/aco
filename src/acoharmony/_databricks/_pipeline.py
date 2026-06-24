# © 2025 HarmonyCares
# All rights reserved.

"""Databricks publishing pipeline orchestration."""

from __future__ import annotations

import argparse
import sys
from time import perf_counter

from .._log import LogWriter
from ._uc_tables import (
    WAREHOUSE_ID_ENV_VAR,
    create_tables,
    resolve_warehouse_id,
    warehouse_id_source,
)
from ._uc_volume import copy_to_uc_volumes

DEFAULT_PIPELINE_ACO_PROFILE = "local"
PIPELINE_NAME = "databricks"

logger = LogWriter("pipeline.databricks")


def selected_pipeline_layers(args: argparse.Namespace) -> list[str] | None:
    layers = getattr(args, "layers", None)
    return list(layers) if layers else None


def pipeline_aco_profile(args: argparse.Namespace) -> str:
    return getattr(args, "aco_profile", None) or DEFAULT_PIPELINE_ACO_PROFILE


def copy_volume_args(args: argparse.Namespace, *, layer: str) -> argparse.Namespace:
    return argparse.Namespace(
        aco_profile=pipeline_aco_profile(args),
        concurrency=args.concurrency,
        databricks_bin=args.databricks_bin,
        destination=None,
        dry_run=args.dry_run,
        force=bool(getattr(args, "force", False)),
        layer=layer,
        overwrite=True,
        pipeline_name=PIPELINE_NAME,
        profile=args.profile,
        skip_mkdir=False,
        source=None,
        state_file=args.state_file,
        target=args.target,
        log_writer=logger,
    )


def create_tables_args(args: argparse.Namespace) -> argparse.Namespace:
    warehouse_id = resolve_warehouse_id(args)
    return argparse.Namespace(
        aco_profile=pipeline_aco_profile(args),
        catalog=None,
        databricks_bin=args.databricks_bin,
        dry_run=args.dry_run,
        duplicate_strategy="prefix-volume",
        force=bool(getattr(args, "force", False)),
        include_volume_prefix=False,
        layers=selected_pipeline_layers(args),
        no_recurse=args.no_recurse,
        poll_interval_seconds=args.poll_interval_seconds,
        pipeline_name=PIPELINE_NAME,
        profile=args.profile,
        replace_existing=True,
        schema=None,
        skip_missing_roots=args.skip_missing_roots,
        state_file=args.state_file,
        table_mode=args.table_mode,
        table_prefix="",
        target=args.target,
        volume_fqns=[],
        volume_roots=[],
        wait_timeout_seconds=args.wait_timeout_seconds,
        warehouse_id=warehouse_id,
        log_writer=logger,
    )


def warn_missing_warehouse_id(args: argparse.Namespace) -> None:
    if args.dry_run or resolve_warehouse_id(args) is not None:
        return

    logger.warning(
        "Databricks SQL warehouse ID is not configured for pipeline table update",
        action="resolve_warehouse_id",
        pipeline=PIPELINE_NAME,
        step="create-tables",
        env_var=WAREHOUSE_ID_ENV_VAR,
        fallback="local_spark",
    )
    print(
        f"WARNING: {WAREHOUSE_ID_ENV_VAR} is not set and --warehouse-id was not passed; "
        "falling back to local Spark if available.",
        file=sys.stderr,
    )


def run_databricks_pipeline(args: argparse.Namespace) -> int:
    if args.copy_only and args.tables_only:
        logger.error(
            "Invalid Databricks pipeline options",
            action="validate_pipeline",
            pipeline=PIPELINE_NAME,
            copy_only=args.copy_only,
            tables_only=args.tables_only,
        )
        print("Choose only one of --copy-only or --tables-only.")
        return 2

    started = perf_counter()
    layers = selected_pipeline_layers(args)
    copy_layers = layers or ["all"]
    logger.info(
        "Starting Databricks pipeline",
        action="start_pipeline",
        pipeline=PIPELINE_NAME,
        aco_profile=pipeline_aco_profile(args),
        databricks_profile=args.profile,
        target=args.target,
        warehouse_id=resolve_warehouse_id(args),
        warehouse_id_source=warehouse_id_source(args),
        layers=layers or ["bronze", "silver", "gold"],
        copy_only=args.copy_only,
        tables_only=args.tables_only,
        dry_run=args.dry_run,
        force=bool(getattr(args, "force", False)),
        table_mode=args.table_mode,
    )

    if not args.tables_only:
        print(
            "Syncing local medallion files to Unity Catalog volumes "
            f"with ACO profile {pipeline_aco_profile(args)!r}."
        )
        for layer in copy_layers:
            step_started = perf_counter()
            logger.info(
                "Starting Databricks volume sync step",
                action="start_step",
                pipeline=PIPELINE_NAME,
                step="copy-volume",
                layer=layer,
            )
            try:
                result = copy_to_uc_volumes(copy_volume_args(args, layer=layer))
            except Exception as exc:  # noqa: BLE001 - CLI should return clean failures.
                result = 1
                logger.error(
                    "Databricks volume sync step failed",
                    action="complete_step",
                    pipeline=PIPELINE_NAME,
                    step="copy-volume",
                    layer=layer,
                    success=False,
                    exit_code=result,
                    error_type=type(exc).__name__,
                    error=str(exc),
                    duration_seconds=round(perf_counter() - step_started, 3),
                )
                print(f"ERROR: Databricks volume sync failed for {layer}: {exc}", file=sys.stderr)
            logger.info(
                "Completed Databricks volume sync step",
                action="complete_step",
                pipeline=PIPELINE_NAME,
                step="copy-volume",
                layer=layer,
                success=result == 0,
                exit_code=result,
                duration_seconds=round(perf_counter() - step_started, 3),
            )
            if result != 0:
                logger.error(
                    "Databricks pipeline failed during volume sync",
                    action="complete_pipeline",
                    pipeline=PIPELINE_NAME,
                    step="copy-volume",
                    layer=layer,
                    success=False,
                    exit_code=result,
                    duration_seconds=round(perf_counter() - started, 3),
                )
                return result

    if args.copy_only:
        logger.info(
            "Completed Databricks pipeline",
            action="complete_pipeline",
            pipeline=PIPELINE_NAME,
            success=True,
            exit_code=0,
            duration_seconds=round(perf_counter() - started, 3),
        )
        return 0

    warn_missing_warehouse_id(args)
    step_started = perf_counter()
    logger.info(
        "Starting Databricks table update step",
        action="start_step",
        pipeline=PIPELINE_NAME,
        step="create-tables",
        layers=layers or ["bronze", "silver", "gold"],
    )
    print("Updating Unity Catalog tables from changed parquet sources.")
    try:
        result = create_tables(create_tables_args(args))
    except Exception as exc:  # noqa: BLE001 - CLI should return clean failures.
        result = 1
        logger.error(
            "Databricks table update step failed",
            action="complete_step",
            pipeline=PIPELINE_NAME,
            step="create-tables",
            success=False,
            exit_code=result,
            error_type=type(exc).__name__,
            error=str(exc),
            duration_seconds=round(perf_counter() - step_started, 3),
        )
        print(f"ERROR: Databricks table update failed: {exc}", file=sys.stderr)
    logger.info(
        "Completed Databricks table update step",
        action="complete_step",
        pipeline=PIPELINE_NAME,
        step="create-tables",
        success=result == 0,
        exit_code=result,
        duration_seconds=round(perf_counter() - step_started, 3),
    )
    logger.log(
        "INFO" if result == 0 else "ERROR",
        "Completed Databricks pipeline",
        action="complete_pipeline",
        pipeline=PIPELINE_NAME,
        success=result == 0,
        exit_code=result,
        duration_seconds=round(perf_counter() - started, 3),
    )
    return result


def cmd_databricks_pipeline(args: argparse.Namespace) -> int:
    """Entry point used by ``aco pipeline databricks``."""
    return run_databricks_pipeline(args)
