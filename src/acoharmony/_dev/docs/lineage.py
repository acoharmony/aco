#!/usr/bin/env python3
# © 2025 HarmonyCares
# All rights reserved.

"""Generate data lineage documentation from schema dependencies."""

from datetime import datetime
from pathlib import Path

from acoharmony._log import get_logger
from acoharmony._registry import SchemaRegistry

logger = get_logger("dev.data_lineage")


def load_all_schemas() -> dict[str, dict]:
    """
    Load all schema files and extract lineage information.

        Returns

        Dict[str, dict]
            Dictionary mapping schema names to their dependencies
        54
        {'depends': ['institutional_claim', 'physician_claim', 'dme_claim']}
        Raw schemas: 23
        Examples: ['cclf0', 'cclf1', 'cclf2', 'cclf3', 'cclf4']
        Derived schemas: 31
    """
    # Ensure _tables models are imported so SchemaRegistry is populated
    from acoharmony import _tables as _  # noqa: F401

    schemas = {}

    for schema_name in SchemaRegistry.list_schemas():
        config = SchemaRegistry.get_full_table_config(schema_name)
        if not config or "name" not in config:
            continue

        name = config["name"]
        depends = []

        # Extract dependencies from staging field
        staging = config.get("staging")
        if staging:
            if isinstance(staging, str):
                depends.append(staging)
            elif isinstance(staging, list):
                depends.extend(staging)

        # Extract dependencies from union sources
        if "union" in config and "sources" in config["union"]:
            sources = config["union"]["sources"]
            if isinstance(sources, list):
                depends.extend(sources)

        # Extract dependencies from pivot sources
        if "pivot" in config and "sources" in config["pivot"]:
            sources = config["pivot"]["sources"]
            if isinstance(sources, list):
                depends.extend(sources)

        # Extract dependencies from lineage
        lineage = config.get("lineage", {})
        if lineage.get("depends_on"):
            depends.extend(lineage["depends_on"])

        # Remove duplicates and store
        schemas[name] = {"depends": list(set(depends))}

    # Add base schemas that have no dependencies (raw data files)
    raw_schemas = [
        "cclf0",
        "cclf1",
        "cclf2",
        "cclf3",
        "cclf4",
        "cclf5",
        "cclf6",
        "cclf7",
        "cclf8",
        "cclf9",
        "cclfa",
        "cclfb",
        "alr",
        "bar",
        "tparc",
        "sva",
        "palmr",
        "pbvar",
        "zip_to_county",
        "provider_list",
        "salesforce_account",
        "mailed",
        "recon",
        "hcmpi_master",
    ]

    for raw_schema in raw_schemas:
        if raw_schema not in schemas:
            schemas[raw_schema] = {"depends": []}

    logger.debug(f"Loaded {len(schemas)} schemas with dependencies")
    return schemas


def find_downstream(schemas: dict[str, dict], schema_name: str) -> set[str]:
    """
    Find all schemas that depend on the given schema.

        Parameters

        schemas : Dict[str, dict]
            Schema dependency dictionary
        schema_name : str
            Name of schema to find downstream dependencies for

        Returns

        Set[str]
            Set of schema names that depend on the given schema
        Schemas depending on CCLF1: ['institutional_claim', 'medical_claim', 'parta_claims_header']
        Impact count: 8 schemas
        Affected: ['eligibility', 'enrollment', 'enrollment_status', 'hcmpi_master', 'monthly_report']
        WARNING: Changes to enrollment affect consolidated_alignment!
    """
    downstream = set()

    for name, lineage in schemas.items():
        if schema_name in lineage.get("depends", []):
            downstream.add(name)
            # Recursively find downstream of downstream
            downstream.update(find_downstream(schemas, name))

    return downstream


def find_upstream(schemas: dict[str, dict], schema_name: str) -> set[str]:
    """
    Find all schemas that this schema depends on.

        Parameters

        schemas : Dict[str, dict]
            Schema dependency dictionary
        schema_name : str
            Name of schema to find upstream dependencies for

        Returns

        Set[str]
            Set of schema names that this schema depends on
        Medical claim depends on: ['cclf1', 'cclf5', 'cclf6', 'dme_claim', 'institutional_claim', 'physician_claim']
        Total dependencies: 15
        Raw CCLF files needed: ['cclf1', 'cclf5', 'cclf6', 'cclf8']
        CCLF1 is a raw source file with no dependencies
    """
    upstream = set()

    if schema_name in schemas:
        deps = schemas[schema_name].get("depends", [])
        upstream.update(deps)

        # Recursively find upstream of upstream
        for dep in deps:
            upstream.update(find_upstream(schemas, dep))

    return upstream


def generate_data_lineage():
    """
    Generate data lineage documentation in docs folder.

        Returns

        bool
            True if successful, False otherwise
        [OK] Data lineage documentation generated
          Output: docs/DATA_LINEAGE.md
        Documentation has 195 lines
        Includes Mermaid diagram: True
        Lineage docs updated successfully
    """
    logger.info("Starting data lineage documentation generation")

    docs_dir = Path("docs")
    docs_dir.mkdir(exist_ok=True)

    # Load schemas
    schemas = load_all_schemas()
    logger.info(f"Loaded {len(schemas)} schemas with lineage information")

    if not schemas:
        logger.error("No schemas found to generate lineage")
        return False

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    content = ["# Data Lineage Documentation\n"]
    content.append(f"*Generated on {timestamp}*\n")

    # Overview
    content.append("\n## Overview\n")
    content.append("This document provides a comprehensive view of data lineage and dependencies ")
    content.append("in the ACO Harmony data processing pipeline.\n")

    # Statistics
    content.append("\n## Statistics\n")

    # Count by category
    raw_count = 0
    processed_count = 0
    report_count = 0
    max_deps = 0
    most_complex = None

    for name, lineage in schemas.items():
        deps = lineage.get("depends", [])

        if not deps:
            raw_count += 1
        elif "report" in name or "engagement" in name or "pivot" in name:
            report_count += 1
        else:
            processed_count += 1

        if len(deps) > max_deps:
            max_deps = len(deps)
            most_complex = name

    content.append(f"- **Total schemas**: {len(schemas)}\n")
    content.append(f"- **Raw sources**: {raw_count}\n")
    content.append(f"- **Processed/Derived**: {processed_count}\n")
    content.append(f"- **Reports/Analytics**: {report_count}\n")
    content.append(f"- **Most complex schema**: {most_complex} ({max_deps} dependencies)\n")

    # Critical schemas (high downstream impact)
    content.append("\n### Critical Schemas (High Downstream Impact)\n")
    critical_schemas = []
    for name in schemas:
        downstream = find_downstream(schemas, name)
        if len(downstream) > 5:
            critical_schemas.append((name, len(downstream)))

    if critical_schemas:
        content.append("\n| Schema | Downstream Impact |\n")
        content.append("|--------|------------------|\n")
        for schema, count in sorted(critical_schemas, key=lambda x: x[1], reverse=True)[:10]:
            content.append(f"| {schema} | {count} schemas |\n")

    # Data Flow Tree
    content.append("\n## Data Flow Tree\n")

    # Find root nodes (no dependencies)
    roots = []
    for name, lineage in schemas.items():
        if not lineage.get("depends"):
            roots.append(name)

    # Group by type
    raw_files = []
    reference_data = []

    for root in sorted(roots):
        if "cclf" in root or root in ["alr", "bar", "palmr", "pbvar", "sva", "tparc"]:
            raw_files.append(root)
        else:
            reference_data.append(root)

    content.append("\n### Raw Data Files (from CMS)\n")
    content.append("```\n")
    for name in sorted(raw_files):
        content.append(f"📁 {name}\n")
        # Find direct dependents
        dependents = []
        for dep_name, lineage in schemas.items():
            if name in lineage.get("depends", []):
                dependents.append(dep_name)
        for dep in sorted(dependents)[:5]:  # Limit to first 5
            content.append(f"   └── {dep}\n")
        if len(dependents) > 5:
            content.append(f"   └── ... and {len(dependents) - 5} more\n")
    content.append("```\n")

    if reference_data:
        content.append("\n### Reference Data\n")
        content.append("```\n")
        for name in sorted(reference_data):
            content.append(f"📚 {name}\n")
            # Find direct dependents
            dependents = []
            for dep_name, lineage in schemas.items():
                if name in lineage.get("depends", []):
                    dependents.append(dep_name)
            for dep in sorted(dependents)[:3]:  # Limit to first 3
                content.append(f"   └── {dep}\n")
            if len(dependents) > 3:
                content.append(f"   └── ... and {len(dependents) - 3} more\n")
        content.append("```\n")

    # Dependency Matrix for key schemas
    content.append("\n## Key Schema Dependencies\n")

    key_schemas = [
        "consolidated_alignment",
        "medical_claim",
        "pharmacy_claim",
        "enrollment",
        "eligibility",
        "institutional_claim",
    ]

    for schema in key_schemas:
        if schema in schemas:
            content.append(f"\n### {schema}\n")

            upstream = find_upstream(schemas, schema)
            downstream = find_downstream(schemas, schema)

            if upstream:
                content.append("\n**Depends on:**\n")
                for dep in sorted(upstream)[:10]:
                    content.append(f"- {dep}\n")
                if len(upstream) > 10:
                    content.append(f"- ... and {len(upstream) - 10} more\n")

            if downstream:
                content.append("\n**Used by:**\n")
                for dep in sorted(downstream)[:10]:
                    content.append(f"- {dep}\n")
                if len(downstream) > 10:
                    content.append(f"- ... and {len(downstream) - 10} more\n")

    # Mermaid diagram
    content.append("\n## Lineage Visualization\n")
    content.append("\n```mermaid\n")
    content.append("flowchart LR\n")
    content.append("  classDef raw fill:#e1f5fe,stroke:#01579b,stroke-width:2px\n")
    content.append("  classDef processed fill:#f3e5f5,stroke:#4a148c,stroke-width:2px\n")
    content.append("  classDef consolidated fill:#e8f5e9,stroke:#1b5e20,stroke-width:2px\n")
    content.append("  classDef report fill:#fff3e0,stroke:#e65100,stroke-width:2px\n\n")

    # Add simplified graph (only show key relationships)
    # Build relationships from actual dependencies
    key_relationships = []

    # First add relationships for key processing chains
    key_chains = [
        # CCLF1 chain (institutional claims)
        "cclf1",
        "institutional_claim",
        "parta_claims_header",
        # CCLF2 chain (revenue center)
        "cclf2",
        "parta_claims_revenue_center_detail",
        "revenue_center",
        # CCLF3 chain (procedure codes)
        "cclf3",
        "parta_procedure_code",
        "procedure",
        # CCLF4 chain (diagnosis codes)
        "cclf4",
        "parta_diagnosis_code",
        "diagnosis",
        # CCLF5 chain (physician claims)
        "cclf5",
        "physician_claim",
        "partb_physicians",
        # CCLF6 chain (DME claims)
        "cclf6",
        "dme_claim",
        "partb_dme",
        # CCLF7 chain (Part D claims)
        "cclf7",
        "partd_claims",
        # CCLF8 chain (beneficiary demographics)
        "cclf8",
        "beneficiary_demographics",
        "enrollment",
        # CCLF9 chain (beneficiary xref)
        "cclf9",
        "beneficiary_xref",
        "beneficiary_mbi_mapping",
        # Medical claim union
        "medical_claim",
        # Eligibility and alignment
        "eligibility",
        "consolidated_alignment",
        # ALR/BAR
        "alr",
        "bar",
    ]

    # Build relationships from dependencies
    for schema_name in key_chains:
        if schema_name in schemas:
            deps = schemas[schema_name].get("depends", [])
            for dep in deps:
                if dep in key_chains:
                    key_relationships.append((dep, schema_name))

    # Build the graph with proper syntax
    # Track which nodes belong to which class
    raw_nodes = []
    processed_nodes = []
    consolidated_nodes = []
    report_nodes = []

    for source, target in key_relationships:
        if source in schemas and target in schemas:
            # Use the node IDs directly - underscores are allowed
            content.append(f"  {source} --> {target}\n")

            # Classify source node
            if source not in (raw_nodes + processed_nodes + consolidated_nodes + report_nodes):
                is_raw = source in schemas and not schemas[source].get("depends", [])
                if (
                    is_raw
                    or "cclf" in source
                    or source in ["alr", "bar", "palmr", "pbvar", "sva", "tparc"]
                ):
                    raw_nodes.append(source)
                elif "consolidated" in source:
                    consolidated_nodes.append(source)
                elif "report" in source or "engagement" in source:  # pragma: no cover
                    report_nodes.append(source)
                else:
                    processed_nodes.append(source)

            # Classify target node
            if target not in (raw_nodes + processed_nodes + consolidated_nodes + report_nodes):
                is_raw = target in schemas and not schemas[target].get("depends", [])
                if (
                    is_raw
                    or "cclf" in target
                    or target in ["alr", "bar", "palmr", "pbvar", "sva", "tparc"]
                ):
                    raw_nodes.append(target)
                elif "consolidated" in target:
                    consolidated_nodes.append(target)
                elif "report" in target or "engagement" in target:  # pragma: no cover
                    report_nodes.append(target)
                else:
                    processed_nodes.append(target)

    # Apply classes using the proper class statement
    content.append("\n")
    if raw_nodes:
        content.append(f"  class {','.join(raw_nodes)} raw\n")
    if processed_nodes:
        content.append(f"  class {','.join(processed_nodes)} processed\n")
    if consolidated_nodes:
        content.append(f"  class {','.join(consolidated_nodes)} consolidated\n")
    if report_nodes:  # pragma: no cover
        content.append(f"  class {','.join(report_nodes)} report\n")

    content.append("```\n")

    # Write to docs folder
    output_path = docs_dir / "DATA_LINEAGE.md"
    try:
        with open(output_path, "w") as f:
            f.write("".join(content))

        logger.info(f"Successfully generated data lineage documentation at {output_path}")
        logger.info(f"Documented {len(schemas)} schemas with dependencies")
        return True

    except Exception as e:  # ALLOWED: Returns False to indicate error as part of API contract
        logger.error(f"Failed to write data lineage documentation: {e}")
        return False
