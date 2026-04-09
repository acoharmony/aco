# © 2025 HarmonyCares
# All rights reserved.

"""
Comprehensive table metadata management system with DRY principles and inheritance.

 a sophisticated table metadata management framework that enables
flexible, maintainable data transformation definitions. It implements the
DRY (Don't Repeat Yourself) principle through table inheritance, template
expansion, and pipeline generation from declarative configurations.

Core Concepts:
    - **Table Inheritance**: Tables can inherit from staging/base tables,
      reducing duplication and ensuring consistency
    - **Pipeline Expansion**: Declarative configurations (dedup, ADR) are
      automatically expanded into executable transformation pipelines
    - **Stage Management**: Single table definition includes all processing stages
      (staging, deduplication, ADR, standardization)
    - **Column Merging**: Intelligent merging of column definitions from
      base and derived tables

Table Structure:
    Table metadata are YAML files in the _schemas directory containing:
    - Basic metadata (name, description)
    - Column definitions with types and transformations
    - Processing configurations (deduplication, ADR, standardization)
    - Storage specifications
    - Optional inheritance from staging tables

Inheritance Model:
    Processed tables can inherit from staging tables:
    - institutional_claim inherits from cclf1
    - physician_claim inherits from cclf5
    - This allows reusing column definitions and configurations

Pipeline Generation:
    Tables define transformations declaratively:
    ```yaml
    deduplication:
      key: [claim_id, line_number]
      sort_by: [file_date]
    adr:
      adjustment_column: adjustment_type
      amount_fields: [payment_amount, allowed_amount]
    ```
    These are expanded into an ordered execution pipeline.

    # Get transformation pipeline

    # Validate table metadata completeness

File Organization:
    src/acoharmony/_schemas/
    ├── cclf0.yml          # CCLF Summary file
    ├── cclf1.yml          # Part A institutional claims
    ├── cclf5.yml          # Part B physician claims
    ├── alr.yml            # Assignment List Report
    └── ...

Note:
    This module is central to ACOHarmony's flexibility, allowing
    new data types and transformations to be added through
    configuration rather than code changes.
"""

import copy
from pathlib import Path
from typing import Any

from ._registry import SchemaRegistry


class TableManager:
    """
    Central manager for table metadata definitions with inheritance and expansion.

        TableManager provides a complete framework for loading, inheriting,
        expanding, and validating data transformation table metadata. It implements
        sophisticated features like table inheritance, pipeline generation,
        and stage-specific column management.

        Table Inheritance:
            Processed tables can inherit from staging tables to reuse:
            - Column definitions
            - File format specifications
            - Key configurations
            - Transformation settings

            The inheritance is intelligent - child tables can:
            - Override specific columns
            - Add new columns
            - Replace entire sections
            - Inherit selectively

        Pipeline Expansion:
            Declarative configurations are expanded into execution pipelines:
            1. Staging (data loading)
            2. Deduplication (remove duplicates)
            3. ADR (adjustments, dedup, ranking)
            4. Standardization (final formatting)

            Each stage becomes an executable transformation step.

        Attributes:
            schemas_dir: Path to directory containing table metadata YAML files
            _table_cache: In-memory cache of loaded table metadata

        Example Usage:

        Table Cache:
            All table metadata loaded once at initialization and cached.
            Deep copies are returned to prevent mutation of cached data.

        Thread Safety:
            The manager is read-only after initialization, making it
            safe for concurrent access from multiple threads.

        Performance:
            - O(1) table lookups from cache
            - Deep copying ensures isolation but has overhead
            - Consider implementing copy-on-write for large tables
    """

    def __init__(self, schemas_dir: Path | None = None):
        """
        Initialize the table metadata manager.

                Creates a new TableManager instance that loads and caches all
                table metadata definitions from the SchemaRegistry (populated
                by _tables Pydantic models). Table metadata is loaded once at
                initialization for performance.

                Args:
                    schemas_dir: Deprecated. Kept for backward compatibility but
                                ignored. All metadata comes from SchemaRegistry.

                Note:
                    The manager loads all table metadata eagerly to fail fast on
                    configuration errors and provide quick access during processing.
        """
        # Initialize table metadata cache
        self._table_cache = {}
        # Backward compatibility alias
        self._schema_cache = self._table_cache

        # Load all table metadata into cache
        self._load_all_tables()

    def _load_all_tables(self):
        """
        Load all table metadata definitions from the SchemaRegistry.

                Reads schema definitions registered via _tables Pydantic model
                decorators and populates the table cache.

                Cache Structure:
                    The cache maps table names to their definitions:
                    {
                        'cclf1': {...table content...},
                        'physician_claim': {...table content...},
                    }

                Note:
                    This is a private method called during initialization.
                    Table metadata is immutable after loading.
        """
        # Ensure _tables models are imported so SchemaRegistry is populated
        from . import _tables as _  # noqa: F401

        for schema_name in SchemaRegistry.list_schemas():
            table = SchemaRegistry.get_full_table_config(schema_name)
            if table and "name" in table:
                self._table_cache[table["name"]] = table

    def get_table_metadata(self, name: str) -> dict[str, Any] | None:
        """
        Retrieve raw table metadata definition by name.

                Returns a deep copy of the cached table metadata to prevent mutation
                of the original. This is the basic table definition as specified in the
                YAML file, without inheritance or expansion.

                Args:
                    name: Name of the table to retrieve (must match the 'name'
                          field in the table YAML file).

                Returns:
                    Optional[Dict[str, Any]]: Deep copy of the table metadata dictionary
                                              if found, None if table doesn't exist.

                Use Cases:
                    - Inspect raw table definition
                    - Base for inheritance and expansion
                    - Table introspection and tooling

                Note:
                    Returns deep copy to ensure isolation. Modifications to
                    the returned table don't affect the cached version.
        """
        return copy.deepcopy(self._table_cache.get(name))

    def expand_table(self, name: str) -> dict[str, Any]:
        """
        Expand table metadata with inheritance and transformation pipeline.

                This is the main table processing method that takes raw table metadata
                and applies inheritance, expands configurations, and generates
                the transformation pipeline. It transforms declarative table definitions
                into executable transformation specifications.

                Processing Steps:
                    1. Load base table metadata by name
                    2. Apply staging inheritance if specified
                    3. Merge column definitions
                    4. Expand transformation configurations into pipeline stages
                    5. Generate ordered execution pipeline

                Args:
                    name: Name of the table to expand

                Returns:
                    Dict[str, Any]: Fully expanded table metadata with:
                                   - Inherited columns and configurations
                                   - Generated transformation pipeline
                                   - Merged settings from parent tables

                Raises:
                    ValueError: If table with given name is not found

                Inheritance Example:
                    If institutional_claim has 'staging: cclf1':
                    1. Loads cclf1 table as base
                    2. Inherits columns, file_format, keys
                    3. Applies institutional_claim overrides
                    4. Generates combined pipeline

                Transformation Expansion:
                    Declarative configs like:
                    ```yaml
                    deduplication:
                      key: [claim_id]
                    ```
                    Become pipeline stages with full specifications.

                Note:
                    This method is idempotent - calling multiple times
                    with the same table name produces identical results.
        """
        table = self.get_table_metadata(name)
        if not table:
            raise ValueError(f"Table {name} not found")

        # Handle staging inheritance (e.g., institutional_claim from cclf1)
        if "staging" in table:
            staging_table = self.get_table_metadata(table["staging"])
            if staging_table:
                table = self._inherit_from_staging(staging_table, table)

        # Expand transformations based on configuration
        table = self._expand_transformations(table)

        return table

    def _inherit_from_staging(self, staging: dict, processed: dict) -> dict:
        """
        Apply inheritance from a staging/base table to a processed table.

                Implements table inheritance by merging configurations from a
                parent (staging) table into a child (processed) table. The
                child table can override any inherited settings.

                Inheritance Rules:
                    - Columns: Merged with child overrides
                    - Deduplication: Inherited if not defined in child
                    - ADR: Inherited if not defined in child
                    - File format: Inherited for reading source data
                    - Other fields: Child takes precedence

                Args:
                    staging: Base/parent table to inherit from (e.g., cclf1)
                    processed: Child table that inherits (e.g., institutional_claim)

                Returns:
                    Dict: New table metadata combining both with inheritance applied

                Merge Behavior:
                    - Columns: Intelligent merge preserving order
                    - Configs: Child completely replaces parent if defined
                    - Missing sections: Inherited from parent

                Column Merging:
                    - Base columns preserved in order
                    - Overrides applied by name match
                    - New columns added at end

                Note:
                    Creates deep copies to prevent mutation of input tables.
        """
        result = copy.deepcopy(processed)

        # Inherit columns if not defined
        if "columns" not in result and "columns" in staging:
            result["columns"] = copy.deepcopy(staging["columns"])
        elif "columns" in staging and "columns" in result:
            # Merge columns - processed can override staging
            result["columns"] = self._merge_columns(staging["columns"], result["columns"])

        # Inherit deduplication config if not defined
        if "deduplication" not in result and "keys" in staging:
            if "deduplication_key" in staging["keys"]:
                result["deduplication"] = {"key": staging["keys"]["deduplication_key"]}

        # Inherit ADR config if not defined
        if "adr" not in result and "adr" in staging:
            result["adr"] = copy.deepcopy(staging["adr"])

        # Inherit file format for reading
        if "file_format" not in result and "file_format" in staging:
            result["file_format"] = copy.deepcopy(staging["file_format"])

        return result

    def _merge_columns(self, base_cols: list, override_cols: list) -> list:
        """
        Intelligently merge column definitions with overrides.

                Merges two lists of column definitions, allowing the override
                list to modify or extend the base list. Preserves order from
                base list while applying updates and additions.

                Args:
                    base_cols: List of base column definitions
                    override_cols: List of column definitions that override/extend base

                Returns:
                    List: Merged column definitions with:
                         - Base columns in original order
                         - Overrides applied to matching columns
                         - New columns appended at end

                Merge Algorithm:
                    1. Create map of base columns by name
                    2. Apply overrides to matching columns
                    3. Preserve base column order
                    4. Append new columns from overrides

                Column Matching:
                    Columns are matched by 'name' field only.
                    Override completely replaces base column definition.

                Order Preservation:
                    - Base column order maintained for existing columns
                    - New columns appear in override order at end
                    - No duplicates in result

                Note:
                    Deep copies ensure input lists are not modified.
        """
        # Create map of base columns
        col_map = {col["name"]: copy.deepcopy(col) for col in base_cols}

        # Apply overrides
        for col in override_cols:
            col_name = col["name"]
            if col_name in col_map:
                # Update existing column
                col_map[col_name].update(col)
            else:
                # Add new column
                col_map[col_name] = col

        # Return as list maintaining original order where possible
        result = []
        seen = set()

        # First add base columns in order (with overrides applied)
        for col in base_cols:
            if col["name"] in col_map and col["name"] not in seen:
                result.append(col_map[col["name"]])
                seen.add(col["name"])

        # Then add any new columns from overrides
        for col in override_cols:
            if col["name"] not in seen:
                result.append(col)
                seen.add(col["name"])

        return result

    def _expand_transformations(self, table: dict) -> dict:
        """
        Expand declarative transformation configurations into executable pipeline.

                Converts high-level transformation specifications (deduplication,
                ADR, standardization) into a detailed, ordered pipeline of
                transformation stages. Each declarative block becomes one or
                more pipeline stages with full configuration.

                Pipeline Generation Order:
                    1. Staging - Load data from source
                    2. Deduplication - Remove duplicate records
                    3. ADR Adjustment - Apply amount adjustments
                    4. ADR Deduplication - Secondary dedup with different keys
                    5. ADR Ranking - Rank records within groups
                    6. Standardization - Final formatting and cleanup

                Args:
                    table: Table metadata dictionary with declarative configurations

                Returns:
                    Dict: Table metadata with added 'pipeline' list containing ordered
                         transformation stages

                Stage Structure:
                    Each pipeline stage contains:
                    {
                        'stage': 'stage_name',
                        'description': 'Human-readable description',
                        'transformation': 'transformation_type',
                        'config': {...}  # Stage-specific configuration
                    }

                Deduplication Expansion:
                    Input:
                        deduplication:
                          key: [claim_id, line]
                          sort_by: [date]
                    Output:
                        Stage with full dedup configuration

                ADR Expansion:
                    The ADR block can generate multiple stages:
                    1. Adjustment stage (if adjustment_column present)
                    2. Deduplication stage (if key_columns present)
                    3. Ranking stage (if rank_by present)

                Configuration Preservation:
                    All configuration from declarative blocks is preserved
                    in the generated pipeline stages for execution.

                Note:
                    Modifies table in place by adding 'pipeline' field.
                    Original declarative configurations remain unchanged.
        """
        if "pipeline" not in table:
            table["pipeline"] = []

        # Stage 1: Raw to staging (if needed)
        if "staging" in table:
            table["pipeline"].append(
                {
                    "stage": "staging",
                    "description": f"Load from {table['staging']}",
                    "transformation": "read_staging",
                }
            )

        # Stage 2: Deduplication
        if "deduplication" in table:
            dedup_config = table["deduplication"]
            table["pipeline"].append(
                {
                    "stage": "deduplication",
                    "description": "Remove duplicate records",
                    "transformation": "deduplication",
                    "config": {
                        "key": dedup_config.get("key"),
                        "sort_by": dedup_config.get("sort_by"),
                        "keep": dedup_config.get("keep", "last"),
                    },
                }
            )

        # Stage 3: ADR (Adjustment, Deduplication, Ranking)
        if "adr" in table:
            adr_config = table["adr"]

            # Adjustment phase
            if "adjustment_column" in adr_config:
                table["pipeline"].append(
                    {
                        "stage": "adjustment",
                        "description": "Apply claim adjustments",
                        "transformation": "adjustment",
                        "config": {
                            "adjustment_column": adr_config["adjustment_column"],
                            "amount_fields": adr_config.get("amount_fields", []),
                        },
                    }
                )

            # Deduplication phase (different from general dedup)
            if "key_columns" in adr_config:
                table["pipeline"].append(
                    {
                        "stage": "adr_deduplication",
                        "description": "ADR deduplication by key",
                        "transformation": "deduplication",
                        "config": {
                            "key": adr_config["key_columns"],
                            "sort_by": adr_config.get("sort_columns", []),
                            "sort_order": adr_config.get("sort_descending", []),
                            "keep": "first",  # ADR typically keeps first after sorting
                        },
                    }
                )

            # Ranking phase
            if "rank_by" in adr_config:
                table["pipeline"].append(
                    {
                        "stage": "ranking",
                        "description": "Rank records within groups",
                        "transformation": "ranking",
                        "config": {
                            "partition_by": adr_config.get("rank_partition", []),
                            "order_by": adr_config["rank_by"],
                        },
                    }
                )

        # Stage 4: Final standardization
        if "standardization" in table:
            table["pipeline"].append(
                {
                    "stage": "standardization",
                    "description": "Apply final standardization",
                    "transformation": "standardization",
                    "config": table["standardization"],
                }
            )

        return table

    def get_transformation_pipeline(self, table_name: str) -> list[dict]:
        """
        Get the complete transformation pipeline for a schema.

                Retrieves the ordered list of transformation stages generated
                from the schema's declarative configuration. This pipeline
                can be executed sequentially to transform raw data.

                Args:
                    table_name: Name of the table to get pipeline for

                Returns:
                    List[Dict]: Ordered list of transformation stages, each containing:
                               - stage: Stage identifier
                               - description: Human-readable description
                               - transformation: Type of transformation
                               - config: Stage-specific configuration

                Raises:
                    ValueError: If table not found (via expand_table)

                Pipeline Execution:
                    The returned pipeline is designed for sequential execution:
                    ```python
                    for stage in pipeline:
                    ```

                Note:
                    Pipeline is generated fresh each call via expand_table,
                    ensuring it reflects the latest table configuration.
        """
        table = self.expand_table(table_name)
        return table.get("pipeline", [])

    def get_output_columns(self, table_name: str, stage: str | None = None) -> list[dict]:
        """
        Get column definitions for a table at a specific processing stage.

                Returns the list of columns that should exist after a particular
                transformation stage. Supports stage-specific column configurations
                where different stages may have different columns.

                Args:
                    table_name: Name of the table to get columns for
                    stage: Optional stage identifier. Common values:
                          - None: Final columns (default)
                          - 'staging': Initial columns from source
                          - 'dedup': Columns after deduplication
                          - 'adr': Columns after ADR processing
                          - 'final': Final output columns

                Returns:
                    List[Dict]: List of column definitions, each containing:
                               - name: Column name
                               - data_type: Data type
                               - description: Optional description
                               - Additional column properties

                Stage-Specific Columns:
                    Tables can define different columns for different stages:
                    ```yaml
                    stages:
                      dedup:
                        columns:
                          - name: dedup_flag
                            type: boolean
                      final:
                        columns:
                          # Remove temporary columns
                          - name: dedup_flag
                            keep: false
                    ```

                Inheritance:
                    Stage-specific columns are merged with base columns
                    using the same merge logic as schema inheritance.

                Use Cases:
                    - Validation: Verify columns exist after each stage
                    - Documentation: Generate stage-specific schemas
                    - Testing: Validate transformation outputs

                Note:
                    Returns empty list if table not found rather than raising error.
        """
        table = self.expand_table(table_name)

        # If no stage specified, return final columns
        if not stage:
            return table.get("columns", [])

        # Check if there are stage-specific column modifications
        if "stages" in table and stage in table["stages"]:
            stage_config = table["stages"][stage]
            if "columns" in stage_config:
                # Merge with base columns
                base_cols = table.get("columns", [])
                return self._merge_columns(base_cols, stage_config["columns"])

        # Default to base columns
        return table.get("columns", [])

    def validate_table(self, table_name: str) -> dict[str, Any]:
        """
        Validate table metadata for completeness and correctness.

                Performs comprehensive validation of table metadata including:
                - Required field presence
                - Column definition completeness
                - Configuration validity
                - Inheritance resolution

                Args:
                    table_name: Name of the table to validate

                Returns:
                    Dict[str, Any]: Validation result containing:
                                   - valid: Boolean indicating if table is valid
                                   - table: Name of validated table
                                   - issues: List of validation issues found
                                   - error: Error message if schema couldn't be loaded

                Validation Checks:
                    1. Table exists and can be expanded
                    2. Required fields present (name, description, storage)
                    3. Has columns (directly or via inheritance)
                    4. Column definitions complete (name, data_type)
                    5. Deduplication config valid (has key)
                    6. ADR config valid (has key_columns)

                Issue Examples:
                    - "Missing required field: description"
                    - "Column claim_id missing data_type"
                    - "Deduplication config missing key"
                    - "Schema must have columns or inherit from staging"

                Use Cases:
                    - Pre-deployment validation
                    - Schema development assistance
                    - CI/CD pipeline checks
                    - Migration validation

                Note:
                    Validation is non-destructive and doesn't modify tables.
                    Consider caching validation results for large table sets.
        """
        try:
            table = self.expand_table(table_name)
        except Exception as e:  # ALLOWED: Validation function returns error dict instead of raising
            return {"valid": False, "table": table_name, "error": str(e)}

        issues = []

        # Required fields
        required = ["name", "description", "storage"]
        for field in required:
            if field not in table:
                issues.append(f"Missing required field: {field}")

        # Must have columns (either directly or via staging)
        if "columns" not in table and "staging" not in table:
            issues.append("Table must have columns or inherit from staging")

        # Validate columns if present
        if "columns" in table:
            for i, col in enumerate(table["columns"]):
                if "name" not in col:
                    issues.append(f"Column {i} missing name")
                if "data_type" not in col:
                    col_name = col.get("name", f"#{i}")
                    issues.append(f"Column {col_name} missing data_type")

        # Validate deduplication if present
        if "deduplication" in table:
            if "key" not in table["deduplication"]:
                issues.append("Deduplication config missing key")

        # Validate ADR if present
        if "adr" in table:
            if "key_columns" not in table["adr"]:
                issues.append("ADR config missing key_columns")

        return {"valid": len(issues) == 0, "table": table_name, "issues": issues}
