# © 2025 HarmonyCares
# All rights reserved.
# ruff: noqa
# ruff: noqa

"""
CCLF Guide Documentation Generator V2.

Enhanced version with:
- Complete structure preservation (TOC, links, anchors)
- Tuva model integration
- Section-wise insertion (no replacement)
- Comprehensive code extraction
- Coverage reporting

"""

import re
import yaml
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class TOCEntry:
    """Table of contents entry."""

    level: int
    title: str
    anchor: str
    line_number: int


@dataclass(frozen=True)
class Section:
    """Document section with header and content."""

    header: str
    level: int
    anchor: str
    content: str
    start_line: int
    end_line: int


@dataclass(frozen=True)
class TuvaModel:
    """Tuva model information."""

    name: str
    type: str  # 'staging', 'intermediate', 'final'
    path: Path
    sql: str | None
    description: str
    depends_on: list[str] = field(default_factory=list)
    used_by: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CCLFFileInfo:
    """Complete CCLF file information."""

    file_num: str
    name: str
    schema_path: Path | None
    schema_content: dict[str, Any] | None
    parser_code: str | None
    transform_code: str | None
    tuva_staging_models: list[TuvaModel] = field(default_factory=list)
    tuva_intermediate_models: list[TuvaModel] = field(default_factory=list)
    cli_examples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class ValidationReport:
    """Validation and coverage report."""

    total_cclf_files: int
    documented_files: int
    total_anchors: int
    broken_links: list[str]
    missing_sections: list[str]
    tuva_models_found: int
    code_examples_count: int
    coverage_percent: float


class StructurePreserver:
    """Preserves original guide structure."""

    def extract_toc(self, content: str) -> list[TOCEntry]:
        """
        Extract table of contents entries.

                Finds both explicit TOC links and implicit headers.
        """
        toc_entries = []
        lines = content.split('\n')

        # Pattern for TOC links: [Title](#anchor) or [Section Number. Title](#anchor)
        toc_pattern = r'\[([^\]]+)\]\(#([^\)]+)\)'

        for i, line in enumerate(lines):
            matches = re.findall(toc_pattern, line)
            for title, anchor in matches:
                # Determine level from indentation or numbering
                level = self._determine_level(title, line)
                toc_entries.append(TOCEntry(
                    level=level,
                    title=title,
                    anchor=anchor,
                    line_number=i
                ))

        return toc_entries

    def _determine_level(self, title: str, line: str) -> int:
        """Determine heading level from title or indentation."""
        # Count leading spaces/dashes
        indent = len(line) - len(line.lstrip(' -'))

        # Or check numbering pattern (1. vs 1.1 vs 1.1.1)
        if re.match(r'^\d+\.', title):
            dots = title.split('.')[0:2]
            return len(dots)

        return max(1, indent // 2)

    def extract_sections(self, content: str) -> list[Section]:
        """Extract all sections with headers."""
        sections = []
        lines = content.split('\n')

        # Pattern for markdown headers
        header_pattern = r'^(#{1,6})\s+(.+)$'

        current_section = None
        section_content = []

        for i, line in enumerate(lines):
            match = re.match(header_pattern, line)

            if match:
                # Save previous section
                if current_section:
                    sections.append(Section(
                        header=current_section['header'],
                        level=current_section['level'],
                        anchor=current_section['anchor'],
                        content='\n'.join(section_content),
                        start_line=current_section['start_line'],
                        end_line=i - 1
                    ))

                # Start new section
                level = len(match.group(1))
                header_text = match.group(2).strip()
                anchor = self._generate_anchor(header_text)

                current_section = {
                    'header': header_text,
                    'level': level,
                    'anchor': anchor,
                    'start_line': i
                }
                section_content = []
            elif current_section:
                section_content.append(line)

        # Save last section
        if current_section:
            sections.append(Section(
                header=current_section['header'],
                level=current_section['level'],
                anchor=current_section['anchor'],
                content='\n'.join(section_content),
                start_line=current_section['start_line'],
                end_line=len(lines) - 1
            ))

        return sections

    def _generate_anchor(self, header_text: str) -> str:
        """Generate anchor from header text."""
        # Lowercase, replace spaces with hyphens, remove special chars
        anchor = header_text.lower()
        anchor = re.sub(r'[^\w\s-]', '', anchor)
        anchor = re.sub(r'[\s]+', '-', anchor)
        return anchor

    def validate_links(self, content: str) -> list[str]:
        """Find all broken internal links."""
        broken_links = []

        # Extract all anchors
        anchors = set()
        sections = self.extract_sections(content)
        for section in sections:
            anchors.add(section.anchor)

        # Also check explicit anchor definitions
        anchor_pattern = r'<a\s+name="([^"]+)"'
        for match in re.finditer(anchor_pattern, content):
            anchors.add(match.group(1))

        # Find all internal links
        link_pattern = r'\[([^\]]+)\]\(#([^\)]+)\)'
        for match in re.finditer(link_pattern, content):
            link_text = match.group(1)
            anchor = match.group(2)

            if anchor not in anchors:
                broken_links.append(f"Broken link: [{link_text}](#{anchor})")

        return broken_links


class TuvaModelExtractor:
    """Extract Tuva model information."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.tuva_dir = project_root / "src" / "acoharmony" / "_tuva"
        self.depends_dir = self.tuva_dir / "_depends" / "repos" / "cclf_connector" / "models"
        self.inject_dir = self.tuva_dir / "_inject"

    def find_staging_models_for_cclf(self, file_num: str) -> list[TuvaModel]:
        """Find Tuva staging models that use this CCLF file."""
        models = []

        # Map CCLF file to source table name
        source_table_map = {
            'CCLF1': 'parta_claims_header',
            'CCLF2': 'parta_claims_revenue_center_detail',
            'CCLF3': 'parta_procedure_code',
            'CCLF4': 'parta_diagnosis_code',
            'CCLF5': 'partb_physicians',
            'CCLF6': 'partb_dme',
            'CCLF7': 'partd_claims',
            'CCLF8': 'beneficiary_demographics',
            'CCLF9': 'beneficiary_xref',
        }

        source_table = source_table_map.get(file_num)
        if not source_table:
            return models

        # Search in staging models
        staging_dir = self.depends_dir / "staging"
        if staging_dir.exists():
            for sql_file in staging_dir.glob("*.sql"):
                content = sql_file.read_text()

                # Check if this model references our source table
                if source_table in content or file_num.lower() in content.lower():
                    models.append(TuvaModel(
                        name=sql_file.stem,
                        type='staging',
                        path=sql_file,
                        sql=content,
                        description=self._extract_description(content),
                        depends_on=[source_table]
                    ))

        # Check inject directory for overrides
        inject_staging = self.inject_dir / "staging"
        if inject_staging.exists():
            for sql_file in inject_staging.glob(f"*{file_num.lower()}*.sql"):
                content = sql_file.read_text()
                models.append(TuvaModel(
                    name=sql_file.stem,
                    type='staging',
                    path=sql_file,
                    sql=content,
                    description=f"Custom staging model (overrides default)",
                    depends_on=[source_table]
                ))

            for py_file in inject_staging.glob(f"*{file_num.lower()}*.py"):
                content = py_file.read_text()
                models.append(TuvaModel(
                    name=py_file.stem,
                    type='staging',
                    path=py_file,
                    sql=None,  # Python implementation
                    description=f"Custom Python staging model",
                    depends_on=[source_table]
                ))

        return models

    def find_intermediate_models(self, staging_model: str) -> list[TuvaModel]:
        """Find intermediate models that depend on this staging model."""
        models = []

        # Map staging models to intermediate models
        staging_to_intermediate = {
            'stg_parta_claims_header': ['int_institutional_claim', 'int_medical_claim'],
            'stg_partb_physicians': ['int_physician_claim', 'int_medical_claim'],
            'stg_partb_dme': ['int_dme_claim', 'int_medical_claim'],
            'stg_partd_claims': ['int_pharmacy_claim'],
            'stg_enrollment': ['int_enrollment'],
            'stg_beneficiary_demographics': ['int_enrollment'],
        }

        intermediate_models = staging_to_intermediate.get(staging_model, [])

        for model_name in intermediate_models:
            # Try to find the model file
            model_path = self._find_model_file(model_name)

            if model_path and model_path.exists():
                content = model_path.read_text()
                models.append(TuvaModel(
                    name=model_name,
                    type='intermediate',
                    path=model_path,
                    sql=content if model_path.suffix == '.sql' else None,
                    description=self._extract_description(content),
                    depends_on=[staging_model]
                ))

        return models

    def _find_model_file(self, model_name: str) -> Path | None:
        """Find a Tuva model file by name."""
        # Search in tuva repos
        search_paths = [
            self.tuva_dir / "_depends" / "repos" / "tuva" / "models",
            self.inject_dir / "intermediate",
        ]

        for search_path in search_paths:
            if not search_path.exists():
                continue

            # Try SQL first
            for sql_file in search_path.rglob(f"{model_name}.sql"):
                return sql_file

            # Try Python
            for py_file in search_path.rglob(f"{model_name}.py"):
                return py_file

        return None

    def _extract_description(self, content: str) -> str:
        """Extract description from SQL or Python file."""
        # Try to find comment block at top
        lines = content.split('\n')
        description_lines = []

        for line in lines[:20]:  # Check first 20 lines
            line = line.strip()
            # Check for SQL (--) or Python (#) comments
            comment = None
            if line.startswith('--'):
                comment = line[2:].strip()
            elif line.startswith('#'):
                comment = line[1:].strip()
            if comment and not comment.startswith('©') and not comment.startswith('All rights'):
                description_lines.append(comment)

        return ' '.join(description_lines[:3]) if description_lines else "No description available"


class EnhancedCCLFGuideGenerator:
    """Enhanced CCLF guide generator with structure preservation."""

    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.schemas_dir = project_root / "src" / "acoharmony" / "_schemas"
        self.docs_dir = project_root / "docs"
        self.cclf_guide_path = self.docs_dir / "reference" / "cclf_guide.md"

        self.structure_preserver = StructurePreserver()
        self.tuva_extractor = TuvaModelExtractor(project_root)

    def generate_enhanced_guide(self, output_path: Path) -> ValidationReport:
        """
        Generate enhanced guide with complete structure preservation.

                Returns:
                    ValidationReport with coverage metrics
        """
        # Load original guide
        original_content = self.cclf_guide_path.read_text()

        # Extract structure
        toc_entries = self.structure_preserver.extract_toc(original_content)
        sections = self.structure_preserver.extract_sections(original_content)

        print(f"Found {len(toc_entries)} TOC entries")
        print(f"Found {len(sections)} sections")

        # Find CCLF file sections
        cclf_sections = self._find_cclf_sections(sections)
        print(f"Found {len(cclf_sections)} CCLF file sections")

        # Generate documentation for each CCLF file
        enhanced_sections = {}
        for file_num, section in cclf_sections.items():
            print(f"Processing {file_num}...")
            file_info = self._gather_file_info(file_num, section)
            enhanced_sections[file_num] = self._generate_enhanced_section(file_info)

        # Insert enhanced content into original
        enhanced_content = self._insert_enhanced_content(
            original_content,
            sections,
            enhanced_sections
        )

        # Validate
        broken_links = self.structure_preserver.validate_links(enhanced_content)

        # Write output
        output_path.write_text(enhanced_content)

        # Generate report
        report = ValidationReport(
            total_cclf_files=12,
            documented_files=len(enhanced_sections),
            total_anchors=len(sections),
            broken_links=broken_links,
            missing_sections=[],
            tuva_models_found=sum(
                len(info.tuva_staging_models) + len(info.tuva_intermediate_models)
                for info in [self._gather_file_info(fn, s) for fn, s in cclf_sections.items()]
            ),
            code_examples_count=sum(
                len(info.cli_examples)
                for info in [self._gather_file_info(fn, s) for fn, s in cclf_sections.items()]
            ),
            coverage_percent=(len(enhanced_sections) / 12) * 100
        )

        return report

    def _find_cclf_sections(self, sections: list[Section]) -> dict[str, Section]:
        """Find sections that correspond to CCLF files."""
        cclf_sections = {}

        # Pattern to match CCLF file headers
        # e.g., "Part A Claims Header File (CCLF1)"
        cclf_pattern = r'.*\(CCLF(\d+|[A-Z]+)\)'

        for section in sections:
            match = re.search(cclf_pattern, section.header)
            if match:
                file_num = f"CCLF{match.group(1)}"
                cclf_sections[file_num] = section

        return cclf_sections

    def _gather_file_info(self, file_num: str, section: Section) -> CCLFFileInfo:
        """Gather all information for a CCLF file."""
        # Load schema
        schema_file = self.schemas_dir / f"{file_num.lower()}.yml"
        schema_content = None
        if schema_file.exists():
            with open(schema_file) as f:
                schema_content = yaml.safe_load(f)

        # Find Tuva models
        staging_models = self.tuva_extractor.find_staging_models_for_cclf(file_num)
        intermediate_models = []
        for staging in staging_models:
            intermediate_models.extend(
                self.tuva_extractor.find_intermediate_models(staging.name)
            )

        # Generate CLI examples
        cli_examples = self._generate_cli_examples(file_num)

        return CCLFFileInfo(
            file_num=file_num,
            name=section.header,
            schema_path=schema_file if schema_file.exists() else None,
            schema_content=schema_content,
            parser_code=None,  # TODO: Extract parser code
            transform_code=None,  # TODO: Extract transform code
            tuva_staging_models=staging_models,
            tuva_intermediate_models=intermediate_models,
            cli_examples=cli_examples
        )

    def _generate_cli_examples(self, file_num: str) -> list[str]:
        """Generate CLI examples for a CCLF file."""
        file_lower = file_num.lower()

        return [
            f"# Transform {file_num} from bronze to silver\naco transform {file_lower}",
            f"# Inspect {file_num} processing status\naco catalog inspect {file_lower}",
            f"# Preview {file_num} data\naco data preview {file_lower} --limit 10",
            f"# Run Tuva pipeline for {file_num}\naco tuva run --select stg_*{file_lower[4:]}*",
        ]

    def _generate_enhanced_section(self, file_info: CCLFFileInfo) -> str:
        """Generate enhanced content for a CCLF file section."""
        sections = []

        sections.append(f"\n---\n")
        sections.append(f"### 📊 ACOHarmony Implementation for {file_info.file_num}\n")

        # Schema section
        if file_info.schema_content:
            sections.append(self._build_schema_section(file_info))

        # Transform Pipeline section (comprehensive implementation docs)
        sections.append(self._build_transform_pipeline_section(file_info))

        # Data pipeline diagram
        sections.append(self._build_pipeline_section(file_info))

        # Tuva section
        if file_info.tuva_staging_models or file_info.tuva_intermediate_models:
            sections.append(self._build_tuva_section(file_info))

        # CLI examples
        sections.append(self._build_cli_examples_section(file_info))

        sections.append(f"\n---\n")

        return '\n'.join(sections)

    def _build_schema_section(self, file_info: CCLFFileInfo) -> str:
        """Build schema documentation section."""
        schema = file_info.schema_content
        parts = ["\n#### Schema Configuration\n"]

        if desc := schema.get('description'):
            parts.append(f"**Description:** {desc}\n")

        if file_format := schema.get('file_format'):
            parts.append("**File Format:**")
            parts.append(f"- Type: `{file_format.get('type', 'N/A')}`")
            parts.append(f"- Encoding: `{file_format.get('encoding', 'N/A')}`")
            if record_length := file_format.get('record_length'):
                parts.append(f"- Record Length: {record_length} characters")
            parts.append("")

        if columns := schema.get('columns'):
            parts.append(f"**Columns:** {len(columns)} fields defined in schema")
            parts.append("")

            # Show all columns
            parts.append("<details>")
            parts.append("<summary>View all columns (click to expand)</summary>\n")
            parts.append("```yaml")
            for col in columns:  # Show all columns
                parts.append(f"- name: {col.get('name')}")
                parts.append(f"  description: {col.get('description', 'N/A')}")
                parts.append(f"  data_type: {col.get('data_type', 'N/A')}")
                if 'start_pos' in col:
                    parts.append(f"  position: {col.get('start_pos')}-{col.get('end_pos')}")
                parts.append("")
            parts.append("```")
            parts.append("</details>\n")

        return '\n'.join(parts)

    def _build_transform_pipeline_section(self, file_info: CCLFFileInfo) -> str:
        """Build comprehensive Transform Pipeline documentation section."""
        parts = []

        parts.append("\n**Transform Pipeline:**\n")
        parts.append("ACOHarmony processes this file through a series of schema-driven transforms:\n")

        # 1. Parse
        parts.append(self._build_parse_subsection(file_info))

        # 2. Validate
        parts.append(self._build_validate_subsection(file_info))

        # 3. Deduplicate (if applicable)
        schema = file_info.schema_content
        if schema and schema.get('deduplication'):
            parts.append(self._build_dedupe_subsection(file_info))

        # 4. Cross-reference (if applicable)
        if schema and schema.get('xref'):
            parts.append(self._build_xref_subsection(file_info))

        # 5. Standardize
        if schema and schema.get('standardization'):
            parts.append(self._build_standardize_subsection(file_info))

        # 6. Write to Silver
        parts.append(self._build_write_subsection(file_info))

        # 7. Complete Example
        parts.append(self._build_complete_transform_example(file_info))

        # 8. Quality Checks
        parts.append(self._build_quality_checks_subsection(file_info))

        return '\n'.join(parts)

    def _build_parse_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build parser implementation documentation."""
        parts = ["\n#### 1. Parse: Bronze → DataFrame\n"]

        schema = file_info.schema_content
        file_format = schema.get('file_format', {}) if schema else {}
        format_type = file_format.get('type', 'unknown')

        if format_type == 'fixed_width':
            record_length = file_format.get('record_length', 'N/A')
            encoding = file_format.get('encoding', 'utf-8')

            parts.append(f"**Parser:** Fixed-width format parser\n")
            parts.append(f"**Record Length:** {record_length} characters\n")
            parts.append(f"**Encoding:** {encoding}\n")

            parts.append("\n**How it works:**\n")
            parts.append("1. Read file line-by-line using memory-mapped access\n")
            parts.append("2. Extract each field by character position (1-based schema positions → 0-based string slicing)\n")
            parts.append("3. Strip padding characters (spaces) from extracted values\n")
            parts.append("4. Return LazyFrame for efficient downstream processing\n")

            parts.append("\n**Key features:**\n")
            parts.append("- Lazy evaluation (no data loaded until needed)\n")
            parts.append("- Memory-efficient streaming for large files\n")
            parts.append("- Automatic handling of fixed-width column positions\n")
            parts.append("- Precise extraction prevents field misalignment\n")

            parts.append("\n<details>")
            parts.append("<summary>View parser code example (click to expand)</summary>\n")
            parts.append("```python")
            parts.append("from acoharmony.parsers import parse_fixed_width")
            parts.append("from acoharmony import catalog\n")
            parts.append(f"# Load schema with column positions")
            parts.append(f"schema = catalog.get_table_metadata('{file_info.file_num.lower()}')\n")
            parts.append(f"# Parse file using fixed-width parser")
            parts.append(f"lf = parse_fixed_width(")
            parts.append(f"    file_path=bronze_path / 'P.A*.ACO.Z{file_info.file_num}*.D*.T*',")
            parts.append(f"    schema=schema,")
            parts.append(f"    encoding='{encoding}'")
            parts.append(")\n")
            parts.append(f"# Returns LazyFrame with {len(schema.get('columns', []))} columns extracted by position")
            parts.append("```")
            parts.append("</details>\n")

        elif format_type == 'csv':
            delimiter = file_format.get('delimiter', ',')
            parts.append(f"**Parser:** CSV parser\n")
            parts.append(f"**Delimiter:** `{delimiter}`\n")
            parts.append(f"**Has Header:** {file_format.get('header', True)}\n")

        elif format_type == 'excel':
            parts.append(f"**Parser:** Excel (.xlsx) parser\n")
            parts.append(f"**Sheet:** {file_format.get('sheet_name', 'First sheet')}\n")

        return '\n'.join(parts)

    def _build_validate_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build validation rules documentation."""
        parts = ["\n#### 2. Validate: Schema Enforcement\n"]

        schema = file_info.schema_content
        if not schema:
            parts.append("Validation rules defined in schema.\n")
            return '\n'.join(parts)

        columns = schema.get('columns', [])

        # Required fields
        required_fields = [col for col in columns if col.get('required')]
        if required_fields:
            parts.append("**Required Fields:**")
            for col in required_fields[:5]:  # Show first 5
                parts.append(f"- `{col.get('name')}`: {col.get('description', 'N/A')}")
            if len(required_fields) > 5:
                parts.append(f"- ... and {len(required_fields) - 5} more required fields\n")
            else:
                parts.append("")

        # Date fields
        date_fields = [col for col in columns if col.get('data_type') == 'date']
        if date_fields:
            parts.append("**Date Fields:**")
            for col in date_fields[:3]:
                date_fmt = col.get('date_format', '%Y-%m-%d')
                parts.append(f"- `{col.get('name')}`: Format `{date_fmt}`")
            if len(date_fields) > 3:
                parts.append(f"- ... and {len(date_fields) - 3} more date fields\n")
            else:
                parts.append("")

        # Numeric fields
        numeric_fields = [col for col in columns if col.get('data_type') in ['decimal', 'integer', 'int32', 'int64', 'float', 'float64']]
        if numeric_fields:
            parts.append("**Numeric Fields:**")
            for col in numeric_fields[:3]:
                parts.append(f"- `{col.get('name')}`: Type `{col.get('data_type')}`")
            if len(numeric_fields) > 3:
                parts.append(f"- ... and {len(numeric_fields) - 3} more numeric fields\n")
            else:
                parts.append("")

        parts.append("**Validation checks:**")
        parts.append("- Required fields must not be null")
        parts.append("- Dates must parse successfully with specified format")
        parts.append("- Numeric fields must convert to proper types")
        parts.append("- Field lengths must match schema specifications\n")

        return '\n'.join(parts)

    def _build_dedupe_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build deduplication documentation."""
        parts = ["\n#### 3. Deduplicate: Ensure Uniqueness\n"]

        schema = file_info.schema_content
        dedup_config = schema.get('deduplication', {})

        parts.append("**Deduplication Strategy:**\n")
        parts.append(f"**Key:** `{', '.join(dedup_config.get('key', []))}`\n")

        sort_by = dedup_config.get('sort_by', [])
        if sort_by:
            parts.append(f"**Sort By:** `{', '.join(sort_by)}`\n")

        keep = dedup_config.get('keep', 'last')
        parts.append(f"**Keep:** `{keep}` record\n")

        parts.append("\n**How it works:**")
        parts.append(f"1. Group records by deduplication key: {', '.join(dedup_config.get('key', []))}")
        parts.append(f"2. Within each group, sort by: {', '.join(sort_by) if sort_by else 'row order'}")
        parts.append(f"3. Keep the `{keep}` record from each group")
        parts.append(f"4. Discard duplicates\n")

        parts.append("**Why this matters:**")
        parts.append("- Prevents duplicate records in downstream analytics")
        parts.append("- Ensures one record per unique identifier")
        parts.append("- Maintains data quality and referential integrity\n")

        parts.append("<details>")
        parts.append("<summary>View deduplication code (click to expand)</summary>\n")
        parts.append("```python")
        parts.append("# Deduplication using Polars window function")
        parts.append("lf = lf.with_columns([")
        parts.append(f"    pl.col('{dedup_config.get('key', ['id'])[0]}').rank(")
        parts.append("        method='ordinal',")
        parts.append(f"        descending={keep == 'first'}")
        parts.append(f"    ).over({dedup_config.get('key', ['id'])}).alias('_rank')")
        parts.append("])")
        parts.append("lf = lf.filter(pl.col('_rank') == 1)")
        parts.append("lf = lf.drop('_rank')")
        parts.append("```")
        parts.append("</details>\n")

        return '\n'.join(parts)

    def _build_xref_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build cross-reference documentation."""
        parts = ["\n#### 4. Cross-Reference: Apply MBI Crosswalk\n"]

        schema = file_info.schema_content
        xref_config = schema.get('xref', {})

        parts.append("**Crosswalk Configuration:**\n")
        parts.append(f"**Table:** `{xref_config.get('table', 'beneficiary_xref')}`\n")
        parts.append(f"**Join Key:** `{xref_config.get('join_key', 'bene_mbi')}`\n")
        parts.append(f"**Xref Key:** `{xref_config.get('xref_key', 'prvs_num')}`\n")
        parts.append(f"**Current Column:** `{xref_config.get('current_column', 'crnt_num')}`\n")
        parts.append(f"**Output Column:** `{xref_config.get('output_column', 'current_bene_mbi_id')}`\n")

        parts.append("\n**What is MBI Crosswalk?**")
        parts.append("Medicare Beneficiary Identifiers (MBIs) can change over time. The crosswalk table")
        parts.append("(built from CCLF9) maps historical MBIs to current MBIs, ensuring consistent")
        parts.append("identification across data sources.\n")

        parts.append("**How it works:**")
        parts.append(f"1. Load beneficiary crosswalk table from `beneficiary_xref.parquet`")
        parts.append(f"2. Join this file's `{xref_config.get('join_key')}` with crosswalk's `{xref_config.get('xref_key')}`")
        parts.append(f"3. Retrieve current MBI from crosswalk's `{xref_config.get('current_column')}`")
        parts.append(f"4. Add as new column: `{xref_config.get('output_column')}`")
        parts.append(f"5. Use current MBI for all downstream joins and analytics\n")

        parts.append("**Why this matters:**")
        parts.append("- Beneficiaries may have multiple historical MBIs")
        parts.append("- Crosswalk ensures we always use the current, valid MBI")
        parts.append("- Critical for joining enrollment, claims, and alignment data")
        parts.append("- Prevents orphaned records due to MBI changes\n")

        parts.append("<details>")
        parts.append("<summary>View xref code example (click to expand)</summary>\n")
        parts.append("```python")
        parts.append("# Load beneficiary crosswalk")
        parts.append("xref = pl.read_parquet('/opt/s3/data/workspace/silver/beneficiary_xref.parquet')")
        parts.append("")
        parts.append("# Apply crosswalk join")
        parts.append("lf = lf.join(")
        parts.append("    xref.select([")
        parts.append(f"        pl.col('{xref_config.get('xref_key', 'prvs_num')}'),")
        parts.append(f"        pl.col('{xref_config.get('current_column', 'crnt_num')}').alias('{xref_config.get('output_column', 'current_bene_mbi_id')}')")
        parts.append("    ]),")
        parts.append(f"    left_on='{xref_config.get('join_key', 'bene_mbi')}',")
        parts.append(f"    right_on='{xref_config.get('xref_key', 'prvs_num')}',")
        parts.append("    how='left'")
        parts.append(")")
        parts.append("```")
        parts.append("</details>\n")

        return '\n'.join(parts)

    def _build_standardize_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build standardization documentation."""
        parts = ["\n#### 5. Standardize: Align to Tuva CDM\n"]

        schema = file_info.schema_content
        std_config = schema.get('standardization', {})

        if not std_config:
            parts.append("Minimal standardization - data kept in CMS native format.\n")
            return '\n'.join(parts)

        renames = std_config.get('rename_columns', {})
        add_cols = std_config.get('add_columns', [])

        if renames:
            parts.append(f"**Column Renames:** {len(renames)} columns renamed for Tuva CDM compatibility\n")
            parts.append("<details>")
            parts.append("<summary>View column renames (click to expand)</summary>\n")
            parts.append("```yaml")
            for old, new in list(renames.items())[:10]:
                parts.append(f"{old}: {new}")
            if len(renames) > 10:
                parts.append(f"# ... and {len(renames) - 10} more renames")
            parts.append("```")
            parts.append("</details>\n")

        if add_cols:
            parts.append(f"**Computed Columns:** {len(add_cols)} columns added\n")
            parts.append("<details>")
            parts.append("<summary>View computed columns (click to expand)</summary>\n")
            parts.append("```yaml")
            for col in add_cols[:5]:
                parts.append(f"- name: {col.get('name')}")
                parts.append(f"  value: {col.get('value')}")
            if len(add_cols) > 5:
                parts.append(f"# ... and {len(add_cols) - 5} more columns")
            parts.append("```")
            parts.append("</details>\n")

        parts.append("**Why standardization?**")
        parts.append("- Aligns column names with Tuva's Common Data Model (CDM)")
        parts.append("- Adds metadata columns for lineage tracking")
        parts.append("- Prepares data for Tuva staging models")
        parts.append("- Ensures consistent naming across all data sources\n")

        return '\n'.join(parts)

    def _build_write_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build write to Silver layer documentation."""
        parts = ["\n#### 6. Write: Persist to Silver Layer\n"]

        schema = file_info.schema_content
        storage = schema.get('storage', {}) if schema else {}
        silver_config = storage.get('silver', {})

        output_name = silver_config.get('output_name', f'{file_info.file_num.lower()}.parquet')
        parts.append(f"**Output Path:** `/opt/s3/data/workspace/silver/{output_name}`\n")
        parts.append(f"**Format:** Parquet (Snappy compression)\n")
        parts.append(f"**Refresh Frequency:** {silver_config.get('refresh_frequency', 'varies')}\n")

        parts.append("\n**Storage Features:**")
        parts.append("- Columnar format (efficient for analytics)")
        parts.append("- Snappy compression (fast compression/decompression)")
        parts.append("- Schema evolution support")
        parts.append("- Optimized for Polars/DuckDB query engines\n")

        return '\n'.join(parts)

    def _build_complete_transform_example(self, file_info: CCLFFileInfo) -> str:
        """Build complete transform code example."""
        parts = ["\n#### Complete Transform Example\n"]

        parts.append("```python")
        parts.append("from acoharmony import TransformRunner\n")
        parts.append("runner = TransformRunner()\n")
        parts.append(f"# Transform {file_info.file_num}: Parse → Validate → Dedupe → Xref → Standardize → Write")
        parts.append(f"result = runner.transform_schema('{file_info.file_num.lower()}')\n")
        parts.append("print(f'[SUCCESS] Processed {result.records_processed:,} records')")
        parts.append("print(f'📁 Output: {result.output_path}')")
        parts.append("print(f'⏱  Duration: {result.duration_seconds:.2f}s')\n")
        parts.append("# Verify output")
        parts.append("import polars as pl")
        parts.append("df = pl.read_parquet(result.output_path)")
        parts.append("print(f'Columns: {len(df.columns)}')")
        parts.append("print(f'Records: {len(df):,}')")
        parts.append("```\n")

        return '\n'.join(parts)

    def _build_quality_checks_subsection(self, file_info: CCLFFileInfo) -> str:
        """Build quality checks documentation."""
        parts = ["\n#### Quality Checks\n"]

        schema = file_info.schema_content
        columns = schema.get('columns', []) if schema else []
        required_fields = [col for col in columns if col.get('required')]

        parts.append("**Row Count Validation:**")
        parts.append("```python")
        parts.append("# Verify no data loss during transform")
        parts.append("bronze_count = count_bronze_records()")
        parts.append("silver_count = len(pl.read_parquet(silver_path))")
        parts.append("assert bronze_count == silver_count, 'Record count mismatch!'")
        parts.append("```\n")

        if required_fields:
            parts.append("**Required Field Validation:**")
            parts.append("```python")
            parts.append("df = pl.read_parquet(silver_path)\n")
            parts.append("# Check for nulls in required fields")
            parts.append("null_checks = df.select([")
            for col in required_fields[:3]:
                parts.append(f"    pl.col('{col.get('name')}').is_null().sum().alias('null_{col.get('name')}'),")
            parts.append("])\n")
            for col in required_fields[:3]:
                parts.append(f"assert null_checks['null_{col.get('name')}'][0] == 0")
            parts.append("```\n")

        parts.append("**Date Range Validation:**")
        parts.append("```python")
        parts.append("from datetime import date\n")
        parts.append("# Ensure dates are reasonable")
        parts.append("date_checks = df.select([")
        parts.append("    pl.col('file_date').min().alias('min_date'),")
        parts.append("    pl.col('file_date').max().alias('max_date'),")
        parts.append("])\n")
        parts.append("assert date_checks['min_date'][0] >= date(2000, 1, 1)")
        parts.append("assert date_checks['max_date'][0] <= date.today()")
        parts.append("```\n")

        return '\n'.join(parts)

    def _build_pipeline_section(self, file_info: CCLFFileInfo) -> str:
        """Build data pipeline section."""
        parts = ["\n#### Data Pipeline: Bronze → Silver → Gold\n"]

        parts.append("```mermaid")
        parts.append("graph LR")
        parts.append(f"    Raw[Raw {file_info.file_num} File] --> Bronze[Bronze Layer]")
        parts.append("    Bronze --> Parser[Fixed-Width Parser]")
        parts.append("    Parser --> Silver[Silver Layer]")
        parts.append("    Silver --> Transforms[Transforms]")
        parts.append("    Transforms --> Gold[Gold Layer]")

        if file_info.tuva_staging_models:
            staging = file_info.tuva_staging_models[0].name
            parts.append(f"    Gold --> Tuva[Tuva: {staging}]")

        parts.append("```\n")

        return '\n'.join(parts)

    def _build_tuva_section(self, file_info: CCLFFileInfo) -> str:
        """Build Tuva integration section."""
        parts = ["\n#### Tuva Integration\n"]

        # Staging models
        if file_info.tuva_staging_models:
            parts.append("**Staging Models:**")
            for model in file_info.tuva_staging_models:
                parts.append(f"- `{model.name}`: {model.description}")

                if model.sql:
                    parts.append(f"\n<details>")
                    parts.append(f"<summary>View {model.name} SQL (click to expand)</summary>\n")
                    parts.append("```sql")
                    # Show first 30 lines
                    sql_lines = model.sql.split('\n')[:30]
                    parts.append('\n'.join(sql_lines))
                    if len(model.sql.split('\n')) > 30:
                        parts.append("-- ... (truncated)")
                    parts.append("```")
                    parts.append("</details>\n")

        # Intermediate models
        if file_info.tuva_intermediate_models:
            parts.append("\n**Intermediate Models:**")
            for model in file_info.tuva_intermediate_models:
                parts.append(f"- `{model.name}`: Used for HCC risk scoring, quality measures, analytics")

        return '\n'.join(parts)

    def _build_non_cclf_data_sources_section(self) -> str:
        """Build documentation section for non-CCLF data sources (BAR, ALR, consolidated_alignment)."""
        parts = []

        parts.append("\n---\n")
        parts.append("## ACOHarmony Data Integration: Beyond CCLF Files\n")
        parts.append("While CCLF files provide the foundation for claims and enrollment data, ACOHarmony ")
        parts.append("integrates additional CMS data sources to create a complete picture of beneficiary ")
        parts.append("alignment, attribution, and program participation.\n")

        # BAR (Beneficiary Alignment Report)
        parts.append("\n### Beneficiary Alignment Report (BAR)\n")
        parts.append("**Source:** Excel files (ALGC/ALGR) from CMS ACO REACH program\n")
        parts.append("**Purpose:** Provides beneficiary assignment information for ACO REACH participants\n")
        parts.append("**Medallion Layer:** Bronze → Silver\n")

        # Load BAR schema
        bar_schema_path = self.project_root / "src" / "acoharmony" / "_schemas" / "bar.yml"
        if bar_schema_path.exists():
            with open(bar_schema_path) as f:
                bar_schema = yaml.safe_load(f)

            parts.append("\n#### Schema Overview\n")
            parts.append(f"**Description:** {bar_schema.get('description', 'N/A')}\n")
            parts.append(f"**File Format:** Excel (.xlsx)\n")
            parts.append(f"**File Patterns:** `*ALGC*.xlsx`, `*ALGR*.xlsx`\n")

            if columns := bar_schema.get('columns'):
                parts.append(f"\n**Columns:** {len(columns)} fields\n")
                parts.append("\n<details>")
                parts.append("<summary>View all columns (click to expand)</summary>\n")
                parts.append("```yaml")
                for col in columns:
                    parts.append(f"- name: {col.get('name')}")
                    parts.append(f"  output_name: {col.get('output_name')}")
                    parts.append(f"  description: {col.get('description', 'N/A')}")
                    parts.append(f"  data_type: {col.get('data_type', 'N/A')}")
                    parts.append("")
                parts.append("```")
                parts.append("</details>\n")

            if xref := bar_schema.get('xref'):
                parts.append("\n**MBI Crosswalk:** Uses beneficiary_xref to map MBI changes\n")
                parts.append(f"- Input column: `{xref.get('join_key')}`\n")
                parts.append(f"- Output column: `{xref.get('output_column')}`\n")

        parts.append("\n#### Processing Pipeline\n")
        parts.append("```bash")
        parts.append("# Transform BAR file to silver layer")
        parts.append("aco transform bar --profile staging")
        parts.append("")
        parts.append("# Verify output")
        parts.append("ls -lh /opt/s3/data/workspace/silver/bar.parquet")
        parts.append("```\n")

        # ALR (Assignment List Report)
        parts.append("\n### Assignment List Report (ALR)\n")
        parts.append("**Source:** CSV files (AALR/QALR) from CMS MSSP program\n")
        parts.append("**Purpose:** Provides beneficiary assignment information for MSSP ACOs\n")
        parts.append("**Medallion Layer:** Bronze → Silver\n")

        # Load ALR schema
        alr_schema_path = self.project_root / "src" / "acoharmony" / "_schemas" / "alr.yml"
        if alr_schema_path.exists():
            with open(alr_schema_path) as f:
                alr_schema = yaml.safe_load(f)

            parts.append("\n#### Schema Overview\n")
            parts.append(f"**Description:** {alr_schema.get('description', 'N/A')}\n")
            parts.append(f"**File Format:** CSV\n")
            parts.append(f"**File Patterns:** Annual (`*AALR*.csv`), Quarterly (`*QALR*.csv`)\n")

            if columns := alr_schema.get('columns'):
                parts.append(f"\n**Columns:** {len(columns)} fields\n")
                parts.append("\n<details>")
                parts.append("<summary>View all columns (click to expand)</summary>\n")
                parts.append("```yaml")
                for col in columns:
                    parts.append(f"- name: {col.get('name')}")
                    parts.append(f"  output_name: {col.get('output_name')}")
                    parts.append(f"  description: {col.get('description', 'N/A')}")
                    parts.append(f"  data_type: {col.get('data_type', 'N/A')}")
                    parts.append("")
                parts.append("```")
                parts.append("</details>\n")

            if xref := alr_schema.get('xref'):
                parts.append("\n**MBI Crosswalk:** Uses beneficiary_xref to map MBI changes\n")
                parts.append(f"- Input column: `{xref.get('join_key')}`\n")
                parts.append(f"- Output column: `{xref.get('output_column')}`\n")

        parts.append("\n#### Processing Pipeline\n")
        parts.append("```bash")
        parts.append("# Transform ALR file to silver layer")
        parts.append("aco transform alr --profile staging")
        parts.append("")
        parts.append("# Verify output")
        parts.append("ls -lh /opt/s3/data/workspace/silver/alr.parquet")
        parts.append("```\n")

        # Consolidated Alignment
        parts.append("\n### Consolidated Alignment\n")
        parts.append("**Source:** Combines BAR, ALR, CCLF8, voluntary alignment (SVA/PBVAR), and provider data\n")
        parts.append("**Purpose:** Creates unified view of beneficiary alignment across MSSP and REACH programs\n")
        parts.append("**Medallion Layer:** Silver (final consolidated output)\n")

        # Load consolidated_alignment schema
        consol_schema_path = self.project_root / "src" / "acoharmony" / "_schemas" / "consolidated_alignment.yml"
        if consol_schema_path.exists():
            with open(consol_schema_path) as f:
                consol_schema = yaml.safe_load(f)

            parts.append("\n#### Schema Overview\n")
            parts.append(f"**Description:** {consol_schema.get('description', 'N/A')}\n")

            if intermediate := consol_schema.get('intermediate'):
                parts.append(f"\n**Integration Type:** {intermediate.get('type')}\n")
                if sources := intermediate.get('sources'):
                    parts.append(f"**Source Tables:** {len(sources)} intermediate tables\n")
                    parts.append("<details>")
                    parts.append("<summary>View source tables (click to expand)</summary>\n")
                    parts.append("```yaml")
                    for source_name, source_table in sources.items():
                        parts.append(f"- {source_name}: {source_table}")
                    parts.append("```")
                    parts.append("</details>\n")

            parts.append("\n**Key Features:**\n")
            parts.append("- [SUCCESS] Idempotent processing based on file temporality (not \"today\")\n")
            parts.append("- [SUCCESS] Proper reconciliation vs current file handling\n")
            parts.append("- [SUCCESS] Death date truncation of enrollment periods\n")
            parts.append("- [SUCCESS] Voluntary alignment with signature validity tracking\n")
            parts.append("- [SUCCESS] Complete data lineage for audit and debugging\n")

            if columns := consol_schema.get('columns'):
                parts.append(f"\n**Columns:** {len(columns)} fields providing comprehensive alignment metadata\n")
                parts.append("\n<details>")
                parts.append("<summary>View all columns (click to expand)</summary>\n")
                parts.append("```yaml")
                for col in columns:
                    parts.append(f"- name: {col.get('name')}")
                    parts.append(f"  output_name: {col.get('output_name')}")
                    parts.append(f"  description: {col.get('description', 'N/A')}")
                    parts.append(f"  data_type: {col.get('data_type', 'N/A')}")
                    if col.get('required'):
                        parts.append(f"  required: true")
                    parts.append("")
                parts.append("```")
                parts.append("</details>\n")

        parts.append("\n#### Complete Integration Pipeline\n")
        parts.append("```mermaid")
        parts.append("graph TB")
        parts.append("    subgraph \"CMS Data Sources\"")
        parts.append("        BAR[BAR: REACH Alignment<br/>ALGC/ALGR Excel]")
        parts.append("        ALR[ALR: MSSP Alignment<br/>AALR/QALR CSV]")
        parts.append("        CCLF8[CCLF8: Demographics<br/>Fixed-width]")
        parts.append("        SVA[SVA: Voluntary Alignment<br/>Submissions]")
        parts.append("        PBVAR[PBVAR: Response Codes<br/>Excel]")
        parts.append("    end")
        parts.append("")
        parts.append("    subgraph \"ACOHarmony Processing\"")
        parts.append("        XREF[Beneficiary Crosswalk<br/>CCLF9]")
        parts.append("        PROV[Provider List<br/>NPIs/TINs]")
        parts.append("        ZIP[Office Location<br/>ZIP mapping]")
        parts.append("    end")
        parts.append("")
        parts.append("    subgraph \"Consolidated Output\"")
        parts.append("        CONSOL[Consolidated Alignment<br/>Silver Layer]")
        parts.append("    end")
        parts.append("")
        parts.append("    BAR --> XREF")
        parts.append("    ALR --> XREF")
        parts.append("    CCLF8 --> XREF")
        parts.append("    SVA --> CONSOL")
        parts.append("    PBVAR --> CONSOL")
        parts.append("    XREF --> CONSOL")
        parts.append("    PROV --> CONSOL")
        parts.append("    ZIP --> CONSOL")
        parts.append("```\n")

        parts.append("\n#### Processing Example\n")
        parts.append("```bash")
        parts.append("# Step 1: Ensure beneficiary crosswalk is up to date")
        parts.append("aco transform beneficiary_xref --profile staging")
        parts.append("")
        parts.append("# Step 2: Process BAR and ALR files")
        parts.append("aco transform bar --profile staging")
        parts.append("aco transform alr --profile staging")
        parts.append("")
        parts.append("# Step 3: Generate consolidated alignment")
        parts.append("aco transform consolidated_alignment --profile staging")
        parts.append("")
        parts.append("# Step 4: Verify output")
        parts.append("python -c \"import polars as pl; df = pl.read_parquet('/opt/s3/data/workspace/gold/consolidated_alignment.parquet'); print(f'Records: {len(df):,}'); print(f'Columns: {len(df.columns)}')\"")
        parts.append("```\n")

        parts.append("\n#### Use Cases\n")
        parts.append("**Attribution Analysis:**")
        parts.append("- Determine which beneficiaries are aligned to which programs (MSSP vs REACH)\n")
        parts.append("- Track program transitions and enrollment gaps\n")
        parts.append("- Identify voluntary vs claims-based alignment\n")
        parts.append("\n**Quality Reporting:**")
        parts.append("- Calculate member-months for performance measures\n")
        parts.append("- Track signature validity for voluntary alignment\n")
        parts.append("- Monitor SVA submission status and response codes\n")
        parts.append("\n**Operational Workflows:**")
        parts.append("- Identify beneficiaries needing SVA renewal (signature expiring)\n")
        parts.append("- Find MSSP beneficiaries eligible for REACH recruitment\n")
        parts.append("- Track provider list changes affecting valid alignments\n")

        return '\n'.join(parts)

    def _build_cli_examples_section(self, file_info: CCLFFileInfo) -> str:
        """Build CLI examples section."""
        parts = ["\n#### Complete Processing Example\n"]

        parts.append("```bash")
        for example in file_info.cli_examples:
            parts.append(example)
            parts.append("")
        parts.append("```\n")

        return '\n'.join(parts)

    def _insert_enhanced_content(
        self,
        original_content: str,
        sections: list[Section],
        enhanced_sections: dict[str, str]
    ) -> str:
        """Insert enhanced content into original guide."""
        lines = original_content.split('\n')
        result = []
        last_cclf_section_end = -1

        # Find where to insert for each CCLF section
        for i, section in enumerate(sections):
            # Check if this is a CCLF section
            cclf_match = re.search(r'\(CCLF(\d+|[A-Z]+)\)', section.header)

            if cclf_match:
                file_num = f"CCLF{cclf_match.group(1)}"

                # Add original section header and content
                section_lines = lines[section.start_line:section.end_line + 1]
                result.extend(section_lines)

                # Insert enhanced content
                if file_num in enhanced_sections:
                    result.append(enhanced_sections[file_num])

                # Track last CCLF section
                last_cclf_section_end = i
            else:
                # Regular section, just copy
                section_lines = lines[section.start_line:section.end_line + 1]
                result.extend(section_lines)

        # Insert non-CCLF data sources section after last CCLF section
        # Find insertion point (before appendices if they exist)
        insertion_point = len(result)
        for i in range(len(result) - 1, -1, -1):
            if 'appendix' in result[i].lower() or '## appendix' in result[i].lower():
                insertion_point = i
                break

        # Generate and insert non-CCLF section
        non_cclf_section = self._build_non_cclf_data_sources_section()
        result.insert(insertion_point, non_cclf_section)

        return '\n'.join(result)


def main():
    """Run the enhanced CCLF guide generator."""
    project_root = Path(__file__).parent.parent.parent.parent

    generator = EnhancedCCLFGuideGenerator(project_root)

    # Generate enhanced guide
    enhanced_guide_path = project_root / "docs" / "reference" / "cclf_guide_enhanced.md"
    print(f"\n🚀 Generating enhanced CCLF guide at {enhanced_guide_path}...")

    report = generator.generate_enhanced_guide(enhanced_guide_path)

    # Print report
    print("\n" + "="*70)
    print("📊 COVERAGE REPORT")
    print("="*70)
    print(f"CCLF Files Documented: {report.documented_files}/{report.total_cclf_files}")
    print(f"Tuva Models Found: {report.tuva_models_found}")
    print(f"Code Examples: {report.code_examples_count}")
    print(f"Coverage: {report.coverage_percent:.1f}%")
    print(f"\nBroken Links: {len(report.broken_links)}")
    if report.broken_links:
        for link in report.broken_links[:10]:  # Show first 10
            print(f"  - {link}")
    print("="*70)
    print("\n[SUCCESS] Documentation generation complete!")


if __name__ == "__main__":
    main()
