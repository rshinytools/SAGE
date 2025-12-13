# SAGE - Context Builder
# =======================
"""
Context Builder
===============
Builds the context for the LLM including:
- Schema information
- Metadata from golden_metadata.json
- Entity resolution hints
- Clinical rules

This is STEP 4 of the 9-step pipeline.
"""

import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from dataclasses import dataclass

from .models import LLMContext, EntityMatch
from .table_resolver import TableResolution
from .clinical_config import ClinicalQueryConfig, DEFAULT_CLINICAL_CONFIG

logger = logging.getLogger(__name__)


@dataclass
class SchemaInfo:
    """Schema information for a table."""
    table_name: str
    columns: List[Dict[str, str]]  # name, dtype, description
    row_count: Optional[int] = None


class ContextBuilder:
    """
    Builds context for the LLM.

    Combines:
    - Table schema from DuckDB
    - Variable descriptions from golden_metadata.json
    - Entity resolution from previous step
    - Clinical rules from configuration

    Example:
        builder = ContextBuilder(metadata_path, db_path)
        context = builder.build(
            query="How many patients had headaches?",
            table_resolution=resolution,
            entities=entities
        )
    """

    def __init__(self,
                 metadata_path: Optional[str] = None,
                 db_path: Optional[str] = None,
                 config: Optional[ClinicalQueryConfig] = None):
        """
        Initialize context builder.

        Args:
            metadata_path: Path to golden_metadata.json
            db_path: Path to DuckDB database
            config: Clinical configuration
        """
        self.config = config or DEFAULT_CLINICAL_CONFIG
        self.metadata = self._load_metadata(metadata_path) if metadata_path else {}
        self.db_path = db_path
        self._schema_cache: Dict[str, SchemaInfo] = {}

    def _load_metadata(self, path: str) -> Dict[str, Any]:
        """Load golden metadata."""
        try:
            with open(path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.warning(f"Could not load metadata from {path}: {e}")
            return {}

    def build(self,
              query: str,
              table_resolution: TableResolution,
              entities: List[EntityMatch],
              include_full_schema: bool = False
             ) -> LLMContext:
        """
        Build LLM context.

        Args:
            query: User's query
            table_resolution: Resolved table information
            entities: Extracted entities
            include_full_schema: Whether to include all columns

        Returns:
            LLMContext ready for LLM
        """
        # Build system prompt with clinical rules
        system_prompt = self._build_system_prompt(table_resolution)

        # Build schema context
        schema_context = self._build_schema_context(
            table_resolution,
            include_full_schema
        )

        # Build entity context
        entity_context = self._build_entity_context(entities)

        # Build clinical rules context
        clinical_rules = self._build_clinical_rules(table_resolution)

        # Build user prompt
        user_prompt = self._build_user_prompt(
            query,
            table_resolution,
            entities
        )

        # Estimate token count (rough: 4 chars per token)
        total_text = system_prompt + schema_context + entity_context + clinical_rules + user_prompt
        token_estimate = len(total_text) // 4

        return LLMContext(
            system_prompt=system_prompt,
            user_prompt=user_prompt,
            schema_context=schema_context,
            entity_context=entity_context,
            clinical_rules=clinical_rules,
            token_count_estimate=token_estimate
        )

    def _build_system_prompt(self, table_resolution: TableResolution) -> str:
        """Build the system prompt."""
        return f"""You are a clinical data SQL expert for DuckDB. Generate accurate SQL queries.

CRITICAL RULES:
1. Use table: {table_resolution.selected_table} ({table_resolution.table_type})
2. Apply population filter: {table_resolution.population_filter or 'None required'}
3. For subject counts, use COUNT(DISTINCT USUBJID)
4. Only generate SELECT queries - no INSERT, UPDATE, DELETE
5. Use exact column names from the schema provided

POPULATION: {table_resolution.population_name}
TABLE: {table_resolution.selected_table}

Show your reasoning in <think> tags, then provide the SQL in a ```sql code block."""

    def _build_schema_context(self,
                              table_resolution: TableResolution,
                              include_full: bool = False
                             ) -> str:
        """Build schema context string."""
        table = table_resolution.selected_table
        columns = table_resolution.table_columns

        lines = [f"## Schema for {table}"]
        lines.append("")
        lines.append("| Column | Description |")
        lines.append("|--------|-------------|")

        # Get metadata descriptions
        table_meta = self.metadata.get('variables', {}).get(table, {})

        for col in columns:
            desc = self._get_column_description(table, col, table_meta)
            lines.append(f"| {col} | {desc} |")

        # Add column priority info
        if table_resolution.columns_resolved:
            lines.append("")
            lines.append("## Column Usage Notes")
            for concept, resolution in table_resolution.columns_resolved.items():
                lines.append(f"- {resolution.column_name}: {resolution.reason}")

        return "\n".join(lines)

    def _get_column_description(self,
                                table: str,
                                column: str,
                                table_meta: Dict
                               ) -> str:
        """Get description for a column."""
        # Try golden metadata
        if column in table_meta:
            return table_meta[column].get('description', column)

        # Common column descriptions
        common = {
            'USUBJID': 'Unique Subject Identifier',
            'SUBJID': 'Subject Identifier',
            'STUDYID': 'Study Identifier',
            'SAFFL': 'Safety Population Flag (Y/N)',
            'ITTFL': 'Intent-to-Treat Population Flag (Y/N)',
            'EFFFL': 'Efficacy Population Flag (Y/N)',
            'TRT01A': 'Actual Treatment (Period 1)',
            'TRT01P': 'Planned Treatment (Period 1)',
            'AEDECOD': 'AE Dictionary-Derived Term (MedDRA PT)',
            'AETERM': 'Reported AE Term (Verbatim)',
            'AETOXGR': 'Toxicity Grade at Onset',
            'ATOXGR': 'Maximum Toxicity Grade (Analysis)',
            'AESEV': 'AE Severity',
            'AESER': 'Serious AE Flag',
            'AEREL': 'AE Relationship to Treatment',
            'AEOUT': 'AE Outcome',
            'AESTDTC': 'AE Start Date/Time',
            'AEENDTC': 'AE End Date/Time',
            'TRTEMFL': 'Treatment-Emergent Flag',
            'AGE': 'Age at Baseline',
            'SEX': 'Sex',
            'RACE': 'Race',
            'ETHNIC': 'Ethnicity',
            'ARM': 'Treatment Arm',
            'ACTARM': 'Actual Treatment Arm',
        }

        return common.get(column.upper(), column)

    def _build_entity_context(self, entities: List[EntityMatch]) -> str:
        """Build entity resolution context."""
        if not entities:
            return ""

        lines = ["## Entity Resolution"]
        lines.append("")
        lines.append("The following terms were resolved:")
        lines.append("")

        for entity in entities:
            if entity.match_type == "grade":
                lines.append(f"- Grade {entity.matched_term} → Use {entity.column}")
            else:
                lines.append(f"- \"{entity.original_term}\" → {entity.matched_term} "
                           f"(table: {entity.table}, column: {entity.column})")

        return "\n".join(lines)

    def _build_clinical_rules(self, table_resolution: TableResolution) -> str:
        """Build clinical rules context."""
        lines = ["## Clinical Rules (MUST FOLLOW)"]
        lines.append("")
        lines.append(f"1. Use {table_resolution.selected_table} table")

        if table_resolution.population_filter:
            lines.append(f"2. Filter: WHERE {table_resolution.population_filter}")
        else:
            lines.append("2. No population filter required")

        # Add column priorities
        grade_col = table_resolution.get_grade_column()
        if grade_col:
            lines.append(f"3. For toxicity grade, use {grade_col}")

        if table_resolution.assumptions:
            lines.append("")
            lines.append("Assumptions made:")
            for assumption in table_resolution.assumptions:
                lines.append(f"- {assumption}")

        return "\n".join(lines)

    def _build_user_prompt(self,
                           query: str,
                           table_resolution: TableResolution,
                           entities: List[EntityMatch]
                          ) -> str:
        """Build the user prompt."""
        lines = ["## Query"]
        lines.append("")
        lines.append(query)
        lines.append("")
        lines.append("Generate a DuckDB SQL query to answer this question.")
        lines.append("")
        lines.append("Requirements:")
        lines.append(f"- Use table: {table_resolution.selected_table}")

        if table_resolution.population_filter:
            lines.append(f"- Apply filter: {table_resolution.population_filter}")

        for entity in entities:
            if entity.table and entity.column:
                lines.append(f"- Use {entity.column} = '{entity.matched_term}' for '{entity.original_term}'")

        lines.append("")
        lines.append("Show your reasoning in <think> tags, then provide SQL in ```sql block.")

        return "\n".join(lines)

    def get_table_schema(self, table_name: str) -> Optional[SchemaInfo]:
        """Get schema for a specific table."""
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        if not self.db_path:
            return None

        try:
            import duckdb
            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                # Get columns
                columns = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()

                # Get row count
                count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]

                schema = SchemaInfo(
                    table_name=table_name,
                    columns=[{'name': c[0], 'dtype': c[1]} for c in columns],
                    row_count=count
                )
                self._schema_cache[table_name] = schema
                return schema
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Could not get schema for {table_name}: {e}")
            return None
