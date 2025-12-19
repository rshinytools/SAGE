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
              include_full_schema: bool = False,
              accumulated_filters: Optional[str] = None,
              preserve_filters: bool = False,
              conversation_context: Optional[str] = None
             ) -> LLMContext:
        """
        Build LLM context.

        Args:
            query: User's query
            table_resolution: Resolved table information
            entities: Extracted entities
            include_full_schema: Whether to include all columns
            accumulated_filters: SQL filters from previous queries that must be preserved
            preserve_filters: Whether to preserve accumulated filters (for REFINE queries)
            conversation_context: Full conversation context for follow-up queries

        Returns:
            LLMContext ready for LLM
        """
        # Build system prompt with clinical rules
        system_prompt = self._build_system_prompt(
            table_resolution,
            accumulated_filters=accumulated_filters if preserve_filters else None
        )

        # Build schema context
        schema_context = self._build_schema_context(
            table_resolution,
            include_full_schema
        )

        # Build entity context
        entity_context = self._build_entity_context(entities)

        # Build clinical rules context
        clinical_rules = self._build_clinical_rules(table_resolution)

        # Add refinement context if preserving filters
        if preserve_filters and accumulated_filters:
            clinical_rules += self._build_refinement_context(accumulated_filters)

        # Build user prompt
        user_prompt = self._build_user_prompt(
            query,
            table_resolution,
            entities,
            accumulated_filters=accumulated_filters if preserve_filters else None
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

    def _build_system_prompt(self,
                             table_resolution: TableResolution,
                             accumulated_filters: Optional[str] = None
                            ) -> str:
        """Build the system prompt - LLM-first approach."""

        # Get grade column for emphasis
        grade_col = table_resolution.get_grade_column()
        grade_note = ""
        if grade_col:
            grade_note = f"\n- For toxicity/grade queries, use {grade_col}"

        # Add accumulated filters for refinement queries
        refinement_rule = ""
        if accumulated_filters:
            refinement_rule = f"""

FOLLOW-UP QUERY:
This refines previous results. Include these filters: {accumulated_filters}"""

        # Get available tables for context
        available = ", ".join(table_resolution.available_tables) if table_resolution.available_tables else "ADSL, ADAE"

        # LLM-first system prompt - gives context, lets LLM decide
        return f"""You are a SQL expert for clinical trial data (DuckDB). Generate accurate SQL.

AVAILABLE TABLES: {available}

TABLE SELECTION GUIDE:
- ADSL (Subject-Level Analysis): Use for demographics (age, sex, race), population counts
- ADAE (Adverse Events Analysis): Use for adverse event queries (nausea, headache, toxicity)

CRITICAL RULES:
- ONLY use columns listed in the schema below - do NOT invent column names
- Use AEDECOD for adverse event terms (not PT_NAME, PREFERRED_TERM, or other names)
- Use ATOXGR for toxicity grades - NOTE: it's VARCHAR with '.' for missing values
  Use: ATOXGR != '.' AND CAST(ATOXGR AS INTEGER) >= 3 for Grade 3+
- Use COUNT(DISTINCT USUBJID) when counting patients/subjects
- Use COUNT(*) when counting events/records{grade_note}

CRITICAL - EXACT TERMS ONLY:
- ONLY use the exact adverse event terms mentioned in the user's query
- DO NOT add synonyms, related terms, or medical variants unless explicitly requested
- If user asks about "headache", query ONLY for "headache" - not fever, migraine, or other terms
- UK/US spelling variants (anaemia/anemia, diarrhoea/diarrhea) are acceptable ONLY for the specific term asked{refinement_rule}

Output SQL in ```sql block."""

    def _build_schema_context(self,
                              table_resolution: TableResolution,
                              include_full: bool = False
                             ) -> str:
        """Build comprehensive schema context for LLM decision-making."""
        lines = []

        # Key columns for each table type
        key_columns = {
            'ADAE': ['USUBJID', 'AEDECOD', 'AETERM', 'ATOXGR', 'AETOXGR', 'AESEV',
                     'AESER', 'AEREL', 'TRTEMFL', 'SAFFL', 'SEX'],
            'ADSL': ['USUBJID', 'AGE', 'SEX', 'RACE', 'ARM', 'TRT01A',
                     'SAFFL', 'ITTFL', 'EFFFL'],
            'AE': ['USUBJID', 'AEDECOD', 'AETERM', 'AETOXGR', 'AESEV', 'AESER'],
            'DM': ['USUBJID', 'AGE', 'SEX', 'RACE', 'ARM', 'ACTARM'],
        }

        # Provide schema for multiple tables so LLM can choose
        tables_to_show = ['ADSL', 'ADAE']  # Primary tables
        for t in tables_to_show:
            if t in [tbl.upper() for tbl in table_resolution.available_tables]:
                cols = key_columns.get(t, [])
                lines.append(f"{t}: {', '.join(cols)}")

        # Add column descriptions
        lines.append("")
        lines.append("COLUMN GUIDE:")
        lines.append("- AEDECOD: Adverse event coded term (use for specific AE searches like 'Nausea', 'Anaemia')")
        lines.append("- AESER: Serious adverse event flag ('Y' = serious)")
        lines.append("- ATOXGR: Maximum toxicity grade (VARCHAR with '.' for missing; cast to INTEGER after filtering '.')")
        lines.append("- TRTEMFL: Treatment-emergent flag ('Y' = occurred during treatment)")
        lines.append("- SAFFL: Safety population flag ('Y' = in safety population)")
        lines.append("- AGE: Patient age in years")
        lines.append("- SEX: Patient sex ('M' or 'F')")

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
        """Build entity resolution context - compact format."""
        if not entities:
            return ""

        # Compact inline format
        mappings = []
        for entity in entities:
            if entity.match_type == "grade":
                mappings.append(f"Grade {entity.matched_term}→{entity.column}")
            else:
                mappings.append(f"'{entity.original_term}'→{entity.matched_term}")

        return f"TERMS: {'; '.join(mappings)}" if mappings else ""

    def _build_clinical_rules(self, table_resolution: TableResolution) -> str:
        """Build clinical rules context - minimal to reduce tokens."""
        rules = []

        # Add CRITICAL grade column rule - must be emphatic
        grade_col = table_resolution.get_grade_column()
        if grade_col:
            # Strongly emphasize which column to use for toxicity grades
            if grade_col == 'ATOXGR':
                rules.append(
                    f"CRITICAL: For toxicity grade queries, MUST use {grade_col} "
                    f"(maximum grade during study). DO NOT use AETOXGR (grade at onset)."
                )
            elif grade_col == 'AETOXGR':
                rules.append(
                    f"CRITICAL: For toxicity grade queries, use {grade_col} "
                    f"(grade at onset). ATOXGR not available."
                )
            else:
                rules.append(f"CRITICAL: For toxicity grade queries, use {grade_col}")

        # Only add assumptions if present
        if table_resolution.assumptions:
            rules.append(f"Note: {'; '.join(table_resolution.assumptions)}")

        return "\n".join(rules) if rules else ""

    def _build_user_prompt(self,
                           query: str,
                           table_resolution: TableResolution,
                           entities: List[EntityMatch],
                           accumulated_filters: Optional[str] = None
                          ) -> str:
        """Build the user prompt - compact for fast inference."""
        lines = [f"Q: {query}"]

        # Add accumulated filters reminder for refinement queries
        if accumulated_filters:
            lines.append(f"REMEMBER: Include these filters from previous query: {accumulated_filters}")

        # Add entity hints inline if present
        entity_hints = []
        for entity in entities:
            if entity.table and entity.column:
                entity_hints.append(f"{entity.column}='{entity.matched_term}'")

        if entity_hints:
            lines.append(f"USE: {', '.join(entity_hints)}")

        return "\n".join(lines)

    def _build_refinement_context(self, accumulated_filters: str) -> str:
        """Build context for refinement queries that need to preserve filters."""
        return f"""

=== REFINEMENT QUERY RULES ===
This question is a FOLLOW-UP that refines previous results.
Your SQL MUST include ALL of these conditions from the previous query:
{accumulated_filters}

DO NOT interpret "of these", "of those", "out of these" as adverse event names.
These are REFERENCE WORDS meaning "from the previous result set".

Example correct pattern:
Previous: ITTFL = 'Y' AND AGE >= 65
New question: "out of these, how many had serious adverse events"
Correct SQL: WHERE ITTFL = 'Y' AND AGE >= 65 AND AESER = 'Y'
=== END REFINEMENT RULES ==="""

    def get_table_schema(self, table_name: str) -> Optional[SchemaInfo]:
        """Get schema for a specific table."""
        if table_name in self._schema_cache:
            return self._schema_cache[table_name]

        if not self.db_path:
            return None

        # Validate table name to prevent SQL injection
        from .sql_security import validate_table_name
        if not validate_table_name(table_name):
            logger.warning(f"Invalid table name rejected in get_table_schema: {table_name}")
            return None

        try:
            import duckdb
            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                # Get columns (table_name validated above)
                columns = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """).fetchall()

                # Get row count (table_name validated above)
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
