# SAGE - Explanation Generator
# ============================
"""
Explanation Generator
=====================
Generates human-readable explanations for query results.

Key Principle: Responses are written for CLINICAL/REGULATORY USERS,
not database engineers. All technical details should be:
- Hidden by default (available in Details section)
- Translated to clinical terminology
- Focused on what the data means, not how it's stored

This is STEP 9 of the 9-step pipeline.
"""

import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass
from datetime import datetime

from .models import (
    QueryMethodology,
    ConfidenceScore,
    ConfidenceLevel,
    ExecutionResult,
    EntityMatch
)
from .table_resolver import TableResolution
from .confidence_scorer import get_confidence_color
from .clinical_naming import ClinicalNamingService, get_naming_service

logger = logging.getLogger(__name__)


# =============================================================================
# CLINICAL-FRIENDLY NAMING - DYNAMIC LOOKUP
# =============================================================================
# Uses ClinicalNamingService for dynamic lookups from:
# 1. Golden Metadata (study-specific, human-approved)
# 2. CDISC Library (standard definitions)
# 3. Fallback to technical name

# Global naming service instance (initialized lazily)
_naming_service: Optional[ClinicalNamingService] = None


def _get_naming_service() -> Optional[ClinicalNamingService]:
    """Get the naming service, creating if needed."""
    global _naming_service
    if _naming_service is None:
        # Will be initialized with paths when pipeline starts
        _naming_service = ClinicalNamingService()
    return _naming_service


def init_naming_service(metadata_path: str = None, cdisc_db_path: str = None):
    """Initialize the naming service with data sources."""
    global _naming_service
    _naming_service = ClinicalNamingService(
        metadata_path=metadata_path,
        cdisc_db_path=cdisc_db_path
    )
    return _naming_service


def get_friendly_table_name(table: str) -> str:
    """Convert technical table name to user-friendly name."""
    service = _get_naming_service()
    if service:
        return service.get_table_label(table)
    return table


def get_friendly_column_name(column: str, table: str = None) -> str:
    """Convert technical column name to user-friendly name."""
    service = _get_naming_service()
    if service:
        return service.get_column_label(column, table)
    return column


def get_friendly_population_name(population: str) -> str:
    """Convert population name to user-friendly description."""
    service = _get_naming_service()
    if service:
        return service.get_population_description(population)
    return population


@dataclass
class ExplanationConfig:
    """Configuration for explanation generator."""
    # Include detailed methodology in answer (always available in metadata)
    include_methodology: bool = True

    # Include SQL query in main answer (DISABLED by default - available in Details)
    # Non-technical users don't need to see SQL
    include_sql: bool = False

    # Include confidence breakdown
    include_confidence_breakdown: bool = True

    # Include assumptions
    include_assumptions: bool = True

    # Use clinical-friendly names instead of technical names
    use_friendly_names: bool = True

    # Format for dates
    date_format: str = "%Y-%m-%d %H:%M:%S"


class ExplanationGenerator:
    """
    Generates human-readable explanations for query results.

    Provides:
    - Plain English answer
    - Methodology transparency
    - Confidence explanation
    - Assumptions made

    Example:
        generator = ExplanationGenerator()
        explanation = generator.generate(
            query="How many patients had headaches?",
            result=execution_result,
            methodology=methodology,
            confidence=confidence_score
        )
        print(explanation.to_markdown())
    """

    def __init__(self, config: Optional[ExplanationConfig] = None):
        """
        Initialize explanation generator.

        Args:
            config: Explanation configuration
        """
        self.config = config or ExplanationConfig()

    def generate(self,
                 query: str,
                 result: ExecutionResult,
                 table_resolution: TableResolution,
                 confidence: ConfidenceScore,
                 entities: List[EntityMatch] = None,
                 sql: str = None
                ) -> QueryMethodology:
        """
        Generate explanation for query result.

        Args:
            query: Original user query
            result: Execution result
            table_resolution: Table resolution information
            confidence: Confidence score
            entities: Resolved entities
            sql: Executed SQL query

        Returns:
            QueryMethodology with full explanation
        """
        # Build methodology
        methodology = QueryMethodology(
            query=query,
            table_used=table_resolution.selected_table,
            population_used=table_resolution.population_name,
            population_filter=table_resolution.population_filter,
            columns_used=table_resolution.table_columns[:10] if table_resolution.table_columns else [],
            entities_resolved=[
                {
                    'original': e.original_term,
                    'resolved': e.matched_term,
                    'confidence': e.confidence
                }
                for e in (entities or [])
            ],
            sql_executed=sql,
            confidence_score=confidence.overall_score,
            confidence_level=confidence.level.value,
            assumptions=table_resolution.assumptions,
            timestamp=datetime.now().isoformat()
        )

        return methodology

    def format_answer(self,
                      query: str,
                      result: ExecutionResult,
                      methodology: QueryMethodology
                     ) -> str:
        """
        Format the answer in plain English for clinical/regulatory users.

        Key principles for non-technical users:
        - Keep it SHORT and SIMPLE - one sentence when possible
        - Technical details (SQL, methodology, assumptions) go in Details section
        - Only show confidence score inline (not breakdown)

        Args:
            query: Original query
            result: Execution result
            methodology: Query methodology

        Returns:
            Formatted answer string - brief and conversational
        """
        # Generate the main answer - keep it brief!
        answer = self._generate_answer_text(query, result, methodology)

        # That's it for non-technical users - everything else is in Details
        return answer

    def _describe_filter(self, filter_sql: str) -> str:
        """Convert SQL filter to plain English description."""
        # Common filter translations
        filter_descriptions = {
            "SAFFL = 'Y'": "Only subjects in the Safety Population",
            "ITTFL = 'Y'": "Only subjects in the Intent-to-Treat Population",
            "EFFFL = 'Y'": "Only subjects in the Efficacy Population",
            "PPROTFL = 'Y'": "Only subjects in the Per-Protocol Population",
            "TRTEMFL = 'Y'": "Only treatment-emergent events",
            "AESER = 'Y'": "Only serious adverse events",
        }

        return filter_descriptions.get(filter_sql, f"Applied criteria: {filter_sql}")

    def _generate_answer_text(self,
                               query: str,
                               result: ExecutionResult,
                               methodology: QueryMethodology = None
                              ) -> str:
        """
        Generate the main answer text in natural, conversational language.

        Key principles for non-technical users:
        - Lead with a clear, direct statement
        - Use natural language that sounds like a human response
        - Keep it brief - no jargon, no technical explanations
        """
        if not result.success:
            return f"I couldn't answer your question due to an error: {result.error_message}"

        if result.row_count == 0:
            return "No results found matching your criteria."

        # Detect query type and format accordingly
        query_lower = query.lower()

        # Count queries - pass the original query for context-aware formatting
        if any(word in query_lower for word in ['how many', 'count', 'number of']):
            return self._format_count_answer(result, query, methodology)

        # List queries
        if any(word in query_lower for word in ['list', 'show', 'find', 'which']):
            return self._format_list_answer(result, query, methodology)

        # Default: summary
        return self._format_summary_answer(result, query, methodology)

    def _format_count_answer(self,
                              result: ExecutionResult,
                              query: str = "",
                              methodology: QueryMethodology = None
                             ) -> str:
        """
        Format count query answer in natural, conversational language.

        Key principles for non-technical users:
        - Lead with a simple, direct sentence
        - Use natural language, not technical jargon
        - Keep it brief - technical details are in the Details section
        """
        count_value = 0

        if result.data and len(result.data) > 0:
            row = result.data[0]
            # Find the count column
            for key, value in row.items():
                if 'count' in key.lower() or isinstance(value, (int, float)):
                    count_value = int(value) if value else 0
                    break

        query_lower = query.lower()

        # Determine response based on query context
        # Population count queries
        if 'population' in query_lower:
            pop_type = ""
            if 'safety' in query_lower:
                pop_type = "the safety population"
            elif 'itt' in query_lower or 'intent' in query_lower:
                pop_type = "the ITT population"
            elif 'efficacy' in query_lower:
                pop_type = "the efficacy population"
            else:
                pop_type = "this population"
            return f"There are **{count_value:,} subjects** in {pop_type}."

        # Adverse event queries - extract what they asked about
        if methodology and methodology.table_used in ['ADAE', 'AE']:
            # Try to extract the AE term from the query or SQL
            ae_term = None
            sql = methodology.sql_executed or ""

            # Look for AEDECOD = 'X' in the SQL
            import re
            match = re.search(r"AEDECOD\s*=\s*'([^']+)'", sql, re.IGNORECASE)
            if match:
                ae_term = match.group(1).title()

            # Also check entities resolved
            if not ae_term and methodology.entities_resolved:
                for entity in methodology.entities_resolved:
                    if entity.get('resolved'):
                        ae_term = entity['resolved'].title()
                        break

            if ae_term:
                return f"**{count_value:,} subjects** reported {ae_term}."
            else:
                return f"**{count_value:,} subjects** had adverse events matching your criteria."

        # Generic count response
        return f"There are **{count_value:,} subjects** matching your criteria."

    def _format_list_answer(self,
                            result: ExecutionResult,
                            query: str = "",
                            methodology: QueryMethodology = None
                           ) -> str:
        """
        Format list query answer in natural language.
        Keep it brief - the data table is shown separately in the UI.
        """
        if not result.data:
            return "No records found matching your criteria."

        # Just a brief statement - the table is shown in the Results section
        if result.row_count == 1:
            return f"Here is the **1 record** found:"
        else:
            return f"Here are the **{result.row_count:,} records** found:"

    def _format_summary_answer(self,
                               result: ExecutionResult,
                               query: str = "",
                               methodology: QueryMethodology = None
                              ) -> str:
        """Format summary answer in natural language - brief and direct."""
        if not result.data:
            return "No results found matching your criteria."

        # Keep it simple - results are shown in the table
        if result.row_count == 1:
            return f"Found **1 record** matching your query."
        else:
            return f"Found **{result.row_count:,} records** matching your query."

    def generate_error_response(self,
                                 query: str,
                                 error: str,
                                 stage: str
                                ) -> str:
        """
        Generate error response.

        Args:
            query: Original query
            error: Error message
            stage: Pipeline stage where error occurred

        Returns:
            Formatted error response
        """
        lines = [
            f"I was unable to process your query.",
            "",
            f"**Error:** {error}",
            "",
            f"**Stage:** {stage}",
            "",
            "Please try rephrasing your question or contact support if the issue persists."
        ]

        return "\n".join(lines)

    def generate_low_confidence_warning(self,
                                         confidence: ConfidenceScore
                                        ) -> str:
        """Generate warning for low confidence results."""
        if confidence.level == ConfidenceLevel.VERY_LOW:
            return (
                "**Warning:** This result has very low confidence. "
                "The answer may not be reliable. Please verify with a data expert."
            )
        elif confidence.level == ConfidenceLevel.LOW:
            return (
                "**Note:** This result has low confidence. "
                "Please review the methodology and verify the assumptions."
            )
        return ""


class ResponseBuilder:
    """
    Builds the final response combining all components.

    This is the final step that assembles:
    - Answer
    - Methodology
    - Confidence
    - Warnings
    """

    def __init__(self,
                 explanation_generator: ExplanationGenerator = None):
        """
        Initialize response builder.

        Args:
            explanation_generator: Explanation generator instance
        """
        self.explainer = explanation_generator or ExplanationGenerator()

    def build(self,
              query: str,
              result: ExecutionResult,
              table_resolution: TableResolution,
              confidence: ConfidenceScore,
              entities: List[EntityMatch] = None,
              sql: str = None
             ) -> Dict[str, Any]:
        """
        Build complete response.

        Args:
            query: Original query
            result: Execution result
            table_resolution: Table resolution
            confidence: Confidence score
            entities: Resolved entities
            sql: Executed SQL

        Returns:
            Complete response dictionary
        """
        # Generate methodology
        methodology = self.explainer.generate(
            query=query,
            result=result,
            table_resolution=table_resolution,
            confidence=confidence,
            entities=entities,
            sql=sql
        )

        # Format answer
        formatted_answer = self.explainer.format_answer(
            query=query,
            result=result,
            methodology=methodology
        )

        # Generate warnings
        warnings = []
        if confidence.level in [ConfidenceLevel.LOW, ConfidenceLevel.VERY_LOW]:
            warning = self.explainer.generate_low_confidence_warning(confidence)
            if warning:
                warnings.append(warning)

        if result.truncated:
            warnings.append("Results were truncated due to size limits.")

        # Build response with STANDARDIZED confidence format
        # CRITICAL: Confidence score must ALWAYS be:
        # - An integer 0-100 (never a float like 0.85, never stars like 5/5)
        # - Accompanied by level (high/medium/low/very_low)
        # This ensures consistent UI/reporting across all responses
        standardized_confidence = {
            'score': int(round(confidence.overall_score)),  # Always integer 0-100
            'level': confidence.level.value,  # Always string: high/medium/low/very_low
            'components': confidence.components,
            'explanation': confidence.explanation
        }

        return {
            'success': result.success,
            'answer': formatted_answer,
            'data': result.data,
            'row_count': result.row_count,
            'methodology': methodology.to_dict(),
            'confidence': standardized_confidence,
            'warnings': warnings,
            'sql': sql,
            'execution_time_ms': result.execution_time_ms
        }

    def build_error_response(self,
                              query: str,
                              error: str,
                              stage: str
                             ) -> Dict[str, Any]:
        """Build error response."""
        return {
            'success': False,
            'answer': self.explainer.generate_error_response(query, error, stage),
            'data': None,
            'row_count': 0,
            'error': error,
            'error_stage': stage,
            'methodology': None,
            'confidence': {
                'score': 0,
                'level': 'very_low',
                'components': {},
                'explanation': f"Query failed at {stage} stage."
            },
            'warnings': [f"Error occurred during {stage}"],
            'sql': None,
            'execution_time_ms': 0
        }
