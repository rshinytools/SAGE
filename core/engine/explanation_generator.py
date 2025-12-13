# SAGE - Explanation Generator
# ============================
"""
Explanation Generator
=====================
Generates human-readable explanations for query results.

Key features:
- Plain English explanations
- Methodology transparency
- Population and table used
- Confidence explanation

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

logger = logging.getLogger(__name__)


@dataclass
class ExplanationConfig:
    """Configuration for explanation generator."""
    # Include detailed methodology
    include_methodology: bool = True

    # Include SQL query
    include_sql: bool = True

    # Include confidence breakdown
    include_confidence_breakdown: bool = True

    # Include assumptions
    include_assumptions: bool = True

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
        Format the answer in plain English.

        Args:
            query: Original query
            result: Execution result
            methodology: Query methodology

        Returns:
            Formatted answer string
        """
        lines = []

        # Add main answer
        answer = self._generate_answer_text(query, result)
        lines.append(answer)
        lines.append("")

        # Add methodology if configured
        if self.config.include_methodology:
            lines.append("---")
            lines.append("**Methodology:**")
            lines.append(f"- **Table:** {methodology.table_used}")
            lines.append(f"- **Population:** {methodology.population_used}")

            if methodology.population_filter:
                lines.append(f"- **Filter:** `{methodology.population_filter}`")

            lines.append("")

        # Add confidence
        level_color = get_confidence_color(ConfidenceLevel(methodology.confidence_level))
        lines.append(f"**Confidence:** {methodology.confidence_score:.0f}% ({methodology.confidence_level})")
        lines.append("")

        # Add assumptions if any
        if self.config.include_assumptions and methodology.assumptions:
            lines.append("**Assumptions:**")
            for assumption in methodology.assumptions:
                lines.append(f"- {assumption}")
            lines.append("")

        # Add SQL if configured
        if self.config.include_sql and methodology.sql_executed:
            lines.append("**SQL Query:**")
            lines.append("```sql")
            lines.append(methodology.sql_executed)
            lines.append("```")

        return "\n".join(lines)

    def _generate_answer_text(self,
                               query: str,
                               result: ExecutionResult
                              ) -> str:
        """Generate the main answer text."""
        if not result.success:
            return f"I was unable to answer your question due to an error: {result.error_message}"

        if result.row_count == 0:
            return "No results found matching your criteria."

        # Detect query type and format accordingly
        query_lower = query.lower()

        # Count queries
        if any(word in query_lower for word in ['how many', 'count', 'number of']):
            return self._format_count_answer(result)

        # List queries
        if any(word in query_lower for word in ['list', 'show', 'find', 'which']):
            return self._format_list_answer(result)

        # Default: summary
        return self._format_summary_answer(result)

    def _format_count_answer(self, result: ExecutionResult) -> str:
        """Format count query answer."""
        if result.data and len(result.data) > 0:
            row = result.data[0]
            # Find the count column
            for key, value in row.items():
                if 'count' in key.lower() or isinstance(value, int):
                    return f"**Answer:** {value:,} subjects"

            # If no count column found, return row count
            return f"**Answer:** {result.row_count:,} records found"

        return "**Answer:** 0 subjects found"

    def _format_list_answer(self, result: ExecutionResult) -> str:
        """Format list query answer."""
        if not result.data:
            return "No records found."

        lines = [f"**Answer:** Found {result.row_count:,} records"]
        lines.append("")

        # Show first few records
        max_show = min(10, len(result.data))
        if result.columns:
            # Table format
            lines.append("| " + " | ".join(result.columns[:5]) + " |")
            lines.append("| " + " | ".join(["---"] * min(5, len(result.columns))) + " |")

            for i, row in enumerate(result.data[:max_show]):
                values = [str(row.get(col, ''))[:30] for col in result.columns[:5]]
                lines.append("| " + " | ".join(values) + " |")

        if result.row_count > max_show:
            lines.append(f"\n*...and {result.row_count - max_show} more records*")

        return "\n".join(lines)

    def _format_summary_answer(self, result: ExecutionResult) -> str:
        """Format summary answer."""
        if not result.data:
            return "No results found."

        lines = [f"**Answer:** Query returned {result.row_count:,} records"]

        if result.data and len(result.data) > 0:
            lines.append("")
            lines.append("First record:")
            for key, value in list(result.data[0].items())[:5]:
                lines.append(f"- {key}: {value}")

        return "\n".join(lines)

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

        # Build response
        return {
            'success': result.success,
            'answer': formatted_answer,
            'data': result.data,
            'row_count': result.row_count,
            'methodology': methodology.to_dict(),
            'confidence': {
                'score': confidence.overall_score,
                'level': confidence.level.value,
                'components': confidence.components,
                'explanation': confidence.explanation
            },
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
