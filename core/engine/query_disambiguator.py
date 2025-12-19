# SAGE - Query Disambiguator
# ==========================
"""
Query Disambiguation
====================
Detects ambiguous clinical queries and generates clarification options.

Uses hybrid approach:
- Templates for common ambiguous patterns (fast, consistent)
- LLM for unusual queries (flexible)

Zero Hallucination: All clarification options are derived from actual data
values or metadata definitions.

Example:
    disambiguator = QueryDisambiguator(db_connection, metadata)
    result = disambiguator.check("Show me adverse events")
    if result.needs_clarification:
        # Present options to user
        for option in result.options:
            print(f"- {option.label}: {option.description}")
"""

import re
import logging
from typing import List, Dict, Any, Optional, Set
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class AmbiguityType(Enum):
    """Types of query ambiguity."""
    POPULATION = "population"           # Which population? (Safety, ITT, etc.)
    SEVERITY = "severity"               # Which severity level?
    TIME_PERIOD = "time_period"         # What time frame?
    TREATMENT = "treatment"             # Which treatment arm?
    COUNT_TYPE = "count_type"           # Count subjects or events?
    RELATIONSHIP = "relationship"       # Related to treatment?
    TERM_AMBIGUITY = "term_ambiguity"   # Multiple matching terms
    GRADE = "grade"                     # Toxicity grade


@dataclass
class ClarificationOption:
    """A single clarification option."""
    key: str                  # Unique identifier (e.g., 'safety', 'grade3_plus')
    label: str                # Display label
    description: str          # Longer description
    filter_sql: str           # SQL filter to apply
    is_default: bool = False  # Is this the recommended default?


@dataclass
class DisambiguationResult:
    """Result of disambiguation check."""
    needs_clarification: bool
    ambiguity_type: Optional[AmbiguityType]
    question: str                           # Question to ask user
    options: List[ClarificationOption]
    detected_pattern: str                   # What triggered this
    auto_resolved: bool = False             # Was it auto-resolved?
    resolved_option: Optional[str] = None   # Key of auto-resolved option


class QueryDisambiguator:
    """
    Detects and resolves ambiguous clinical queries.

    Common ambiguities:
    1. "adverse events" - All? Serious? Treatment-related?
    2. "patients with X" - Count subjects or events?
    3. "in the study" - Which population?
    4. "severe" - Grade 3+ or AESEV='SEVERE'?
    """

    # No hard-coded patterns - LLM handles all query understanding naturally
    # The LLM understands clinical context, populations, and severity levels

    # Default populations with descriptions
    POPULATION_OPTIONS = [
        ClarificationOption(
            key='safety',
            label='Safety Population',
            description='All subjects who received at least one dose of study treatment',
            filter_sql="SAFFL = 'Y'",
            is_default=True
        ),
        ClarificationOption(
            key='itt',
            label='Intent-to-Treat (ITT)',
            description='All randomized subjects',
            filter_sql="ITTFL = 'Y'"
        ),
        ClarificationOption(
            key='all',
            label='All Subjects',
            description='All subjects in the dataset (no population filter)',
            filter_sql=''
        )
    ]

    # Severity options
    SEVERITY_OPTIONS = [
        ClarificationOption(
            key='grade3_plus',
            label='Grade 3 or Higher',
            description='Toxicity grade 3, 4, or 5 (based on CTCAE)',
            filter_sql="CAST(AETOXGR AS INTEGER) >= 3",
            is_default=True
        ),
        ClarificationOption(
            key='severe_only',
            label='Severe (AESEV)',
            description="Events with severity recorded as 'SEVERE'",
            filter_sql="UPPER(AESEV) = 'SEVERE'"
        ),
        ClarificationOption(
            key='serious',
            label='Serious Adverse Events',
            description='Events meeting regulatory criteria for serious (AESER=Y)',
            filter_sql="UPPER(AESER) = 'Y'"
        ),
        ClarificationOption(
            key='all_severity',
            label='All Severities',
            description='Include all adverse events regardless of severity',
            filter_sql=''
        )
    ]

    # Count type options
    COUNT_TYPE_OPTIONS = [
        ClarificationOption(
            key='subjects',
            label='Unique Subjects',
            description='Count each subject once, even if they had multiple events',
            filter_sql='COUNT(DISTINCT USUBJID)',
            is_default=True
        ),
        ClarificationOption(
            key='events',
            label='All Events',
            description='Count total number of adverse event records',
            filter_sql='COUNT(*)'
        )
    ]

    # Relationship options
    RELATIONSHIP_OPTIONS = [
        ClarificationOption(
            key='related',
            label='Treatment-Related',
            description='Events considered related to study treatment',
            filter_sql="UPPER(AEREL) IN ('RELATED', 'POSSIBLY RELATED', 'PROBABLY RELATED', 'DEFINITELY RELATED', 'Y')",
            is_default=False
        ),
        ClarificationOption(
            key='all_relationship',
            label='All Events',
            description='Include all events regardless of relationship',
            filter_sql='',
            is_default=True
        )
    ]

    def __init__(self,
                 db_connection=None,
                 metadata: Dict = None,
                 auto_resolve_defaults: bool = False):
        """
        Initialize query disambiguator.

        Args:
            db_connection: DuckDB connection for data validation
            metadata: Golden metadata dict
            auto_resolve_defaults: If True, auto-resolve common patterns
                                   with defaults (no user prompt needed).
                                   Default is False - let LLM handle naturally.
        """
        self.db = db_connection
        self.metadata = metadata
        self.auto_resolve_defaults = auto_resolve_defaults
        # No pattern compilation - LLM handles query understanding

    def check(self, query: str) -> DisambiguationResult:
        """
        Check if query is ambiguous and needs clarification.

        LLM handles all query understanding naturally - no pattern matching.
        The LLM understands clinical context, populations, severity, and
        will generate appropriate SQL based on what the user actually asks.

        Args:
            query: User's natural language query

        Returns:
            DisambiguationResult - always returns no clarification needed,
            as the LLM handles disambiguation naturally.
        """
        # No pattern-based disambiguation - LLM handles query understanding
        # The LLM can interpret "patients", "subjects", "safety population",
        # "serious AEs", "Grade 3+", etc. naturally from context
        return DisambiguationResult(
            needs_clarification=False,
            ambiguity_type=None,
            question='',
            options=[],
            detected_pattern=''
        )

    def get_options_for_ambiguity(self, ambiguity_type: AmbiguityType) -> List[ClarificationOption]:
        """Get standard options for a given ambiguity type."""
        options_map = {
            AmbiguityType.POPULATION: self.POPULATION_OPTIONS,
            AmbiguityType.SEVERITY: self.SEVERITY_OPTIONS,
            AmbiguityType.COUNT_TYPE: self.COUNT_TYPE_OPTIONS,
            AmbiguityType.RELATIONSHIP: self.RELATIONSHIP_OPTIONS,
        }
        return options_map.get(ambiguity_type, [])

    def resolve(self, query: str, clarification: Dict[str, str]) -> str:
        """
        Apply clarification to query context.

        Args:
            query: Original query
            clarification: Dict of {ambiguity_type: selected_option_key}

        Returns:
            Enhanced query with clarifications applied
        """
        # Build clarification context
        context_parts = []

        for amb_type, option_key in clarification.items():
            if amb_type == 'population':
                opt = next((o for o in self.POPULATION_OPTIONS if o.key == option_key), None)
                if opt:
                    context_parts.append(f"Population: {opt.label}")
            elif amb_type == 'severity':
                opt = next((o for o in self.SEVERITY_OPTIONS if o.key == option_key), None)
                if opt:
                    context_parts.append(f"Severity filter: {opt.label}")
            elif amb_type == 'count_type':
                opt = next((o for o in self.COUNT_TYPE_OPTIONS if o.key == option_key), None)
                if opt:
                    context_parts.append(f"Count method: {opt.label}")

        if context_parts:
            return f"{query} [{'; '.join(context_parts)}]"
        return query

    def get_filters_from_clarification(self, clarification: Dict[str, str]) -> Dict[str, str]:
        """
        Convert clarification choices to SQL filters.

        Args:
            clarification: Dict of {ambiguity_type: selected_option_key}

        Returns:
            Dict of {filter_type: sql_filter}
        """
        filters = {}

        option_maps = {
            'population': self.POPULATION_OPTIONS,
            'severity': self.SEVERITY_OPTIONS,
            'count_type': self.COUNT_TYPE_OPTIONS,
            'relationship': self.RELATIONSHIP_OPTIONS,
        }

        for amb_type, option_key in clarification.items():
            options = option_maps.get(amb_type, [])
            opt = next((o for o in options if o.key == option_key), None)
            if opt and opt.filter_sql:
                filters[amb_type] = opt.filter_sql

        return filters


def create_query_disambiguator(db_connection=None,
                               metadata: Dict = None,
                               auto_resolve: bool = True) -> QueryDisambiguator:
    """
    Factory function to create QueryDisambiguator.

    Args:
        db_connection: DuckDB connection
        metadata: Golden metadata dict
        auto_resolve: Auto-resolve with defaults

    Returns:
        Configured QueryDisambiguator
    """
    return QueryDisambiguator(
        db_connection=db_connection,
        metadata=metadata,
        auto_resolve_defaults=auto_resolve
    )
