"""
Helpful Refusal System - Actionable guidance when confidence is low.

Instead of generic refusals, provides specific guidance on what
information is needed or how to rephrase the query.
"""

from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum


class RefusalReason(Enum):
    """Reasons for refusing to answer a query."""
    AMBIGUOUS_TERM = "AMBIGUOUS_TERM"
    LOW_SIMILARITY = "LOW_SIMILARITY"
    SCHEMA_MISMATCH = "SCHEMA_MISMATCH"
    COMPLEX_QUERY = "COMPLEX_QUERY"
    OUT_OF_SCOPE = "OUT_OF_SCOPE"
    MISSING_DATA = "MISSING_DATA"
    VALIDATION_FAILED = "VALIDATION_FAILED"
    CONFIDENCE_TOO_LOW = "CONFIDENCE_TOO_LOW"


@dataclass
class HelpfulRefusal:
    """A helpful refusal with actionable guidance."""
    reason: RefusalReason
    message: str
    clarifying_questions: List[str]
    suggestions: List[str]
    alternative_queries: List[str]
    can_partially_answer: bool
    partial_answer: Optional[str]


class HelpfulRefusalSystem:
    """Generate helpful, actionable refusals."""

    def generate_refusal(
        self,
        reason: RefusalReason,
        context: Optional[Dict] = None
    ) -> HelpfulRefusal:
        """
        Generate a helpful refusal based on reason and context.

        Args:
            reason: Why the query cannot be answered
            context: Additional context (terms, issues, etc.)

        Returns:
            HelpfulRefusal with actionable guidance
        """
        context = context or {}

        if reason == RefusalReason.AMBIGUOUS_TERM:
            return self._ambiguous_term_refusal(context)

        elif reason == RefusalReason.LOW_SIMILARITY:
            return self._low_similarity_refusal(context)

        elif reason == RefusalReason.SCHEMA_MISMATCH:
            return self._schema_mismatch_refusal(context)

        elif reason == RefusalReason.COMPLEX_QUERY:
            return self._complex_query_refusal(context)

        elif reason == RefusalReason.OUT_OF_SCOPE:
            return self._out_of_scope_refusal(context)

        elif reason == RefusalReason.MISSING_DATA:
            return self._missing_data_refusal(context)

        elif reason == RefusalReason.CONFIDENCE_TOO_LOW:
            return self._low_confidence_refusal(context)

        else:
            return self._generic_refusal(context)

    def _ambiguous_term_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal for ambiguous clinical terms."""
        terms = context.get('ambiguous_terms', [])
        term_list = ', '.join(f"'{t}'" for t in terms) if terms else "some terms"

        clarifications = context.get('clarifications_needed', [])
        if not clarifications:
            clarifications = [f"Please define: {term_list}"]

        return HelpfulRefusal(
            reason=RefusalReason.AMBIGUOUS_TERM,
            message=f"I need clarification on the following terms to provide an accurate answer: {term_list}",
            clarifying_questions=clarifications,
            suggestions=[
                "Specify exact thresholds (e.g., 'blood pressure > 140/90' instead of 'high blood pressure')",
                "Use standard CDISC terminology when possible",
                "Reference the study protocol definition if applicable"
            ],
            alternative_queries=[
                context.get('suggested_query', '')
            ] if context.get('suggested_query') else [],
            can_partially_answer=False,
            partial_answer=None
        )

    def _low_similarity_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal for queries with no similar examples."""
        best_match = context.get('best_match', {})

        suggestions = [
            "Try rephrasing your question using clinical terminology",
            "Break down complex questions into simpler parts",
            "Check if the data you're asking about is available in the system"
        ]

        alternatives = []
        if best_match and best_match.get('question'):
            alternatives.append(
                f"Similar question I can answer: \"{best_match.get('question', '')}\""
            )

        return HelpfulRefusal(
            reason=RefusalReason.LOW_SIMILARITY,
            message="I don't have enough confidence to answer this query accurately. This type of question may not be in my training data.",
            clarifying_questions=[
                "Could you rephrase your question?",
                "What specific data are you looking for?"
            ],
            suggestions=suggestions,
            alternative_queries=alternatives,
            can_partially_answer=False,
            partial_answer=None
        )

    def _schema_mismatch_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal for schema validation failures."""
        issues = context.get('schema_issues', [])

        issue_suggestions = []
        for issue in issues:
            if hasattr(issue, 'suggestion') and issue.suggestion:
                issue_suggestions.append(
                    f"Column '{issue.element}' not found. Did you mean '{issue.suggestion}'?"
                )

        return HelpfulRefusal(
            reason=RefusalReason.SCHEMA_MISMATCH,
            message="The requested data columns or tables are not available in the current dataset.",
            clarifying_questions=[
                "Are you looking for data from a specific domain (e.g., demographics, adverse events, labs)?"
            ],
            suggestions=issue_suggestions + [
                "Check the available tables in the Data Factory",
                "Verify column names against the CDISC standard"
            ],
            alternative_queries=[],
            can_partially_answer=False,
            partial_answer=None
        )

    def _complex_query_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal for overly complex queries."""
        simpler_queries = context.get('simpler_queries', [
            "First: How many subjects are in the study?",
            "Then: Filter by your specific criteria"
        ])

        return HelpfulRefusal(
            reason=RefusalReason.COMPLEX_QUERY,
            message="This query is too complex for me to answer with high confidence. Let me suggest breaking it down.",
            clarifying_questions=[],
            suggestions=[
                "Break this into multiple simpler questions",
                "Ask for one analysis at a time",
                "Start with a basic count, then add filters"
            ],
            alternative_queries=simpler_queries,
            can_partially_answer=True,
            partial_answer=context.get('partial_answer', "I can help with parts of this query if you break it down.")
        )

    def _out_of_scope_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal for out-of-scope queries."""
        return HelpfulRefusal(
            reason=RefusalReason.OUT_OF_SCOPE,
            message="This question is outside my capabilities. I can only query and analyze clinical trial data.",
            clarifying_questions=[],
            suggestions=[
                "I can help with: patient counts, demographics, adverse events, lab values, concomitant medications",
                "I cannot: make clinical judgments, predict outcomes, compare to external literature, or provide medical advice"
            ],
            alternative_queries=[],
            can_partially_answer=False,
            partial_answer=None
        )

    def _missing_data_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal when required data is missing."""
        missing = context.get('missing_tables', [])
        missing_str = ', '.join(missing) if missing else "required data"

        return HelpfulRefusal(
            reason=RefusalReason.MISSING_DATA,
            message=f"The required data ({missing_str}) is not loaded in the system.",
            clarifying_questions=[],
            suggestions=[
                "Check if the data has been uploaded in the Data Factory",
                "Verify the data domain is supported (SDTM or ADaM)",
                "Contact an administrator to load the required data"
            ],
            alternative_queries=[],
            can_partially_answer=False,
            partial_answer=None
        )

    def _low_confidence_refusal(self, context: Dict) -> HelpfulRefusal:
        """Refusal when confidence is too low."""
        confidence = context.get('confidence', 0)
        warnings = context.get('warnings', [])

        return HelpfulRefusal(
            reason=RefusalReason.CONFIDENCE_TOO_LOW,
            message=f"My confidence in answering this query is only {confidence:.0f}%, which is below the threshold for reliable answers.",
            clarifying_questions=[
                "Can you provide more specific criteria?",
                "Would you like me to show you what data is available?"
            ],
            suggestions=[
                f"Issue: {w}" for w in warnings[:3]
            ] if warnings else [
                "Try using more specific terminology",
                "Reference specific table or column names if known"
            ],
            alternative_queries=[],
            can_partially_answer=True,
            partial_answer=context.get('partial_answer')
        )

    def _generic_refusal(self, context: Dict) -> HelpfulRefusal:
        """Generic refusal with helpful guidance."""
        return HelpfulRefusal(
            reason=RefusalReason.VALIDATION_FAILED,
            message="I cannot provide a reliable answer to this question.",
            clarifying_questions=[
                "Could you provide more details about what you're looking for?",
                "Is there a specific data point or metric you need?"
            ],
            suggestions=[
                "Try a simpler, more specific question",
                "Use exact column names if known",
                "Refer to the documentation for supported query types"
            ],
            alternative_queries=[],
            can_partially_answer=False,
            partial_answer=None
        )

    def format_refusal_message(self, refusal: HelpfulRefusal) -> str:
        """
        Format refusal as user-friendly message.

        Args:
            refusal: The HelpfulRefusal to format

        Returns:
            Formatted message string
        """
        lines = [refusal.message, ""]

        if refusal.clarifying_questions:
            lines.append("**I need to know:**")
            for q in refusal.clarifying_questions:
                lines.append(f"- {q}")
            lines.append("")

        if refusal.suggestions:
            lines.append("**Suggestions:**")
            for s in refusal.suggestions:
                lines.append(f"- {s}")
            lines.append("")

        if refusal.alternative_queries:
            lines.append("**Try asking:**")
            for a in refusal.alternative_queries:
                if a:  # Skip empty strings
                    lines.append(f"- {a}")
            lines.append("")

        if refusal.can_partially_answer and refusal.partial_answer:
            lines.append("**What I can tell you:**")
            lines.append(refusal.partial_answer)

        return "\n".join(lines)
