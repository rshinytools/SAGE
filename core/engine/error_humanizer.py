# SAGE - Error Humanizer
# ======================
"""
Natural Language Error Messages
===============================
Converts technical errors into user-friendly messages with actionable suggestions.

Error Categories:
1. Table not found - Suggest available tables or domains
2. Column not found - Suggest similar columns
3. Term not found - Suggest fuzzy matches or synonyms
4. SQL syntax error - Explain in plain language
5. Connection error - Service unavailability message
6. Timeout error - Explain delay and suggest simpler query
7. Blocked query - Explain why query was blocked

Example:
    humanizer = ErrorHumanizer()
    friendly = humanizer.humanize(
        error_type='term_not_found',
        technical_error="AEDECOD value 'highbloodpressure' not found",
        context={'term': 'highbloodpressure', 'suggestions': ['Hypertension', 'Blood pressure increased']}
    )
    # Returns: "The term 'highbloodpressure' wasn't found in the adverse events data.
    #           Did you mean: Hypertension, Blood pressure increased?"
"""

import re
import logging
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum

logger = logging.getLogger(__name__)


class ErrorType(Enum):
    """Types of errors that can be humanized."""
    TABLE_NOT_FOUND = "table_not_found"
    COLUMN_NOT_FOUND = "column_not_found"
    TERM_NOT_FOUND = "term_not_found"
    SQL_SYNTAX = "sql_syntax"
    SQL_VALIDATION = "sql_validation"
    CONNECTION = "connection"
    TIMEOUT = "timeout"
    BLOCKED_PHI = "blocked_phi"
    BLOCKED_INJECTION = "blocked_injection"
    BLOCKED_DANGEROUS = "blocked_dangerous"
    NO_RESULTS = "no_results"
    EXECUTION = "execution"
    LLM_UNAVAILABLE = "llm_unavailable"
    UNKNOWN = "unknown"


@dataclass
class HumanizedError:
    """A user-friendly error message."""
    error_type: ErrorType
    title: str                        # Short title
    message: str                      # Full message
    suggestions: List[str]            # Actionable suggestions
    technical_detail: Optional[str]   # Original error (for debugging)
    severity: str                     # 'error', 'warning', 'info'


class ErrorHumanizer:
    """
    Converts technical errors into user-friendly messages.

    Provides:
    - Clear explanation of what went wrong
    - Actionable suggestions to fix the issue
    - Context-appropriate tone
    """

    # Error message templates
    TEMPLATES = {
        ErrorType.TABLE_NOT_FOUND: {
            'title': "Data Not Found",
            'message': "I couldn't find data for '{table}' in the clinical database.",
            'suggestions': [
                "Available datasets include: {available_tables}",
                "Try asking about adverse events (ADAE), demographics (ADSL), or lab values (ADLB)"
            ]
        },
        ErrorType.COLUMN_NOT_FOUND: {
            'title': "Column Not Found",
            'message': "The column '{column}' wasn't found in the {table} dataset.",
            'suggestions': [
                "Similar columns that exist: {similar_columns}",
                "Check the data dictionary for available columns"
            ]
        },
        ErrorType.TERM_NOT_FOUND: {
            'title': "Term Not Found",
            'message': "The term '{term}' wasn't found in the {column} column.",
            'suggestions': [
                "Did you mean: {suggestions}?",
                "Try a more general term or check spelling"
            ]
        },
        ErrorType.SQL_SYNTAX: {
            'title': "Query Processing Error",
            'message': "There was a problem processing your query.",
            'suggestions': [
                "Try rephrasing your question",
                "Use simpler terms and avoid special characters"
            ]
        },
        ErrorType.SQL_VALIDATION: {
            'title': "Query Validation Failed",
            'message': "The generated query couldn't be validated: {reason}",
            'suggestions': [
                "Try a simpler question",
                "Avoid requesting operations that modify data"
            ]
        },
        ErrorType.CONNECTION: {
            'title': "Service Unavailable",
            'message': "The {service} service is currently unavailable.",
            'suggestions': [
                "Please try again in a few moments",
                "If the problem persists, contact support"
            ]
        },
        ErrorType.TIMEOUT: {
            'title': "Query Timeout",
            'message': "Your query took longer than expected to process.",
            'suggestions': [
                "Try a simpler or more specific question",
                "Add filters to reduce the data scope",
                "Complex queries may take 30-60 seconds on this system"
            ]
        },
        ErrorType.BLOCKED_PHI: {
            'title': "Protected Information",
            'message': "Your query appears to contain or request protected health information.",
            'suggestions': [
                "Remove any patient names, dates of birth, or other identifiers",
                "Ask aggregate questions (e.g., 'how many' rather than 'list patients')"
            ]
        },
        ErrorType.BLOCKED_INJECTION: {
            'title': "Invalid Query Format",
            'message': "Your query contains patterns that aren't allowed.",
            'suggestions': [
                "Use natural language questions",
                "Avoid SQL syntax or special characters in your question"
            ]
        },
        ErrorType.BLOCKED_DANGEROUS: {
            'title': "Operation Not Permitted",
            'message': "The requested operation is not allowed for safety reasons.",
            'suggestions': [
                "This system only supports read-only queries",
                "Data modification requests are blocked"
            ]
        },
        ErrorType.NO_RESULTS: {
            'title': "No Results Found",
            'message': "Your query returned no results.",
            'suggestions': [
                "The data may not contain matches for your criteria",
                "Try broadening your search terms",
                "Check if the terms are spelled correctly"
            ]
        },
        ErrorType.EXECUTION: {
            'title': "Execution Error",
            'message': "An error occurred while running the query.",
            'suggestions': [
                "Try rephrasing your question",
                "This may be a temporary issue - please try again"
            ]
        },
        ErrorType.LLM_UNAVAILABLE: {
            'title': "AI Service Unavailable",
            'message': "The AI processing service is currently unavailable.",
            'suggestions': [
                "The service may be starting up - please wait 30 seconds and try again",
                "If the problem persists, contact system administrator"
            ]
        },
        ErrorType.UNKNOWN: {
            'title': "Unexpected Error",
            'message': "An unexpected error occurred while processing your request.",
            'suggestions': [
                "Please try again",
                "If the problem continues, contact support"
            ]
        }
    }

    # Pattern matching for automatic error classification
    ERROR_PATTERNS = [
        (r'table.*not found|no such table', ErrorType.TABLE_NOT_FOUND),
        (r'column.*not found|unknown column|no such column', ErrorType.COLUMN_NOT_FOUND),
        (r'term.*not found|value.*not found', ErrorType.TERM_NOT_FOUND),
        (r'syntax error|parse error', ErrorType.SQL_SYNTAX),
        (r'validation.*fail|invalid.*sql', ErrorType.SQL_VALIDATION),
        (r'connection.*refused|cannot connect|unreachable', ErrorType.CONNECTION),
        (r'timeout|timed out|deadline exceeded', ErrorType.TIMEOUT),
        (r'phi|pii|protected|patient name|ssn|social security', ErrorType.BLOCKED_PHI),
        (r'injection|malicious|blocked.*pattern', ErrorType.BLOCKED_INJECTION),
        (r'delete|update|drop|truncate|alter|dangerous', ErrorType.BLOCKED_DANGEROUS),
        (r'no result|empty result|0 rows', ErrorType.NO_RESULTS),
        (r'execution.*error|failed to execute', ErrorType.EXECUTION),
        (r'ollama|llm.*unavailable|model.*not.*found', ErrorType.LLM_UNAVAILABLE),
    ]

    def __init__(self, available_tables: List[str] = None):
        """
        Initialize error humanizer.

        Args:
            available_tables: List of available table names for suggestions
        """
        self.available_tables = available_tables or [
            'ADAE', 'ADSL', 'ADLB', 'ADVS', 'ADCM', 'ADEX',
            'AE', 'DM', 'LB', 'VS', 'CM', 'EX'
        ]

    def humanize(self,
                 error_type: str = None,
                 technical_error: str = '',
                 context: Dict[str, Any] = None) -> HumanizedError:
        """
        Convert a technical error into a user-friendly message.

        Args:
            error_type: Optional explicit error type
            technical_error: The technical error message
            context: Additional context for formatting
                     e.g., {'term': 'xyz', 'suggestions': ['abc', 'def']}

        Returns:
            HumanizedError with friendly message and suggestions
        """
        context = context or {}

        # Determine error type
        if error_type:
            try:
                err_type = ErrorType(error_type)
            except ValueError:
                err_type = self._classify_error(technical_error)
        else:
            err_type = self._classify_error(technical_error)

        # Get template
        template = self.TEMPLATES.get(err_type, self.TEMPLATES[ErrorType.UNKNOWN])

        # Format message with context
        message = self._format_message(template['message'], context)
        suggestions = self._format_suggestions(template['suggestions'], context)

        return HumanizedError(
            error_type=err_type,
            title=template['title'],
            message=message,
            suggestions=suggestions,
            technical_detail=technical_error if technical_error else None,
            severity='error' if err_type not in [ErrorType.NO_RESULTS] else 'info'
        )

    def _classify_error(self, error_message: str) -> ErrorType:
        """Classify error based on message content."""
        if not error_message:
            return ErrorType.UNKNOWN

        error_lower = error_message.lower()

        for pattern, error_type in self.ERROR_PATTERNS:
            if re.search(pattern, error_lower, re.IGNORECASE):
                return error_type

        return ErrorType.UNKNOWN

    def _format_message(self, template: str, context: Dict[str, Any]) -> str:
        """Format message template with context values."""
        try:
            # Add available tables to context
            if 'available_tables' not in context:
                context['available_tables'] = ', '.join(self.available_tables[:5])

            # Handle list values in context
            for key, value in context.items():
                if isinstance(value, list):
                    context[key] = ', '.join(str(v) for v in value[:5])

            return template.format(**context)
        except KeyError:
            # If template variable not in context, return template with placeholders removed
            return re.sub(r'\{[^}]+\}', '', template).strip()

    def _format_suggestions(self, templates: List[str], context: Dict[str, Any]) -> List[str]:
        """Format suggestion templates with context values."""
        formatted = []

        for template in templates:
            try:
                # Only include suggestion if it has meaningful content
                suggestion = self._format_message(template, context)
                if suggestion and not suggestion.isspace():
                    # Skip suggestions like "Did you mean: ?" when no suggestions available
                    if 'Did you mean:' in suggestion and suggestion.endswith('?'):
                        if context.get('suggestions'):
                            formatted.append(suggestion)
                    else:
                        formatted.append(suggestion)
            except Exception:
                pass

        return formatted

    def humanize_from_exception(self, exception: Exception, context: Dict[str, Any] = None) -> HumanizedError:
        """
        Create humanized error from an exception.

        Args:
            exception: The exception that was raised
            context: Additional context

        Returns:
            HumanizedError
        """
        error_message = str(exception)
        return self.humanize(
            technical_error=error_message,
            context=context
        )

    def format_for_chat(self, error: HumanizedError) -> str:
        """
        Format error for chat UI display.

        Args:
            error: HumanizedError to format

        Returns:
            Formatted markdown string
        """
        lines = [
            f"**{error.title}**",
            "",
            error.message,
            ""
        ]

        if error.suggestions:
            lines.append("**Suggestions:**")
            for suggestion in error.suggestions:
                lines.append(f"- {suggestion}")

        return "\n".join(lines)


def create_error_humanizer(available_tables: List[str] = None) -> ErrorHumanizer:
    """
    Factory function to create ErrorHumanizer.

    Args:
        available_tables: List of available table names

    Returns:
        Configured ErrorHumanizer
    """
    return ErrorHumanizer(available_tables=available_tables)
