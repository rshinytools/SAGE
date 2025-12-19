# SAGE - Unified Response Format
# ================================
"""
Unified Response Format
=======================
Single response structure for ALL SAGE responses.
This is the ONLY response format used anywhere in the system.

Key Principle: One format. Always. Everywhere.
- Confidence is ALWAYS an integer 0-100
- Structure is consistent regardless of query type
- All metadata for transparency is included
"""

import uuid
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


# =============================================================================
# RESPONSE TYPES
# =============================================================================

class ResponseType(Enum):
    """Types of responses SAGE can return."""
    ANSWER = "answer"                # Successful data query response
    CLARIFICATION = "clarification"  # Need more info from user
    ERROR = "error"                  # Something went wrong
    GREETING = "greeting"            # Hi, hello responses
    HELP = "help"                    # Help/capabilities responses
    CONVERSATION = "conversation"    # General conversational response
    IDENTITY = "identity"            # Questions about what SAGE is


# =============================================================================
# METHODOLOGY INFO
# =============================================================================

@dataclass
class EntityResolution:
    """How a term was resolved."""
    original: str           # What user said
    resolved: str           # What it mapped to
    source: str             # Where the mapping came from
    confidence: float       # 0-1


@dataclass
class MethodologyInfo:
    """How the answer was derived - for transparency."""

    # Data source used
    data_source: str  # "Adverse Events Analysis Dataset"

    # Technical table name
    table_name: str  # "ADAE"

    # Population
    population: str  # "Safety Population"
    population_filter: Optional[str] = None  # "SAFFL='Y'"

    # Key filters applied (in plain English)
    filters_applied: List[str] = field(default_factory=list)

    # SQL (hidden by default, available on request)
    sql: Optional[str] = None

    # Entities resolved
    entities: List[EntityResolution] = field(default_factory=list)

    # Columns used
    columns_used: List[str] = field(default_factory=list)

    # Assumptions made
    assumptions: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'data_source': self.data_source,
            'table_name': self.table_name,
            'population': self.population,
            'population_filter': self.population_filter,
            'filters_applied': self.filters_applied,
            'sql': self.sql,
            'entities': [
                {'original': e.original, 'resolved': e.resolved,
                 'source': e.source, 'confidence': e.confidence}
                for e in self.entities
            ],
            'columns_used': self.columns_used,
            'assumptions': self.assumptions
        }


# =============================================================================
# CLARIFICATION INFO
# =============================================================================

@dataclass
class ClarificationOption:
    """An option for user to choose."""
    id: int
    text: str


@dataclass
class ClarificationInfo:
    """Information for clarification request."""
    message: str
    questions: List[Dict[str, Any]]  # List of {question, options}
    suggested_rephrasing: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'message': self.message,
            'questions': self.questions,
            'suggested_rephrasing': self.suggested_rephrasing
        }


# =============================================================================
# ERROR INFO
# =============================================================================

@dataclass
class ErrorInfo:
    """Information about an error."""
    message: str           # User-friendly message
    stage: str             # Where error occurred
    code: Optional[str] = None  # Error code
    details: Optional[str] = None  # Technical details
    suggestion: Optional[str] = None  # What user can do

    def to_dict(self) -> Dict[str, Any]:
        return {
            'message': self.message,
            'stage': self.stage,
            'code': self.code,
            'details': self.details,
            'suggestion': self.suggestion
        }


# =============================================================================
# UNIFIED SAGE RESPONSE
# =============================================================================

@dataclass
class SAGEResponse:
    """
    Unified response format for ALL SAGE responses.
    This is the ONLY response format used anywhere in the system.
    """

    # === Always Present ===

    # Unique response ID
    response_id: str = field(default_factory=lambda: f"resp_{uuid.uuid4().hex[:12]}")

    # Original query
    query: str = ""

    # Response type
    type: ResponseType = ResponseType.ANSWER

    # Timestamp
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    # === Answer Fields (when type=ANSWER) ===

    # The answer in natural language
    answer: Optional[str] = None

    # Structured data (if applicable)
    data: Optional[List[Dict]] = None

    # Row count
    row_count: int = 0

    # === Confidence (ALWAYS present for ANSWER type) ===

    # Single confidence score: 0-100 integer - NEVER decimals, NEVER stars
    confidence_score: int = 0

    # Confidence level: high/medium/low/very_low
    confidence_level: str = "unknown"

    # Confidence explanation
    confidence_explanation: str = ""

    # === Methodology (for transparency) ===

    methodology: Optional[MethodologyInfo] = None

    # === Clarification (when type=CLARIFICATION) ===

    clarification: Optional[ClarificationInfo] = None

    # === Error (when type=ERROR) ===

    error: Optional[ErrorInfo] = None

    # === Warnings ===

    warnings: List[str] = field(default_factory=list)

    # === Metadata ===

    execution_time_ms: float = 0
    cached: bool = False
    pipeline_used: bool = True

    # Additional metadata
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        result = {
            'response_id': self.response_id,
            'query': self.query,
            'type': self.type.value,
            'timestamp': self.timestamp,
            'success': self.type not in [ResponseType.ERROR, ResponseType.CLARIFICATION],

            # Confidence - ALWAYS integer 0-100
            'confidence': {
                'score': int(self.confidence_score),  # Ensure integer
                'level': self.confidence_level,
                'explanation': self.confidence_explanation
            },

            'execution_time_ms': self.execution_time_ms,
            'cached': self.cached,
            'warnings': self.warnings
        }

        # Add type-specific fields
        if self.type == ResponseType.ANSWER:
            result['answer'] = self.answer
            result['data'] = self.data
            result['row_count'] = self.row_count
            if self.methodology:
                result['methodology'] = self.methodology.to_dict()

        elif self.type == ResponseType.CLARIFICATION:
            if self.clarification:
                result['clarification'] = self.clarification.to_dict()

        elif self.type == ResponseType.ERROR:
            if self.error:
                result['error'] = self.error.to_dict()

        elif self.type in [ResponseType.GREETING, ResponseType.HELP,
                          ResponseType.CONVERSATION, ResponseType.IDENTITY]:
            result['answer'] = self.answer

        # Add metadata
        if self.metadata:
            result['metadata'] = self.metadata

        return result

    def to_api_response(self) -> Dict[str, Any]:
        """Convert to API response format (alias for to_dict)."""
        return self.to_dict()


# =============================================================================
# RESPONSE BUILDERS
# =============================================================================

class SAGEResponseBuilder:
    """Builder for creating standardized SAGE responses."""

    @staticmethod
    def success(
        query: str,
        answer: str,
        data: Optional[List[Dict]] = None,
        row_count: int = 0,
        confidence_score: int = 85,
        confidence_level: str = "high",
        confidence_explanation: str = "",
        methodology: Optional[MethodologyInfo] = None,
        execution_time_ms: float = 0,
        cached: bool = False,
        warnings: List[str] = None
    ) -> SAGEResponse:
        """Build a successful answer response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.ANSWER,
            answer=answer,
            data=data,
            row_count=row_count,
            confidence_score=int(confidence_score),  # Ensure integer
            confidence_level=confidence_level,
            confidence_explanation=confidence_explanation,
            methodology=methodology,
            execution_time_ms=execution_time_ms,
            cached=cached,
            warnings=warnings or [],
            pipeline_used=True
        )

    @staticmethod
    def clarification_needed(
        query: str,
        message: str,
        questions: List[Dict[str, Any]],
        suggested_rephrasing: Optional[str] = None
    ) -> SAGEResponse:
        """Build a clarification request response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.CLARIFICATION,
            answer=message,
            clarification=ClarificationInfo(
                message=message,
                questions=questions,
                suggested_rephrasing=suggested_rephrasing
            ),
            confidence_score=0,
            confidence_level="unknown",
            pipeline_used=False
        )

    @staticmethod
    def error(
        query: str,
        message: str,
        stage: str,
        suggestion: Optional[str] = None,
        code: Optional[str] = None
    ) -> SAGEResponse:
        """Build an error response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.ERROR,
            answer=f"I was unable to process your query: {message}",
            error=ErrorInfo(
                message=message,
                stage=stage,
                suggestion=suggestion,
                code=code
            ),
            confidence_score=0,
            confidence_level="very_low",
            pipeline_used=True
        )

    @staticmethod
    def greeting(query: str, answer: str) -> SAGEResponse:
        """Build a greeting response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.GREETING,
            answer=answer,
            confidence_score=100,
            confidence_level="high",
            pipeline_used=False,
            metadata={'instant': True}
        )

    @staticmethod
    def help_response(query: str, answer: str) -> SAGEResponse:
        """Build a help response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.HELP,
            answer=answer,
            confidence_score=100,
            confidence_level="high",
            pipeline_used=False,
            metadata={'instant': True}
        )

    @staticmethod
    def conversation(query: str, answer: str) -> SAGEResponse:
        """Build a conversational response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.CONVERSATION,
            answer=answer,
            confidence_score=100,
            confidence_level="high",
            pipeline_used=False,
            metadata={'intent': 'conversation'}
        )

    @staticmethod
    def identity(query: str, answer: str) -> SAGEResponse:
        """Build an identity response."""
        return SAGEResponse(
            query=query,
            type=ResponseType.IDENTITY,
            answer=answer,
            confidence_score=100,
            confidence_level="high",
            pipeline_used=False,
            metadata={'intent': 'identity'}
        )


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def standardize_confidence(score: float) -> int:
    """
    Standardize confidence score to integer 0-100.

    Args:
        score: Raw score (could be 0-1, 0-100, or any format)

    Returns:
        Integer 0-100
    """
    # Handle 0-1 scale
    if 0 <= score <= 1:
        score = score * 100

    # Clamp to 0-100
    score = max(0, min(100, score))

    # Return as integer
    return int(round(score))


def get_confidence_level(score: int) -> str:
    """
    Get confidence level from score.

    Args:
        score: Integer 0-100

    Returns:
        Level string: high/medium/low/very_low
    """
    if score >= 90:
        return "high"
    elif score >= 70:
        return "medium"
    elif score >= 50:
        return "low"
    else:
        return "very_low"


def confidence_to_color(level: str) -> str:
    """Get display color for confidence level."""
    colors = {
        "high": "green",
        "medium": "yellow",
        "low": "orange",
        "very_low": "red",
        "unknown": "gray"
    }
    return colors.get(level, "gray")
