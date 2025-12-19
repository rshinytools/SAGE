# SAGE - Engine Models
# =====================
"""
Common dataclasses and models for the Inference Engine.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional, Any
from enum import Enum
from datetime import datetime
import pandas as pd


class QueryIntent(Enum):
    """Types of query intent."""
    DATA = "data"           # Needs SQL execution
    DOCUMENT = "document"   # Needs RAG/documentation
    HYBRID = "hybrid"       # Both data and explanation
    UNKNOWN = "unknown"


class ConfidenceLevel(Enum):
    """Confidence score thresholds."""
    HIGH = "high"           # 90-100%: High confidence (green)
    MEDIUM = "medium"       # 70-89%: Medium confidence (yellow)
    LOW = "low"             # 50-69%: Low confidence (orange)
    VERY_LOW = "very_low"   # <50%: Very low confidence (red)

    @classmethod
    def from_score(cls, score: float) -> 'ConfidenceLevel':
        """Get confidence level from score."""
        if score >= 90:
            return cls.HIGH
        elif score >= 70:
            return cls.MEDIUM
        elif score >= 50:
            return cls.LOW
        else:
            return cls.VERY_LOW


@dataclass
class SanitizationResult:
    """Result of input sanitization."""
    is_safe: bool
    sanitized_query: str
    original_query: str
    blocked_reason: Optional[str] = None
    detected_patterns: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class EntityMatch:
    """A matched entity from the query."""
    original_term: str          # What user typed
    matched_term: str           # What we found in data
    match_type: str             # exact, fuzzy, meddra, medical_synonym, etc.
    confidence: float           # 0-100
    table: Optional[str] = None
    column: Optional[str] = None
    meddra_code: Optional[str] = None
    meddra_level: Optional[str] = None  # PT, HLT, HLGT, SOC
    metadata: Optional[Dict[str, Any]] = None  # Additional data (e.g., all_variants for synonyms)


@dataclass
class EntityExtractionResult:
    """Result of entity extraction."""
    entities: List[EntityMatch]
    query_with_resolved: str    # Query with entities replaced
    unresolved_terms: List[str]
    processing_time_ms: float


@dataclass
class RouteDecision:
    """Decision from query router."""
    intent: QueryIntent
    confidence: float
    reasoning: str
    suggested_tables: List[str] = field(default_factory=list)


@dataclass
class LLMContext:
    """Context prepared for the LLM."""
    system_prompt: str
    user_prompt: str
    schema_context: str
    entity_context: str
    clinical_rules: str
    token_count_estimate: int


@dataclass
class GeneratedSQL:
    """Result of SQL generation."""
    sql: str
    reasoning: str              # LLM's thinking process
    tables_used: List[str]
    columns_used: List[str]
    filters_applied: List[str]
    generation_time_ms: float
    model_used: str
    raw_response: Optional[str] = None


@dataclass
class ValidationResult:
    """Result of SQL validation."""
    is_valid: bool
    validated_sql: str
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    tables_verified: List[str] = field(default_factory=list)
    columns_verified: List[str] = field(default_factory=list)
    dangerous_patterns_found: List[str] = field(default_factory=list)


@dataclass
class ExecutionResult:
    """Result of SQL execution."""
    success: bool
    data: Optional[List[Dict]] = None  # Result as list of dicts
    columns: List[str] = field(default_factory=list)  # Column names
    row_count: int = 0
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None
    truncated: bool = False  # If results were truncated
    sql_executed: Optional[str] = None  # The SQL that was run

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'success': self.success,
            'data': self.data,
            'columns': self.columns,
            'row_count': self.row_count,
            'execution_time_ms': self.execution_time_ms,
            'error_message': self.error_message,
            'truncated': self.truncated,
            'sql_executed': self.sql_executed
        }


@dataclass
class ConfidenceScore:
    """Detailed confidence scoring."""
    overall_score: float          # 0-100
    level: ConfidenceLevel

    # Component breakdown
    components: Dict[str, Any] = field(default_factory=dict)

    # Human-readable explanation
    explanation: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'score': self.overall_score,
            'level': self.level.value,
            'components': self.components,
            'explanation': self.explanation
        }


@dataclass
class QueryMethodology:
    """Documents exactly how a query was answered."""
    # Original query
    query: str = ""

    # Data source
    table_used: str = ""
    population_used: str = ""  # Population name
    population_filter: Optional[str] = None

    # Columns used
    columns_used: List[str] = field(default_factory=list)

    # Entity resolution
    entities_resolved: List[Dict[str, Any]] = field(default_factory=list)

    # SQL executed
    sql_executed: Optional[str] = None

    # Confidence
    confidence_score: float = 0.0
    confidence_level: str = "very_low"

    # Any assumptions
    assumptions: List[str] = field(default_factory=list)

    # Timestamp
    timestamp: str = ""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'query': self.query,
            'table_used': self.table_used,
            'population_used': self.population_used,
            'population_filter': self.population_filter,
            'columns_used': self.columns_used,
            'entities_resolved': self.entities_resolved,
            'sql_executed': self.sql_executed,
            'confidence_score': self.confidence_score,
            'confidence_level': self.confidence_level,
            'assumptions': self.assumptions,
            'timestamp': self.timestamp
        }

    def to_markdown(self) -> str:
        """Generate markdown explanation."""
        lines = [
            "---",
            "### Methodology",
            "",
            f"**Data Source:** {self.table_used}",
            "",
        ]

        lines.append(f"**Population:** {self.population_used}")

        if self.population_filter:
            lines.append(f"  - Filter: `{self.population_filter}`")

        if self.columns_used:
            lines.append("")
            lines.append("**Key Columns Used:**")
            for column in self.columns_used[:10]:
                lines.append(f"  - {column}")

        if self.entities_resolved:
            lines.append("")
            lines.append("**Term Resolution:**")
            for resolution in self.entities_resolved:
                lines.append(f"  - \"{resolution.get('original')}\" → {resolution.get('resolved')}")

        if self.assumptions:
            lines.append("")
            lines.append("**Assumptions Made:**")
            for a in self.assumptions:
                lines.append(f"  - {a}")

        lines.append("")
        lines.append(f"**Confidence:** {self.confidence_score:.0f}% ({self.confidence_level})")

        lines.append("---")
        return "\n".join(lines)


@dataclass
class PipelineResult:
    """Complete result of the inference pipeline."""
    # Query info
    success: bool
    query: str  # Original query

    # Results
    answer: str  # Formatted answer text
    data: Optional[List[Dict]] = None  # Result data as list of dicts
    row_count: int = 0

    # SQL
    sql: Optional[str] = None

    # Methodology (transparency)
    methodology: Optional[Dict] = None

    # Confidence
    confidence: Optional[Dict] = None

    # Warnings
    warnings: List[str] = field(default_factory=list)

    # Pipeline execution details
    pipeline_stages: Dict[str, Any] = field(default_factory=dict)
    total_time_ms: float = 0.0

    # Errors
    error: Optional[str] = None
    error_stage: Optional[str] = None

    # Additional metadata (for caching, instant responses, etc.)
    metadata: Dict[str, Any] = field(default_factory=dict)

    # Timestamp
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def format_response(self, include_sql: bool = True) -> str:
        """Format complete response for user."""
        lines = [
            "## Answer",
            "",
            self.answer,
            "",
        ]

        # Confidence indicator
        if self.confidence:
            score = self.confidence.get('score', 0)
            level = self.confidence.get('level', 'unknown')
            lines.append(f"**Confidence:** {score:.0f}% ({level})")
            lines.append("")

        # Methodology
        if self.methodology:
            lines.append("**Methodology:**")
            if isinstance(self.methodology, dict):
                lines.append(f"- Table: {self.methodology.get('table_used', 'N/A')}")
                lines.append(f"- Population: {self.methodology.get('population_used', 'N/A')}")

        # SQL (collapsible)
        if include_sql and self.sql:
            lines.append("")
            lines.append("<details>")
            lines.append("<summary>View SQL Query</summary>")
            lines.append("")
            lines.append("```sql")
            lines.append(self.sql)
            lines.append("```")
            lines.append("</details>")

        return "\n".join(lines)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            'success': self.success,
            'query': self.query,
            'answer': self.answer,
            'data': self.data,
            'row_count': self.row_count,
            'confidence': self.confidence,
            'methodology': self.methodology,
            'sql': self.sql,
            'total_time_ms': self.total_time_ms,
            'error': self.error,
            'error_stage': self.error_stage,
            'warnings': self.warnings,
            'pipeline_stages': self.pipeline_stages,
            'metadata': self.metadata,
            'timestamp': self.timestamp
        }
