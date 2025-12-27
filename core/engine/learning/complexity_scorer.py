"""
Complexity Scorer - Assess query difficulty.

Determines if a query is SIMPLE, MODERATE, COMPLEX, or VERY_COMPLEX.
Used to adjust confidence thresholds based on query difficulty.
"""

import re
from dataclasses import dataclass
from typing import List, Dict, Optional
from enum import Enum


class ComplexityLevel(Enum):
    """Query complexity levels."""
    SIMPLE = "SIMPLE"
    MODERATE = "MODERATE"
    COMPLEX = "COMPLEX"
    VERY_COMPLEX = "VERY_COMPLEX"


@dataclass
class ComplexityAssessment:
    """Result of complexity assessment."""
    level: ComplexityLevel
    score: float  # 0-100
    factors: Dict[str, float]
    recommended_threshold: float
    warnings: List[str]


class ComplexityScorer:
    """Assess query complexity to adjust confidence thresholds."""

    # Complexity indicators
    COMPLEX_KEYWORDS = [
        "compare", "difference", "change", "trend", "over time",
        "correlation", "relationship", "versus", "vs", "between",
        "ratio", "proportion", "relative", "adjusted"
    ]

    JOIN_INDICATORS = [
        "with", "and their", "along with", "including",
        "combined with", "matching", "related", "associated"
    ]

    AGGREGATION_KEYWORDS = [
        "average", "mean", "median", "sum", "total", "count",
        "maximum", "minimum", "percentage", "proportion", "rate",
        "distribution", "breakdown", "summary"
    ]

    TEMPORAL_KEYWORDS = [
        "first", "last", "before", "after", "during", "between",
        "prior to", "following", "baseline", "endpoint", "visit",
        "week", "day", "month", "time", "date", "period"
    ]

    CONDITIONAL_KEYWORDS = [
        "if", "when", "where", "only", "excluding", "except",
        "unless", "provided", "given that", "assuming"
    ]

    def assess(
        self,
        question: str,
        detected_tables: Optional[List[str]] = None,
        detected_columns: Optional[List[str]] = None
    ) -> ComplexityAssessment:
        """
        Assess the complexity of a query.

        Args:
            question: Natural language question
            detected_tables: Tables detected in query
            detected_columns: Columns detected in query

        Returns:
            ComplexityAssessment with level, score, and factors
        """
        question_lower = question.lower()
        factors: Dict[str, float] = {}
        warnings: List[str] = []

        # Factor 1: Question length (0-15 points)
        word_count = len(question.split())
        length_score = min(word_count / 2, 15)
        factors["question_length"] = length_score

        # Factor 2: Number of tables (0-25 points)
        table_count = len(detected_tables or [])
        table_score = table_count * 10 if table_count > 1 else 0
        factors["table_count"] = min(table_score, 25)
        if table_count > 2:
            warnings.append(f"Query involves {table_count} tables - complex join required")

        # Factor 3: Complex keywords (0-20 points)
        complex_count = sum(1 for kw in self.COMPLEX_KEYWORDS if kw in question_lower)
        factors["complex_keywords"] = min(complex_count * 5, 20)

        # Factor 4: Join indicators (0-15 points)
        join_count = sum(1 for kw in self.JOIN_INDICATORS if kw in question_lower)
        factors["join_indicators"] = min(join_count * 5, 15)

        # Factor 5: Aggregation complexity (0-10 points)
        agg_count = sum(1 for kw in self.AGGREGATION_KEYWORDS if kw in question_lower)
        factors["aggregations"] = min(agg_count * 3, 10)
        if agg_count > 2:
            warnings.append("Multiple aggregations may require subqueries")

        # Factor 6: Temporal complexity (0-15 points)
        temporal_count = sum(1 for kw in self.TEMPORAL_KEYWORDS if kw in question_lower)
        factors["temporal"] = min(temporal_count * 5, 15)
        if temporal_count > 2:
            warnings.append("Multiple temporal conditions detected")

        # Factor 7: Subquery indicators (0-20 points)
        subquery_patterns = [
            r"for each", r"per ", r"by .+ by", r"grouped by",
            r"within", r"among", r"excluding", r"only those",
            r"having", r"top \d+", r"first \d+", r"last \d+"
        ]
        subquery_score = sum(5 for p in subquery_patterns if re.search(p, question_lower))
        factors["subquery_indicators"] = min(subquery_score, 20)

        # Factor 8: Conditional complexity (0-10 points)
        conditional_count = sum(1 for kw in self.CONDITIONAL_KEYWORDS if kw in question_lower)
        factors["conditional"] = min(conditional_count * 3, 10)

        # Factor 9: Column count (0-10 points)
        column_count = len(detected_columns or [])
        if column_count > 5:
            factors["column_count"] = min((column_count - 5) * 2, 10)
        else:
            factors["column_count"] = 0

        # Calculate total score
        total_score = sum(factors.values())

        # Determine level and threshold
        if total_score < 15:
            level = ComplexityLevel.SIMPLE
            threshold = 0.75
        elif total_score < 35:
            level = ComplexityLevel.MODERATE
            threshold = 0.80
        elif total_score < 55:
            level = ComplexityLevel.COMPLEX
            threshold = 0.85
            if not warnings:
                warnings.append("Complex query - may require verification")
        else:
            level = ComplexityLevel.VERY_COMPLEX
            threshold = 0.90
            warnings.append("Very complex query - recommend expert review")

        return ComplexityAssessment(
            level=level,
            score=total_score,
            factors=factors,
            recommended_threshold=threshold,
            warnings=warnings
        )

    def get_threshold_adjustment(self, level: ComplexityLevel) -> float:
        """
        Get confidence threshold adjustment for complexity level.

        Args:
            level: Complexity level

        Returns:
            Adjustment to apply to confidence threshold
        """
        adjustments = {
            ComplexityLevel.SIMPLE: 0.0,
            ComplexityLevel.MODERATE: 0.05,
            ComplexityLevel.COMPLEX: 0.10,
            ComplexityLevel.VERY_COMPLEX: 0.15
        }
        return adjustments.get(level, 0.0)

    def is_simple_count_query(self, question: str) -> bool:
        """
        Check if query is a simple count query.

        Args:
            question: Question to check

        Returns:
            True if simple count query
        """
        question_lower = question.lower()

        # Simple patterns
        simple_patterns = [
            r"^how many",
            r"^count of",
            r"^total number of",
            r"^number of"
        ]

        for pattern in simple_patterns:
            if re.match(pattern, question_lower):
                # Check for complexity indicators
                complex_found = any(
                    kw in question_lower
                    for kw in self.COMPLEX_KEYWORDS + self.TEMPORAL_KEYWORDS
                )
                if not complex_found:
                    return True

        return False

    def get_complexity_factors(self) -> Dict[str, List[str]]:
        """
        Get all complexity factor keywords.

        Returns:
            Dictionary of factor names to keyword lists
        """
        return {
            "complex_keywords": self.COMPLEX_KEYWORDS,
            "join_indicators": self.JOIN_INDICATORS,
            "aggregation_keywords": self.AGGREGATION_KEYWORDS,
            "temporal_keywords": self.TEMPORAL_KEYWORDS,
            "conditional_keywords": self.CONDITIONAL_KEYWORDS
        }
