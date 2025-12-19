# SAGE - Answer Verifier Module
# ==============================
"""
Answer Verifier
===============
Verifies that generated answers are accurate and sensible.

Key Principle: Don't just execute SQL - verify the answer makes sense
- SQL matches the intent
- Result is plausible
- Cross-reference checks pass
- Sanity bounds are respected
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime

from .query_analyzer import QueryAnalysis, QueryIntent, QuerySubject
from .models import ExecutionResult

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class VerificationCheck:
    """Result of a single verification check."""
    name: str
    passed: bool
    score: float  # 0.0 to 1.0
    weight: float  # How much this check matters (0.0 to 1.0)
    issue: Optional[str] = None
    details: Optional[str] = None


@dataclass
class VerificationResult:
    """Complete verification result."""
    checks: List[VerificationCheck]
    overall_score: float  # 0.0 to 1.0
    passed: bool
    issues: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'overall_score': self.overall_score,
            'passed': self.passed,
            'checks': [
                {
                    'name': c.name,
                    'passed': c.passed,
                    'score': c.score,
                    'issue': c.issue
                }
                for c in self.checks
            ],
            'issues': self.issues,
            'warnings': self.warnings,
            'timestamp': self.timestamp
        }


# =============================================================================
# ANSWER VERIFIER
# =============================================================================

class AnswerVerifier:
    """
    Verifies that generated answers are accurate and sensible.
    """

    # Verification thresholds
    PASS_THRESHOLD = 0.7  # Overall score needed to pass
    MAX_REASONABLE_COUNT = 100000  # Maximum reasonable count value
    MIN_REASONABLE_COUNT = 0  # Minimum (counts shouldn't be negative)

    def __init__(self, db_path: str = None):
        """
        Initialize answer verifier.

        Args:
            db_path: Path to DuckDB database for cross-reference checks
        """
        self.db_path = db_path
        self._total_subjects = None  # Cache

    def verify(
        self,
        query: str,
        analysis: QueryAnalysis,
        sql: str,
        result: ExecutionResult
    ) -> VerificationResult:
        """
        Multi-layer verification of the answer.

        Args:
            query: Original query
            analysis: Query analysis
            sql: Generated SQL
            result: Execution result

        Returns:
            VerificationResult with all checks
        """
        checks = []

        # 1. SQL matches intent (30% weight)
        checks.append(self._verify_sql_matches_intent(analysis, sql))

        # 2. SQL contains expected elements (20% weight)
        checks.append(self._verify_sql_structure(analysis, sql))

        # 3. Result is plausible (25% weight)
        checks.append(self._verify_result_plausible(analysis, result))

        # 4. Sanity bounds (15% weight)
        checks.append(self._verify_sanity_bounds(analysis, result))

        # 5. Data quality (10% weight)
        checks.append(self._verify_data_quality(result))

        # Calculate overall verification score
        total_weight = sum(c.weight for c in checks)
        if total_weight > 0:
            overall_score = sum(c.score * c.weight for c in checks) / total_weight
        else:
            overall_score = 0.0

        # Collect issues
        issues = [c.issue for c in checks if c.issue and not c.passed]
        warnings = [c.issue for c in checks if c.issue and c.passed and c.score < 1.0]

        passed = overall_score >= self.PASS_THRESHOLD

        logger.info(f"Verification complete: score={overall_score:.2f}, "
                   f"passed={passed}, issues={len(issues)}")

        return VerificationResult(
            checks=checks,
            overall_score=overall_score,
            passed=passed,
            issues=issues,
            warnings=warnings
        )

    def _verify_sql_matches_intent(
        self,
        analysis: QueryAnalysis,
        sql: str
    ) -> VerificationCheck:
        """
        Verify SQL structure matches query intent.
        """
        sql_upper = sql.upper()
        issues = []
        score = 1.0

        # COUNT intent should have COUNT() in SQL
        if analysis.intent in [QueryIntent.COUNT_SUBJECTS, QueryIntent.COUNT_EVENTS,
                               QueryIntent.COUNT_RECORDS]:
            if 'COUNT(' not in sql_upper:
                issues.append("Query asks for count but SQL doesn't use COUNT()")
                score = 0.0
            elif analysis.intent == QueryIntent.COUNT_SUBJECTS:
                # Should use COUNT(DISTINCT USUBJID) for unique subjects
                if 'DISTINCT' not in sql_upper:
                    issues.append("Counting subjects should use DISTINCT")
                    score = 0.7
                elif 'USUBJID' not in sql_upper:
                    issues.append("Counting subjects should reference USUBJID")
                    score = 0.5

        # LIST intent should have SELECT without aggregation (or proper grouping)
        if analysis.intent in [QueryIntent.LIST_VALUES, QueryIntent.LIST_RECORDS]:
            # Allow COUNT with GROUP BY for frequency lists
            if 'GROUP BY' not in sql_upper and 'COUNT(' in sql_upper:
                issues.append("List query has COUNT without GROUP BY")
                score = 0.6

        # TOP N should have ORDER BY and LIMIT
        if analysis.intent == QueryIntent.LIST_TOP_N:
            if 'ORDER BY' not in sql_upper:
                issues.append("Top N query should have ORDER BY")
                score = 0.7
            if 'LIMIT' not in sql_upper:
                issues.append("Top N query should have LIMIT")
                score = min(score, 0.8)

        # DISTRIBUTION should have GROUP BY
        if analysis.intent == QueryIntent.DISTRIBUTION:
            if 'GROUP BY' not in sql_upper:
                issues.append("Distribution query should have GROUP BY")
                score = 0.5

        # AVERAGE should use AVG()
        if analysis.intent == QueryIntent.AVERAGE:
            if 'AVG(' not in sql_upper:
                issues.append("Average query should use AVG()")
                score = 0.0

        issue = "; ".join(issues) if issues else None

        return VerificationCheck(
            name="intent_match",
            passed=score >= 0.7,
            score=score,
            weight=0.30,
            issue=issue
        )

    def _verify_sql_structure(
        self,
        analysis: QueryAnalysis,
        sql: str
    ) -> VerificationCheck:
        """
        Verify SQL has expected structural elements.
        """
        sql_upper = sql.upper()
        issues = []
        score = 1.0

        # Must have SELECT
        if 'SELECT' not in sql_upper:
            issues.append("SQL must have SELECT")
            score = 0.0

        # Must have FROM
        if 'FROM' not in sql_upper:
            issues.append("SQL must have FROM")
            score = 0.0

        # Check if expected table is referenced
        if analysis.suggested_table:
            if analysis.suggested_table.upper() not in sql_upper:
                issues.append(f"Expected table {analysis.suggested_table} not in SQL")
                score = min(score, 0.6)

        # Check conditions are applied
        for condition in analysis.conditions:
            if condition.column and condition.column.upper() not in sql_upper:
                issues.append(f"Condition column {condition.column} not in SQL")
                score = min(score, 0.7)

        # Check for population filter if suggested
        if analysis.suggested_population:
            pop_filters = {
                'Safety': 'SAFFL',
                'ITT': 'ITTFL',
                'Efficacy': 'EFFFL'
            }
            expected_filter = pop_filters.get(analysis.suggested_population)
            if expected_filter and expected_filter not in sql_upper:
                issues.append(f"Expected population filter {expected_filter} not in SQL")
                score = min(score, 0.8)

        issue = "; ".join(issues) if issues else None

        return VerificationCheck(
            name="sql_structure",
            passed=score >= 0.6,
            score=score,
            weight=0.20,
            issue=issue
        )

    def _verify_result_plausible(
        self,
        analysis: QueryAnalysis,
        result: ExecutionResult
    ) -> VerificationCheck:
        """
        Verify result values are plausible.
        """
        if not result.success:
            return VerificationCheck(
                name="plausibility",
                passed=False,
                score=0.0,
                weight=0.25,
                issue="Query execution failed"
            )

        issues = []
        score = 1.0

        # For count queries, verify the count is reasonable
        if analysis.intent in [QueryIntent.COUNT_SUBJECTS, QueryIntent.COUNT_EVENTS]:
            count = self._extract_count(result)

            if count is not None:
                # Check against total subjects if available
                total_subjects = self._get_total_subjects()
                if total_subjects and analysis.intent == QueryIntent.COUNT_SUBJECTS:
                    if count > total_subjects:
                        issues.append(
                            f"Result ({count}) exceeds total subjects ({total_subjects})"
                        )
                        score = 0.0

                # Check for negative counts
                if count < 0:
                    issues.append(f"Negative count ({count}) is impossible")
                    score = 0.0

                # Check for unreasonably high counts
                if count > self.MAX_REASONABLE_COUNT:
                    issues.append(f"Count ({count}) seems unreasonably high")
                    score = 0.3

        # For empty results, check if this is expected
        if result.row_count == 0:
            # Empty result might be valid, but add a warning
            issues.append("No results returned - verify query criteria")
            score = min(score, 0.8)

        issue = "; ".join(issues) if issues else None

        return VerificationCheck(
            name="plausibility",
            passed=score >= 0.5,
            score=score,
            weight=0.25,
            issue=issue
        )

    def _verify_sanity_bounds(
        self,
        analysis: QueryAnalysis,
        result: ExecutionResult
    ) -> VerificationCheck:
        """
        Verify results are within sanity bounds.
        """
        if not result.success or not result.data:
            return VerificationCheck(
                name="sanity_bounds",
                passed=True,
                score=1.0,
                weight=0.15
            )

        issues = []
        score = 1.0

        for row in result.data:
            for key, value in row.items():
                if value is None:
                    continue

                # Check numeric values
                if isinstance(value, (int, float)):
                    # Age should be 0-120
                    if 'age' in key.lower():
                        if value < 0 or value > 120:
                            issues.append(f"Age {value} outside reasonable range")
                            score = min(score, 0.5)

                    # Percentages should be 0-100 (or 0-1)
                    if 'pct' in key.lower() or 'percent' in key.lower():
                        if value < 0 or value > 100:
                            issues.append(f"Percentage {value} outside 0-100")
                            score = min(score, 0.7)

                    # Counts should not be negative
                    if 'count' in key.lower():
                        if value < 0:
                            issues.append(f"Negative count: {value}")
                            score = min(score, 0.3)

        issue = "; ".join(issues) if issues else None

        return VerificationCheck(
            name="sanity_bounds",
            passed=score >= 0.7,
            score=score,
            weight=0.15,
            issue=issue
        )

    def _verify_data_quality(self, result: ExecutionResult) -> VerificationCheck:
        """
        Verify data quality of results.
        """
        if not result.success:
            return VerificationCheck(
                name="data_quality",
                passed=False,
                score=0.0,
                weight=0.10,
                issue="Query execution failed"
            )

        issues = []
        score = 1.0

        # Check for excessive NULLs in results
        if result.data:
            total_cells = 0
            null_cells = 0
            for row in result.data:
                for value in row.values():
                    total_cells += 1
                    if value is None:
                        null_cells += 1

            if total_cells > 0:
                null_ratio = null_cells / total_cells
                if null_ratio > 0.5:
                    issues.append(f"High NULL ratio in results: {null_ratio:.0%}")
                    score = min(score, 0.6)
                elif null_ratio > 0.2:
                    issues.append(f"Some NULL values in results: {null_ratio:.0%}")
                    score = min(score, 0.8)

        # Check if result was truncated
        if result.truncated:
            issues.append("Results were truncated")
            score = min(score, 0.9)

        issue = "; ".join(issues) if issues else None

        return VerificationCheck(
            name="data_quality",
            passed=score >= 0.6,
            score=score,
            weight=0.10,
            issue=issue
        )

    def _extract_count(self, result: ExecutionResult) -> Optional[int]:
        """Extract count value from result."""
        if not result.data or len(result.data) == 0:
            return None

        row = result.data[0]
        for key, value in row.items():
            if 'count' in key.lower() or isinstance(value, (int, float)):
                try:
                    return int(value)
                except (TypeError, ValueError):
                    pass

        return None

    def _get_total_subjects(self) -> Optional[int]:
        """Get total number of subjects from database."""
        if self._total_subjects is not None:
            return self._total_subjects

        if not self.db_path:
            return None

        try:
            import duckdb
            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                # Try ADSL first
                try:
                    result = conn.execute(
                        "SELECT COUNT(DISTINCT USUBJID) FROM ADSL"
                    ).fetchone()
                    if result:
                        self._total_subjects = result[0]
                except Exception:
                    # Try DM if ADSL doesn't exist
                    try:
                        result = conn.execute(
                            "SELECT COUNT(DISTINCT USUBJID) FROM DM"
                        ).fetchone()
                        if result:
                            self._total_subjects = result[0]
                    except Exception:
                        pass
            finally:
                conn.close()
        except Exception as e:
            logger.warning(f"Could not get total subjects: {e}")

        return self._total_subjects


# =============================================================================
# QUICK VERIFICATION
# =============================================================================

def quick_verify(sql: str, result: ExecutionResult) -> bool:
    """
    Quick verification for simple checks.

    Returns True if result passes basic checks.
    """
    # Must have successful execution
    if not result.success:
        return False

    # Basic SQL structure
    sql_upper = sql.upper()
    if 'SELECT' not in sql_upper or 'FROM' not in sql_upper:
        return False

    # No dangerous patterns
    dangerous = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'ALTER', 'TRUNCATE']
    if any(d in sql_upper for d in dangerous):
        return False

    return True
