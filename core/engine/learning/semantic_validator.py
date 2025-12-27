"""
Semantic Validator - Verify SQL matches user intent.

Checks that generated SQL aligns with the understood query intent.
"""

import re
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from enum import Enum

# Try to import sqlparse
try:
    import sqlparse
    SQLPARSE_AVAILABLE = True
except ImportError:
    SQLPARSE_AVAILABLE = False
    sqlparse = None


class ValidationResult(Enum):
    """Validation result status."""
    VALID = "VALID"
    WARNING = "WARNING"
    INVALID = "INVALID"


@dataclass
class SemanticValidation:
    """Result of semantic validation."""
    result: ValidationResult
    score: float  # 0-1
    checks: Dict[str, bool]
    issues: List[str]
    suggestions: List[str]


class SemanticValidator:
    """Validate SQL semantically matches query intent."""

    # Intent to SQL pattern mappings
    INTENT_PATTERNS = {
        "count": {
            "required": [r"COUNT\s*\("],
            "optional": [r"GROUP\s+BY"],
            "forbidden": []
        },
        "list": {
            "required": [r"SELECT"],
            "optional": [r"ORDER\s+BY", r"LIMIT"],
            "forbidden": []
        },
        "average": {
            "required": [r"AVG\s*\("],
            "optional": [r"GROUP\s+BY"],
            "forbidden": []
        },
        "sum": {
            "required": [r"SUM\s*\("],
            "optional": [r"GROUP\s+BY"],
            "forbidden": []
        },
        "maximum": {
            "required": [r"MAX\s*\("],
            "optional": [],
            "forbidden": []
        },
        "minimum": {
            "required": [r"MIN\s*\("],
            "optional": [],
            "forbidden": []
        },
        "compare": {
            "required": [r"SELECT"],
            "optional": [r"GROUP\s+BY", r"CASE\s+WHEN", r"JOIN"],
            "forbidden": []
        },
        "filter": {
            "required": [r"WHERE"],
            "optional": [],
            "forbidden": []
        }
    }

    def validate(
        self,
        sql: str,
        intent: str,
        expected_tables: Optional[List[str]] = None,
        expected_columns: Optional[List[str]] = None,
        filter_values: Optional[Dict[str, str]] = None
    ) -> SemanticValidation:
        """
        Validate SQL against understood intent.

        Args:
            sql: Generated SQL query
            intent: Understood intent (count, list, average, etc.)
            expected_tables: Tables expected in query
            expected_columns: Columns expected in query
            filter_values: Expected filter values

        Returns:
            SemanticValidation with result and issues
        """
        sql_upper = sql.upper()
        checks: Dict[str, bool] = {}
        issues: List[str] = []
        suggestions: List[str] = []

        # Check 1: Intent alignment
        intent_lower = intent.lower()
        intent_check = self._check_intent_patterns(sql_upper, intent_lower)
        checks["intent_alignment"] = intent_check["valid"]
        if not intent_check["valid"]:
            issues.extend(intent_check["issues"])
            suggestions.extend(intent_check["suggestions"])

        # Check 2: Required tables present
        if expected_tables:
            tables_check = all(
                any(t.upper() in sql_upper for t in [table, table.lower()])
                for table in expected_tables
            )
            checks["required_tables"] = tables_check
            if not tables_check:
                missing = [t for t in expected_tables if t.upper() not in sql_upper]
                issues.append(f"Missing tables: {missing}")

        # Check 3: Required columns present
        if expected_columns:
            columns_check = all(
                col.upper() in sql_upper or col.lower() in sql.lower()
                for col in expected_columns
            )
            checks["required_columns"] = columns_check
            if not columns_check:
                missing = [c for c in expected_columns if c.upper() not in sql_upper]
                issues.append(f"Missing columns: {missing}")

        # Check 4: Filter values preserved
        if filter_values:
            filters_check = all(
                str(value).upper() in sql_upper or str(value) in sql
                for value in filter_values.values()
            )
            checks["filter_values"] = filters_check
            if not filters_check:
                issues.append("Some filter values not found in SQL")

        # Check 5: Grouping consistency
        if self._needs_grouping(intent_lower):
            has_group = "GROUP BY" in sql_upper
            checks["grouping"] = has_group
            if not has_group and "count" in intent_lower and "by" in intent_lower:
                issues.append("Expected GROUP BY clause for aggregation")
                suggestions.append("Add GROUP BY clause for proper aggregation")

        # Check 6: No dangerous operations
        dangerous = self._check_dangerous_operations(sql_upper)
        checks["no_dangerous"] = not dangerous
        if dangerous:
            issues.append(f"Dangerous operations detected: {dangerous}")

        # Check 7: Basic SQL syntax
        checks["valid_syntax"] = self._check_basic_syntax(sql)
        if not checks["valid_syntax"]:
            issues.append("SQL may have syntax issues")

        # Calculate score
        check_count = len(checks)
        passed_count = sum(1 for v in checks.values() if v)
        score = passed_count / check_count if check_count > 0 else 0

        # Determine result
        if score >= 0.9:
            result = ValidationResult.VALID
        elif score >= 0.7:
            result = ValidationResult.WARNING
        else:
            result = ValidationResult.INVALID

        return SemanticValidation(
            result=result,
            score=score,
            checks=checks,
            issues=issues,
            suggestions=suggestions
        )

    def _check_intent_patterns(
        self,
        sql: str,
        intent: str
    ) -> Dict[str, Any]:
        """Check if SQL matches intent patterns."""
        result = {"valid": True, "issues": [], "suggestions": []}

        # Find matching intent patterns
        for intent_key, patterns in self.INTENT_PATTERNS.items():
            if intent_key in intent:
                # Check required patterns
                for pattern in patterns["required"]:
                    if not re.search(pattern, sql, re.IGNORECASE):
                        result["valid"] = False
                        result["issues"].append(
                            f"Missing required pattern for '{intent_key}': {pattern}"
                        )
                        result["suggestions"].append(
                            f"Add {intent_key.upper()} function for '{intent_key}' queries"
                        )

                # Check forbidden patterns
                for pattern in patterns["forbidden"]:
                    if re.search(pattern, sql, re.IGNORECASE):
                        result["valid"] = False
                        result["issues"].append(
                            f"Forbidden pattern for '{intent_key}': {pattern}"
                        )

        return result

    def _needs_grouping(self, intent: str) -> bool:
        """Check if intent typically needs GROUP BY."""
        grouping_intents = [
            "count by", "average by", "sum by", "per",
            "for each", "grouped", "breakdown", "distribution"
        ]
        return any(g in intent for g in grouping_intents)

    def _check_dangerous_operations(self, sql: str) -> List[str]:
        """Check for dangerous SQL operations."""
        dangerous = []
        dangerous_keywords = [
            "DELETE", "DROP", "UPDATE", "INSERT",
            "ALTER", "TRUNCATE", "GRANT", "REVOKE"
        ]

        for keyword in dangerous_keywords:
            if re.search(rf"\b{keyword}\b", sql):
                dangerous.append(keyword)

        return dangerous

    def _check_basic_syntax(self, sql: str) -> bool:
        """Check basic SQL syntax."""
        sql_upper = sql.upper().strip()

        # Must start with SELECT
        if not sql_upper.startswith("SELECT"):
            return False

        # Must have FROM
        if "FROM" not in sql_upper:
            return False

        # Check balanced parentheses
        if sql.count("(") != sql.count(")"):
            return False

        # Check balanced quotes
        single_quotes = sql.count("'")
        if single_quotes % 2 != 0:
            return False

        return True

    def quick_validate(self, sql: str) -> bool:
        """
        Quick validation check.

        Args:
            sql: SQL to validate

        Returns:
            True if basic validation passes
        """
        sql_upper = sql.upper().strip()

        # Must be SELECT
        if not sql_upper.startswith("SELECT"):
            return False

        # Must have FROM
        if "FROM" not in sql_upper:
            return False

        # No dangerous operations
        if self._check_dangerous_operations(sql_upper):
            return False

        return True
