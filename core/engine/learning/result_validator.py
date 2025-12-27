"""
Result Validator - Sanity checks on query results.

Compares results against historical data and validates reasonableness.
"""

import json
import hashlib
import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from pathlib import Path
import uuid


@dataclass
class ResultValidation:
    """Result of result validation."""
    is_valid: bool
    confidence_adjustment: float  # -0.2 to +0.1
    checks: Dict[str, bool]
    warnings: List[str]
    historical_match: bool
    anomalies: List[str]


class ResultValidator:
    """Validate query results for sanity and historical consistency."""

    def __init__(self, db_path: str = "data/learning.db"):
        """
        Initialize Result Validator.

        Args:
            db_path: Path to database for historical results
        """
        self.db_path = Path(db_path)
        self._init_database()

    def _init_database(self):
        """Initialize historical results table."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS historical_results (
                    id TEXT PRIMARY KEY,
                    query_hash TEXT NOT NULL,
                    question TEXT NOT NULL,
                    sql TEXT NOT NULL,
                    result_hash TEXT,
                    result_summary TEXT,
                    row_count INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    data_version TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_historical_hash
                ON historical_results(query_hash)
            """)

    def validate(
        self,
        question: str,
        sql: str,
        result: Any,
        expected_type: str = "count"
    ) -> ResultValidation:
        """
        Validate query result.

        Args:
            question: Original question
            sql: SQL that was executed
            result: Query result
            expected_type: Expected result type (count, list, etc.)

        Returns:
            ResultValidation with checks and any issues
        """
        checks: Dict[str, bool] = {}
        warnings: List[str] = []
        anomalies: List[str] = []
        adjustment = 0.0

        # Convert result to analyzable format
        result_data, row_count = self._normalize_result(result)

        # Check 1: Not empty (unless expected)
        checks["not_empty"] = row_count > 0
        if row_count == 0:
            warnings.append("Query returned no results")
            adjustment -= 0.1

        # Check 2: Reasonable row count
        if row_count > 100000:
            checks["reasonable_size"] = False
            warnings.append(f"Very large result set: {row_count:,} rows")
            adjustment -= 0.05
        else:
            checks["reasonable_size"] = True

        # Check 3: Value bounds (for numeric results)
        bounds_check, bounds_warnings = self._check_bounds(result_data, expected_type)
        checks["value_bounds"] = bounds_check
        warnings.extend(bounds_warnings)
        if not bounds_check:
            adjustment -= 0.1

        # Check 4: Null ratio
        null_check, null_ratio = self._check_null_ratio(result_data)
        checks["acceptable_nulls"] = null_check
        if not null_check:
            warnings.append(f"High null ratio: {null_ratio:.1%}")
            adjustment -= 0.05

        # Check 5: Historical comparison
        query_hash = self._hash_query(question, sql)
        historical = self._get_historical(query_hash)

        if historical:
            hist_match, hist_warnings = self._compare_historical(
                result_data, row_count, historical
            )
            checks["historical_match"] = hist_match
            if not hist_match:
                anomalies.extend(hist_warnings)
                adjustment -= 0.1
        else:
            checks["historical_match"] = True  # No history to compare

        # Store result for future comparison
        self._store_result(question, sql, result_data, row_count)

        # Calculate final validation
        is_valid = all(checks.values())

        return ResultValidation(
            is_valid=is_valid,
            confidence_adjustment=max(-0.3, min(0.1, adjustment)),
            checks=checks,
            warnings=warnings,
            historical_match=checks.get("historical_match", True),
            anomalies=anomalies
        )

    def _normalize_result(self, result: Any) -> tuple:
        """Normalize result to list of dicts."""
        if result is None:
            return [], 0

        if hasattr(result, 'to_dict'):
            # DataFrame
            result_data = result.to_dict('records')
            row_count = len(result_data)
        elif isinstance(result, list):
            if result and isinstance(result[0], dict):
                result_data = result
            else:
                result_data = [{"value": r} for r in result]
            row_count = len(result)
        elif isinstance(result, (int, float)):
            result_data = [{"value": result}]
            row_count = 1
        elif isinstance(result, dict):
            result_data = [result]
            row_count = 1
        else:
            result_data = [{"value": str(result)}]
            row_count = 1

        return result_data, row_count

    def _check_bounds(
        self,
        result_data: List[Dict],
        expected_type: str
    ) -> tuple:
        """Check if numeric values are within reasonable bounds."""
        warnings = []

        if not result_data:
            return True, warnings

        for row in result_data:
            for key, value in row.items():
                if isinstance(value, (int, float)) and value is not None:
                    key_lower = key.lower()

                    # Percentage check
                    if "percent" in key_lower or "pct" in key_lower or "rate" in key_lower:
                        if value < 0 or value > 100:
                            if value > 100 and value <= 1:
                                # Might be decimal format (0.5 instead of 50%)
                                pass
                            else:
                                warnings.append(f"Invalid percentage: {key}={value}")
                                return False, warnings

                    # Count check
                    if "count" in key_lower or key_lower == "n" or key_lower == "cnt":
                        if value < 0:
                            warnings.append(f"Negative count: {key}={value}")
                            return False, warnings

                    # Age check
                    if "age" in key_lower:
                        if value < 0 or value > 150:
                            warnings.append(f"Invalid age: {key}={value}")
                            return False, warnings

                    # General negative check for counts
                    if expected_type == "count" and value < 0:
                        warnings.append(f"Negative value for count: {value}")
                        return False, warnings

        return True, warnings

    def _check_null_ratio(self, result_data: List[Dict]) -> tuple:
        """Check the ratio of null values."""
        if not result_data:
            return True, 0.0

        total_values = 0
        null_count = 0

        for row in result_data:
            for value in row.values():
                total_values += 1
                if value is None:
                    null_count += 1

        ratio = null_count / total_values if total_values > 0 else 0
        return ratio < 0.5, ratio

    def _hash_query(self, question: str, sql: str) -> str:
        """Create hash for query comparison."""
        normalized = f"{question.lower().strip()}|{sql.lower().strip()}"
        return hashlib.md5(normalized.encode()).hexdigest()

    def _get_historical(self, query_hash: str) -> Optional[Dict]:
        """Get historical result for comparison."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute("""
                    SELECT * FROM historical_results
                    WHERE query_hash = ?
                    ORDER BY created_at DESC
                    LIMIT 1
                """, (query_hash,))
                row = cursor.fetchone()
                return dict(row) if row else None
        except Exception:
            return None

    def _compare_historical(
        self,
        current_data: List[Dict],
        current_count: int,
        historical: Dict
    ) -> tuple:
        """Compare current result with historical."""
        warnings = []

        hist_count = historical.get("row_count", 0)

        # Check row count deviation
        if hist_count and hist_count > 0:
            deviation = abs(current_count - hist_count) / hist_count
            if deviation > 0.5:  # 50% deviation
                warnings.append(
                    f"Row count changed significantly: {hist_count} -> {current_count} ({deviation:.0%} change)"
                )
                return False, warnings

        return True, warnings

    def _store_result(
        self,
        question: str,
        sql: str,
        result_data: List[Dict],
        row_count: int
    ):
        """Store result for future comparison."""
        try:
            query_hash = self._hash_query(question, sql)
            result_summary = json.dumps({
                "row_count": row_count,
                "columns": list(result_data[0].keys()) if result_data else [],
                "sample": result_data[:3] if result_data else []
            })

            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    INSERT INTO historical_results
                    (id, query_hash, question, sql, result_summary, row_count)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (
                    str(uuid.uuid4()),
                    query_hash,
                    question,
                    sql,
                    result_summary,
                    row_count
                ))
        except Exception as e:
            # Don't fail on storage errors
            print(f"Warning: Could not store historical result: {e}")

    def clear_history(self):
        """Clear historical results (for testing)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM historical_results")
