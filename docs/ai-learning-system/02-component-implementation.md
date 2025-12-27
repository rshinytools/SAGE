# AI Chat Learning System - Phase 2: Component Implementation

## Overview

This document contains the detailed code specifications for each new component in the learning system.

---

## Component 1: Example Store

**File:** `core/engine/learning/example_store.py`

```python
"""
Example Store - ChromaDB integration for query-SQL pairs.

Stores verified query examples with vector embeddings for semantic search.
"""

import uuid
import json
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path

import chromadb
from chromadb.config import Settings


@dataclass
class LearningExample:
    """A learning example with query and SQL."""
    id: str
    question: str
    normalized_question: str
    sql: str
    intent: str
    tables_used: List[str]
    columns_used: List[str]
    complexity: str
    category: str
    source: str
    verified: bool
    created_by: str
    created_at: datetime
    usage_count: int
    success_count: int
    status: str


class ExampleStore:
    """Vector store for query-SQL learning examples."""

    def __init__(
        self,
        db_path: str = "data/learning.db",
        chroma_path: str = "knowledge/chroma",
        collection_name: str = "query_examples"
    ):
        self.db_path = Path(db_path)
        self.chroma_path = Path(chroma_path)
        self.collection_name = collection_name

        # Initialize SQLite
        self._init_database()

        # Initialize ChromaDB
        self.chroma_client = chromadb.PersistentClient(
            path=str(self.chroma_path),
            settings=Settings(anonymized_telemetry=False)
        )
        self.collection = self.chroma_client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "Query examples for few-shot learning"}
        )

    def _init_database(self):
        """Initialize SQLite tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS learning_examples (
                    id TEXT PRIMARY KEY,
                    question TEXT NOT NULL,
                    normalized_question TEXT,
                    sql TEXT NOT NULL,
                    intent TEXT,
                    tables_used TEXT,
                    columns_used TEXT,
                    complexity TEXT,
                    category TEXT,
                    source TEXT NOT NULL,
                    verified INTEGER DEFAULT 0,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usage_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    last_used_at TIMESTAMP,
                    status TEXT DEFAULT 'active'
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_examples_status
                ON learning_examples(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_examples_category
                ON learning_examples(category)
            """)

    def add_example(
        self,
        question: str,
        sql: str,
        intent: str = "DATA",
        tables_used: List[str] = None,
        columns_used: List[str] = None,
        complexity: str = "MODERATE",
        category: str = "general",
        source: str = "manual",
        verified: bool = False,
        created_by: str = "system"
    ) -> str:
        """Add a new learning example."""
        example_id = str(uuid.uuid4())
        normalized = self._normalize_question(question)

        # Store in SQLite
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO learning_examples
                (id, question, normalized_question, sql, intent,
                 tables_used, columns_used, complexity, category,
                 source, verified, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                example_id,
                question,
                normalized,
                sql,
                intent,
                json.dumps(tables_used or []),
                json.dumps(columns_used or []),
                complexity,
                category,
                source,
                1 if verified else 0,
                created_by
            ))

        # Store embedding in ChromaDB
        self.collection.add(
            documents=[normalized],
            ids=[example_id],
            metadatas=[{
                "question": question,
                "sql": sql,
                "intent": intent,
                "category": category,
                "complexity": complexity,
                "verified": str(verified)
            }]
        )

        return example_id

    def find_similar(
        self,
        question: str,
        n_results: int = 5,
        min_similarity: float = 0.7,
        verified_only: bool = True
    ) -> List[Dict[str, Any]]:
        """Find semantically similar examples."""
        normalized = self._normalize_question(question)

        # Query ChromaDB
        where_filter = {"verified": "True"} if verified_only else None

        results = self.collection.query(
            query_texts=[normalized],
            n_results=n_results,
            where=where_filter,
            include=["documents", "metadatas", "distances"]
        )

        examples = []
        if results["ids"] and results["ids"][0]:
            for i, example_id in enumerate(results["ids"][0]):
                # Convert distance to similarity (ChromaDB uses L2 distance)
                distance = results["distances"][0][i] if results["distances"] else 0
                similarity = 1 / (1 + distance)

                if similarity >= min_similarity:
                    metadata = results["metadatas"][0][i]
                    examples.append({
                        "id": example_id,
                        "question": metadata.get("question", ""),
                        "sql": metadata.get("sql", ""),
                        "intent": metadata.get("intent", ""),
                        "category": metadata.get("category", ""),
                        "complexity": metadata.get("complexity", ""),
                        "similarity": similarity
                    })

        return sorted(examples, key=lambda x: x["similarity"], reverse=True)

    def get_exact_match(self, question: str) -> Optional[Dict[str, Any]]:
        """Check for exact or near-exact match."""
        normalized = self._normalize_question(question)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM learning_examples
                WHERE normalized_question = ? AND status = 'active'
                LIMIT 1
            """, (normalized,))
            row = cursor.fetchone()

            if row:
                return dict(row)

        return None

    def update_usage_stats(
        self,
        example_id: str,
        success: bool = True
    ):
        """Update usage statistics for an example."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE learning_examples
                SET usage_count = usage_count + 1,
                    success_count = success_count + ?,
                    last_used_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (1 if success else 0, example_id))

    def verify_example(self, example_id: str, verified_by: str):
        """Mark an example as verified."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE learning_examples
                SET verified = 1
                WHERE id = ?
            """, (example_id,))

        # Update ChromaDB metadata
        self.collection.update(
            ids=[example_id],
            metadatas=[{"verified": "True"}]
        )

    def get_statistics(self) -> Dict[str, Any]:
        """Get learning store statistics."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            total = conn.execute(
                "SELECT COUNT(*) as count FROM learning_examples WHERE status = 'active'"
            ).fetchone()["count"]

            verified = conn.execute(
                "SELECT COUNT(*) as count FROM learning_examples WHERE verified = 1 AND status = 'active'"
            ).fetchone()["count"]

            by_category = conn.execute("""
                SELECT category, COUNT(*) as count
                FROM learning_examples
                WHERE status = 'active'
                GROUP BY category
            """).fetchall()

            by_source = conn.execute("""
                SELECT source, COUNT(*) as count
                FROM learning_examples
                WHERE status = 'active'
                GROUP BY source
            """).fetchall()

            return {
                "total_examples": total,
                "verified_examples": verified,
                "unverified_examples": total - verified,
                "by_category": {row["category"]: row["count"] for row in by_category},
                "by_source": {row["source"]: row["count"] for row in by_source}
            }

    def _normalize_question(self, question: str) -> str:
        """Normalize question for comparison."""
        return question.lower().strip()
```

---

## Component 2: Complexity Scorer

**File:** `core/engine/learning/complexity_scorer.py`

```python
"""
Complexity Scorer - Assess query difficulty.

Determines if a query is SIMPLE, MODERATE, COMPLEX, or VERY_COMPLEX.
"""

import re
from dataclasses import dataclass
from typing import List, Dict, Any
from enum import Enum


class ComplexityLevel(Enum):
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
        "correlation", "relationship", "versus", "vs", "between"
    ]

    JOIN_INDICATORS = [
        "with", "and their", "along with", "including",
        "combined with", "matching", "related"
    ]

    AGGREGATION_KEYWORDS = [
        "average", "mean", "median", "sum", "total", "count",
        "maximum", "minimum", "percentage", "proportion", "rate"
    ]

    TEMPORAL_KEYWORDS = [
        "first", "last", "before", "after", "during", "between",
        "prior to", "following", "baseline", "endpoint", "visit"
    ]

    def assess(
        self,
        question: str,
        detected_tables: List[str] = None,
        detected_columns: List[str] = None
    ) -> ComplexityAssessment:
        """Assess the complexity of a query."""
        question_lower = question.lower()
        factors = {}
        warnings = []

        # Factor 1: Question length (0-15 points)
        word_count = len(question.split())
        length_score = min(word_count / 2, 15)
        factors["question_length"] = length_score

        # Factor 2: Number of tables (0-25 points)
        table_count = len(detected_tables or [])
        table_score = table_count * 10 if table_count > 1 else 0
        factors["table_count"] = min(table_score, 25)
        if table_count > 2:
            warnings.append(f"Query involves {table_count} tables - complex join")

        # Factor 3: Complex keywords (0-20 points)
        complex_count = sum(1 for kw in self.COMPLEX_KEYWORDS if kw in question_lower)
        factors["complex_keywords"] = complex_count * 5

        # Factor 4: Join indicators (0-15 points)
        join_count = sum(1 for kw in self.JOIN_INDICATORS if kw in question_lower)
        factors["join_indicators"] = join_count * 5

        # Factor 5: Aggregation complexity (0-10 points)
        agg_count = sum(1 for kw in self.AGGREGATION_KEYWORDS if kw in question_lower)
        factors["aggregations"] = min(agg_count * 3, 10)

        # Factor 6: Temporal complexity (0-15 points)
        temporal_count = sum(1 for kw in self.TEMPORAL_KEYWORDS if kw in question_lower)
        factors["temporal"] = temporal_count * 5
        if temporal_count > 1:
            warnings.append("Multiple temporal conditions detected")

        # Factor 7: Subquery indicators (0-20 points)
        subquery_patterns = [
            r"for each", r"per ", r"by .+ by", r"grouped by",
            r"within", r"among", r"excluding", r"only those"
        ]
        subquery_score = sum(5 for p in subquery_patterns if re.search(p, question_lower))
        factors["subquery_indicators"] = min(subquery_score, 20)

        # Calculate total score
        total_score = sum(factors.values())

        # Determine level
        if total_score < 15:
            level = ComplexityLevel.SIMPLE
            threshold = 0.75
        elif total_score < 35:
            level = ComplexityLevel.MODERATE
            threshold = 0.80
        elif total_score < 55:
            level = ComplexityLevel.COMPLEX
            threshold = 0.85
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
        """Get confidence threshold adjustment for complexity level."""
        adjustments = {
            ComplexityLevel.SIMPLE: 0.0,
            ComplexityLevel.MODERATE: 0.05,
            ComplexityLevel.COMPLEX: 0.10,
            ComplexityLevel.VERY_COMPLEX: 0.15
        }
        return adjustments.get(level, 0.0)
```

---

## Component 3: Semantic Validator

**File:** `core/engine/learning/semantic_validator.py`

```python
"""
Semantic Validator - Verify SQL matches user intent.

Checks that generated SQL aligns with the understood query intent.
"""

import re
import sqlparse
from dataclasses import dataclass
from typing import List, Dict, Any, Optional
from enum import Enum


class ValidationResult(Enum):
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
            "required": [r"COUNT\s*\(", r"SELECT"],
            "optional": [r"GROUP\s+BY"],
            "forbidden": []
        },
        "list": {
            "required": [r"SELECT"],
            "optional": [r"ORDER\s+BY", r"LIMIT"],
            "forbidden": [r"COUNT\s*\(.*\)\s*$"]  # Only COUNT without other columns
        },
        "average": {
            "required": [r"AVG\s*\(", r"SELECT"],
            "optional": [r"GROUP\s+BY"],
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
        expected_tables: List[str] = None,
        expected_columns: List[str] = None,
        filter_values: Dict[str, str] = None
    ) -> SemanticValidation:
        """Validate SQL against understood intent."""
        sql_upper = sql.upper()
        checks = {}
        issues = []
        suggestions = []

        # Check 1: Intent alignment
        intent_lower = intent.lower()
        intent_check = self._check_intent_patterns(sql_upper, intent_lower)
        checks["intent_alignment"] = intent_check["valid"]
        if not intent_check["valid"]:
            issues.extend(intent_check["issues"])
            suggestions.extend(intent_check["suggestions"])

        # Check 2: Required tables present
        if expected_tables:
            tables_in_sql = self._extract_tables(sql)
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
                            f"Missing required pattern for {intent_key}: {pattern}"
                        )

                # Check forbidden patterns
                for pattern in patterns["forbidden"]:
                    if re.search(pattern, sql, re.IGNORECASE):
                        result["valid"] = False
                        result["issues"].append(
                            f"Forbidden pattern for {intent_key}: {pattern}"
                        )

        return result

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        parsed = sqlparse.parse(sql)[0]
        tables = []

        from_seen = False
        for token in parsed.tokens:
            if token.ttype is None:
                if from_seen:
                    # Extract table name
                    tables.append(str(token).strip())
                    from_seen = False
            if token.ttype is sqlparse.tokens.Keyword and token.value.upper() == "FROM":
                from_seen = True

        return tables

    def _needs_grouping(self, intent: str) -> bool:
        """Check if intent typically needs GROUP BY."""
        grouping_intents = [
            "count by", "average by", "sum by", "per",
            "for each", "grouped", "breakdown"
        ]
        return any(g in intent for g in grouping_intents)

    def _check_dangerous_operations(self, sql: str) -> List[str]:
        """Check for dangerous SQL operations."""
        dangerous = []
        dangerous_keywords = ["DELETE", "DROP", "UPDATE", "INSERT", "ALTER", "TRUNCATE"]

        for keyword in dangerous_keywords:
            if re.search(rf"\b{keyword}\b", sql):
                dangerous.append(keyword)

        return dangerous
```

---

## Component 4: Result Validator

**File:** `core/engine/learning/result_validator.py`

```python
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
        self.db_path = Path(db_path)
        self._init_database()

    def _init_database(self):
        """Initialize historical results table."""
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
        """Validate query result."""
        checks = {}
        warnings = []
        anomalies = []
        adjustment = 0.0

        # Convert result to analyzable format
        if hasattr(result, 'to_dict'):
            result_data = result.to_dict('records')
            row_count = len(result_data)
        elif isinstance(result, list):
            result_data = result
            row_count = len(result)
        elif isinstance(result, (int, float)):
            result_data = [{"value": result}]
            row_count = 1
        else:
            result_data = [{"value": str(result)}]
            row_count = 1

        # Check 1: Not empty (unless expected)
        checks["not_empty"] = row_count > 0
        if row_count == 0:
            warnings.append("Query returned no results")
            adjustment -= 0.1

        # Check 2: Reasonable row count
        if row_count > 100000:
            checks["reasonable_size"] = False
            warnings.append(f"Very large result set: {row_count} rows")
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
            confidence_adjustment=adjustment,
            checks=checks,
            warnings=warnings,
            historical_match=checks.get("historical_match", True),
            anomalies=anomalies
        )

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
                if isinstance(value, (int, float)):
                    # Percentage check
                    if "percent" in key.lower() or "rate" in key.lower():
                        if value < 0 or value > 100:
                            warnings.append(f"Invalid percentage: {key}={value}")
                            return False, warnings

                    # Count check
                    if "count" in key.lower() or "n" == key.lower():
                        if value < 0:
                            warnings.append(f"Negative count: {key}={value}")
                            return False, warnings

                    # Age check
                    if "age" in key.lower():
                        if value < 0 or value > 150:
                            warnings.append(f"Invalid age: {key}={value}")
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
        if hist_count > 0:
            deviation = abs(current_count - hist_count) / hist_count
            if deviation > 0.5:  # 50% deviation
                warnings.append(
                    f"Row count changed significantly: {hist_count} → {current_count}"
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
        import uuid

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
```

---

## Component 5: Confidence Manager

**File:** `core/engine/learning/confidence_manager.py`

```python
"""
Confidence Manager - Calculate final confidence and determine response action.

Aggregates all confidence signals to determine how to respond to user.
"""

from dataclasses import dataclass
from typing import Dict, List, Any, Optional
from enum import Enum


class ResponseAction(Enum):
    RETURN_NORMAL = "RETURN_NORMAL"              # 90%+ confidence
    RETURN_WITH_WARNING = "RETURN_WITH_WARNING"  # 75-89%
    RETURN_WITH_VERIFICATION = "RETURN_WITH_VERIFICATION"  # 60-74%
    ASK_CLARIFICATION = "ASK_CLARIFICATION"      # 40-59%
    REFUSE = "REFUSE"                            # <40%


@dataclass
class ConfidenceResult:
    """Final confidence calculation result."""
    score: float  # 0-100
    action: ResponseAction
    components: Dict[str, float]
    warnings: List[str]
    explanation: str


class ConfidenceManager:
    """Calculate final confidence from all signals."""

    # Component weights (total = 1.0)
    WEIGHTS = {
        "example_similarity": 0.20,
        "dictionary_match": 0.15,
        "metadata_coverage": 0.15,
        "semantic_alignment": 0.15,
        "complexity_match": 0.10,
        "execution_success": 0.10,
        "result_validation": 0.10,
        "result_sanity": 0.05
    }

    # Action thresholds
    THRESHOLDS = {
        ResponseAction.RETURN_NORMAL: 0.90,
        ResponseAction.RETURN_WITH_WARNING: 0.75,
        ResponseAction.RETURN_WITH_VERIFICATION: 0.60,
        ResponseAction.ASK_CLARIFICATION: 0.40,
        ResponseAction.REFUSE: 0.0
    }

    def calculate(
        self,
        example_similarity: float = 0.0,
        dictionary_match: float = 0.0,
        metadata_coverage: float = 0.0,
        semantic_alignment: float = 0.0,
        complexity_match: float = 1.0,
        execution_success: float = 0.0,
        result_validation: float = 1.0,
        result_sanity: float = 1.0,
        complexity_adjustment: float = 0.0,
        result_adjustment: float = 0.0
    ) -> ConfidenceResult:
        """Calculate final confidence score."""

        # Build components dict
        components = {
            "example_similarity": example_similarity,
            "dictionary_match": dictionary_match,
            "metadata_coverage": metadata_coverage,
            "semantic_alignment": semantic_alignment,
            "complexity_match": complexity_match,
            "execution_success": execution_success,
            "result_validation": result_validation,
            "result_sanity": result_sanity
        }

        # Calculate weighted score
        raw_score = sum(
            components[k] * self.WEIGHTS[k]
            for k in self.WEIGHTS
        )

        # Apply adjustments
        adjusted_score = raw_score + complexity_adjustment + result_adjustment

        # Clamp to 0-1
        final_score = max(0.0, min(1.0, adjusted_score))

        # Convert to percentage
        score_percent = final_score * 100

        # Determine action
        action = self._determine_action(final_score)

        # Generate warnings
        warnings = self._generate_warnings(components, final_score)

        # Generate explanation
        explanation = self._generate_explanation(components, action)

        return ConfidenceResult(
            score=score_percent,
            action=action,
            components={k: v * 100 for k, v in components.items()},
            warnings=warnings,
            explanation=explanation
        )

    def _determine_action(self, score: float) -> ResponseAction:
        """Determine response action based on score."""
        if score >= self.THRESHOLDS[ResponseAction.RETURN_NORMAL]:
            return ResponseAction.RETURN_NORMAL
        elif score >= self.THRESHOLDS[ResponseAction.RETURN_WITH_WARNING]:
            return ResponseAction.RETURN_WITH_WARNING
        elif score >= self.THRESHOLDS[ResponseAction.RETURN_WITH_VERIFICATION]:
            return ResponseAction.RETURN_WITH_VERIFICATION
        elif score >= self.THRESHOLDS[ResponseAction.ASK_CLARIFICATION]:
            return ResponseAction.ASK_CLARIFICATION
        else:
            return ResponseAction.REFUSE

    def _generate_warnings(
        self,
        components: Dict[str, float],
        score: float
    ) -> List[str]:
        """Generate warnings based on component scores."""
        warnings = []

        if components["example_similarity"] < 0.5:
            warnings.append("No similar examples found in training data")

        if components["semantic_alignment"] < 0.7:
            warnings.append("SQL may not fully match query intent")

        if components["result_validation"] < 0.8:
            warnings.append("Result differs from historical patterns")

        if components["metadata_coverage"] < 0.5:
            warnings.append("Limited metadata available for variables")

        if score < 0.6:
            warnings.append("Low confidence - please verify results")

        return warnings

    def _generate_explanation(
        self,
        components: Dict[str, float],
        action: ResponseAction
    ) -> str:
        """Generate explanation for confidence level."""

        if action == ResponseAction.RETURN_NORMAL:
            return "High confidence answer based on similar verified examples."

        elif action == ResponseAction.RETURN_WITH_WARNING:
            weak = [k for k, v in components.items() if v < 0.7]
            if weak:
                return f"Moderate confidence. Verify: {', '.join(weak)}"
            return "Moderate confidence. Please verify assumptions."

        elif action == ResponseAction.RETURN_WITH_VERIFICATION:
            return "Lower confidence. Result provided but requires verification."

        elif action == ResponseAction.ASK_CLARIFICATION:
            return "Insufficient confidence. Clarification needed."

        else:  # REFUSE
            return "Cannot provide reliable answer for this query."
```

---

## Component 6: Feedback Handler

**File:** `core/engine/learning/feedback_handler.py`

```python
"""
Feedback Handler - Process user feedback and update learning store.
"""

import uuid
import sqlite3
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, Dict, Any
from pathlib import Path
from enum import Enum

from .example_store import ExampleStore


class FeedbackType(Enum):
    CORRECT = "CORRECT"
    INCORRECT = "INCORRECT"
    CORRECTED = "CORRECTED"


@dataclass
class FeedbackResult:
    """Result of processing feedback."""
    success: bool
    feedback_id: str
    action_taken: str
    example_created: bool
    example_id: Optional[str]


class FeedbackHandler:
    """Process user feedback and update learning store."""

    def __init__(
        self,
        db_path: str = "data/learning.db",
        example_store: Optional[ExampleStore] = None
    ):
        self.db_path = Path(db_path)
        self.example_store = example_store or ExampleStore()
        self._init_database()

    def _init_database(self):
        """Initialize feedback table."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_feedback (
                    id TEXT PRIMARY KEY,
                    query_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    generated_sql TEXT,
                    feedback_type TEXT NOT NULL,
                    corrected_sql TEXT,
                    user_id TEXT NOT NULL,
                    session_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed INTEGER DEFAULT 0,
                    processed_at TIMESTAMP
                )
            """)

    def submit_feedback(
        self,
        query_id: str,
        question: str,
        generated_sql: str,
        feedback_type: FeedbackType,
        corrected_sql: Optional[str] = None,
        user_id: str = "anonymous",
        session_id: Optional[str] = None
    ) -> FeedbackResult:
        """Submit feedback for a query."""
        feedback_id = str(uuid.uuid4())

        # Store feedback
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO query_feedback
                (id, query_id, question, generated_sql, feedback_type,
                 corrected_sql, user_id, session_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                feedback_id,
                query_id,
                question,
                generated_sql,
                feedback_type.value,
                corrected_sql,
                user_id,
                session_id
            ))

        # Process feedback immediately
        return self._process_feedback(
            feedback_id, question, generated_sql,
            feedback_type, corrected_sql, user_id
        )

    def _process_feedback(
        self,
        feedback_id: str,
        question: str,
        generated_sql: str,
        feedback_type: FeedbackType,
        corrected_sql: Optional[str],
        user_id: str
    ) -> FeedbackResult:
        """Process feedback and update learning store."""
        example_created = False
        example_id = None
        action_taken = ""

        if feedback_type == FeedbackType.CORRECT:
            # Add as verified example
            example_id = self.example_store.add_example(
                question=question,
                sql=generated_sql,
                source="feedback",
                verified=True,
                created_by=user_id
            )
            example_created = True
            action_taken = "Added as verified example"

        elif feedback_type == FeedbackType.CORRECTED and corrected_sql:
            # Add corrected version as verified example
            example_id = self.example_store.add_example(
                question=question,
                sql=corrected_sql,
                source="correction",
                verified=True,
                created_by=user_id
            )
            example_created = True
            action_taken = "Added corrected SQL as verified example"

            # Also store the incorrect version to avoid
            self._store_negative_example(question, generated_sql)

        elif feedback_type == FeedbackType.INCORRECT:
            # Store negative example
            self._store_negative_example(question, generated_sql)
            action_taken = "Recorded as incorrect example"

        # Mark feedback as processed
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE query_feedback
                SET processed = 1, processed_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (feedback_id,))

        return FeedbackResult(
            success=True,
            feedback_id=feedback_id,
            action_taken=action_taken,
            example_created=example_created,
            example_id=example_id
        )

    def _store_negative_example(self, question: str, sql: str):
        """Store incorrect example to avoid in future."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS negative_examples (
                    id TEXT PRIMARY KEY,
                    question TEXT NOT NULL,
                    incorrect_sql TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                INSERT INTO negative_examples (id, question, incorrect_sql)
                VALUES (?, ?, ?)
            """, (str(uuid.uuid4()), question, sql))

    def get_pending_feedback(self, limit: int = 50) -> list:
        """Get unprocessed feedback."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM query_feedback
                WHERE processed = 0
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_feedback_stats(self) -> Dict[str, Any]:
        """Get feedback statistics."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            total = conn.execute(
                "SELECT COUNT(*) as count FROM query_feedback"
            ).fetchone()["count"]

            by_type = conn.execute("""
                SELECT feedback_type, COUNT(*) as count
                FROM query_feedback
                GROUP BY feedback_type
            """).fetchall()

            return {
                "total": total,
                "by_type": {row["feedback_type"]: row["count"] for row in by_type}
            }
```

---

## Next Document

See **[Phase 3: Implementation Checklist](./03-implementation-checklist.md)** for step-by-step implementation guide.
