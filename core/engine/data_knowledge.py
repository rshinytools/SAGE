# SAGE - Data-Driven Knowledge Module
# ====================================
"""
Data-Driven Knowledge
=====================
Build knowledge from actual data, not hardcoded rules.

Key Capabilities:
- Learn column value distributions
- Discover common patterns in data
- Suggest corrections based on actual values
- Generate query templates from schema
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any, Tuple
from datetime import datetime
import re

from .sql_security import validate_table_name, validate_column_name, SQLSecurityError

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ColumnKnowledge:
    """Knowledge about a single column."""
    table: str
    column: str
    data_type: str
    distinct_count: int = 0
    sample_values: List[str] = field(default_factory=list)
    value_frequencies: Dict[str, int] = field(default_factory=dict)
    has_nulls: bool = False
    null_percentage: float = 0.0


@dataclass
class PatternKnowledge:
    """A discovered pattern in the data."""
    pattern_type: str  # "value_location", "relationship", etc.
    description: str
    table: str
    column: str
    examples: List[str] = field(default_factory=list)


@dataclass
class Suggestion:
    """A suggested correction."""
    original: str
    suggested: str
    source_column: str
    confidence: float


# =============================================================================
# DATA KNOWLEDGE STORE
# =============================================================================

class DataKnowledge:
    """
    Stores knowledge learned from data.
    """

    def __init__(self):
        """Initialize data knowledge store."""
        self.columns: Dict[str, Dict[str, ColumnKnowledge]] = {}  # table -> column -> knowledge
        self.patterns: List[PatternKnowledge] = []
        self.value_index: Dict[str, List[Tuple[str, str]]] = {}  # value -> [(table, column)]
        self.learned_at: Optional[datetime] = None

    def add_column_knowledge(
        self,
        table: str,
        column: str,
        knowledge: ColumnKnowledge
    ):
        """Add knowledge about a column."""
        if table not in self.columns:
            self.columns[table] = {}
        self.columns[table][column] = knowledge

        # Index values for quick lookup
        for value in knowledge.sample_values:
            value_lower = value.lower() if isinstance(value, str) else str(value).lower()
            if value_lower not in self.value_index:
                self.value_index[value_lower] = []
            self.value_index[value_lower].append((table, column))

    def add_pattern(self, pattern: PatternKnowledge):
        """Add a discovered pattern."""
        self.patterns.append(pattern)

    def find_column_for_value(
        self,
        value: str,
        table_hint: str = None
    ) -> List[Tuple[str, str, float]]:
        """
        Find which column(s) might contain a value.

        Args:
            value: Value to find
            table_hint: Optional table hint

        Returns:
            List of (table, column, confidence) tuples
        """
        results = []
        value_lower = value.lower()

        # Exact match
        if value_lower in self.value_index:
            for table, column in self.value_index[value_lower]:
                if table_hint and table != table_hint:
                    continue
                results.append((table, column, 1.0))

        # Fuzzy match if no exact matches
        if not results:
            from rapidfuzz import fuzz
            for indexed_value, locations in self.value_index.items():
                similarity = fuzz.ratio(value_lower, indexed_value) / 100
                if similarity > 0.8:
                    for table, column in locations:
                        if table_hint and table != table_hint:
                            continue
                        results.append((table, column, similarity))

        return sorted(results, key=lambda x: x[2], reverse=True)

    def get_similar_values(
        self,
        term: str,
        limit: int = 5
    ) -> List[Tuple[str, str, List[Tuple[str, float]]]]:
        """
        Get similar values from the data.

        Args:
            term: Term to match
            limit: Maximum matches per column

        Returns:
            List of (table, column, [(value, similarity)]) tuples
        """
        results = []
        term_lower = term.lower()

        try:
            from rapidfuzz import fuzz

            for table, columns in self.columns.items():
                for column, knowledge in columns.items():
                    matches = []
                    for value in knowledge.sample_values[:100]:  # Limit sample
                        value_str = str(value).lower()
                        similarity = fuzz.ratio(term_lower, value_str) / 100
                        if similarity > 0.6:
                            matches.append((value, similarity))

                    if matches:
                        matches.sort(key=lambda x: x[1], reverse=True)
                        results.append((table, column, matches[:limit]))
        except ImportError:
            logger.warning("rapidfuzz not available for fuzzy matching")

        return results

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            'tables': list(self.columns.keys()),
            'total_columns': sum(len(cols) for cols in self.columns.values()),
            'patterns_count': len(self.patterns),
            'indexed_values': len(self.value_index),
            'learned_at': self.learned_at.isoformat() if self.learned_at else None
        }


# =============================================================================
# DATA KNOWLEDGE LEARNER
# =============================================================================

class DataKnowledgeLearner:
    """
    Learns knowledge from actual data.
    """

    # Maximum values to sample per column
    MAX_SAMPLE_VALUES = 100

    # Columns to prioritize for learning
    PRIORITY_COLUMNS = [
        'AEDECOD', 'AEBODSYS', 'AESEV', 'AESER', 'AEREL', 'ATOXGR',
        'PARAMCD', 'PARAM', 'TRT01A', 'TRT01P',
        'SEX', 'RACE', 'ETHNIC', 'COUNTRY', 'SITEID'
    ]

    def __init__(self, db_path: str):
        """
        Initialize learner.

        Args:
            db_path: Path to DuckDB database
        """
        self.db_path = db_path
        self._conn = None

    def _get_connection(self):
        """Get database connection."""
        if self._conn is None:
            import duckdb
            self._conn = duckdb.connect(self.db_path, read_only=True)
        return self._conn

    def learn(self) -> DataKnowledge:
        """
        Learn knowledge from the database.

        Returns:
            DataKnowledge populated from data
        """
        logger.info("Learning data knowledge from database...")
        knowledge = DataKnowledge()

        try:
            conn = self._get_connection()

            # Get all tables
            tables = self._get_tables(conn)

            for table in tables:
                # Get columns for this table
                columns = self._get_columns(conn, table)

                for column in columns:
                    # Learn about this column
                    col_knowledge = self._learn_column(conn, table, column)
                    if col_knowledge:
                        knowledge.add_column_knowledge(table, column, col_knowledge)

            # Discover patterns
            patterns = self._discover_patterns(knowledge)
            for pattern in patterns:
                knowledge.add_pattern(pattern)

            knowledge.learned_at = datetime.now()

            logger.info(f"Learned knowledge: {len(tables)} tables, "
                       f"{sum(len(cols) for cols in knowledge.columns.values())} columns, "
                       f"{len(knowledge.patterns)} patterns")

        except Exception as e:
            logger.error(f"Error learning data knowledge: {e}")

        return knowledge

    def _get_tables(self, conn) -> List[str]:
        """Get list of tables."""
        try:
            result = conn.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'main'
                  AND table_type = 'BASE TABLE'
            """).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            # Fallback for DuckDB
            logger.debug(f"information_schema query failed, using SHOW TABLES: {e}")
            try:
                result = conn.execute("SHOW TABLES").fetchall()
                return [row[0] for row in result]
            except Exception as e2:
                logger.warning(f"Failed to get tables: {e2}")
                return []

    def _get_columns(self, conn, table: str) -> List[str]:
        """Get columns for a table."""
        # Validate table name to prevent SQL injection
        if not validate_table_name(table):
            logger.warning(f"Invalid table name rejected: {table}")
            return []

        try:
            # Use parameterized query pattern (table name validated above)
            result = conn.execute(f"""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '{table}'
            """).fetchall()
            return [row[0] for row in result]
        except Exception as e:
            # Fallback - table name already validated
            logger.debug(f"information_schema query failed, using DESCRIBE: {e}")
            try:
                result = conn.execute(f"DESCRIBE {table}").fetchall()
                return [row[0] for row in result]
            except Exception as e2:
                logger.warning(f"Failed to get columns for {table}: {e2}")
                return []

    def _learn_column(
        self,
        conn,
        table: str,
        column: str
    ) -> Optional[ColumnKnowledge]:
        """Learn about a specific column."""
        # Validate table and column names to prevent SQL injection
        if not validate_table_name(table):
            logger.warning(f"Invalid table name rejected in _learn_column: {table}")
            return None
        if not validate_column_name(column):
            logger.warning(f"Invalid column name rejected in _learn_column: {column}")
            return None

        try:
            # Get data type (table/column validated above)
            type_result = conn.execute(f"""
                SELECT data_type FROM information_schema.columns
                WHERE table_name = '{table}' AND column_name = '{column}'
            """).fetchone()
            data_type = type_result[0] if type_result else "unknown"

            # Get distinct count and sample values (table/column validated)
            result = conn.execute(f"""
                SELECT
                    COUNT(DISTINCT "{column}") as distinct_count,
                    COUNT(*) FILTER (WHERE "{column}" IS NULL) as null_count,
                    COUNT(*) as total_count
                FROM {table}
            """).fetchone()

            distinct_count = result[0] if result else 0
            null_count = result[1] if result else 0
            total_count = result[2] if result else 1
            null_percentage = (null_count / total_count * 100) if total_count > 0 else 0

            # Get sample values (most common) - table/column validated
            sample_result = conn.execute(f"""
                SELECT "{column}", COUNT(*) as freq
                FROM {table}
                WHERE "{column}" IS NOT NULL
                GROUP BY "{column}"
                ORDER BY freq DESC
                LIMIT {self.MAX_SAMPLE_VALUES}
            """).fetchall()

            sample_values = [str(row[0]) for row in sample_result if row[0] is not None]
            value_frequencies = {str(row[0]): row[1] for row in sample_result if row[0] is not None}

            return ColumnKnowledge(
                table=table,
                column=column,
                data_type=data_type,
                distinct_count=distinct_count,
                sample_values=sample_values,
                value_frequencies=value_frequencies,
                has_nulls=null_count > 0,
                null_percentage=null_percentage
            )

        except Exception as e:
            logger.warning(f"Error learning column {table}.{column}: {e}")
            return None

    def _discover_patterns(self, knowledge: DataKnowledge) -> List[PatternKnowledge]:
        """Discover patterns in the knowledge."""
        patterns = []

        # Pattern: Adverse event terms in AEDECOD
        if 'ADAE' in knowledge.columns and 'AEDECOD' in knowledge.columns.get('ADAE', {}):
            patterns.append(PatternKnowledge(
                pattern_type="value_location",
                description="Adverse event preferred terms are in AEDECOD",
                table="ADAE",
                column="AEDECOD",
                examples=knowledge.columns['ADAE']['AEDECOD'].sample_values[:5]
            ))

        # Pattern: Body systems in AEBODSYS
        if 'ADAE' in knowledge.columns and 'AEBODSYS' in knowledge.columns.get('ADAE', {}):
            patterns.append(PatternKnowledge(
                pattern_type="value_location",
                description="Body system/organ class in AEBODSYS",
                table="ADAE",
                column="AEBODSYS",
                examples=knowledge.columns['ADAE']['AEBODSYS'].sample_values[:5]
            ))

        # Pattern: Treatment names in TRT01A
        for table in ['ADSL', 'DM']:
            if table in knowledge.columns and 'TRT01A' in knowledge.columns.get(table, {}):
                patterns.append(PatternKnowledge(
                    pattern_type="value_location",
                    description="Treatment names in TRT01A",
                    table=table,
                    column="TRT01A",
                    examples=knowledge.columns[table]['TRT01A'].sample_values[:5]
                ))

        return patterns

    def quick_learn(self, tables: List[str] = None) -> DataKnowledge:
        """
        Quick learning for specific tables only.

        Args:
            tables: List of tables to learn, or None for priority tables

        Returns:
            DataKnowledge with limited scope
        """
        if tables is None:
            tables = ['ADAE', 'ADSL', 'DM', 'AE']

        logger.info(f"Quick learning for tables: {tables}")
        knowledge = DataKnowledge()

        try:
            conn = self._get_connection()

            for table in tables:
                # Validate table name before SQL use
                if not validate_table_name(table):
                    logger.debug(f"Skipping invalid table name in quick_learn: {table}")
                    continue

                # Check if table exists
                try:
                    conn.execute(f"SELECT 1 FROM {table} LIMIT 1")
                except Exception:
                    continue

                # Learn priority columns
                for column in self.PRIORITY_COLUMNS:
                    try:
                        col_knowledge = self._learn_column(conn, table, column)
                        if col_knowledge:
                            knowledge.add_column_knowledge(table, column, col_knowledge)
                    except Exception:
                        pass

            knowledge.learned_at = datetime.now()

        except Exception as e:
            logger.error(f"Error in quick learning: {e}")

        return knowledge

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None


# =============================================================================
# SUGGESTION ENGINE
# =============================================================================

class SuggestionEngine:
    """
    Suggests corrections based on data knowledge.
    """

    def __init__(self, knowledge: DataKnowledge):
        """
        Initialize suggestion engine.

        Args:
            knowledge: Data knowledge to use
        """
        self.knowledge = knowledge

    def suggest_corrections(
        self,
        term: str,
        context: str = None
    ) -> List[Suggestion]:
        """
        Suggest corrections for a term.

        Args:
            term: Term to correct
            context: Optional context (e.g., "adverse event", "treatment")

        Returns:
            List of suggestions sorted by confidence
        """
        suggestions = []

        # Get similar values from knowledge
        similar = self.knowledge.get_similar_values(term)

        for table, column, matches in similar:
            # Filter by context if provided
            if context:
                if 'adverse' in context.lower() and table not in ['ADAE', 'AE']:
                    continue
                if 'treatment' in context.lower() and column not in ['TRT01A', 'TRT01P']:
                    continue

            for value, similarity in matches:
                suggestions.append(Suggestion(
                    original=term,
                    suggested=value,
                    source_column=f"{table}.{column}",
                    confidence=similarity
                ))

        # Sort by confidence
        suggestions.sort(key=lambda s: s.confidence, reverse=True)

        # Remove duplicates
        seen = set()
        unique = []
        for s in suggestions:
            if s.suggested not in seen:
                seen.add(s.suggested)
                unique.append(s)

        return unique[:10]  # Return top 10

    def find_column(
        self,
        term: str,
        table_hint: str = None
    ) -> Optional[Tuple[str, str]]:
        """
        Find which column contains a term.

        Args:
            term: Term to find
            table_hint: Optional table hint

        Returns:
            (table, column) or None
        """
        results = self.knowledge.find_column_for_value(term, table_hint)
        if results:
            return results[0][:2]  # Return best match
        return None


# =============================================================================
# GLOBAL INSTANCE
# =============================================================================

_data_knowledge: Optional[DataKnowledge] = None


def get_data_knowledge(db_path: str = None) -> DataKnowledge:
    """Get or create data knowledge."""
    global _data_knowledge
    if _data_knowledge is None and db_path:
        learner = DataKnowledgeLearner(db_path)
        _data_knowledge = learner.quick_learn()
        learner.close()
    return _data_knowledge or DataKnowledge()


def refresh_data_knowledge(db_path: str) -> DataKnowledge:
    """Refresh data knowledge from database."""
    global _data_knowledge
    learner = DataKnowledgeLearner(db_path)
    _data_knowledge = learner.learn()
    learner.close()
    return _data_knowledge
