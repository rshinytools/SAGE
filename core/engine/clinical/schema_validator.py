"""
Schema Validation Layer - Validate SQL against current database schema.

Prevents execution of SQL with outdated column/table references.
"""

import re
from dataclasses import dataclass
from typing import List, Dict, Set, Optional, Any
from enum import Enum


class SchemaValidationStatus(Enum):
    """Status of schema validation."""
    VALID = "VALID"
    COLUMN_MISSING = "COLUMN_MISSING"
    TABLE_MISSING = "TABLE_MISSING"
    TYPE_MISMATCH = "TYPE_MISMATCH"
    REPAIRABLE = "REPAIRABLE"


@dataclass
class SchemaIssue:
    """A schema validation issue."""
    issue_type: SchemaValidationStatus
    element: str  # Table or column name
    expected: Optional[str]
    actual: Optional[str]
    suggestion: Optional[str]


@dataclass
class SchemaValidationResult:
    """Result of schema validation."""
    is_valid: bool
    status: SchemaValidationStatus
    issues: List[SchemaIssue]
    tables_found: List[str]
    columns_found: List[str]
    can_auto_repair: bool
    repaired_sql: Optional[str]


class SchemaValidator:
    """
    Schema Validation Layer - Ensures SQL matches current database schema.

    Key Principle: Never execute SQL with invalid references.
    """

    # Common column renames to auto-repair
    COLUMN_ALIASES: Dict[str, List[str]] = {
        "VISIT": ["VISITNUM", "AVISIT", "VISIT_NAME"],
        "VISITNUM": ["VISIT", "AVISITN"],
        "SUBJID": ["USUBJID", "SUBJECT_ID", "SUBJECTID"],
        "USUBJID": ["SUBJID", "SUBJECT_ID"],
        "TRT": ["TRT01P", "TRTP", "TREATMENT"],
        "TRT01P": ["TRT", "TRTP", "ARM"],
        "AESTDTC": ["AESTDT", "AE_START_DATE"],
        "AGE": ["APTS", "AGE_YEARS"],
        "SEX": ["GENDER"],
        "RACE": ["ETHNIC"]
    }

    def __init__(self, db_connection: Any = None):
        """
        Initialize with database connection.

        Args:
            db_connection: DuckDB or similar database connection
        """
        self.db = db_connection
        self._schema_cache: Dict[str, Set[str]] = {}
        if db_connection:
            self._refresh_schema()

    def _refresh_schema(self):
        """Refresh schema cache from database."""
        if not self.db:
            return

        self._schema_cache = {}

        try:
            # Get all tables
            tables = self.db.execute("""
                SELECT table_name
                FROM information_schema.tables
                WHERE table_schema = 'main'
            """).fetchall()

            for (table_name,) in tables:
                # Get columns for each table
                columns = self.db.execute(f"""
                    SELECT column_name
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                """).fetchall()

                self._schema_cache[table_name.upper()] = {
                    col[0].upper() for col in columns
                }
        except Exception as e:
            print(f"Warning: Could not refresh schema: {e}")

    def set_schema(self, schema: Dict[str, Set[str]]):
        """Manually set schema for testing."""
        self._schema_cache = {k.upper(): {c.upper() for c in v} for k, v in schema.items()}

    def validate(self, sql: str) -> SchemaValidationResult:
        """
        Validate SQL against current schema.

        Args:
            sql: SQL query to validate

        Returns:
            SchemaValidationResult with validation status and any issues
        """
        if not self._schema_cache:
            # No schema loaded, assume valid
            return SchemaValidationResult(
                is_valid=True,
                status=SchemaValidationStatus.VALID,
                issues=[],
                tables_found=[],
                columns_found=[],
                can_auto_repair=False,
                repaired_sql=None
            )

        issues: List[SchemaIssue] = []
        sql_upper = sql.upper()

        # Extract tables
        tables_in_sql = self._extract_tables(sql)
        tables_found = list(tables_in_sql)

        # Validate tables exist
        for table in tables_in_sql:
            table_upper = table.upper()
            if table_upper not in self._schema_cache:
                issues.append(SchemaIssue(
                    issue_type=SchemaValidationStatus.TABLE_MISSING,
                    element=table,
                    expected=table,
                    actual=None,
                    suggestion=self._suggest_table(table_upper)
                ))

        # Extract and validate columns
        columns_in_sql = self._extract_columns(sql)
        columns_found = list(columns_in_sql)

        for column in columns_in_sql:
            column_upper = column.upper()
            found = False

            # Check if column exists in any referenced table
            for table in tables_in_sql:
                table_upper = table.upper()
                if table_upper in self._schema_cache:
                    if column_upper in self._schema_cache[table_upper]:
                        found = True
                        break

            if not found:
                # Check if it's a known alias
                suggestion = self._suggest_column(column_upper, tables_in_sql)
                issues.append(SchemaIssue(
                    issue_type=SchemaValidationStatus.COLUMN_MISSING,
                    element=column,
                    expected=column,
                    actual=None,
                    suggestion=suggestion
                ))

        # Determine overall status
        if not issues:
            return SchemaValidationResult(
                is_valid=True,
                status=SchemaValidationStatus.VALID,
                issues=[],
                tables_found=tables_found,
                columns_found=columns_found,
                can_auto_repair=False,
                repaired_sql=None
            )

        # Check if all issues are repairable
        can_auto_repair = all(issue.suggestion is not None for issue in issues)

        if can_auto_repair:
            status = SchemaValidationStatus.REPAIRABLE
            repaired_sql = self._auto_repair(sql, issues)
        else:
            status = issues[0].issue_type
            repaired_sql = None

        return SchemaValidationResult(
            is_valid=False,
            status=status,
            issues=issues,
            tables_found=tables_found,
            columns_found=columns_found,
            can_auto_repair=can_auto_repair,
            repaired_sql=repaired_sql
        )

    def _extract_tables(self, sql: str) -> Set[str]:
        """Extract table names from SQL."""
        tables = set()

        # Simple regex for FROM and JOIN clauses
        patterns = [
            r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)',
            r'\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)',
        ]

        for pattern in patterns:
            matches = re.findall(pattern, sql, re.IGNORECASE)
            tables.update(matches)

        return tables

    def _extract_columns(self, sql: str) -> Set[str]:
        """Extract column names from SQL."""
        columns = set()

        # Remove string literals to avoid false matches
        sql_clean = re.sub(r"'[^']*'", "", sql)

        # Match table.column patterns
        table_col_matches = re.findall(
            r'([a-zA-Z_][a-zA-Z0-9_]*)\.([a-zA-Z_][a-zA-Z0-9_]*)',
            sql_clean
        )
        for _, col in table_col_matches:
            columns.add(col)

        # Match standalone columns in WHERE clause
        where_match = re.search(r'\bWHERE\s+(.+?)(?:ORDER|GROUP|LIMIT|$)', sql_clean, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_clause = where_match.group(1)
            # Extract column names (words before operators)
            col_matches = re.findall(r'\b([a-zA-Z_][a-zA-Z0-9_]*)\s*[=<>!]', where_clause)
            columns.update(col_matches)

        # Remove SQL keywords that might be captured
        sql_keywords = {'AND', 'OR', 'NOT', 'IN', 'IS', 'NULL', 'LIKE', 'BETWEEN', 'SELECT', 'FROM', 'WHERE'}
        columns = {c for c in columns if c.upper() not in sql_keywords}

        return columns

    def _suggest_table(self, table: str) -> Optional[str]:
        """Suggest a replacement table name."""
        for existing in self._schema_cache.keys():
            if table in existing or existing in table:
                return existing
        return None

    def _suggest_column(self, column: str, tables: Set[str]) -> Optional[str]:
        """Suggest a replacement column name."""
        # Check known aliases
        if column in self.COLUMN_ALIASES:
            for alias in self.COLUMN_ALIASES[column]:
                for table in tables:
                    table_upper = table.upper()
                    if table_upper in self._schema_cache:
                        if alias in self._schema_cache[table_upper]:
                            return alias

        # Check all tables for similar columns
        for table in tables:
            table_upper = table.upper()
            if table_upper in self._schema_cache:
                for existing in self._schema_cache[table_upper]:
                    if column in existing or existing in column:
                        return existing

        return None

    def _auto_repair(self, sql: str, issues: List[SchemaIssue]) -> str:
        """Attempt to auto-repair SQL with suggested replacements."""
        repaired = sql

        for issue in issues:
            if issue.suggestion:
                # Replace the problematic element with suggestion
                pattern = rf'\b{re.escape(issue.element)}\b'
                repaired = re.sub(pattern, issue.suggestion, repaired, flags=re.IGNORECASE)

        return repaired

    def get_schema_info(self) -> Dict[str, List[str]]:
        """Get current schema information."""
        return {table: list(columns) for table, columns in self._schema_cache.items()}
