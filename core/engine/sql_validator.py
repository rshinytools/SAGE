# SAGE - SQL Validator
# =====================
"""
SQL Validator
=============
Validates generated SQL for:
1. Syntax correctness
2. Table/column existence
3. Dangerous operations
4. SQL injection patterns

This is STEP 6 of the 9-step pipeline.
"""

import re
import logging
from typing import List, Set, Optional, Tuple
from dataclasses import dataclass

from .models import ValidationResult

logger = logging.getLogger(__name__)


@dataclass
class ValidatorConfig:
    """Configuration for SQL validator."""
    # Block dangerous operations
    block_delete: bool = True
    block_update: bool = True
    block_drop: bool = True
    block_insert: bool = True
    block_truncate: bool = True
    block_alter: bool = True
    block_create: bool = True

    # Block information schema access (potential info disclosure)
    block_info_schema: bool = True

    # Maximum query complexity (number of joins)
    max_joins: int = 5

    # Maximum result limit
    max_limit: int = 10000


class SQLValidator:
    """
    Validates SQL queries for safety and correctness.

    Checks:
    - Dangerous operations (DELETE, UPDATE, DROP, etc.)
    - SQL injection patterns
    - Table and column existence
    - Query complexity

    Example:
        validator = SQLValidator(available_tables)
        result = validator.validate(sql)
        if result.is_valid:
            # Execute query
            pass
        else:
            print(f"Invalid: {result.errors}")
    """

    # Dangerous SQL patterns
    DANGEROUS_PATTERNS = {
        'delete': re.compile(r'\bDELETE\s+FROM\b', re.IGNORECASE),
        'update': re.compile(r'\bUPDATE\s+\w+\s+SET\b', re.IGNORECASE),
        'drop': re.compile(r'\bDROP\s+(?:TABLE|DATABASE|INDEX|VIEW)\b', re.IGNORECASE),
        'insert': re.compile(r'\bINSERT\s+INTO\b', re.IGNORECASE),
        'truncate': re.compile(r'\bTRUNCATE\s+TABLE\b', re.IGNORECASE),
        'alter': re.compile(r'\bALTER\s+TABLE\b', re.IGNORECASE),
        'create': re.compile(r'\bCREATE\s+(?:TABLE|DATABASE|INDEX|VIEW)\b', re.IGNORECASE),
        'exec': re.compile(r'\b(?:EXEC|EXECUTE)\b', re.IGNORECASE),
        'info_schema': re.compile(r'\bINFORMATION_SCHEMA\b', re.IGNORECASE),
    }

    # SQL injection patterns
    INJECTION_PATTERNS = {
        'comment': re.compile(r'(?:--|#|/\*)', re.IGNORECASE),
        'union_attack': re.compile(r"'\s*UNION\s+(?:ALL\s+)?SELECT", re.IGNORECASE),
        'or_true': re.compile(r"'\s*OR\s+['\d]\s*=\s*['\d]", re.IGNORECASE),
        'semicolon': re.compile(r';\s*(?:SELECT|INSERT|UPDATE|DELETE|DROP)', re.IGNORECASE),
        'hex_encode': re.compile(r'0x[0-9a-fA-F]+', re.IGNORECASE),
        'char_encode': re.compile(r'\bCHAR\s*\(\s*\d+', re.IGNORECASE),
    }

    def __init__(self,
                 available_tables: dict = None,
                 config: ValidatorConfig = None):
        """
        Initialize validator.

        Args:
            available_tables: Dict of table_name -> [columns]
            config: Validator configuration
        """
        self.config = config or ValidatorConfig()
        self.available_tables = {
            t.upper(): [c.upper() for c in cols]
            for t, cols in (available_tables or {}).items()
        }

    def validate(self, sql: str) -> ValidationResult:
        """
        Validate SQL query.

        Args:
            sql: SQL query to validate

        Returns:
            ValidationResult with status and details
        """
        errors = []
        warnings = []
        dangerous_found = []

        if not sql or not sql.strip():
            return ValidationResult(
                is_valid=False,
                validated_sql="",
                errors=["Empty SQL query"]
            )

        sql = sql.strip()

        # Check for dangerous operations
        dangerous = self._check_dangerous_operations(sql)
        if dangerous:
            dangerous_found.extend(dangerous)
            errors.append(f"Dangerous operations blocked: {', '.join(dangerous)}")

        # Check for SQL injection
        injections = self._check_injection_patterns(sql)
        if injections:
            dangerous_found.extend(injections)
            errors.append(f"SQL injection patterns detected: {', '.join(injections)}")

        # Verify it's a SELECT query
        if not self._is_select_query(sql):
            errors.append("Only SELECT queries are allowed")

        # Check query complexity
        join_count = self._count_joins(sql)
        if join_count > self.config.max_joins:
            warnings.append(f"Query has {join_count} joins (max recommended: {self.config.max_joins})")

        # Verify tables exist
        tables_used = self._extract_tables(sql)
        tables_verified = []
        for table in tables_used:
            if table.upper() in self.available_tables:
                tables_verified.append(table)
            else:
                errors.append(f"Table not found: {table}")

        # Verify columns exist
        columns_verified = []
        if self.available_tables:
            columns_used = self._extract_columns(sql, tables_used)
            for col, table in columns_used:
                table_upper = table.upper() if table else None
                col_upper = col.upper()

                if table_upper and table_upper in self.available_tables:
                    if col_upper in self.available_tables[table_upper]:
                        columns_verified.append(col)
                    else:
                        # Check if column exists in any table
                        found = False
                        for t, cols in self.available_tables.items():
                            if col_upper in cols:
                                found = True
                                break
                        if not found:
                            warnings.append(f"Column '{col}' not found in expected tables")

        # Add LIMIT if not present
        validated_sql = sql
        if not re.search(r'\bLIMIT\s+\d+', sql, re.IGNORECASE):
            validated_sql = f"{sql} LIMIT {self.config.max_limit}"
            warnings.append(f"Added LIMIT {self.config.max_limit} for safety")

        return ValidationResult(
            is_valid=len(errors) == 0,
            validated_sql=validated_sql if len(errors) == 0 else "",
            errors=errors,
            warnings=warnings,
            tables_verified=tables_verified,
            columns_verified=columns_verified,
            dangerous_patterns_found=dangerous_found
        )

    def _check_dangerous_operations(self, sql: str) -> List[str]:
        """Check for dangerous SQL operations."""
        found = []

        checks = {
            'delete': self.config.block_delete,
            'update': self.config.block_update,
            'drop': self.config.block_drop,
            'insert': self.config.block_insert,
            'truncate': self.config.block_truncate,
            'alter': self.config.block_alter,
            'create': self.config.block_create,
            'exec': True,
            'info_schema': self.config.block_info_schema,
        }

        for name, should_block in checks.items():
            if should_block and name in self.DANGEROUS_PATTERNS:
                if self.DANGEROUS_PATTERNS[name].search(sql):
                    found.append(name.upper())

        return found

    def _check_injection_patterns(self, sql: str) -> List[str]:
        """Check for SQL injection patterns."""
        found = []

        for name, pattern in self.INJECTION_PATTERNS.items():
            if pattern.search(sql):
                found.append(f"injection:{name}")

        return found

    def _is_select_query(self, sql: str) -> bool:
        """Check if query is a SELECT statement."""
        # Remove leading whitespace and comments
        clean = re.sub(r'^\s*--.*$', '', sql, flags=re.MULTILINE)
        clean = clean.strip()

        return clean.upper().startswith('SELECT')

    def _count_joins(self, sql: str) -> int:
        """Count number of JOINs in query."""
        return len(re.findall(r'\bJOIN\b', sql, re.IGNORECASE))

    def _extract_tables(self, sql: str) -> List[str]:
        """Extract table names from SQL."""
        tables = []

        # FROM clause
        from_match = re.search(r'\bFROM\s+(\w+)', sql, re.IGNORECASE)
        if from_match:
            tables.append(from_match.group(1))

        # JOIN clauses
        join_matches = re.findall(r'\bJOIN\s+(\w+)', sql, re.IGNORECASE)
        tables.extend(join_matches)

        return list(set(tables))

    def _extract_columns(self, sql: str, tables: List[str]) -> List[Tuple[str, Optional[str]]]:
        """
        Extract column names with their tables.

        Returns:
            List of (column_name, table_name or None)
        """
        columns = []

        # Look for table.column patterns
        qualified = re.findall(r'(\w+)\.(\w+)', sql)
        for table, col in qualified:
            columns.append((col, table))

        # Look for unqualified columns in SELECT
        select_match = re.search(r'SELECT\s+(.*?)\s+FROM', sql, re.IGNORECASE | re.DOTALL)
        if select_match:
            select_part = select_match.group(1)
            # Extract column names (excluding functions and aliases)
            col_matches = re.findall(r'\b([A-Za-z_]\w*)\b', select_part)
            for col in col_matches:
                if col.upper() not in {'DISTINCT', 'AS', 'COUNT', 'SUM', 'AVG', 'MIN', 'MAX'}:
                    columns.append((col, None))

        # Look for columns in WHERE clause
        where_match = re.search(r'WHERE\s+(.*?)(?:GROUP|ORDER|LIMIT|$)',
                                sql, re.IGNORECASE | re.DOTALL)
        if where_match:
            where_part = where_match.group(1)
            col_matches = re.findall(r'\b([A-Za-z_]\w*)\s*[=<>!]', where_part)
            for col in col_matches:
                columns.append((col, None))

        return columns

    def quick_validate(self, sql: str) -> bool:
        """Quick validation - just check if query is safe."""
        result = self.validate(sql)
        return result.is_valid
