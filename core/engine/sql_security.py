# SAGE - SQL Security Utilities
# =============================
"""
SQL Security Utilities
======================
Provides safe SQL identifier handling to prevent SQL injection attacks.

All table and column names MUST be validated through this module before
being used in SQL queries via f-strings or string concatenation.

Usage:
    from core.engine.sql_security import validate_identifier, safe_quote_identifier

    # Validate table name
    if validate_identifier(table_name, IdentifierType.TABLE):
        sql = f"SELECT * FROM {safe_quote_identifier(table_name)}"

    # Or use the safe query builder
    builder = SafeQueryBuilder()
    sql = builder.select_from(table_name, columns=['USUBJID', 'AGE'])
"""

import re
import logging
from typing import Set, List, Optional, Union
from enum import Enum

logger = logging.getLogger(__name__)


class IdentifierType(Enum):
    """Types of SQL identifiers."""
    TABLE = "table"
    COLUMN = "column"
    SCHEMA = "schema"


# Whitelist of valid CDISC table names (SDTM and ADaM)
VALID_TABLES: Set[str] = {
    # ADaM datasets
    'ADSL', 'ADAE', 'ADLB', 'ADVS', 'ADCM', 'ADEX', 'ADMH', 'ADPC', 'ADPP',
    'ADTTE', 'ADEFF', 'ADQOL', 'ADEF',
    # SDTM domains
    'DM', 'AE', 'CM', 'LB', 'VS', 'EX', 'MH', 'DS', 'DV', 'SV', 'SE', 'TA',
    'TV', 'TS', 'TI', 'TE', 'TD', 'EG', 'QS', 'SC', 'FA', 'FT', 'IE', 'PE',
    'PP', 'PC', 'DA', 'EC', 'SU', 'HO', 'AG', 'MI', 'MO', 'MS', 'TU', 'TR',
    'RS', 'IS', 'MB', 'CV', 'DD', 'RP', 'RE', 'SS',
    # System tables (read-only access allowed)
    'information_schema',
}

# Pattern for valid SQL identifiers (CDISC naming convention)
# Must start with letter, contain only alphanumeric and underscore
VALID_IDENTIFIER_PATTERN = re.compile(r'^[A-Za-z][A-Za-z0-9_]*$')

# Maximum identifier length (prevent DoS via very long names)
MAX_IDENTIFIER_LENGTH = 64

# Dangerous SQL keywords that should never appear in identifiers
DANGEROUS_KEYWORDS: Set[str] = {
    'DROP', 'DELETE', 'INSERT', 'UPDATE', 'TRUNCATE', 'ALTER', 'CREATE',
    'EXEC', 'EXECUTE', 'UNION', 'SELECT', 'FROM', 'WHERE', 'INTO',
    'GRANT', 'REVOKE', 'COMMIT', 'ROLLBACK', '--', '/*', '*/', ';',
}


class SQLSecurityError(Exception):
    """Raised when SQL security validation fails."""
    pass


def validate_identifier(
    name: str,
    identifier_type: IdentifierType = IdentifierType.TABLE,
    allow_qualified: bool = False
) -> bool:
    """
    Validate a SQL identifier (table or column name).

    Args:
        name: The identifier to validate
        identifier_type: Type of identifier (TABLE, COLUMN, SCHEMA)
        allow_qualified: Allow schema.table or table.column notation

    Returns:
        True if identifier is safe to use, False otherwise

    Examples:
        >>> validate_identifier('ADAE', IdentifierType.TABLE)
        True
        >>> validate_identifier("DROP TABLE; --", IdentifierType.TABLE)
        False
    """
    if not name or not isinstance(name, str):
        return False

    # Check length
    if len(name) > MAX_IDENTIFIER_LENGTH:
        logger.warning(f"Identifier too long: {name[:20]}... ({len(name)} chars)")
        return False

    # Handle qualified names (schema.table or table.column)
    if '.' in name:
        if not allow_qualified:
            logger.warning(f"Qualified identifier not allowed: {name}")
            return False
        parts = name.split('.')
        if len(parts) != 2:
            return False
        return all(validate_identifier(p, identifier_type, allow_qualified=False) for p in parts)

    # Check for dangerous keywords
    name_upper = name.upper()
    for keyword in DANGEROUS_KEYWORDS:
        if keyword in name_upper:
            logger.warning(f"Dangerous keyword '{keyword}' in identifier: {name}")
            return False

    # Check pattern
    if not VALID_IDENTIFIER_PATTERN.match(name):
        logger.warning(f"Invalid identifier pattern: {name}")
        return False

    # For tables, also check whitelist
    if identifier_type == IdentifierType.TABLE:
        if name_upper not in {t.upper() for t in VALID_TABLES}:
            logger.warning(f"Table not in whitelist: {name}")
            return False

    return True


def validate_table_name(table: str) -> bool:
    """
    Validate a table name against the whitelist.

    Args:
        table: Table name to validate

    Returns:
        True if table name is valid and whitelisted
    """
    return validate_identifier(table, IdentifierType.TABLE)


def validate_column_name(column: str) -> bool:
    """
    Validate a column name.

    Args:
        column: Column name to validate

    Returns:
        True if column name is valid
    """
    return validate_identifier(column, IdentifierType.COLUMN)


def safe_quote_identifier(name: str) -> str:
    """
    Safely quote a SQL identifier for use in queries.

    This MUST be called after validate_identifier() returns True.

    Args:
        name: The pre-validated identifier

    Returns:
        Quoted identifier safe for SQL use

    Raises:
        SQLSecurityError: If identifier is not valid

    Examples:
        >>> safe_quote_identifier('ADAE')
        '"ADAE"'
        >>> safe_quote_identifier('My Table')
        Raises SQLSecurityError
    """
    if not validate_identifier(name, IdentifierType.COLUMN, allow_qualified=True):
        raise SQLSecurityError(f"Cannot quote invalid identifier: {name}")

    # Double-quote the identifier (ANSI SQL standard)
    # Also escape any embedded quotes (though our validation should prevent this)
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def safe_table_name(table: str) -> str:
    """
    Return a safe table name for use in SQL queries.

    Args:
        table: Table name to validate and return

    Returns:
        The validated table name (uppercase)

    Raises:
        SQLSecurityError: If table name is invalid
    """
    if not validate_table_name(table):
        raise SQLSecurityError(f"Invalid or unknown table: {table}")
    return table.upper()


def safe_column_name(column: str) -> str:
    """
    Return a safe column name for use in SQL queries.

    Args:
        column: Column name to validate and return

    Returns:
        The validated column name (uppercase)

    Raises:
        SQLSecurityError: If column name is invalid
    """
    if not validate_column_name(column):
        raise SQLSecurityError(f"Invalid column name: {column}")
    return column.upper()


def safe_column_list(columns: List[str]) -> str:
    """
    Create a safe comma-separated column list for SELECT.

    Args:
        columns: List of column names

    Returns:
        Safe column list string

    Raises:
        SQLSecurityError: If any column is invalid
    """
    if not columns:
        return "*"

    validated = []
    for col in columns:
        if not validate_column_name(col):
            raise SQLSecurityError(f"Invalid column name: {col}")
        validated.append(col.upper())

    return ", ".join(validated)


def add_table_to_whitelist(table: str) -> None:
    """
    Dynamically add a table to the whitelist.

    Use this carefully - only for tables discovered from the database schema.

    Args:
        table: Table name to add
    """
    if VALID_IDENTIFIER_PATTERN.match(table):
        VALID_TABLES.add(table.upper())
        logger.info(f"Added table to whitelist: {table}")
    else:
        logger.warning(f"Cannot add invalid table name to whitelist: {table}")


def discover_and_whitelist_tables(db_connection) -> Set[str]:
    """
    Discover tables from database and add to whitelist.

    This should be called during initialization to add any
    custom tables that exist in the database.

    Args:
        db_connection: DuckDB connection

    Returns:
        Set of discovered table names
    """
    discovered = set()
    try:
        # Get tables from information_schema (safe query, no user input)
        result = db_connection.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()

        for row in result:
            table_name = row[0]
            if VALID_IDENTIFIER_PATTERN.match(table_name):
                VALID_TABLES.add(table_name.upper())
                discovered.add(table_name.upper())

        logger.info(f"Discovered and whitelisted {len(discovered)} tables")
    except Exception as e:
        logger.warning(f"Failed to discover tables: {e}")

    return discovered


class SafeQueryBuilder:
    """
    Build safe SQL queries with validated identifiers.

    Example:
        builder = SafeQueryBuilder()
        sql = (builder
            .select(['USUBJID', 'AGE'])
            .from_table('ADSL')
            .where("SAFFL = 'Y'")
            .build())
    """

    def __init__(self):
        self._select_cols: List[str] = []
        self._table: Optional[str] = None
        self._where: List[str] = []
        self._limit: Optional[int] = None

    def select(self, columns: Union[List[str], str] = "*") -> 'SafeQueryBuilder':
        """Add SELECT columns."""
        if columns == "*":
            self._select_cols = ["*"]
        elif isinstance(columns, str):
            if not validate_column_name(columns):
                raise SQLSecurityError(f"Invalid column: {columns}")
            self._select_cols = [columns.upper()]
        else:
            for col in columns:
                if not validate_column_name(col):
                    raise SQLSecurityError(f"Invalid column: {col}")
            self._select_cols = [c.upper() for c in columns]
        return self

    def from_table(self, table: str) -> 'SafeQueryBuilder':
        """Add FROM table."""
        if not validate_table_name(table):
            raise SQLSecurityError(f"Invalid table: {table}")
        self._table = table.upper()
        return self

    def where(self, condition: str) -> 'SafeQueryBuilder':
        """Add WHERE condition (must be pre-sanitized)."""
        # Note: WHERE conditions should use parameterized queries
        # This is a simplified builder; full implementation would use params
        self._where.append(condition)
        return self

    def limit(self, n: int) -> 'SafeQueryBuilder':
        """Add LIMIT clause."""
        self._limit = max(0, int(n))
        return self

    def build(self) -> str:
        """Build the SQL query."""
        if not self._table:
            raise SQLSecurityError("No table specified")

        parts = ["SELECT"]

        # Columns
        if not self._select_cols or self._select_cols == ["*"]:
            parts.append("*")
        else:
            parts.append(", ".join(self._select_cols))

        # Table
        parts.append(f"FROM {self._table}")

        # Where
        if self._where:
            parts.append("WHERE " + " AND ".join(self._where))

        # Limit
        if self._limit is not None:
            parts.append(f"LIMIT {self._limit}")

        return " ".join(parts)
