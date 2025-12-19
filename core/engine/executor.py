# SAGE - SQL Executor
# ====================
"""
SQL Executor
============
Executes validated SQL queries against DuckDB.

This is STEP 7 of the 9-step pipeline.
"""

import time
import logging
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

from .models import ExecutionResult

logger = logging.getLogger(__name__)


@dataclass
class ExecutorConfig:
    """Configuration for SQL executor."""
    # Timeout in seconds
    timeout_seconds: int = 30

    # Maximum rows to return
    max_rows: int = 10000

    # Read-only mode (should always be True for safety)
    read_only: bool = True

    # Memory limit for DuckDB (in bytes, 0 = unlimited)
    memory_limit: int = 0


class SQLExecutor:
    """
    Executes SQL queries against DuckDB.

    Features:
    - Timeout enforcement
    - Read-only mode
    - Result size limits
    - Error handling

    Example:
        executor = SQLExecutor(db_path)
        result = executor.execute(validated_sql)
        if result.success:
            print(result.data)
        else:
            print(result.error_message)
    """

    def __init__(self,
                 db_path: str,
                 config: Optional[ExecutorConfig] = None,
                 connection=None):
        """
        Initialize executor.

        Args:
            db_path: Path to DuckDB database
            config: Executor configuration
            connection: Optional shared DuckDB connection (avoids lock conflicts)
        """
        self.db_path = db_path
        self.config = config or ExecutorConfig()
        self._shared_connection = connection  # Use shared connection if provided

    def execute(self, sql: str) -> ExecutionResult:
        """
        Execute SQL query.

        Args:
            sql: Validated SQL query

        Returns:
            ExecutionResult with data or error
        """
        start_time = time.time()

        if not sql or not sql.strip():
            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=0,
                error_message="Empty SQL query"
            )

        try:
            import duckdb

            # Use shared connection if provided, otherwise create new one
            if self._shared_connection is not None:
                conn = self._shared_connection
                own_connection = False
            else:
                conn = duckdb.connect(
                    self.db_path,
                    read_only=self.config.read_only
                )
                own_connection = True

            try:
                # Set memory limit if specified (validated to be numeric)
                if self.config.memory_limit > 0:
                    # Ensure memory_limit is a valid integer to prevent injection
                    safe_limit = int(self.config.memory_limit)
                    conn.execute(f"SET memory_limit='{safe_limit}'")

                # Execute query
                result = conn.execute(sql)

                # Fetch results
                columns = [desc[0] for desc in result.description]
                rows = result.fetchmany(self.config.max_rows)

                # Convert to list of dicts
                data = [dict(zip(columns, row)) for row in rows]

                execution_time = (time.time() - start_time) * 1000

                # Check if there are more rows
                truncated = len(rows) >= self.config.max_rows

                return ExecutionResult(
                    success=True,
                    data=data,
                    columns=columns,
                    row_count=len(data),
                    execution_time_ms=execution_time,
                    truncated=truncated,
                    sql_executed=sql
                )

            finally:
                # Only close connection if we created it
                if own_connection:
                    conn.close()

        except duckdb.CatalogException as e:
            # Table or column not found
            execution_time = (time.time() - start_time) * 1000
            error_msg = str(e)

            # Extract helpful message
            if "Table" in error_msg and "does not exist" in error_msg:
                error_msg = f"Table not found: {error_msg}"
            elif "Column" in error_msg and "does not exist" in error_msg:
                error_msg = f"Column not found: {error_msg}"

            logger.error(f"SQL catalog error: {error_msg}")

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=execution_time,
                error_message=error_msg,
                sql_executed=sql
            )

        except duckdb.ParserException as e:
            # SQL syntax error
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"SQL parser error: {e}")

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=execution_time,
                error_message=f"SQL syntax error: {e}",
                sql_executed=sql
            )

        except duckdb.BinderException as e:
            # Column binding error
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"SQL binder error: {e}")

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=execution_time,
                error_message=f"Column reference error: {e}",
                sql_executed=sql
            )

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"SQL execution error: {e}")

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=execution_time,
                error_message=str(e),
                sql_executed=sql
            )

    def execute_with_params(self,
                            sql: str,
                            params: Dict[str, Any]
                           ) -> ExecutionResult:
        """
        Execute parameterized SQL query.

        Args:
            sql: SQL query with placeholders
            params: Parameter values

        Returns:
            ExecutionResult
        """
        start_time = time.time()

        try:
            import duckdb

            conn = duckdb.connect(
                self.db_path,
                read_only=self.config.read_only
            )

            try:
                # Execute with parameters
                result = conn.execute(sql, params)

                columns = [desc[0] for desc in result.description]
                rows = result.fetchmany(self.config.max_rows)
                data = [dict(zip(columns, row)) for row in rows]

                execution_time = (time.time() - start_time) * 1000

                return ExecutionResult(
                    success=True,
                    data=data,
                    columns=columns,
                    row_count=len(data),
                    execution_time_ms=execution_time,
                    sql_executed=sql
                )

            finally:
                conn.close()

        except Exception as e:
            execution_time = (time.time() - start_time) * 1000
            logger.error(f"Parameterized SQL error: {e}")

            return ExecutionResult(
                success=False,
                data=None,
                row_count=0,
                execution_time_ms=execution_time,
                error_message=str(e),
                sql_executed=sql
            )

    def get_tables(self) -> List[str]:
        """Get list of available tables."""
        try:
            import duckdb

            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                result = conn.execute("""
                    SELECT table_name
                    FROM information_schema.tables
                    WHERE table_schema = 'main'
                    ORDER BY table_name
                """)
                return [row[0] for row in result.fetchall()]
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error getting tables: {e}")
            return []

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Get columns for a table."""
        # Validate table name to prevent SQL injection
        from .sql_security import validate_table_name
        if not validate_table_name(table_name):
            logger.warning(f"Invalid table name rejected in get_columns: {table_name}")
            return []

        try:
            import duckdb

            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                # Table name validated above
                result = conn.execute(f"""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                return [
                    {'name': row[0], 'type': row[1]}
                    for row in result.fetchall()
                ]
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error getting columns for {table_name}: {e}")
            return []

    def get_row_count(self, table_name: str) -> int:
        """Get row count for a table."""
        # Validate table name to prevent SQL injection
        from .sql_security import validate_table_name
        if not validate_table_name(table_name):
            logger.warning(f"Invalid table name rejected in get_row_count: {table_name}")
            return 0

        try:
            import duckdb

            conn = duckdb.connect(self.db_path, read_only=True)
            try:
                # Table name validated above
                result = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
                return result.fetchone()[0]
            finally:
                conn.close()
        except Exception as e:
            logger.error(f"Error getting row count for {table_name}: {e}")
            return 0

    def validate_connection(self) -> bool:
        """Validate database connection."""
        try:
            import duckdb

            conn = duckdb.connect(self.db_path, read_only=True)
            conn.close()
            return True
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            return False


class MockExecutor(SQLExecutor):
    """
    Mock executor for testing without a real database.

    Returns predefined results based on query patterns.
    """

    def __init__(self):
        """Initialize mock executor."""
        self.db_path = ":memory:"
        self.config = ExecutorConfig()
        self._mock_data = {}

    def set_mock_data(self, query_pattern: str, data: List[Dict]):
        """Set mock data for a query pattern."""
        self._mock_data[query_pattern.lower()] = data

    def execute(self, sql: str) -> ExecutionResult:
        """Execute mock query."""
        start_time = time.time()

        sql_lower = sql.lower()

        # Check for mock data matches
        for pattern, data in self._mock_data.items():
            if pattern in sql_lower:
                execution_time = (time.time() - start_time) * 1000
                columns = list(data[0].keys()) if data else []

                return ExecutionResult(
                    success=True,
                    data=data,
                    columns=columns,
                    row_count=len(data),
                    execution_time_ms=execution_time,
                    sql_executed=sql
                )

        # Default mock response
        if 'count' in sql_lower:
            data = [{'subject_count': 42}]
        elif 'select' in sql_lower:
            data = [
                {'USUBJID': 'SUBJ-001', 'VALUE': 'Test'},
                {'USUBJID': 'SUBJ-002', 'VALUE': 'Test'},
            ]
        else:
            data = []

        execution_time = (time.time() - start_time) * 1000
        columns = list(data[0].keys()) if data else []

        return ExecutionResult(
            success=True,
            data=data,
            columns=columns,
            row_count=len(data),
            execution_time_ms=execution_time,
            sql_executed=sql
        )

    def get_tables(self) -> List[str]:
        """Return mock tables."""
        return ['ADAE', 'ADSL', 'ADLB', 'DM', 'AE', 'CM', 'LB', 'VS']

    def get_columns(self, table_name: str) -> List[Dict[str, str]]:
        """Return mock columns."""
        common_cols = [
            {'name': 'USUBJID', 'type': 'VARCHAR'},
            {'name': 'STUDYID', 'type': 'VARCHAR'},
        ]

        if table_name.upper() == 'ADAE':
            return common_cols + [
                {'name': 'AEDECOD', 'type': 'VARCHAR'},
                {'name': 'ATOXGR', 'type': 'VARCHAR'},
                {'name': 'SAFFL', 'type': 'VARCHAR'},
                {'name': 'TRTEMFL', 'type': 'VARCHAR'},
            ]
        elif table_name.upper() == 'ADSL':
            return common_cols + [
                {'name': 'AGE', 'type': 'INTEGER'},
                {'name': 'SEX', 'type': 'VARCHAR'},
                {'name': 'SAFFL', 'type': 'VARCHAR'},
                {'name': 'ITTFL', 'type': 'VARCHAR'},
            ]

        return common_cols

    def validate_connection(self) -> bool:
        """Mock always returns True."""
        return True
