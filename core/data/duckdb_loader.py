# SAGE - DuckDB Loader Module
# ============================
# Loads processed clinical data into DuckDB with validation
"""
DuckDB database loader for clinical trial data with support for:
- Automatic table creation from DataFrames
- Schema validation and type mapping
- Data quality validation
- Incremental updates and versioning
- Query optimization hints
"""

import os
import logging
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Union
from dataclasses import dataclass, field
from datetime import datetime
import json

import pandas as pd
import duckdb

logger = logging.getLogger(__name__)


@dataclass
class TableInfo:
    """Information about a loaded table."""
    name: str
    row_count: int
    column_count: int
    columns: List[Dict[str, str]]
    created_at: datetime
    source_file: Optional[str] = None
    checksum: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'row_count': self.row_count,
            'column_count': self.column_count,
            'columns': self.columns,
            'created_at': self.created_at.isoformat(),
            'source_file': self.source_file,
            'checksum': self.checksum
        }


@dataclass
class LoadResult:
    """Result of a data load operation."""
    success: bool
    table_name: str
    rows_loaded: int
    error: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    duration_seconds: float = 0.0


@dataclass
class ValidationResult:
    """Result of data validation."""
    is_valid: bool
    table_name: str
    checks: List[Dict[str, Any]] = field(default_factory=list)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class DuckDBLoader:
    """
    Loader for clinical data into DuckDB.

    Features:
    - Automatic schema inference and creation
    - Data type optimization
    - Validation checks (row counts, nulls, types)
    - Metadata tracking
    - Query interface

    Example:
        loader = DuckDBLoader('data/clinical.duckdb')

        # Load a dataframe
        result = loader.load_dataframe(df, 'DM')

        # Query data
        result = loader.query("SELECT COUNT(*) FROM DM")

        # Get table info
        info = loader.get_table_info('DM')
    """

    # Type mapping from pandas to DuckDB
    DTYPE_MAP = {
        'int64': 'BIGINT',
        'int32': 'INTEGER',
        'int16': 'SMALLINT',
        'int8': 'TINYINT',
        'float64': 'DOUBLE',
        'float32': 'FLOAT',
        'bool': 'BOOLEAN',
        'object': 'VARCHAR',
        'string': 'VARCHAR',
        'category': 'VARCHAR',
        'datetime64[ns]': 'TIMESTAMP',
        'date': 'DATE',
    }

    def __init__(self, db_path: str, read_only: bool = False):
        """
        Initialize DuckDB loader.

        Args:
            db_path: Path to DuckDB database file
            read_only: Whether to open in read-only mode
        """
        self.db_path = Path(db_path)
        self.read_only = read_only

        # Ensure directory exists
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize connection
        self._conn = None
        self._connect()

        # Create metadata table if not exists
        if not read_only:
            self._init_metadata_table()

    def _connect(self):
        """Establish database connection."""
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass

        self._conn = duckdb.connect(str(self.db_path), read_only=self.read_only)
        logger.info(f"Connected to DuckDB: {self.db_path}")

    def _init_metadata_table(self):
        """Create metadata tracking table."""
        self._conn.execute("""
            CREATE TABLE IF NOT EXISTS _sage_metadata (
                table_name VARCHAR PRIMARY KEY,
                source_file VARCHAR,
                row_count BIGINT,
                column_count INTEGER,
                columns JSON,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                checksum VARCHAR,
                version INTEGER DEFAULT 1
            )
        """)

    def close(self):
        """Close database connection."""
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def load_dataframe(self, df: pd.DataFrame, table_name: str,
                       source_file: Optional[str] = None,
                       if_exists: str = 'replace',
                       validate: bool = True) -> LoadResult:
        """
        Load a pandas DataFrame into DuckDB.

        Args:
            df: DataFrame to load
            table_name: Name of the target table
            source_file: Original source file path (for metadata)
            if_exists: 'replace', 'append', or 'fail'
            validate: Whether to validate after loading

        Returns:
            LoadResult with status and details
        """
        start_time = datetime.now()
        table_name = table_name.upper()
        warnings = []

        try:
            # Check if table exists
            existing = self._table_exists(table_name)

            if existing and if_exists == 'fail':
                return LoadResult(
                    success=False,
                    table_name=table_name,
                    rows_loaded=0,
                    error=f"Table '{table_name}' already exists"
                )

            # Optimize datatypes
            df = self._optimize_for_duckdb(df)

            # Load data
            if if_exists == 'replace' or not existing:
                self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self._conn.execute(f"CREATE TABLE {table_name} AS SELECT * FROM df")
            elif if_exists == 'append':
                self._conn.execute(f"INSERT INTO {table_name} SELECT * FROM df")

            # Update metadata
            self._update_metadata(table_name, df, source_file)

            # Validate if requested
            if validate:
                validation = self.validate_table(table_name, expected_rows=len(df))
                if not validation.is_valid:
                    warnings.extend(validation.warnings)
                    warnings.extend(validation.errors)

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"Loaded {len(df)} rows into {table_name} in {duration:.2f}s")

            return LoadResult(
                success=True,
                table_name=table_name,
                rows_loaded=len(df),
                warnings=warnings,
                duration_seconds=duration
            )

        except Exception as e:
            logger.error(f"Failed to load table {table_name}: {e}")
            return LoadResult(
                success=False,
                table_name=table_name,
                rows_loaded=0,
                error=str(e)
            )

    def _optimize_for_duckdb(self, df: pd.DataFrame) -> pd.DataFrame:
        """Optimize DataFrame for DuckDB loading."""
        df = df.copy()

        for col in df.columns:
            # Convert category to string for DuckDB
            if df[col].dtype.name == 'category':
                df[col] = df[col].astype(str)

            # Ensure datetime columns are proper type
            if 'datetime' in str(df[col].dtype):
                df[col] = pd.to_datetime(df[col], errors='coerce')

        return df

    def _table_exists(self, table_name: str) -> bool:
        """Check if a table exists."""
        result = self._conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables
            WHERE table_name = ?
        """, [table_name.upper()]).fetchone()
        return result[0] > 0

    def _update_metadata(self, table_name: str, df: pd.DataFrame,
                        source_file: Optional[str] = None):
        """Update metadata for a loaded table."""
        columns = [
            {'name': col, 'dtype': str(df[col].dtype)}
            for col in df.columns
        ]
        columns_json = json.dumps(columns)
        now = datetime.now()

        # Check if metadata exists
        existing = self._conn.execute(
            "SELECT version FROM _sage_metadata WHERE table_name = ?",
            [table_name]
        ).fetchone()

        if existing:
            version = existing[0] + 1
            self._conn.execute("""
                UPDATE _sage_metadata SET
                    source_file = ?,
                    row_count = ?,
                    column_count = ?,
                    columns = ?,
                    updated_at = ?,
                    version = ?
                WHERE table_name = ?
            """, [source_file, len(df), len(df.columns), columns_json,
                  now, version, table_name])
        else:
            self._conn.execute("""
                INSERT INTO _sage_metadata
                (table_name, source_file, row_count, column_count, columns, created_at, updated_at, version)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1)
            """, [table_name, source_file, len(df), len(df.columns),
                  columns_json, now, now])

    def load_parquet(self, parquet_path: str, table_name: str,
                    if_exists: str = 'replace') -> LoadResult:
        """
        Load a Parquet file directly into DuckDB.

        Args:
            parquet_path: Path to Parquet file
            table_name: Name of the target table
            if_exists: 'replace', 'append', or 'fail'

        Returns:
            LoadResult with status and details
        """
        start_time = datetime.now()
        table_name = table_name.upper()

        try:
            if not Path(parquet_path).exists():
                return LoadResult(
                    success=False,
                    table_name=table_name,
                    rows_loaded=0,
                    error=f"File not found: {parquet_path}"
                )

            existing = self._table_exists(table_name)

            if existing and if_exists == 'fail':
                return LoadResult(
                    success=False,
                    table_name=table_name,
                    rows_loaded=0,
                    error=f"Table '{table_name}' already exists"
                )

            if if_exists == 'replace' or not existing:
                self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
                self._conn.execute(f"""
                    CREATE TABLE {table_name} AS
                    SELECT * FROM read_parquet('{parquet_path}')
                """)
            elif if_exists == 'append':
                self._conn.execute(f"""
                    INSERT INTO {table_name}
                    SELECT * FROM read_parquet('{parquet_path}')
                """)

            # Get row count
            row_count = self._conn.execute(
                f"SELECT COUNT(*) FROM {table_name}"
            ).fetchone()[0]

            # Update metadata
            df_sample = self._conn.execute(
                f"SELECT * FROM {table_name} LIMIT 1"
            ).fetchdf()
            self._update_metadata(table_name, df_sample, parquet_path)

            duration = (datetime.now() - start_time).total_seconds()

            logger.info(f"Loaded {row_count} rows from Parquet into {table_name}")

            return LoadResult(
                success=True,
                table_name=table_name,
                rows_loaded=row_count,
                duration_seconds=duration
            )

        except Exception as e:
            logger.error(f"Failed to load Parquet: {e}")
            return LoadResult(
                success=False,
                table_name=table_name,
                rows_loaded=0,
                error=str(e)
            )

    def validate_table(self, table_name: str,
                      expected_rows: Optional[int] = None) -> ValidationResult:
        """
        Validate a loaded table.

        Args:
            table_name: Table to validate
            expected_rows: Expected row count (optional)

        Returns:
            ValidationResult with check details
        """
        table_name = table_name.upper()
        checks = []
        errors = []
        warnings = []

        # Check table exists
        if not self._table_exists(table_name):
            return ValidationResult(
                is_valid=False,
                table_name=table_name,
                errors=[f"Table '{table_name}' does not exist"]
            )

        # Row count check
        actual_rows = self._conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        checks.append({
            'check': 'row_count',
            'actual': actual_rows,
            'expected': expected_rows,
            'passed': expected_rows is None or actual_rows == expected_rows
        })

        if expected_rows is not None and actual_rows != expected_rows:
            errors.append(f"Row count mismatch: expected {expected_rows}, got {actual_rows}")

        # Column info
        columns = self._conn.execute(f"""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """).fetchall()

        checks.append({
            'check': 'column_count',
            'count': len(columns),
            'passed': len(columns) > 0
        })

        # Null checks for each column
        for col_name, dtype, nullable in columns:
            null_count = self._conn.execute(
                f'SELECT COUNT(*) FROM {table_name} WHERE "{col_name}" IS NULL'
            ).fetchone()[0]

            null_pct = (null_count / actual_rows * 100) if actual_rows > 0 else 0

            checks.append({
                'check': 'null_count',
                'column': col_name,
                'null_count': null_count,
                'null_percent': round(null_pct, 2),
                'passed': True  # Informational
            })

            if null_pct > 50:
                warnings.append(f"Column '{col_name}' has {null_pct:.1f}% null values")

        is_valid = len(errors) == 0

        return ValidationResult(
            is_valid=is_valid,
            table_name=table_name,
            checks=checks,
            errors=errors,
            warnings=warnings
        )

    def query(self, sql: str) -> pd.DataFrame:
        """
        Execute a SQL query and return results as DataFrame.

        Args:
            sql: SQL query string

        Returns:
            DataFrame with query results
        """
        return self._conn.execute(sql).fetchdf()

    def execute(self, sql: str) -> Any:
        """Execute a SQL statement."""
        return self._conn.execute(sql)

    def get_table_info(self, table_name: str) -> Optional[TableInfo]:
        """
        Get information about a table.

        Args:
            table_name: Table name

        Returns:
            TableInfo or None if table doesn't exist
        """
        table_name = table_name.upper()

        if not self._table_exists(table_name):
            return None

        # Get from metadata table
        meta = self._conn.execute("""
            SELECT source_file, row_count, column_count, columns, created_at, checksum
            FROM _sage_metadata WHERE table_name = ?
        """, [table_name]).fetchone()

        if meta:
            return TableInfo(
                name=table_name,
                row_count=meta[1],
                column_count=meta[2],
                columns=json.loads(meta[3]) if meta[3] else [],
                created_at=meta[4],
                source_file=meta[0],
                checksum=meta[5]
            )

        # Fallback to direct query
        row_count = self._conn.execute(
            f"SELECT COUNT(*) FROM {table_name}"
        ).fetchone()[0]

        columns = self._conn.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table_name}'
        """).fetchall()

        return TableInfo(
            name=table_name,
            row_count=row_count,
            column_count=len(columns),
            columns=[{'name': c[0], 'dtype': c[1]} for c in columns],
            created_at=datetime.now()
        )

    def list_tables(self) -> List[str]:
        """List all user tables in the database."""
        result = self._conn.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'main'
            AND table_name NOT LIKE '_sage_%'
            ORDER BY table_name
        """).fetchall()
        return [r[0] for r in result]

    def get_all_table_info(self) -> List[TableInfo]:
        """Get information about all tables."""
        tables = self.list_tables()
        return [self.get_table_info(t) for t in tables if self.get_table_info(t)]

    def drop_table(self, table_name: str) -> bool:
        """
        Drop a table from the database.

        Args:
            table_name: Table to drop

        Returns:
            True if successful
        """
        table_name = table_name.upper()
        try:
            self._conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            self._conn.execute(
                "DELETE FROM _sage_metadata WHERE table_name = ?",
                [table_name]
            )
            logger.info(f"Dropped table: {table_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to drop table {table_name}: {e}")
            return False

    def get_sample_data(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        """Get sample rows from a table."""
        table_name = table_name.upper()
        return self.query(f"SELECT * FROM {table_name} LIMIT {limit}")

    def get_column_statistics(self, table_name: str, column_name: str) -> Dict[str, Any]:
        """
        Get statistics for a specific column.

        Args:
            table_name: Table name
            column_name: Column name

        Returns:
            Dictionary with column statistics
        """
        table_name = table_name.upper()

        # Get basic stats
        result = self._conn.execute(f"""
            SELECT
                COUNT(*) as total,
                COUNT("{column_name}") as non_null,
                COUNT(DISTINCT "{column_name}") as unique_values
            FROM {table_name}
        """).fetchone()

        stats = {
            'column': column_name,
            'total_rows': result[0],
            'non_null_count': result[1],
            'null_count': result[0] - result[1],
            'unique_count': result[2]
        }

        # Try to get numeric stats
        try:
            num_result = self._conn.execute(f"""
                SELECT
                    MIN("{column_name}") as min_val,
                    MAX("{column_name}") as max_val,
                    AVG("{column_name}") as avg_val
                FROM {table_name}
            """).fetchone()
            stats['min'] = num_result[0]
            stats['max'] = num_result[1]
            stats['mean'] = num_result[2]
        except Exception:
            pass  # Not numeric

        # Get sample values
        sample = self._conn.execute(f"""
            SELECT DISTINCT "{column_name}"
            FROM {table_name}
            WHERE "{column_name}" IS NOT NULL
            LIMIT 10
        """).fetchall()
        stats['sample_values'] = [str(r[0]) for r in sample]

        return stats

    def export_to_parquet(self, table_name: str, output_path: str) -> bool:
        """
        Export a table to Parquet format.

        Args:
            table_name: Table to export
            output_path: Output file path

        Returns:
            True if successful
        """
        table_name = table_name.upper()
        try:
            self._conn.execute(f"""
                COPY {table_name} TO '{output_path}' (FORMAT PARQUET)
            """)
            logger.info(f"Exported {table_name} to {output_path}")
            return True
        except Exception as e:
            logger.error(f"Failed to export {table_name}: {e}")
            return False
