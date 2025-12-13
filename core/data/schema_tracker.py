# SAGE - Schema Tracker Module
# =============================
# Version control for data schemas with change detection
"""
Schema tracking and version control for data tables:
- Track schema versions for each table
- Detect schema changes between uploads
- Calculate schema diffs (added/removed/changed columns)
- Block or warn on breaking changes
- Store version history in SQLite
"""

import os
import sqlite3
import json
import hashlib
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from contextlib import contextmanager
from enum import Enum

import pandas as pd

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Types of schema changes."""
    ADDED = "added"
    REMOVED = "removed"
    TYPE_CHANGED = "type_changed"
    COMPATIBLE_TYPE_CHANGE = "compatible_type_change"
    NONE = "none"


class ChangeSeverity(Enum):
    """Severity of schema changes."""
    NONE = "none"
    INFO = "info"           # New columns added
    WARNING = "warning"     # Compatible type changes
    BREAKING = "breaking"   # Columns removed or incompatible type changes


@dataclass
class ColumnChange:
    """Represents a change to a column."""
    column_name: str
    change_type: ChangeType
    old_dtype: Optional[str] = None
    new_dtype: Optional[str] = None
    severity: ChangeSeverity = ChangeSeverity.INFO

    def to_dict(self) -> Dict[str, Any]:
        return {
            'column_name': self.column_name,
            'change_type': self.change_type.value,
            'old_dtype': self.old_dtype,
            'new_dtype': self.new_dtype,
            'severity': self.severity.value
        }


@dataclass
class SchemaDiff:
    """Result of comparing two schemas."""
    has_changes: bool
    added_columns: List[ColumnChange]
    removed_columns: List[ColumnChange]
    type_changes: List[ColumnChange]
    severity: ChangeSeverity
    old_row_count: int
    new_row_count: int
    row_count_change: int

    def to_dict(self) -> Dict[str, Any]:
        return {
            'has_changes': self.has_changes,
            'added_columns': [c.to_dict() for c in self.added_columns],
            'removed_columns': [c.to_dict() for c in self.removed_columns],
            'type_changes': [c.to_dict() for c in self.type_changes],
            'severity': self.severity.value,
            'old_row_count': self.old_row_count,
            'new_row_count': self.new_row_count,
            'row_count_change': self.row_count_change
        }

    def get_summary(self) -> str:
        """Get a human-readable summary of changes."""
        if not self.has_changes:
            return "No schema changes detected."

        parts = []
        if self.added_columns:
            parts.append(f"{len(self.added_columns)} column(s) added")
        if self.removed_columns:
            parts.append(f"{len(self.removed_columns)} column(s) removed")
        if self.type_changes:
            parts.append(f"{len(self.type_changes)} type change(s)")
        if self.row_count_change != 0:
            sign = "+" if self.row_count_change > 0 else ""
            parts.append(f"rows: {sign}{self.row_count_change}")

        return ", ".join(parts)


@dataclass
class SchemaVersion:
    """A version of a table schema."""
    version_id: str
    table_name: str
    version_number: int
    source_file: str
    source_format: str
    row_count: int
    column_count: int
    schema_json: Dict[str, Any]
    schema_hash: str
    created_at: datetime
    created_by: str
    is_current: bool
    change_summary: Optional[str] = None

    @property
    def version(self) -> int:
        """Alias for version_number for convenience."""
        return self.version_number

    @property
    def columns(self) -> List[Dict[str, Any]]:
        """Get columns from schema_json."""
        return self.schema_json.get('columns', [])

    def to_dict(self) -> Dict[str, Any]:
        return {
            'version_id': self.version_id,
            'table_name': self.table_name,
            'version_number': self.version_number,
            'source_file': self.source_file,
            'source_format': self.source_format,
            'row_count': self.row_count,
            'column_count': self.column_count,
            'schema_json': self.schema_json,
            'schema_hash': self.schema_hash,
            'created_at': self.created_at.isoformat(),
            'created_by': self.created_by,
            'is_current': self.is_current,
            'change_summary': self.change_summary
        }


class SchemaTracker:
    """
    Tracks schema versions and detects changes for data tables.

    Features:
    - Store schema history in SQLite
    - Compare schemas between versions
    - Detect breaking changes
    - Provide detailed change diffs

    Example:
        tracker = SchemaTracker('data/schema_versions.db')

        # Record a new schema version
        version = tracker.record_version(
            table_name='DM',
            df=dataframe,
            source_file='dm.sas7bdat',
            source_format='sas7bdat'
        )

        # Compare with previous version
        diff = tracker.compare_with_previous('DM', dataframe)
        if diff.severity == ChangeSeverity.BREAKING:
            print("Breaking changes detected!")
    """

    # Type compatibility matrix (old_type -> new_type -> is_compatible)
    TYPE_COMPATIBILITY = {
        ('int64', 'float64'): True,     # int to float is safe
        ('int32', 'int64'): True,       # smaller to larger int is safe
        ('int32', 'float64'): True,
        ('float32', 'float64'): True,   # smaller to larger float is safe
        ('object', 'category'): True,   # string to category is safe
        ('category', 'object'): True,   # category to string is safe
    }

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize schema tracker.

        Args:
            db_path: Path to SQLite database. Defaults to knowledge/schema_versions.db
        """
        if db_path is None:
            project_root = Path(__file__).parent.parent.parent
            db_path = project_root / "knowledge" / "schema_versions.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        self._init_database()

    @contextmanager
    def _get_connection(self):
        """Context manager for database connections."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def _init_database(self):
        """Initialize the database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            cursor.execute('''
                CREATE TABLE IF NOT EXISTS schema_versions (
                    version_id TEXT PRIMARY KEY,
                    table_name TEXT NOT NULL,
                    version_number INTEGER NOT NULL,
                    source_file TEXT,
                    source_format TEXT,
                    row_count INTEGER,
                    column_count INTEGER,
                    schema_json TEXT,
                    schema_hash TEXT,
                    created_at TEXT,
                    created_by TEXT DEFAULT 'system',
                    is_current INTEGER DEFAULT 1,
                    change_summary TEXT
                )
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_schema_table
                ON schema_versions(table_name)
            ''')

            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_schema_current
                ON schema_versions(table_name, is_current)
            ''')

    def calculate_schema_hash(self, df: pd.DataFrame) -> str:
        """Calculate a hash of the DataFrame schema."""
        schema_str = '|'.join([
            f"{col}:{str(df[col].dtype)}"
            for col in sorted(df.columns)
        ])
        return hashlib.sha256(schema_str.encode()).hexdigest()[:16]

    def extract_schema(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Extract schema information from a DataFrame."""
        columns = []
        for col in df.columns:
            col_info = {
                'name': col,
                'dtype': str(df[col].dtype),
                'nullable': bool(df[col].isnull().any())
            }
            columns.append(col_info)

        return {
            'columns': columns,
            'column_count': len(df.columns),
            'row_count': len(df)
        }

    def get_current_version(self, table_name: str) -> Optional[SchemaVersion]:
        """Get the current schema version for a table."""
        table_name = table_name.upper()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM schema_versions
                WHERE table_name = ? AND is_current = 1
                ORDER BY version_number DESC
                LIMIT 1
            ''', (table_name,))

            row = cursor.fetchone()
            if not row:
                return None

            return self._row_to_version(row)

    def get_version_history(self, table_name: str, limit: int = 10) -> List[SchemaVersion]:
        """Get version history for a table."""
        table_name = table_name.upper()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM schema_versions
                WHERE table_name = ?
                ORDER BY version_number DESC
                LIMIT ?
            ''', (table_name, limit))

            return [self._row_to_version(row) for row in cursor.fetchall()]

    def _row_to_version(self, row) -> SchemaVersion:
        """Convert database row to SchemaVersion."""
        return SchemaVersion(
            version_id=row['version_id'],
            table_name=row['table_name'],
            version_number=row['version_number'],
            source_file=row['source_file'],
            source_format=row['source_format'],
            row_count=row['row_count'],
            column_count=row['column_count'],
            schema_json=json.loads(row['schema_json']) if row['schema_json'] else {},
            schema_hash=row['schema_hash'],
            created_at=datetime.fromisoformat(row['created_at']) if row['created_at'] else datetime.now(),
            created_by=row['created_by'],
            is_current=bool(row['is_current']),
            change_summary=row['change_summary']
        )

    def compare_schemas(self,
                       old_schema: Dict[str, Any],
                       new_schema: Dict[str, Any],
                       old_row_count: int = 0,
                       new_row_count: int = 0) -> SchemaDiff:
        """
        Compare two schemas and return the differences.

        Args:
            old_schema: Previous schema (from schema_json)
            new_schema: New schema (from extract_schema)
            old_row_count: Previous row count
            new_row_count: New row count

        Returns:
            SchemaDiff with detailed change information
        """
        old_columns = {c['name']: c for c in old_schema.get('columns', [])}
        new_columns = {c['name']: c for c in new_schema.get('columns', [])}

        old_names = set(old_columns.keys())
        new_names = set(new_columns.keys())

        added = new_names - old_names
        removed = old_names - new_names
        common = old_names & new_names

        added_changes = [
            ColumnChange(
                column_name=name,
                change_type=ChangeType.ADDED,
                new_dtype=new_columns[name]['dtype'],
                severity=ChangeSeverity.INFO
            )
            for name in sorted(added)
        ]

        removed_changes = [
            ColumnChange(
                column_name=name,
                change_type=ChangeType.REMOVED,
                old_dtype=old_columns[name]['dtype'],
                severity=ChangeSeverity.BREAKING
            )
            for name in sorted(removed)
        ]

        type_changes = []
        for name in common:
            old_dtype = old_columns[name]['dtype']
            new_dtype = new_columns[name]['dtype']

            if old_dtype != new_dtype:
                is_compatible = self._is_type_compatible(old_dtype, new_dtype)
                type_changes.append(ColumnChange(
                    column_name=name,
                    change_type=ChangeType.COMPATIBLE_TYPE_CHANGE if is_compatible else ChangeType.TYPE_CHANGED,
                    old_dtype=old_dtype,
                    new_dtype=new_dtype,
                    severity=ChangeSeverity.WARNING if is_compatible else ChangeSeverity.BREAKING
                ))

        # Determine overall severity
        severity = ChangeSeverity.NONE
        if added_changes:
            severity = ChangeSeverity.INFO
        if any(c.severity == ChangeSeverity.WARNING for c in type_changes):
            severity = ChangeSeverity.WARNING
        if removed_changes or any(c.severity == ChangeSeverity.BREAKING for c in type_changes):
            severity = ChangeSeverity.BREAKING

        has_changes = bool(added_changes or removed_changes or type_changes)

        return SchemaDiff(
            has_changes=has_changes,
            added_columns=added_changes,
            removed_columns=removed_changes,
            type_changes=type_changes,
            severity=severity,
            old_row_count=old_row_count,
            new_row_count=new_row_count,
            row_count_change=new_row_count - old_row_count
        )

    def _is_type_compatible(self, old_dtype: str, new_dtype: str) -> bool:
        """Check if a type change is compatible (non-breaking)."""
        # Direct match
        if old_dtype == new_dtype:
            return True

        # Check compatibility matrix
        return self.TYPE_COMPATIBILITY.get((old_dtype, new_dtype), False)

    def compare_with_previous(self, table_name: str, new_df: pd.DataFrame) -> SchemaDiff:
        """
        Compare a new DataFrame with the current version.

        Args:
            table_name: Table name to compare
            new_df: New DataFrame

        Returns:
            SchemaDiff with changes from current version
        """
        table_name = table_name.upper()
        current = self.get_current_version(table_name)

        new_schema = self.extract_schema(new_df)

        if not current:
            # No previous version - everything is new
            return SchemaDiff(
                has_changes=True,
                added_columns=[
                    ColumnChange(
                        column_name=c['name'],
                        change_type=ChangeType.ADDED,
                        new_dtype=c['dtype'],
                        severity=ChangeSeverity.INFO
                    )
                    for c in new_schema['columns']
                ],
                removed_columns=[],
                type_changes=[],
                severity=ChangeSeverity.INFO,
                old_row_count=0,
                new_row_count=len(new_df),
                row_count_change=len(new_df)
            )

        return self.compare_schemas(
            old_schema=current.schema_json,
            new_schema=new_schema,
            old_row_count=current.row_count,
            new_row_count=len(new_df)
        )

    def record_version(self,
                      table_name: str,
                      df: pd.DataFrame,
                      source_file: str,
                      source_format: Optional[str] = None,
                      user: str = 'system',
                      change_summary: Optional[str] = None,
                      notes: Optional[str] = None) -> SchemaVersion:
        """
        Record a new schema version for a table.

        Args:
            table_name: Table name
            df: DataFrame with the data
            source_file: Source file path
            source_format: Format of the source file (auto-detected if not provided)
            user: User who created this version
            change_summary: Optional summary of changes
            notes: Optional notes about this version

        Returns:
            The created SchemaVersion
        """
        table_name = table_name.upper()

        # Auto-detect format from source_file if not provided
        if source_format is None:
            ext = os.path.splitext(source_file)[1].lower()
            format_map = {
                '.sas7bdat': 'sas7bdat',
                '.parquet': 'parquet',
                '.csv': 'csv',
                '.xpt': 'xpt'
            }
            source_format = format_map.get(ext, 'unknown')

        # Get current version number
        current = self.get_current_version(table_name)
        version_number = (current.version_number + 1) if current else 1

        # Extract schema
        schema = self.extract_schema(df)
        schema_hash = self.calculate_schema_hash(df)

        # Generate change summary if not provided
        if not change_summary and current:
            diff = self.compare_schemas(
                current.schema_json, schema,
                current.row_count, len(df)
            )
            change_summary = diff.get_summary()

        # Generate version ID
        version_id = f"{table_name}_v{version_number}_{datetime.now().strftime('%Y%m%d%H%M%S')}"

        now = datetime.now().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Mark previous versions as not current
            cursor.execute('''
                UPDATE schema_versions
                SET is_current = 0
                WHERE table_name = ?
            ''', (table_name,))

            # Insert new version
            cursor.execute('''
                INSERT INTO schema_versions
                (version_id, table_name, version_number, source_file, source_format,
                 row_count, column_count, schema_json, schema_hash, created_at,
                 created_by, is_current, change_summary)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?)
            ''', (
                version_id, table_name, version_number, source_file, source_format,
                len(df), len(df.columns), json.dumps(schema), schema_hash, now,
                user, change_summary
            ))

        logger.info(f"Recorded schema version {version_number} for {table_name}")

        return SchemaVersion(
            version_id=version_id,
            table_name=table_name,
            version_number=version_number,
            source_file=source_file,
            source_format=source_format,
            row_count=len(df),
            column_count=len(df.columns),
            schema_json=schema,
            schema_hash=schema_hash,
            created_at=datetime.fromisoformat(now),
            created_by=user,
            is_current=True,
            change_summary=change_summary
        )

    def get_all_tables(self) -> List[Dict[str, Any]]:
        """Get summary of all tracked tables."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT table_name,
                       MAX(version_number) as latest_version,
                       MAX(row_count) as row_count,
                       MAX(column_count) as column_count,
                       MAX(created_at) as last_updated
                FROM schema_versions
                WHERE is_current = 1
                GROUP BY table_name
                ORDER BY table_name
            ''')

            return [
                {
                    'table_name': row['table_name'],
                    'latest_version': row['latest_version'],
                    'row_count': row['row_count'],
                    'column_count': row['column_count'],
                    'last_updated': row['last_updated']
                }
                for row in cursor.fetchall()
            ]

    def list_tables(self) -> List[str]:
        """Get list of all tracked table names."""
        tables = self.get_all_tables()
        return [t['table_name'] for t in tables]

    def delete_table_history(self, table_name: str) -> bool:
        """Delete all version history for a table."""
        table_name = table_name.upper()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM schema_versions WHERE table_name = ?
            ''', (table_name,))

            deleted = cursor.rowcount
            logger.info(f"Deleted {deleted} version records for {table_name}")
            return deleted > 0

    def should_block_upload(self, table_name: str, df: pd.DataFrame,
                           block_on_breaking: bool = True) -> Tuple[bool, Optional[SchemaDiff]]:
        """
        Check if an upload should be blocked due to breaking changes.

        Args:
            table_name: Table name
            df: New DataFrame to upload
            block_on_breaking: Whether to block on breaking changes

        Returns:
            Tuple of (should_block, schema_diff)
        """
        diff = self.compare_with_previous(table_name, df)

        if not diff.has_changes:
            return False, diff

        if block_on_breaking and diff.severity == ChangeSeverity.BREAKING:
            return True, diff

        return False, diff

    def get_version(self, table_name: str, version_number: int) -> Optional[SchemaVersion]:
        """Get a specific schema version for a table."""
        table_name = table_name.upper()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM schema_versions
                WHERE table_name = ? AND version_number = ?
            ''', (table_name, version_number))

            row = cursor.fetchone()
            if not row:
                return None

            return self._row_to_version(row)

    def rollback_to_version(self, table_name: str, target_version: int) -> Tuple[bool, str]:
        """
        Rollback a table's current schema version to a previous version.

        This marks the target version as current and unmarks all other versions.
        Note: This only changes schema metadata - it doesn't recreate the actual table.
        The table structure would need to be recreated from a file with the old schema.

        Args:
            table_name: Table name
            target_version: Version number to rollback to

        Returns:
            Tuple of (success, message)
        """
        table_name = table_name.upper()

        # Check target version exists
        target = self.get_version(table_name, target_version)
        if not target:
            return False, f"Version {target_version} not found for table {table_name}"

        # Get current version for the message
        current = self.get_current_version(table_name)
        if current and current.version == target_version:
            return False, f"Version {target_version} is already the current version"

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Unmark all versions as current
            cursor.execute('''
                UPDATE schema_versions
                SET is_current = 0
                WHERE table_name = ?
            ''', (table_name,))

            # Mark target version as current
            cursor.execute('''
                UPDATE schema_versions
                SET is_current = 1
                WHERE table_name = ? AND version_number = ?
            ''', (table_name, target_version))

            logger.info(f"Rolled back {table_name} from v{current.version if current else '?'} to v{target_version}")
            return True, f"Successfully rolled back to version {target_version}"
