# SAGE File Store
# ================
# Persistent file status tracking for Data Foundry
"""
Track uploaded files, processing status, and history.

This module provides:
- FileStatus: Current status of a file in the pipeline
- FileRecord: Complete record of a file upload
- FileStore: SQLite-based persistent storage for file tracking
"""

import sqlite3
import json
import os
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field, asdict
from typing import Optional, List, Dict, Any
from pathlib import Path


class FileStatus(str, Enum):
    """Status of a file in the processing pipeline."""
    PENDING = "pending"           # Uploaded, waiting to process
    VALIDATING = "validating"     # Checking file integrity
    READING = "reading"           # Reading file contents
    TRANSFORMING = "transforming" # Applying transformations
    LOADING = "loading"           # Loading to DuckDB
    COMPLETED = "completed"       # Successfully processed
    FAILED = "failed"             # Processing failed
    ARCHIVED = "archived"         # Replaced by newer version


@dataclass
class ProcessingStep:
    """A single step in file processing."""
    step_name: str
    status: str  # "pending", "running", "completed", "failed"
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None


@dataclass
class FileRecord:
    """Complete record of a file upload and processing."""
    id: str                              # Unique identifier (UUID)
    filename: str                        # Original filename
    table_name: str                      # Target table name (e.g., "DM")
    file_format: str                     # Format (sas7bdat, parquet, csv, xpt)
    file_size: int                       # Size in bytes
    file_hash: str                       # SHA256 hash of file content
    schema_hash: Optional[str] = None    # Hash of the schema
    status: FileStatus = FileStatus.PENDING
    uploaded_at: str = field(default_factory=lambda: datetime.now().isoformat())
    processed_at: Optional[str] = None
    row_count: Optional[int] = None
    column_count: Optional[int] = None
    schema_version: Optional[int] = None
    error_message: Optional[str] = None
    processing_steps: List[ProcessingStep] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        d = asdict(self)
        d['status'] = self.status.value
        d['processing_steps'] = [asdict(s) for s in self.processing_steps]
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'FileRecord':
        """Create from dictionary."""
        data['status'] = FileStatus(data['status'])
        data['processing_steps'] = [ProcessingStep(**s) for s in data.get('processing_steps', [])]
        return cls(**data)


class FileStore:
    """
    SQLite-based persistent storage for file tracking.

    Features:
    - Track all uploaded files and their status
    - Query file history by table name
    - Get processing statistics
    - Support for file versioning
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the file store.

        Args:
            db_path: Path to SQLite database. Defaults to knowledge/file_store.db
        """
        if db_path is None:
            # Default path relative to project root
            base_dir = Path(__file__).parent.parent.parent
            db_path = str(base_dir / "knowledge" / "file_store.db")

        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS files (
                    id TEXT PRIMARY KEY,
                    filename TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    file_format TEXT NOT NULL,
                    file_size INTEGER NOT NULL,
                    file_hash TEXT NOT NULL,
                    schema_hash TEXT,
                    status TEXT NOT NULL,
                    uploaded_at TEXT NOT NULL,
                    processed_at TEXT,
                    row_count INTEGER,
                    column_count INTEGER,
                    schema_version INTEGER,
                    error_message TEXT,
                    processing_steps TEXT,  -- JSON array
                    metadata TEXT,          -- JSON object
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Indexes for common queries
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_table_name ON files(table_name)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_status ON files(status)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_uploaded_at ON files(uploaded_at)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_files_file_hash ON files(file_hash)")

            # Trigger to update updated_at
            conn.execute("""
                CREATE TRIGGER IF NOT EXISTS update_files_timestamp
                AFTER UPDATE ON files
                BEGIN
                    UPDATE files SET updated_at = CURRENT_TIMESTAMP WHERE id = NEW.id;
                END
            """)

            conn.commit()

    def save(self, record: FileRecord) -> FileRecord:
        """
        Save or update a file record.

        Args:
            record: FileRecord to save

        Returns:
            The saved FileRecord
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT OR REPLACE INTO files (
                    id, filename, table_name, file_format, file_size, file_hash,
                    schema_hash, status, uploaded_at, processed_at, row_count,
                    column_count, schema_version, error_message, processing_steps, metadata
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                record.id,
                record.filename,
                record.table_name,
                record.file_format,
                record.file_size,
                record.file_hash,
                record.schema_hash,
                record.status.value,
                record.uploaded_at,
                record.processed_at,
                record.row_count,
                record.column_count,
                record.schema_version,
                record.error_message,
                json.dumps([asdict(s) for s in record.processing_steps]),
                json.dumps(record.metadata)
            ))
            conn.commit()

        return record

    def get(self, file_id: str) -> Optional[FileRecord]:
        """
        Get a file record by ID.

        Args:
            file_id: The file's unique identifier

        Returns:
            FileRecord if found, None otherwise
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("SELECT * FROM files WHERE id = ?", (file_id,))
            row = cursor.fetchone()

            if row is None:
                return None

            return self._row_to_record(row)

    def get_by_hash(self, file_hash: str) -> Optional[FileRecord]:
        """
        Get a file record by file hash.

        Args:
            file_hash: SHA256 hash of the file

        Returns:
            FileRecord if found, None otherwise
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM files WHERE file_hash = ? ORDER BY uploaded_at DESC LIMIT 1",
                (file_hash,)
            )
            row = cursor.fetchone()

            if row is None:
                return None

            return self._row_to_record(row)

    def get_by_table(self, table_name: str, include_archived: bool = False) -> List[FileRecord]:
        """
        Get all file records for a table.

        Args:
            table_name: Target table name (e.g., "DM")
            include_archived: Whether to include archived records

        Returns:
            List of FileRecords sorted by upload time (newest first)
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if include_archived:
                cursor = conn.execute(
                    "SELECT * FROM files WHERE table_name = ? ORDER BY uploaded_at DESC",
                    (table_name.upper(),)
                )
            else:
                cursor = conn.execute(
                    "SELECT * FROM files WHERE table_name = ? AND status != ? ORDER BY uploaded_at DESC",
                    (table_name.upper(), FileStatus.ARCHIVED.value)
                )

            return [self._row_to_record(row) for row in cursor.fetchall()]

    def get_current(self, table_name: str) -> Optional[FileRecord]:
        """
        Get the current (active) file record for a table.

        Args:
            table_name: Target table name

        Returns:
            The current FileRecord or None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                """SELECT * FROM files
                   WHERE table_name = ? AND status = ?
                   ORDER BY uploaded_at DESC LIMIT 1""",
                (table_name.upper(), FileStatus.COMPLETED.value)
            )
            row = cursor.fetchone()

            if row is None:
                return None

            return self._row_to_record(row)

    def list_all(self, status: Optional[FileStatus] = None, limit: int = 100) -> List[FileRecord]:
        """
        List all file records.

        Args:
            status: Optional status filter
            limit: Maximum records to return

        Returns:
            List of FileRecords sorted by upload time (newest first)
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if status:
                cursor = conn.execute(
                    "SELECT * FROM files WHERE status = ? ORDER BY uploaded_at DESC LIMIT ?",
                    (status.value, limit)
                )
            else:
                cursor = conn.execute(
                    "SELECT * FROM files ORDER BY uploaded_at DESC LIMIT ?",
                    (limit,)
                )

            return [self._row_to_record(row) for row in cursor.fetchall()]

    def update_status(self, file_id: str, status: FileStatus,
                      error_message: Optional[str] = None) -> Optional[FileRecord]:
        """
        Update the status of a file.

        Args:
            file_id: The file's unique identifier
            status: New status
            error_message: Optional error message (for failed status)

        Returns:
            Updated FileRecord or None if not found
        """
        record = self.get(file_id)
        if record is None:
            return None

        record.status = status
        if error_message:
            record.error_message = error_message
        if status == FileStatus.COMPLETED:
            record.processed_at = datetime.now().isoformat()

        return self.save(record)

    def update_processing_step(self, file_id: str, step: ProcessingStep) -> Optional[FileRecord]:
        """
        Update or add a processing step.

        Args:
            file_id: The file's unique identifier
            step: ProcessingStep to update/add

        Returns:
            Updated FileRecord or None if not found
        """
        record = self.get(file_id)
        if record is None:
            return None

        # Find and update existing step or add new one
        found = False
        for i, existing in enumerate(record.processing_steps):
            if existing.step_name == step.step_name:
                record.processing_steps[i] = step
                found = True
                break

        if not found:
            record.processing_steps.append(step)

        return self.save(record)

    def archive_previous(self, table_name: str, exclude_id: str) -> int:
        """
        Archive all previous completed files for a table.

        Args:
            table_name: Target table name
            exclude_id: ID of file to exclude (the new current file)

        Returns:
            Number of files archived
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """UPDATE files
                   SET status = ?
                   WHERE table_name = ? AND status = ? AND id != ?""",
                (FileStatus.ARCHIVED.value, table_name.upper(),
                 FileStatus.COMPLETED.value, exclude_id)
            )
            conn.commit()
            return cursor.rowcount

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get overall statistics.

        Returns:
            Dictionary with statistics
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Total counts by status
            status_counts = {}
            for status in FileStatus:
                cursor = conn.execute(
                    "SELECT COUNT(*) as count FROM files WHERE status = ?",
                    (status.value,)
                )
                status_counts[status.value] = cursor.fetchone()['count']

            # Total files
            cursor = conn.execute("SELECT COUNT(*) as count FROM files")
            total_files = cursor.fetchone()['count']

            # Unique tables
            cursor = conn.execute("SELECT COUNT(DISTINCT table_name) as count FROM files")
            unique_tables = cursor.fetchone()['count']

            # Total rows processed
            cursor = conn.execute(
                "SELECT SUM(row_count) as total FROM files WHERE status = ?",
                (FileStatus.COMPLETED.value,)
            )
            total_rows = cursor.fetchone()['total'] or 0

            # Total size processed
            cursor = conn.execute(
                "SELECT SUM(file_size) as total FROM files WHERE status = ?",
                (FileStatus.COMPLETED.value,)
            )
            total_size = cursor.fetchone()['total'] or 0

            # Recent uploads (last 24 hours)
            cursor = conn.execute(
                """SELECT COUNT(*) as count FROM files
                   WHERE uploaded_at > datetime('now', '-1 day')"""
            )
            recent_uploads = cursor.fetchone()['count']

            # Format breakdown
            cursor = conn.execute(
                """SELECT file_format, COUNT(*) as count
                   FROM files GROUP BY file_format"""
            )
            format_counts = {row['file_format']: row['count'] for row in cursor.fetchall()}

            return {
                'total_files': total_files,
                'unique_tables': unique_tables,
                'total_rows_processed': total_rows,
                'total_size_bytes': total_size,
                'total_size_mb': round(total_size / (1024 * 1024), 2),
                'recent_uploads_24h': recent_uploads,
                'status_counts': status_counts,
                'format_counts': format_counts
            }

    def get_table_summary(self) -> List[Dict[str, Any]]:
        """
        Get summary for each table.

        Returns:
            List of table summaries
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            cursor = conn.execute("""
                SELECT
                    table_name,
                    COUNT(*) as total_uploads,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                    MAX(row_count) as latest_rows,
                    MAX(column_count) as latest_columns,
                    MAX(uploaded_at) as last_upload
                FROM files
                GROUP BY table_name
                ORDER BY table_name
            """)

            return [dict(row) for row in cursor.fetchall()]

    def delete(self, file_id: str) -> bool:
        """
        Delete a file record.

        Args:
            file_id: The file's unique identifier

        Returns:
            True if deleted, False if not found
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM files WHERE id = ?", (file_id,))
            conn.commit()
            return cursor.rowcount > 0

    def _row_to_record(self, row: sqlite3.Row) -> FileRecord:
        """Convert a database row to FileRecord."""
        return FileRecord(
            id=row['id'],
            filename=row['filename'],
            table_name=row['table_name'],
            file_format=row['file_format'],
            file_size=row['file_size'],
            file_hash=row['file_hash'],
            schema_hash=row['schema_hash'],
            status=FileStatus(row['status']),
            uploaded_at=row['uploaded_at'],
            processed_at=row['processed_at'],
            row_count=row['row_count'],
            column_count=row['column_count'],
            schema_version=row['schema_version'],
            error_message=row['error_message'],
            processing_steps=[
                ProcessingStep(**s) for s in json.loads(row['processing_steps'] or '[]')
            ],
            metadata=json.loads(row['metadata'] or '{}')
        )
