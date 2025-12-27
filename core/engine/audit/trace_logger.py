"""
Audit Trace Logger - 21 CFR Part 11 compliant structured logging.

Provides full traceability of query processing for regulatory compliance.
"""

import json
import sqlite3
import uuid
import hashlib
from datetime import datetime
from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional
from pathlib import Path
from enum import Enum


class AuditLevel(Enum):
    """Audit severity levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


class AuditEvent(Enum):
    """Audit event types."""
    # Query lifecycle
    QUERY_RECEIVED = "QUERY_RECEIVED"
    QUERY_SANITIZED = "QUERY_SANITIZED"
    QUERY_CLASSIFIED = "QUERY_CLASSIFIED"
    QUERY_CERTIFIED = "QUERY_CERTIFIED"

    # Processing stages
    ENTITY_EXTRACTED = "ENTITY_EXTRACTED"
    DICTIONARY_MATCHED = "DICTIONARY_MATCHED"
    METADATA_RETRIEVED = "METADATA_RETRIEVED"
    PROTOCOL_CHECKED = "PROTOCOL_CHECKED"
    TERM_RESOLVED = "TERM_RESOLVED"

    # SQL generation
    SQL_GENERATED = "SQL_GENERATED"
    SQL_VALIDATED = "SQL_VALIDATED"
    SCHEMA_VALIDATED = "SCHEMA_VALIDATED"
    VIEW_ROUTED = "VIEW_ROUTED"

    # Execution
    QUERY_EXECUTED = "QUERY_EXECUTED"
    RESULT_VALIDATED = "RESULT_VALIDATED"
    CONFIDENCE_CALCULATED = "CONFIDENCE_CALCULATED"

    # Response
    RESPONSE_GENERATED = "RESPONSE_GENERATED"
    REFUSAL_GENERATED = "REFUSAL_GENERATED"

    # User interaction
    USER_LOGIN = "USER_LOGIN"
    USER_LOGOUT = "USER_LOGOUT"
    USER_FEEDBACK = "USER_FEEDBACK"
    CLARIFICATION_REQUESTED = "CLARIFICATION_REQUESTED"
    CLARIFICATION_PROVIDED = "CLARIFICATION_PROVIDED"

    # Admin actions
    EXAMPLE_ADDED = "EXAMPLE_ADDED"
    EXAMPLE_VERIFIED = "EXAMPLE_VERIFIED"
    METADATA_APPROVED = "METADATA_APPROVED"
    PROTOCOL_UPDATED = "PROTOCOL_UPDATED"

    # Errors
    VALIDATION_ERROR = "VALIDATION_ERROR"
    EXECUTION_ERROR = "EXECUTION_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"


@dataclass
class AuditEntry:
    """A single audit log entry."""
    id: str
    trace_id: str  # Groups related entries for a single query
    timestamp: datetime
    event: AuditEvent
    level: AuditLevel
    component: str
    message: str
    details: Dict[str, Any]
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    checksum: Optional[str] = None


@dataclass
class QueryTrace:
    """Full trace of a query's processing."""
    trace_id: str
    question: str
    started_at: datetime
    user_id: Optional[str] = None
    session_id: Optional[str] = None
    completed_at: Optional[datetime] = None
    entries: List[AuditEntry] = field(default_factory=list)
    final_sql: Optional[str] = None
    final_confidence: Optional[float] = None
    final_action: Optional[str] = None
    execution_time_ms: Optional[int] = None
    success: bool = False


class AuditTraceLogger:
    """
    Structured audit logging for 21 CFR Part 11 compliance.

    Features:
    - Tamper-evident checksums
    - Full query traceability
    - Searchable structured logs
    - Retention management
    """

    def __init__(self, db_path: str = "data/audit.db"):
        """
        Initialize Audit Trace Logger.

        Args:
            db_path: Path to audit database
        """
        self.db_path = Path(db_path)
        self._init_database()
        self._current_traces: Dict[str, QueryTrace] = {}

    def _init_database(self):
        """Initialize audit tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            # Main audit log table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS audit_log (
                    id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    event TEXT NOT NULL,
                    level TEXT NOT NULL,
                    component TEXT NOT NULL,
                    message TEXT NOT NULL,
                    details TEXT,
                    user_id TEXT,
                    session_id TEXT,
                    checksum TEXT NOT NULL
                )
            """)

            # Query trace summary table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_traces (
                    trace_id TEXT PRIMARY KEY,
                    question TEXT NOT NULL,
                    user_id TEXT,
                    session_id TEXT,
                    started_at TEXT NOT NULL,
                    completed_at TEXT,
                    final_sql TEXT,
                    final_confidence REAL,
                    final_action TEXT,
                    execution_time_ms INTEGER,
                    success INTEGER DEFAULT 0
                )
            """)

            # Indexes for efficient querying
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_trace
                ON audit_log(trace_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_timestamp
                ON audit_log(timestamp)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_event
                ON audit_log(event)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_audit_user
                ON audit_log(user_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_traces_user
                ON query_traces(user_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_traces_started
                ON query_traces(started_at)
            """)

    def start_trace(
        self,
        question: str,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> str:
        """
        Start a new query trace.

        Args:
            question: The user's question
            user_id: User ID if authenticated
            session_id: Session ID

        Returns:
            Trace ID for subsequent logging
        """
        trace_id = str(uuid.uuid4())

        trace = QueryTrace(
            trace_id=trace_id,
            question=question,
            user_id=user_id,
            session_id=session_id,
            started_at=datetime.utcnow()
        )

        self._current_traces[trace_id] = trace

        # Store trace summary
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO query_traces
                (trace_id, question, user_id, session_id, started_at)
                VALUES (?, ?, ?, ?, ?)
            """, (
                trace_id,
                question,
                user_id,
                session_id,
                trace.started_at.isoformat()
            ))

        # Log initial event
        self.log(
            trace_id=trace_id,
            event=AuditEvent.QUERY_RECEIVED,
            level=AuditLevel.INFO,
            component="trace_logger",
            message="Query trace started",
            details={"question": question},
            user_id=user_id,
            session_id=session_id
        )

        return trace_id

    def log(
        self,
        trace_id: str,
        event: AuditEvent,
        level: AuditLevel,
        component: str,
        message: str,
        details: Optional[Dict[str, Any]] = None,
        user_id: Optional[str] = None,
        session_id: Optional[str] = None
    ) -> str:
        """
        Log an audit entry.

        Args:
            trace_id: Trace ID from start_trace
            event: Event type
            level: Severity level
            component: Component that generated the log
            message: Human-readable message
            details: Structured details
            user_id: User ID
            session_id: Session ID

        Returns:
            Entry ID
        """
        entry_id = str(uuid.uuid4())
        timestamp = datetime.utcnow()

        # Create checksum for tamper detection
        checksum_data = f"{entry_id}|{trace_id}|{timestamp.isoformat()}|{event.value}|{message}"
        checksum = hashlib.sha256(checksum_data.encode()).hexdigest()[:16]

        entry = AuditEntry(
            id=entry_id,
            trace_id=trace_id,
            timestamp=timestamp,
            event=event,
            level=level,
            component=component,
            message=message,
            details=details or {},
            user_id=user_id,
            session_id=session_id,
            checksum=checksum
        )

        # Add to in-memory trace if exists
        if trace_id in self._current_traces:
            self._current_traces[trace_id].entries.append(entry)

        # Persist to database
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO audit_log
                (id, trace_id, timestamp, event, level, component,
                 message, details, user_id, session_id, checksum)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                entry_id,
                trace_id,
                timestamp.isoformat(),
                event.value,
                level.value,
                component,
                message,
                json.dumps(details or {}),
                user_id,
                session_id,
                checksum
            ))

        return entry_id

    def complete_trace(
        self,
        trace_id: str,
        sql: Optional[str] = None,
        confidence: Optional[float] = None,
        action: Optional[str] = None,
        success: bool = True
    ):
        """
        Complete a query trace.

        Args:
            trace_id: Trace ID
            sql: Final SQL executed
            confidence: Final confidence score
            action: Action taken (RETURN_NORMAL, REFUSE, etc.)
            success: Whether query was successful
        """
        completed_at = datetime.utcnow()

        if trace_id in self._current_traces:
            trace = self._current_traces[trace_id]
            trace.completed_at = completed_at
            trace.final_sql = sql
            trace.final_confidence = confidence
            trace.final_action = action
            trace.success = success

            execution_time = int(
                (completed_at - trace.started_at).total_seconds() * 1000
            )
            trace.execution_time_ms = execution_time

            # Update database
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("""
                    UPDATE query_traces
                    SET completed_at = ?,
                        final_sql = ?,
                        final_confidence = ?,
                        final_action = ?,
                        execution_time_ms = ?,
                        success = ?
                    WHERE trace_id = ?
                """, (
                    completed_at.isoformat(),
                    sql,
                    confidence,
                    action,
                    execution_time,
                    1 if success else 0,
                    trace_id
                ))

            # Log completion
            self.log(
                trace_id=trace_id,
                event=AuditEvent.RESPONSE_GENERATED,
                level=AuditLevel.INFO,
                component="trace_logger",
                message="Query trace completed",
                details={
                    "execution_time_ms": execution_time,
                    "confidence": confidence,
                    "action": action,
                    "success": success
                }
            )

            # Clean up
            del self._current_traces[trace_id]

    def get_trace(self, trace_id: str) -> Optional[Dict[str, Any]]:
        """
        Get a complete query trace.

        Args:
            trace_id: Trace ID

        Returns:
            Complete trace with all entries
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Get trace summary
            cursor = conn.execute(
                "SELECT * FROM query_traces WHERE trace_id = ?",
                (trace_id,)
            )
            trace_row = cursor.fetchone()

            if not trace_row:
                return None

            # Get all entries
            cursor = conn.execute(
                "SELECT * FROM audit_log WHERE trace_id = ? ORDER BY timestamp",
                (trace_id,)
            )
            entries = [dict(row) for row in cursor.fetchall()]

            return {
                **dict(trace_row),
                "entries": entries
            }

    def get_entries(
        self,
        trace_id: Optional[str] = None,
        event: Optional[AuditEvent] = None,
        level: Optional[AuditLevel] = None,
        user_id: Optional[str] = None,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Query audit entries with filters.

        Args:
            trace_id: Filter by trace
            event: Filter by event type
            level: Filter by level
            user_id: Filter by user
            start_time: Start of time range
            end_time: End of time range
            limit: Maximum results

        Returns:
            List of matching entries
        """
        query = "SELECT * FROM audit_log WHERE 1=1"
        params = []

        if trace_id:
            query += " AND trace_id = ?"
            params.append(trace_id)

        if event:
            query += " AND event = ?"
            params.append(event.value)

        if level:
            query += " AND level = ?"
            params.append(level.value)

        if user_id:
            query += " AND user_id = ?"
            params.append(user_id)

        if start_time:
            query += " AND timestamp >= ?"
            params.append(start_time.isoformat())

        if end_time:
            query += " AND timestamp <= ?"
            params.append(end_time.isoformat())

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_recent_traces(
        self,
        user_id: Optional[str] = None,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get recent query traces.

        Args:
            user_id: Filter by user
            limit: Maximum results

        Returns:
            List of recent traces
        """
        query = "SELECT * FROM query_traces"
        params = []

        if user_id:
            query += " WHERE user_id = ?"
            params.append(user_id)

        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_statistics(
        self,
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None
    ) -> Dict[str, Any]:
        """
        Get audit statistics.

        Args:
            start_time: Start of period
            end_time: End of period

        Returns:
            Statistics dictionary
        """
        time_filter = ""
        params: List[Any] = []

        if start_time:
            time_filter += " AND started_at >= ?"
            params.append(start_time.isoformat())
        if end_time:
            time_filter += " AND started_at <= ?"
            params.append(end_time.isoformat())

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Total queries
            total = conn.execute(
                f"SELECT COUNT(*) as count FROM query_traces WHERE 1=1 {time_filter}",
                params
            ).fetchone()["count"]

            # Successful queries
            successful = conn.execute(
                f"SELECT COUNT(*) as count FROM query_traces WHERE success = 1 {time_filter}",
                params
            ).fetchone()["count"]

            # Average confidence
            avg_confidence = conn.execute(
                f"""SELECT AVG(final_confidence) as avg
                    FROM query_traces
                    WHERE final_confidence IS NOT NULL {time_filter}""",
                params
            ).fetchone()["avg"]

            # Average execution time
            avg_time = conn.execute(
                f"""SELECT AVG(execution_time_ms) as avg
                    FROM query_traces
                    WHERE execution_time_ms IS NOT NULL {time_filter}""",
                params
            ).fetchone()["avg"]

            # Actions breakdown
            actions = conn.execute(
                f"""SELECT final_action, COUNT(*) as count
                    FROM query_traces
                    WHERE final_action IS NOT NULL {time_filter}
                    GROUP BY final_action""",
                params
            ).fetchall()

            # Unique users
            users = conn.execute(
                f"""SELECT COUNT(DISTINCT user_id) as count
                    FROM query_traces
                    WHERE user_id IS NOT NULL {time_filter}""",
                params
            ).fetchone()["count"]

            return {
                "total_queries": total,
                "successful_queries": successful,
                "success_rate": round(successful / total * 100, 1) if total > 0 else 0,
                "average_confidence": round(avg_confidence, 1) if avg_confidence else None,
                "average_execution_ms": round(avg_time, 0) if avg_time else None,
                "actions": {row["final_action"]: row["count"] for row in actions},
                "unique_users": users
            }

    def verify_integrity(self, trace_id: str) -> Dict[str, Any]:
        """
        Verify integrity of a trace (tamper detection).

        Args:
            trace_id: Trace ID to verify

        Returns:
            Verification result
        """
        entries = self.get_entries(trace_id=trace_id, limit=1000)

        valid = True
        issues = []

        for entry in entries:
            # Recreate checksum
            checksum_data = f"{entry['id']}|{entry['trace_id']}|{entry['timestamp']}|{entry['event']}|{entry['message']}"
            expected_checksum = hashlib.sha256(checksum_data.encode()).hexdigest()[:16]

            if entry['checksum'] != expected_checksum:
                valid = False
                issues.append({
                    "entry_id": entry["id"],
                    "issue": "Checksum mismatch - possible tampering"
                })

        return {
            "trace_id": trace_id,
            "valid": valid,
            "entries_checked": len(entries),
            "issues": issues
        }

    def export_trace(self, trace_id: str) -> Dict[str, Any]:
        """
        Export a complete trace for compliance reporting.

        Args:
            trace_id: Trace ID

        Returns:
            Complete trace in export format
        """
        trace = self.get_trace(trace_id)
        if not trace:
            return {"error": "Trace not found"}

        # Add verification
        verification = self.verify_integrity(trace_id)

        return {
            "export_timestamp": datetime.utcnow().isoformat(),
            "trace": trace,
            "integrity_verification": verification
        }

    def cleanup_old_entries(self, days: int = 365):
        """
        Remove entries older than specified days.

        Args:
            days: Retention period in days
        """
        cutoff = datetime.utcnow().replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        cutoff = cutoff.replace(day=cutoff.day - days) if cutoff.day > days else cutoff

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "DELETE FROM audit_log WHERE timestamp < ?",
                (cutoff.isoformat(),)
            )
            conn.execute(
                "DELETE FROM query_traces WHERE started_at < ?",
                (cutoff.isoformat(),)
            )

    def clear_all(self):
        """Clear all audit data (for testing)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM audit_log")
            conn.execute("DELETE FROM query_traces")
