"""
Audit Database Module
=====================

SQLite database operations for the SAGE audit logging system.
Provides immutable, 21 CFR Part 11 compliant audit trail.
"""

import sqlite3
import json
import hashlib
import hmac
import os
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager

from .models import (
    AuditEvent,
    QueryAuditDetails,
    AuditLog,
    AuditFilters,
    AuditStatistics,
    ElectronicSignature,
    IntegrityCheckResult,
)


# Secret key for HMAC signatures (should be set via environment variable)
AUDIT_SECRET_KEY = os.getenv("AUDIT_SECRET_KEY", "sage-audit-secret-key-change-in-production")


class AuditDB:
    """
    Database manager for the SAGE Audit Trail.

    Implements 21 CFR Part 11 requirements:
    - Immutable records (no UPDATE/DELETE on audit_logs)
    - SHA-256 checksums for integrity verification
    - Electronic signatures with HMAC
    - Complete audit trail with timestamps
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the AuditDB.

        Args:
            db_path: Path to SQLite database file. If None, uses default location.
        """
        if db_path is None:
            # Default path relative to project root
            db_path = os.getenv("AUDIT_DB_PATH", None)
            if db_path is None:
                project_root = Path(__file__).parent.parent.parent
                db_path = project_root / "data" / "audit.db"

        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        # Initialize database schema
        self._init_schema()

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

    def _init_schema(self):
        """Initialize the database schema."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Main audit log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS audit_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    username TEXT NOT NULL,
                    action TEXT NOT NULL,
                    resource_type TEXT,
                    resource_id TEXT,
                    status TEXT NOT NULL,
                    ip_address TEXT,
                    user_agent TEXT,
                    request_method TEXT,
                    request_path TEXT,
                    request_body TEXT,
                    response_status INTEGER,
                    response_body TEXT,
                    duration_ms INTEGER,
                    error_message TEXT,
                    details TEXT,
                    checksum TEXT NOT NULL,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Query audit details table (for LLM interactions)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS query_audit_details (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    audit_log_id INTEGER NOT NULL,
                    original_question TEXT,
                    sanitized_question TEXT,
                    intent_classification TEXT,
                    matched_entities TEXT,
                    generated_sql TEXT,
                    llm_prompt TEXT,
                    llm_response TEXT,
                    llm_model TEXT,
                    llm_tokens_used INTEGER,
                    confidence_score REAL,
                    confidence_breakdown TEXT,
                    execution_time_ms INTEGER,
                    result_row_count INTEGER,
                    tables_accessed TEXT,
                    columns_used TEXT,
                    FOREIGN KEY (audit_log_id) REFERENCES audit_logs(id)
                )
            ''')

            # Electronic signatures table (21 CFR Part 11)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS electronic_signatures (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    audit_log_id INTEGER NOT NULL,
                    signer_user_id TEXT NOT NULL,
                    signer_username TEXT NOT NULL,
                    signature_meaning TEXT NOT NULL,
                    signature_timestamp TEXT NOT NULL,
                    signature_hash TEXT NOT NULL,
                    FOREIGN KEY (audit_log_id) REFERENCES audit_logs(id)
                )
            ''')

            # Create indexes for efficient querying
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_timestamp ON audit_logs(timestamp)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_user ON audit_logs(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_status ON audit_logs(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_resource ON audit_logs(resource_type)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_audit_created ON audit_logs(created_at)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_query_audit_log ON query_audit_details(audit_log_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_signature_audit_log ON electronic_signatures(audit_log_id)')

    def _compute_checksum(self, data: Dict[str, Any]) -> str:
        """Compute SHA-256 checksum for audit record integrity."""
        # Sort keys for consistent hashing
        serialized = json.dumps(data, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode('utf-8')).hexdigest()

    def _compute_signature_hash(self, audit_log_id: int, user_id: str, meaning: str, timestamp: str) -> str:
        """Compute HMAC signature for electronic signatures."""
        message = f"{audit_log_id}:{user_id}:{meaning}:{timestamp}"
        return hmac.new(
            AUDIT_SECRET_KEY.encode('utf-8'),
            message.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

    # ==================== INSERT OPERATIONS ====================

    def insert_log(self, event: AuditEvent) -> int:
        """
        Insert an audit log entry.

        Args:
            event: The audit event to log.

        Returns:
            The ID of the inserted log entry.
        """
        # Prepare data for checksum
        checksum_data = {
            'timestamp': event.timestamp.isoformat(),
            'user_id': event.user_id,
            'username': event.username,
            'action': event.action.value if hasattr(event.action, 'value') else str(event.action),
            'resource_type': event.resource_type,
            'resource_id': event.resource_id,
            'status': event.status.value if hasattr(event.status, 'value') else str(event.status),
            'request_method': event.request_method,
            'request_path': event.request_path,
        }
        checksum = self._compute_checksum(checksum_data)

        # Serialize details if present
        details_json = json.dumps(event.details) if event.details else None

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO audit_logs (
                    timestamp, user_id, username, action, resource_type, resource_id,
                    status, ip_address, user_agent, request_method, request_path,
                    request_body, response_status, response_body, duration_ms,
                    error_message, details, checksum
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                event.timestamp.isoformat(),
                event.user_id,
                event.username,
                event.action.value if hasattr(event.action, 'value') else str(event.action),
                event.resource_type,
                event.resource_id,
                event.status.value if hasattr(event.status, 'value') else str(event.status),
                event.ip_address,
                event.user_agent,
                event.request_method,
                event.request_path,
                event.request_body,
                event.response_status,
                event.response_body,
                event.duration_ms,
                event.error_message,
                details_json,
                checksum,
            ))
            return cursor.lastrowid

    def insert_query_details(self, audit_log_id: int, details: QueryAuditDetails) -> int:
        """
        Insert query audit details linked to an audit log.

        Args:
            audit_log_id: The ID of the parent audit log.
            details: The query details to log.

        Returns:
            The ID of the inserted details record.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO query_audit_details (
                    audit_log_id, original_question, sanitized_question,
                    intent_classification, matched_entities, generated_sql,
                    llm_prompt, llm_response, llm_model, llm_tokens_used,
                    confidence_score, confidence_breakdown, execution_time_ms,
                    result_row_count, tables_accessed, columns_used
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                audit_log_id,
                details.original_question,
                details.sanitized_question,
                details.intent_classification,
                json.dumps(details.matched_entities) if details.matched_entities else None,
                details.generated_sql,
                details.llm_prompt,
                details.llm_response,
                details.llm_model,
                details.llm_tokens_used,
                details.confidence_score,
                json.dumps(details.confidence_breakdown) if details.confidence_breakdown else None,
                details.execution_time_ms,
                details.result_row_count,
                json.dumps(details.tables_accessed) if details.tables_accessed else None,
                json.dumps(details.columns_used) if details.columns_used else None,
            ))
            return cursor.lastrowid

    def insert_signature(self, signature: ElectronicSignature) -> int:
        """
        Insert an electronic signature for an audit log.

        Args:
            signature: The signature to add.

        Returns:
            The ID of the inserted signature.
        """
        timestamp = signature.signature_timestamp.isoformat()
        signature_hash = self._compute_signature_hash(
            signature.audit_log_id,
            signature.signer_user_id,
            signature.signature_meaning,
            timestamp
        )

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO electronic_signatures (
                    audit_log_id, signer_user_id, signer_username,
                    signature_meaning, signature_timestamp, signature_hash
                ) VALUES (?, ?, ?, ?, ?, ?)
            ''', (
                signature.audit_log_id,
                signature.signer_user_id,
                signature.signer_username,
                signature.signature_meaning,
                timestamp,
                signature_hash,
            ))
            return cursor.lastrowid

    # ==================== QUERY OPERATIONS ====================

    def get_log(self, log_id: int, include_details: bool = True) -> Optional[AuditLog]:
        """
        Get a single audit log by ID.

        Args:
            log_id: The ID of the log to retrieve.
            include_details: Whether to include query details and signatures.

        Returns:
            The audit log or None if not found.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM audit_logs WHERE id = ?', (log_id,))
            row = cursor.fetchone()

            if not row:
                return None

            log = self._row_to_audit_log(row)

            if include_details:
                # Get query details if exists
                cursor.execute('SELECT * FROM query_audit_details WHERE audit_log_id = ?', (log_id,))
                qd_row = cursor.fetchone()
                if qd_row:
                    log.query_details = self._row_to_query_details(qd_row)

                # Get signatures
                cursor.execute('SELECT * FROM electronic_signatures WHERE audit_log_id = ?', (log_id,))
                signatures = []
                for sig_row in cursor.fetchall():
                    signatures.append(ElectronicSignature(
                        id=sig_row['id'],
                        audit_log_id=sig_row['audit_log_id'],
                        signer_user_id=sig_row['signer_user_id'],
                        signer_username=sig_row['signer_username'],
                        signature_meaning=sig_row['signature_meaning'],
                        signature_timestamp=datetime.fromisoformat(sig_row['signature_timestamp']),
                        signature_hash=sig_row['signature_hash'],
                    ))
                if signatures:
                    log.signatures = signatures

            return log

    def search_logs(self, filters: AuditFilters) -> Tuple[List[AuditLog], int]:
        """
        Search audit logs with filters.

        Args:
            filters: The search filters to apply.

        Returns:
            Tuple of (list of matching logs, total count).
        """
        conditions = []
        params = []

        if filters.user_id:
            conditions.append("user_id = ?")
            params.append(filters.user_id)

        if filters.username:
            conditions.append("username LIKE ?")
            params.append(f"%{filters.username}%")

        if filters.action:
            conditions.append("action = ?")
            params.append(filters.action)

        if filters.actions:
            placeholders = ','.join('?' * len(filters.actions))
            conditions.append(f"action IN ({placeholders})")
            params.extend(filters.actions)

        if filters.resource_type:
            conditions.append("resource_type = ?")
            params.append(filters.resource_type)

        if filters.status:
            conditions.append("status = ?")
            params.append(filters.status)

        if filters.start_date:
            conditions.append("timestamp >= ?")
            params.append(filters.start_date.isoformat())

        if filters.end_date:
            conditions.append("timestamp <= ?")
            params.append(filters.end_date.isoformat())

        if filters.ip_address:
            conditions.append("ip_address = ?")
            params.append(filters.ip_address)

        if filters.search_text:
            conditions.append("(request_path LIKE ? OR error_message LIKE ? OR details LIKE ?)")
            search_pattern = f"%{filters.search_text}%"
            params.extend([search_pattern, search_pattern, search_pattern])

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get total count
            cursor.execute(f"SELECT COUNT(*) as total FROM audit_logs WHERE {where_clause}", params)
            total = cursor.fetchone()['total']

            # Get paginated results
            offset = (filters.page - 1) * filters.page_size
            cursor.execute(f'''
                SELECT * FROM audit_logs
                WHERE {where_clause}
                ORDER BY timestamp DESC
                LIMIT ? OFFSET ?
            ''', params + [filters.page_size, offset])

            logs = [self._row_to_audit_log(row) for row in cursor.fetchall()]

            return logs, total

    def get_query_details(self, audit_log_id: int) -> Optional[QueryAuditDetails]:
        """Get query details for an audit log."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM query_audit_details WHERE audit_log_id = ?', (audit_log_id,))
            row = cursor.fetchone()
            return self._row_to_query_details(row) if row else None

    def get_statistics(self, start_date: Optional[datetime] = None,
                       end_date: Optional[datetime] = None) -> AuditStatistics:
        """
        Get audit statistics.

        Args:
            start_date: Filter start date.
            end_date: Filter end date.

        Returns:
            Audit statistics.
        """
        conditions = []
        params = []

        if start_date:
            conditions.append("timestamp >= ?")
            params.append(start_date.isoformat())

        if end_date:
            conditions.append("timestamp <= ?")
            params.append(end_date.isoformat())

        where_clause = " AND ".join(conditions) if conditions else "1=1"

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Total events
            cursor.execute(f"SELECT COUNT(*) as total FROM audit_logs WHERE {where_clause}", params)
            total = cursor.fetchone()['total']

            # By action
            cursor.execute(f'''
                SELECT action, COUNT(*) as count
                FROM audit_logs WHERE {where_clause}
                GROUP BY action
            ''', params)
            by_action = {row['action']: row['count'] for row in cursor.fetchall()}

            # By status
            cursor.execute(f'''
                SELECT status, COUNT(*) as count
                FROM audit_logs WHERE {where_clause}
                GROUP BY status
            ''', params)
            by_status = {row['status']: row['count'] for row in cursor.fetchall()}

            # By user
            cursor.execute(f'''
                SELECT username, COUNT(*) as count
                FROM audit_logs WHERE {where_clause}
                GROUP BY username
                ORDER BY count DESC
                LIMIT 10
            ''', params)
            by_user = {row['username']: row['count'] for row in cursor.fetchall()}

            # By resource type
            cursor.execute(f'''
                SELECT resource_type, COUNT(*) as count
                FROM audit_logs WHERE {where_clause} AND resource_type IS NOT NULL
                GROUP BY resource_type
            ''', params)
            by_resource = {row['resource_type']: row['count'] for row in cursor.fetchall()}

            # Average query confidence
            cursor.execute(f'''
                SELECT AVG(q.confidence_score) as avg_confidence
                FROM query_audit_details q
                JOIN audit_logs a ON q.audit_log_id = a.id
                WHERE {where_clause} AND q.confidence_score IS NOT NULL
            ''', params)
            avg_conf_row = cursor.fetchone()
            avg_confidence = avg_conf_row['avg_confidence'] if avg_conf_row else None

            # Average duration
            cursor.execute(f'''
                SELECT AVG(duration_ms) as avg_duration
                FROM audit_logs
                WHERE {where_clause} AND duration_ms IS NOT NULL
            ''', params)
            avg_dur_row = cursor.fetchone()
            avg_duration = avg_dur_row['avg_duration'] if avg_dur_row else None

            # Only include date_range if we have valid dates
            date_range = None
            if start_date or end_date:
                date_range = {}
                if start_date:
                    date_range['start'] = start_date.isoformat()
                if end_date:
                    date_range['end'] = end_date.isoformat()

            return AuditStatistics(
                total_events=total,
                by_action=by_action,
                by_status=by_status,
                by_user=by_user,
                by_resource_type=by_resource,
                average_query_confidence=round(avg_confidence, 2) if avg_confidence else None,
                average_duration_ms=round(avg_duration, 2) if avg_duration else None,
                date_range=date_range
            )

    def get_available_actions(self) -> List[str]:
        """Get list of all distinct actions in the log."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT DISTINCT action FROM audit_logs ORDER BY action')
            return [row['action'] for row in cursor.fetchall()]

    def get_available_users(self) -> List[Dict[str, str]]:
        """Get list of all users who have audit entries."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT user_id, username
                FROM audit_logs
                ORDER BY username
            ''')
            return [{'user_id': row['user_id'], 'username': row['username']}
                    for row in cursor.fetchall()]

    def get_available_resource_types(self) -> List[str]:
        """Get list of all distinct resource types in the log."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT DISTINCT resource_type
                FROM audit_logs
                WHERE resource_type IS NOT NULL
                ORDER BY resource_type
            ''')
            return [row['resource_type'] for row in cursor.fetchall()]

    # ==================== INTEGRITY VERIFICATION ====================

    def verify_integrity(self, log_id: int) -> IntegrityCheckResult:
        """
        Verify the integrity of an audit log record.

        Args:
            log_id: The ID of the log to verify.

        Returns:
            Integrity check result.
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM audit_logs WHERE id = ?', (log_id,))
            row = cursor.fetchone()

            if not row:
                return IntegrityCheckResult(
                    log_id=log_id,
                    integrity_valid=False,
                    stored_checksum="",
                    computed_checksum="",
                    discrepancy_details="Log not found"
                )

            # Recompute checksum
            checksum_data = {
                'timestamp': row['timestamp'],
                'user_id': row['user_id'],
                'username': row['username'],
                'action': row['action'],
                'resource_type': row['resource_type'],
                'resource_id': row['resource_id'],
                'status': row['status'],
                'request_method': row['request_method'],
                'request_path': row['request_path'],
            }
            computed_checksum = self._compute_checksum(checksum_data)

            return IntegrityCheckResult(
                log_id=log_id,
                integrity_valid=(computed_checksum == row['checksum']),
                stored_checksum=row['checksum'],
                computed_checksum=computed_checksum,
                discrepancy_details=None if computed_checksum == row['checksum'] else "Checksum mismatch - record may have been tampered"
            )

    # ==================== HELPER METHODS ====================

    def _row_to_audit_log(self, row) -> AuditLog:
        """Convert a database row to an AuditLog model."""
        details = json.loads(row['details']) if row['details'] else None

        return AuditLog(
            id=row['id'],
            timestamp=datetime.fromisoformat(row['timestamp']),
            user_id=row['user_id'],
            username=row['username'],
            action=row['action'],
            resource_type=row['resource_type'],
            resource_id=row['resource_id'],
            status=row['status'],
            ip_address=row['ip_address'],
            user_agent=row['user_agent'],
            request_method=row['request_method'],
            request_path=row['request_path'],
            request_body=row['request_body'],
            response_status=row['response_status'],
            response_body=row['response_body'],
            duration_ms=row['duration_ms'],
            error_message=row['error_message'],
            checksum=row['checksum'],
            created_at=datetime.fromisoformat(row['created_at']),
            details=details,
        )

    def _row_to_query_details(self, row) -> QueryAuditDetails:
        """Convert a database row to a QueryAuditDetails model."""
        return QueryAuditDetails(
            original_question=row['original_question'],
            sanitized_question=row['sanitized_question'],
            intent_classification=row['intent_classification'],
            matched_entities=json.loads(row['matched_entities']) if row['matched_entities'] else None,
            generated_sql=row['generated_sql'],
            llm_prompt=row['llm_prompt'],
            llm_response=row['llm_response'],
            llm_model=row['llm_model'],
            llm_tokens_used=row['llm_tokens_used'],
            confidence_score=row['confidence_score'],
            confidence_breakdown=json.loads(row['confidence_breakdown']) if row['confidence_breakdown'] else None,
            execution_time_ms=row['execution_time_ms'],
            result_row_count=row['result_row_count'],
            tables_accessed=json.loads(row['tables_accessed']) if row['tables_accessed'] else None,
            columns_used=json.loads(row['columns_used']) if row['columns_used'] else None,
        )
