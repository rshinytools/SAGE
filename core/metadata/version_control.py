# SAGE - Version Control Module
# ==============================
# Tracks metadata changes and maintains version history
"""
Version control for metadata management.

Features:
- Track all changes to metadata definitions
- Maintain full version history
- Compute diffs between versions
- Support rollback to previous versions
- Audit trail with user attribution
"""

import logging
import json
import hashlib
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum

logger = logging.getLogger(__name__)


class ChangeType(Enum):
    """Types of metadata changes."""
    CREATED = "created"
    MODIFIED = "modified"
    DELETED = "deleted"
    APPROVED = "approved"
    REJECTED = "rejected"
    IMPORTED = "imported"


@dataclass
class MetadataChange:
    """Represents a single change to metadata."""
    entity_type: str  # domain, variable, codelist
    entity_id: str    # Unique identifier
    change_type: ChangeType
    field_name: Optional[str] = None
    old_value: Optional[str] = None
    new_value: Optional[str] = None
    user: str = "system"
    timestamp: str = ""
    comment: Optional[str] = None

    def __post_init__(self):
        if not self.timestamp:
            self.timestamp = datetime.now().isoformat()

    def to_dict(self) -> Dict[str, Any]:
        return {
            'entity_type': self.entity_type,
            'entity_id': self.entity_id,
            'change_type': self.change_type.value,
            'field_name': self.field_name,
            'old_value': self.old_value,
            'new_value': self.new_value,
            'user': self.user,
            'timestamp': self.timestamp,
            'comment': self.comment
        }


@dataclass
class MetadataVersion:
    """A versioned snapshot of metadata."""
    version_id: str
    version_number: int
    content_hash: str
    created_at: str
    created_by: str
    comment: Optional[str] = None
    parent_version: Optional[str] = None
    changes: List[MetadataChange] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'version_id': self.version_id,
            'version_number': self.version_number,
            'content_hash': self.content_hash,
            'created_at': self.created_at,
            'created_by': self.created_by,
            'comment': self.comment,
            'parent_version': self.parent_version,
            'changes': [c.to_dict() for c in self.changes]
        }


@dataclass
class DiffResult:
    """Result of comparing two metadata versions."""
    added: List[Dict[str, Any]] = field(default_factory=list)
    modified: List[Dict[str, Any]] = field(default_factory=list)
    deleted: List[Dict[str, Any]] = field(default_factory=list)
    unchanged_count: int = 0

    @property
    def has_changes(self) -> bool:
        return bool(self.added or self.modified or self.deleted)

    def to_dict(self) -> Dict[str, Any]:
        return {
            'added': self.added,
            'modified': self.modified,
            'deleted': self.deleted,
            'unchanged_count': self.unchanged_count,
            'has_changes': self.has_changes,
            'summary': {
                'added_count': len(self.added),
                'modified_count': len(self.modified),
                'deleted_count': len(self.deleted)
            }
        }


class VersionControl:
    """
    Version control system for metadata.

    Tracks all changes to metadata definitions, maintains version history,
    and supports rollback operations.

    Example:
        vc = VersionControl("metadata_history.db")

        # Create initial version
        vc.create_version(metadata_dict, "Initial import", "admin")

        # Record a change
        vc.record_change(MetadataChange(
            entity_type="variable",
            entity_id="DM.USUBJID",
            change_type=ChangeType.MODIFIED,
            field_name="label",
            old_value="Subject ID",
            new_value="Unique Subject Identifier",
            user="admin"
        ))

        # Get history
        history = vc.get_history()
    """

    def __init__(self, db_path: str = "metadata_versions.db"):
        """
        Initialize version control.

        Args:
            db_path: Path to SQLite database for version storage
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_database()

    def _init_database(self):
        """Initialize the version control database."""
        with sqlite3.connect(self.db_path) as conn:
            conn.executescript("""
                -- Metadata versions table
                CREATE TABLE IF NOT EXISTS versions (
                    version_id TEXT PRIMARY KEY,
                    version_number INTEGER NOT NULL,
                    content_hash TEXT NOT NULL,
                    content TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    comment TEXT,
                    parent_version TEXT,
                    FOREIGN KEY (parent_version) REFERENCES versions(version_id)
                );

                -- Change log table
                CREATE TABLE IF NOT EXISTS changes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    version_id TEXT,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    change_type TEXT NOT NULL,
                    field_name TEXT,
                    old_value TEXT,
                    new_value TEXT,
                    user TEXT NOT NULL,
                    timestamp TEXT NOT NULL,
                    comment TEXT,
                    FOREIGN KEY (version_id) REFERENCES versions(version_id)
                );

                -- Approval log table
                CREATE TABLE IF NOT EXISTS approvals (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    entity_id TEXT NOT NULL,
                    status TEXT NOT NULL,  -- pending, approved, rejected
                    reviewed_by TEXT,
                    reviewed_at TEXT,
                    comment TEXT,
                    created_at TEXT NOT NULL
                );

                -- Create indexes
                CREATE INDEX IF NOT EXISTS idx_changes_version ON changes(version_id);
                CREATE INDEX IF NOT EXISTS idx_changes_entity ON changes(entity_type, entity_id);
                CREATE INDEX IF NOT EXISTS idx_approvals_entity ON approvals(entity_type, entity_id);
                CREATE INDEX IF NOT EXISTS idx_approvals_status ON approvals(status);
            """)

    def _compute_hash(self, content: Dict[str, Any]) -> str:
        """Compute SHA-256 hash of content."""
        content_str = json.dumps(content, sort_keys=True, default=str)
        return hashlib.sha256(content_str.encode()).hexdigest()

    def _generate_version_id(self) -> str:
        """Generate a unique version ID."""
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S%f")
        return f"v_{timestamp}"

    def get_latest_version(self) -> Optional[MetadataVersion]:
        """Get the latest version."""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM versions
                ORDER BY version_number DESC
                LIMIT 1
            """)
            row = cursor.fetchone()
            if row:
                return MetadataVersion(
                    version_id=row['version_id'],
                    version_number=row['version_number'],
                    content_hash=row['content_hash'],
                    created_at=row['created_at'],
                    created_by=row['created_by'],
                    comment=row['comment'],
                    parent_version=row['parent_version']
                )
        return None

    def get_version(self, version_id: str) -> Optional[Tuple[MetadataVersion, Dict[str, Any]]]:
        """
        Get a specific version with its content.

        Args:
            version_id: Version identifier

        Returns:
            Tuple of (MetadataVersion, content_dict) or None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM versions WHERE version_id = ?",
                (version_id,)
            )
            row = cursor.fetchone()
            if row:
                version = MetadataVersion(
                    version_id=row['version_id'],
                    version_number=row['version_number'],
                    content_hash=row['content_hash'],
                    created_at=row['created_at'],
                    created_by=row['created_by'],
                    comment=row['comment'],
                    parent_version=row['parent_version']
                )
                content = json.loads(row['content'])
                return version, content
        return None

    def create_version(
        self,
        content: Dict[str, Any],
        comment: str = "",
        user: str = "system",
        changes: Optional[List[MetadataChange]] = None
    ) -> MetadataVersion:
        """
        Create a new version of metadata.

        Args:
            content: Metadata content dictionary
            comment: Version comment
            user: User creating the version
            changes: List of changes in this version

        Returns:
            Created MetadataVersion
        """
        # Get latest version for parent reference
        latest = self.get_latest_version()
        parent_id = latest.version_id if latest else None
        version_number = (latest.version_number + 1) if latest else 1

        # Generate version info
        version_id = self._generate_version_id()
        content_hash = self._compute_hash(content)
        created_at = datetime.now().isoformat()

        # Store version
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO versions (
                    version_id, version_number, content_hash, content,
                    created_at, created_by, comment, parent_version
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                version_id, version_number, content_hash,
                json.dumps(content, default=str),
                created_at, user, comment, parent_id
            ))

            # Store changes if provided
            if changes:
                for change in changes:
                    conn.execute("""
                        INSERT INTO changes (
                            version_id, entity_type, entity_id, change_type,
                            field_name, old_value, new_value, user, timestamp, comment
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """, (
                        version_id, change.entity_type, change.entity_id,
                        change.change_type.value, change.field_name,
                        change.old_value, change.new_value,
                        change.user, change.timestamp, change.comment
                    ))

        logger.info(f"Created version {version_id} (v{version_number})")

        return MetadataVersion(
            version_id=version_id,
            version_number=version_number,
            content_hash=content_hash,
            created_at=created_at,
            created_by=user,
            comment=comment,
            parent_version=parent_id,
            changes=changes or []
        )

    def record_change(self, change: MetadataChange, version_id: Optional[str] = None):
        """
        Record a single change to metadata.

        Args:
            change: The change to record
            version_id: Optional version to associate with
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO changes (
                    version_id, entity_type, entity_id, change_type,
                    field_name, old_value, new_value, user, timestamp, comment
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                version_id, change.entity_type, change.entity_id,
                change.change_type.value, change.field_name,
                change.old_value, change.new_value,
                change.user, change.timestamp, change.comment
            ))

        logger.debug(f"Recorded change: {change.change_type.value} {change.entity_type}/{change.entity_id}")

    def get_history(
        self,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None,
        limit: int = 100
    ) -> List[MetadataChange]:
        """
        Get change history.

        Args:
            entity_type: Filter by entity type
            entity_id: Filter by entity ID
            limit: Maximum number of changes to return

        Returns:
            List of MetadataChange objects
        """
        query = "SELECT * FROM changes WHERE 1=1"
        params = []

        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)
        if entity_id:
            query += " AND entity_id = ?"
            params.append(entity_id)

        query += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)

        changes = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)
            for row in cursor:
                changes.append(MetadataChange(
                    entity_type=row['entity_type'],
                    entity_id=row['entity_id'],
                    change_type=ChangeType(row['change_type']),
                    field_name=row['field_name'],
                    old_value=row['old_value'],
                    new_value=row['new_value'],
                    user=row['user'],
                    timestamp=row['timestamp'],
                    comment=row['comment']
                ))

        return changes

    def get_versions(self, limit: int = 50) -> List[MetadataVersion]:
        """
        Get list of all versions.

        Args:
            limit: Maximum number of versions to return

        Returns:
            List of MetadataVersion objects
        """
        versions = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM versions
                ORDER BY version_number DESC
                LIMIT ?
            """, (limit,))

            for row in cursor:
                versions.append(MetadataVersion(
                    version_id=row['version_id'],
                    version_number=row['version_number'],
                    content_hash=row['content_hash'],
                    created_at=row['created_at'],
                    created_by=row['created_by'],
                    comment=row['comment'],
                    parent_version=row['parent_version']
                ))

        return versions

    def diff_versions(
        self,
        version1_id: str,
        version2_id: str
    ) -> Optional[DiffResult]:
        """
        Compare two versions and return differences.

        Args:
            version1_id: First version ID (older)
            version2_id: Second version ID (newer)

        Returns:
            DiffResult with added, modified, deleted items
        """
        v1_result = self.get_version(version1_id)
        v2_result = self.get_version(version2_id)

        if not v1_result or not v2_result:
            return None

        _, content1 = v1_result
        _, content2 = v2_result

        return self._compute_diff(content1, content2)

    def _compute_diff(
        self,
        old_content: Dict[str, Any],
        new_content: Dict[str, Any]
    ) -> DiffResult:
        """Compute diff between two content dictionaries."""
        result = DiffResult()

        # Get domains from both versions
        old_domains = {d['name']: d for d in old_content.get('domains', [])}
        new_domains = {d['name']: d for d in new_content.get('domains', [])}

        # Find added domains
        for name in new_domains:
            if name not in old_domains:
                result.added.append({
                    'type': 'domain',
                    'name': name,
                    'data': new_domains[name]
                })

        # Find deleted domains
        for name in old_domains:
            if name not in new_domains:
                result.deleted.append({
                    'type': 'domain',
                    'name': name,
                    'data': old_domains[name]
                })

        # Find modified domains
        for name in old_domains:
            if name in new_domains:
                old_vars = {v['name']: v for v in old_domains[name].get('variables', [])}
                new_vars = {v['name']: v for v in new_domains[name].get('variables', [])}

                # Check for variable changes
                for var_name in new_vars:
                    if var_name not in old_vars:
                        result.added.append({
                            'type': 'variable',
                            'domain': name,
                            'name': var_name,
                            'data': new_vars[var_name]
                        })
                    elif new_vars[var_name] != old_vars[var_name]:
                        result.modified.append({
                            'type': 'variable',
                            'domain': name,
                            'name': var_name,
                            'old': old_vars[var_name],
                            'new': new_vars[var_name],
                            'changes': self._get_field_changes(
                                old_vars[var_name],
                                new_vars[var_name]
                            )
                        })
                    else:
                        result.unchanged_count += 1

                for var_name in old_vars:
                    if var_name not in new_vars:
                        result.deleted.append({
                            'type': 'variable',
                            'domain': name,
                            'name': var_name,
                            'data': old_vars[var_name]
                        })

        return result

    def _get_field_changes(
        self,
        old_item: Dict[str, Any],
        new_item: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Get list of changed fields between two items."""
        changes = []
        all_keys = set(old_item.keys()) | set(new_item.keys())

        for key in all_keys:
            old_val = old_item.get(key)
            new_val = new_item.get(key)
            if old_val != new_val:
                changes.append({
                    'field': key,
                    'old': old_val,
                    'new': new_val
                })

        return changes

    def rollback(self, version_id: str, user: str = "system") -> Optional[Dict[str, Any]]:
        """
        Rollback to a previous version.

        Creates a new version with the content from the specified version.

        Args:
            version_id: Version to rollback to
            user: User performing the rollback

        Returns:
            The restored content, or None if version not found
        """
        result = self.get_version(version_id)
        if not result:
            logger.warning(f"Version {version_id} not found for rollback")
            return None

        version, content = result

        # Create a new version with the old content
        self.create_version(
            content=content,
            comment=f"Rollback to version {version.version_number}",
            user=user,
            changes=[MetadataChange(
                entity_type="metadata",
                entity_id="all",
                change_type=ChangeType.MODIFIED,
                old_value=f"current",
                new_value=f"v{version.version_number}",
                user=user,
                comment=f"Rollback to version {version.version_number}"
            )]
        )

        logger.info(f"Rolled back to version {version_id} (v{version.version_number})")
        return content

    # Approval workflow methods
    def set_approval_status(
        self,
        entity_type: str,
        entity_id: str,
        status: str,
        user: str,
        comment: Optional[str] = None
    ):
        """
        Set approval status for an entity.

        Args:
            entity_type: Type of entity (variable, codelist, domain)
            entity_id: Entity identifier
            status: New status (pending, approved, rejected)
            user: User setting the status
            comment: Optional comment
        """
        now = datetime.now().isoformat()

        with sqlite3.connect(self.db_path) as conn:
            # Check if approval record exists
            cursor = conn.execute("""
                SELECT id FROM approvals
                WHERE entity_type = ? AND entity_id = ?
                ORDER BY created_at DESC
                LIMIT 1
            """, (entity_type, entity_id))

            row = cursor.fetchone()

            if row and status == 'pending':
                # Update existing pending record
                conn.execute("""
                    UPDATE approvals
                    SET status = ?, comment = ?
                    WHERE id = ?
                """, (status, comment, row[0]))
            else:
                # Insert new approval record
                conn.execute("""
                    INSERT INTO approvals (
                        entity_type, entity_id, status,
                        reviewed_by, reviewed_at, comment, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    entity_type, entity_id, status,
                    user if status != 'pending' else None,
                    now if status != 'pending' else None,
                    comment, now
                ))

        # Record change
        change_type = ChangeType.APPROVED if status == 'approved' else \
                      ChangeType.REJECTED if status == 'rejected' else \
                      ChangeType.CREATED

        self.record_change(MetadataChange(
            entity_type=entity_type,
            entity_id=entity_id,
            change_type=change_type,
            field_name='status',
            old_value=None,
            new_value=status,
            user=user,
            comment=comment
        ))

    def get_pending_approvals(self) -> List[Dict[str, Any]]:
        """Get all items pending approval."""
        pending = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM approvals
                WHERE status = 'pending'
                ORDER BY created_at DESC
            """)

            for row in cursor:
                pending.append({
                    'id': row['id'],
                    'entity_type': row['entity_type'],
                    'entity_id': row['entity_id'],
                    'status': row['status'],
                    'comment': row['comment'],
                    'created_at': row['created_at']
                })

        return pending

    def get_approval_history(
        self,
        entity_type: Optional[str] = None,
        entity_id: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get approval history for entities."""
        query = "SELECT * FROM approvals WHERE 1=1"
        params = []

        if entity_type:
            query += " AND entity_type = ?"
            params.append(entity_type)
        if entity_id:
            query += " AND entity_id = ?"
            params.append(entity_id)

        query += " ORDER BY created_at DESC"

        history = []
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(query, params)

            for row in cursor:
                history.append({
                    'id': row['id'],
                    'entity_type': row['entity_type'],
                    'entity_id': row['entity_id'],
                    'status': row['status'],
                    'reviewed_by': row['reviewed_by'],
                    'reviewed_at': row['reviewed_at'],
                    'comment': row['comment'],
                    'created_at': row['created_at']
                })

        return history

    def get_statistics(self) -> Dict[str, Any]:
        """Get version control statistics."""
        with sqlite3.connect(self.db_path) as conn:
            # Version count
            version_count = conn.execute(
                "SELECT COUNT(*) FROM versions"
            ).fetchone()[0]

            # Change count
            change_count = conn.execute(
                "SELECT COUNT(*) FROM changes"
            ).fetchone()[0]

            # Changes by type
            changes_by_type = {}
            cursor = conn.execute("""
                SELECT change_type, COUNT(*) as cnt
                FROM changes
                GROUP BY change_type
            """)
            for row in cursor:
                changes_by_type[row[0]] = row[1]

            # Approval stats
            approval_stats = {}
            cursor = conn.execute("""
                SELECT status, COUNT(*) as cnt
                FROM approvals
                GROUP BY status
            """)
            for row in cursor:
                approval_stats[row[0]] = row[1]

            # Recent activity
            recent_changes = conn.execute("""
                SELECT COUNT(*) FROM changes
                WHERE timestamp > datetime('now', '-7 days')
            """).fetchone()[0]

        return {
            'total_versions': version_count,
            'total_changes': change_count,
            'changes_by_type': changes_by_type,
            'approval_stats': approval_stats,
            'recent_changes_7d': recent_changes
        }
