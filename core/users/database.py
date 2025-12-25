"""
User Database Module
====================

SQLite database operations for the SAGE user management system.
"""

import sqlite3
import json
import hashlib
import os
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Tuple
from contextlib import contextmanager

from .models import User, Session


class UserDB:
    """
    Database manager for SAGE User Management.

    Uses SQLite for OLTP (transactional) user operations,
    separate from DuckDB which is used for analytics.
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the UserDB.

        Args:
            db_path: Path to SQLite database file. If None, uses default location.
        """
        if db_path is None:
            db_path = os.getenv("USERS_DB_PATH", None)
            if db_path is None:
                project_root = Path(__file__).parent.parent.parent
                db_path = project_root / "data" / "users.db"

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

            # Users table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    username TEXT UNIQUE NOT NULL,
                    email TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'user',
                    permissions TEXT,
                    is_active INTEGER DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT,
                    last_login TEXT,
                    failed_login_attempts INTEGER DEFAULT 0,
                    locked_until TEXT,
                    password_changed_at TEXT,
                    must_change_password INTEGER DEFAULT 0
                )
            ''')

            # Sessions table (persistent token management)
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS sessions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    token_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    revoked INTEGER DEFAULT 0,
                    revoked_at TEXT,
                    ip_address TEXT,
                    user_agent TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            ''')

            # Password reset tokens
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS password_reset_tokens (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    token_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    expires_at TEXT NOT NULL,
                    used INTEGER DEFAULT 0,
                    used_at TEXT,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                )
            ''')

            # Create indexes
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_users_is_active ON users(is_active)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_user_id ON sessions(user_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_token_hash ON sessions(token_hash)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_sessions_expires_at ON sessions(expires_at)')

    # ==================== USER CRUD OPERATIONS ====================

    def create_user(
        self,
        username: str,
        email: str,
        password_hash: str,
        role: str = "user",
        permissions: Optional[List[str]] = None,
        must_change_password: bool = False,
    ) -> User:
        """
        Create a new user.

        Args:
            username: Unique username
            email: Unique email address
            password_hash: bcrypt hashed password
            role: User role (admin, user, viewer)
            permissions: List of permission strings
            must_change_password: Force password change on first login

        Returns:
            Created User object

        Raises:
            ValueError: If username or email already exists
        """
        user_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()
        permissions_json = json.dumps(permissions or [])

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute('''
                    INSERT INTO users (
                        id, username, email, password_hash, role, permissions,
                        is_active, created_at, must_change_password
                    ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, ?)
                ''', (
                    user_id, username, email, password_hash, role,
                    permissions_json, now, 1 if must_change_password else 0
                ))
            except sqlite3.IntegrityError as e:
                if "username" in str(e).lower():
                    raise ValueError(f"Username '{username}' already exists")
                elif "email" in str(e).lower():
                    raise ValueError(f"Email '{email}' already exists")
                raise

        return self.get_user_by_id(user_id)

    def get_user_by_id(self, user_id: str) -> Optional[User]:
        """Get a user by their ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            return self._row_to_user(row) if row else None

    def get_user_by_username(self, username: str) -> Optional[User]:
        """Get a user by their username."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
            row = cursor.fetchone()
            return self._row_to_user(row) if row else None

    def get_user_by_email(self, email: str) -> Optional[User]:
        """Get a user by their email."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM users WHERE email = ?', (email,))
            row = cursor.fetchone()
            return self._row_to_user(row) if row else None

    def list_users(
        self,
        page: int = 1,
        page_size: int = 50,
        include_inactive: bool = False,
    ) -> Tuple[List[User], int]:
        """
        List users with pagination.

        Args:
            page: Page number (1-indexed)
            page_size: Number of users per page
            include_inactive: Whether to include inactive users

        Returns:
            Tuple of (list of users, total count)
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()

            where_clause = "" if include_inactive else "WHERE is_active = 1"

            # Get total count
            cursor.execute(f'SELECT COUNT(*) as total FROM users {where_clause}')
            total = cursor.fetchone()['total']

            # Get paginated results
            offset = (page - 1) * page_size
            cursor.execute(f'''
                SELECT * FROM users {where_clause}
                ORDER BY created_at DESC
                LIMIT ? OFFSET ?
            ''', (page_size, offset))

            users = [self._row_to_user(row) for row in cursor.fetchall()]
            return users, total

    def update_user(
        self,
        user_id: str,
        email: Optional[str] = None,
        role: Optional[str] = None,
        permissions: Optional[List[str]] = None,
        is_active: Optional[bool] = None,
    ) -> Optional[User]:
        """
        Update a user's information.

        Args:
            user_id: ID of user to update
            email: New email (optional)
            role: New role (optional)
            permissions: New permissions list (optional)
            is_active: New active status (optional)

        Returns:
            Updated User object or None if not found
        """
        updates = []
        params = []

        if email is not None:
            updates.append("email = ?")
            params.append(email)

        if role is not None:
            updates.append("role = ?")
            params.append(role)

        if permissions is not None:
            updates.append("permissions = ?")
            params.append(json.dumps(permissions))

        if is_active is not None:
            updates.append("is_active = ?")
            params.append(1 if is_active else 0)

        if not updates:
            return self.get_user_by_id(user_id)

        updates.append("updated_at = ?")
        params.append(datetime.utcnow().isoformat())
        params.append(user_id)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            try:
                cursor.execute(f'''
                    UPDATE users SET {", ".join(updates)} WHERE id = ?
                ''', params)
            except sqlite3.IntegrityError as e:
                if "email" in str(e).lower():
                    raise ValueError(f"Email '{email}' already exists")
                raise

        return self.get_user_by_id(user_id)

    def delete_user(self, user_id: str) -> bool:
        """
        Delete a user and all their sessions.

        Args:
            user_id: ID of user to delete

        Returns:
            True if user was deleted, False if not found
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM users WHERE id = ?', (user_id,))
            return cursor.rowcount > 0

    # ==================== PASSWORD OPERATIONS ====================

    def update_password(
        self,
        user_id: str,
        password_hash: str,
        must_change: bool = False,
    ) -> bool:
        """
        Update a user's password.

        Args:
            user_id: ID of user
            password_hash: New bcrypt password hash
            must_change: Whether user must change password on next login

        Returns:
            True if updated, False if user not found
        """
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET
                    password_hash = ?,
                    password_changed_at = ?,
                    must_change_password = ?,
                    updated_at = ?
                WHERE id = ?
            ''', (password_hash, now, 1 if must_change else 0, now, user_id))
            return cursor.rowcount > 0

    def record_failed_login(self, user_id: str) -> int:
        """
        Record a failed login attempt.

        Args:
            user_id: ID of user

        Returns:
            New failed login count
        """
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET failed_login_attempts = failed_login_attempts + 1
                WHERE id = ?
            ''', (user_id,))
            cursor.execute(
                'SELECT failed_login_attempts FROM users WHERE id = ?',
                (user_id,)
            )
            row = cursor.fetchone()
            return row['failed_login_attempts'] if row else 0

    def reset_failed_logins(self, user_id: str) -> bool:
        """Reset failed login counter after successful login."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET
                    failed_login_attempts = 0,
                    locked_until = NULL,
                    last_login = ?
                WHERE id = ?
            ''', (now, user_id))
            return cursor.rowcount > 0

    def lock_account(self, user_id: str, duration_minutes: int = 15) -> bool:
        """
        Lock a user account for a specified duration.

        Args:
            user_id: ID of user to lock
            duration_minutes: How long to lock the account

        Returns:
            True if locked, False if user not found
        """
        locked_until = (datetime.utcnow() + timedelta(minutes=duration_minutes)).isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET locked_until = ? WHERE id = ?
            ''', (locked_until, user_id))
            return cursor.rowcount > 0

    def unlock_account(self, user_id: str) -> bool:
        """Unlock a user account."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE users SET locked_until = NULL, failed_login_attempts = 0
                WHERE id = ?
            ''', (user_id,))
            return cursor.rowcount > 0

    def is_account_locked(self, user_id: str) -> bool:
        """Check if a user account is currently locked."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT locked_until FROM users WHERE id = ?', (user_id,))
            row = cursor.fetchone()
            if not row or not row['locked_until']:
                return False
            locked_until = datetime.fromisoformat(row['locked_until'])
            return datetime.utcnow() < locked_until

    # ==================== SESSION OPERATIONS ====================

    def create_session(
        self,
        user_id: str,
        token_hash: str,
        expires_at: datetime,
        ip_address: Optional[str] = None,
        user_agent: Optional[str] = None,
    ) -> str:
        """
        Create a new session for a user.

        Args:
            user_id: ID of the user
            token_hash: SHA256 hash of the JWT token
            expires_at: When the session expires
            ip_address: Client IP address
            user_agent: Client user agent

        Returns:
            Session ID
        """
        session_id = str(uuid.uuid4())
        now = datetime.utcnow().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO sessions (
                    id, user_id, token_hash, created_at, expires_at,
                    ip_address, user_agent
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                session_id, user_id, token_hash, now,
                expires_at.isoformat(), ip_address, user_agent
            ))

        return session_id

    def revoke_session(self, session_id: str) -> bool:
        """Revoke a specific session."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE sessions SET revoked = 1, revoked_at = ? WHERE id = ?
            ''', (now, session_id))
            return cursor.rowcount > 0

    def revoke_session_by_token(self, token_hash: str) -> bool:
        """Revoke a session by its token hash."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE sessions SET revoked = 1, revoked_at = ?
                WHERE token_hash = ?
            ''', (now, token_hash))
            return cursor.rowcount > 0

    def revoke_all_sessions(self, user_id: str) -> int:
        """Revoke all sessions for a user."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                UPDATE sessions SET revoked = 1, revoked_at = ?
                WHERE user_id = ? AND revoked = 0
            ''', (now, user_id))
            return cursor.rowcount

    def is_token_revoked(self, token_hash: str) -> bool:
        """Check if a token has been revoked."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT revoked, expires_at FROM sessions WHERE token_hash = ?
            ''', (token_hash,))
            row = cursor.fetchone()
            if not row:
                return False  # Unknown token, let other validation handle it
            if row['revoked']:
                return True
            # Also check expiration
            expires_at = datetime.fromisoformat(row['expires_at'])
            return datetime.utcnow() > expires_at

    def cleanup_expired_sessions(self) -> int:
        """Remove expired sessions from the database."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                DELETE FROM sessions WHERE expires_at < ?
            ''', (now,))
            return cursor.rowcount

    def get_user_sessions(self, user_id: str) -> List[Session]:
        """Get all active sessions for a user."""
        now = datetime.utcnow().isoformat()
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT * FROM sessions
                WHERE user_id = ? AND revoked = 0 AND expires_at > ?
                ORDER BY created_at DESC
            ''', (user_id, now))
            return [self._row_to_session(row) for row in cursor.fetchall()]

    # ==================== HELPER METHODS ====================

    def _row_to_user(self, row) -> User:
        """Convert a database row to a User model."""
        permissions = json.loads(row['permissions']) if row['permissions'] else []

        return User(
            id=row['id'],
            username=row['username'],
            email=row['email'],
            password_hash=row['password_hash'],
            role=row['role'],
            permissions=permissions,
            is_active=bool(row['is_active']),
            created_at=datetime.fromisoformat(row['created_at']),
            updated_at=datetime.fromisoformat(row['updated_at']) if row['updated_at'] else None,
            last_login=datetime.fromisoformat(row['last_login']) if row['last_login'] else None,
            failed_login_attempts=row['failed_login_attempts'],
            locked_until=datetime.fromisoformat(row['locked_until']) if row['locked_until'] else None,
            password_changed_at=datetime.fromisoformat(row['password_changed_at']) if row['password_changed_at'] else None,
            must_change_password=bool(row['must_change_password']),
        )

    def _row_to_session(self, row) -> Session:
        """Convert a database row to a Session model."""
        return Session(
            id=row['id'],
            user_id=row['user_id'],
            token_hash=row['token_hash'],
            created_at=datetime.fromisoformat(row['created_at']),
            expires_at=datetime.fromisoformat(row['expires_at']),
            revoked=bool(row['revoked']),
            revoked_at=datetime.fromisoformat(row['revoked_at']) if row['revoked_at'] else None,
            ip_address=row['ip_address'],
            user_agent=row['user_agent'],
        )

    def user_count(self) -> int:
        """Get total number of users."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT COUNT(*) as count FROM users')
            return cursor.fetchone()['count']
