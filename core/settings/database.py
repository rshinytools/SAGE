"""
Settings Database
==================
SQLite storage for platform settings with audit trail.
"""

import sqlite3
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Optional
from contextlib import contextmanager

from .defaults import SETTING_DEFINITIONS, SETTING_CATEGORIES, get_all_defaults
from .schemas import SettingType


class SettingsDB:
    """SQLite database for settings storage."""

    def __init__(self, db_path: Optional[str] = None):
        """Initialize settings database.

        Args:
            db_path: Path to SQLite database. Defaults to data/settings.db
        """
        if db_path is None:
            # Default to data directory
            data_dir = Path(os.getenv("DATA_DIR", "data"))
            data_dir.mkdir(parents=True, exist_ok=True)
            db_path = str(data_dir / "settings.db")

        self.db_path = db_path
        self._init_database()

    @contextmanager
    def _get_connection(self):
        """Get a database connection with row factory."""
        conn = sqlite3.connect(self.db_path)
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
        """Initialize database schema and default values."""
        with self._get_connection() as conn:
            # Create settings table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS settings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    category TEXT NOT NULL,
                    key TEXT NOT NULL,
                    value TEXT,
                    value_type TEXT NOT NULL,
                    is_sensitive INTEGER DEFAULT 0,
                    updated_at TEXT,
                    updated_by TEXT,
                    UNIQUE(category, key)
                )
            """)

            # Create audit table for setting changes
            conn.execute("""
                CREATE TABLE IF NOT EXISTS settings_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    setting_category TEXT NOT NULL,
                    setting_key TEXT NOT NULL,
                    old_value TEXT,
                    new_value TEXT,
                    changed_by TEXT,
                    changed_at TEXT NOT NULL
                )
            """)

            # Create indexes
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_settings_category
                ON settings(category)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_settings_audit_time
                ON settings_audit(changed_at)
            """)

            # Initialize default values for any missing settings
            self._initialize_defaults(conn)

    def _initialize_defaults(self, conn: sqlite3.Connection):
        """Insert default values for settings that don't exist."""
        for category, definitions in SETTING_DEFINITIONS.items():
            for definition in definitions:
                # Check if setting exists
                cursor = conn.execute(
                    "SELECT id FROM settings WHERE category = ? AND key = ?",
                    (category, definition.key)
                )
                if cursor.fetchone() is None:
                    # Insert default value
                    value_json = json.dumps(definition.default_value)
                    conn.execute(
                        """INSERT INTO settings
                           (category, key, value, value_type, is_sensitive, updated_at)
                           VALUES (?, ?, ?, ?, ?, ?)""",
                        (
                            category,
                            definition.key,
                            value_json,
                            definition.value_type.value,
                            1 if definition.is_sensitive else 0,
                            datetime.now().isoformat()
                        )
                    )

    def get_setting(self, category: str, key: str) -> Optional[Dict[str, Any]]:
        """Get a single setting value.

        Args:
            category: Setting category
            key: Setting key

        Returns:
            Setting dict with value and metadata, or None if not found
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT category, key, value, value_type, is_sensitive,
                          updated_at, updated_by
                   FROM settings
                   WHERE category = ? AND key = ?""",
                (category, key)
            )
            row = cursor.fetchone()
            if row:
                return self._row_to_dict(row)
        return None

    def get_category_settings(self, category: str) -> List[Dict[str, Any]]:
        """Get all settings in a category.

        Args:
            category: Setting category

        Returns:
            List of setting dicts
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT category, key, value, value_type, is_sensitive,
                          updated_at, updated_by
                   FROM settings
                   WHERE category = ?
                   ORDER BY key""",
                (category,)
            )
            return [self._row_to_dict(row) for row in cursor.fetchall()]

    def get_all_settings(self) -> Dict[str, List[Dict[str, Any]]]:
        """Get all settings grouped by category.

        Returns:
            Dict with category keys and lists of setting dicts
        """
        with self._get_connection() as conn:
            cursor = conn.execute(
                """SELECT category, key, value, value_type, is_sensitive,
                          updated_at, updated_by
                   FROM settings
                   ORDER BY category, key"""
            )

            result = {}
            for row in cursor.fetchall():
                setting = self._row_to_dict(row)
                category = setting["category"]
                if category not in result:
                    result[category] = []
                result[category].append(setting)

            return result

    def update_setting(
        self,
        category: str,
        key: str,
        value: Any,
        updated_by: Optional[str] = None
    ) -> bool:
        """Update a setting value.

        Args:
            category: Setting category
            key: Setting key
            value: New value
            updated_by: Username making the change

        Returns:
            True if updated, False if setting doesn't exist
        """
        with self._get_connection() as conn:
            # Get current value for audit
            cursor = conn.execute(
                "SELECT value FROM settings WHERE category = ? AND key = ?",
                (category, key)
            )
            row = cursor.fetchone()
            if row is None:
                return False

            old_value = row["value"]
            new_value = json.dumps(value)
            now = datetime.now().isoformat()

            # Update setting
            conn.execute(
                """UPDATE settings
                   SET value = ?, updated_at = ?, updated_by = ?
                   WHERE category = ? AND key = ?""",
                (new_value, now, updated_by, category, key)
            )

            # Log to audit trail
            conn.execute(
                """INSERT INTO settings_audit
                   (setting_category, setting_key, old_value, new_value,
                    changed_by, changed_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (category, key, old_value, new_value, updated_by, now)
            )

            return True

    def reset_to_defaults(self, category: Optional[str] = None, updated_by: Optional[str] = None):
        """Reset settings to default values.

        Args:
            category: Specific category to reset, or None for all
            updated_by: Username making the change
        """
        defaults = get_all_defaults()
        categories = [category] if category else list(defaults.keys())

        for cat in categories:
            if cat in defaults:
                for key, value in defaults[cat].items():
                    self.update_setting(cat, key, value, updated_by)

    def get_audit_history(
        self,
        category: Optional[str] = None,
        key: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get setting change history.

        Args:
            category: Filter by category
            key: Filter by key
            limit: Maximum records to return

        Returns:
            List of audit entries
        """
        with self._get_connection() as conn:
            query = """SELECT setting_category, setting_key, old_value, new_value,
                              changed_by, changed_at
                       FROM settings_audit"""
            params = []

            conditions = []
            if category:
                conditions.append("setting_category = ?")
                params.append(category)
            if key:
                conditions.append("setting_key = ?")
                params.append(key)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY changed_at DESC LIMIT ?"
            params.append(limit)

            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def _row_to_dict(self, row: sqlite3.Row) -> Dict[str, Any]:
        """Convert a database row to a setting dict."""
        value = json.loads(row["value"]) if row["value"] else None

        return {
            "category": row["category"],
            "key": row["key"],
            "value": value,
            "value_type": row["value_type"],
            "is_sensitive": bool(row["is_sensitive"]),
            "updated_at": row["updated_at"],
            "updated_by": row["updated_by"],
        }
