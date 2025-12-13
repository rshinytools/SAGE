"""
Project Tracker Database Module

Provides all database operations for the SAGE project tracking system.
Uses SQLite for persistence with full CRUD operations for phases, tasks,
subtasks, activity logging, and milestones.
"""

import sqlite3
import json
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from contextlib import contextmanager


class TrackerDB:
    """
    Database manager for the SAGE Project Tracker.

    Handles all database operations including:
    - Phase management (CRUD, progress calculation)
    - Task management (CRUD, status updates, notes)
    - Subtask management (CRUD, completion tracking)
    - Activity logging (all changes tracked)
    - Milestone tracking
    - Progress calculations and statistics
    """

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize the TrackerDB.

        Args:
            db_path: Path to SQLite database file. If None, uses default location.
        """
        if db_path is None:
            # Default path relative to project root
            project_root = Path(__file__).parent.parent.parent
            db_path = project_root / "tracker" / "project_tracker.db"

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

            # Phases table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS phases (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    order_index INTEGER NOT NULL,
                    status TEXT DEFAULT 'not_started',
                    progress_percent REAL DEFAULT 0,
                    estimated_hours INTEGER,
                    actual_hours INTEGER DEFAULT 0,
                    start_date TEXT,
                    end_date TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Tasks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    phase_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    description TEXT,
                    status TEXT DEFAULT 'pending',
                    priority TEXT DEFAULT 'medium',
                    assignee TEXT,
                    estimated_hours REAL,
                    actual_hours REAL DEFAULT 0,
                    dependencies TEXT,
                    notes TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    started_at TEXT,
                    completed_at TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (phase_id) REFERENCES phases(id) ON DELETE CASCADE
                )
            ''')

            # Subtasks table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS subtasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER NOT NULL,
                    name TEXT NOT NULL,
                    status TEXT DEFAULT 'pending',
                    completed_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE
                )
            ''')

            # Activity log table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS activity_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER,
                    phase_id INTEGER,
                    action TEXT NOT NULL,
                    details TEXT,
                    user TEXT DEFAULT 'system',
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE SET NULL,
                    FOREIGN KEY (phase_id) REFERENCES phases(id) ON DELETE SET NULL
                )
            ''')

            # Milestones table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS milestones (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    phase_id INTEGER,
                    target_date TEXT,
                    achieved_date TEXT,
                    status TEXT DEFAULT 'pending',
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (phase_id) REFERENCES phases(id) ON DELETE SET NULL
                )
            ''')

            # Create indexes for performance
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_phase ON tasks(phase_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_subtasks_task ON subtasks(task_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_activity_timestamp ON activity_log(timestamp)')

    # ==================== PHASE OPERATIONS ====================

    def create_phase(self, name: str, description: str = "", order_index: int = 0,
                     estimated_hours: int = None) -> int:
        """Create a new phase."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO phases (name, description, order_index, estimated_hours)
                VALUES (?, ?, ?, ?)
            ''', (name, description, order_index, estimated_hours))
            phase_id = cursor.lastrowid

            self._log_activity(conn, phase_id=phase_id, action='phase_created',
                             details=f'Phase "{name}" created')
            return phase_id

    def get_phase(self, phase_id: int) -> Optional[Dict[str, Any]]:
        """Get a phase by ID."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM phases WHERE id = ?', (phase_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_all_phases(self) -> List[Dict[str, Any]]:
        """Get all phases ordered by order_index."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM phases ORDER BY order_index')
            return [dict(row) for row in cursor.fetchall()]

    def update_phase(self, phase_id: int, **kwargs) -> bool:
        """Update phase fields."""
        allowed_fields = {'name', 'description', 'order_index', 'status',
                         'estimated_hours', 'actual_hours', 'start_date', 'end_date'}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}

        if not updates:
            return False

        updates['updated_at'] = datetime.now().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()
            set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
            cursor.execute(f'UPDATE phases SET {set_clause} WHERE id = ?',
                          (*updates.values(), phase_id))

            if cursor.rowcount > 0:
                self._log_activity(conn, phase_id=phase_id, action='phase_updated',
                                 details=f'Updated: {list(updates.keys())}')
                return True
            return False

    def update_phase_status(self, phase_id: int, status: str, user: str = 'system') -> bool:
        """Update phase status with automatic date tracking."""
        now = datetime.now().isoformat()
        updates = {'status': status, 'updated_at': now}

        if status == 'in_progress':
            updates['start_date'] = now
        elif status == 'completed':
            updates['end_date'] = now

        with self._get_connection() as conn:
            cursor = conn.cursor()
            set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
            cursor.execute(f'UPDATE phases SET {set_clause} WHERE id = ?',
                          (*updates.values(), phase_id))

            if cursor.rowcount > 0:
                self._log_activity(conn, phase_id=phase_id, action='status_changed',
                                 details=f'Status changed to {status}', user=user)
                return True
            return False

    # ==================== TASK OPERATIONS ====================

    def create_task(self, phase_id: int, name: str, description: str = "",
                    priority: str = 'medium', estimated_hours: float = None,
                    dependencies: List[int] = None) -> int:
        """Create a new task."""
        deps_json = json.dumps(dependencies) if dependencies else None

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO tasks (phase_id, name, description, priority,
                                  estimated_hours, dependencies)
                VALUES (?, ?, ?, ?, ?, ?)
            ''', (phase_id, name, description, priority, estimated_hours, deps_json))
            task_id = cursor.lastrowid

            self._log_activity(conn, task_id=task_id, phase_id=phase_id,
                             action='task_created', details=f'Task "{name}" created')
            return task_id

    def get_task(self, task_id: int) -> Optional[Dict[str, Any]]:
        """Get a task by ID with its subtasks."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM tasks WHERE id = ?', (task_id,))
            row = cursor.fetchone()
            if not row:
                return None

            task = dict(row)
            task['dependencies'] = json.loads(task['dependencies']) if task['dependencies'] else []

            # Get subtasks
            cursor.execute('SELECT * FROM subtasks WHERE task_id = ? ORDER BY id', (task_id,))
            task['subtasks'] = [dict(r) for r in cursor.fetchall()]

            return task

    def get_tasks_by_phase(self, phase_id: int) -> List[Dict[str, Any]]:
        """Get all tasks for a phase with their subtasks."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT * FROM tasks WHERE phase_id = ? ORDER BY id', (phase_id,))
            tasks = []
            for row in cursor.fetchall():
                task = dict(row)
                task['dependencies'] = json.loads(task['dependencies']) if task['dependencies'] else []

                # Get subtasks
                cursor.execute('SELECT * FROM subtasks WHERE task_id = ? ORDER BY id', (task['id'],))
                task['subtasks'] = [dict(r) for r in cursor.fetchall()]
                tasks.append(task)

            return tasks

    def get_all_tasks(self) -> List[Dict[str, Any]]:
        """Get all tasks across all phases."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT t.*, p.name as phase_name, p.order_index as phase_order
                FROM tasks t
                JOIN phases p ON t.phase_id = p.id
                ORDER BY p.order_index, t.id
            ''')
            tasks = []
            for row in cursor.fetchall():
                task = dict(row)
                task['dependencies'] = json.loads(task['dependencies']) if task['dependencies'] else []
                tasks.append(task)
            return tasks

    def update_task(self, task_id: int, user: str = 'system', **kwargs) -> bool:
        """Update task fields."""
        allowed_fields = {'name', 'description', 'status', 'priority', 'assignee',
                         'estimated_hours', 'actual_hours', 'dependencies', 'notes'}
        updates = {k: v for k, v in kwargs.items() if k in allowed_fields}

        if not updates:
            return False

        # Handle dependencies serialization
        if 'dependencies' in updates:
            updates['dependencies'] = json.dumps(updates['dependencies'])

        updates['updated_at'] = datetime.now().isoformat()

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get phase_id for logging
            cursor.execute('SELECT phase_id FROM tasks WHERE id = ?', (task_id,))
            row = cursor.fetchone()
            phase_id = row['phase_id'] if row else None

            set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
            cursor.execute(f'UPDATE tasks SET {set_clause} WHERE id = ?',
                          (*updates.values(), task_id))

            if cursor.rowcount > 0:
                self._log_activity(conn, task_id=task_id, phase_id=phase_id,
                                 action='task_updated', details=f'Updated: {list(kwargs.keys())}',
                                 user=user)
                return True
            return False

    def update_task_status(self, task_id: int, status: str, user: str = 'system') -> bool:
        """Update task status with automatic date tracking."""
        now = datetime.now().isoformat()
        updates = {'status': status, 'updated_at': now}

        if status == 'in_progress':
            updates['started_at'] = now
        elif status == 'completed':
            updates['completed_at'] = now

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get phase_id for logging and progress update
            cursor.execute('SELECT phase_id FROM tasks WHERE id = ?', (task_id,))
            row = cursor.fetchone()
            phase_id = row['phase_id'] if row else None

            set_clause = ', '.join(f'{k} = ?' for k in updates.keys())
            cursor.execute(f'UPDATE tasks SET {set_clause} WHERE id = ?',
                          (*updates.values(), task_id))

            if cursor.rowcount > 0:
                self._log_activity(conn, task_id=task_id, phase_id=phase_id,
                                 action='status_changed', details=f'Status changed to {status}',
                                 user=user)

                # Update phase progress
                if phase_id:
                    self._update_phase_progress(conn, phase_id)

                return True
            return False

    def add_task_note(self, task_id: int, note: str, user: str = 'system') -> bool:
        """Append a note to a task."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get current notes and phase_id
            cursor.execute('SELECT notes, phase_id FROM tasks WHERE id = ?', (task_id,))
            row = cursor.fetchone()
            if not row:
                return False

            current_notes = row['notes'] or ''
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M')
            new_note = f"\n[{timestamp}] {user}: {note}" if current_notes else f"[{timestamp}] {user}: {note}"
            updated_notes = current_notes + new_note

            cursor.execute('UPDATE tasks SET notes = ?, updated_at = ? WHERE id = ?',
                          (updated_notes, datetime.now().isoformat(), task_id))

            self._log_activity(conn, task_id=task_id, phase_id=row['phase_id'],
                             action='note_added', details=note[:100], user=user)
            return True

    # ==================== SUBTASK OPERATIONS ====================

    def create_subtask(self, task_id: int, name: str) -> int:
        """Create a new subtask."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('INSERT INTO subtasks (task_id, name) VALUES (?, ?)',
                          (task_id, name))
            return cursor.lastrowid

    def update_subtask_status(self, subtask_id: int, status: str, user: str = 'system') -> bool:
        """Update subtask status."""
        completed_at = datetime.now().isoformat() if status == 'completed' else None

        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get task_id and phase_id for logging
            cursor.execute('''
                SELECT s.task_id, t.phase_id
                FROM subtasks s
                JOIN tasks t ON s.task_id = t.id
                WHERE s.id = ?
            ''', (subtask_id,))
            row = cursor.fetchone()
            if not row:
                return False

            cursor.execute('UPDATE subtasks SET status = ?, completed_at = ? WHERE id = ?',
                          (status, completed_at, subtask_id))

            if cursor.rowcount > 0:
                self._log_activity(conn, task_id=row['task_id'], phase_id=row['phase_id'],
                                 action='subtask_status_changed',
                                 details=f'Subtask {subtask_id} marked {status}', user=user)

                # Update phase progress
                self._update_phase_progress(conn, row['phase_id'])
                return True
            return False

    def delete_subtask(self, subtask_id: int) -> bool:
        """Delete a subtask."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM subtasks WHERE id = ?', (subtask_id,))
            return cursor.rowcount > 0

    # ==================== ACTIVITY LOG ====================

    def _log_activity(self, conn, task_id: int = None, phase_id: int = None,
                     action: str = '', details: str = '', user: str = 'system'):
        """Internal method to log activity."""
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO activity_log (task_id, phase_id, action, details, user)
            VALUES (?, ?, ?, ?, ?)
        ''', (task_id, phase_id, action, details, user))

    def get_recent_activity(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get recent activity log entries."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT a.*, t.name as task_name, p.name as phase_name
                FROM activity_log a
                LEFT JOIN tasks t ON a.task_id = t.id
                LEFT JOIN phases p ON a.phase_id = p.id
                ORDER BY a.timestamp DESC
                LIMIT ?
            ''', (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_activity_by_phase(self, phase_id: int, limit: int = 50) -> List[Dict[str, Any]]:
        """Get activity log for a specific phase."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT a.*, t.name as task_name
                FROM activity_log a
                LEFT JOIN tasks t ON a.task_id = t.id
                WHERE a.phase_id = ?
                ORDER BY a.timestamp DESC
                LIMIT ?
            ''', (phase_id, limit))
            return [dict(row) for row in cursor.fetchall()]

    # ==================== MILESTONE OPERATIONS ====================

    def create_milestone(self, name: str, description: str = "", phase_id: int = None,
                        target_date: str = None) -> int:
        """Create a new milestone."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT INTO milestones (name, description, phase_id, target_date)
                VALUES (?, ?, ?, ?)
            ''', (name, description, phase_id, target_date))
            return cursor.lastrowid

    def get_all_milestones(self) -> List[Dict[str, Any]]:
        """Get all milestones."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT m.*, p.name as phase_name
                FROM milestones m
                LEFT JOIN phases p ON m.phase_id = p.id
                ORDER BY m.target_date
            ''')
            return [dict(row) for row in cursor.fetchall()]

    def update_milestone_status(self, milestone_id: int, status: str) -> bool:
        """Update milestone status."""
        achieved_date = datetime.now().isoformat() if status == 'achieved' else None

        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('UPDATE milestones SET status = ?, achieved_date = ? WHERE id = ?',
                          (status, achieved_date, milestone_id))
            return cursor.rowcount > 0

    # ==================== PROGRESS CALCULATIONS ====================

    def _update_phase_progress(self, conn, phase_id: int):
        """Update phase progress based on task completion."""
        cursor = conn.cursor()

        # Count total tasks and completed tasks
        cursor.execute('''
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed
            FROM tasks WHERE phase_id = ?
        ''', (phase_id,))
        row = cursor.fetchone()

        if row['total'] > 0:
            progress = (row['completed'] / row['total']) * 100
        else:
            progress = 0

        # Update phase status based on progress
        if progress == 0:
            status = 'not_started'
        elif progress == 100:
            status = 'completed'
        else:
            status = 'in_progress'

        cursor.execute('''
            UPDATE phases
            SET progress_percent = ?, status = ?, updated_at = ?
            WHERE id = ?
        ''', (progress, status, datetime.now().isoformat(), phase_id))

    def recalculate_all_progress(self):
        """Recalculate progress for all phases."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('SELECT id FROM phases')
            for row in cursor.fetchall():
                self._update_phase_progress(conn, row['id'])

    def get_overall_progress(self) -> Dict[str, Any]:
        """Get overall project progress statistics."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Phase statistics
            cursor.execute('''
                SELECT
                    COUNT(*) as total_phases,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_phases,
                    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_phases,
                    AVG(progress_percent) as avg_progress
                FROM phases
            ''')
            phase_stats = dict(cursor.fetchone())

            # Task statistics
            cursor.execute('''
                SELECT
                    COUNT(*) as total_tasks,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                    SUM(CASE WHEN status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_tasks,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_tasks,
                    SUM(CASE WHEN status = 'blocked' THEN 1 ELSE 0 END) as blocked_tasks
                FROM tasks
            ''')
            task_stats = dict(cursor.fetchone())

            # Subtask statistics
            cursor.execute('''
                SELECT
                    COUNT(*) as total_subtasks,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_subtasks
                FROM subtasks
            ''')
            subtask_stats = dict(cursor.fetchone())

            # Calculate overall progress (weighted by tasks)
            total_tasks = task_stats['total_tasks'] or 0
            completed_tasks = task_stats['completed_tasks'] or 0
            overall_progress = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0

            return {
                'overall_progress': round(overall_progress, 1),
                'phases': phase_stats,
                'tasks': task_stats,
                'subtasks': subtask_stats
            }

    def get_next_steps(self, limit: int = 5) -> List[Dict[str, Any]]:
        """Get recommended next tasks to work on."""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # Get pending tasks from the earliest incomplete phase
            # prioritized by priority level and dependencies
            cursor.execute('''
                SELECT t.*, p.name as phase_name, p.order_index
                FROM tasks t
                JOIN phases p ON t.phase_id = p.id
                WHERE t.status = 'pending'
                AND p.id = (
                    SELECT id FROM phases
                    WHERE status != 'completed'
                    ORDER BY order_index
                    LIMIT 1
                )
                ORDER BY
                    CASE t.priority
                        WHEN 'critical' THEN 1
                        WHEN 'high' THEN 2
                        WHEN 'medium' THEN 3
                        WHEN 'low' THEN 4
                    END,
                    t.id
                LIMIT ?
            ''', (limit,))

            tasks = []
            for row in cursor.fetchall():
                task = dict(row)
                task['dependencies'] = json.loads(task['dependencies']) if task['dependencies'] else []

                # Check if dependencies are met
                if task['dependencies']:
                    cursor.execute('''
                        SELECT COUNT(*) as incomplete
                        FROM tasks
                        WHERE id IN ({}) AND status != 'completed'
                    '''.format(','.join('?' * len(task['dependencies']))),
                    task['dependencies'])
                    dep_row = cursor.fetchone()
                    task['dependencies_met'] = dep_row['incomplete'] == 0
                else:
                    task['dependencies_met'] = True

                tasks.append(task)

            return tasks

    # ==================== EXPORT FUNCTIONS ====================

    def export_to_dict(self) -> Dict[str, Any]:
        """Export entire project state to dictionary."""
        return {
            'phases': self.get_all_phases(),
            'tasks': self.get_all_tasks(),
            'milestones': self.get_all_milestones(),
            'progress': self.get_overall_progress(),
            'exported_at': datetime.now().isoformat()
        }

    def get_phase_summary(self) -> List[Dict[str, Any]]:
        """Get summary of all phases with task counts."""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
                SELECT
                    p.*,
                    COUNT(t.id) as total_tasks,
                    SUM(CASE WHEN t.status = 'completed' THEN 1 ELSE 0 END) as completed_tasks,
                    SUM(CASE WHEN t.status = 'in_progress' THEN 1 ELSE 0 END) as in_progress_tasks
                FROM phases p
                LEFT JOIN tasks t ON p.id = t.phase_id
                GROUP BY p.id
                ORDER BY p.order_index
            ''')
            return [dict(row) for row in cursor.fetchall()]


# Singleton instance for easy access
_tracker_instance: Optional[TrackerDB] = None

def get_tracker(db_path: Optional[str] = None) -> TrackerDB:
    """Get or create the global TrackerDB instance."""
    global _tracker_instance
    if _tracker_instance is None:
        _tracker_instance = TrackerDB(db_path)
    return _tracker_instance
