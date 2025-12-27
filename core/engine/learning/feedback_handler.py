"""
Feedback Handler - Process user feedback on query results.

Enables continuous learning by incorporating user corrections and confirmations.
"""

import json
import sqlite3
import uuid
from datetime import datetime
from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from pathlib import Path
from enum import Enum


class FeedbackType(Enum):
    """Types of user feedback."""
    CONFIRM = "CONFIRM"          # Result was correct
    CORRECT = "CORRECT"          # User provided correction
    REJECT = "REJECT"            # Result was wrong (no correction)
    CLARIFY = "CLARIFY"          # User provided clarification
    RATE = "RATE"                # Star rating


@dataclass
class FeedbackResult:
    """Result of processing feedback."""
    id: str
    processed: bool
    action_taken: str
    learning_updated: bool
    message: str


class FeedbackHandler:
    """
    Handle user feedback on query results.

    Stores feedback and updates learning examples based on confirmations.
    """

    def __init__(self, db_path: str = "data/learning.db"):
        """
        Initialize Feedback Handler.

        Args:
            db_path: Path to learning database
        """
        self.db_path = Path(db_path)
        self._init_database()

    def _init_database(self):
        """Initialize feedback tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS feedback (
                    id TEXT PRIMARY KEY,
                    query_id TEXT NOT NULL,
                    question TEXT NOT NULL,
                    original_sql TEXT,
                    original_result TEXT,
                    feedback_type TEXT NOT NULL,
                    user_input TEXT,
                    corrected_sql TEXT,
                    rating INTEGER,
                    user_id TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processed INTEGER DEFAULT 0,
                    processed_at TIMESTAMP,
                    action_taken TEXT
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_feedback_query
                ON feedback(query_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_feedback_type
                ON feedback(feedback_type)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_feedback_processed
                ON feedback(processed)
            """)

    def submit_feedback(
        self,
        query_id: str,
        question: str,
        feedback_type: FeedbackType,
        original_sql: Optional[str] = None,
        original_result: Optional[Any] = None,
        user_input: Optional[str] = None,
        corrected_sql: Optional[str] = None,
        rating: Optional[int] = None,
        user_id: Optional[str] = None
    ) -> FeedbackResult:
        """
        Submit user feedback on a query result.

        Args:
            query_id: ID of the original query
            question: Original question
            feedback_type: Type of feedback
            original_sql: SQL that was executed
            original_result: Result that was returned
            user_input: Free-form user input
            corrected_sql: User-provided SQL correction
            rating: Star rating (1-5)
            user_id: User who provided feedback

        Returns:
            FeedbackResult with processing status
        """
        feedback_id = str(uuid.uuid4())

        # Serialize result if needed
        result_json = None
        if original_result is not None:
            try:
                if hasattr(original_result, 'to_dict'):
                    result_json = json.dumps(original_result.to_dict('records'))
                else:
                    result_json = json.dumps(original_result, default=str)
            except Exception:
                result_json = str(original_result)

        # Store feedback
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO feedback
                (id, query_id, question, original_sql, original_result,
                 feedback_type, user_input, corrected_sql, rating, user_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                feedback_id,
                query_id,
                question,
                original_sql,
                result_json,
                feedback_type.value,
                user_input,
                corrected_sql,
                rating,
                user_id
            ))

        # Process feedback immediately
        return self._process_feedback(
            feedback_id,
            question,
            feedback_type,
            original_sql,
            corrected_sql,
            rating
        )

    def _process_feedback(
        self,
        feedback_id: str,
        question: str,
        feedback_type: FeedbackType,
        original_sql: Optional[str],
        corrected_sql: Optional[str],
        rating: Optional[int]
    ) -> FeedbackResult:
        """Process feedback and update learning."""
        action_taken = "stored"
        learning_updated = False
        message = "Feedback recorded"

        if feedback_type == FeedbackType.CONFIRM:
            # High-quality confirmation - could promote to verified
            if original_sql:
                action_taken = "marked_for_verification"
                message = "Marked for verification as a learning example"
                learning_updated = True

        elif feedback_type == FeedbackType.CORRECT:
            if corrected_sql:
                action_taken = "correction_stored"
                message = "Correction stored for review"
                learning_updated = True
            else:
                action_taken = "correction_incomplete"
                message = "Correction noted but no SQL provided"

        elif feedback_type == FeedbackType.REJECT:
            action_taken = "rejection_logged"
            message = "Rejection logged for analysis"

        elif feedback_type == FeedbackType.CLARIFY:
            action_taken = "clarification_stored"
            message = "Clarification stored"

        elif feedback_type == FeedbackType.RATE:
            if rating is not None:
                if rating >= 4:
                    action_taken = "high_rating_noted"
                    message = f"High rating ({rating}/5) noted"
                elif rating <= 2:
                    action_taken = "low_rating_flagged"
                    message = f"Low rating ({rating}/5) flagged for review"
                else:
                    action_taken = "rating_stored"
                    message = f"Rating ({rating}/5) stored"

        # Mark as processed
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE feedback
                SET processed = 1,
                    processed_at = CURRENT_TIMESTAMP,
                    action_taken = ?
                WHERE id = ?
            """, (action_taken, feedback_id))

        return FeedbackResult(
            id=feedback_id,
            processed=True,
            action_taken=action_taken,
            learning_updated=learning_updated,
            message=message
        )

    def get_feedback(self, feedback_id: str) -> Optional[Dict[str, Any]]:
        """
        Get feedback by ID.

        Args:
            feedback_id: Feedback ID

        Returns:
            Feedback record or None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM feedback WHERE id = ?",
                (feedback_id,)
            )
            row = cursor.fetchone()
            return dict(row) if row else None

    def get_feedback_for_query(self, query_id: str) -> List[Dict[str, Any]]:
        """
        Get all feedback for a query.

        Args:
            query_id: Query ID

        Returns:
            List of feedback records
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute(
                "SELECT * FROM feedback WHERE query_id = ? ORDER BY created_at DESC",
                (query_id,)
            )
            return [dict(row) for row in cursor.fetchall()]

    def get_pending_corrections(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Get pending corrections for review.

        Args:
            limit: Maximum number of results

        Returns:
            List of correction records
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM feedback
                WHERE feedback_type = 'CORRECT'
                  AND corrected_sql IS NOT NULL
                ORDER BY created_at DESC
                LIMIT ?
            """, (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_low_rated_queries(
        self,
        max_rating: int = 2,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get queries with low ratings.

        Args:
            max_rating: Maximum rating to include
            limit: Maximum number of results

        Returns:
            List of low-rated query feedback
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM feedback
                WHERE feedback_type = 'RATE'
                  AND rating <= ?
                ORDER BY rating ASC, created_at DESC
                LIMIT ?
            """, (max_rating, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_confirmation_candidates(
        self,
        min_confirmations: int = 3,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get queries with multiple confirmations (candidates for verification).

        Args:
            min_confirmations: Minimum number of confirmations
            limit: Maximum number of results

        Returns:
            List of frequently confirmed queries
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT question, original_sql, COUNT(*) as confirmation_count
                FROM feedback
                WHERE feedback_type = 'CONFIRM'
                GROUP BY question, original_sql
                HAVING COUNT(*) >= ?
                ORDER BY confirmation_count DESC
                LIMIT ?
            """, (min_confirmations, limit))
            return [dict(row) for row in cursor.fetchall()]

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get feedback statistics.

        Returns:
            Statistics dictionary
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            total = conn.execute(
                "SELECT COUNT(*) as count FROM feedback"
            ).fetchone()["count"]

            by_type = conn.execute("""
                SELECT feedback_type, COUNT(*) as count
                FROM feedback
                GROUP BY feedback_type
            """).fetchall()

            avg_rating = conn.execute("""
                SELECT AVG(rating) as avg
                FROM feedback
                WHERE rating IS NOT NULL
            """).fetchone()["avg"]

            processed = conn.execute("""
                SELECT COUNT(*) as count
                FROM feedback
                WHERE processed = 1
            """).fetchone()["count"]

            recent_7d = conn.execute("""
                SELECT COUNT(*) as count
                FROM feedback
                WHERE created_at >= datetime('now', '-7 days')
            """).fetchone()["count"]

            return {
                "total_feedback": total,
                "by_type": {row["feedback_type"]: row["count"] for row in by_type},
                "average_rating": round(avg_rating, 2) if avg_rating else None,
                "processed_count": processed,
                "unprocessed_count": total - processed,
                "last_7_days": recent_7d
            }

    def export_for_training(
        self,
        feedback_types: Optional[List[FeedbackType]] = None,
        min_rating: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Export feedback for training data.

        Args:
            feedback_types: Filter by types (default: CONFIRM, CORRECT)
            min_rating: Minimum rating for RATE type

        Returns:
            List of training-ready examples
        """
        if feedback_types is None:
            feedback_types = [FeedbackType.CONFIRM, FeedbackType.CORRECT]

        type_values = [t.value for t in feedback_types]

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            placeholders = ','.join('?' * len(type_values))
            cursor = conn.execute(f"""
                SELECT question, original_sql, corrected_sql, feedback_type, rating
                FROM feedback
                WHERE feedback_type IN ({placeholders})
                ORDER BY created_at DESC
            """, type_values)

            examples = []
            for row in cursor.fetchall():
                # Use corrected SQL if available, otherwise original
                sql = row["corrected_sql"] or row["original_sql"]

                if sql:
                    # Filter by rating if specified
                    if min_rating and row["rating"] and row["rating"] < min_rating:
                        continue

                    examples.append({
                        "question": row["question"],
                        "sql": sql,
                        "source": "feedback",
                        "feedback_type": row["feedback_type"],
                        "verified": row["feedback_type"] == "CONFIRM"
                    })

            return examples

    def clear_all(self):
        """Clear all feedback (for testing)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM feedback")
