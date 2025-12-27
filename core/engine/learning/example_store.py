"""
Example Store - ChromaDB integration for query-SQL pairs.

Stores verified query examples with vector embeddings for semantic search.
Enables few-shot learning by retrieving similar past queries.
"""

import uuid
import json
import sqlite3
from datetime import datetime
from typing import Optional, List, Dict, Any
from dataclasses import dataclass
from pathlib import Path

# Try to import ChromaDB
try:
    import chromadb
    from chromadb.config import Settings
    CHROMADB_AVAILABLE = True
except ImportError:
    CHROMADB_AVAILABLE = False
    chromadb = None


@dataclass
class LearningExample:
    """A learning example with query and SQL."""
    id: str
    question: str
    normalized_question: str
    sql: str
    intent: str
    tables_used: List[str]
    columns_used: List[str]
    complexity: str
    category: str
    source: str
    verified: bool
    created_by: str
    created_at: datetime
    usage_count: int
    success_count: int
    status: str


class ExampleStore:
    """
    Vector store for query-SQL learning examples.

    Uses ChromaDB for semantic similarity search and SQLite for metadata.
    """

    def __init__(
        self,
        db_path: str = "data/learning.db",
        chroma_path: str = "knowledge/chroma",
        collection_name: str = "query_examples"
    ):
        """
        Initialize Example Store.

        Args:
            db_path: Path to SQLite database
            chroma_path: Path to ChromaDB storage
            collection_name: Name of ChromaDB collection
        """
        self.db_path = Path(db_path)
        self.chroma_path = Path(chroma_path)
        self.collection_name = collection_name

        # Initialize SQLite
        self._init_database()

        # Initialize ChromaDB if available
        self.chroma_client = None
        self.collection = None

        if CHROMADB_AVAILABLE:
            try:
                self.chroma_path.mkdir(parents=True, exist_ok=True)
                self.chroma_client = chromadb.PersistentClient(
                    path=str(self.chroma_path),
                    settings=Settings(anonymized_telemetry=False)
                )
                self.collection = self.chroma_client.get_or_create_collection(
                    name=collection_name,
                    metadata={"description": "Query examples for few-shot learning"}
                )
            except Exception as e:
                print(f"Warning: ChromaDB initialization failed: {e}")
                self.chroma_client = None
                self.collection = None

    def _init_database(self):
        """Initialize SQLite tables."""
        self.db_path.parent.mkdir(parents=True, exist_ok=True)

        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS learning_examples (
                    id TEXT PRIMARY KEY,
                    question TEXT NOT NULL,
                    normalized_question TEXT,
                    sql TEXT NOT NULL,
                    intent TEXT,
                    tables_used TEXT,
                    columns_used TEXT,
                    complexity TEXT,
                    category TEXT,
                    source TEXT NOT NULL,
                    verified INTEGER DEFAULT 0,
                    created_by TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    usage_count INTEGER DEFAULT 0,
                    success_count INTEGER DEFAULT 0,
                    last_used_at TIMESTAMP,
                    status TEXT DEFAULT 'active'
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_examples_status
                ON learning_examples(status)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_examples_category
                ON learning_examples(category)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_examples_verified
                ON learning_examples(verified)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_examples_normalized
                ON learning_examples(normalized_question)
            """)

    def add_example(
        self,
        question: str,
        sql: str,
        intent: str = "DATA",
        tables_used: Optional[List[str]] = None,
        columns_used: Optional[List[str]] = None,
        complexity: str = "MODERATE",
        category: str = "general",
        source: str = "manual",
        verified: bool = False,
        created_by: str = "system"
    ) -> str:
        """
        Add a new learning example.

        Args:
            question: Natural language question
            sql: SQL query that answers the question
            intent: Query intent (DATA, DOCUMENT, HYBRID)
            tables_used: List of tables in the query
            columns_used: List of columns in the query
            complexity: Query complexity level
            category: Category (patient_count, demographics, etc.)
            source: Source (manual, feedback, training)
            verified: Whether example is verified
            created_by: User who created the example

        Returns:
            Example ID
        """
        example_id = str(uuid.uuid4())
        normalized = self._normalize_question(question)

        # Store in SQLite
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO learning_examples
                (id, question, normalized_question, sql, intent,
                 tables_used, columns_used, complexity, category,
                 source, verified, created_by)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                example_id,
                question,
                normalized,
                sql,
                intent,
                json.dumps(tables_used or []),
                json.dumps(columns_used or []),
                complexity,
                category,
                source,
                1 if verified else 0,
                created_by
            ))

        # Store embedding in ChromaDB if available
        if self.collection is not None:
            try:
                self.collection.add(
                    documents=[normalized],
                    ids=[example_id],
                    metadatas=[{
                        "question": question,
                        "sql": sql,
                        "intent": intent,
                        "category": category,
                        "complexity": complexity,
                        "verified": str(verified)
                    }]
                )
            except Exception as e:
                print(f"Warning: ChromaDB add failed: {e}")

        return example_id

    def find_similar(
        self,
        question: str,
        n_results: int = 5,
        min_similarity: float = 0.7,
        verified_only: bool = True
    ) -> List[Dict[str, Any]]:
        """
        Find semantically similar examples.

        Args:
            question: Question to find similar examples for
            n_results: Maximum number of results
            min_similarity: Minimum similarity threshold (0-1)
            verified_only: Only return verified examples

        Returns:
            List of similar examples with similarity scores
        """
        if self.collection is None:
            # Fallback to exact match if ChromaDB not available
            exact = self.get_exact_match(question)
            if exact:
                return [{**exact, "similarity": 1.0}]
            return []

        normalized = self._normalize_question(question)

        try:
            # Query ChromaDB
            where_filter = {"verified": "True"} if verified_only else None

            results = self.collection.query(
                query_texts=[normalized],
                n_results=n_results,
                where=where_filter,
                include=["documents", "metadatas", "distances"]
            )

            examples = []
            if results["ids"] and results["ids"][0]:
                for i, example_id in enumerate(results["ids"][0]):
                    # Convert distance to similarity
                    # ChromaDB uses L2 distance by default
                    distance = results["distances"][0][i] if results["distances"] else 0
                    # Convert L2 distance to similarity score
                    similarity = 1 / (1 + distance)

                    if similarity >= min_similarity:
                        metadata = results["metadatas"][0][i] if results["metadatas"] else {}
                        examples.append({
                            "id": example_id,
                            "question": metadata.get("question", ""),
                            "sql": metadata.get("sql", ""),
                            "intent": metadata.get("intent", ""),
                            "category": metadata.get("category", ""),
                            "complexity": metadata.get("complexity", ""),
                            "similarity": similarity
                        })

            return sorted(examples, key=lambda x: x["similarity"], reverse=True)

        except Exception as e:
            print(f"Warning: ChromaDB query failed: {e}")
            return []

    def get_exact_match(self, question: str) -> Optional[Dict[str, Any]]:
        """
        Check for exact or near-exact match.

        Args:
            question: Question to match

        Returns:
            Matching example or None
        """
        normalized = self._normalize_question(question)

        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM learning_examples
                WHERE normalized_question = ? AND status = 'active'
                LIMIT 1
            """, (normalized,))
            row = cursor.fetchone()

            if row:
                return dict(row)

        return None

    def update_usage_stats(
        self,
        example_id: str,
        success: bool = True
    ):
        """
        Update usage statistics for an example.

        Args:
            example_id: Example ID
            success: Whether the usage was successful
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE learning_examples
                SET usage_count = usage_count + 1,
                    success_count = success_count + ?,
                    last_used_at = CURRENT_TIMESTAMP
                WHERE id = ?
            """, (1 if success else 0, example_id))

    def verify_example(self, example_id: str, verified_by: str = "system"):
        """
        Mark an example as verified.

        Args:
            example_id: Example ID
            verified_by: User who verified
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE learning_examples
                SET verified = 1
                WHERE id = ?
            """, (example_id,))

        # Update ChromaDB metadata
        if self.collection is not None:
            try:
                self.collection.update(
                    ids=[example_id],
                    metadatas=[{"verified": "True"}]
                )
            except Exception as e:
                print(f"Warning: ChromaDB update failed: {e}")

    def deactivate_example(self, example_id: str):
        """
        Deactivate an example (soft delete).

        Args:
            example_id: Example ID
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                UPDATE learning_examples
                SET status = 'inactive'
                WHERE id = ?
            """, (example_id,))

        # Remove from ChromaDB
        if self.collection is not None:
            try:
                self.collection.delete(ids=[example_id])
            except Exception as e:
                print(f"Warning: ChromaDB delete failed: {e}")

    def get_example(self, example_id: str) -> Optional[LearningExample]:
        """
        Get a specific example by ID.

        Args:
            example_id: Example ID

        Returns:
            LearningExample or None
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.execute("""
                SELECT * FROM learning_examples WHERE id = ?
            """, (example_id,))
            row = cursor.fetchone()

            if row:
                return LearningExample(
                    id=row['id'],
                    question=row['question'],
                    normalized_question=row['normalized_question'],
                    sql=row['sql'],
                    intent=row['intent'] or 'DATA',
                    tables_used=json.loads(row['tables_used'] or '[]'),
                    columns_used=json.loads(row['columns_used'] or '[]'),
                    complexity=row['complexity'] or 'MODERATE',
                    category=row['category'] or 'general',
                    source=row['source'],
                    verified=bool(row['verified']),
                    created_by=row['created_by'] or 'system',
                    created_at=row['created_at'],
                    usage_count=row['usage_count'] or 0,
                    success_count=row['success_count'] or 0,
                    status=row['status'] or 'active'
                )

        return None

    def get_examples_by_category(
        self,
        category: str,
        verified_only: bool = True,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get examples by category.

        Args:
            category: Category to filter by
            verified_only: Only return verified examples
            limit: Maximum number of results

        Returns:
            List of examples
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            if verified_only:
                cursor = conn.execute("""
                    SELECT * FROM learning_examples
                    WHERE category = ? AND verified = 1 AND status = 'active'
                    ORDER BY usage_count DESC
                    LIMIT ?
                """, (category, limit))
            else:
                cursor = conn.execute("""
                    SELECT * FROM learning_examples
                    WHERE category = ? AND status = 'active'
                    ORDER BY usage_count DESC
                    LIMIT ?
                """, (category, limit))

            return [dict(row) for row in cursor.fetchall()]

    def get_statistics(self) -> Dict[str, Any]:
        """
        Get learning store statistics.

        Returns:
            Statistics dictionary
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            total = conn.execute(
                "SELECT COUNT(*) as count FROM learning_examples WHERE status = 'active'"
            ).fetchone()["count"]

            verified = conn.execute(
                "SELECT COUNT(*) as count FROM learning_examples WHERE verified = 1 AND status = 'active'"
            ).fetchone()["count"]

            by_category = conn.execute("""
                SELECT category, COUNT(*) as count
                FROM learning_examples
                WHERE status = 'active'
                GROUP BY category
            """).fetchall()

            by_source = conn.execute("""
                SELECT source, COUNT(*) as count
                FROM learning_examples
                WHERE status = 'active'
                GROUP BY source
            """).fetchall()

            by_complexity = conn.execute("""
                SELECT complexity, COUNT(*) as count
                FROM learning_examples
                WHERE status = 'active'
                GROUP BY complexity
            """).fetchall()

            return {
                "total_examples": total,
                "verified_examples": verified,
                "unverified_examples": total - verified,
                "by_category": {row["category"]: row["count"] for row in by_category},
                "by_source": {row["source"]: row["count"] for row in by_source},
                "by_complexity": {row["complexity"]: row["count"] for row in by_complexity},
                "chromadb_available": self.collection is not None
            }

    def _normalize_question(self, question: str) -> str:
        """
        Normalize question for comparison.

        Args:
            question: Question to normalize

        Returns:
            Normalized question
        """
        # Lowercase, strip, remove extra spaces
        normalized = question.lower().strip()
        normalized = ' '.join(normalized.split())
        # Remove trailing punctuation
        normalized = normalized.rstrip('?!.')
        return normalized

    def clear_all(self):
        """Clear all examples (for testing)."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("DELETE FROM learning_examples")

        if self.collection is not None:
            try:
                # Get all IDs and delete
                all_items = self.collection.get()
                if all_items["ids"]:
                    self.collection.delete(ids=all_items["ids"])
            except Exception as e:
                print(f"Warning: ChromaDB clear failed: {e}")
