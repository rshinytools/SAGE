"""Tests for Example Store."""

import pytest
import tempfile
import os
import shutil
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.learning.example_store import ExampleStore, LearningExample


class TestExampleStore:
    """Test suite for ExampleStore."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp, ignore_errors=True)

    @pytest.fixture
    def store(self, temp_dir):
        """Create ExampleStore with temp paths."""
        db_path = os.path.join(temp_dir, "learning.db")
        chroma_path = os.path.join(temp_dir, "chroma")
        return ExampleStore(
            db_path=db_path,
            chroma_path=chroma_path,
            collection_name="test_examples"
        )

    def test_init_creates_database(self, store, temp_dir):
        """Test that initialization creates the database."""
        db_path = Path(temp_dir) / "learning.db"
        assert db_path.exists()

    def test_add_example(self, store):
        """Test adding an example."""
        example_id = store.add_example(
            question="How many patients are in the study?",
            sql="SELECT COUNT(*) FROM adsl",
            intent="DATA",
            category="patient_count",
            source="test"
        )
        assert example_id is not None
        assert len(example_id) == 36  # UUID format

    def test_get_example(self, store):
        """Test retrieving an example by ID."""
        example_id = store.add_example(
            question="How many adverse events?",
            sql="SELECT COUNT(*) FROM adae",
            category="ae_count"
        )

        example = store.get_example(example_id)
        assert example is not None
        assert example.question == "How many adverse events?"
        assert example.sql == "SELECT COUNT(*) FROM adae"
        assert example.category == "ae_count"

    def test_get_example_not_found(self, store):
        """Test retrieving non-existent example."""
        example = store.get_example("non-existent-id")
        assert example is None

    def test_exact_match(self, store):
        """Test exact question matching."""
        store.add_example(
            question="Count of males",
            sql="SELECT COUNT(*) FROM adsl WHERE SEX = 'M'",
            verified=True
        )

        match = store.get_exact_match("Count of males")
        assert match is not None
        assert match["sql"] == "SELECT COUNT(*) FROM adsl WHERE SEX = 'M'"

    def test_exact_match_case_insensitive(self, store):
        """Test exact match is case insensitive."""
        store.add_example(
            question="Count of Females",
            sql="SELECT COUNT(*) FROM adsl WHERE SEX = 'F'"
        )

        match = store.get_exact_match("count of females")
        assert match is not None

    def test_exact_match_no_match(self, store):
        """Test exact match returns None when no match."""
        match = store.get_exact_match("some random question")
        assert match is None

    def test_update_usage_stats(self, store):
        """Test updating usage statistics."""
        example_id = store.add_example(
            question="Test question",
            sql="SELECT 1"
        )

        store.update_usage_stats(example_id, success=True)
        store.update_usage_stats(example_id, success=True)
        store.update_usage_stats(example_id, success=False)

        example = store.get_example(example_id)
        assert example.usage_count == 3
        assert example.success_count == 2

    def test_verify_example(self, store):
        """Test verifying an example."""
        example_id = store.add_example(
            question="Test question",
            sql="SELECT 1",
            verified=False
        )

        example = store.get_example(example_id)
        assert not example.verified

        store.verify_example(example_id)

        example = store.get_example(example_id)
        assert example.verified

    def test_deactivate_example(self, store):
        """Test deactivating an example."""
        example_id = store.add_example(
            question="Test question",
            sql="SELECT 1"
        )

        store.deactivate_example(example_id)

        example = store.get_example(example_id)
        assert example.status == "inactive"

    def test_get_examples_by_category(self, store):
        """Test getting examples by category."""
        store.add_example(
            question="Count patients",
            sql="SELECT COUNT(*) FROM adsl",
            category="demographics",
            verified=True
        )
        store.add_example(
            question="Count AEs",
            sql="SELECT COUNT(*) FROM adae",
            category="adverse_events",
            verified=True
        )
        store.add_example(
            question="Average age",
            sql="SELECT AVG(AGE) FROM adsl",
            category="demographics",
            verified=True
        )

        demographics = store.get_examples_by_category("demographics")
        assert len(demographics) == 2

        ae = store.get_examples_by_category("adverse_events")
        assert len(ae) == 1

    def test_get_statistics(self, store):
        """Test getting store statistics."""
        store.add_example(
            question="Test 1",
            sql="SELECT 1",
            category="cat1",
            source="manual",
            complexity="SIMPLE",
            verified=True
        )
        store.add_example(
            question="Test 2",
            sql="SELECT 2",
            category="cat1",
            source="feedback",
            complexity="MODERATE",
            verified=False
        )
        store.add_example(
            question="Test 3",
            sql="SELECT 3",
            category="cat2",
            source="manual",
            complexity="SIMPLE"
        )

        stats = store.get_statistics()

        assert stats["total_examples"] == 3
        assert stats["verified_examples"] == 1
        assert stats["unverified_examples"] == 2
        assert stats["by_category"]["cat1"] == 2
        assert stats["by_category"]["cat2"] == 1
        assert stats["by_source"]["manual"] == 2
        assert stats["by_source"]["feedback"] == 1

    def test_normalize_question(self, store):
        """Test question normalization."""
        normalized = store._normalize_question("  How Many Patients?  ")
        assert normalized == "how many patients"

        normalized = store._normalize_question("What is the count!!")
        assert normalized == "what is the count"

    def test_clear_all(self, store):
        """Test clearing all examples."""
        store.add_example(question="Test 1", sql="SELECT 1")
        store.add_example(question="Test 2", sql="SELECT 2")

        stats = store.get_statistics()
        assert stats["total_examples"] == 2

        store.clear_all()

        stats = store.get_statistics()
        assert stats["total_examples"] == 0

    def test_add_example_with_tables_and_columns(self, store):
        """Test adding example with table and column metadata."""
        example_id = store.add_example(
            question="Count patients by treatment",
            sql="SELECT TRT01P, COUNT(*) FROM adsl GROUP BY TRT01P",
            tables_used=["adsl"],
            columns_used=["TRT01P"],
            complexity="MODERATE"
        )

        example = store.get_example(example_id)
        assert example.tables_used == ["adsl"]
        assert example.columns_used == ["TRT01P"]
        assert example.complexity == "MODERATE"

    def test_find_similar_fallback_without_chromadb(self, store):
        """Test find_similar fallback when ChromaDB not available."""
        store.add_example(
            question="How many patients",
            sql="SELECT COUNT(*) FROM adsl",
            verified=True
        )

        # If ChromaDB not available, find_similar should return exact match
        # or empty list
        results = store.find_similar("How many patients", verified_only=False)
        # Results depend on ChromaDB availability
        assert isinstance(results, list)
