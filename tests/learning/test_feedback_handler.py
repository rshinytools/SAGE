"""Tests for Feedback Handler."""

import pytest
import tempfile
import shutil
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.learning.feedback_handler import (
    FeedbackHandler,
    FeedbackType,
    FeedbackResult
)


class TestFeedbackHandler:
    """Test suite for FeedbackHandler."""

    @pytest.fixture
    def temp_dir(self):
        """Create temporary directory for tests."""
        temp = tempfile.mkdtemp()
        yield temp
        shutil.rmtree(temp, ignore_errors=True)

    @pytest.fixture
    def handler(self, temp_dir):
        """Create FeedbackHandler with temp path."""
        db_path = Path(temp_dir) / "learning.db"
        return FeedbackHandler(db_path=str(db_path))

    def test_submit_confirm_feedback(self, handler):
        """Test submitting confirmation feedback."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.CONFIRM,
            original_sql="SELECT COUNT(*) FROM adsl",
            user_id="test_user"
        )

        assert result.processed
        assert result.learning_updated
        assert "verification" in result.action_taken

    def test_submit_correction_with_sql(self, handler):
        """Test submitting correction with SQL."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many male patients?",
            feedback_type=FeedbackType.CORRECT,
            original_sql="SELECT COUNT(*) FROM adsl",
            corrected_sql="SELECT COUNT(*) FROM adsl WHERE SEX = 'M'"
        )

        assert result.processed
        assert result.learning_updated
        assert "correction_stored" in result.action_taken

    def test_submit_correction_without_sql(self, handler):
        """Test submitting correction without SQL."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.CORRECT,
            user_input="The result should be filtered by treatment"
        )

        assert result.processed
        assert "incomplete" in result.action_taken

    def test_submit_rejection(self, handler):
        """Test submitting rejection."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.REJECT
        )

        assert result.processed
        assert "rejection" in result.action_taken

    def test_submit_clarification(self, handler):
        """Test submitting clarification."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many high patients?",
            feedback_type=FeedbackType.CLARIFY,
            user_input="High means blood pressure > 140"
        )

        assert result.processed
        assert "clarification" in result.action_taken

    def test_submit_high_rating(self, handler):
        """Test submitting high rating."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.RATE,
            rating=5
        )

        assert result.processed
        assert "high_rating" in result.action_taken
        assert "5/5" in result.message

    def test_submit_low_rating(self, handler):
        """Test submitting low rating."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.RATE,
            rating=1
        )

        assert result.processed
        assert "low_rating" in result.action_taken
        assert "1/5" in result.message

    def test_submit_medium_rating(self, handler):
        """Test submitting medium rating."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="How many patients?",
            feedback_type=FeedbackType.RATE,
            rating=3
        )

        assert result.processed
        assert "rating_stored" in result.action_taken

    def test_get_feedback(self, handler):
        """Test retrieving feedback by ID."""
        result = handler.submit_feedback(
            query_id="query-123",
            question="Test question",
            feedback_type=FeedbackType.CONFIRM
        )

        feedback = handler.get_feedback(result.id)
        assert feedback is not None
        assert feedback["question"] == "Test question"
        assert feedback["feedback_type"] == "CONFIRM"

    def test_get_feedback_not_found(self, handler):
        """Test retrieving non-existent feedback."""
        feedback = handler.get_feedback("non-existent-id")
        assert feedback is None

    def test_get_feedback_for_query(self, handler):
        """Test retrieving all feedback for a query."""
        handler.submit_feedback(
            query_id="query-456",
            question="How many?",
            feedback_type=FeedbackType.CONFIRM
        )
        handler.submit_feedback(
            query_id="query-456",
            question="How many?",
            feedback_type=FeedbackType.RATE,
            rating=4
        )

        feedback_list = handler.get_feedback_for_query("query-456")
        assert len(feedback_list) == 2

    def test_get_pending_corrections(self, handler):
        """Test getting pending corrections."""
        handler.submit_feedback(
            query_id="q1",
            question="Question 1",
            feedback_type=FeedbackType.CORRECT,
            corrected_sql="SELECT 1"
        )
        handler.submit_feedback(
            query_id="q2",
            question="Question 2",
            feedback_type=FeedbackType.CONFIRM
        )
        handler.submit_feedback(
            query_id="q3",
            question="Question 3",
            feedback_type=FeedbackType.CORRECT,
            corrected_sql="SELECT 3"
        )

        corrections = handler.get_pending_corrections()
        assert len(corrections) == 2
        assert all(c["feedback_type"] == "CORRECT" for c in corrections)

    def test_get_low_rated_queries(self, handler):
        """Test getting low-rated queries."""
        handler.submit_feedback(
            query_id="q1",
            question="Bad query",
            feedback_type=FeedbackType.RATE,
            rating=1
        )
        handler.submit_feedback(
            query_id="q2",
            question="Good query",
            feedback_type=FeedbackType.RATE,
            rating=5
        )
        handler.submit_feedback(
            query_id="q3",
            question="Poor query",
            feedback_type=FeedbackType.RATE,
            rating=2
        )

        low_rated = handler.get_low_rated_queries(max_rating=2)
        assert len(low_rated) == 2
        assert all(r["rating"] <= 2 for r in low_rated)

    def test_get_confirmation_candidates(self, handler):
        """Test getting confirmation candidates."""
        # Add multiple confirmations for same query
        for _ in range(5):
            handler.submit_feedback(
                query_id="q1",
                question="Popular question",
                original_sql="SELECT COUNT(*) FROM adsl",
                feedback_type=FeedbackType.CONFIRM
            )

        # Add fewer confirmations for another
        for _ in range(2):
            handler.submit_feedback(
                query_id="q2",
                question="Less popular",
                original_sql="SELECT * FROM adae",
                feedback_type=FeedbackType.CONFIRM
            )

        candidates = handler.get_confirmation_candidates(min_confirmations=3)
        assert len(candidates) == 1
        assert candidates[0]["confirmation_count"] >= 3

    def test_get_statistics(self, handler):
        """Test getting feedback statistics."""
        handler.submit_feedback(
            query_id="q1",
            question="Q1",
            feedback_type=FeedbackType.CONFIRM
        )
        handler.submit_feedback(
            query_id="q2",
            question="Q2",
            feedback_type=FeedbackType.RATE,
            rating=4
        )
        handler.submit_feedback(
            query_id="q3",
            question="Q3",
            feedback_type=FeedbackType.RATE,
            rating=5
        )

        stats = handler.get_statistics()

        assert stats["total_feedback"] == 3
        assert stats["by_type"]["CONFIRM"] == 1
        assert stats["by_type"]["RATE"] == 2
        assert stats["average_rating"] == 4.5
        assert stats["processed_count"] == 3

    def test_export_for_training(self, handler):
        """Test exporting feedback for training."""
        handler.submit_feedback(
            query_id="q1",
            question="Question 1",
            original_sql="SELECT 1",
            feedback_type=FeedbackType.CONFIRM
        )
        handler.submit_feedback(
            query_id="q2",
            question="Question 2",
            original_sql="SELECT 2",
            corrected_sql="SELECT 2 + 1",
            feedback_type=FeedbackType.CORRECT
        )
        handler.submit_feedback(
            query_id="q3",
            question="Question 3",
            feedback_type=FeedbackType.REJECT
        )

        examples = handler.export_for_training()

        assert len(examples) == 2  # Only CONFIRM and CORRECT
        assert any(e["verified"] for e in examples)

    def test_export_with_rating_filter(self, handler):
        """Test export with rating filter."""
        handler.submit_feedback(
            query_id="q1",
            question="Q1",
            original_sql="SELECT 1",
            feedback_type=FeedbackType.RATE,
            rating=5
        )
        handler.submit_feedback(
            query_id="q2",
            question="Q2",
            original_sql="SELECT 2",
            feedback_type=FeedbackType.RATE,
            rating=2
        )

        examples = handler.export_for_training(
            feedback_types=[FeedbackType.RATE],
            min_rating=4
        )

        assert len(examples) == 1
        assert examples[0]["question"] == "Q1"

    def test_clear_all(self, handler):
        """Test clearing all feedback."""
        handler.submit_feedback(
            query_id="q1",
            question="Q1",
            feedback_type=FeedbackType.CONFIRM
        )
        handler.submit_feedback(
            query_id="q2",
            question="Q2",
            feedback_type=FeedbackType.REJECT
        )

        stats = handler.get_statistics()
        assert stats["total_feedback"] == 2

        handler.clear_all()

        stats = handler.get_statistics()
        assert stats["total_feedback"] == 0

    def test_result_serialization(self, handler):
        """Test that results are serialized properly."""
        result = handler.submit_feedback(
            query_id="q1",
            question="Q1",
            original_result=[{"a": 1}, {"a": 2}],
            feedback_type=FeedbackType.CONFIRM
        )

        feedback = handler.get_feedback(result.id)
        assert feedback["original_result"] is not None

    def test_dataframe_result_serialization(self, handler):
        """Test DataFrame result serialization."""
        class MockDataFrame:
            def to_dict(self, orient):
                return [{"col": "value"}]

        result = handler.submit_feedback(
            query_id="q1",
            question="Q1",
            original_result=MockDataFrame(),
            feedback_type=FeedbackType.CONFIRM
        )

        feedback = handler.get_feedback(result.id)
        assert feedback["original_result"] is not None
