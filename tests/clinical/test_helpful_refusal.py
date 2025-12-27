"""
Tests for Helpful Refusal System.

Tests generation of actionable refusal messages.
"""

import pytest
import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from core.engine.clinical.helpful_refusal import (
    HelpfulRefusalSystem,
    HelpfulRefusal,
    RefusalReason
)


class TestHelpfulRefusalSystem:
    """Test suite for HelpfulRefusalSystem."""

    @pytest.fixture
    def system(self):
        """Create a HelpfulRefusalSystem instance."""
        return HelpfulRefusalSystem()

    # ==========================================
    # Test Refusal Generation
    # ==========================================

    def test_generates_ambiguous_term_refusal(self, system):
        """Test generation of ambiguous term refusal."""
        refusal = system.generate_refusal(
            RefusalReason.AMBIGUOUS_TERM,
            context={
                "ambiguous_terms": ["high", "recent"],
                "clarifications_needed": ["Define 'high': What threshold?"]
            }
        )

        assert refusal.reason == RefusalReason.AMBIGUOUS_TERM
        assert "high" in refusal.message or "recent" in refusal.message
        assert len(refusal.clarifying_questions) > 0
        assert len(refusal.suggestions) > 0

    def test_generates_low_similarity_refusal(self, system):
        """Test generation of low similarity refusal."""
        refusal = system.generate_refusal(
            RefusalReason.LOW_SIMILARITY,
            context={
                "best_match": {"question": "How many patients?", "similarity": 0.5}
            }
        )

        assert refusal.reason == RefusalReason.LOW_SIMILARITY
        assert "confidence" in refusal.message.lower() or "training" in refusal.message.lower()
        assert len(refusal.suggestions) > 0

    def test_generates_schema_mismatch_refusal(self, system):
        """Test generation of schema mismatch refusal."""
        refusal = system.generate_refusal(
            RefusalReason.SCHEMA_MISMATCH,
            context={
                "schema_issues": []
            }
        )

        assert refusal.reason == RefusalReason.SCHEMA_MISMATCH
        assert "data" in refusal.message.lower() or "column" in refusal.message.lower()

    def test_generates_complex_query_refusal(self, system):
        """Test generation of complex query refusal."""
        refusal = system.generate_refusal(
            RefusalReason.COMPLEX_QUERY,
            context={
                "simpler_queries": ["How many subjects?", "Filter by treatment"]
            }
        )

        assert refusal.reason == RefusalReason.COMPLEX_QUERY
        assert "complex" in refusal.message.lower()
        assert refusal.can_partially_answer is True
        assert len(refusal.alternative_queries) > 0

    def test_generates_out_of_scope_refusal(self, system):
        """Test generation of out of scope refusal."""
        refusal = system.generate_refusal(
            RefusalReason.OUT_OF_SCOPE,
            context={}
        )

        assert refusal.reason == RefusalReason.OUT_OF_SCOPE
        assert "outside" in refusal.message.lower() or "capabilities" in refusal.message.lower()
        assert len(refusal.suggestions) > 0

    def test_generates_missing_data_refusal(self, system):
        """Test generation of missing data refusal."""
        refusal = system.generate_refusal(
            RefusalReason.MISSING_DATA,
            context={"missing_tables": ["CM", "VS"]}
        )

        assert refusal.reason == RefusalReason.MISSING_DATA
        assert "CM" in refusal.message or "VS" in refusal.message or "data" in refusal.message.lower()

    def test_generates_low_confidence_refusal(self, system):
        """Test generation of low confidence refusal."""
        refusal = system.generate_refusal(
            RefusalReason.CONFIDENCE_TOO_LOW,
            context={
                "confidence": 35.0,
                "warnings": ["No similar examples", "Complex query"]
            }
        )

        assert refusal.reason == RefusalReason.CONFIDENCE_TOO_LOW
        assert "35" in refusal.message or "confidence" in refusal.message.lower()
        assert refusal.can_partially_answer is True

    def test_generates_generic_refusal(self, system):
        """Test generation of generic refusal."""
        refusal = system.generate_refusal(
            RefusalReason.VALIDATION_FAILED,
            context={}
        )

        assert refusal.reason == RefusalReason.VALIDATION_FAILED
        assert len(refusal.message) > 0
        assert len(refusal.suggestions) > 0

    # ==========================================
    # Test Message Formatting
    # ==========================================

    def test_format_refusal_message(self, system):
        """Test formatting refusal as message."""
        refusal = HelpfulRefusal(
            reason=RefusalReason.AMBIGUOUS_TERM,
            message="I need clarification on 'high'",
            clarifying_questions=["Define 'high'"],
            suggestions=["Use exact thresholds"],
            alternative_queries=["Show patients with BP > 140"],
            can_partially_answer=False,
            partial_answer=None
        )

        message = system.format_refusal_message(refusal)

        assert "I need clarification" in message
        assert "Define 'high'" in message
        assert "exact thresholds" in message
        assert "BP > 140" in message

    def test_format_includes_partial_answer(self, system):
        """Test that partial answer is included when available."""
        refusal = HelpfulRefusal(
            reason=RefusalReason.COMPLEX_QUERY,
            message="Query is too complex",
            clarifying_questions=[],
            suggestions=[],
            alternative_queries=[],
            can_partially_answer=True,
            partial_answer="I can tell you there are 339 total subjects."
        )

        message = system.format_refusal_message(refusal)

        assert "339 total subjects" in message

    def test_format_handles_empty_lists(self, system):
        """Test formatting with empty lists."""
        refusal = HelpfulRefusal(
            reason=RefusalReason.OUT_OF_SCOPE,
            message="Cannot answer this",
            clarifying_questions=[],
            suggestions=[],
            alternative_queries=[],
            can_partially_answer=False,
            partial_answer=None
        )

        message = system.format_refusal_message(refusal)

        # Should still produce a valid message
        assert "Cannot answer this" in message

    # ==========================================
    # Test Refusal Structure
    # ==========================================

    def test_refusal_has_all_fields(self, system):
        """Test that refusals have all required fields."""
        for reason in RefusalReason:
            refusal = system.generate_refusal(reason, context={})

            assert refusal.reason == reason
            assert isinstance(refusal.message, str)
            assert isinstance(refusal.clarifying_questions, list)
            assert isinstance(refusal.suggestions, list)
            assert isinstance(refusal.alternative_queries, list)
            assert isinstance(refusal.can_partially_answer, bool)

    def test_suggestions_are_actionable(self, system):
        """Test that suggestions provide actionable guidance."""
        refusal = system.generate_refusal(
            RefusalReason.LOW_SIMILARITY,
            context={}
        )

        # Suggestions should be actual guidance, not empty
        for suggestion in refusal.suggestions:
            assert len(suggestion) > 10  # Reasonable length for guidance


class TestRefusalReason:
    """Test RefusalReason enum."""

    def test_all_reasons_defined(self):
        """Test that all expected reasons are defined."""
        expected_reasons = [
            "AMBIGUOUS_TERM",
            "LOW_SIMILARITY",
            "SCHEMA_MISMATCH",
            "COMPLEX_QUERY",
            "OUT_OF_SCOPE",
            "MISSING_DATA",
            "VALIDATION_FAILED",
            "CONFIDENCE_TOO_LOW"
        ]

        actual_reasons = [r.value for r in RefusalReason]

        for expected in expected_reasons:
            assert expected in actual_reasons


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
