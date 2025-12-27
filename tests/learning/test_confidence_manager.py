"""Tests for Confidence Manager."""

import pytest
from pathlib import Path

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from core.engine.learning.confidence_manager import (
    ConfidenceManager,
    ConfidenceResult,
    ResponseAction
)


class TestConfidenceManager:
    """Test suite for ConfidenceManager."""

    @pytest.fixture
    def manager(self):
        """Create ConfidenceManager instance."""
        return ConfidenceManager()

    def test_return_normal_high_confidence(self, manager):
        """Test RETURN_NORMAL for high confidence."""
        result = manager.calculate(
            example_similarity=1.0,
            dictionary_match=1.0,
            metadata_coverage=1.0,
            semantic_alignment=1.0,
            complexity_match=1.0,
            execution_success=1.0,
            result_validation=1.0,
            result_sanity=1.0
        )
        assert result.action == ResponseAction.RETURN_NORMAL
        assert result.score >= 90

    def test_return_with_warning_moderate_confidence(self, manager):
        """Test RETURN_WITH_WARNING for moderate confidence."""
        result = manager.calculate(
            example_similarity=0.8,
            dictionary_match=0.8,
            metadata_coverage=0.8,
            semantic_alignment=0.8,
            complexity_match=0.8,
            execution_success=0.8,
            result_validation=0.8,
            result_sanity=0.8
        )
        assert result.action == ResponseAction.RETURN_WITH_WARNING
        assert 75 <= result.score < 90

    def test_return_with_verification_lower_confidence(self, manager):
        """Test RETURN_WITH_VERIFICATION for lower confidence."""
        result = manager.calculate(
            example_similarity=0.6,
            dictionary_match=0.7,
            metadata_coverage=0.6,
            semantic_alignment=0.7,
            complexity_match=0.6,
            execution_success=0.7,
            result_validation=0.6,
            result_sanity=0.7
        )
        assert result.action == ResponseAction.RETURN_WITH_VERIFICATION
        assert 60 <= result.score < 75

    def test_ask_clarification_low_confidence(self, manager):
        """Test ASK_CLARIFICATION for low confidence."""
        result = manager.calculate(
            example_similarity=0.4,
            dictionary_match=0.5,
            metadata_coverage=0.4,
            semantic_alignment=0.5,
            complexity_match=0.5,
            execution_success=0.5,
            result_validation=0.5,
            result_sanity=0.5
        )
        assert result.action == ResponseAction.ASK_CLARIFICATION
        assert 40 <= result.score < 60

    def test_refuse_very_low_confidence(self, manager):
        """Test REFUSE for very low confidence."""
        result = manager.calculate(
            example_similarity=0.1,
            dictionary_match=0.2,
            metadata_coverage=0.1,
            semantic_alignment=0.2,
            complexity_match=0.3,
            execution_success=0.2,
            result_validation=0.2,
            result_sanity=0.3
        )
        assert result.action == ResponseAction.REFUSE
        assert result.score < 40

    def test_weighted_calculation(self, manager):
        """Test that weights are applied correctly."""
        # High example_similarity (weight 0.20) should boost score
        high_example = manager.calculate(
            example_similarity=1.0,
            dictionary_match=0.5,
            metadata_coverage=0.5,
            semantic_alignment=0.5,
            complexity_match=0.5,
            execution_success=0.5,
            result_validation=0.5,
            result_sanity=0.5
        )

        low_example = manager.calculate(
            example_similarity=0.0,
            dictionary_match=0.5,
            metadata_coverage=0.5,
            semantic_alignment=0.5,
            complexity_match=0.5,
            execution_success=0.5,
            result_validation=0.5,
            result_sanity=0.5
        )

        assert high_example.score > low_example.score

    def test_complexity_adjustment_negative(self, manager):
        """Test negative complexity adjustment."""
        base = manager.calculate(
            example_similarity=0.85,
            dictionary_match=0.85,
            metadata_coverage=0.85,
            semantic_alignment=0.85
        )

        with_adjustment = manager.calculate(
            example_similarity=0.85,
            dictionary_match=0.85,
            metadata_coverage=0.85,
            semantic_alignment=0.85,
            complexity_adjustment=-0.1
        )

        assert with_adjustment.score < base.score

    def test_result_adjustment_positive(self, manager):
        """Test positive result adjustment."""
        base = manager.calculate(
            example_similarity=0.85,
            dictionary_match=0.85,
            metadata_coverage=0.85,
            semantic_alignment=0.85
        )

        with_adjustment = manager.calculate(
            example_similarity=0.85,
            dictionary_match=0.85,
            metadata_coverage=0.85,
            semantic_alignment=0.85,
            result_adjustment=0.1
        )

        assert with_adjustment.score > base.score

    def test_score_clamped_to_100(self, manager):
        """Test score is clamped to 100."""
        result = manager.calculate(
            example_similarity=1.0,
            dictionary_match=1.0,
            metadata_coverage=1.0,
            semantic_alignment=1.0,
            complexity_match=1.0,
            execution_success=1.0,
            result_validation=1.0,
            result_sanity=1.0,
            result_adjustment=0.5  # Would push over 100%
        )
        assert result.score <= 100

    def test_score_clamped_to_0(self, manager):
        """Test score is clamped to 0."""
        result = manager.calculate(
            example_similarity=0.0,
            dictionary_match=0.0,
            metadata_coverage=0.0,
            semantic_alignment=0.0,
            complexity_match=0.0,
            execution_success=0.0,
            result_validation=0.0,
            result_sanity=0.0,
            complexity_adjustment=-0.5  # Would push below 0
        )
        assert result.score >= 0

    def test_components_in_result(self, manager):
        """Test components are included in result."""
        result = manager.calculate(
            example_similarity=0.8,
            dictionary_match=0.7,
            metadata_coverage=0.6,
            semantic_alignment=0.9
        )

        assert "example_similarity" in result.components
        assert "dictionary_match" in result.components
        assert result.components["example_similarity"] == 80  # Converted to %
        assert result.components["dictionary_match"] == 70

    def test_warnings_generated(self, manager):
        """Test warnings are generated for low components."""
        result = manager.calculate(
            example_similarity=0.3,  # Low - should warn
            dictionary_match=0.3,    # Low - should warn
            metadata_coverage=0.3,   # Low - should warn
            semantic_alignment=0.5
        )

        assert len(result.warnings) > 0
        assert any("similar" in w.lower() for w in result.warnings)

    def test_explanation_high_confidence(self, manager):
        """Test explanation for high confidence."""
        result = manager.calculate(
            example_similarity=1.0,
            dictionary_match=1.0,
            metadata_coverage=1.0,
            semantic_alignment=1.0,
            complexity_match=1.0,
            execution_success=1.0,
            result_validation=1.0,
            result_sanity=1.0
        )
        assert "high confidence" in result.explanation.lower()

    def test_explanation_low_confidence(self, manager):
        """Test explanation for low confidence."""
        result = manager.calculate(
            example_similarity=0.1,
            dictionary_match=0.1,
            metadata_coverage=0.1,
            semantic_alignment=0.1
        )
        assert "cannot" in result.explanation.lower() or "insufficient" in result.explanation.lower()

    def test_custom_weights(self):
        """Test custom weights."""
        custom_weights = {
            "example_similarity": 0.50,  # Much higher weight
            "dictionary_match": 0.10,
            "metadata_coverage": 0.10,
            "semantic_alignment": 0.10,
            "complexity_match": 0.05,
            "execution_success": 0.05,
            "result_validation": 0.05,
            "result_sanity": 0.05
        }
        manager = ConfidenceManager(weights=custom_weights)

        result = manager.calculate(
            example_similarity=1.0,
            dictionary_match=0.0,
            metadata_coverage=0.0,
            semantic_alignment=0.0
        )

        # With custom weights, high example_similarity dominates
        assert result.score >= 50

    def test_get_action_description(self, manager):
        """Test action descriptions."""
        assert "normally" in manager.get_action_description(ResponseAction.RETURN_NORMAL).lower()
        assert "clarification" in manager.get_action_description(ResponseAction.ASK_CLARIFICATION).lower()
        assert "decline" in manager.get_action_description(ResponseAction.REFUSE).lower()

    def test_set_thresholds(self, manager):
        """Test setting custom thresholds."""
        manager.set_thresholds({
            ResponseAction.RETURN_NORMAL: 95  # Higher threshold
        })

        # 92% would normally be RETURN_NORMAL, but now needs 95%
        result = manager.calculate(
            example_similarity=0.92,
            dictionary_match=0.92,
            metadata_coverage=0.92,
            semantic_alignment=0.92,
            complexity_match=0.92,
            execution_success=0.92,
            result_validation=0.92,
            result_sanity=0.92
        )

        assert result.action == ResponseAction.RETURN_WITH_WARNING

    def test_get_component_breakdown(self, manager):
        """Test component breakdown string."""
        result = manager.calculate(
            example_similarity=0.8,
            dictionary_match=0.7,
            metadata_coverage=0.6,
            semantic_alignment=0.9
        )

        breakdown = manager.get_component_breakdown(result)

        assert "Confidence Breakdown" in breakdown
        assert "example similarity" in breakdown
        assert "Total Score" in breakdown

    def test_default_values(self, manager):
        """Test default values for optional components."""
        result = manager.calculate(
            example_similarity=0.8,
            dictionary_match=0.8
            # Other components use defaults
        )

        # Should calculate without error
        assert isinstance(result.score, float)
        assert result.action is not None
