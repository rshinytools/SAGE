"""
Confidence Manager - Calculate final confidence and determine response action.

Aggregates all confidence signals to determine how to respond to user.
"""

from dataclasses import dataclass
from typing import Dict, List, Optional
from enum import Enum


class ResponseAction(Enum):
    """Response actions based on confidence level."""
    RETURN_NORMAL = "RETURN_NORMAL"              # 90%+ confidence
    RETURN_WITH_WARNING = "RETURN_WITH_WARNING"  # 75-89%
    RETURN_WITH_VERIFICATION = "RETURN_WITH_VERIFICATION"  # 60-74%
    ASK_CLARIFICATION = "ASK_CLARIFICATION"      # 40-59%
    REFUSE = "REFUSE"                            # <40%


@dataclass
class ConfidenceResult:
    """Final confidence calculation result."""
    score: float  # 0-100
    action: ResponseAction
    components: Dict[str, float]
    warnings: List[str]
    explanation: str


class ConfidenceManager:
    """Calculate final confidence from all signals."""

    # Component weights (total = 1.0)
    DEFAULT_WEIGHTS = {
        "example_similarity": 0.20,
        "dictionary_match": 0.15,
        "metadata_coverage": 0.15,
        "semantic_alignment": 0.15,
        "complexity_match": 0.10,
        "execution_success": 0.10,
        "result_validation": 0.10,
        "result_sanity": 0.05
    }

    # Action thresholds (as percentages)
    THRESHOLDS = {
        ResponseAction.RETURN_NORMAL: 90,
        ResponseAction.RETURN_WITH_WARNING: 75,
        ResponseAction.RETURN_WITH_VERIFICATION: 60,
        ResponseAction.ASK_CLARIFICATION: 40,
        ResponseAction.REFUSE: 0
    }

    def __init__(self, weights: Optional[Dict[str, float]] = None):
        """
        Initialize Confidence Manager.

        Args:
            weights: Optional custom weights for components
        """
        self.weights = weights or self.DEFAULT_WEIGHTS

    def calculate(
        self,
        example_similarity: float = 0.0,
        dictionary_match: float = 0.0,
        metadata_coverage: float = 0.0,
        semantic_alignment: float = 0.0,
        complexity_match: float = 1.0,
        execution_success: float = 0.0,
        result_validation: float = 1.0,
        result_sanity: float = 1.0,
        complexity_adjustment: float = 0.0,
        result_adjustment: float = 0.0
    ) -> ConfidenceResult:
        """
        Calculate final confidence score.

        Args:
            example_similarity: Similarity to verified examples (0-1)
            dictionary_match: Dictionary/fuzzy match quality (0-1)
            metadata_coverage: Metadata coverage for variables (0-1)
            semantic_alignment: SQL matches intent (0-1)
            complexity_match: Appropriate for complexity (0-1)
            execution_success: Query executed successfully (0-1)
            result_validation: Result passed validation (0-1)
            result_sanity: Result sanity checks passed (0-1)
            complexity_adjustment: Adjustment for complexity (-0.15 to 0)
            result_adjustment: Adjustment from result validation (-0.2 to 0.1)

        Returns:
            ConfidenceResult with score, action, and details
        """
        # Build components dict
        components = {
            "example_similarity": example_similarity,
            "dictionary_match": dictionary_match,
            "metadata_coverage": metadata_coverage,
            "semantic_alignment": semantic_alignment,
            "complexity_match": complexity_match,
            "execution_success": execution_success,
            "result_validation": result_validation,
            "result_sanity": result_sanity
        }

        # Calculate weighted score
        raw_score = sum(
            components[k] * self.weights.get(k, 0)
            for k in components
        )

        # Apply adjustments
        adjusted_score = raw_score + complexity_adjustment + result_adjustment

        # Clamp to 0-1
        final_score = max(0.0, min(1.0, adjusted_score))

        # Convert to percentage
        score_percent = final_score * 100

        # Determine action
        action = self._determine_action(score_percent)

        # Generate warnings
        warnings = self._generate_warnings(components, score_percent)

        # Generate explanation
        explanation = self._generate_explanation(components, action, score_percent)

        return ConfidenceResult(
            score=score_percent,
            action=action,
            components={k: v * 100 for k, v in components.items()},
            warnings=warnings,
            explanation=explanation
        )

    def _determine_action(self, score: float) -> ResponseAction:
        """Determine response action based on score."""
        if score >= self.THRESHOLDS[ResponseAction.RETURN_NORMAL]:
            return ResponseAction.RETURN_NORMAL
        elif score >= self.THRESHOLDS[ResponseAction.RETURN_WITH_WARNING]:
            return ResponseAction.RETURN_WITH_WARNING
        elif score >= self.THRESHOLDS[ResponseAction.RETURN_WITH_VERIFICATION]:
            return ResponseAction.RETURN_WITH_VERIFICATION
        elif score >= self.THRESHOLDS[ResponseAction.ASK_CLARIFICATION]:
            return ResponseAction.ASK_CLARIFICATION
        else:
            return ResponseAction.REFUSE

    def _generate_warnings(
        self,
        components: Dict[str, float],
        score: float
    ) -> List[str]:
        """Generate warnings based on component scores."""
        warnings = []

        if components["example_similarity"] < 0.5:
            warnings.append("No similar examples found in training data")

        if components["semantic_alignment"] < 0.7:
            warnings.append("SQL may not fully match query intent")

        if components["result_validation"] < 0.8:
            warnings.append("Result differs from historical patterns")

        if components["metadata_coverage"] < 0.5:
            warnings.append("Limited metadata available for variables")

        if components["dictionary_match"] < 0.5:
            warnings.append("Some terms may not have been recognized correctly")

        if score < 60:
            warnings.append("Low confidence - please verify results carefully")

        return warnings

    def _generate_explanation(
        self,
        components: Dict[str, float],
        action: ResponseAction,
        score: float
    ) -> str:
        """Generate explanation for confidence level."""
        if action == ResponseAction.RETURN_NORMAL:
            return f"High confidence ({score:.0f}%) answer based on similar verified examples."

        elif action == ResponseAction.RETURN_WITH_WARNING:
            weak = [k for k, v in components.items() if v < 0.7]
            if weak:
                weak_names = [k.replace("_", " ") for k in weak[:2]]
                return f"Moderate confidence ({score:.0f}%). Verify: {', '.join(weak_names)}"
            return f"Moderate confidence ({score:.0f}%). Please verify assumptions."

        elif action == ResponseAction.RETURN_WITH_VERIFICATION:
            return f"Lower confidence ({score:.0f}%). Result provided but requires verification."

        elif action == ResponseAction.ASK_CLARIFICATION:
            return f"Insufficient confidence ({score:.0f}%). Please clarify your question."

        else:  # REFUSE
            return f"Cannot provide reliable answer (confidence: {score:.0f}%)."

    def get_action_description(self, action: ResponseAction) -> str:
        """Get human-readable description of action."""
        descriptions = {
            ResponseAction.RETURN_NORMAL: "Return answer normally",
            ResponseAction.RETURN_WITH_WARNING: "Return answer with verification note",
            ResponseAction.RETURN_WITH_VERIFICATION: "Return answer with detailed verification",
            ResponseAction.ASK_CLARIFICATION: "Ask user for clarification before answering",
            ResponseAction.REFUSE: "Decline to answer due to low confidence"
        }
        return descriptions.get(action, "Unknown action")

    def set_thresholds(self, thresholds: Dict[ResponseAction, float]):
        """Update action thresholds."""
        self.THRESHOLDS.update(thresholds)

    def get_component_breakdown(self, result: ConfidenceResult) -> str:
        """Get formatted component breakdown."""
        lines = ["Confidence Breakdown:"]
        for component, value in result.components.items():
            weight = self.weights.get(component, 0) * 100
            contribution = value * weight / 100
            lines.append(f"  {component.replace('_', ' ')}: {value:.0f}% (weight: {weight:.0f}%, contribution: {contribution:.1f}%)")
        lines.append(f"  Total Score: {result.score:.0f}%")
        return "\n".join(lines)
