# SAGE - Confidence Scorer
# ========================
"""
Confidence Scorer
=================
Calculates confidence scores for query results.

4-Component Scoring:
- Dictionary Match (40%): How well entities resolved
- Metadata Coverage (30%): Schema and column verification
- Execution Success (20%): Query executed correctly
- Result Sanity (10%): Results make sense

This is STEP 8 of the 9-step pipeline.
"""

import logging
from typing import List, Dict, Optional, Any
from dataclasses import dataclass

from .models import (
    ConfidenceScore,
    ConfidenceLevel,
    EntityMatch,
    ValidationResult,
    ExecutionResult
)
from .table_resolver import TableResolution

logger = logging.getLogger(__name__)


@dataclass
class ScorerConfig:
    """Configuration for confidence scorer."""
    # Component weights (must sum to 100)
    weight_dictionary: float = 40.0
    weight_metadata: float = 30.0
    weight_execution: float = 20.0
    weight_sanity: float = 10.0

    # Thresholds
    high_confidence: float = 90.0
    medium_confidence: float = 70.0
    low_confidence: float = 50.0

    # Sanity check limits
    max_reasonable_count: int = 100000
    min_reasonable_count: int = 0


class ConfidenceScorer:
    """
    Calculates confidence scores for query results.

    Uses a 4-component scoring system:
    1. Dictionary Match (40%): Entity resolution quality
    2. Metadata Coverage (30%): Schema verification
    3. Execution Success (20%): Query execution
    4. Result Sanity (10%): Result validation

    Example:
        scorer = ConfidenceScorer()
        score = scorer.score(
            entities=entities,
            table_resolution=resolution,
            validation=validation,
            execution=execution
        )
        print(f"Score: {score.overall_score}% ({score.level})")
    """

    def __init__(self, config: Optional[ScorerConfig] = None):
        """
        Initialize confidence scorer.

        Args:
            config: Scorer configuration
        """
        self.config = config or ScorerConfig()

    def score(self,
              entities: List[EntityMatch],
              table_resolution: TableResolution,
              validation: ValidationResult,
              execution: ExecutionResult
             ) -> ConfidenceScore:
        """
        Calculate confidence score.

        Args:
            entities: Extracted and resolved entities
            table_resolution: Table and column resolution
            validation: SQL validation result
            execution: Query execution result

        Returns:
            ConfidenceScore with breakdown
        """
        # Calculate component scores
        dict_score = self._score_dictionary_match(entities)
        meta_score = self._score_metadata_coverage(table_resolution, validation)
        exec_score = self._score_execution(execution)
        sanity_score = self._score_sanity(execution)

        # Calculate weighted overall score
        overall = (
            dict_score * (self.config.weight_dictionary / 100) +
            meta_score * (self.config.weight_metadata / 100) +
            exec_score * (self.config.weight_execution / 100) +
            sanity_score * (self.config.weight_sanity / 100)
        )

        # Determine confidence level
        level = self._determine_level(overall)

        # Build component breakdown
        components = {
            'dictionary_match': {
                'score': dict_score,
                'weight': self.config.weight_dictionary,
                'weighted': dict_score * (self.config.weight_dictionary / 100)
            },
            'metadata_coverage': {
                'score': meta_score,
                'weight': self.config.weight_metadata,
                'weighted': meta_score * (self.config.weight_metadata / 100)
            },
            'execution_success': {
                'score': exec_score,
                'weight': self.config.weight_execution,
                'weighted': exec_score * (self.config.weight_execution / 100)
            },
            'result_sanity': {
                'score': sanity_score,
                'weight': self.config.weight_sanity,
                'weighted': sanity_score * (self.config.weight_sanity / 100)
            }
        }

        # Generate explanation
        explanation = self._generate_explanation(
            overall, level, components,
            entities, table_resolution, validation, execution
        )

        # IMPORTANT: Score must ALWAYS be an integer 0-100, never a float
        # This standardization prevents inconsistent output like "95%", "0.95", "5/5 stars"
        standardized_score = int(round(overall))

        return ConfidenceScore(
            overall_score=standardized_score,
            level=level,
            components=components,
            explanation=explanation
        )

    def _score_dictionary_match(self, entities: List[EntityMatch]) -> float:
        """
        Score dictionary match quality.

        Factors:
        - Entity confidence scores
        - Match type quality (exact > fuzzy > dictionary)
        - Number of entities resolved
        """
        if not entities:
            # No entities needed - full score
            return 100.0

        total_score = 0.0
        for entity in entities:
            # Base score from entity confidence
            entity_score = entity.confidence

            # Bonus for match type
            if entity.match_type == 'exact':
                entity_score = min(100, entity_score + 5)
            elif entity.match_type == 'meddra':
                entity_score = min(100, entity_score + 3)
            elif entity.match_type == 'fuzzy':
                entity_score = max(0, entity_score - 5)

            total_score += entity_score

        return total_score / len(entities)

    def _score_metadata_coverage(self,
                                  table_resolution: TableResolution,
                                  validation: ValidationResult
                                 ) -> float:
        """
        Score metadata coverage.

        Factors:
        - Table verified
        - Columns verified
        - No validation errors
        """
        score = 100.0

        # Table verification (40% of metadata score)
        if not table_resolution.selected_table:
            score -= 40
        elif not table_resolution.table_columns:
            score -= 20

        # Column verification (40% of metadata score)
        if validation.columns_verified:
            # Partial credit for verified columns
            verified_ratio = len(validation.columns_verified) / max(1, len(validation.columns_verified) + 1)
            score -= (1 - verified_ratio) * 40

        # Validation warnings (20% of metadata score)
        if validation.warnings:
            score -= min(20, len(validation.warnings) * 5)

        return max(0, score)

    def _score_execution(self, execution: ExecutionResult) -> float:
        """
        Score execution success.

        Factors:
        - Query executed successfully
        - Execution time reasonable
        - No errors
        """
        if not execution.success:
            return 0.0

        score = 100.0

        # Penalty for slow execution (> 5 seconds)
        if execution.execution_time_ms > 5000:
            score -= 10
        elif execution.execution_time_ms > 10000:
            score -= 20

        # Penalty for truncated results
        if execution.truncated:
            score -= 5

        return max(0, score)

    def _score_sanity(self, execution: ExecutionResult) -> float:
        """
        Score result sanity.

        Factors:
        - Results not empty (if expected)
        - Results not unreasonably large
        - Values make sense
        """
        if not execution.success:
            return 0.0

        score = 100.0

        # Check for empty results
        if execution.row_count == 0:
            # Empty results might be valid, slight penalty
            score -= 10

        # Check for unreasonably large counts
        if execution.data:
            for row in execution.data:
                for key, value in row.items():
                    if 'count' in key.lower() and isinstance(value, (int, float)):
                        if value > self.config.max_reasonable_count:
                            score -= 20
                        elif value < self.config.min_reasonable_count:
                            score -= 30

        return max(0, score)

    def _determine_level(self, score: float) -> ConfidenceLevel:
        """Determine confidence level from score."""
        if score >= self.config.high_confidence:
            return ConfidenceLevel.HIGH
        elif score >= self.config.medium_confidence:
            return ConfidenceLevel.MEDIUM
        elif score >= self.config.low_confidence:
            return ConfidenceLevel.LOW
        else:
            return ConfidenceLevel.VERY_LOW

    def _generate_explanation(self,
                              overall: float,
                              level: ConfidenceLevel,
                              components: Dict,
                              entities: List[EntityMatch],
                              table_resolution: TableResolution,
                              validation: ValidationResult,
                              execution: ExecutionResult
                             ) -> str:
        """Generate human-readable explanation."""
        explanations = []

        # Overall assessment
        if level == ConfidenceLevel.HIGH:
            explanations.append("High confidence result - reliable answer.")
        elif level == ConfidenceLevel.MEDIUM:
            explanations.append("Medium confidence - verify assumptions.")
        elif level == ConfidenceLevel.LOW:
            explanations.append("Low confidence - review methodology.")
        else:
            explanations.append("Very low confidence - result may not be reliable.")

        # Component explanations
        dict_score = components['dictionary_match']['score']
        if dict_score < 80:
            explanations.append(f"Entity resolution: {dict_score:.0f}% - some terms may not match exactly.")

        meta_score = components['metadata_coverage']['score']
        if meta_score < 80:
            explanations.append(f"Schema coverage: {meta_score:.0f}% - some columns not verified.")

        exec_score = components['execution_success']['score']
        if exec_score < 100:
            if not execution.success:
                explanations.append("Query execution failed.")
            elif execution.truncated:
                explanations.append("Results were truncated due to size limit.")

        sanity_score = components['result_sanity']['score']
        if sanity_score < 80:
            if execution.row_count == 0:
                explanations.append("No results returned - verify query criteria.")

        # Add assumptions if any
        if table_resolution.assumptions:
            explanations.append("Assumptions made:")
            for assumption in table_resolution.assumptions[:3]:  # Limit to 3
                explanations.append(f"  - {assumption}")

        return " ".join(explanations)

    def quick_score(self, execution: ExecutionResult) -> int:
        """
        Quick scoring based only on execution result.

        Used when full context isn't available.
        Returns an integer 0-100 for consistency.
        """
        if not execution.success:
            return 0

        score = 80  # Base score for successful execution

        # Adjustments
        if execution.row_count == 0:
            score -= 10
        if execution.truncated:
            score -= 5
        if execution.execution_time_ms > 5000:
            score -= 5

        return max(0, min(100, int(score)))


def get_confidence_color(level: ConfidenceLevel) -> str:
    """Get display color for confidence level."""
    colors = {
        ConfidenceLevel.HIGH: "green",
        ConfidenceLevel.MEDIUM: "yellow",
        ConfidenceLevel.LOW: "orange",
        ConfidenceLevel.VERY_LOW: "red"
    }
    return colors.get(level, "gray")


def get_confidence_emoji(level: ConfidenceLevel) -> str:
    """Get emoji for confidence level."""
    emojis = {
        ConfidenceLevel.HIGH: "",
        ConfidenceLevel.MEDIUM: "",
        ConfidenceLevel.LOW: "",
        ConfidenceLevel.VERY_LOW: ""
    }
    return emojis.get(level, "")
