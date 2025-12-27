"""
Learning Module for SAGE AI Chat System.

This module provides components for:
- Example Store: Store and retrieve query-SQL pairs
- Complexity Scorer: Assess query difficulty
- Semantic Validator: Validate SQL matches intent
- Result Validator: Check result sanity
- Confidence Manager: Calculate final confidence
- Feedback Handler: Process user feedback
- Training Manager: Admin training interface
"""

from .example_store import (
    ExampleStore,
    LearningExample
)

from .complexity_scorer import (
    ComplexityScorer,
    ComplexityLevel,
    ComplexityAssessment
)

from .semantic_validator import (
    SemanticValidator,
    SemanticValidation,
    ValidationResult
)

from .result_validator import (
    ResultValidator,
    ResultValidation
)

from .confidence_manager import (
    ConfidenceManager,
    ConfidenceResult,
    ResponseAction
)

from .feedback_handler import (
    FeedbackHandler,
    FeedbackType,
    FeedbackResult
)

__all__ = [
    # Example Store
    'ExampleStore',
    'LearningExample',

    # Complexity Scorer
    'ComplexityScorer',
    'ComplexityLevel',
    'ComplexityAssessment',

    # Semantic Validator
    'SemanticValidator',
    'SemanticValidation',
    'ValidationResult',

    # Result Validator
    'ResultValidator',
    'ResultValidation',

    # Confidence Manager
    'ConfidenceManager',
    'ConfidenceResult',
    'ResponseAction',

    # Feedback Handler
    'FeedbackHandler',
    'FeedbackType',
    'FeedbackResult'
]
