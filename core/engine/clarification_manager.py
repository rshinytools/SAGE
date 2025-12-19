# SAGE - Clarification Manager
# =============================
"""
Clarification Manager
=====================
Manages clarification dialogs when queries are ambiguous.

Key Principle: Ask when unsure - don't guess
- Better to ask for clarification than give wrong answer
- Provides structured options for user to choose from
- Learns from clarifications to improve future responses
"""

import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum

from .query_analyzer import QueryAnalysis, QueryIntent, Ambiguity

logger = logging.getLogger(__name__)


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ClarificationOption:
    """An option for clarification."""
    id: int
    text: str
    interpretation: str  # What this choice means technically
    confidence_boost: float = 0.2  # How much this would boost confidence


@dataclass
class ClarificationQuestion:
    """A single clarification question with options."""
    question: str
    context: str  # Why we're asking
    options: List[ClarificationOption]
    allow_custom: bool = True  # Allow "other" response


@dataclass
class ClarificationRequest:
    """Complete clarification request to user."""
    original_query: str
    message: str  # Friendly message explaining need for clarification
    questions: List[ClarificationQuestion]
    suggested_rephrasing: Optional[str] = None
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API response."""
        return {
            'type': 'clarification_needed',
            'original_query': self.original_query,
            'message': self.message,
            'questions': [
                {
                    'question': q.question,
                    'context': q.context,
                    'options': [
                        {'id': o.id, 'text': o.text}
                        for o in q.options
                    ],
                    'allow_custom': q.allow_custom
                }
                for q in self.questions
            ],
            'suggested_rephrasing': self.suggested_rephrasing,
            'timestamp': self.timestamp
        }


@dataclass
class ClarificationResponse:
    """User's response to clarification request."""
    original_query: str
    selected_options: Dict[int, int]  # question_index -> option_id
    custom_responses: Dict[int, str]  # question_index -> custom text
    timestamp: str = field(default_factory=lambda: datetime.now().isoformat())


# =============================================================================
# CLARIFICATION MANAGER
# =============================================================================

class ClarificationManager:
    """
    Manages clarification dialogs when queries are ambiguous.
    """

    CLARIFICATION_THRESHOLD = 0.7  # Below this, ask for clarification

    # Common clarification templates
    AMBIGUITY_TEMPLATES = {
        'term_meaning': {
            'question': "When you say '{term}', do you mean:",
            'context': "This term could refer to different things"
        },
        'scope': {
            'question': "What scope should I use?",
            'context': "Your question could apply to different populations"
        },
        'time_period': {
            'question': "What time period are you interested in?",
            'context': "I can filter by different time periods"
        },
        'severity': {
            'question': "Should I include all severities or filter?",
            'context': "Results can vary significantly by severity"
        }
    }

    def __init__(self):
        """Initialize clarification manager."""
        self.history: List[ClarificationRequest] = []

    def needs_clarification(self, analysis: QueryAnalysis) -> bool:
        """
        Check if clarification is needed.

        Args:
            analysis: The query analysis

        Returns:
            True if clarification should be requested
        """
        # Low confidence in understanding
        if analysis.understanding_confidence < self.CLARIFICATION_THRESHOLD:
            logger.info(f"Clarification needed: low confidence "
                       f"({analysis.understanding_confidence:.2f})")
            return True

        # Has ambiguities that need resolution
        if any(a.clarification_needed for a in analysis.ambiguities):
            logger.info("Clarification needed: ambiguities detected")
            return True

        # Intent is unclear
        if analysis.intent == QueryIntent.UNCLEAR:
            logger.info("Clarification needed: unclear intent")
            return True

        return False

    def generate_clarification_request(
        self,
        analysis: QueryAnalysis
    ) -> ClarificationRequest:
        """
        Generate a clarification request for the user.

        Args:
            analysis: The query analysis

        Returns:
            ClarificationRequest with questions for user
        """
        questions = []

        # Handle specific ambiguities
        for i, ambiguity in enumerate(analysis.ambiguities):
            if ambiguity.clarification_needed:
                question = self._create_ambiguity_question(i, ambiguity)
                questions.append(question)

        # If intent unclear, add intent clarification
        if analysis.intent == QueryIntent.UNCLEAR:
            questions.append(self._create_intent_question(analysis))

        # If low confidence but no specific ambiguities, ask general clarification
        if not questions and analysis.understanding_confidence < self.CLARIFICATION_THRESHOLD:
            questions.append(self._create_general_clarification(analysis))

        # Generate friendly message
        message = self._generate_clarification_message(analysis, len(questions))

        # Generate suggested rephrasing
        suggested = self._suggest_rephrasing(analysis)

        request = ClarificationRequest(
            original_query=analysis.original_query,
            message=message,
            questions=questions,
            suggested_rephrasing=suggested
        )

        # Store in history
        self.history.append(request)

        return request

    def _create_ambiguity_question(
        self,
        index: int,
        ambiguity: Ambiguity
    ) -> ClarificationQuestion:
        """Create a question for a specific ambiguity."""
        options = [
            ClarificationOption(
                id=i + 1,
                text=interp,
                interpretation=interp,
                confidence_boost=0.2
            )
            for i, interp in enumerate(ambiguity.interpretations)
        ]

        return ClarificationQuestion(
            question=f"When you say '{ambiguity.text}', do you mean:",
            context="This could be interpreted in different ways",
            options=options,
            allow_custom=True
        )

    def _create_intent_question(self, analysis: QueryAnalysis) -> ClarificationQuestion:
        """Create a question to clarify intent."""
        options = [
            ClarificationOption(
                id=1,
                text="How many patients/subjects experienced this? (Count)",
                interpretation="COUNT_SUBJECTS",
                confidence_boost=0.3
            ),
            ClarificationOption(
                id=2,
                text="Which patients experienced this? (List)",
                interpretation="LIST_RECORDS",
                confidence_boost=0.3
            ),
            ClarificationOption(
                id=3,
                text="How many events/occurrences were there? (Event count)",
                interpretation="COUNT_EVENTS",
                confidence_boost=0.3
            ),
            ClarificationOption(
                id=4,
                text="Show me a breakdown/distribution",
                interpretation="DISTRIBUTION",
                confidence_boost=0.3
            )
        ]

        return ClarificationQuestion(
            question="I want to make sure I understand. Are you asking for:",
            context="Different types of analysis are available",
            options=options,
            allow_custom=True
        )

    def _create_general_clarification(
        self,
        analysis: QueryAnalysis
    ) -> ClarificationQuestion:
        """Create a general clarification when confidence is low."""
        return ClarificationQuestion(
            question="Could you help me understand your question better?",
            context=f"I'm {int(analysis.understanding_confidence * 100)}% confident "
                    "I understand what you're asking",
            options=[
                ClarificationOption(
                    id=1,
                    text="I'm asking about adverse events",
                    interpretation="domain:ADAE",
                    confidence_boost=0.2
                ),
                ClarificationOption(
                    id=2,
                    text="I'm asking about patient demographics",
                    interpretation="domain:ADSL",
                    confidence_boost=0.2
                ),
                ClarificationOption(
                    id=3,
                    text="I'm asking about lab results",
                    interpretation="domain:ADLB",
                    confidence_boost=0.2
                ),
                ClarificationOption(
                    id=4,
                    text="Let me rephrase my question",
                    interpretation="rephrase",
                    confidence_boost=0.0
                )
            ],
            allow_custom=True
        )

    def _generate_clarification_message(
        self,
        analysis: QueryAnalysis,
        num_questions: int
    ) -> str:
        """Generate a friendly clarification message."""
        if analysis.intent == QueryIntent.UNCLEAR:
            return ("I want to make sure I give you the right answer. "
                    "Could you help me understand your question better?")

        if analysis.ambiguities:
            return ("Your question has some terms that could be interpreted "
                    "in different ways. Let me clarify to ensure accuracy.")

        if analysis.understanding_confidence < 0.5:
            return ("I'm not entirely sure I understand your question correctly. "
                    "Could you help me clarify?")

        return ("Just to be sure I answer correctly, I have a quick question.")

    def _suggest_rephrasing(self, analysis: QueryAnalysis) -> Optional[str]:
        """Suggest how user might rephrase for clarity."""
        suggestions = []

        # Suggest being specific about what to count
        if analysis.intent == QueryIntent.UNCLEAR:
            suggestions.append("Try specifying what you want to count or list")

        # Suggest mentioning the data domain
        if not analysis.suggested_table:
            suggestions.append("mentioning whether you're asking about "
                              "adverse events, demographics, or lab data")

        # Suggest specifying population
        if not analysis.suggested_population:
            suggestions.append("specifying which population "
                              "(Safety, ITT, or all subjects)")

        if suggestions:
            return "Try: " + ", ".join(suggestions)

        return None

    def apply_clarification(
        self,
        analysis: QueryAnalysis,
        response: ClarificationResponse
    ) -> QueryAnalysis:
        """
        Apply user's clarification to improve the analysis.

        Args:
            analysis: Original analysis
            response: User's clarification response

        Returns:
            Updated QueryAnalysis with clarifications applied
        """
        # Create updated analysis
        updated = QueryAnalysis(
            original_query=analysis.original_query,
            intent=analysis.intent,
            subject=analysis.subject,
            conditions=analysis.conditions.copy(),
            group_by=analysis.group_by,
            limit=analysis.limit,
            ambiguities=[],  # Clear ambiguities - they're now resolved
            understanding_confidence=min(1.0, analysis.understanding_confidence + 0.2),
            needs_clarification=False,
            suggested_table=analysis.suggested_table,
            suggested_population=analysis.suggested_population
        )

        # Apply selected options
        # This would need to be customized based on how options are structured

        logger.info("Applied clarification, new confidence: "
                   f"{updated.understanding_confidence:.2f}")

        return updated


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def create_quick_clarification(
    query: str,
    issue: str,
    options: List[str]
) -> ClarificationRequest:
    """Create a quick clarification request."""
    return ClarificationRequest(
        original_query=query,
        message=f"I need a quick clarification: {issue}",
        questions=[
            ClarificationQuestion(
                question=issue,
                context="",
                options=[
                    ClarificationOption(id=i+1, text=opt, interpretation=opt)
                    for i, opt in enumerate(options)
                ],
                allow_custom=True
            )
        ]
    )
