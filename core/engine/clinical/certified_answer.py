"""
Certified Answer System - Deterministic answers for verified queries.

Bypasses LLM for high-confidence matches to guarantee accuracy.
"""

from dataclasses import dataclass
from typing import Optional, Dict, Any, List
from enum import Enum


class CertificationLevel(Enum):
    """Certification levels for query answers."""
    CERTIFIED = "CERTIFIED"      # 98%+ match, deterministic execution
    VERIFIED = "VERIFIED"        # 90-97% match, high confidence
    ASSISTED = "ASSISTED"        # 70-89% match, LLM with examples
    MANUAL = "MANUAL"            # <70% match, needs clarification


@dataclass
class CertifiedMatch:
    """A certified match from the example store."""
    example_id: str
    question: str
    sql: str
    similarity: float
    certification_level: CertificationLevel
    category: str
    usage_count: int
    last_validated: str


@dataclass
class CertificationResult:
    """Result of certification check."""
    is_certified: bool
    certification_level: CertificationLevel
    match: Optional[CertifiedMatch]
    bypass_llm: bool
    sql_to_execute: Optional[str]
    confidence: float
    explanation: str


class CertifiedAnswerSystem:
    """
    Certified Answer System - Deterministic execution for verified queries.

    Key Principle: If we KNOW the answer, don't ask the LLM.
    """

    # Certification thresholds
    CERTIFIED_THRESHOLD = 0.98   # Bypass LLM entirely
    VERIFIED_THRESHOLD = 0.90    # High confidence, still validate
    ASSISTED_THRESHOLD = 0.70    # Use as few-shot example

    def __init__(self, example_store=None):
        """Initialize with optional example store."""
        self.example_store = example_store

    def check_certification(self, question: str) -> CertificationResult:
        """
        Check if query can be answered with certified accuracy.

        Returns:
            CertificationResult indicating whether to bypass LLM.
        """
        # If no example store, can't certify
        if self.example_store is None:
            return CertificationResult(
                is_certified=False,
                certification_level=CertificationLevel.MANUAL,
                match=None,
                bypass_llm=False,
                sql_to_execute=None,
                confidence=0.0,
                explanation="No example store configured."
            )

        # Check for exact match
        exact_match = self.example_store.get_exact_match(question)
        if exact_match:
            return CertificationResult(
                is_certified=True,
                certification_level=CertificationLevel.CERTIFIED,
                match=self._to_certified_match(exact_match, 1.0),
                bypass_llm=True,
                sql_to_execute=exact_match.get('sql'),
                confidence=100.0,
                explanation="Exact match found. Executing verified SQL directly."
            )

        # Check semantic similarity
        similar = self.example_store.find_similar(
            question,
            n_results=1,
            min_similarity=0.0,
            verified_only=True
        )

        if not similar:
            return CertificationResult(
                is_certified=False,
                certification_level=CertificationLevel.MANUAL,
                match=None,
                bypass_llm=False,
                sql_to_execute=None,
                confidence=0.0,
                explanation="No similar verified examples found."
            )

        best_match = similar[0]
        similarity = best_match.get('similarity', 0)

        # Determine certification level
        if similarity >= self.CERTIFIED_THRESHOLD:
            return CertificationResult(
                is_certified=True,
                certification_level=CertificationLevel.CERTIFIED,
                match=self._to_certified_match(best_match, similarity),
                bypass_llm=True,
                sql_to_execute=best_match.get('sql'),
                confidence=similarity * 100,
                explanation=f"98%+ match to verified query. Executing certified SQL directly."
            )

        elif similarity >= self.VERIFIED_THRESHOLD:
            return CertificationResult(
                is_certified=False,
                certification_level=CertificationLevel.VERIFIED,
                match=self._to_certified_match(best_match, similarity),
                bypass_llm=False,
                sql_to_execute=None,
                confidence=similarity * 100,
                explanation=f"90%+ match. Using verified example for few-shot learning."
            )

        elif similarity >= self.ASSISTED_THRESHOLD:
            return CertificationResult(
                is_certified=False,
                certification_level=CertificationLevel.ASSISTED,
                match=self._to_certified_match(best_match, similarity),
                bypass_llm=False,
                sql_to_execute=None,
                confidence=similarity * 100,
                explanation=f"70%+ match. LLM will use similar example as guide."
            )

        else:
            return CertificationResult(
                is_certified=False,
                certification_level=CertificationLevel.MANUAL,
                match=self._to_certified_match(best_match, similarity) if similarity > 0.5 else None,
                bypass_llm=False,
                sql_to_execute=None,
                confidence=similarity * 100,
                explanation="Low similarity. May require clarification."
            )

    def _to_certified_match(self, data: Dict, similarity: float) -> CertifiedMatch:
        """Convert raw data to CertifiedMatch."""
        if similarity >= self.CERTIFIED_THRESHOLD:
            level = CertificationLevel.CERTIFIED
        elif similarity >= self.VERIFIED_THRESHOLD:
            level = CertificationLevel.VERIFIED
        elif similarity >= self.ASSISTED_THRESHOLD:
            level = CertificationLevel.ASSISTED
        else:
            level = CertificationLevel.MANUAL

        return CertifiedMatch(
            example_id=data.get('id', ''),
            question=data.get('question', ''),
            sql=data.get('sql', ''),
            similarity=similarity,
            certification_level=level,
            category=data.get('category', 'general'),
            usage_count=data.get('usage_count', 0),
            last_validated=data.get('last_used_at', '')
        )

    def get_certification_stats(self) -> Dict[str, Any]:
        """Get statistics on certified answers."""
        if self.example_store is None:
            return {"error": "No example store configured"}

        stats = self.example_store.get_statistics()
        return {
            "total_verified_examples": stats.get('verified_examples', 0),
            "certification_ready": stats.get('verified_examples', 0),
            "categories": stats.get('by_category', {}),
            "thresholds": {
                "certified": self.CERTIFIED_THRESHOLD,
                "verified": self.VERIFIED_THRESHOLD,
                "assisted": self.ASSISTED_THRESHOLD
            }
        }
