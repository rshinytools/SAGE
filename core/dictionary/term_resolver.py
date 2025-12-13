# SAGE Term Resolver
# ===================
"""
Unified term resolution combining:
1. Fuzzy matching against dataset values
2. MedDRA controlled vocabulary lookup

Resolution Flow:
1. Exact match in dataset → Use directly
2. High-confidence fuzzy match (>90%) → Likely typo, use corrected value
3. MedDRA abbreviation lookup → Expand MI to "MYOCARDIAL INFARCTION"
4. MedDRA term lookup → Validate term exists in controlled vocabulary
5. No match → Return clarification with suggestions from both sources

This ensures clinical data integrity by:
- Only accepting terms that exist in the dataset OR MedDRA
- Never guessing or using AI-based synonym matching
- Always asking for clarification when uncertain
"""

import logging
from pathlib import Path
from typing import Optional, List, Dict, Any, Tuple
from dataclasses import dataclass, field
from enum import Enum

logger = logging.getLogger(__name__)


class MatchSource(str, Enum):
    """Source of the term match."""
    DATASET_EXACT = "dataset_exact"
    DATASET_FUZZY = "dataset_fuzzy"
    MEDDRA_ABBREVIATION = "meddra_abbreviation"
    MEDDRA_EXACT = "meddra_exact"
    NOT_FOUND = "not_found"


@dataclass
class ResolvedTerm:
    """A resolved clinical term."""
    original: str  # User's input
    resolved: str  # Resolved/corrected value
    source: MatchSource
    confidence: float  # 0-100
    table: Optional[str] = None
    column: Optional[str] = None
    meddra_code: Optional[str] = None
    meddra_level: Optional[str] = None


@dataclass
class TermSuggestion:
    """A suggestion for term clarification."""
    value: str
    source: str  # "dataset" or "meddra"
    score: float
    table: Optional[str] = None
    column: Optional[str] = None
    meddra_code: Optional[str] = None
    meddra_level: Optional[str] = None


@dataclass
class ResolutionResult:
    """Result of term resolution."""
    success: bool
    query: str
    resolved_term: Optional[ResolvedTerm] = None
    needs_clarification: bool = False
    message: str = ""
    suggestions: List[TermSuggestion] = field(default_factory=list)


class TermResolver:
    """
    Unified term resolver for clinical data queries.

    Combines fuzzy matching (for typo correction) with MedDRA lookup
    (for controlled vocabulary validation).
    """

    def __init__(
        self,
        fuzzy_matcher=None,
        meddra_lookup=None,
        fuzzy_threshold: float = 85.0,
        auto_correct_threshold: float = 92.0
    ):
        """
        Initialize term resolver.

        Args:
            fuzzy_matcher: FuzzyMatcher instance for dataset lookups
            meddra_lookup: MedDRALookup instance for controlled vocabulary
            fuzzy_threshold: Minimum score for fuzzy suggestions
            auto_correct_threshold: Score above which to auto-correct typos
        """
        self.fuzzy_matcher = fuzzy_matcher
        self.meddra_lookup = meddra_lookup
        self.fuzzy_threshold = fuzzy_threshold
        self.auto_correct_threshold = auto_correct_threshold

    def resolve(self, term: str, column_hint: Optional[str] = None) -> ResolutionResult:
        """
        Resolve a clinical term.

        Resolution order:
        1. Exact match in dataset
        2. High-confidence fuzzy match (typo correction)
        3. MedDRA abbreviation expansion
        4. MedDRA exact term lookup
        5. Return suggestions for clarification

        Args:
            term: Term to resolve
            column_hint: Optional hint about which column (e.g., "AEDECOD")

        Returns:
            ResolutionResult with resolution status and suggestions
        """
        term_clean = term.strip()
        term_upper = term_clean.upper()

        # Step 1: Check exact match in dataset
        if self.fuzzy_matcher:
            exact_matches = self.fuzzy_matcher.match(term_upper, threshold=100, limit=1)
            if exact_matches:
                match = exact_matches[0]
                return ResolutionResult(
                    success=True,
                    query=term,
                    resolved_term=ResolvedTerm(
                        original=term,
                        resolved=match.value,
                        source=MatchSource.DATASET_EXACT,
                        confidence=100.0,
                        table=match.table,
                        column=match.column
                    ),
                    message=f"Exact match found in dataset: {match.value}"
                )

        # Step 2: Check high-confidence fuzzy match (typo correction)
        if self.fuzzy_matcher:
            fuzzy_matches = self.fuzzy_matcher.match(
                term_upper,
                threshold=self.auto_correct_threshold,
                limit=1
            )
            if fuzzy_matches:
                match = fuzzy_matches[0]
                return ResolutionResult(
                    success=True,
                    query=term,
                    resolved_term=ResolvedTerm(
                        original=term,
                        resolved=match.value,
                        source=MatchSource.DATASET_FUZZY,
                        confidence=match.score,
                        table=match.table,
                        column=match.column
                    ),
                    message=f"Typo corrected: '{term}' → '{match.value}' (confidence: {match.score:.1f}%)"
                )

        # Step 3: Check MedDRA abbreviation
        if self.meddra_lookup:
            meddra_result = self.meddra_lookup.lookup(term_upper)

            if meddra_result.found and meddra_result.exact_match:
                # Check if the MedDRA term exists in our dataset
                meddra_term = meddra_result.exact_match.name

                if self.fuzzy_matcher:
                    dataset_match = self.fuzzy_matcher.match(meddra_term.upper(), threshold=100, limit=1)
                    if dataset_match:
                        # MedDRA term exists in dataset
                        return ResolutionResult(
                            success=True,
                            query=term,
                            resolved_term=ResolvedTerm(
                                original=term,
                                resolved=dataset_match[0].value,
                                source=MatchSource.MEDDRA_ABBREVIATION if term_upper != meddra_term.upper() else MatchSource.MEDDRA_EXACT,
                                confidence=95.0,
                                table=dataset_match[0].table,
                                column=dataset_match[0].column,
                                meddra_code=meddra_result.exact_match.code,
                                meddra_level=meddra_result.exact_match.level
                            ),
                            message=f"MedDRA term '{meddra_term}' found in dataset" +
                                   (f" (expanded from '{term}')" if term_upper != meddra_term.upper() else "")
                        )
                    else:
                        # MedDRA term NOT in dataset - need clarification
                        return self._build_clarification(
                            term,
                            f"'{meddra_term}' is a valid MedDRA term but not found in your dataset. "
                            f"Please check if a similar term exists or if this AE was not reported."
                        )

        # Step 4: No match found - gather suggestions for clarification
        return self._build_clarification(
            term,
            f"'{term}' not found in dataset or MedDRA."
        )

    def _build_clarification(self, term: str, message: str) -> ResolutionResult:
        """Build clarification result with suggestions from both sources."""
        suggestions = []
        term_upper = term.strip().upper()

        # Get dataset suggestions (lower threshold for more options)
        if self.fuzzy_matcher:
            fuzzy_matches = self.fuzzy_matcher.match(
                term_upper,
                threshold=50,
                limit=5
            )
            for match in fuzzy_matches:
                suggestions.append(TermSuggestion(
                    value=match.value,
                    source="dataset",
                    score=match.score,
                    table=match.table,
                    column=match.column
                ))

        # Get MedDRA suggestions
        if self.meddra_lookup:
            meddra_result = self.meddra_lookup.lookup(term_upper)
            for related in meddra_result.related_terms[:5]:
                # Check if this MedDRA term exists in dataset
                in_dataset = False
                if self.fuzzy_matcher:
                    dataset_check = self.fuzzy_matcher.match(related.name.upper(), threshold=100, limit=1)
                    in_dataset = len(dataset_check) > 0

                suggestions.append(TermSuggestion(
                    value=related.name,
                    source="meddra" + (" (in dataset)" if in_dataset else ""),
                    score=80.0,  # MedDRA matches are valid terms
                    meddra_code=related.code,
                    meddra_level=related.level
                ))

        # Sort by score and remove duplicates
        seen = set()
        unique_suggestions = []
        for s in sorted(suggestions, key=lambda x: x.score, reverse=True):
            if s.value.upper() not in seen:
                seen.add(s.value.upper())
                unique_suggestions.append(s)

        return ResolutionResult(
            success=False,
            query=term,
            needs_clarification=True,
            message=message + (" Did you mean one of these?" if unique_suggestions else " No similar terms found."),
            suggestions=unique_suggestions[:10]
        )

    def resolve_multiple(self, terms: List[str]) -> Dict[str, ResolutionResult]:
        """
        Resolve multiple terms.

        Args:
            terms: List of terms to resolve

        Returns:
            Dict mapping each term to its resolution result
        """
        results = {}
        for term in terms:
            results[term] = self.resolve(term)
        return results

    def validate_for_query(self, terms: List[str]) -> Tuple[bool, List[ResolvedTerm], List[ResolutionResult]]:
        """
        Validate all terms for a query.

        Returns:
            Tuple of:
            - success: True if all terms resolved
            - resolved_terms: List of successfully resolved terms
            - failed: List of terms needing clarification
        """
        resolved = []
        failed = []

        for term in terms:
            result = self.resolve(term)
            if result.success and result.resolved_term:
                resolved.append(result.resolved_term)
            else:
                failed.append(result)

        return len(failed) == 0, resolved, failed


def create_term_resolver(
    db_path: str,
    knowledge_dir: str,
    fuzzy_index_path: Optional[str] = None
) -> TermResolver:
    """
    Factory function to create a TermResolver with all components.

    Args:
        db_path: Path to DuckDB database
        knowledge_dir: Path to knowledge directory
        fuzzy_index_path: Optional path to fuzzy index pickle file

    Returns:
        Configured TermResolver instance
    """
    fuzzy_matcher = None
    meddra_lookup = None

    # Load fuzzy matcher
    if fuzzy_index_path:
        fuzzy_path = Path(fuzzy_index_path)
    else:
        fuzzy_path = Path(knowledge_dir) / "fuzzy_index.pkl"

    if fuzzy_path.exists():
        try:
            from .fuzzy_matcher import FuzzyMatcher
            fuzzy_matcher = FuzzyMatcher.load(str(fuzzy_path))
            logger.info(f"Loaded fuzzy index with {len(fuzzy_matcher)} entries")
        except Exception as e:
            logger.warning(f"Failed to load fuzzy index: {e}")

    # Load MedDRA lookup
    db_path_obj = Path(db_path)
    if db_path_obj.exists():
        try:
            from core.meddra import MedDRALookup, MedDRALoader
            loader = MedDRALoader(str(db_path), knowledge_dir)
            if loader.is_available():
                meddra_lookup = MedDRALookup(str(db_path))
                logger.info("MedDRA lookup initialized")
        except Exception as e:
            logger.warning(f"Failed to initialize MedDRA lookup: {e}")

    return TermResolver(
        fuzzy_matcher=fuzzy_matcher,
        meddra_lookup=meddra_lookup
    )
