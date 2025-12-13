# SAGE - Entity Extractor
# ========================
"""
Entity Extractor
================
Extracts clinical entities from user queries and resolves them
using Factory 3 (FuzzyMatcher) and Factory 3.5 (MedDRA).

This is STEP 2 of the 9-step pipeline.
"""

import re
import logging
import time
from typing import List, Optional, Dict, Any, Tuple
from dataclasses import dataclass

from .models import EntityMatch, EntityExtractionResult

logger = logging.getLogger(__name__)


class EntityExtractor:
    """
    Extracts and resolves clinical entities from queries.

    Uses:
    - Factory 3: FuzzyMatcher for term resolution
    - Factory 3.5: MedDRA for medical terminology

    Example:
        extractor = EntityExtractor(fuzzy_matcher, meddra_lookup)
        result = extractor.extract("How many patients had headakes?")
        # Returns: EntityExtractionResult with "headakes" -> "HEADACHE"
    """

    # Common clinical term patterns
    GRADE_PATTERN = re.compile(r'\bgrade\s*(\d+)\b', re.IGNORECASE)
    POPULATION_PATTERN = re.compile(r'\b(safety|itt|efficacy|per.?protocol)\s*population\b', re.IGNORECASE)
    COUNT_PATTERN = re.compile(r'\b(how\s+many|count|number\s+of)\b', re.IGNORECASE)

    # Words to skip (not clinical entities)
    STOP_WORDS = {
        'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
        'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
        'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'can', 'need',
        'how', 'many', 'much', 'what', 'which', 'who', 'whom', 'whose',
        'where', 'when', 'why', 'that', 'this', 'these', 'those', 'it',
        'its', 'they', 'them', 'their', 'we', 'us', 'our', 'you', 'your',
        'he', 'him', 'his', 'she', 'her', 'hers', 'i', 'me', 'my', 'mine',
        'all', 'any', 'each', 'every', 'both', 'few', 'more', 'most',
        'other', 'some', 'such', 'no', 'not', 'only', 'same', 'so', 'than',
        'too', 'very', 'just', 'also', 'now', 'here', 'there', 'then',
        'show', 'list', 'find', 'get', 'give', 'tell', 'patients', 'subjects',
        'reported', 'experienced', 'had', 'have', 'having', 'who', 'whom',
    }

    def __init__(self,
                 fuzzy_matcher=None,
                 meddra_lookup=None,
                 min_confidence: float = 70.0):
        """
        Initialize entity extractor.

        Args:
            fuzzy_matcher: FuzzyMatcher instance from Factory 3
            meddra_lookup: MedDRA lookup instance from Factory 3.5
            min_confidence: Minimum confidence for entity matches
        """
        self.fuzzy_matcher = fuzzy_matcher
        self.meddra_lookup = meddra_lookup
        self.min_confidence = min_confidence

    def extract(self, query: str) -> EntityExtractionResult:
        """
        Extract entities from query.

        Args:
            query: User's natural language query

        Returns:
            EntityExtractionResult with matched entities
        """
        start_time = time.time()
        entities: List[EntityMatch] = []
        unresolved: List[str] = []

        # Extract special patterns first
        grade_matches = self._extract_grades(query)
        entities.extend(grade_matches)

        # Extract candidate terms
        candidates = self._extract_candidates(query)

        # Resolve each candidate
        for term in candidates:
            match = self._resolve_term(term)
            if match:
                entities.append(match)
            else:
                unresolved.append(term)

        # Build query with resolved entities
        query_resolved = self._build_resolved_query(query, entities)

        processing_time = (time.time() - start_time) * 1000

        return EntityExtractionResult(
            entities=entities,
            query_with_resolved=query_resolved,
            unresolved_terms=unresolved,
            processing_time_ms=processing_time
        )

    def _extract_candidates(self, query: str) -> List[str]:
        """Extract candidate terms from query."""
        # Tokenize
        words = re.findall(r'\b[a-zA-Z]+\b', query.lower())

        # Filter stop words and short words
        candidates = []
        for word in words:
            if word not in self.STOP_WORDS and len(word) > 2:
                candidates.append(word)

        # Also extract multi-word phrases (bigrams)
        for i in range(len(words) - 1):
            if words[i] not in self.STOP_WORDS or words[i+1] not in self.STOP_WORDS:
                phrase = f"{words[i]} {words[i+1]}"
                candidates.append(phrase)

        return candidates

    def _extract_grades(self, query: str) -> List[EntityMatch]:
        """Extract grade patterns."""
        matches = []
        for match in self.GRADE_PATTERN.finditer(query):
            grade = match.group(1)
            matches.append(EntityMatch(
                original_term=match.group(0),
                matched_term=grade,
                match_type="grade",
                confidence=100.0,
                column="ATOXGR"  # Default to analysis grade
            ))
        return matches

    def _resolve_term(self, term: str) -> Optional[EntityMatch]:
        """
        Resolve a term using fuzzy matcher and MedDRA.

        Args:
            term: Term to resolve

        Returns:
            EntityMatch if resolved, None otherwise
        """
        # Try MedDRA first (for medical terms)
        if self.meddra_lookup:
            meddra_match = self._try_meddra(term)
            if meddra_match:
                return meddra_match

        # Try fuzzy matcher
        if self.fuzzy_matcher:
            fuzzy_match = self._try_fuzzy(term)
            if fuzzy_match:
                return fuzzy_match

        return None

    def _try_meddra(self, term: str) -> Optional[EntityMatch]:
        """Try to resolve term via MedDRA."""
        if not self.meddra_lookup:
            return None

        try:
            # Search MedDRA using the search() method
            # MedDRALookup.search() returns List[SearchResult]
            # SearchResult has: term (MedDRATerm), match_score, hierarchy
            results = self.meddra_lookup.search(term, limit=1)
            if results and len(results) > 0:
                result = results[0]
                # match_score is 0-100
                confidence = result.match_score

                if confidence >= self.min_confidence:
                    return EntityMatch(
                        original_term=term,
                        matched_term=result.term.name,
                        match_type="meddra",
                        confidence=confidence,
                        table="ADAE",
                        column="AEDECOD",
                        meddra_code=str(result.term.code),
                        meddra_level=result.term.level
                    )
        except Exception as e:
            logger.warning(f"MedDRA lookup failed for '{term}': {e}")

        return None

    def _try_fuzzy(self, term: str) -> Optional[EntityMatch]:
        """Try to resolve term via fuzzy matcher."""
        if not self.fuzzy_matcher:
            return None

        try:
            # Search fuzzy index
            # FuzzyMatcher.match() returns List[FuzzyMatch]
            # FuzzyMatch has: value, score, table, column, match_type, original_query
            results = self.fuzzy_matcher.match(term, threshold=self.min_confidence, limit=1)
            if results and len(results) > 0:
                result = results[0]
                # FuzzyMatch is an object, not a dict
                confidence = result.score

                if confidence >= self.min_confidence:
                    return EntityMatch(
                        original_term=term,
                        matched_term=result.value,
                        match_type=result.match_type,
                        confidence=confidence,
                        table=result.table,
                        column=result.column
                    )
        except Exception as e:
            logger.warning(f"Fuzzy match failed for '{term}': {e}")

        return None

    def _build_resolved_query(self, query: str, entities: List[EntityMatch]) -> str:
        """Build query with resolved entities noted."""
        resolved = query

        for entity in entities:
            if entity.match_type != "grade":  # Don't replace grade patterns
                # Add resolution note
                pattern = re.compile(re.escape(entity.original_term), re.IGNORECASE)
                replacement = f"{entity.matched_term}"
                resolved = pattern.sub(replacement, resolved)

        return resolved


class SimpleEntityExtractor(EntityExtractor):
    """
    Simplified entity extractor that works without Factory 3/3.5.

    Uses basic pattern matching and keyword extraction.
    """

    # Common clinical terms (basic dictionary)
    CLINICAL_TERMS = {
        # Adverse events
        'headache': ('HEADACHE', 'ADAE', 'AEDECOD'),
        'headaches': ('HEADACHE', 'ADAE', 'AEDECOD'),
        'headake': ('HEADACHE', 'ADAE', 'AEDECOD'),  # Common typo
        'nausea': ('NAUSEA', 'ADAE', 'AEDECOD'),
        'vomiting': ('VOMITING', 'ADAE', 'AEDECOD'),
        'fever': ('PYREXIA', 'ADAE', 'AEDECOD'),
        'hypertension': ('HYPERTENSION', 'ADAE', 'AEDECOD'),
        'death': ('DEATH', 'ADAE', 'AEDECOD'),
        'fatigue': ('FATIGUE', 'ADAE', 'AEDECOD'),
        'diarrhea': ('DIARRHOEA', 'ADAE', 'AEDECOD'),
        'diarrhoea': ('DIARRHOEA', 'ADAE', 'AEDECOD'),
        'rash': ('RASH', 'ADAE', 'AEDECOD'),
        'pain': ('PAIN', 'ADAE', 'AEDECOD'),
        'cough': ('COUGH', 'ADAE', 'AEDECOD'),

        # Medications
        'tylenol': ('TYLENOL', 'CM', 'CMTRT'),
        'aspirin': ('ASPIRIN', 'CM', 'CMTRT'),
        'ibuprofen': ('IBUPROFEN', 'CM', 'CMTRT'),
    }

    def __init__(self, min_confidence: float = 80.0):
        """Initialize simple extractor."""
        super().__init__(None, None, min_confidence)

    def _resolve_term(self, term: str) -> Optional[EntityMatch]:
        """Resolve term using built-in dictionary."""
        term_lower = term.lower()

        if term_lower in self.CLINICAL_TERMS:
            matched, table, column = self.CLINICAL_TERMS[term_lower]
            return EntityMatch(
                original_term=term,
                matched_term=matched,
                match_type="dictionary",
                confidence=95.0,
                table=table,
                column=column
            )

        # Try fuzzy match against dictionary keys
        from difflib import get_close_matches
        matches = get_close_matches(term_lower, self.CLINICAL_TERMS.keys(), n=1, cutoff=0.8)
        if matches:
            matched, table, column = self.CLINICAL_TERMS[matches[0]]
            return EntityMatch(
                original_term=term,
                matched_term=matched,
                match_type="fuzzy_dictionary",
                confidence=85.0,
                table=table,
                column=column
            )

        return None
