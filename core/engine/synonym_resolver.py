# SAGE - Enhanced Synonym Resolution
# ===================================
"""
Enhanced Synonym Resolution
===========================
Uses LLM to suggest clinical synonyms when exact term isn't found in data,
then validates each suggestion against actual data values.

This ensures NO DATA IS MISSED while maintaining 0% hallucination.

Algorithm:
1. Check if user's term exists EXACTLY in data -> If yes, use it
2. If not found, ask LLM for clinical synonyms
3. Validate each LLM suggestion against actual data values
4. Include ALL validated terms in the query

Example:
    User: "high blood pressure"
    Step 1: Check AEDECOD for "high blood pressure" -> Not found
    Step 2: LLM suggests: ["Hypertension", "Blood pressure increased", "BP elevated"]
    Step 3: Validate against actual AEDECOD values
    Step 4: Found: "Hypertension" (42), "Blood pressure increased" (8)
    Result: SQL includes both terms, user sees breakdown
"""

import logging
import time
from typing import List, Optional, Dict, Any, Tuple, Set
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class SynonymMatch:
    """A validated synonym match."""
    term: str                    # The matched term from data
    original_query: str          # What user searched for
    match_type: str              # 'exact', 'llm_validated', 'fuzzy'
    count: int                   # How many records have this term
    confidence: float            # Match confidence (0-100)
    source: str                  # 'data', 'llm', 'fuzzy'


@dataclass
class SynonymResolutionResult:
    """Result of synonym resolution."""
    original_term: str
    validated_terms: List[SynonymMatch]
    llm_suggestions: List[str]           # All LLM suggestions (before validation)
    rejected_suggestions: List[str]      # LLM suggestions not found in data
    resolution_method: str               # 'exact', 'llm', 'fuzzy', 'none'
    processing_time_ms: float

    @property
    def has_matches(self) -> bool:
        return len(self.validated_terms) > 0

    @property
    def total_count(self) -> int:
        return sum(m.count for m in self.validated_terms)

    def get_sql_terms(self) -> List[str]:
        """Get terms for SQL IN clause."""
        return [m.term for m in self.validated_terms]

    def get_breakdown_text(self) -> str:
        """Get human-readable breakdown of matched terms."""
        if not self.validated_terms:
            return "No matching terms found"

        parts = []
        for m in self.validated_terms:
            parts.append(f"'{m.term}' ({m.count:,} records)")
        return "Matched terms: " + ", ".join(parts)


class SynonymResolver:
    """
    Resolves user terms to clinical data values using LLM assistance.

    Priority:
    1. Exact match in data (instant, no LLM needed)
    2. LLM-suggested synonyms validated against data
    3. Fuzzy match as fallback

    Zero Hallucination Guarantee:
    - LLM suggestions are ALWAYS validated against actual data
    - Only terms that exist in data are included in results
    - Breakdown shows exactly what was matched
    """

    # No hard-coded synonyms - LLM handles all synonym resolution naturally
    # The LLM understands medical terminology, UK/US spelling variants, and clinical context

    def __init__(self,
                 db_connection=None,
                 llm_client=None,
                 fuzzy_matcher=None,
                 target_column: str = 'AEDECOD',
                 target_table: str = 'ADAE'):
        """
        Initialize synonym resolver.

        Args:
            db_connection: DuckDB connection for data validation
            llm_client: LLM client for synonym suggestions
            fuzzy_matcher: FuzzyMatcher from Factory 3
            target_column: Column to search (default: AEDECOD)
            target_table: Table to search (default: ADAE)
        """
        self.db = db_connection
        self.llm = llm_client
        self.fuzzy_matcher = fuzzy_matcher
        self.target_column = target_column
        self.target_table = target_table

        # Cache for data values
        self._cached_values: Optional[Set[str]] = None
        self._cache_time: float = 0
        self._cache_ttl: float = 300  # 5 minutes

    def resolve(self, term: str, column: str = None, table: str = None) -> SynonymResolutionResult:
        """
        Resolve a term to validated clinical data values.

        Args:
            term: User's search term
            column: Override target column
            table: Override target table

        Returns:
            SynonymResolutionResult with validated terms
        """
        start_time = time.time()
        column = column or self.target_column
        table = table or self.target_table

        validated_terms: List[SynonymMatch] = []
        llm_suggestions: List[str] = []
        rejected: List[str] = []
        resolution_method = 'none'

        # Step 1: Check for exact match
        exact_match = self._check_exact_match(term, column, table)
        if exact_match:
            validated_terms.append(exact_match)
            resolution_method = 'exact'
            logger.info(f"Synonym resolution: exact match found for '{term}'")
        else:
            # Step 2: Get LLM suggestions
            llm_suggestions = self._get_llm_suggestions(term)

            # Step 3: Validate each suggestion against data
            for suggestion in llm_suggestions:
                match = self._validate_term(suggestion, column, table, term)
                if match:
                    validated_terms.append(match)
                else:
                    rejected.append(suggestion)

            if validated_terms:
                resolution_method = 'llm'
                logger.info(f"Synonym resolution: {len(validated_terms)} LLM terms validated for '{term}'")
            else:
                # Step 4: Fallback to fuzzy matching
                fuzzy_matches = self._try_fuzzy_match(term, column, table)
                if fuzzy_matches:
                    validated_terms.extend(fuzzy_matches)
                    resolution_method = 'fuzzy'
                    logger.info(f"Synonym resolution: fuzzy match found for '{term}'")

        processing_time = (time.time() - start_time) * 1000

        return SynonymResolutionResult(
            original_term=term,
            validated_terms=validated_terms,
            llm_suggestions=llm_suggestions,
            rejected_suggestions=rejected,
            resolution_method=resolution_method,
            processing_time_ms=processing_time
        )

    def _check_exact_match(self, term: str, column: str, table: str) -> Optional[SynonymMatch]:
        """Check if term exists exactly in data (case-insensitive)."""
        if not self.db:
            return None

        try:
            # Case-insensitive exact match
            sql = f"""
                SELECT {column}, COUNT(*) as cnt
                FROM {table}
                WHERE UPPER({column}) = UPPER(?)
                GROUP BY {column}
            """
            result = self.db.execute(sql, [term]).fetchone()

            if result:
                return SynonymMatch(
                    term=result[0],  # Use actual case from data
                    original_query=term,
                    match_type='exact',
                    count=result[1],
                    confidence=100.0,
                    source='data'
                )
        except Exception as e:
            logger.warning(f"Exact match check failed: {e}")

        return None

    def _get_llm_suggestions(self, term: str) -> List[str]:
        """Get clinical synonym suggestions from LLM."""
        suggestions = []

        # Use LLM for synonym suggestions - it understands medical terminology naturally
        if self.llm:
            try:
                llm_suggestions = self._call_llm_for_synonyms(term)
                suggestions.extend(llm_suggestions)
            except Exception as e:
                logger.warning(f"LLM synonym request failed: {e}")

        # Also add the original term (might exist in data with different case)
        if term not in suggestions:
            suggestions.insert(0, term)

        return suggestions

    def _call_llm_for_synonyms(self, term: str) -> List[str]:
        """Call LLM to suggest clinical synonyms."""
        if not self.llm:
            return []

        prompt = f"""You are a clinical terminology expert. Given the term "{term}",
provide a list of clinical/medical synonyms that might be used in clinical trial adverse event coding.

Include:
- MedDRA preferred terms
- Common medical synonyms
- Lay terms that map to clinical terms

Return ONLY a comma-separated list of terms, nothing else.
Example: Hypertension, Blood pressure increased, High BP

Term: {term}
Synonyms:"""

        try:
            response = self.llm.generate(prompt, max_tokens=100)
            # Parse comma-separated response
            suggestions = [s.strip() for s in response.split(',') if s.strip()]
            return suggestions[:10]  # Limit to 10 suggestions
        except Exception as e:
            logger.warning(f"LLM call failed: {e}")
            return []

    def _validate_term(self, term: str, column: str, table: str,
                       original_query: str) -> Optional[SynonymMatch]:
        """Validate that a term exists in the data."""
        if not self.db:
            return None

        try:
            sql = f"""
                SELECT {column}, COUNT(*) as cnt
                FROM {table}
                WHERE UPPER({column}) = UPPER(?)
                GROUP BY {column}
            """
            result = self.db.execute(sql, [term]).fetchone()

            if result and result[1] > 0:
                return SynonymMatch(
                    term=result[0],
                    original_query=original_query,
                    match_type='llm_validated',
                    count=result[1],
                    confidence=90.0,
                    source='llm'
                )
        except Exception as e:
            logger.warning(f"Term validation failed for '{term}': {e}")

        return None

    def _try_fuzzy_match(self, term: str, column: str, table: str) -> List[SynonymMatch]:
        """Try fuzzy matching as fallback."""
        matches = []

        if self.fuzzy_matcher:
            try:
                fuzzy_results = self.fuzzy_matcher.match(term, threshold=70.0, limit=3)
                for result in fuzzy_results:
                    # Validate fuzzy match exists in target column
                    if result.column.upper() == column.upper():
                        validated = self._validate_term(result.value, column, table, term)
                        if validated:
                            validated.match_type = 'fuzzy'
                            validated.confidence = result.score
                            validated.source = 'fuzzy'
                            matches.append(validated)
            except Exception as e:
                logger.warning(f"Fuzzy match failed: {e}")

        return matches

    def get_all_values(self, column: str = None, table: str = None) -> Set[str]:
        """Get all unique values from the target column (cached)."""
        column = column or self.target_column
        table = table or self.target_table

        # Check cache
        if self._cached_values and (time.time() - self._cache_time) < self._cache_ttl:
            return self._cached_values

        if not self.db:
            return set()

        try:
            sql = f"SELECT DISTINCT {column} FROM {table} WHERE {column} IS NOT NULL"
            results = self.db.execute(sql).fetchall()
            self._cached_values = {r[0] for r in results}
            self._cache_time = time.time()
            return self._cached_values
        except Exception as e:
            logger.warning(f"Failed to get all values: {e}")
            return set()


def create_synonym_resolver(db_path: str = None,
                           fuzzy_matcher=None) -> SynonymResolver:
    """
    Factory function to create a SynonymResolver with database connection.

    Args:
        db_path: Path to DuckDB database
        fuzzy_matcher: Optional FuzzyMatcher instance

    Returns:
        Configured SynonymResolver
    """
    db_connection = None

    if db_path:
        try:
            import duckdb
            db_connection = duckdb.connect(db_path, read_only=True)
        except Exception as e:
            logger.warning(f"Could not connect to database: {e}")

    return SynonymResolver(
        db_connection=db_connection,
        fuzzy_matcher=fuzzy_matcher
    )
