# SAGE Dictionary - Fuzzy Matcher
# ================================
"""
Fast fuzzy string matching using RapidFuzz library.
Supports typo correction, partial matching, and token-based matching.
"""

import pickle
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass, field
from collections import defaultdict

from rapidfuzz import fuzz, process
from rapidfuzz.distance import Levenshtein

logger = logging.getLogger(__name__)


@dataclass
class FuzzyMatch:
    """A single fuzzy match result."""
    value: str              # The matched value
    score: float            # Match score (0-100)
    table: str              # Source table
    column: str             # Source column
    match_type: str         # "exact", "fuzzy", "partial", "token"
    original_query: str = ""  # The original query string

    @property
    def id(self) -> str:
        """Unique identifier for this match."""
        return f"{self.table}.{self.column}.{self.value}"

    @property
    def sql_condition(self) -> str:
        """Generate SQL WHERE condition for this match."""
        # Escape single quotes in value
        escaped_value = self.value.replace("'", "''")
        return f'"{self.column}" = \'{escaped_value}\''

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "value": self.value,
            "score": self.score,
            "table": self.table,
            "column": self.column,
            "match_type": self.match_type,
            "id": self.id,
            "sql_condition": self.sql_condition
        }


@dataclass
class IndexEntry:
    """Entry in the fuzzy index."""
    value: str
    value_lower: str
    table: str
    column: str


class FuzzyMatcher:
    """
    RapidFuzz-based fuzzy string matcher for clinical data values.

    Supports multiple matching strategies:
    - Simple ratio: Basic Levenshtein similarity
    - Partial ratio: Best substring match
    - Token sort ratio: Word order insensitive
    - Token set ratio: Handles extra/missing words
    """

    def __init__(self):
        """Initialize empty matcher."""
        self._index: List[IndexEntry] = []
        self._values_by_table: Dict[str, Dict[str, List[str]]] = defaultdict(lambda: defaultdict(list))
        self._all_values: List[str] = []
        self._value_to_entries: Dict[str, List[IndexEntry]] = defaultdict(list)

    # Minimum length for values to be indexed (prevents Y/N and short codes)
    MIN_VALUE_LENGTH = 2

    # Values to exclude from indexing (common flags that cause false matches)
    EXCLUDED_VALUES = {
        'y', 'n', 'yes', 'no', 'na', 'unk', 'u', 'm', 'f',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    }

    # Columns containing codes that should be excluded from fuzzy search
    CODE_COLUMNS = {
        'SOC_CODE', 'HLGT_CODE', 'HLT_CODE', 'PT_CODE', 'LLT_CODE',
        'USUBJID', 'STUDYID', 'SUBJID', 'SITEID',
    }

    def build_index(self, values: Dict[str, Dict[str, List[str]]]) -> int:
        """
        Build flat index from nested value dictionary.

        Filters out:
        - Values shorter than MIN_VALUE_LENGTH
        - Common flag values (Y, N, etc.)
        - Code columns (contain IDs, not searchable text)

        Args:
            values: {table: {column: [values]}}

        Returns:
            Number of entries indexed
        """
        self._index = []
        self._values_by_table = defaultdict(lambda: defaultdict(list))
        self._all_values = []
        self._value_to_entries = defaultdict(list)

        seen_values = set()
        skipped_short = 0
        skipped_excluded = 0
        skipped_codes = 0

        for table, columns in values.items():
            for column, value_list in columns.items():
                column_upper = column.upper()

                # Skip code columns
                if column_upper in self.CODE_COLUMNS:
                    skipped_codes += len(value_list)
                    continue

                for value in value_list:
                    if not value or not isinstance(value, str):
                        continue

                    value_clean = value.strip()
                    if not value_clean:
                        continue

                    # Skip short values
                    if len(value_clean) < self.MIN_VALUE_LENGTH:
                        skipped_short += 1
                        continue

                    # Skip excluded values (common flags)
                    if value_clean.lower() in self.EXCLUDED_VALUES:
                        skipped_excluded += 1
                        continue

                    entry = IndexEntry(
                        value=value_clean,
                        value_lower=value_clean.lower(),
                        table=table.upper(),
                        column=column_upper
                    )

                    self._index.append(entry)
                    self._values_by_table[table.upper()][column_upper].append(value_clean)
                    self._value_to_entries[value_clean.lower()].append(entry)

                    if value_clean.lower() not in seen_values:
                        self._all_values.append(value_clean)
                        seen_values.add(value_clean.lower())

        logger.info(
            f"Built fuzzy index with {len(self._index)} entries, {len(self._all_values)} unique values. "
            f"Skipped: {skipped_short} short, {skipped_excluded} excluded, {skipped_codes} codes"
        )
        return len(self._index)

    def match(self,
              query: str,
              threshold: float = 80.0,
              limit: int = 10,
              scorer: str = "smart") -> List[FuzzyMatch]:
        """
        Find fuzzy matches for a query across all indexed values.

        Uses a smart scoring algorithm optimized for clinical data:
        - Penalizes large length differences (prevents "Y" matching "TYLENOL")
        - Prioritizes ratio and token-based matching over partial matching
        - Requires minimum match quality based on string similarity

        Args:
            query: The search query
            threshold: Minimum score threshold (0-100)
            limit: Maximum number of results
            scorer: Scoring method - "ratio", "partial", "token_sort", "token_set", "smart"

        Returns:
            List of FuzzyMatch results, sorted by score descending
        """
        if not query or not self._all_values:
            return []

        query_clean = query.strip()
        query_lower = query_clean.lower()
        query_len = len(query_clean)

        # Check for exact match first
        if query_lower in self._value_to_entries:
            exact_matches = []
            for entry in self._value_to_entries[query_lower]:
                exact_matches.append(FuzzyMatch(
                    value=entry.value,
                    score=100.0,
                    table=entry.table,
                    column=entry.column,
                    match_type="exact",
                    original_query=query_clean
                ))
            if exact_matches:
                return exact_matches[:limit]

        # Smart scoring for clinical data
        scored_results = []

        for value in self._all_values:
            value_len = len(value)

            # Skip values that are too short compared to query (prevents "Y" matching "TYLENOL")
            # Allow some flexibility: value should be at least 40% of query length
            min_len_ratio = 0.4
            max_len_ratio = 3.0  # Value shouldn't be more than 3x query length

            len_ratio = value_len / query_len if query_len > 0 else 0
            if len_ratio < min_len_ratio or len_ratio > max_len_ratio:
                # Only skip if query is reasonably long (> 3 chars)
                if query_len > 3:
                    continue

            # Calculate multiple scores
            ratio_score = fuzz.ratio(query_clean, value)
            token_sort_score = fuzz.token_sort_ratio(query_clean, value)
            token_set_score = fuzz.token_set_ratio(query_clean, value)

            # Only use partial_ratio for longer queries where substring match makes sense
            if query_len >= 4:
                partial_score = fuzz.partial_ratio(query_clean, value)
                # Penalize partial matches where the value is very short
                if value_len < query_len * 0.5:
                    partial_score = partial_score * 0.5  # Heavy penalty
            else:
                partial_score = ratio_score  # For short queries, use ratio

            # Smart weighted score - prioritize full matches over partial
            if scorer == "smart" or scorer == "weighted":
                # Weight: ratio (40%), token_sort (30%), token_set (20%), partial (10%)
                final_score = (
                    ratio_score * 0.40 +
                    token_sort_score * 0.30 +
                    token_set_score * 0.20 +
                    partial_score * 0.10
                )

                # Bonus for similar length (encourages better matches)
                len_similarity = 1 - abs(query_len - value_len) / max(query_len, value_len)
                final_score = final_score * (0.9 + 0.1 * len_similarity)
            elif scorer == "ratio":
                final_score = ratio_score
            elif scorer == "partial":
                final_score = partial_score
            elif scorer == "token_sort":
                final_score = token_sort_score
            elif scorer == "token_set":
                final_score = token_set_score
            else:
                final_score = ratio_score

            if final_score >= threshold:
                scored_results.append((value, final_score, ratio_score))

        # Sort by final score, then by ratio score (for tie-breaking)
        scored_results.sort(key=lambda x: (-x[1], -x[2], x[0]))

        # Convert to FuzzyMatch objects
        matches = []
        seen = set()

        for value, score, ratio_score in scored_results[:limit * 3]:
            value_lower = value.lower()
            if value_lower in seen:
                continue
            seen.add(value_lower)

            # Get all entries for this value
            for entry in self._value_to_entries.get(value_lower, []):
                match = FuzzyMatch(
                    value=entry.value,
                    score=round(score, 1),
                    table=entry.table,
                    column=entry.column,
                    match_type=self._determine_match_type(query_clean, entry.value, score),
                    original_query=query_clean
                )
                matches.append(match)

        # Sort by score and deduplicate
        matches.sort(key=lambda m: (-m.score, m.value))

        # Keep top matches, but allow multiple columns for same value
        unique_matches = []
        seen_ids = set()
        for m in matches:
            if m.id not in seen_ids:
                seen_ids.add(m.id)
                unique_matches.append(m)
                if len(unique_matches) >= limit:
                    break

        return unique_matches

    def match_in_column(self,
                        query: str,
                        table: str,
                        column: str,
                        threshold: float = 80.0,
                        limit: int = 10) -> List[FuzzyMatch]:
        """
        Find fuzzy matches within a specific table.column.

        Args:
            query: The search query
            table: Table name to search in
            column: Column name to search in
            threshold: Minimum score threshold
            limit: Maximum results

        Returns:
            List of FuzzyMatch results
        """
        table_upper = table.upper()
        column_upper = column.upper()

        values = self._values_by_table.get(table_upper, {}).get(column_upper, [])
        if not values:
            return []

        query_clean = query.strip()

        # Check exact match
        query_lower = query_clean.lower()
        for v in values:
            if v.lower() == query_lower:
                return [FuzzyMatch(
                    value=v,
                    score=100.0,
                    table=table_upper,
                    column=column_upper,
                    match_type="exact",
                    original_query=query_clean
                )]

        # Fuzzy match
        results = process.extract(
            query_clean,
            values,
            scorer=fuzz.WRatio,
            limit=limit,
            score_cutoff=threshold
        )

        matches = []
        for value, score, _ in results:
            matches.append(FuzzyMatch(
                value=value,
                score=score,
                table=table_upper,
                column=column_upper,
                match_type=self._determine_match_type(query_clean, value, score),
                original_query=query_clean
            ))

        return matches

    def match_multi_strategy(self,
                             query: str,
                             threshold: float = 75.0,
                             limit: int = 10) -> List[FuzzyMatch]:
        """
        Match using multiple strategies and combine results.

        Uses ratio, partial_ratio, token_sort_ratio, and token_set_ratio,
        then combines scores for best results.

        Args:
            query: The search query
            threshold: Minimum score threshold
            limit: Maximum results

        Returns:
            List of FuzzyMatch with combined scores
        """
        if not query or not self._all_values:
            return []

        query_clean = query.strip()
        query_lower = query_clean.lower()

        # Check exact match first
        if query_lower in self._value_to_entries:
            exact_matches = []
            for entry in self._value_to_entries[query_lower]:
                exact_matches.append(FuzzyMatch(
                    value=entry.value,
                    score=100.0,
                    table=entry.table,
                    column=entry.column,
                    match_type="exact",
                    original_query=query_clean
                ))
            if exact_matches:
                return exact_matches[:limit]

        # Calculate scores with multiple strategies
        scored_values: Dict[str, Dict[str, float]] = {}

        for value in self._all_values:
            scores = {
                "ratio": fuzz.ratio(query_clean, value),
                "partial": fuzz.partial_ratio(query_clean, value),
                "token_sort": fuzz.token_sort_ratio(query_clean, value),
                "token_set": fuzz.token_set_ratio(query_clean, value),
            }

            # Weighted combination
            combined = (
                scores["ratio"] * 0.25 +
                scores["partial"] * 0.25 +
                scores["token_sort"] * 0.25 +
                scores["token_set"] * 0.25
            )

            if combined >= threshold:
                scored_values[value] = {**scores, "combined": combined}

        # Sort by combined score
        sorted_values = sorted(
            scored_values.items(),
            key=lambda x: -x[1]["combined"]
        )[:limit * 2]

        # Build match results
        matches = []
        seen_ids = set()

        for value, scores in sorted_values:
            value_lower = value.lower()
            for entry in self._value_to_entries.get(value_lower, []):
                match_id = f"{entry.table}.{entry.column}.{entry.value}"
                if match_id in seen_ids:
                    continue
                seen_ids.add(match_id)

                # Determine best match type
                max_type = max(
                    [("ratio", scores["ratio"]),
                     ("partial", scores["partial"]),
                     ("token_sort", scores["token_sort"]),
                     ("token_set", scores["token_set"])],
                    key=lambda x: x[1]
                )[0]

                match = FuzzyMatch(
                    value=entry.value,
                    score=scores["combined"],
                    table=entry.table,
                    column=entry.column,
                    match_type=f"fuzzy_{max_type}",
                    original_query=query_clean
                )
                matches.append(match)

                if len(matches) >= limit:
                    break

            if len(matches) >= limit:
                break

        return matches

    def _determine_match_type(self, query: str, value: str, score: float) -> str:
        """Determine the type of match based on query and value."""
        if score >= 100:
            return "exact"
        elif score >= 95:
            return "near_exact"
        elif query.lower() in value.lower() or value.lower() in query.lower():
            return "partial"
        else:
            return "fuzzy"

    def get_statistics(self) -> Dict[str, Any]:
        """Get index statistics."""
        table_counts = {}
        for entry in self._index:
            key = f"{entry.table}.{entry.column}"
            table_counts[key] = table_counts.get(key, 0) + 1

        return {
            "total_entries": len(self._index),
            "unique_values": len(self._all_values),
            "tables": len(self._values_by_table),
            "columns": sum(len(cols) for cols in self._values_by_table.values()),
            "entries_by_column": table_counts
        }

    def save(self, path: str) -> None:
        """Save index to pickle file."""
        data = {
            "index": self._index,
            "values_by_table": dict(self._values_by_table),
            "all_values": self._all_values,
            "value_to_entries": dict(self._value_to_entries)
        }

        with open(path, 'wb') as f:
            pickle.dump(data, f)

        logger.info(f"Saved fuzzy index to {path}")

    @classmethod
    def load(cls, path: str) -> 'FuzzyMatcher':
        """Load index from pickle file."""
        matcher = cls()

        with open(path, 'rb') as f:
            data = pickle.load(f)

        matcher._index = data["index"]
        matcher._values_by_table = defaultdict(lambda: defaultdict(list), data["values_by_table"])
        matcher._all_values = data["all_values"]
        matcher._value_to_entries = defaultdict(list, data["value_to_entries"])

        logger.info(f"Loaded fuzzy index from {path} with {len(matcher._index)} entries")
        return matcher

    def __len__(self) -> int:
        return len(self._index)
