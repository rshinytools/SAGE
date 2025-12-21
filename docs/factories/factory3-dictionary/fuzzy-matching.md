# Fuzzy Matching

The FuzzyMatcher component provides intelligent term matching using RapidFuzz.

---

## Overview

**File:** `core/dictionary/fuzzy_matcher.py`

**Class:** `FuzzyMatcher`

**Library:** RapidFuzz (C++ optimized)

---

## Why Fuzzy Matching?

Users may enter terms with:

- **Typos**: `Tyleonl` → `TYLENOL`
- **Case differences**: `headache` → `HEADACHE`
- **Partial terms**: `naus` → `NAUSEA`
- **Spelling variations**: `diarrhea` / `diarrhoea`

---

## Usage

### Build Index

```python
from core.dictionary.fuzzy_matcher import FuzzyMatcher

# Initialize and build
matcher = FuzzyMatcher()
matcher.build_index([
    ("HEADACHE", "ADAE", "AEDECOD"),
    ("NAUSEA", "ADAE", "AEDECOD"),
    ("FATIGUE", "ADAE", "AEDECOD")
])
```

### Find Matches

```python
# Find best matches for a term
matches = matcher.find_matches("headche")

# Returns:
# [
#   MatchResult(term="HEADACHE", score=94.0, table="ADAE", column="AEDECOD"),
#   MatchResult(term="HEARTACHE", score=82.0, table="ADAE", column="AEDECOD")
# ]
```

---

## API Reference

### FuzzyMatcher Class

```python
class FuzzyMatcher:
    def __init__(
        self,
        threshold: float = 80.0,
        max_results: int = 5
    ):
        """
        Initialize fuzzy matcher.

        Args:
            threshold: Minimum score (0-100) for matches
            max_results: Maximum matches to return
        """

    def build_index(
        self,
        terms: List[Tuple[str, str, str]]
    ) -> None:
        """
        Build search index from terms.

        Args:
            terms: List of (term, table, column) tuples
        """

    def find_matches(
        self,
        query: str,
        limit: Optional[int] = None,
        threshold: Optional[float] = None
    ) -> List[MatchResult]:
        """
        Find matching terms for a query.

        Args:
            query: Search term
            limit: Max results (overrides default)
            threshold: Min score (overrides default)

        Returns:
            List of MatchResult, sorted by score descending
        """

    def exact_match(self, query: str) -> Optional[MatchResult]:
        """Find exact match only."""

    def save(self, path: str) -> None:
        """Save index to file."""

    def load(self, path: str) -> None:
        """Load index from file."""
```

### MatchResult

```python
@dataclass
class MatchResult:
    term: str          # Matched term
    score: float       # Match score (0-100)
    table: str         # Source table
    column: str        # Source column
    match_type: str    # "exact", "fuzzy"
```

---

## Scoring Algorithm

### Weighted Ratio

RapidFuzz's WRatio combines multiple algorithms:

```python
from rapidfuzz import fuzz

# Simple ratio
fuzz.ratio("headache", "headche")  # 88.0

# Partial ratio (for substrings)
fuzz.partial_ratio("head", "headache")  # 100.0

# Weighted ratio (best combined)
fuzz.WRatio("headache", "headche")  # 94.0
```

### Score Interpretation

| Score | Meaning | Example |
|-------|---------|---------|
| 100 | Exact match | headache → HEADACHE |
| 95-99 | Near exact | headache → HEADACHES |
| 85-94 | Good match | headche → HEADACHE |
| 70-84 | Fair match | head → HEADACHE |
| <70 | Poor match | xyz → HEADACHE |

---

## Advanced Matching

### Case Insensitive

All matching is case-insensitive:

```python
matches = matcher.find_matches("HEADACHE")  # Same as "headache"
```

### Multiple Columns

Index can include multiple columns:

```python
matcher.build_index([
    ("HEADACHE", "ADAE", "AEDECOD"),
    ("HEADACHE", "AE", "AEDECOD"),  # Same term, different table
])
```

### Threshold Override

```python
# Strict matching
strict = matcher.find_matches("head", threshold=95)  # Few results

# Loose matching
loose = matcher.find_matches("head", threshold=60)  # More results
```

---

## Performance Optimization

### Index Caching

```python
# Save index after build
matcher.build_index(terms)
matcher.save("knowledge/fuzzy_index.pkl")

# Load for fast startup
matcher.load("knowledge/fuzzy_index.pkl")
```

### Batch Processing

```python
# Process multiple queries efficiently
queries = ["headache", "nausea", "fatigue"]
results = matcher.batch_find(queries)
```

---

## Integration Example

```python
from core.dictionary.fuzzy_matcher import FuzzyMatcher
from core.dictionary.value_scanner import ValueScanner
from core.data.duckdb_loader import DuckDBLoader

# 1. Get values from database
loader = DuckDBLoader("clinical.duckdb", read_only=True)
scanner = ValueScanner(loader)
terms = scanner.scan_column("ADAE", "AEDECOD")

# 2. Build index
matcher = FuzzyMatcher(threshold=80)
matcher.build_index([
    (term, "ADAE", "AEDECOD") for term in terms
])

# 3. Use for matching
user_input = "headche"  # User typo
matches = matcher.find_matches(user_input)
if matches:
    best_match = matches[0]
    print(f"Did you mean '{best_match.term}'?")
```

---

## Handling Special Cases

### No Matches

```python
matches = matcher.find_matches("xyzabc123")
if not matches:
    # No matches found - ask for clarification
    pass
```

### Ambiguous Matches

```python
matches = matcher.find_matches("pain")
if len(matches) > 1 and matches[0].score < 95:
    # Multiple similar matches - show options
    for m in matches:
        print(f"{m.term}: {m.score}%")
```

---

## Next Steps

- [Value Scanning](value-scanning.md)
- [Factory 3 Overview](overview.md)
- [API Reference](api-reference.md)
