# Factory 3 API Reference

Complete API documentation for Factory 3: Dictionary Plant components.

---

## Module: core.dictionary.fuzzy_matcher

### FuzzyMatcher

```python
class FuzzyMatcher:
    """Fuzzy term matching using RapidFuzz."""

    def __init__(
        self,
        threshold: float = 80.0,
        max_results: int = 5
    ):
        """
        Initialize fuzzy matcher.

        Args:
            threshold: Minimum match score (0-100)
            max_results: Maximum results per query
        """

    def build_index(
        self,
        terms: List[Tuple[str, str, str]]
    ) -> int:
        """
        Build search index from terms.

        Args:
            terms: List of (term, table, column) tuples

        Returns:
            Number of terms indexed
        """

    def find_matches(
        self,
        query: str,
        limit: Optional[int] = None,
        threshold: Optional[float] = None
    ) -> List[MatchResult]:
        """
        Find matching terms.

        Args:
            query: Search string
            limit: Override max_results
            threshold: Override threshold

        Returns:
            Matches sorted by score (highest first)
        """

    def exact_match(self, query: str) -> Optional[MatchResult]:
        """Find exact case-insensitive match."""

    def batch_find(
        self,
        queries: List[str]
    ) -> Dict[str, List[MatchResult]]:
        """Find matches for multiple queries."""

    def save(self, path: str) -> None:
        """Save index to pickle file."""

    def load(self, path: str) -> None:
        """Load index from pickle file."""

    def get_all_terms(self) -> List[str]:
        """Get all indexed terms."""

    def get_index_stats(self) -> Dict[str, Any]:
        """Get index statistics."""
```

### MatchResult

```python
@dataclass
class MatchResult:
    """Result of a fuzzy match."""
    term: str
    score: float
    table: str
    column: str
    match_type: str  # "exact" or "fuzzy"

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
```

---

## Module: core.dictionary.value_scanner

### ValueScanner

```python
class ValueScanner:
    """Scan database columns for unique values."""

    def __init__(
        self,
        loader: DuckDBLoader,
        config: Optional[Dict] = None
    ):
        """
        Initialize scanner.

        Args:
            loader: DuckDB connection
            config: Scanning configuration
        """

    def scan_column(
        self,
        table: str,
        column: str,
        limit: Optional[int] = None,
        min_occurrences: int = 1
    ) -> List[str]:
        """
        Get unique values from column.

        Args:
            table: Table name
            column: Column name
            limit: Max values
            min_occurrences: Min occurrence count

        Returns:
            List of unique values
        """

    def scan_all(self) -> Dict[Tuple[str, str], List[str]]:
        """Scan all configured columns."""

    def get_statistics(
        self,
        table: str,
        column: str
    ) -> ColumnStats:
        """Get column statistics."""

    def scan_pattern(
        self,
        table: str,
        column: str,
        pattern: str
    ) -> List[str]:
        """Get values matching SQL LIKE pattern."""

    def clear_cache(self) -> None:
        """Clear cached scan results."""
```

### ColumnStats

```python
@dataclass
class ColumnStats:
    """Statistics for a column."""
    table: str
    column: str
    unique_count: int
    null_count: int
    total_rows: int
    top_values: List[Dict[str, Any]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
```

---

## Module: core.dictionary.schema_mapper

### SchemaMapper

```python
class SchemaMapper:
    """Map clinical concepts to table columns."""

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize mapper.

        Args:
            config_path: Path to schema map JSON
        """

    def get_column(
        self,
        concept: str,
        available_tables: List[str]
    ) -> Optional[Tuple[str, str]]:
        """
        Get table/column for a concept.

        Args:
            concept: Clinical concept name
            available_tables: Tables in database

        Returns:
            (table, column) or None if not found
        """

    def get_all_concepts(self) -> List[str]:
        """Get list of known concepts."""

    def add_mapping(
        self,
        concept: str,
        table: str,
        column: str,
        priority: int = 0
    ) -> None:
        """Add a concept mapping."""

    def save(self, path: str) -> None:
        """Save mappings to JSON file."""

    def load(self, path: str) -> None:
        """Load mappings from JSON file."""
```

---

## Configuration

### Scanner Configuration

```python
config = {
    "columns": [
        ("ADAE", "AEDECOD"),
        ("ADAE", "AETERM"),
    ],
    "max_values_per_column": 10000,
    "min_value_length": 2,
    "exclude_patterns": ["^\\s*$"],  # Exclude empty
}

scanner = ValueScanner(loader, config=config)
```

### Schema Map Format

```json
{
  "adverse_event_term": {
    "mappings": [
      {"table": "ADAE", "column": "AEDECOD", "priority": 1},
      {"table": "AE", "column": "AEDECOD", "priority": 2}
    ]
  },
  "medication_name": {
    "mappings": [
      {"table": "ADCM", "column": "CMDECOD", "priority": 1},
      {"table": "CM", "column": "CMDECOD", "priority": 2}
    ]
  }
}
```

---

## Complete Example

```python
from core.data.duckdb_loader import DuckDBLoader
from core.dictionary.value_scanner import ValueScanner
from core.dictionary.fuzzy_matcher import FuzzyMatcher
from core.dictionary.schema_mapper import SchemaMapper

def build_dictionary(db_path: str, output_dir: str):
    """Build complete dictionary from database."""

    # 1. Connect to database
    loader = DuckDBLoader(db_path, read_only=True)

    # 2. Scan for values
    scanner = ValueScanner(loader)
    all_values = scanner.scan_all()

    # 3. Build fuzzy index
    terms = []
    for (table, column), values in all_values.items():
        for value in values:
            terms.append((value, table, column))

    matcher = FuzzyMatcher(threshold=80)
    matcher.build_index(terms)
    matcher.save(f"{output_dir}/fuzzy_index.pkl")

    # 4. Build schema map
    mapper = SchemaMapper()
    mapper.add_mapping("adverse_event", "ADAE", "AEDECOD", priority=1)
    mapper.add_mapping("medication", "ADCM", "CMDECOD", priority=1)
    mapper.save(f"{output_dir}/schema_map.json")

    # 5. Export statistics
    stats = {}
    for table, column in all_values.keys():
        key = f"{table}.{column}"
        stats[key] = scanner.get_statistics(table, column).to_dict()

    import json
    with open(f"{output_dir}/value_stats.json", "w") as f:
        json.dump(stats, f, indent=2)

    loader.close()
    return len(terms)
```

---

## Exceptions

```python
class DictionaryError(Exception):
    """Base exception for dictionary operations."""
    pass

class IndexBuildError(DictionaryError):
    """Error building index."""
    pass

class MatchError(DictionaryError):
    """Error during matching."""
    pass
```

---

## Next Steps

- [Factory 3 Overview](overview.md)
- [Factory 3.5 MedDRA](../factory35-meddra/overview.md)
