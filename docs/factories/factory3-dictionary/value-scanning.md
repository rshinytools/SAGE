# Value Scanning

The ValueScanner component extracts unique values from database columns for indexing.

---

## Overview

**File:** `core/dictionary/value_scanner.py`

**Class:** `ValueScanner`

**Purpose:** Profile data and extract searchable values

---

## Usage

### Basic Scanning

```python
from core.dictionary.value_scanner import ValueScanner
from core.data.duckdb_loader import DuckDBLoader

loader = DuckDBLoader("clinical.duckdb", read_only=True)
scanner = ValueScanner(loader)

# Scan single column
values = scanner.scan_column("ADAE", "AEDECOD")
# ["HEADACHE", "NAUSEA", "FATIGUE", ...]
```

### Scan All Tables

```python
# Scan all configured columns
all_values = scanner.scan_all()
# {
#   ("ADAE", "AEDECOD"): ["HEADACHE", ...],
#   ("ADCM", "CMDECOD"): ["ASPIRIN", ...],
# }
```

---

## API Reference

### ValueScanner Class

```python
class ValueScanner:
    def __init__(
        self,
        loader: DuckDBLoader,
        config: Optional[Dict] = None
    ):
        """
        Initialize scanner.

        Args:
            loader: DuckDB loader instance
            config: Configuration with columns to scan
        """

    def scan_column(
        self,
        table: str,
        column: str,
        limit: Optional[int] = None
    ) -> List[str]:
        """
        Get unique values from a column.

        Args:
            table: Table name
            column: Column name
            limit: Max values to return

        Returns:
            List of unique values
        """

    def scan_all(self) -> Dict[Tuple[str, str], List[str]]:
        """
        Scan all configured columns.

        Returns:
            Dict mapping (table, column) to value lists
        """

    def get_statistics(
        self,
        table: str,
        column: str
    ) -> Dict[str, Any]:
        """
        Get column statistics.

        Returns:
            Dict with unique_count, null_count, top_values
        """

    def scan_pattern(
        self,
        table: str,
        column: str,
        pattern: str
    ) -> List[str]:
        """
        Get values matching a pattern.

        Args:
            pattern: SQL LIKE pattern

        Returns:
            Matching values
        """
```

---

## Default Configuration

### Columns Scanned

```python
DEFAULT_COLUMNS = [
    # Adverse Events
    ("ADAE", "AEDECOD"),
    ("ADAE", "AETERM"),
    ("ADAE", "AEBODSYS"),
    ("AE", "AEDECOD"),
    ("AE", "AETERM"),

    # Medications
    ("ADCM", "CMDECOD"),
    ("CM", "CMDECOD"),

    # Laboratory
    ("ADLB", "LBTESTCD"),
    ("ADLB", "LBTEST"),
    ("LB", "LBTESTCD"),

    # Medical History
    ("ADMH", "MHDECOD"),
    ("MH", "MHDECOD"),
]
```

### Custom Configuration

```python
scanner = ValueScanner(loader, config={
    "columns": [
        ("ADAE", "AEDECOD"),
        ("ADAE", "AELLT"),  # Add LLT terms
    ],
    "max_values_per_column": 10000,
    "min_value_length": 2
})
```

---

## Value Statistics

### Get Column Profile

```python
stats = scanner.get_statistics("ADAE", "AEDECOD")

# Returns:
{
    "table": "ADAE",
    "column": "AEDECOD",
    "unique_count": 245,
    "null_count": 0,
    "total_rows": 1500,
    "top_values": [
        {"value": "HEADACHE", "count": 150},
        {"value": "NAUSEA", "count": 120},
        {"value": "FATIGUE", "count": 95}
    ]
}
```

### Null Handling

```python
# Values with nulls are excluded
values = scanner.scan_column("ADAE", "AEDECOD")
# None/NULL values not included
```

---

## Filtering

### By Pattern

```python
# Get values starting with "HEAD"
values = scanner.scan_pattern("ADAE", "AEDECOD", "HEAD%")
# ["HEADACHE", "HEAD INJURY", ...]
```

### By Length

```python
# Skip very short values
scanner = ValueScanner(loader, config={
    "min_value_length": 3
})
```

### By Frequency

```python
# Only include values with multiple occurrences
values = scanner.scan_column(
    "ADAE", "AEDECOD",
    min_occurrences=2
)
```

---

## Performance

### Caching

```python
# Results are cached after first scan
values1 = scanner.scan_column("ADAE", "AEDECOD")  # Queries DB
values2 = scanner.scan_column("ADAE", "AEDECOD")  # Uses cache
```

### Clear Cache

```python
scanner.clear_cache()
```

### Parallel Scanning

```python
# Scan multiple columns in parallel
from concurrent.futures import ThreadPoolExecutor

with ThreadPoolExecutor(max_workers=4) as executor:
    futures = {
        executor.submit(scanner.scan_column, t, c): (t, c)
        for t, c in columns_to_scan
    }
```

---

## Output Format

### For Fuzzy Matcher

```python
# Format values for fuzzy matcher
terms = []
for (table, column), values in scanner.scan_all().items():
    for value in values:
        terms.append((value, table, column))

matcher.build_index(terms)
```

### Export to JSON

```python
import json

all_values = scanner.scan_all()
export = {
    f"{table}.{column}": values
    for (table, column), values in all_values.items()
}
with open("value_stats.json", "w") as f:
    json.dump(export, f, indent=2)
```

---

## Error Handling

### Missing Table

```python
try:
    values = scanner.scan_column("MISSING", "COL")
except TableNotFoundError:
    # Table doesn't exist
    pass
```

### Missing Column

```python
try:
    values = scanner.scan_column("ADAE", "MISSING_COL")
except ColumnNotFoundError:
    # Column doesn't exist
    pass
```

---

## Next Steps

- [Fuzzy Matching](fuzzy-matching.md)
- [Factory 3 Overview](overview.md)
- [API Reference](api-reference.md)
