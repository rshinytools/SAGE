# Schema Tracker

The Schema Tracker component maintains version history of table schemas for audit compliance.

---

## Overview

**File:** `core/data/schema_tracker.py`

**Class:** `SchemaTracker`

**Purpose:** Track schema changes for GAMP 5 compliance

---

## Why Track Schemas?

Clinical data systems require:

1. **Audit Trail**: Know when data changed
2. **Version Control**: Track schema evolution
3. **Reproducibility**: Recreate historical states
4. **Compliance**: Meet regulatory requirements

---

## Usage

### Initialize Tracker

```python
from core.data.schema_tracker import SchemaTracker

tracker = SchemaTracker("data/database/schema_versions.json")
```

### Record Load Operation

```python
tracker.record_load(
    table_name="ADSL",
    columns=["USUBJID", "AGE", "SEX", "RACE"],
    row_count=233,
    file_path="data/raw/adsl.sas7bdat",
    file_hash="sha256:abc123..."
)
```

### Get History

```python
history = tracker.get_history("ADSL")
for version in history:
    print(f"{version['timestamp']}: {version['row_count']} rows")
```

---

## API Reference

### SchemaTracker Class

```python
class SchemaTracker:
    def __init__(self, tracking_file: str):
        """
        Initialize schema tracker.

        Args:
            tracking_file: Path to JSON tracking file
        """

    def record_load(
        self,
        table_name: str,
        columns: List[str],
        row_count: int,
        file_path: Optional[str] = None,
        file_hash: Optional[str] = None,
        metadata: Optional[Dict] = None
    ) -> str:
        """
        Record a table load operation.

        Args:
            table_name: Name of table loaded
            columns: List of column names
            row_count: Number of rows loaded
            file_path: Source file path
            file_hash: SHA256 hash of source file
            metadata: Additional metadata

        Returns:
            Version ID
        """

    def get_history(
        self,
        table_name: str,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Get version history for a table.

        Args:
            table_name: Name of table
            limit: Maximum versions to return

        Returns:
            List of version records
        """

    def get_current(self, table_name: str) -> Optional[Dict]:
        """Get current (latest) version for a table."""

    def compare_versions(
        self,
        table_name: str,
        version1: str,
        version2: str
    ) -> Dict:
        """Compare two versions and return differences."""
```

---

## Version Record Structure

```json
{
  "version_id": "v_20240115_103000",
  "timestamp": "2024-01-15T10:30:00Z",
  "table_name": "ADSL",
  "columns": [
    {"name": "USUBJID", "type": "VARCHAR"},
    {"name": "AGE", "type": "DOUBLE"},
    {"name": "SEX", "type": "VARCHAR"}
  ],
  "row_count": 233,
  "file_path": "data/raw/adsl.sas7bdat",
  "file_hash": "sha256:abc123def456...",
  "metadata": {
    "source_system": "SAS",
    "loaded_by": "factory1_data.py"
  }
}
```

---

## Change Detection

### Schema Comparison

```python
diff = tracker.compare_versions("ADSL", "v1", "v2")

# Returns:
{
    "columns_added": ["NEW_COL"],
    "columns_removed": ["OLD_COL"],
    "columns_modified": [
        {"name": "AGE", "old_type": "INTEGER", "new_type": "DOUBLE"}
    ],
    "row_count_change": 10
}
```

### Detecting Changes

```python
current = tracker.get_current("ADSL")
if current:
    # Check if columns changed
    if set(new_columns) != set(current["columns"]):
        print("Schema changed!")

    # Check if data changed
    if new_hash != current["file_hash"]:
        print("Data changed!")
```

---

## File Hashing

### Computing Hash

```python
import hashlib

def compute_file_hash(file_path: str) -> str:
    """Compute SHA256 hash of file."""
    sha256 = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            sha256.update(chunk)
    return f"sha256:{sha256.hexdigest()}"
```

### Using Hash for Deduplication

```python
file_hash = compute_file_hash("adsl.sas7bdat")
current = tracker.get_current("ADSL")

if current and current["file_hash"] == file_hash:
    print("File unchanged, skipping load")
else:
    # Load and record
    loader.load_parquet(...)
    tracker.record_load(table_name="ADSL", file_hash=file_hash, ...)
```

---

## Tracking File Format

```json
{
  "tables": {
    "ADSL": {
      "versions": [
        {
          "version_id": "v_20240115_103000",
          "timestamp": "2024-01-15T10:30:00Z",
          "columns": [...],
          "row_count": 233,
          "file_hash": "sha256:abc..."
        },
        {
          "version_id": "v_20240101_090000",
          "timestamp": "2024-01-01T09:00:00Z",
          "columns": [...],
          "row_count": 230,
          "file_hash": "sha256:def..."
        }
      ]
    },
    "ADAE": {
      "versions": [...]
    }
  },
  "metadata": {
    "created": "2024-01-01T00:00:00Z",
    "last_updated": "2024-01-15T10:30:00Z"
  }
}
```

---

## Compliance Features

### Immutable History

Once recorded, versions cannot be deleted:

```python
# This is prevented:
tracker.delete_version("ADSL", "v_20240115")  # Error!
```

### Audit Report

```python
report = tracker.generate_audit_report()

# Returns:
{
    "tables": ["ADSL", "ADAE", "ADLB"],
    "total_loads": 45,
    "date_range": {
        "first": "2024-01-01",
        "last": "2024-01-15"
    },
    "changes_by_table": {
        "ADSL": 3,
        "ADAE": 2,
        "ADLB": 1
    }
}
```

---

## Integration with Factory 1

```python
# In factory1_data.py
def load_table(file_path: str, table_name: str):
    # Read SAS file
    reader = SASReader()
    df, metadata = reader.read(file_path)

    # Load to DuckDB
    loader = DuckDBLoader("clinical.duckdb")
    row_count = loader.load_dataframe(df, table_name)

    # Track schema
    tracker = SchemaTracker("schema_versions.json")
    tracker.record_load(
        table_name=table_name,
        columns=list(df.columns),
        row_count=row_count,
        file_path=file_path,
        file_hash=compute_file_hash(file_path)
    )
```

---

## Next Steps

- [SAS Reader](sas-reader.md)
- [DuckDB Loader](duckdb-loader.md)
- [Factory 1 Overview](overview.md)
