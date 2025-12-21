# DuckDB Loader

The DuckDB Loader component loads data into DuckDB for fast analytical queries.

---

## Overview

**File:** `core/data/duckdb_loader.py`

**Class:** `DuckDBLoader`

**Dependencies:**
- `duckdb` - Database engine
- `pandas` - DataFrame handling

---

## Usage

### Basic Loading

```python
from core.data.duckdb_loader import DuckDBLoader

loader = DuckDBLoader("data/database/clinical.duckdb")
loader.load_parquet("data/processed/ADSL.parquet", "ADSL")
```

### From DataFrame

```python
loader.load_dataframe(df, "ADSL")
```

---

## API Reference

### DuckDBLoader Class

```python
class DuckDBLoader:
    def __init__(self, db_path: str, read_only: bool = False):
        """
        Initialize DuckDB loader.

        Args:
            db_path: Path to DuckDB database file
            read_only: Open in read-only mode
        """

    def load_parquet(
        self,
        parquet_path: str,
        table_name: str,
        replace: bool = True
    ) -> int:
        """
        Load Parquet file into DuckDB table.

        Args:
            parquet_path: Path to Parquet file
            table_name: Target table name
            replace: Drop existing table if exists

        Returns:
            Number of rows loaded
        """

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        replace: bool = True
    ) -> int:
        """
        Load DataFrame into DuckDB table.

        Args:
            df: Pandas DataFrame
            table_name: Target table name
            replace: Drop existing table if exists

        Returns:
            Number of rows loaded
        """

    def execute(self, sql: str) -> List[Tuple]:
        """Execute SQL query and return results."""

    def get_tables(self) -> List[str]:
        """Get list of all tables in database."""

    def get_columns(self, table_name: str) -> List[Dict]:
        """Get column info for a table."""
```

---

## Loading Strategies

### Replace (Default)

Drops existing table and creates new:

```python
loader.load_parquet("ADSL.parquet", "ADSL", replace=True)
# DROP TABLE IF EXISTS ADSL;
# CREATE TABLE ADSL AS SELECT * FROM 'ADSL.parquet';
```

### Append

Adds to existing table:

```python
loader.load_parquet("ADSL_new.parquet", "ADSL", replace=False)
# INSERT INTO ADSL SELECT * FROM 'ADSL_new.parquet';
```

---

## Schema Management

### View Table Schema

```python
columns = loader.get_columns("ADSL")

# Returns:
[
    {"name": "USUBJID", "type": "VARCHAR", "nullable": True},
    {"name": "AGE", "type": "DOUBLE", "nullable": True},
    {"name": "SEX", "type": "VARCHAR", "nullable": True}
]
```

### Type Mapping

| Parquet Type | DuckDB Type |
|--------------|-------------|
| string | VARCHAR |
| int32/int64 | BIGINT |
| float/double | DOUBLE |
| timestamp | TIMESTAMP |
| date | DATE |
| bool | BOOLEAN |

---

## Query Execution

### Simple Query

```python
result = loader.execute("SELECT COUNT(*) FROM ADSL")
print(result[0][0])  # 233
```

### With Parameters

```python
result = loader.execute(
    "SELECT * FROM ADAE WHERE AEDECOD = ?",
    parameters=["HEADACHE"]
)
```

### Get DataFrame

```python
df = loader.execute_df("SELECT * FROM ADSL WHERE AGE > 65")
```

---

## Performance Features

### Native Parquet Reading

DuckDB reads Parquet files directly without intermediate steps:

```python
# This is very fast - no data copying
loader.execute("SELECT * FROM 'data/*.parquet'")
```

### Parallel Processing

DuckDB automatically uses multiple CPU cores:

```python
# Configure thread count
loader.execute("SET threads = 4")
```

### Memory Management

```python
# Set memory limit
loader.execute("SET memory_limit = '4GB'")
```

---

## Maintenance Operations

### Optimize Table

```python
loader.execute("VACUUM ADSL")
```

### Check Database Size

```python
result = loader.execute("""
    SELECT table_name,
           estimated_size / 1024 / 1024 as size_mb
    FROM duckdb_tables()
""")
```

### Export Data

```python
# Export to Parquet
loader.execute("COPY ADSL TO 'export/adsl.parquet'")

# Export to CSV
loader.execute("COPY ADSL TO 'export/adsl.csv' (HEADER)")
```

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `TableNotFoundError` | Table doesn't exist | Check table name |
| `ColumnNotFoundError` | Column doesn't exist | Check column name |
| `IOException` | File access error | Check permissions |

### Example

```python
try:
    loader.load_parquet("missing.parquet", "TEST")
except FileNotFoundError:
    print("Parquet file not found")
except duckdb.IOException as e:
    print(f"Database error: {e}")
```

---

## Read-Only Mode

For query execution, use read-only mode:

```python
# Safe for concurrent reads
loader = DuckDBLoader("clinical.duckdb", read_only=True)

# This will fail:
# loader.execute("DROP TABLE ADSL")  # Error!
```

---

## Connection Management

### Context Manager

```python
with DuckDBLoader("clinical.duckdb") as loader:
    loader.load_parquet("ADSL.parquet", "ADSL")
# Connection automatically closed
```

### Manual Close

```python
loader = DuckDBLoader("clinical.duckdb")
try:
    loader.load_parquet("ADSL.parquet", "ADSL")
finally:
    loader.close()
```

---

## Next Steps

- [Schema Tracker](schema-tracker.md)
- [SAS Reader](sas-reader.md)
- [Factory 1 Overview](overview.md)
