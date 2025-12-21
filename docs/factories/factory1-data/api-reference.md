# Factory 1 API Reference

Complete API documentation for Factory 1: Data Foundry components.

---

## Module: core.data.sas_reader

### SASReader

```python
class SASReader:
    """Read SAS7BDAT files and extract metadata."""

    def __init__(self, encoding: str = "utf-8"):
        """
        Initialize SAS reader.

        Args:
            encoding: Default character encoding for reading files
        """

    def read(
        self,
        file_path: str,
        encoding: Optional[str] = None,
        convert_dates: bool = True,
        columns: Optional[List[str]] = None
    ) -> Tuple[pd.DataFrame, Dict[str, Any]]:
        """
        Read a SAS7BDAT file into a DataFrame.

        Args:
            file_path: Path to the SAS7BDAT file
            encoding: Character encoding (None uses default)
            convert_dates: Convert SAS dates to Python datetime
            columns: Specific columns to read (None reads all)

        Returns:
            Tuple of (DataFrame with data, metadata dictionary)

        Raises:
            FileNotFoundError: If file doesn't exist
            SASReadError: If file is corrupt or unreadable
        """

    def get_metadata(self, file_path: str) -> Dict[str, Any]:
        """
        Get file metadata without reading data.

        Args:
            file_path: Path to the SAS7BDAT file

        Returns:
            Dictionary containing:
            - file_path: str
            - row_count: int
            - column_count: int
            - columns: List[Dict] with name, type, length, label
            - encoding: str
            - created: datetime
            - modified: datetime
        """

    def read_chunks(
        self,
        file_path: str,
        chunk_size: int = 100000
    ) -> Iterator[pd.DataFrame]:
        """
        Read file in chunks for memory efficiency.

        Args:
            file_path: Path to the SAS7BDAT file
            chunk_size: Number of rows per chunk

        Yields:
            DataFrame chunks
        """
```

---

## Module: core.data.duckdb_loader

### DuckDBLoader

```python
class DuckDBLoader:
    """Load data into DuckDB database."""

    def __init__(
        self,
        db_path: str,
        read_only: bool = False
    ):
        """
        Initialize DuckDB loader.

        Args:
            db_path: Path to DuckDB database file
            read_only: Open database in read-only mode
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
            table_name: Name of target table
            replace: If True, drop existing table first

        Returns:
            Number of rows loaded

        Raises:
            FileNotFoundError: If Parquet file doesn't exist
            DuckDBError: If load fails
        """

    def load_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        replace: bool = True
    ) -> int:
        """
        Load DataFrame directly into DuckDB.

        Args:
            df: Pandas DataFrame to load
            table_name: Name of target table
            replace: If True, drop existing table first

        Returns:
            Number of rows loaded
        """

    def execute(
        self,
        sql: str,
        parameters: Optional[List] = None
    ) -> List[Tuple]:
        """
        Execute SQL query and return results.

        Args:
            sql: SQL query string
            parameters: Query parameters for placeholders

        Returns:
            List of result tuples
        """

    def execute_df(
        self,
        sql: str,
        parameters: Optional[List] = None
    ) -> pd.DataFrame:
        """
        Execute SQL and return results as DataFrame.

        Args:
            sql: SQL query string
            parameters: Query parameters

        Returns:
            DataFrame with query results
        """

    def get_tables(self) -> List[str]:
        """
        Get list of all tables in database.

        Returns:
            List of table names
        """

    def get_columns(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Get column information for a table.

        Args:
            table_name: Name of table

        Returns:
            List of dicts with name, type, nullable keys
        """

    def table_exists(self, table_name: str) -> bool:
        """Check if table exists in database."""

    def drop_table(self, table_name: str) -> None:
        """Drop a table from database."""

    def close(self) -> None:
        """Close database connection."""

    def __enter__(self) -> "DuckDBLoader":
        """Context manager entry."""

    def __exit__(self, *args) -> None:
        """Context manager exit - closes connection."""
```

---

## Module: core.data.schema_tracker

### SchemaTracker

```python
class SchemaTracker:
    """Track schema versions for audit compliance."""

    def __init__(self, tracking_file: str):
        """
        Initialize schema tracker.

        Args:
            tracking_file: Path to JSON file for storing versions
        """

    def record_load(
        self,
        table_name: str,
        columns: List[str],
        row_count: int,
        file_path: Optional[str] = None,
        file_hash: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Record a table load operation.

        Args:
            table_name: Name of table that was loaded
            columns: List of column names
            row_count: Number of rows in table
            file_path: Path to source file
            file_hash: SHA256 hash of source file
            metadata: Additional metadata to store

        Returns:
            Version ID (e.g., "v_20240115_103000")
        """

    def get_history(
        self,
        table_name: str,
        limit: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Get version history for a table.

        Args:
            table_name: Name of table
            limit: Maximum number of versions to return

        Returns:
            List of version records, newest first
        """

    def get_current(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get current (latest) version for a table.

        Args:
            table_name: Name of table

        Returns:
            Current version record, or None if no history
        """

    def compare_versions(
        self,
        table_name: str,
        version1: str,
        version2: str
    ) -> Dict[str, Any]:
        """
        Compare two versions and return differences.

        Args:
            table_name: Name of table
            version1: First version ID
            version2: Second version ID

        Returns:
            Dict with columns_added, columns_removed,
            columns_modified, row_count_change
        """

    def get_all_tables(self) -> List[str]:
        """Get list of all tracked tables."""

    def generate_audit_report(self) -> Dict[str, Any]:
        """
        Generate audit compliance report.

        Returns:
            Dict with tables, total_loads, date_range,
            changes_by_table
        """
```

---

## Exceptions

### SASReadError

```python
class SASReadError(Exception):
    """Raised when SAS file cannot be read."""

    def __init__(self, message: str, file_path: str):
        self.message = message
        self.file_path = file_path
        super().__init__(f"{message}: {file_path}")
```

### LoaderError

```python
class LoaderError(Exception):
    """Raised when data loading fails."""

    def __init__(self, message: str, table_name: str):
        self.message = message
        self.table_name = table_name
        super().__init__(f"{message}: {table_name}")
```

---

## Data Types

### ColumnInfo

```python
@dataclass
class ColumnInfo:
    """Column metadata."""
    name: str
    type: str
    nullable: bool = True
    length: Optional[int] = None
    label: Optional[str] = None
```

### VersionRecord

```python
@dataclass
class VersionRecord:
    """Schema version record."""
    version_id: str
    timestamp: datetime
    table_name: str
    columns: List[str]
    row_count: int
    file_path: Optional[str] = None
    file_hash: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
```

---

## Usage Examples

### Complete Data Loading Pipeline

```python
from core.data.sas_reader import SASReader
from core.data.duckdb_loader import DuckDBLoader
from core.data.schema_tracker import SchemaTracker
import hashlib

def load_clinical_data(sas_file: str, table_name: str):
    """Load SAS file into DuckDB with tracking."""

    # Initialize components
    reader = SASReader()
    loader = DuckDBLoader("data/database/clinical.duckdb")
    tracker = SchemaTracker("data/database/schema_versions.json")

    # Read SAS file
    df, metadata = reader.read(sas_file)

    # Compute file hash
    with open(sas_file, 'rb') as f:
        file_hash = f"sha256:{hashlib.sha256(f.read()).hexdigest()}"

    # Load into DuckDB
    row_count = loader.load_dataframe(df, table_name)

    # Track schema version
    version_id = tracker.record_load(
        table_name=table_name,
        columns=list(df.columns),
        row_count=row_count,
        file_path=sas_file,
        file_hash=file_hash
    )

    loader.close()

    return {
        "table": table_name,
        "rows": row_count,
        "version": version_id
    }
```

---

## Next Steps

- [Factory 1 Overview](overview.md)
- [Factory 2 Documentation](../factory2-metadata/overview.md)
