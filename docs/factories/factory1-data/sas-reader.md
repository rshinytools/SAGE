# SAS Reader

The SAS Reader component reads SAS7BDAT files and converts them to Pandas DataFrames.

---

## Overview

**File:** `core/data/sas_reader.py`

**Class:** `SASReader`

**Dependencies:**
- `pyreadstat` - SAS file reading
- `pandas` - DataFrame handling

---

## Usage

### Basic Reading

```python
from core.data.sas_reader import SASReader

reader = SASReader()
df, metadata = reader.read("path/to/file.sas7bdat")
```

### With Options

```python
df, metadata = reader.read(
    file_path="data/raw/adsl.sas7bdat",
    encoding="latin1",
    convert_dates=True
)
```

---

## API Reference

### SASReader Class

```python
class SASReader:
    def __init__(self, encoding: str = "utf-8"):
        """
        Initialize SAS reader.

        Args:
            encoding: Default character encoding
        """

    def read(
        self,
        file_path: str,
        encoding: Optional[str] = None,
        convert_dates: bool = True
    ) -> Tuple[pd.DataFrame, Dict]:
        """
        Read a SAS7BDAT file.

        Args:
            file_path: Path to SAS file
            encoding: Character encoding (overrides default)
            convert_dates: Convert SAS dates to datetime

        Returns:
            Tuple of (DataFrame, metadata dict)
        """

    def get_metadata(self, file_path: str) -> Dict:
        """
        Get file metadata without reading data.

        Returns:
            Dict with column names, types, labels
        """
```

---

## Metadata Extraction

The reader extracts rich metadata:

```python
metadata = reader.get_metadata("data/raw/adsl.sas7bdat")

# metadata structure:
{
    "file_path": "data/raw/adsl.sas7bdat",
    "row_count": 233,
    "column_count": 45,
    "columns": [
        {
            "name": "USUBJID",
            "type": "character",
            "length": 20,
            "label": "Unique Subject Identifier"
        },
        {
            "name": "AGE",
            "type": "numeric",
            "length": 8,
            "label": "Age"
        }
    ],
    "encoding": "latin1",
    "created": "2024-01-01T00:00:00",
    "modified": "2024-01-15T10:30:00"
}
```

---

## Date Handling

### SAS Date Conversion

SAS stores dates as days since January 1, 1960:

```python
# SAS date 22280 = 2020-12-31
sas_date = 22280
python_date = datetime(1960, 1, 1) + timedelta(days=sas_date)
```

### Automatic Detection

Date columns are detected by:

1. SAS format (DATE9., DATETIME20., etc.)
2. Column name patterns (*DT, *DTC, *STDT)
3. Metadata labels containing "date"

---

## Encoding Handling

### Common Encodings

| Region | Encoding |
|--------|----------|
| US/Europe | latin1, utf-8 |
| Japan | shift_jis |
| China | gbk |

### Auto-Detection

```python
# Try multiple encodings
encodings = ['utf-8', 'latin1', 'shift_jis']
for enc in encodings:
    try:
        df, meta = reader.read(file_path, encoding=enc)
        break
    except UnicodeDecodeError:
        continue
```

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `FileNotFoundError` | File doesn't exist | Check path |
| `SAS7BDATReadError` | Corrupt file | Re-export from SAS |
| `UnicodeDecodeError` | Wrong encoding | Try different encoding |

### Example Error Handling

```python
from core.data.sas_reader import SASReader, SASReadError

reader = SASReader()

try:
    df, metadata = reader.read("data/raw/adsl.sas7bdat")
except FileNotFoundError:
    print("File not found")
except SASReadError as e:
    print(f"Error reading SAS file: {e}")
```

---

## Performance

### Large File Handling

For files with millions of rows:

```python
# Read in chunks
reader = SASReader()
for chunk in reader.read_chunks(file_path, chunk_size=100000):
    process(chunk)
```

### Memory Optimization

```python
# Read specific columns only
df, metadata = reader.read(
    file_path,
    columns=["USUBJID", "AGE", "SEX"]
)
```

---

## Testing

```python
def test_read_sas_file():
    reader = SASReader()
    df, metadata = reader.read("tests/data/test_adsl.sas7bdat")

    assert len(df) > 0
    assert "USUBJID" in df.columns
    assert metadata["row_count"] == len(df)
```

---

## Next Steps

- [DuckDB Loader](duckdb-loader.md)
- [Schema Tracker](schema-tracker.md)
- [Factory 1 Overview](overview.md)
