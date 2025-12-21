# Factory 3.5 API Reference

Complete API documentation for Factory 3.5: MedDRA Integration.

---

## Module: core.meddra.meddra_loader

### MedDRALoader

```python
class MedDRALoader:
    """Load MedDRA ASCII files into SQLite database."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize loader.

        Args:
            db_path: Path for output database
        """

    def load_from_files(self, directory: str) -> Dict[str, int]:
        """
        Load MedDRA from ASCII files.

        Args:
            directory: Directory containing MedDRA files

        Returns:
            Dict of table names to row counts
        """

    def load_file(
        self,
        file_path: str,
        table_name: str
    ) -> int:
        """Load single MedDRA file."""

    def save_database(self, path: str) -> None:
        """Save loaded data to SQLite database."""

    def get_stats(self) -> Dict[str, Any]:
        """Get loading statistics."""
```

---

## Module: core.meddra.meddra_lookup

### MedDRALookup

```python
class MedDRALookup:
    """Query MedDRA terminology."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize lookup.

        Args:
            db_path: Path to MedDRA SQLite database
        """

    def lookup(self, term: str) -> Optional[Dict[str, Any]]:
        """
        Look up a term (LLT, PT, or SOC).

        Args:
            term: Term to look up (case-insensitive)

        Returns:
            Dict with term info and hierarchy, or None
        """

    def lookup_pt(self, pt_name: str) -> Optional[Dict[str, Any]]:
        """
        Look up a Preferred Term.

        Returns:
            Dict with pt_code, pt_name, soc_code, soc_name
        """

    def lookup_llt(self, llt_name: str) -> Optional[Dict[str, Any]]:
        """
        Look up a Lowest Level Term.

        Returns:
            Dict with llt info and mapped PT
        """

    def get_hierarchy(self, pt_code: str) -> Dict[str, Any]:
        """
        Get full hierarchy for a PT code.

        Returns:
            Dict with soc, hlgt, hlt, pt, llts
        """

    def get_hierarchy_from_llt(
        self,
        llt_name: str
    ) -> Optional[Dict[str, Any]]:
        """Get hierarchy starting from LLT name."""

    def get_llts_by_pt(
        self,
        pt_name: str
    ) -> List[str]:
        """Get all LLT names for a PT."""

    def get_pts_by_soc(
        self,
        soc_name: str
    ) -> List[str]:
        """Get all PT names under a SOC."""

    def get_pts_by_hlt(
        self,
        hlt_name: str
    ) -> List[str]:
        """Get all PT names under an HLT."""

    def search(
        self,
        query: str,
        level: str = "pt",
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """
        Search terms by prefix.

        Args:
            query: Search string
            level: "soc", "hlt", "pt", or "llt"
            limit: Maximum results

        Returns:
            List of matching terms
        """

    def get_all_socs(self) -> List[Dict[str, str]]:
        """Get all System Organ Classes."""

    def get_version(self) -> str:
        """Get MedDRA version string."""

    def is_valid_pt(self, term: str) -> bool:
        """Check if term is a valid PT."""

    def get_pt_code(self, pt_name: str) -> Optional[str]:
        """Get PT code for a PT name."""
```

---

## Data Types

### PTInfo

```python
@dataclass
class PTInfo:
    """Preferred Term information."""
    pt_code: str
    pt_name: str
    soc_code: str
    soc_name: str
    primary_soc: bool = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
```

### LLTInfo

```python
@dataclass
class LLTInfo:
    """Lowest Level Term information."""
    llt_code: str
    llt_name: str
    pt_code: str
    pt_name: str
    current: bool = True  # Is term current in MedDRA

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
```

### Hierarchy

```python
@dataclass
class Hierarchy:
    """Complete MedDRA hierarchy for a term."""
    soc: Dict[str, str]
    hlgt: Dict[str, str]
    hlt: Dict[str, str]
    pt: Dict[str, str]
    llts: List[Dict[str, str]]

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
```

---

## Configuration

### Environment Variables

```env
MEDDRA_DB_PATH=/app/knowledge/meddra.db
MEDDRA_VERSION=26.0
```

### File Structure

MedDRA ASCII files expected:

```
meddra_files/
├── llt.asc          # Lowest Level Terms
├── pt.asc           # Preferred Terms
├── hlt.asc          # High Level Terms
├── hlgt.asc         # High Level Group Terms
├── soc.asc          # System Organ Classes
├── hlt_pt.asc       # HLT-PT relationships
├── hlgt_hlt.asc     # HLGT-HLT relationships
└── soc_hlgt.asc     # SOC-HLGT relationships
```

---

## Database Schema

```sql
-- System Organ Classes
CREATE TABLE soc (
    soc_code TEXT PRIMARY KEY,
    soc_name TEXT NOT NULL,
    soc_abbrev TEXT
);

-- High Level Group Terms
CREATE TABLE hlgt (
    hlgt_code TEXT PRIMARY KEY,
    hlgt_name TEXT NOT NULL
);

-- High Level Terms
CREATE TABLE hlt (
    hlt_code TEXT PRIMARY KEY,
    hlt_name TEXT NOT NULL
);

-- Preferred Terms
CREATE TABLE pt (
    pt_code TEXT PRIMARY KEY,
    pt_name TEXT NOT NULL,
    soc_code TEXT,
    primary_soc BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (soc_code) REFERENCES soc(soc_code)
);

-- Lowest Level Terms
CREATE TABLE llt (
    llt_code TEXT PRIMARY KEY,
    llt_name TEXT NOT NULL,
    pt_code TEXT NOT NULL,
    llt_current BOOLEAN DEFAULT TRUE,
    FOREIGN KEY (pt_code) REFERENCES pt(pt_code)
);

-- Relationship tables
CREATE TABLE soc_hlgt (
    soc_code TEXT,
    hlgt_code TEXT,
    PRIMARY KEY (soc_code, hlgt_code)
);

CREATE TABLE hlgt_hlt (
    hlgt_code TEXT,
    hlt_code TEXT,
    PRIMARY KEY (hlgt_code, hlt_code)
);

CREATE TABLE hlt_pt (
    hlt_code TEXT,
    pt_code TEXT,
    PRIMARY KEY (hlt_code, pt_code)
);

-- Indexes for fast lookup
CREATE INDEX idx_llt_name ON llt(UPPER(llt_name));
CREATE INDEX idx_pt_name ON pt(UPPER(pt_name));
CREATE INDEX idx_soc_name ON soc(UPPER(soc_name));
CREATE INDEX idx_llt_pt ON llt(pt_code);
```

---

## Usage Examples

### Load MedDRA Data

```python
from core.meddra.meddra_loader import MedDRALoader

loader = MedDRALoader()
stats = loader.load_from_files("/path/to/meddra_26/")
print(f"Loaded {stats['pt']} PTs and {stats['llt']} LLTs")

loader.save_database("knowledge/meddra.db")
```

### Term Lookup

```python
from core.meddra.meddra_lookup import MedDRALookup

meddra = MedDRALookup("knowledge/meddra.db")

# Look up LLT
result = meddra.lookup("cephalalgia")
if result:
    print(f"PT: {result['pt_name']}")  # Headache
    print(f"SOC: {result['soc_name']}")  # Nervous system disorders

# Get full hierarchy
hierarchy = meddra.get_hierarchy_from_llt("cephalalgia")
print(f"HLT: {hierarchy['hlt']['name']}")  # Headaches NEC
```

### Search Terms

```python
# Find terms starting with "head"
matches = meddra.search("head", level="pt", limit=10)
for match in matches:
    print(f"{match['pt_name']} ({match['pt_code']})")
```

### SOC Analysis

```python
# Get all PTs under a SOC
nervous_pts = meddra.get_pts_by_soc("Nervous system disorders")
print(f"Found {len(nervous_pts)} PTs in Nervous system disorders")
```

---

## Error Handling

```python
class MedDRAError(Exception):
    """Base exception for MedDRA operations."""
    pass

class TermNotFoundError(MedDRAError):
    """Term not found in MedDRA."""
    def __init__(self, term: str):
        self.term = term
        super().__init__(f"Term not found: {term}")

class HierarchyError(MedDRAError):
    """Error navigating hierarchy."""
    pass
```

---

## Next Steps

- [Factory 3.5 Overview](overview.md)
- [MedDRA Hierarchy](hierarchy.md)
- [Factory 4 Integration](../factory4-engine/overview.md)
