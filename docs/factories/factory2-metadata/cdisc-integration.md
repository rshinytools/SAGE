# CDISC Integration

Factory 2 integrates with CDISC (Clinical Data Interchange Standards Consortium) controlled terminology.

---

## Overview

**File:** `core/metadata/cdisc_library.py`

**Class:** `CDISCLibrary`

**Purpose:** Enrich metadata with CDISC standard definitions

---

## What is CDISC?

CDISC provides standardized terminology for clinical trials:

- **SDTM**: Study Data Tabulation Model
- **ADaM**: Analysis Data Model
- **Controlled Terminology**: Standard code lists

---

## CDISC Library Features

### Term Lookup

```python
from core.metadata.cdisc_library import CDISCLibrary

cdisc = CDISCLibrary()

# Lookup a variable
info = cdisc.lookup_variable("USUBJID")
# Returns: {
#   "name": "USUBJID",
#   "label": "Unique Subject Identifier",
#   "description": "Identifier used to uniquely identify a subject...",
#   "type": "Char",
#   "core": "Required"
# }
```

### Codelist Lookup

```python
# Get codelist values
values = cdisc.get_codelist("SEX")
# Returns: ["M", "F", "U", "UNDIFFERENTIATED"]
```

### Domain Information

```python
# Get domain definition
domain = cdisc.get_domain("AE")
# Returns: {
#   "name": "AE",
#   "description": "Adverse Events",
#   "class": "Events",
#   "structure": "One record per adverse event per subject"
# }
```

---

## API Reference

### CDISCLibrary Class

```python
class CDISCLibrary:
    def __init__(self, db_path: str = None):
        """
        Initialize CDISC library.

        Args:
            db_path: Path to CDISC SQLite database
        """

    def lookup_variable(
        self,
        name: str,
        domain: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Look up variable definition.

        Args:
            name: Variable name
            domain: Optional domain context

        Returns:
            Variable definition or None
        """

    def get_codelist(self, codelist_name: str) -> List[str]:
        """
        Get codelist values.

        Args:
            codelist_name: Name of codelist

        Returns:
            List of allowed values
        """

    def get_domain(self, domain_name: str) -> Optional[Dict]:
        """Get domain definition."""

    def validate_value(
        self,
        value: str,
        codelist_name: str
    ) -> bool:
        """Check if value is in codelist."""

    def enrich(self, metadata: Dict) -> Dict:
        """
        Enrich metadata with CDISC definitions.

        Args:
            metadata: Parsed metadata

        Returns:
            Enriched metadata with CDISC info
        """
```

---

## Enrichment Process

### Before Enrichment

```json
{
  "variables": {
    "USUBJID": {
      "label": "Subject ID",
      "type": "Char"
    }
  }
}
```

### After Enrichment

```json
{
  "variables": {
    "USUBJID": {
      "label": "Subject ID",
      "type": "Char",
      "cdisc": {
        "standard_label": "Unique Subject Identifier",
        "description": "Identifier used to uniquely identify a subject across all studies for all applications",
        "core": "Required",
        "role": "Identifier"
      }
    }
  }
}
```

---

## Controlled Terminology

### Common Codelists

| Codelist | Description | Example Values |
|----------|-------------|----------------|
| SEX | Sex of subject | M, F, U |
| RACE | Race category | WHITE, BLACK, ASIAN |
| NY | No/Yes | N, Y |
| AEOUT | AE Outcome | RECOVERED, NOT RECOVERED |
| AEREL | AE Relationship | RELATED, NOT RELATED |

### Using Codelists

```python
# Validate a value
if cdisc.validate_value("RELATED", "AEREL"):
    print("Valid AEREL value")

# Get all valid values
valid_values = cdisc.get_codelist("AEOUT")
```

---

## Database Structure

The CDISC library uses SQLite for fast lookups:

```sql
-- Variables table
CREATE TABLE variables (
    id INTEGER PRIMARY KEY,
    domain TEXT,
    name TEXT,
    label TEXT,
    description TEXT,
    type TEXT,
    core TEXT,
    role TEXT
);

-- Codelists table
CREATE TABLE codelists (
    id INTEGER PRIMARY KEY,
    name TEXT,
    extensible BOOLEAN
);

-- Codelist values table
CREATE TABLE codelist_values (
    id INTEGER PRIMARY KEY,
    codelist_id INTEGER,
    code TEXT,
    decode TEXT
);
```

---

## Updating CDISC Data

### Manual Update

```python
# Load from CDISC website export
cdisc.import_from_excel("cdisc_ct_2024_03.xlsx")
```

### Version Control

```python
# Check current version
version = cdisc.get_version()
# "CDISC CT 2024-03-29"

# Update to new version
cdisc.update_to_version("2024-06-28")
```

---

## Error Handling

### Unknown Terms

```python
info = cdisc.lookup_variable("CUSTOM_VAR")
if info is None:
    # Variable not in CDISC standard
    # Use study-specific definition
    pass
```

### Extensible Codelists

Some codelists allow custom values:

```python
codelist_info = cdisc.get_codelist_info("MEDDRA")
if codelist_info["extensible"]:
    # Custom values allowed
    pass
```

---

## Configuration

### Environment Variables

```env
# CDISC Library path
CDISC_LIBRARY_PATH=/app/knowledge/cdisc_library.db

# Auto-update settings
CDISC_AUTO_UPDATE=false
CDISC_VERSION=2024-03-29
```

---

## Best Practices

1. **Keep Updated**: Use latest CDISC controlled terminology
2. **Document Deviations**: Note when using non-standard terms
3. **Validate Early**: Check values during data entry
4. **Use Standard Labels**: Prefer CDISC labels when possible

---

## Next Steps

- [Excel Parser](excel-parser.md)
- [Approval Workflow](approval-workflow.md)
- [Factory 2 Overview](overview.md)
