# Excel Parser

The Excel Parser extracts variable definitions from clinical trial specification files.

---

## Overview

**File:** `core/metadata/excel_parser.py`

**Class:** `ExcelParser`

**Purpose:** Parse SDTM/ADaM Excel specifications into structured metadata

---

## Supported Formats

### SDTM Specification

Expected sheets:

| Sheet Name | Content |
|------------|---------|
| Domains | Domain list with descriptions |
| Variables | Variable definitions by domain |
| Codelists | Controlled terminology definitions |
| Value Level | Value-level metadata |

### ADaM Specification

Expected sheets:

| Sheet Name | Content |
|------------|---------|
| Datasets | Dataset list with descriptions |
| Variables | Analysis variable definitions |
| Results Metadata | Analysis results info |

---

## Usage

### Basic Parsing

```python
from core.metadata.excel_parser import ExcelParser

parser = ExcelParser()
metadata = parser.parse("specs/raw/sdtm_spec.xlsx")
```

### With Options

```python
metadata = parser.parse(
    file_path="specs/raw/adam_spec.xlsx",
    spec_type="adam",
    validate=True
)
```

---

## API Reference

### ExcelParser Class

```python
class ExcelParser:
    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize parser.

        Args:
            config: Optional configuration dictionary
        """

    def parse(
        self,
        file_path: str,
        spec_type: str = "auto",
        validate: bool = True
    ) -> Dict[str, Any]:
        """
        Parse Excel specification file.

        Args:
            file_path: Path to Excel file
            spec_type: "sdtm", "adam", or "auto"
            validate: Validate parsed data

        Returns:
            Parsed metadata dictionary
        """

    def parse_domains(self, workbook) -> List[Dict]:
        """Parse domain definitions sheet."""

    def parse_variables(self, workbook) -> Dict[str, List[Dict]]:
        """Parse variable definitions sheet."""

    def parse_codelists(self, workbook) -> Dict[str, List[str]]:
        """Parse codelist definitions."""
```

---

## Output Structure

### Domain Metadata

```json
{
  "domains": [
    {
      "name": "AE",
      "description": "Adverse Events",
      "class": "Events",
      "structure": "One record per adverse event",
      "keys": ["STUDYID", "USUBJID", "AESEQ"]
    }
  ]
}
```

### Variable Metadata

```json
{
  "variables": {
    "AE": [
      {
        "name": "USUBJID",
        "label": "Unique Subject Identifier",
        "type": "Char",
        "length": 20,
        "origin": "Derived",
        "required": "Required",
        "role": "Identifier"
      },
      {
        "name": "AEDECOD",
        "label": "Dictionary-Derived Term",
        "type": "Char",
        "length": 200,
        "codelist": "MEDDRA",
        "required": "Required"
      }
    ]
  }
}
```

---

## Column Mapping

### Standard Column Names

The parser recognizes these column headers:

| Expected | Alternatives |
|----------|-------------|
| Variable Name | Name, Variable, Var |
| Label | Variable Label, Description |
| Type | Data Type, SAS Type |
| Length | SAS Length, Size |
| Codelist | Controlled Terms, CT |
| Origin | Source, Derivation |

### Custom Mapping

```python
parser = ExcelParser(config={
    "column_mapping": {
        "Variable": "name",
        "Var Label": "label",
        "Data Type": "type"
    }
})
```

---

## Validation

### Required Fields

The parser validates these required fields:

| Field | Validation |
|-------|------------|
| name | Non-empty, valid identifier |
| label | Non-empty |
| type | "Char" or "Num" |
| length | Positive integer |

### Validation Errors

```python
try:
    metadata = parser.parse("spec.xlsx", validate=True)
except ValidationError as e:
    print(f"Validation failed: {e.errors}")
    # [{"variable": "AGE", "error": "Missing label"}]
```

---

## Auto-Detection

The parser auto-detects specification type:

```python
# Auto-detect based on sheet names
metadata = parser.parse("spec.xlsx", spec_type="auto")

# Explicit type
metadata = parser.parse("spec.xlsx", spec_type="adam")
```

### Detection Rules

| Contains Sheet | Detected Type |
|----------------|---------------|
| "Domains" | SDTM |
| "Datasets" + "Analysis" | ADaM |
| "Variables" only | Custom |

---

## Error Handling

### Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| `FileNotFoundError` | File missing | Check path |
| `SheetNotFoundError` | Required sheet missing | Check Excel structure |
| `ColumnNotFoundError` | Required column missing | Add column or map |
| `ValidationError` | Invalid data | Fix specification |

### Error Details

```python
from core.metadata.excel_parser import ParseError

try:
    metadata = parser.parse("spec.xlsx")
except ParseError as e:
    print(f"Sheet: {e.sheet}")
    print(f"Row: {e.row}")
    print(f"Error: {e.message}")
```

---

## Best Practices

1. **Consistent Naming**: Use standard CDISC column names
2. **Complete Data**: Fill all required fields
3. **Valid Values**: Use controlled terminology
4. **Clean Data**: Remove empty rows/columns
5. **UTF-8**: Save files with UTF-8 encoding

---

## Next Steps

- [CDISC Integration](cdisc-integration.md)
- [Approval Workflow](approval-workflow.md)
- [Factory 2 Overview](overview.md)
