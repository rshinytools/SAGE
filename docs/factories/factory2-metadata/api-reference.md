# Factory 2 API Reference

Complete API documentation for Factory 2: Metadata Refinery components.

---

## Module: core.metadata.excel_parser

### ExcelParser

```python
class ExcelParser:
    """Parse Excel specification files."""

    def __init__(self, config: Optional[Dict] = None):
        """
        Initialize parser.

        Args:
            config: Configuration with column mappings
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
            validate: Run validation on parsed data

        Returns:
            Parsed metadata dictionary with:
            - domains: List of domain definitions
            - variables: Dict of variable lists by domain
            - codelists: Dict of codelist values

        Raises:
            FileNotFoundError: File doesn't exist
            ParseError: Invalid file format
            ValidationError: Validation failed
        """

    def parse_sheet(
        self,
        workbook,
        sheet_name: str
    ) -> List[Dict]:
        """Parse a single sheet into list of records."""

    def validate(self, metadata: Dict) -> List[str]:
        """
        Validate parsed metadata.

        Returns:
            List of validation error messages
        """
```

---

## Module: core.metadata.cdisc_library

### CDISCLibrary

```python
class CDISCLibrary:
    """CDISC controlled terminology lookup."""

    def __init__(self, db_path: Optional[str] = None):
        """
        Initialize CDISC library.

        Args:
            db_path: Path to SQLite database
        """

    def lookup_variable(
        self,
        name: str,
        domain: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """
        Look up variable definition.

        Args:
            name: Variable name
            domain: Optional domain context

        Returns:
            Variable info dict or None if not found
        """

    def get_codelist(self, name: str) -> List[str]:
        """
        Get values for a codelist.

        Args:
            name: Codelist name

        Returns:
            List of allowed values
        """

    def get_codelist_with_decodes(
        self,
        name: str
    ) -> List[Dict[str, str]]:
        """
        Get codelist with code and decode pairs.

        Returns:
            List of {"code": "M", "decode": "Male"}
        """

    def validate_value(
        self,
        value: str,
        codelist: str
    ) -> bool:
        """Check if value is valid for codelist."""

    def get_domain(self, name: str) -> Optional[Dict]:
        """Get domain definition."""

    def enrich(self, metadata: Dict) -> Dict:
        """
        Enrich metadata with CDISC definitions.

        Adds CDISC info to each variable where available.
        """

    def get_version(self) -> str:
        """Get current CDISC CT version."""
```

---

## Module: core.metadata.metadata_store

### MetadataStore

```python
class MetadataStore:
    """Store and retrieve approved metadata."""

    def __init__(self, store_path: str):
        """
        Initialize metadata store.

        Args:
            store_path: Path to JSON storage file
        """

    def add_approved(
        self,
        metadata: Dict,
        approved_by: str,
        version: Optional[str] = None
    ) -> str:
        """
        Add approved metadata to store.

        Args:
            metadata: Approved metadata
            approved_by: Approver email
            version: Optional version label

        Returns:
            Version ID
        """

    def get_current(self) -> Dict:
        """Get current (latest) metadata version."""

    def get_version(self, version_id: str) -> Dict:
        """Get specific metadata version."""

    def get_variable(
        self,
        variable_name: str,
        domain: Optional[str] = None
    ) -> Optional[Dict]:
        """Get variable definition from current metadata."""

    def get_domain(self, domain_name: str) -> Optional[Dict]:
        """Get domain definition."""

    def search(self, query: str) -> List[Dict]:
        """Search variables by name or label."""

    def export(
        self,
        format: str = "json",
        output_path: str = None
    ) -> Union[str, None]:
        """
        Export metadata.

        Args:
            format: "json", "excel", or "html"
            output_path: Output file path (None returns string)
        """
```

---

## Module: core.metadata.approval_workflow

### ApprovalWorkflow

```python
class ApprovalWorkflow:
    """Manage metadata approval workflow."""

    def __init__(
        self,
        storage_path: str = "approvals.db",
        notifications: Optional[Dict] = None
    ):
        """
        Initialize workflow.

        Args:
            storage_path: SQLite database path
            notifications: Email notification config
        """

    def submit(
        self,
        metadata: Dict,
        submitter: str,
        description: str = ""
    ) -> str:
        """
        Submit metadata for approval.

        Returns:
            Submission ID
        """

    def assign_reviewer(
        self,
        submission_id: str,
        reviewer: str,
        assigned_by: str
    ) -> None:
        """Assign a reviewer to submission."""

    def approve(
        self,
        submission_id: str,
        reviewer: str,
        comments: str = ""
    ) -> None:
        """Approve a submission."""

    def reject(
        self,
        submission_id: str,
        reviewer: str,
        comments: str
    ) -> None:
        """Reject a submission (comments required)."""

    def get_pending(
        self,
        assigned_to: Optional[str] = None
    ) -> List[Dict]:
        """Get pending submissions."""

    def get_submission(self, submission_id: str) -> Dict:
        """Get submission details."""

    def get_history(
        self,
        submission_id: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict]:
        """Get approval history."""

    def bulk_approve(
        self,
        submission_ids: List[str],
        reviewer: str,
        comments: str
    ) -> None:
        """Bulk approve multiple submissions."""
```

---

## Data Types

### ParsedMetadata

```python
@dataclass
class ParsedMetadata:
    """Result of parsing specification file."""
    spec_type: str  # "sdtm" or "adam"
    domains: List[DomainInfo]
    variables: Dict[str, List[VariableInfo]]
    codelists: Dict[str, List[str]]
    source_file: str
    parsed_at: datetime
```

### VariableInfo

```python
@dataclass
class VariableInfo:
    """Variable definition."""
    name: str
    label: str
    type: str  # "Char" or "Num"
    length: Optional[int] = None
    description: Optional[str] = None
    codelist: Optional[str] = None
    origin: Optional[str] = None
    core: Optional[str] = None  # "Required", "Expected", "Permissible"
    cdisc: Optional[Dict] = None  # CDISC enrichment
```

### ApprovalSubmission

```python
@dataclass
class ApprovalSubmission:
    """Approval submission record."""
    id: str
    status: str  # "pending", "under_review", "approved", "rejected"
    metadata: Dict
    submitter: str
    submitted_at: datetime
    reviewer: Optional[str] = None
    reviewed_at: Optional[datetime] = None
    comments: Optional[str] = None
    history: List[Dict] = field(default_factory=list)
```

---

## Exceptions

```python
class ParseError(Exception):
    """Error parsing specification file."""
    def __init__(self, message: str, sheet: str = None, row: int = None):
        self.message = message
        self.sheet = sheet
        self.row = row

class ValidationError(Exception):
    """Metadata validation error."""
    def __init__(self, errors: List[str]):
        self.errors = errors

class ApprovalError(Exception):
    """Error in approval workflow."""
    pass
```

---

## Usage Example

```python
from core.metadata.excel_parser import ExcelParser
from core.metadata.cdisc_library import CDISCLibrary
from core.metadata.approval_workflow import ApprovalWorkflow
from core.metadata.metadata_store import MetadataStore

# 1. Parse specification
parser = ExcelParser()
raw_metadata = parser.parse("specs/raw/sdtm_spec.xlsx")

# 2. Enrich with CDISC
cdisc = CDISCLibrary()
enriched = cdisc.enrich(raw_metadata)

# 3. Submit for approval
workflow = ApprovalWorkflow()
submission_id = workflow.submit(
    metadata=enriched,
    submitter="analyst@example.com",
    description="SDTM spec v2.0"
)

# 4. Approve (after human review)
workflow.approve(
    submission_id=submission_id,
    reviewer="reviewer@example.com"
)

# 5. Store approved metadata
store = MetadataStore("knowledge/golden_metadata.json")
store.add_approved(
    metadata=workflow.get_submission(submission_id)["metadata"],
    approved_by="reviewer@example.com"
)
```

---

## Next Steps

- [Factory 2 Overview](overview.md)
- [Factory 3 Documentation](../factory3-dictionary/overview.md)
