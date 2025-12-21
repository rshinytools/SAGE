# Factory 4 API Reference

Complete API documentation for Factory 4: Inference Engine.

---

## Main Pipeline

### InferencePipeline

```python
class InferencePipeline:
    """Main orchestrator for query processing."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        """
        Initialize pipeline with all components.

        Args:
            config: Optional pipeline configuration
        """

    def process(
        self,
        query: str,
        session_id: Optional[str] = None,
        include_sql: bool = True
    ) -> PipelineResult:
        """
        Process a natural language query.

        Args:
            query: User's question
            session_id: Conversation ID for context
            include_sql: Include SQL in response

        Returns:
            PipelineResult with answer, confidence, etc.
        """

    def is_ready(self) -> bool:
        """Check if all components are initialized."""

    def get_status(self) -> Dict[str, str]:
        """Get status of each component."""
```

### PipelineResult

```python
@dataclass
class PipelineResult:
    success: bool
    query: str
    answer: str
    data: Optional[List[Dict]] = None
    row_count: int = 0
    sql: Optional[str] = None
    methodology: Optional[Dict] = None
    confidence: Optional[Dict] = None
    warnings: List[str] = field(default_factory=list)
    pipeline_stages: Dict[str, Any] = field(default_factory=dict)
    total_time_ms: float = 0.0
    error: Optional[str] = None
    error_stage: Optional[str] = None
    timestamp: str = field(default_factory=...)

    def format_response(self, include_sql: bool = True) -> str:
        """Format complete response for user."""

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for API."""
```

---

## Step 1: Input Sanitizer

### InputSanitizer

```python
class InputSanitizer:
    def __init__(self, config: Optional[Dict] = None):
        """Initialize with optional custom patterns."""

    def sanitize(self, query: str) -> SanitizationResult:
        """Sanitize query for security."""

    def check_phi(self, query: str) -> List[str]:
        """Check for PHI patterns."""

    def check_sql_injection(self, query: str) -> List[str]:
        """Check for SQL injection."""

    def check_prompt_injection(self, query: str) -> List[str]:
        """Check for prompt injection."""
```

---

## Step 2: Entity Extractor

### EntityExtractor

```python
class EntityExtractor:
    def __init__(
        self,
        fuzzy_matcher: Optional[FuzzyMatcher] = None,
        meddra_lookup: Optional[MedDRALookup] = None,
        min_confidence: float = 70.0
    ):
        """Initialize with matching components."""

    def extract(self, query: str) -> EntityExtractionResult:
        """
        Extract clinical entities from query.

        Returns:
            EntityExtractionResult with entities list
        """

    def extract_with_synonyms(
        self,
        query: str
    ) -> EntityExtractionResult:
        """Extract with medical synonym lookup."""
```

### EntityMatch

```python
@dataclass
class EntityMatch:
    original_term: str
    matched_term: str
    match_type: str  # exact, fuzzy, meddra, medical_synonym
    confidence: float
    table: Optional[str] = None
    column: Optional[str] = None
    meddra_code: Optional[str] = None
    metadata: Optional[Dict] = None
```

---

## Step 3: Table Resolver

### TableResolver

```python
class TableResolver:
    def __init__(self, clinical_config: Optional[ClinicalConfig] = None):
        """Initialize with clinical rules."""

    def resolve(
        self,
        entities: List[EntityMatch],
        query: str,
        available_tables: List[str]
    ) -> TableResolution:
        """
        Resolve which table and population to use.

        Returns:
            TableResolution with table, population, columns
        """
```

### TableResolution

```python
@dataclass
class TableResolution:
    selected_table: str
    table_type: str  # ADaM or SDTM
    domain: QueryDomain
    selection_reason: str
    population: PopulationType
    population_filter: str
    population_name: str
    columns_resolved: Dict[str, ColumnResolution]
    fallback_used: bool
    available_tables: List[str]
    table_columns: List[str]
    assumptions: List[str] = field(default_factory=list)
```

---

## Step 4: Context Builder

### ContextBuilder

```python
class ContextBuilder:
    def __init__(self, config: Optional[Dict] = None):
        """Initialize context builder."""

    def build(
        self,
        query: str,
        table_resolution: TableResolution,
        entities: List[EntityMatch],
        session_context: Optional[Dict] = None
    ) -> LLMContext:
        """
        Build LLM context for SQL generation.

        Returns:
            LLMContext with system_prompt, user_prompt, etc.
        """
```

### LLMContext

```python
@dataclass
class LLMContext:
    system_prompt: str
    user_prompt: str
    schema_context: str
    entity_context: str
    clinical_rules: str
    token_count_estimate: int
```

---

## Step 5: SQL Generator

### SQLGenerator

```python
class SQLGenerator:
    def __init__(
        self,
        api_key: Optional[str] = None,
        model: str = "claude-sonnet-4-20250514"
    ):
        """Initialize with Claude API."""

    def generate(self, context: LLMContext) -> GeneratedSQL:
        """
        Generate SQL from context.

        Returns:
            GeneratedSQL with sql, reasoning, metadata
        """
```

### GeneratedSQL

```python
@dataclass
class GeneratedSQL:
    sql: str
    reasoning: str
    tables_used: List[str]
    columns_used: List[str]
    filters_applied: List[str]
    generation_time_ms: float
    model_used: str
    raw_response: Optional[str] = None
```

---

## Step 6: SQL Validator

### SQLValidator

```python
class SQLValidator:
    def __init__(self, schema: Optional[Dict] = None):
        """Initialize with schema for validation."""

    def validate(
        self,
        sql: str,
        schema: Dict
    ) -> ValidationResult:
        """
        Validate SQL for safety and correctness.

        Returns:
            ValidationResult with is_valid, errors, warnings
        """

    def check_syntax(self, sql: str) -> bool:
        """Check SQL syntax validity."""

    def check_tables(self, sql: str, tables: List[str]) -> List[str]:
        """Verify all tables exist."""

    def check_dangerous_patterns(self, sql: str) -> List[str]:
        """Check for dangerous SQL patterns."""
```

---

## Step 7: Executor

### Executor

```python
class Executor:
    def __init__(
        self,
        db_path: str,
        timeout_seconds: int = 30,
        max_rows: int = 10000,
        read_only: bool = True
    ):
        """Initialize DuckDB executor."""

    def execute(self, sql: str) -> ExecutionResult:
        """
        Execute SQL query.

        Returns:
            ExecutionResult with data, row_count, etc.
        """
```

### ExecutionResult

```python
@dataclass
class ExecutionResult:
    success: bool
    data: Optional[List[Dict]] = None
    columns: List[str] = field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    error_message: Optional[str] = None
    truncated: bool = False
    sql_executed: Optional[str] = None
```

---

## Step 8: Confidence Scorer

### ConfidenceScorer

```python
class ConfidenceScorer:
    def __init__(self, config: Optional[Dict] = None):
        """Initialize with optional weight configuration."""

    def score(
        self,
        entities: List[EntityMatch],
        validation: ValidationResult,
        execution: ExecutionResult,
        metadata: Optional[Dict] = None
    ) -> ConfidenceScore:
        """Calculate confidence score."""
```

### ConfidenceScore

```python
@dataclass
class ConfidenceScore:
    overall_score: float
    level: ConfidenceLevel
    components: Dict[str, Any]
    explanation: str

    def to_dict(self) -> Dict[str, Any]:
        """Convert to API response format."""
```

---

## Step 9: Explanation Generator

### ExplanationGenerator

```python
class ExplanationGenerator:
    def __init__(self, config: Optional[Dict] = None):
        """Initialize explanation generator."""

    def generate(
        self,
        query: str,
        execution: ExecutionResult,
        confidence: ConfidenceScore,
        resolution: TableResolution,
        entities: List[EntityMatch]
    ) -> str:
        """Generate human-readable answer."""

    def generate_methodology(
        self,
        resolution: TableResolution,
        entities: List[EntityMatch]
    ) -> QueryMethodology:
        """Generate methodology documentation."""
```

---

## Support Components

### SessionMemory

```python
class SessionMemory:
    def __init__(self, max_history: int = 10):
        """Initialize session memory."""

    def add_turn(
        self,
        session_id: str,
        query: str,
        result: PipelineResult
    ) -> None:
        """Add conversation turn."""

    def get_context(
        self,
        session_id: str
    ) -> Optional[Dict]:
        """Get accumulated context for session."""

    def get_filters(
        self,
        session_id: str
    ) -> List[str]:
        """Get filters from previous queries."""
```

### MedicalSynonyms

```python
def resolve_medical_term(term: str) -> Optional[SynonymMapping]:
    """
    Resolve a term using synonym dictionaries.

    Checks:
    1. Complex phrase mappings
    2. Colloquial mappings
    3. UK/US spelling variants
    """

def get_spelling_variants(term: str) -> Optional[Tuple[str, ...]]:
    """Get UK/US spelling variants."""

def build_in_clause(mapping: SynonymMapping) -> str:
    """Build SQL IN clause for variants."""
```

---

## Usage Example

```python
from core.engine.pipeline import InferencePipeline

# Initialize pipeline
pipeline = InferencePipeline()

# Process query
result = pipeline.process(
    query="How many subjects had headache in the safety population?",
    session_id="user_123_session_1"
)

# Use result
if result.success:
    print(result.answer)
    print(f"Confidence: {result.confidence['score']}%")
    print(f"SQL: {result.sql}")
else:
    print(f"Error: {result.error}")
```

---

## Next Steps

- [Factory 4 Overview](overview.md)
- [Pipeline Steps](pipeline-steps.md)
